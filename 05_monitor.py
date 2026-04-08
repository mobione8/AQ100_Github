#!/usr/bin/env python3
"""
AQS Strategy Monitor  v16.0
==========================================
Run with: streamlit run 05_monitor.py

Changelog:
  v16.0 - Production hardening: rotating log, execId dedup, auto-reconnect,
           CAGR display, iterrows() removed, bare-except logging, commission
           flag, 0-byte integrity check, thread-join warning, import cleanup,
           MIN_CAGR_DAYS at module level, auto-refresh data fetch, last_refresh
           semantics, live_executions reset, use_container_width.
"""

import streamlit as st
import streamlit.components.v1 as components

# streamlit-autorefresh is REQUIRED for live-IBKR use.
# A JS location.reload() fallback destroys st.session_state on every cycle,
# killing the IBKR connection and forcing manual reconnect N times per hour.
# st_autorefresh triggers a Streamlit script rerun, preserving session state.
# Install: pip install streamlit-autorefresh
try:
    from streamlit_autorefresh import st_autorefresh  # type: ignore
    _HAS_ST_AUTOR = True
except ImportError:
    st_autorefresh = None
    _HAS_ST_AUTOR = False
import hashlib
import logging
import logging.handlers
import os
import re
import sys
import threading
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from decimal import Decimal

try:
    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper
    from ibapi.execution import ExecutionFilter
    from ibapi.contract import Contract
    IBKR_AVAILABLE = True
except ImportError:
    IBKR_AVAILABLE = False

# --- Path anchors (script lives at project root) ---
IBKR_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ibkr_deployment')

# Client ID manager lives in ibkr_deployment/
sys.path.insert(0, IBKR_DIR)
try:
    from client_id_manager import get_or_allocate_client_id as _alloc_client_id
    _CLIENT_ID_MGR_AVAILABLE = True
except ImportError:
    _CLIENT_ID_MGR_AVAILABLE = False


@st.cache_resource
def _resolve_monitor_client_id() -> int:
    """Allocate a stable clientId for this monitor instance. Runs once per server lifetime."""
    if not _CLIENT_ID_MGR_AVAILABLE:
        return 101  # fallback: original default
    try:
        name = os.path.splitext(os.path.basename(__file__))[0]  # e.g. "05_monitor"
        return _alloc_client_id(name=name, role="monitor", preferred=101)
    except Exception:
        return 101  # never crash Streamlit on startup


# =============================================================================
# CSV PERSISTENCE CONFIG
# =============================================================================
# Dashboard looks for trade CSVs in a ./trades/ folder relative to this script.
# The portfolio strategy writes to: trades/trades_{SYMBOL}_all.csv
# The dashboard merges all CSVs it finds in that folder.
TRADES_DIR = os.path.join(IBKR_DIR, 'trades')

# Master snapshot — full merged history written on every refresh so the
# trade log survives dashboard restarts regardless of IBKR connectivity.
SNAPSHOT_FILE = os.path.join(TRADES_DIR, '_snapshot_all_trades.csv')

# Eagerly create the trades directory so offline saves always have a home.
os.makedirs(TRADES_DIR, exist_ok=True)

# Seconds to wait for market-data ticks after issuing reqMktData requests.
# Increase on slow connections or paper-trading accounts with delayed data.
MARKET_DATA_WAIT_SECS: float = 2.0

# Minimum trade history (days) before CAGR is considered meaningful.
# With < 30 days, a 1% daily gain annualises to 3,678% — suppress as N/A.
MIN_CAGR_DAYS: int = 30

# Column schema — must match portfolio strategy's TRADE_CSV_FIELDS
CSV_SCHEMA = [
    'execId', 'datetime', 'symbol', 'secType', 'exchange', 'currency',
    'side', 'quantity', 'price', 'commission', 'orderRef',
]

# Fallback orderRef value written when an execution carries no strategy tag
UNTAGGED_ORDERREF = "UNTAGGED"

# Supported datetime formats tried in order: IBKR native first, then ISO variants.
# Used by save_executions_to_csv, save_merged_snapshot, and process_executions.
TIME_FORMATS = (
    '%Y%m%d-%H:%M:%S', '%Y%m%d %H:%M:%S',
    '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%d %H:%M:%S.%f',
)


# =============================================================================
# IBKR ERROR CODE TRANSLATIONS
# =============================================================================
# Maps IBKR numeric error/info codes to human-readable descriptions.
# Used by _translate_error_status() to decode raw price_errors strings.
IBKR_ERROR_MESSAGES = {
    # ── Connection / market-data farm status ─────────────────────────────────
    2100: "New account data requested — API client unsubscribed from account data",
    2101: "Market data not subscribed for this symbol",
    2102: "API subscription request failed",
    2103: "Market data farm connection broken",
    2104: "Market data farm connection OK",
    2105: "HMDS data farm connection broken",
    2106: "HMDS data farm connection OK",
    2107: "HMDS data farm connection inactive",
    2108: "Market data farm connection inactive",
    2109: "Order event warning: advance orders rejected at this time",
    2110: "TWS–server connectivity broken",
    2119: "Market data farm is connecting",
    2137: "Cross-side warning",
    2148: "Requested market data is not subscribed (ETradeOnly/FirmQuoteOnly)",
    2158: "Sec-def data farm connection OK",
    # ── Common request errors ─────────────────────────────────────────────────
    200:  "No security definition found for the request",
    300:  "Invalid order size: can't be zero",
    320:  "Error in reqMktData — no subscription",
    321:  "Error in reqMktDepth — no subscription",
    354:  "Requested market data not subscribed — snapshot data temporarily unavailable",
    366:  "No historical data query found for ticker",
    382:  "Order size does not conform to market rule",
    # ── Connectivity ──────────────────────────────────────────────────────────
    1100: "Connection to TWS lost",
    1101: "Connection restored — data lost: re-subscribing",
    1102: "Connection restored — data maintained",
    10167: "Requested market data is not subscribed — delayed data available",
}

# =============================================================================
# LOGGING
# =============================================================================
_LOG_DIR = os.path.join(IBKR_DIR, 'logs')
os.makedirs(_LOG_DIR, exist_ok=True)
_log = logging.getLogger('aqs_monitor')
_log.setLevel(logging.DEBUG)
if not _log.handlers:
    _lh = logging.handlers.RotatingFileHandler(
        os.path.join(_LOG_DIR, 'monitor.log'),
        maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8')
    _lh.setFormatter(logging.Formatter('%(asctime)s %(levelname)-8s %(message)s'))
    _log.addHandler(_lh)


def _translate_error_status(raw: str) -> str:
    """
    Convert a raw IBKR error string stored in price_errors into a readable message.

    Handles two formats produced by the ibapi version-detection branch:
      • Normal (ibapi 9.x parsed correctly):
            "Error 2103: Market data farm connection is broken."
        → looks up code 2103 in IBKR_ERROR_MESSAGES
      • Mangled (ibapi 10.x timestamp passed as int, triggers 9.x branch):
            "Error 1772471400109: 2103"
        → the 'errorCode' field is a ms-timestamp; the 'errorString' is the real code
        → parses the real code from the second token and looks it up
    """
    if not raw or not raw.startswith("Error "):
        return raw
    m = re.match(r"Error\s+(\d+):\s+(.*)", raw)
    if not m:
        return raw
    part1, part2 = m.group(1), m.group(2).strip()
    # If part2 is a bare integer it's the ibapi-10.x mangled case:
    # part1 = millisecond timestamp, part2 = real IBKR code
    if part2.isdigit():
        code = int(part2)
        description = IBKR_ERROR_MESSAGES.get(code, f"IBKR info/error code {code}")
        return f"[{code}] {description}"
    else:
        code = int(part1)
        description = IBKR_ERROR_MESSAGES.get(code, part2 or f"IBKR code {code}")
        return f"[{code}] {description}"


# ---------------------------------------------------------------------------
# Translate IBKR "Error <ts>: <code>" print lines to readable messages.
# The error() callback in IBClient uses print() directly; since that function
# is not modified here we intercept stdout and rewrite matching lines so the
# terminal log is readable without any logic change in the callback.
# ---------------------------------------------------------------------------
class _IBKRPrintTranslator:
    """Wraps sys.stdout to translate raw IBKR error print lines on the fly."""

    def __init__(self, wrapped):
        self._wrapped = wrapped

    def write(self, text):
        if text and text.startswith("Error "):
            translated = _translate_error_status(text.rstrip("\n"))
            if translated != text.rstrip("\n"):
                self._wrapped.write(translated + "\n")
                return
        self._wrapped.write(text)

    def flush(self):
        self._wrapped.flush()

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


if not isinstance(sys.stdout, _IBKRPrintTranslator):
    sys.stdout = _IBKRPrintTranslator(sys.stdout)


def to_float(value):
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


def stable_exec_id(ex: dict) -> str:
    """
    Return a deterministic ID for a trade execution.
    Uses the real execId when present; otherwise derives a stable hash from
    the trade's key fields so deduplication works even for CSV-sourced rows
    that were never assigned an IBKR execId.
    """
    eid = str(ex.get('execId', '') or '').strip()
    # Guard against sentinel strings produced when pandas reads a blank execId
    # cell with dtype=str: float('nan') -> 'nan', pd.NA -> '<NA>', None -> 'None'.
    # Without this, every blank-execId row shares the literal key 'nan', causing
    # all but the last such row to be silently dropped during deduplication.
    _MISSING = {'', 'nan', 'none', '<na>', 'nat'}
    if eid and eid.lower() not in _MISSING and not eid.startswith('SYNTHETIC_'):
        return eid
    raw = "|".join([
        str(ex.get('time', ex.get('datetime', ''))),
        str(ex.get('symbol', '')),
        str(ex.get('side', '')),
        str(ex.get('quantity', '')),
        str(ex.get('price', '')),
    ])
    return 'SYNTHETIC_' + hashlib.md5(raw.encode()).hexdigest()[:16]


# =============================================================================
# CSV LOAD / SAVE
# =============================================================================

def load_all_csv_trades() -> list:
    """
    Load all trade CSVs from the trades/ directory.
    Returns a list of execution dicts in the same format as IBClient.executions.
    """
    if not os.path.exists(TRADES_DIR):
        return []

    all_rows = []
    for fname in os.listdir(TRADES_DIR):
        if not fname.endswith('.csv'):
            continue
        # Skip the master snapshot — it is loaded separately by load_snapshot()
        # and merging it here would cause every trade to be processed twice.
        if fname == os.path.basename(SNAPSHOT_FILE):
            continue
        fpath = os.path.join(TRADES_DIR, fname)
        try:
            df = pd.read_csv(fpath, dtype=str)
            for row in df.to_dict('records'):
                raw_exec = {
                    'time':        ' '.join(str(row.get('datetime', '') or '').replace('T', ' ').replace('Z', '').strip().split()[:2]),
                    'symbol':      row.get('symbol', ''),
                    'secType':     row.get('secType', 'STK'),
                    'exchange':    row.get('exchange', 'SMART'),
                    'currency':    row.get('currency', 'USD'),
                    'localSymbol': row.get('symbol', ''),
                    'side':        row.get('side', ''),
                    'quantity':    to_float(row.get('quantity', 0)),
                    'price':       to_float(row.get('price', 0)),
                    'orderRef':    row.get('orderRef', UNTAGGED_ORDERREF),
                    'execId':      row.get('execId', ''),
                    'commission':  to_float(row.get('commission', 0)),
                }
                # Ensure every row has a stable, non-empty execId.
                # Use a proper copy so we don't mutate raw_exec in place.
                exec_dict = {**raw_exec}
                exec_dict['execId'] = stable_exec_id(raw_exec)
                all_rows.append(exec_dict)
        except Exception as e:
            st.warning(f"Could not load {fname}: {e}")

    return all_rows


def save_executions_to_csv(executions: list, symbol_filter: str = None):
    """
    Save IBKR live executions to the master CSV file, deduplicating on execId.
    If symbol_filter provided, only save rows for that symbol.
    """
    os.makedirs(TRADES_DIR, exist_ok=True)

    # Determine output file — one per symbol to match portfolio strategy convention
    symbols = set(e['symbol'] for e in executions if e.get('symbol'))
    for symbol in symbols:
        if symbol_filter and symbol != symbol_filter:
            continue

        fpath = os.path.join(TRADES_DIR, f'trades_{symbol}_all.csv')

        # Load existing rows for dedup
        existing_ids = set()
        existing_rows = []
        if os.path.exists(fpath):
            try:
                df_existing = pd.read_csv(fpath, dtype=str)
                existing_ids = set(df_existing['execId'].dropna().tolist())
                existing_rows = df_existing.to_dict('records')
            except Exception as _e:
                _log.warning("Could not read existing CSV %s — starting fresh: %s", fpath, _e)

        new_rows = []
        for ex in executions:
            if ex.get('symbol') != symbol:
                continue
            exec_id = ex.get('execId', '')
            if exec_id in existing_ids:
                continue

            # Convert IBKR time format to ISO.
            # IBKR sends either '20260304-09:31:04' or '20260304 09:31:04 US/Eastern'.
            raw_time = ex.get('time', '')
            dt = raw_time
            for _fmt in TIME_FORMATS:
                try:
                    # Strip timezone suffix before parsing (take first two space-parts)
                    _s = ' '.join(raw_time.strip().split()[:2])
                    dt = datetime.strptime(_s[:26], _fmt).strftime('%Y-%m-%d %H:%M:%S')
                    break
                except Exception:
                    continue

            # Map side: IBKR uses BOT/SLD
            side = ex.get('side', '')
            if side == 'BOT':
                ibkr_side = 'BOT'
            elif side in ('SLD', 'SELL'):
                ibkr_side = 'SLD'
            else:
                ibkr_side = side

            row = {
                'execId':      exec_id,
                'datetime':    dt,
                'symbol':      symbol,
                'secType':     ex.get('secType', 'STK'),
                'exchange':    ex.get('exchange', 'SMART'),
                'currency':    ex.get('currency', 'USD'),
                'side':        ibkr_side,
                'quantity':    ex.get('quantity', 0),
                'price':       ex.get('price', 0),
                'commission':  ex.get('commission', 0),
                'orderRef':    ex.get('orderRef', UNTAGGED_ORDERREF),
            }
            new_rows.append(row)
            existing_ids.add(exec_id)

        if new_rows:
            all_rows = existing_rows + new_rows
            df_out = pd.DataFrame(all_rows, columns=CSV_SCHEMA)
            # Atomic write: same pattern as save_merged_snapshot.
            # A direct .to_csv(fpath) write leaves the file partially written if
            # the process is killed mid-write (TWS crash, SIGKILL, power loss).
            # On next startup pd.read_csv raises ParserError → outer except skips
            # the entire file → all trades for that symbol are temporarily lost.
            # Writing to a .tmp sidecar then renaming atomically (POSIX rename
            # syscall) ensures the reader always sees a complete file.
            _tmp = fpath + '.tmp'
            try:
                df_out.to_csv(_tmp, index=False)
                os.replace(_tmp, fpath)
                if os.path.getsize(fpath) == 0:
                    _log.warning("Atomic write to %s produced 0-byte file — possible disk error", fpath)
            finally:
                if os.path.exists(_tmp):
                    try:
                        os.remove(_tmp)
                    except OSError:
                        pass


def merge_executions(csv_executions: list, live_executions: list) -> list:
    """
    Merge CSV-loaded executions with live IBKR executions.
    Deduplicates on execId (live takes priority for matching IDs).
    """
    seen_ids = {}
    # Live executions first (most authoritative)
    for ex in live_executions:
        key = stable_exec_id(ex)
        seen_ids[key] = ex

    # CSV fills in historical data not in live
    for ex in csv_executions:
        key = stable_exec_id(ex)
        if key not in seen_ids:
            seen_ids[key] = ex

    return list(seen_ids.values())


def save_merged_snapshot(executions: list):
    """
    Persist the COMPLETE merged execution list to a single master snapshot file.
    Called on every refresh (live OR offline) so the trade log is never lost
    when the dashboard is restarted.  Deduplicates on stable_exec_id.
    """
    if not executions:
        return
    os.makedirs(TRADES_DIR, exist_ok=True)

    # Build a dict keyed on stable id so the file always reflects the union
    # of everything seen, with no duplicates.
    rows_by_id: dict = {}

    # Load whatever is already on disk first
    if os.path.exists(SNAPSHOT_FILE):
        try:
            df_existing = pd.read_csv(SNAPSHOT_FILE, dtype=str)
            for row_dict in df_existing.to_dict('records'):
                # Use stable_exec_id (produces SYNTHETIC_ for blank execIds) so the key
                # is consistent with what merge_executions and load functions produce.
                # The old 'EXISTING_N' pattern was non-deterministic and caused the same
                # blank-execId row to be stored under different keys on each save cycle,
                # silently duplicating trades in the snapshot.
                key = stable_exec_id(row_dict)
                rows_by_id[key] = row_dict
        except Exception as _e:
            _log.warning("Corrupt snapshot — will be overwritten cleanly: %s", _e)

    # Merge in the new executions — live/in-memory data ALWAYS overwrites on-disk.
    # This is critical: commissionReport() fires asynchronously after execDetails(),
    # so a trade saved with commission=0.0 must be corrected on the next refresh.
    # The original `continue` preserved the stale on-disk value indefinitely.
    for ex in executions:
        key = stable_exec_id(ex)

        raw_time = ex.get('time', ex.get('datetime', ''))
        dt = str(raw_time)[:19]  # fallback: keep raw string truncated to seconds
        for _fmt in TIME_FORMATS:
            try:
                dt = datetime.strptime(str(raw_time)[:26], _fmt).strftime('%Y-%m-%d %H:%M:%S')
                break
            except Exception:
                continue

        side = ex.get('side', '')
        if side == 'BOT':
            ibkr_side = 'BOT'
        elif side in ('SLD', 'SELL'):
            ibkr_side = 'SLD'
        else:
            ibkr_side = side

        rows_by_id[key] = {
            'execId':     key,
            'datetime':   dt,
            'symbol':     ex.get('symbol', ''),
            'secType':    ex.get('secType', 'STK'),
            'exchange':   ex.get('exchange', 'SMART'),
            'currency':   ex.get('currency', 'USD'),
            'side':       ibkr_side,
            'quantity':   ex.get('quantity', 0),
            'price':      ex.get('price', 0),
            'commission': ex.get('commission', 0),
            'orderRef':   ex.get('orderRef', UNTAGGED_ORDERREF),
        }

    if not rows_by_id:
        return

    df_out = pd.DataFrame(list(rows_by_id.values()), columns=CSV_SCHEMA)

    # Atomic write: stream to a .tmp file first, then rename into place.
    # pandas .to_csv() writes row-by-row. If the process is killed mid-write
    # (TWS crash, SIGKILL, power failure), a direct write leaves a partial CSV.
    # pd.read_csv on a partial file raises ParserError → the except in
    # load_snapshot() silently returns [] → all trade history is lost.
    # os.replace() is atomic on POSIX (rename syscall) and near-atomic on
    # Windows (replaces the destination in a single API call), so the reader
    # always sees either the complete old file or the complete new file.
    _tmp = SNAPSHOT_FILE + '.tmp'
    try:
        df_out.to_csv(_tmp, index=False)
        os.replace(_tmp, SNAPSHOT_FILE)
        if os.path.getsize(SNAPSHOT_FILE) == 0:
            _log.warning("Atomic write to snapshot produced 0-byte file — possible disk error")
    finally:
        # Clean up the temp file if the rename didn't happen (e.g. to_csv failed).
        if os.path.exists(_tmp):
            try:
                os.remove(_tmp)
            except OSError:
                pass


def load_snapshot() -> list:
    """
    Load the master snapshot file and return a list of execution dicts.
    This is the primary source on startup — it holds the full merged history.
    """
    if not os.path.exists(SNAPSHOT_FILE):
        return []
    try:
        df = pd.read_csv(SNAPSHOT_FILE, dtype=str)
        rows = []
        for row in df.to_dict('records'):
            rows.append({
                'time':        ' '.join(str(row.get('datetime', '') or '').replace('T', ' ').replace('Z', '').strip().split()[:2]),
                'symbol':      row.get('symbol', ''),
                'secType':     row.get('secType', 'STK'),
                'exchange':    row.get('exchange', 'SMART'),
                'currency':    row.get('currency', 'USD'),
                'localSymbol': row.get('symbol', ''),
                'side':        row.get('side', ''),
                'quantity':    to_float(row.get('quantity', 0)),
                'price':       to_float(row.get('price', 0)),
                'orderRef':    row.get('orderRef', UNTAGGED_ORDERREF),
                'execId':      row.get('execId', ''),
                'commission':  to_float(row.get('commission', 0)),
            })
        return rows
    except Exception:
        return []


# =============================================================================
# IBKR Connection (unchanged from v11.1, with guard)
# =============================================================================

if IBKR_AVAILABLE:
    class IBClient(EWrapper, EClient):
        def __init__(self):
            EClient.__init__(self, self)
            self.connected    = False
            self.executions   = []
            self.lock         = threading.Lock()
            self.market_prices = {}
            self.price_requests = {}
            self.next_req_id  = 1000
            self.price_errors = {}
            self.price_status = {}
            # Event set by execDetailsEnd so request_executions() can wait for
            # completion rather than relying on a fixed sleep(2).
            self.exec_details_done = threading.Event()
            # Generation counter — bumped by request_market_prices() so stale
            # ticks from prior reqMktData cycles are silently discarded in tickPrice().
            self.md_generation = 0
            # Event set by nextValidId so connect_to_ibkr() can wait for the
            # connection handshake without polling with time.sleep(0.5).
            self.connected_event = threading.Event()
            # Set to True by commissionReport() so the auto-refresh block can
            # force a snapshot save when new commission data arrives.
            self._commission_updated = False

        def nextValidId(self, orderId):
            self.connected = True
            self.connected_event.set()

        def connectionClosed(self):
            self.connected = False

        def error(self, reqId, *args):
            # Defensive *args signature that works with both ibapi versions:
            #   ibapi 9.x : (reqId, errorCode, errorString, advancedOrderRejectJson="")
            #   ibapi 10.x+: (reqId, errorTime, errorCode, errorString, advancedOrderRejectJson="")
            # With the old fixed 5-param signature (errorTime explicit), ibapi 9.x shifts
            # all args left so errorCode receives a timestamp string — integer comparisons
            # silently fail and disconnect events go undetected.
            # *args normalises both: check if arg[0] is int (9.x) or str (10.x+).
            if not args:
                return
            if isinstance(args[0], int):
                # ibapi 9.x: args = (errorCode, errorString, ...)
                errorCode, errorString = args[0], args[1] if len(args) > 1 else ""
            else:
                # ibapi 10.x+: args = (errorTime, errorCode, errorString, ...)
                errorCode, errorString = (args[1], args[2]) if len(args) > 2 else (0, str(args))
            ignore_codes = [2104, 2106, 2158, 2119, 2107, 2108, 10167]
            if errorCode == 1100:
                self.connected = False
            elif errorCode in (1101, 1102):
                self.connected = True
            if errorCode not in ignore_codes:
                print(f"Error {errorCode}: {errorString}")
                if reqId in self.price_requests:
                    # price_requests values are (symbol, gen) tuples since generation-tagging
                    # was added. Extract the symbol string before using it as a dict key so
                    # the debug panel (which looks up by plain str) can find the error message.
                    _val = self.price_requests[reqId]
                    _sym = _val[0] if isinstance(_val, (tuple, list)) else _val
                    self.price_errors[_sym] = f"Error {errorCode}: {errorString}"

        def execDetails(self, reqId, contract, execution):
            with self.lock:
                if any(e['execId'] == execution.execId for e in self.executions):
                    _log.debug("Skipped duplicate execId: %s", execution.execId)
                    return
                exec_data = {
                    "time":        execution.time,
                    "symbol":      contract.symbol,
                    "secType":     contract.secType,
                    "exchange":    contract.exchange,
                    "currency":    contract.currency,
                    "localSymbol": contract.localSymbol,
                    "side":        execution.side,
                    "quantity":    to_float(execution.shares),
                    "price":       to_float(execution.price),
                    "orderRef":    execution.orderRef or UNTAGGED_ORDERREF,
                    "execId":      execution.execId,
                    "commission":  0.0,
                }
                self.executions.append(exec_data)

        def execDetailsEnd(self, reqId):
            # IB fires this when the server has finished sending all execDetails
            # responses for a given reqExecutions() call.  Set the event so
            # request_executions() can stop waiting rather than relying on a
            # fixed sleep(2) that may expire before all callbacks arrive on
            # slow connections or large execution histories.
            self.exec_details_done.set()

        def commissionReport(self, commissionReport):
            with self.lock:
                for ex in self.executions:
                    if ex["execId"] == commissionReport.execId:
                        ex["commission"] = to_float(commissionReport.commission)
                        self._commission_updated = True
                        break

        def tickPrice(self, reqId, tickType, price, attrib):
            price = to_float(price)
            # Always take the lock BEFORE checking/using shared maps that can be
            # mutated by the Streamlit thread (request_market_prices) and read/written
            # by the IBKR network thread (this callback).
            with self.lock:
                if price <= 0:
                    return
                symbol_key = self.price_requests.get(reqId)
                if not symbol_key:
                    return
                # Support generation-tagged requests: (symbol, gen)
                if isinstance(symbol_key, (tuple, list)) and len(symbol_key) == 2:
                    symbol_key, gen = symbol_key
                    if gen != self.md_generation:
                        return

                # tickType mapping:
                #  4 / 68  = LAST (delayed / real-time)
                #  9 / 75  = CLOSE
                #  1 / 66  = BID
                if tickType in [4, 68]:
                    self.market_prices[symbol_key] = price
                    self.price_status[symbol_key] = f"Last: {price:.4f}"
                elif tickType in [9, 75] and symbol_key not in self.market_prices:
                    self.market_prices[symbol_key] = price
                    self.price_status[symbol_key] = f"Close: {price:.4f}"
                elif tickType in [1, 66] and symbol_key not in self.market_prices:
                    self.market_prices[symbol_key] = price
                    self.price_status[symbol_key] = f"Bid: {price:.4f}"



# =============================================================================
# SESSION STATE
# =============================================================================

def init_session_state():
    defaults = {
        "ib_client":        None,
        "connected":        False,
        "ib_thread":       None,
        "last_refresh":     None,
        "capital":          100_000.0,
        "csv_executions":   [],           # Loaded from CSV on startup
        "live_executions":  [],           # From IBKR live
        "merged_executions": [],          # Combined
        "csv_loaded":       False,
        "auto_refresh":     False,
        "auto_refresh_secs": 60,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# =============================================================================
# IBKR HELPERS
# =============================================================================

def safe_ib_ready(client) -> bool:
    if not IBKR_AVAILABLE or client is None:
        return False
    try:
        if hasattr(client, "isConnected") and not client.isConnected():
            return False
        sv = client.serverVersion()
        return sv is not None
    except Exception:
        return False


def connect_to_ibkr(host, port, client_id):
    """Connect to IBKR and start the network thread.

    Returns:
        (client, thread) on success, (None, None) on failure.
    """
    if not IBKR_AVAILABLE:
        return None, None

    client = IBClient()
    client.connect(host, port, client_id)

    thread = threading.Thread(target=client.run, daemon=True, name=f"IBClient-{client_id}")
    thread.start()

    # Wait for nextValidId (connection handshake) without polling.
    # connected_event is set by nextValidId the instant the handshake completes,
    # so this returns immediately rather than sleeping in 500 ms increments.
    client.connected_event.wait(timeout=5.0)

    if client.connected:
        return client, thread

    # Best-effort cleanup on failed connect
    try:
        client.disconnect()
    except Exception as _e:
        _log.debug("disconnect() during failed connect cleanup: %s", _e)
    return None, None


def disconnect_from_ibkr(client, thread, join_timeout: float = 2.0):
    """Best-effort graceful disconnect to prevent orphaned IB threads."""
    if client is None:
        return
    try:
        client.disconnect()
    except Exception as _e:
        _log.debug("disconnect() raised: %s", _e)
    if thread is not None and getattr(thread, "is_alive", lambda: False)():
        try:
            thread.join(timeout=join_timeout)
        except Exception as _e:
            _log.debug("thread.join() raised: %s", _e)
        if thread.is_alive():
            _log.warning(
                "IBClient network thread did not stop after %.1fs — orphaned thread may exist",
                join_timeout,
            )


def request_executions(client, days_back: int = 30) -> bool:
    """
    Request executions for the past N days (not just today).
    This ensures multi-day strategy history is captured.

    Uses execDetailsEnd (via exec_details_done Event) to know when IB has
    finished sending all execDetails callbacks, rather than a fixed sleep(2)
    that can expire before all callbacks arrive on slow connections or large
    execution histories.  A 10-second wall-clock fallback guards against
    servers that do not fire execDetailsEnd at all.
    """
    if not safe_ib_ready(client):
        return False
    exec_filter = ExecutionFilter()
    from_date = (datetime.now() - timedelta(days=days_back))
    exec_filter.time = from_date.strftime("%Y%m%d-00:00:00")

    with client.lock:
        client.executions = []
        # Reset the completion event before issuing the request so a stale
        # set() from a previous call does not cause an immediate return.
        client.exec_details_done.clear()
        # Use a monotonically increasing reqId so execution requests never
        # collide with in-flight market-data requests.
        req_id = client.next_req_id
        client.next_req_id += 1
    try:
        client.reqExecutions(req_id, exec_filter)
        # Wait for execDetailsEnd (set by the callback) with a 10-second cap.
        # Returns quickly once the server sends the terminator; the cap handles
        # edge cases where older TWS builds omit execDetailsEnd.
        client.exec_details_done.wait(timeout=10)
        return True
    except Exception:
        return False


def _build_symbols_info(executions: list) -> dict:
    """Build a {symbol: {secType, currency}} dict from an execution list.

    Used before requesting market prices so IBKR receives the correct
    contract details for each symbol.
    """
    result = {}
    for ex in executions:
        sym = ex.get('symbol')
        if sym and sym not in result:
            result[sym] = {
                'secType':  ex.get('secType',  'STK'),
                'currency': ex.get('currency', 'USD'),
            }
    return result


def request_market_prices(client, symbols_info) -> bool:
    if not safe_ib_ready(client):
        return False

    # Reset maps under lock to avoid orphaned dict objects being updated by callbacks.
    with client.lock:
        client.market_prices  = {}
        client.price_requests = {}
        client.price_errors   = {}
        client.price_status   = {}
        # Bump generation so late ticks from prior requests are ignored
        client.md_generation = getattr(client, 'md_generation', 0) + 1
        current_gen = client.md_generation

    try:
        # 3 = delayed-frozen (safer for accounts without real-time data subscriptions)
        client.reqMarketDataType(3)
    except Exception as _e:
        _log.debug("reqMarketDataType(3) raised (non-fatal): %s", _e)

    # Issue requests
    for symbol, info in symbols_info.items():
        if not safe_ib_ready(client):
            return False

        contract = Contract()
        sec_type = info.get("secType", "STK")
        currency = info.get("currency", "USD")

        if sec_type == "CASH":
            contract.symbol   = symbol
            contract.secType  = "CASH"
            contract.currency = currency
            contract.exchange = "IDEALPRO"
        else:
            contract.symbol   = symbol
            contract.secType  = sec_type
            contract.currency = currency
            contract.exchange = "SMART"

        with client.lock:
            req_id = client.next_req_id
            client.next_req_id += 1
            client.price_requests[req_id] = (symbol, current_gen)
            client.price_status[symbol]   = "Requesting..."

        try:
            client.reqMktData(req_id, contract, "", False, False, [])
        except Exception as e:
            with client.lock:
                client.price_errors[symbol] = str(e)

    # Allow a short window for ticks to arrive
    time.sleep(MARKET_DATA_WAIT_SECS)

    # Cancel all requests safely
    with client.lock:
        req_ids = [rid for rid, val in client.price_requests.items()
                  if not (isinstance(val, (tuple, list)) and len(val) == 2) or val[1] == current_gen]

    for req_id in req_ids:
        if not safe_ib_ready(client):
            break
        try:
            client.cancelMktData(req_id)
        except Exception as _e:
            _log.debug("cancelMktData(%s) raised: %s", req_id, _e)

    return True


# =============================================================================
# DATA PROCESSING
# =============================================================================

def process_executions(executions: list) -> pd.DataFrame:
    if not executions:
        return pd.DataFrame()
    df = pd.DataFrame(executions)

    # Handle both ISO and IBKR time formats.
    # IBKR may send times as '20260304 09:31:04 US/Eastern' (space-separated with tz suffix).
    # load_all_csv_trades strips the tz suffix via .split()[:2], producing '20260304 09:31:04'.
    # parse_time must handle both dash ('20260304-09:31:04') and space ('20260304 09:31:04').
    def parse_time(t):
        # Strip trailing timezone suffix (e.g. 'US/Eastern', 'UTC') before parsing
        s = str(t).strip()
        parts = s.split()
        if len(parts) >= 3:  # '20260304 09:31:04 US/Eastern' -> '20260304 09:31:04'
            s = ' '.join(parts[:2])
        for fmt in TIME_FORMATS:
            try:
                return datetime.strptime(s[:26], fmt)
            except Exception:
                continue
        return pd.NaT

    df['datetime'] = df['time'].apply(parse_time)

    # Drop rows whose timestamp could not be parsed.
    # Rationale: NaT rows sort to the END of the DataFrame regardless of their true
    # trade time. A NaT-dated BUY would be processed AFTER its corresponding SELL,
    # causing the engine to open a spurious short first — producing completely wrong
    # PnL sign and magnitude for the entire symbol's trade history.
    nat_mask = df['datetime'].isna()
    if nat_mask.any():
        bad_ids = df.loc[nat_mask, 'execId'].tolist() if 'execId' in df.columns else []
        st.warning(
            f"⚠️ Dropped {nat_mask.sum()} execution(s) with unparseable timestamps "
            f"(execIds: {bad_ids}). Check source CSV for malformed datetime values."
        )
        df = df[~nat_mask].copy()

    df = df.sort_values('datetime').reset_index(drop=True)
    df['quantity']   = df['quantity'].apply(to_float)
    df['price']      = df['price'].apply(to_float)
    df['commission'] = df['commission'].apply(to_float)

    # Normalise side: dashboard expects BOT / SLD
    df['side'] = df['side'].str.upper().map(
        lambda s: 'BOT' if s in ('BOT', 'BUY') else ('SLD' if s in ('SLD', 'SELL') else s)
    )

    return df


def calculate_positions_and_pnl(df: pd.DataFrame, strategy_ref: str, market_prices: dict):
    strategy_df = df[df['orderRef'] == strategy_ref].copy()
    if strategy_df.empty:
        return pd.DataFrame(), {}, []

    pnl_records   = []
    open_positions = {}
    closed_trades  = []

    for symbol in strategy_df['symbol'].unique():
        sym_df = strategy_df[strategy_df['symbol'] == symbol].sort_values('datetime').reset_index(drop=True)

        position   = 0.0
        avg_cost   = 0.0
        total_cost = 0.0
        sec_type   = sym_df.iloc[0].get('secType', 'STK')
        currency   = sym_df.iloc[0].get('currency', 'USD')

        for row in sym_df.to_dict('records'):
            qty        = to_float(row['quantity'])
            price      = to_float(row['price'])
            commission = to_float(row['commission'])

            if row['side'] == 'BOT':
                if position < 0:
                    close_qty    = min(qty, abs(position))
                    # Split commission proportionally: only the fraction of shares
                    # that close the existing short position bears realized commission.
                    # The remainder (if any) opens a new long and carries its own
                    # cost basis — charging it the full commission overstates realized
                    # PnL loss and understates the new position's true cost.
                    close_comm   = commission * (close_qty / qty) if qty > 0 else commission
                    realized_pnl = close_qty * (avg_cost - price) - close_comm
                    closed_trades.append({'datetime': row['datetime'], 'symbol': symbol, 'pnl': realized_pnl})
                    position    += close_qty
                    remaining    = qty - close_qty
                    if position == 0:
                        total_cost = avg_cost = 0.0
                    else:
                        total_cost = abs(position) * avg_cost
                    if remaining > 0:
                        # Crossed zero: excess qty opens a new long position.
                        # Carry the remaining commission fraction into cost basis.
                        open_comm  = commission - close_comm
                        total_cost = remaining * price + open_comm
                        position   = remaining
                        avg_cost   = total_cost / position
                else:
                    total_cost += qty * price
                    position   += qty
                    avg_cost    = total_cost / position if position > 0 else 0.0
                    realized_pnl = -commission
                pnl_records.append({'datetime': row['datetime'], 'symbol': symbol,
                                    'action': 'BUY', 'quantity': qty, 'price': price,
                                    'commission': commission, 'realized_pnl': realized_pnl,
                                    'position': position})
            else:  # SLD / SELL
                if position > 0:
                    close_qty    = min(qty, position)
                    # Split commission proportionally: only the closing fraction
                    # bears realized commission; the remainder goes to the new short.
                    close_comm   = commission * (close_qty / qty) if qty > 0 else commission
                    realized_pnl = close_qty * (price - avg_cost) - close_comm
                    closed_trades.append({'datetime': row['datetime'], 'symbol': symbol, 'pnl': realized_pnl})
                    position  -= close_qty
                    remaining  = qty - close_qty
                    if position == 0:
                        total_cost = avg_cost = 0.0
                    else:
                        total_cost = position * avg_cost
                    if remaining > 0:
                        # Crossed zero: excess qty opens a new short position.
                        # Carry the remaining commission fraction into cost basis.
                        open_comm  = commission - close_comm
                        total_cost = remaining * price + open_comm
                        position   = -remaining
                        avg_cost   = total_cost / remaining
                else:
                    realized_pnl = -commission
                    position    -= qty
                    total_cost  += qty * price
                    avg_cost     = abs(total_cost / position) if position != 0 else 0.0
                pnl_records.append({'datetime': row['datetime'], 'symbol': symbol,
                                    'action': 'SELL', 'quantity': qty, 'price': price,
                                    'commission': commission, 'realized_pnl': realized_pnl,
                                    'position': position})

        if position != 0:
            current_price = market_prices.get(symbol, 0)
            open_positions[symbol] = {
                'position':     position,
                'avg_cost':     avg_cost,
                'secType':      sec_type,
                'currency':     currency,
                'current_price': to_float(current_price) if current_price else 0.0,
            }

    if not pnl_records:
        return pd.DataFrame(), {}, []

    pnl_df = pd.DataFrame(pnl_records).sort_values('datetime').reset_index(drop=True)
    pnl_df['cumulative_pnl'] = pnl_df['realized_pnl'].cumsum()
    pnl_df['trade_number']   = range(1, len(pnl_df) + 1)
    pnl_df['date']           = pnl_df['datetime'].dt.date
    return pnl_df, open_positions, closed_trades


def calculate_unrealized_pnl(open_positions: dict):
    total_unrealized = 0.0
    position_details = []
    for symbol, info in open_positions.items():
        position      = to_float(info['position'])
        avg_cost      = to_float(info['avg_cost'])
        current_price = to_float(info['current_price'])
        sec_type      = info.get('secType', 'STK')
        if current_price > 0:
            unrealized = position * (current_price - avg_cost) if position > 0 \
                         else abs(position) * (avg_cost - current_price)
            total_unrealized += unrealized
            position_details.append({'symbol': symbol, 'position': position, 'avg_cost': avg_cost,
                                     'current_price': current_price, 'unrealized_pnl': unrealized,
                                     'secType': sec_type})
        else:
            position_details.append({'symbol': symbol, 'position': position, 'avg_cost': avg_cost,
                                     'current_price': None, 'unrealized_pnl': None, 'secType': sec_type})
    return total_unrealized, position_details


def calculate_consecutive_loss_stats(closed_trades: list) -> tuple:
    """
    Return (current_streak, max_streak) of consecutive losing trades.
    current_streak: number of losses at the tail of the sorted trade list.
    max_streak:     longest losing run across the full history.
    """
    if not closed_trades:
        return 0, 0
    sorted_trades = sorted(closed_trades, key=lambda x: x['datetime'])
    consecutive = max_consecutive = 0
    for t in sorted_trades:
        if t['pnl'] < 0:
            consecutive += 1
            max_consecutive = max(max_consecutive, consecutive)
        else:
            consecutive = 0
    # consecutive now holds the current (trailing) losing streak
    return consecutive, max_consecutive


def calculate_strategy_metrics(df: pd.DataFrame, capital: float, market_prices: dict) -> dict:
    if df.empty:
        return {}
    capital    = to_float(capital)
    strategies = {}

    for ref in df['orderRef'].unique():
        pnl_df, open_positions, closed_trades = calculate_positions_and_pnl(df, ref, market_prices)
        if pnl_df.empty:
            continue

        trades         = len(pnl_df)
        buys           = len(pnl_df[pnl_df['action'] == 'BUY'])
        sells          = len(pnl_df[pnl_df['action'] == 'SELL'])
        total_commission = to_float(pnl_df['commission'].sum())
        realized_pnl   = to_float(pnl_df['realized_pnl'].sum())
        unrealized_pnl, position_details = calculate_unrealized_pnl(open_positions)
        total_pnl      = realized_pnl + unrealized_pnl
        symbols        = pnl_df['symbol'].unique().tolist()

        winning_trades = sum(1 for t in closed_trades if t['pnl'] > 0)
        losing_trades  = sum(1 for t in closed_trades if t['pnl'] < 0)
        total_closed   = winning_trades + losing_trades
        win_rate       = (winning_trades / total_closed * 100) if total_closed > 0 else 0
        roi_pct        = (total_pnl / capital) * 100 if capital > 0 else 0

        if len(pnl_df) > 1:
            start_time = pnl_df['datetime'].min()
            end_time   = pnl_df['datetime'].max()
            days       = (end_time - start_time).total_seconds() / 86400
            years      = days / 365 if days > 0 else 1 / 365
        else:
            days, years = 1, 1 / 365

        ending_value  = capital + total_pnl
        # CAGR is only meaningful with sufficient history. With < MIN_CAGR_DAYS,
        # the exponent (1/years) exceeds ~12 and a single good day produces
        # thousands of percent CAGR — a 1% daily gain annualises to 3,678% after
        # one day. Suppress to None for short histories so the UI shows "N/A".
        if capital > 0 and ending_value > 0 and days >= MIN_CAGR_DAYS:
            cagr = ((ending_value / capital) ** (1 / years) - 1) * 100
        elif ending_value <= 0:
            cagr = -100.0
        else:
            cagr = None  # insufficient history

        cumulative   = pnl_df['cumulative_pnl'].values.astype(float)
        # Extend the equity curve with the current mark-to-market point so that
        # an open position sitting on an unrealized loss is reflected in Max DD.
        # When unrealized_pnl == 0 (no market prices available) the curve is
        # left unchanged — appending zero would be a no-op on min() anyway.
        if unrealized_pnl != 0:
            cumulative = np.append(cumulative, cumulative[-1] + unrealized_pnl)
        running_max  = pd.Series(cumulative).cummax()
        drawdown     = cumulative - running_max
        max_drawdown = float(drawdown.min())
        max_drawdown_pct = (max_drawdown / capital) * 100 if capital > 0 else 0

        # Sharpe is computed over closed-trade returns only. Open-side executions
        # record realized_pnl = -commission (no position was closed), which is not
        # a trade outcome. Using closed_trades eliminates that bias.
        n_closed = len(closed_trades)
        if n_closed > 1 and capital > 0:
            ct_pnls     = np.array([t['pnl'] for t in closed_trades], dtype=float)
            returns     = ct_pnls / capital
            mean_return = float(returns.mean())
            std_return  = float(returns.std())
            if std_return > 0.0001:
                tpd        = n_closed / max(days, 1)
                # NOTE: max(tpd, 1) clips annualisation to a minimum of sqrt(252).
                # For low-frequency strategies (<1 trade/day) this over-inflates the
                # Sharpe — e.g. 12 trades/year is inflated ~4.6x vs the correct sqrt(12).
                # Intentional design choice to avoid near-zero ann_factor noise; treat
                # Sharpe as indicative only for strategies with < ~50 trades per year.
                ann_factor = np.sqrt(252 * max(tpd, 1))
                sharpe     = (mean_return / std_return) * ann_factor
            else:
                sharpe = 0.0
        else:
            sharpe = 0.0

        consec_losses_current, consec_losses_max = calculate_consecutive_loss_stats(closed_trades)
        best_trade    = max((t['pnl'] for t in closed_trades), default=0)
        worst_trade   = min((t['pnl'] for t in closed_trades), default=0)
        avg_trade     = sum(t['pnl'] for t in closed_trades) / len(closed_trades) if closed_trades else 0

        daily_pnl = pnl_df.groupby('date').agg(
            realized_pnl=('realized_pnl', 'sum'),
            cumulative_pnl=('cumulative_pnl', 'last')
        ).reset_index()

        strategies[ref] = {
            'trades':           trades,
            'closed_trades':    len(closed_trades),
            'buys':             buys,
            'sells':            sells,
            'commission':       total_commission,
            'realized_pnl':     realized_pnl,
            'unrealized_pnl':   unrealized_pnl,
            'total_pnl':        total_pnl,
            'roi_pct':          roi_pct,
            'cagr_pct':         cagr,
            'win_rate':         win_rate,
            'winning_trades':   winning_trades,
            'losing_trades':    losing_trades,
            'max_drawdown':     max_drawdown,
            'max_drawdown_pct': max_drawdown_pct,
            'sharpe_ratio':     sharpe,
            'consecutive_losses':     consec_losses_current,
            'max_consecutive_losses': consec_losses_max,
            'avg_trade':        avg_trade,
            'best_trade':       best_trade,
            'worst_trade':      worst_trade,
            'symbols':          symbols,
            'pnl_df':           pnl_df,
            'daily_pnl':        daily_pnl,
            'open_positions':   open_positions,
            'position_details': position_details,
        }

    return strategies


# =============================================================================
# STREAMLIT UI
# =============================================================================

def main():
    st.set_page_config(page_title="Strategy Monitor", page_icon="📊", layout="wide")
    init_session_state()

    # Initialise button-state flags before any conditional branch so that the
    # auto-refresh block at the end of main() never hits an UnboundLocalError.
    refresh_clicked: bool = False
    reload_csv:      bool = False

    st.title("📊 AQS Strategy Monitor")

    # ── Load persisted trades on first run ──────────────────────────────────
    if not st.session_state.csv_loaded:
        # Primary source: master snapshot (written by this dashboard on every refresh)
        snapshot_trades = load_snapshot()
        # Secondary source: individual per-symbol CSVs written by the portfolio strategy
        symbol_trades   = load_all_csv_trades()
        # Merge both, snapshot first (it may already contain everything)
        csv_trades = merge_executions(symbol_trades, snapshot_trades)
        st.session_state.csv_executions  = csv_trades
        st.session_state.csv_loaded      = True
        if csv_trades:
            st.session_state.merged_executions = csv_trades
            snapshot_src = "snapshot" if snapshot_trades else "per-symbol CSVs"
            st.success(f"📂 Loaded {len(csv_trades)} historical trades from {snapshot_src}.")

    # ── Sidebar ──────────────────────────────────────────────────────────────
    st.sidebar.header("⚙️ Connection")
    host      = st.sidebar.text_input("Host", value="127.0.0.1")
    port      = st.sidebar.number_input("Port", value=4002, min_value=1, max_value=65535)
    _default_cid = _resolve_monitor_client_id()
    client_id = st.sidebar.number_input(
        "Client ID",
        value=_default_cid,
        min_value=1,
        max_value=999,
        help="Auto-allocated by client_id_manager (monitor range 100–199, preferred: 101).",
    )
    days_back = st.sidebar.number_input("Fetch executions (days back)", value=30, min_value=1, max_value=365)

    st.sidebar.divider()
    st.sidebar.header("💰 Settings")
    capital = st.sidebar.number_input("Starting Capital ($)", value=100_000.0, min_value=1000.0, step=10_000.0)
    st.session_state.capital = capital

    st.sidebar.divider()
    st.sidebar.header("🔄 Auto Refresh")
    auto_refresh = st.sidebar.toggle("Enable Auto-Refresh", value=st.session_state.auto_refresh)
    st.session_state.auto_refresh = auto_refresh
    if auto_refresh:
        refresh_secs = st.sidebar.slider("Interval (seconds)", 10, 300,
                                          st.session_state.auto_refresh_secs, 10)
        st.session_state.auto_refresh_secs = refresh_secs
        st.sidebar.caption(f"Next refresh in ~{refresh_secs}s")

    st.sidebar.divider()

    # ── IBKR Connection ──────────────────────────────────────────────────────
    if not IBKR_AVAILABLE:
        st.sidebar.warning("⚠️ ibapi not installed — offline mode only")
    else:
        if not st.session_state.connected:
            # Guard: only show Connect button when not already connected.
            # Without this guard, each click spawns a new IBClient + daemon
            # thread, leaving the old client running as a "ghost" — its
            # callbacks (execDetails, tickPrice) keep firing into an orphaned
            # object, producing duplicate executions and hard-to-debug state.
            if st.sidebar.button("🔌 Connect to IBKR"):
                with st.spinner("Connecting..."):
                    client, thread = connect_to_ibkr(host, port, client_id)
                    if client:
                        st.session_state.ib_client = client
                        st.session_state.ib_thread = thread
                        st.session_state.connected  = True
                        st.sidebar.success("✅ Connected!")
                    else:
                        st.sidebar.error("❌ Connection failed — running in offline mode")
        else:
            # Already connected: show status + disconnect only.
            st.sidebar.success("✅ Connected to IBKR")

        if st.session_state.connected and (
            st.session_state.ib_client is None or
            not safe_ib_ready(st.session_state.ib_client)
        ):
            _log.warning("IBKR connection lost — attempting auto-reconnect")
            with st.spinner("Reconnecting to IBKR..."):
                _rc, _rt = connect_to_ibkr(host, port, int(client_id))
            if _rc:
                st.session_state.ib_client = _rc
                st.session_state.ib_thread = _rt
                st.session_state.connected = True
                st.sidebar.success("✅ Reconnected!")
            else:
                st.session_state.connected  = False
                st.session_state.ib_client  = None
                st.session_state.ib_thread  = None
                st.sidebar.warning("⚠️ Auto-reconnect failed — running in offline mode")

        if st.session_state.connected:
            if st.sidebar.button("🔴 Disconnect"):
                disconnect_from_ibkr(st.session_state.ib_client, st.session_state.ib_thread)
                st.session_state.connected  = False
                st.session_state.ib_client  = None
                st.session_state.ib_thread  = None
                st.rerun()

    status = "🟢 Connected" if st.session_state.connected else "🔴 Offline"
    st.sidebar.markdown(f"**Status:** {status}")

    # ── Offline mode banner ──────────────────────────────────────────────────
    if not st.session_state.connected:
        if st.session_state.csv_executions:
            st.info("📂 **Offline Mode** — Displaying historical data from CSV. "
                    "Connect to IBKR for live updates.")
        else:
            st.warning("📂 No CSV data found and no IBKR connection. "
                       "Ensure trades/ folder exists or connect to IBKR.")

    # ── Refresh Data ─────────────────────────────────────────────────────────
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        refresh_clicked = st.button("🔄 Refresh Data")
    with col2:
        reload_csv = st.button("📂 Reload CSV")
    with col3:
        if st.session_state.last_refresh:
            st.caption(f"Last data fetch: {st.session_state.last_refresh.strftime('%Y-%m-%d %H:%M:%S')}")

    if reload_csv:
        snapshot_trades = load_snapshot()
        symbol_trades   = load_all_csv_trades()
        csv_trades      = merge_executions(symbol_trades, snapshot_trades)
        st.session_state.csv_executions    = csv_trades
        # Reset live_executions so stale live data doesn't contaminate the CSV reload.
        st.session_state.live_executions   = []
        merged = merge_executions(csv_trades, [])
        st.session_state.merged_executions = merged
        # Persist the freshly merged state
        if merged:
            save_merged_snapshot(merged)
        st.success(f"Reloaded {len(csv_trades)} trades from CSV / snapshot.")

    if refresh_clicked:
        if st.session_state.connected:
            client = st.session_state.ib_client
            if not safe_ib_ready(client):
                st.warning("IBKR disconnected — showing cached data.")
            else:
                with st.spinner("Fetching live executions and prices..."):
                    ok_exec = request_executions(client, days_back=int(days_back))
                    # Snapshot executions under the lock to avoid a data race with
                    # the IBKR callback thread, which may append to client.executions
                    # concurrently via execDetails() / commissionReport().
                    with client.lock:
                        live_execs_snapshot = list(client.executions)

                    if ok_exec and live_execs_snapshot:
                        st.session_state.live_executions = live_execs_snapshot
                        st.session_state.last_refresh = datetime.now()
                        # Save individual per-symbol CSVs (IBKR live path)
                        save_executions_to_csv(live_execs_snapshot)
                        # Merge live with full CSV history
                        st.session_state.merged_executions = merge_executions(
                            st.session_state.csv_executions,
                            live_execs_snapshot,
                        )
                        # Request market prices for symbols that exist in the MERGED history (not just the
                        # latest executions). This prevents "Current Price = N/A" when an open position
                        # exists but reqExecutions returns empty for the current lookback.
                        merged_execs = st.session_state.merged_executions or []
                        symbols_info = _build_symbols_info(merged_execs)
                        if symbols_info:
                            request_market_prices(client, symbols_info)
                    else:
                        # Even if executions are empty/failed for this lookback, we may still have
                        # open positions from prior trades. Try requesting prices from the current cache.
                        merged_execs = st.session_state.merged_executions or st.session_state.csv_executions or []
                        symbols_info = _build_symbols_info(merged_execs)
                        if symbols_info:
                            request_market_prices(client, symbols_info)

                        if not ok_exec:
                            st.warning("Execution fetch failed — showing cached data.")
                        elif ok_exec and not live_execs_snapshot:
                            st.info("No executions returned for the current lookback — prices refreshed from cached history.")

        # ── ALWAYS persist the current merged state to the master snapshot ──
        # This runs whether IBKR is connected or not, ensuring the trade log
        # is never lost when the dashboard is restarted.
        merged = st.session_state.merged_executions
        if merged:
            save_merged_snapshot(merged)

    # ── Get working dataset ──────────────────────────────────────────────────
    executions = st.session_state.merged_executions
    if not executions:
        st.info("No trade data available. Click '🔄 Refresh Data' or ensure CSV trades/ folder is populated.")
        # Auto-refresh must still fire here: if the user enables auto-refresh
        # while the trades folder is empty (waiting for the first execution),
        # the rerun() below is the only way the dashboard ever polls again.
        # Without this, the auto-refresh block at the bottom of main() is
        # unreachable and the toggle becomes a no-op with no data present.
        # st_autorefresh is mandatory (see module import). Call it inline
        # here so the no-data state still polls on schedule without blocking
        # the Python thread with time.sleep(). The explicit st.rerun() after
        # a button press was redundant — when refresh_clicked=True, the fetch
        # already ran in this script pass; if merged_executions is still empty,
        # a second immediate rerun shows the same empty state.
        if auto_refresh and not refresh_clicked and not reload_csv:
            if _HAS_ST_AUTOR and st_autorefresh is not None:
                secs = max(5, int(st.session_state.auto_refresh_secs))
                st_autorefresh(interval=secs * 1000, key="aqs_autorefresh")
        return

    market_prices = {}
    if st.session_state.connected and st.session_state.ib_client:
        # Copy under the lock: tickPrice() runs on the IBKR network thread and
        # can mutate the dict while calculate_strategy_metrics iterates it,
        # causing RuntimeError: dictionary changed size during iteration.
        with st.session_state.ib_client.lock:
            market_prices = dict(st.session_state.ib_client.market_prices)

    df = process_executions(executions)
    strategies = calculate_strategy_metrics(df, capital, market_prices)

    if not strategies:
        st.warning("Could not parse trade data into strategies. Check orderRef tagging.")
        return

    st.divider()

    # ── Overall Summary ──────────────────────────────────────────────────────
    st.subheader("📈 Overall Summary")

    total_trades      = sum(s['trades']        for s in strategies.values())
    total_closed      = sum(s['closed_trades'] for s in strategies.values())
    total_realized    = sum(s['realized_pnl']  for s in strategies.values())
    total_unrealized  = sum(s['unrealized_pnl'] for s in strategies.values())
    total_pnl         = total_realized + total_unrealized
    total_commission  = sum(s['commission']    for s in strategies.values())
    total_roi         = (total_pnl / capital) * 100 if capital else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Trades",     f"{total_trades:,}")
    c2.metric("Closed Trades",    f"{total_closed:,}")
    c3.metric("Realized P&L",     f"${total_realized:,.2f}")
    c4.metric("Unrealized P&L",   f"${total_unrealized:,.2f}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total P&L",        f"${total_pnl:,.2f}")
    c2.metric("ROI %",            f"{total_roi:.2f}%")
    c3.metric("Commission",       f"${total_commission:,.2f}")
    c4.metric("Capital",          f"${capital:,.0f}")

    st.divider()

    # ── Strategy Summary Table ────────────────────────────────────────────────
    st.subheader("📋 Strategy Summary")
    table_data = []
    for ref, data in sorted(strategies.items()):
        _cagr_val = data['cagr_pct']
        table_data.append({
            "Strategy":   ref,
            "Trades":     data['trades'],
            "Closed":     data['closed_trades'],
            "Realized":   f"${data['realized_pnl']:,.2f}",
            "Unrealized": f"${data['unrealized_pnl']:,.2f}",
            "Total P&L":  f"${data['total_pnl']:,.2f}",
            "ROI %":      f"{data['roi_pct']:.2f}%",
            "CAGR %":     f"{_cagr_val:.2f}%" if _cagr_val is not None else "N/A",
            "Max DD %":   f"{data['max_drawdown_pct']:.2f}%",
            "Sharpe":     f"{data['sharpe_ratio']:.2f}",
            "Win Rate":   f"{data['win_rate']:.1f}%",
            "Consec Loss (Cur/Max)": f"{data['consecutive_losses']} / {data['max_consecutive_losses']}",
            "Symbols":    ", ".join(data['symbols']),
        })
    st.dataframe(table_data, width='stretch', hide_index=True)

    st.divider()

    # ── P&L Curves ───────────────────────────────────────────────────────────
    st.subheader("📉 Cumulative P&L Curves")
    chart_data = pd.DataFrame()
    for ref, data in strategies.items():
        daily = data['daily_pnl']
        if not daily.empty:
            daily = daily.copy()
            daily['date'] = pd.to_datetime(daily['date'])
            series = daily.set_index('date')['cumulative_pnl'].astype(float)
            series.name = ref
            chart_data = series.to_frame() if chart_data.empty else chart_data.join(series, how='outer')

    if not chart_data.empty and len(chart_data) > 1:
        chart_data = chart_data.ffill().fillna(0)   # FIXED: was fillna(method='ffill')
        st.line_chart(chart_data, width='stretch')
        st.caption("X-axis: Date | Y-axis: Cumulative Realized P&L ($)")
    elif not chart_data.empty:
        st.bar_chart(chart_data)
    else:
        st.info("No P&L data available for charting.")

    st.divider()

    # ── Individual Strategy Details ───────────────────────────────────────────
    st.subheader("🔍 Strategy Details")
    selected = st.selectbox("Select Strategy", options=list(strategies.keys()), index=0)

    if selected:
        data = strategies[selected]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Realized P&L",   f"${data['realized_pnl']:,.2f}")
        c2.metric("Unrealized P&L", f"${data['unrealized_pnl']:,.2f}")
        c3.metric("Total P&L",      f"${data['total_pnl']:,.2f}")
        c4.metric("ROI %",          f"{data['roi_pct']:.2f}%")

        c1, c2, c3, c4 = st.columns(4)
        _cagr = data['cagr_pct']
        c1.metric("CAGR %", f"{_cagr:.2f}%" if _cagr is not None else "N/A",
                  help=f"Requires ≥{MIN_CAGR_DAYS} days of history")
        c2.metric("Total Executions", data['trades'])
        c3.metric("Closed Trades",    data['closed_trades'])
        c4.metric("Sharpe Ratio",     f"{data['sharpe_ratio']:.2f}")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Win Rate",          f"{data['win_rate']:.1f}%")
        c2.metric("Max DD %",          f"{data['max_drawdown_pct']:.2f}%")
        c3.metric("Cur Consec. Loss",  data['consecutive_losses'])
        c4.metric("Max Consec. Loss",  data['max_consecutive_losses'])

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Best Trade",        f"${data['best_trade']:,.2f}" if data['closed_trades'] > 0 else "N/A")
        c2.metric("Worst Trade",       f"${data['worst_trade']:,.2f}" if data['closed_trades'] > 0 else "N/A")

        st.markdown(f"**Symbols:** {', '.join(data['symbols'])}")

        if data['open_positions']:
            st.markdown("---")
            st.markdown("**📊 Open Positions**")
            pos_data = []
            for detail in data['position_details']:
                pos_data.append({
                    "Symbol":        detail['symbol'],
                    "Type":          detail.get('secType', 'STK'),
                    "Position":      f"{detail['position']:,.0f}",
                    "Avg Cost":      f"${detail['avg_cost']:,.4f}",
                    "Current Price": f"${detail['current_price']:,.4f}" if detail['current_price'] else "N/A",
                    "Unrealized P&L": f"${detail['unrealized_pnl']:,.2f}" if detail['unrealized_pnl'] is not None else "N/A",
                })
            st.dataframe(pos_data, width='stretch', hide_index=True)

        # P&L Curve for selected strategy
        st.markdown(f"**{selected} — P&L Curve**")
        daily = data['daily_pnl']
        if not daily.empty and len(daily) > 1:
            st.line_chart(daily.set_index('date')[['cumulative_pnl']].astype(float),
                          width='stretch')
        elif not daily.empty:
            pnl_df = data['pnl_df']
            if len(pnl_df) > 1:
                st.line_chart(pd.DataFrame(
                    {'Cumulative P&L': pnl_df['cumulative_pnl'].astype(float).values},
                    index=range(1, len(pnl_df) + 1)
                ), width='stretch')
                st.caption("X-axis: Trade # | Y-axis: Cumulative P&L ($)")
        else:
            st.info("No P&L data for this strategy.")

        # Trade history + CSV export
        with st.expander(f"📋 {selected} — Trade History"):
            pnl_df = data['pnl_df']
            if not pnl_df.empty:
                display_df = pnl_df[['trade_number', 'datetime', 'symbol', 'action',
                                      'quantity', 'price', 'commission',
                                      'realized_pnl', 'cumulative_pnl']].copy()
                display_df['datetime']      = display_df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
                display_df['realized_pnl']  = display_df['realized_pnl'].apply(lambda x: f"${float(x):,.2f}")
                display_df['cumulative_pnl'] = display_df['cumulative_pnl'].apply(lambda x: f"${float(x):,.2f}")
                display_df.columns = ['#', 'DateTime', 'Symbol', 'Action', 'Qty',
                                       'Price', 'Comm', 'P&L', 'Cum P&L']
                st.dataframe(display_df, width='stretch', hide_index=True)

                # CSV export button
                csv_bytes = pnl_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="⬇️ Export Trade History CSV",
                    data=csv_bytes,
                    file_name=f"trades_{selected}_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                )

    st.divider()

    with st.expander("📋 Raw Executions"):
        st.dataframe(df, width='stretch', hide_index=True)

    if st.session_state.connected and st.session_state.ib_client:
        with st.expander("🔧 Market Prices Debug"):
            client = st.session_state.ib_client
            with client.lock:
                execs_copy = list(client.executions)
                market_prices_copy = dict(client.market_prices)
                price_errors_copy = dict(client.price_errors)
                price_status_copy = dict(client.price_status)

            symbols_info = {ex['symbol']: {'secType': ex.get('secType', 'STK'),
                                            'currency': ex.get('currency', 'USD')}
                            for ex in execs_copy}
            debug_data = [{
                "Symbol":   sym,
                "SecType":  info['secType'],
                "Currency": info['currency'],
                "Price":    f"${market_prices_copy[sym]:,.4f}" if sym in market_prices_copy else "N/A",
                "Status":   _translate_error_status(
                                price_errors_copy.get(sym, price_status_copy.get(sym, "No response"))
                            ),
            } for sym, info in symbols_info.items()]
            st.dataframe(debug_data, width='stretch', hide_index=True)

    # Data source info
    st.sidebar.divider()
    snapshot_exists = os.path.exists(SNAPSHOT_FILE)
    st.sidebar.caption(
        f"CSV trades: {len(st.session_state.csv_executions)} | "
        f"Live: {len(st.session_state.live_executions)} | "
        f"Merged: {len(executions)}"
    )
    st.sidebar.caption(
        f"💾 Snapshot: {'✅ ' + os.path.basename(SNAPSHOT_FILE) if snapshot_exists else '⚠️ Not yet saved'}"
    )

    # ── Auto-refresh trigger ─────────────────────────────────────────────────
    # MUST use st_autorefresh — NOT window.parent.location.reload().
    # A browser-level reload creates a brand-new Streamlit session, wiping
    # st.session_state entirely: ib_client → None, connected → False,
    # live_executions → []. The IBKR daemon thread becomes a true orphan.
    # Result: user must manually reconnect after every auto-refresh cycle.
    # st_autorefresh triggers a script rerun which preserves all session state.
    if auto_refresh:
        if not _HAS_ST_AUTOR:
            st.error(
                "streamlit-autorefresh is not installed — auto-refresh disabled. "
                "Run: pip install streamlit-autorefresh, then restart the dashboard."
            )
        else:
            # On each auto-refresh cycle, fetch fresh data from IBKR when connected.
            if st.session_state.connected and not refresh_clicked and not reload_csv:
                _ar_client = st.session_state.ib_client
                if safe_ib_ready(_ar_client):
                    ok_ar = request_executions(_ar_client, days_back=int(days_back))
                    with _ar_client.lock:
                        _ar_live = list(_ar_client.executions)
                        _ar_client._commission_updated = False
                    if ok_ar and _ar_live:
                        st.session_state.live_executions = _ar_live
                        st.session_state.merged_executions = merge_executions(
                            st.session_state.csv_executions, _ar_live)
                        st.session_state.last_refresh = datetime.now()
                    _ar_symbols = _build_symbols_info(st.session_state.merged_executions or [])
                    if _ar_symbols:
                        request_market_prices(_ar_client, _ar_symbols)

            merged = st.session_state.merged_executions
            if merged:
                save_merged_snapshot(merged)

            # Skip the rerun trigger when user just pressed a button this cycle.
            if not refresh_clicked and not reload_csv:
                secs = max(5, int(st.session_state.auto_refresh_secs))
                st_autorefresh(interval=secs * 1000, key="aqs_autorefresh")



if __name__ == "__main__":
    main()
