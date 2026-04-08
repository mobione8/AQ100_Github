#!/usr/bin/env python3
"""
Alpha Launch Pad
================
Unified entry point that lets you deploy either Single Strategies
(from deploy_ibkr/) or Portfolio Strategies (from deploy_ibkr_portfolio/).

Usage:
    python 06_alpha_launch_pad.py                          # interactive mode
    python 06_alpha_launch_pad.py --single                 # go straight to single strategy
    python 06_alpha_launch_pad.py --portfolio              # go straight to portfolio strategy
    python 06_alpha_launch_pad.py --list                   # list available strategies/portfolios
    python 06_alpha_launch_pad.py --portfolio --interval 1h  # skip interval prompt
"""

import os
import sys
import json
import time
import shlex
import hashlib
import logging
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
from collections import defaultdict


# =============================================================================
# PATHS
# =============================================================================

BASE_DIR       = Path(__file__).resolve().parent
DEPLOY_DIR     = BASE_DIR / "deploy_ibkr"
PORTFOLIO_DIR  = BASE_DIR / "deploy_ibkr_portfolio"
LOG_DIR        = BASE_DIR / "logs"

SKIP_DIRS = {"logs", "__pycache__", ".git"}

MODE_SINGLE    = "single"
MODE_PORTFOLIO = "portfolio"
ST_RUNNING     = "running"

KNOWN_INTERVALS = {"1min", "5min", "15min", "30min", "1h", "4h", "1d", "1w"}

# IBKR HMDS pacing ─────────────────────────────────────────────────────────
# Simultaneous reqHistoricalData calls from multiple subprocesses trigger
# error 162 ("API historical data query cancelled").  Staggering launches
# keeps the initial HMDS burst well inside IBKR's ~6-requests-per-2-second
# limit.  The 15-second rule (mandatory wait after error 162 on the *same*
# contract) must be enforced inside each strategy script, not here.
HMDS_LAUNCH_STAGGER_S = 3   # seconds between consecutive subprocess spawns

REGISTRY_FILE  = BASE_DIR / "strategy_registry.json"

log = logging.getLogger(__name__)


# =============================================================================
# LOGGING SETUP
# =============================================================================

def _setup_logging() -> Path:
    """
    Configure file-only logging for this launcher session.

    One dated log file is created per run under logs/.  The console is left
    untouched — all interactive output still comes from print() calls.
    Returns the Path of the log file so main() can display it to the user.
    """
    try:
        LOG_DIR.mkdir(exist_ok=True)
    except OSError as e:
        print(f"  [WARN] Cannot create log directory '{LOG_DIR}': {e}  — file logging disabled.")
        return Path(os.devnull)

    log_file = LOG_DIR / f"launch_pad_{datetime.now():%Y%m%d_%H%M%S}.log"

    # Use direct handler attachment rather than basicConfig so the file handler
    # is always installed regardless of whether any prior import has already
    # configured the root logger (basicConfig is a silent no-op in that case).
    handler = logging.FileHandler(log_file, encoding="utf-8")
    handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(handler)

    return log_file


# =============================================================================
# STRATEGY REGISTRY  (read/write for 09_strategy_watchdog.py)
# =============================================================================

def _registry_read() -> list[dict]:
    """Load the existing registry (empty list if file is missing or corrupt)."""
    if not REGISTRY_FILE.exists():
        return []
    try:
        return json.loads(REGISTRY_FILE.read_text(encoding="utf-8")).get("entries", [])
    except Exception as e:
        print(f"  [WARN] Registry read failed ({e}); existing watchdog entries lost.")
        log.warning("Registry read failed: %s", e)
        return []


def _registry_write(entries: list[dict]) -> None:
    """Atomically write the registry (tmp → rename)."""
    payload = {
        "version": 1,
        "updated": datetime.now().isoformat(timespec="seconds"),
        "entries": entries,
    }
    tmp = REGISTRY_FILE.with_suffix(".json.tmp")
    try:
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        os.replace(tmp, REGISTRY_FILE)
    except Exception as e:
        print(f"  [WARN] Could not write strategy registry: {e}")
        log.error("Registry write failed: %s", e)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _register_launched(pairs: list[tuple], mode: str) -> None:
    """
    Add newly launched processes to strategy_registry.json so that
    09_strategy_watchdog.py can monitor and auto-restart them.

    Existing entries for the same script are replaced so the PID is
    always current after a manual relaunch via 06.
    """
    entries = _registry_read()

    # Index existing entries by script path for deduplication
    by_script: dict[str, int] = {
        e["script"]: i for i, e in enumerate(entries)
    }

    now = datetime.now().isoformat(timespec="seconds")
    for proc, item in pairs:
        script = str(item["path"])
        label  = item.get("name") or item.get("symbol", "unknown")
        entry  = {
            "id":            hashlib.sha1(f"{script}{now}".encode(), usedforsecurity=False).hexdigest()[:12],
            "label":         label,
            "mode":          mode,
            "script":        script,
            "cwd":           str(item["dir"]),
            "pid":           proc.pid,
            "launch_time":   now,
            "restart_count": 0,
            "restart_times": [],
            "status":        ST_RUNNING,
        }
        if script in by_script:
            entries[by_script[script]] = entry   # replace stale entry
        else:
            entries.append(entry)

    _registry_write(entries)
    print(f"\n  [WATCHDOG] {len(pairs)} entr{'y' if len(pairs) == 1 else 'ies'} "
          f"written to {REGISTRY_FILE.name}")
    for proc, item in pairs:
        log.info("REGISTRY  label=%s  pid=%d  script=%s",
                 item.get("name") or item.get("symbol", "unknown"),
                 proc.pid, item["path"])


# =============================================================================
# BANNER
# =============================================================================

def print_banner():
    print("""
╔══════════════════════════════════════════════════════════════╗
║              Alpha Launch Pad  ·  AQS100                     ║
╚══════════════════════════════════════════════════════════════╝
""")


# =============================================================================
# MODE SELECTION
# =============================================================================

def prompt_mode() -> str:
    """
    Ask the user whether to deploy Single or Portfolio strategies.
    Returns 'single' or 'portfolio'.
    """
    print("  What would you like to deploy?")
    print()
    print("    [1]  Single Strategy   — deploy_ibkr/")
    print("    [2]  Portfolio         — deploy_ibkr_portfolio/")
    print()

    while True:
        try:
            raw = input("  Your choice (1 or 2): ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)

        if raw == "1":
            return MODE_SINGLE
        elif raw == "2":
            return MODE_PORTFOLIO
        else:
            print("  Please enter 1 or 2.\n")


def prompt_interval(intervals: list[str]) -> str:
    """Prompt user to select interval. Auto-selects if only one available."""
    if len(intervals) == 1:
        print(f"  Interval: {intervals[0]}\n")
        return intervals[0]

    print("  Available intervals:")
    for i, iv in enumerate(intervals, 1):
        print(f"    [{i}]  {iv}")
    print()

    while True:
        try:
            raw = input("  Select interval: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)

        if raw.isdigit() and 1 <= int(raw) <= len(intervals):
            return intervals[int(raw) - 1]
        if raw in intervals:
            return raw
        print("  Please enter a number or interval name.\n")


# =============================================================================
# SINGLE STRATEGY — DISCOVERY
# =============================================================================

def extract_symbol(filename: str) -> str:
    return filename.split("_")[0].upper()


def extract_interval(stem: str) -> str:
    """Extract interval from filename stem (e.g. 'META_1h_...' → '1h'). Default '1h'."""
    tokens = stem.split("_")
    if len(tokens) > 1 and tokens[1] in KNOWN_INTERVALS:
        return tokens[1]
    return "1h"


def discover_single_intervals() -> list[str]:
    """Scan single strategy dir and return sorted unique intervals found."""
    if not DEPLOY_DIR.exists():
        return []
    intervals = set()
    for fpath in DEPLOY_DIR.rglob("*.py"):
        if any(part in SKIP_DIRS for part in fpath.parts):
            continue
        if fpath.stem.startswith("__"):
            continue
        intervals.add(extract_interval(fpath.stem))
    return sorted(intervals)


def scan_strategies(interval: str = None) -> dict[str, list[dict]]:
    """
    Recursively scan DEPLOY_DIR for *.py files.
    Returns dict keyed by symbol, each value a sorted list of strategy dicts.
    If interval is provided, only returns strategies matching that interval.
    """
    if not DEPLOY_DIR.exists():
        return {}

    by_symbol: dict[str, list[dict]] = defaultdict(list)

    for fpath in sorted(DEPLOY_DIR.rglob("*.py")):
        if any(part in SKIP_DIRS for part in fpath.parts):
            continue
        if fpath.stem.startswith("__"):
            continue

        symbol = extract_symbol(fpath.stem)
        ivl = extract_interval(fpath.stem)
        if interval and ivl != interval:
            continue
        by_symbol[symbol].append({
            "name":   fpath.stem,
            "path":   fpath,
            "dir":    fpath.parent,
            "symbol": symbol,
        })

    return dict(sorted(by_symbol.items()))


# =============================================================================
# SINGLE STRATEGY — DISPLAY
# =============================================================================

def print_symbol_table(by_symbol: dict[str, list[dict]]):
    if not by_symbol:
        print("  (no strategy scripts found)")
        return
    print(f"  {'#':<4}  {'Symbol':<12}  {'Strategies':<12}  {'Subfolder(s)'}")
    print(f"  {'-'*4}  {'-'*12}  {'-'*12}  {'-'*40}")
    for i, (symbol, strategies) in enumerate(by_symbol.items(), 1):
        folders = sorted({
            str(s["path"].parent.relative_to(DEPLOY_DIR)) for s in strategies
        })
        folders_str = ", ".join(folders) if folders else "."
        print(f"  {i:<4}  {symbol:<12}  {len(strategies):<12}  {folders_str}")
    print()


# =============================================================================
# SINGLE STRATEGY — SELECTION
# =============================================================================

def prompt_single_selection(by_symbol: dict[str, list[dict]]) -> list[dict]:
    """
    Interactive symbol-level selection for single strategies.
    Returns flat list of strategy dicts to launch.
    """
    symbols    = list(by_symbol.keys())
    n          = len(symbols)
    symbol_map = {s.upper(): s for s in symbols}

    print("  Select symbols to deploy:")
    print("    • 'all'          → every strategy in deploy_ibkr/")
    print("    • numbers        → e.g.  1 3   or   1,3")
    print("    • symbol names   → e.g.  QQQ UVXY")
    print()

    while True:
        try:
            raw = input("  Your selection: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return []

        if not raw:
            print("  Please enter a selection.\n")
            continue

        if raw.lower() == "all":
            return [s for strats in by_symbol.values() for s in strats]

        tokens   = [t.strip() for t in raw.replace(",", " ").split() if t.strip()]
        selected = []
        errors   = []

        for tok in tokens:
            if tok.isdigit():
                idx = int(tok)
                if 1 <= idx <= n:
                    selected.extend(by_symbol[symbols[idx - 1]])
                else:
                    errors.append(f"'{tok}' out of range (1-{n})")
            elif tok.upper() in symbol_map:
                selected.extend(by_symbol[symbol_map[tok.upper()]])
            else:
                errors.append(f"'{tok}' not recognised")

        if errors:
            print(f"  ✗ Unrecognised: {', '.join(errors)}")
            print(f"    Use numbers 1-{n}, symbol names, or 'all'.\n")
            continue

        if not selected:
            print("  No valid entries — try again.\n")
            continue

        seen, unique = set(), []
        for s in selected:
            if s["path"] not in seen:
                seen.add(s["path"])
                unique.append(s)

        return unique


# =============================================================================
# SHARED LAUNCH HELPER
# =============================================================================

def _spawn_process(script: Path, cwd: Path, label: str) -> "subprocess.Popen | None":
    """Launch a Python script in its own console window (platform-aware)."""
    cmd = [sys.executable, str(script)]
    try:
        if sys.platform == "win32":
            proc = subprocess.Popen(
                cmd,
                cwd=cwd,
                creationflags=subprocess.CREATE_NEW_CONSOLE,
            )
        else:
            proc = subprocess.Popen(
                ["bash", "-c", f"cd {shlex.quote(str(cwd))} && python {shlex.quote(str(script))}; exec bash"],
                cwd=cwd,
            )
        print(f"  ✓  {label}  (PID {proc.pid})")
        log.info("SPAWN OK   label=%s  pid=%d  script=%s", label, proc.pid, script)
        return proc
    except Exception as e:
        print(f"  ✗  {label}  ERROR: {e}")
        log.error("SPAWN FAIL  label=%s  error=%s", label, e)
        return None


# =============================================================================
# SINGLE STRATEGY — LAUNCH
# =============================================================================

def launch_strategy(strategy: dict) -> "subprocess.Popen | None":
    return _spawn_process(strategy["path"], strategy["dir"], strategy["name"])


def launch_strategies(strategies: list[dict]) -> "list[tuple[subprocess.Popen, dict]]":
    print()
    print(f"  Launching {len(strategies)} strategy window(s)...\n")
    pairs = []
    for i, s in enumerate(strategies):
        h = launch_strategy(s)
        if h:
            pairs.append((h, s))
        # Stagger launches to avoid simultaneous reqHistoricalData calls that
        # trigger IBKR HMDS error 162.  Skip the sleep after the last item.
        if i < len(strategies) - 1:
            print(f"  [HMDS pacing] waiting {HMDS_LAUNCH_STAGGER_S}s before next launch …")
            log.debug("HMDS pacing: sleeping %ds before next strategy launch", HMDS_LAUNCH_STAGGER_S)
            time.sleep(HMDS_LAUNCH_STAGGER_S)
    return pairs


# =============================================================================
# SINGLE STRATEGY — CONFIRM
# =============================================================================

def confirm_single(strategies: list[dict]) -> bool:
    by_sym: dict[str, list[dict]] = defaultdict(list)
    for s in strategies:
        by_sym[s["symbol"]].append(s)

    print()
    print(f"  About to launch {len(strategies)} strategy window(s):")
    for sym, strats in sorted(by_sym.items()):
        print(f"    {sym}  ({len(strats)} strategies)")
        for s in strats:
            print(f"      • {s['path'].relative_to(BASE_DIR)}")
    print()

    try:
        answer = input("  Confirm launch? (y/n): ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print("\n  Cancelled.")
        return False

    return answer == "y"


# =============================================================================
# SINGLE STRATEGY — MONITOR
# =============================================================================

def monitor_strategies(handles: list["subprocess.Popen"], strategies: list[dict]):
    if not handles:
        return

    pid_map  = {h.pid: s["name"] for h, s in zip(handles, strategies)}
    reported = set()

    print()
    print("=" * 62)
    print(f"  {len(handles)} strategy window(s) running.")
    print()

    by_sym: dict[str, list[str]] = defaultdict(list)
    for s in strategies:
        by_sym[s["symbol"]].append(s["name"])
    for sym, names in sorted(by_sym.items()):
        print(f"  {sym}  ({len(names)} strategies)")
        for n in names:
            print(f"      • {n}")

    print()
    print("  Each strategy window manages its own IBKR connection.")
    print("  Close individual console windows to stop a strategy.")
    print()
    print("  Press Ctrl+C here to exit this launcher (strategies keep running).")
    print("=" * 62)

    try:
        while True:
            time.sleep(10)
            for h in handles:
                if h.poll() is not None and h.pid not in reported:
                    reported.add(h.pid)
                    name = pid_map.get(h.pid, f"PID {h.pid}")
                    print(
                        f"  ⚠  [{name}] exited "
                        f"(code {h.returncode}) — {datetime.now():%H:%M:%S}"
                    )
                    log.warning("EXITED  name=%s  pid=%d  code=%d", name, h.pid, h.returncode)
            if len(reported) == len(handles):
                print()
                print("  All strategy processes have exited.")
                log.info("All strategy processes have exited.")
                break
    except KeyboardInterrupt:
        print("\n\n  Launcher exited.  Strategy processes continue running.")
        log.info("Launcher exited via KeyboardInterrupt; strategy processes still running.")


# =============================================================================
# PORTFOLIO — DISCOVERY
# =============================================================================

def discover_portfolio_intervals() -> list[str]:
    """Scan portfolio dir and return sorted unique intervals found."""
    if not PORTFOLIO_DIR.exists():
        return []
    intervals = set()
    for fpath in PORTFOLIO_DIR.glob("*_portfolio.py"):
        stem = fpath.stem.replace("_portfolio", "")
        intervals.add(extract_interval(stem))
    return sorted(intervals)


def scan_portfolios(interval: str = None) -> list[dict]:
    """
    Scan deploy_ibkr_portfolio/ for *_portfolio.py files.
    Returns list of dicts with keys: symbol, interval, path, dir.
    If interval is provided, only returns portfolios matching that interval.
    """
    if not PORTFOLIO_DIR.exists():
        return []

    found = {}
    for fpath in sorted(PORTFOLIO_DIR.glob("*_portfolio.py")):
        stem = fpath.stem.replace("_portfolio", "")
        sym = extract_symbol(stem)
        ivl = extract_interval(stem)
        if interval and ivl != interval:
            continue
        if sym not in found:
            found[sym] = {
                "symbol":   sym,
                "interval": ivl,
                "path":     fpath,
                "dir":      fpath.parent,
            }

    return sorted(found.values(), key=lambda x: x["symbol"])


# =============================================================================
# PORTFOLIO — DISPLAY
# =============================================================================

def print_portfolio_table(portfolios: list[dict]):
    if not portfolios:
        print("  (no portfolio scripts found)")
        return
    print(f"  {'#':<4}  {'Symbol':<12}  {'File'}")
    print(f"  {'-'*4}  {'-'*12}  {'-'*50}")
    for i, p in enumerate(portfolios, 1):
        rel = p["path"].relative_to(BASE_DIR)
        print(f"  {i:<4}  {p['symbol']:<12}  {rel}")
    print()


# =============================================================================
# PORTFOLIO — SELECTION
# =============================================================================

def prompt_portfolio_selection(portfolios: list[dict]) -> list[dict]:
    symbols_upper = {p["symbol"]: p for p in portfolios}
    n = len(portfolios)

    print("  Select portfolios to deploy:")
    print("    • Enter 'all' to deploy everything")
    print("    • Enter numbers: e.g. 1 3  or  1,3")
    print("    • Enter symbols: e.g. QQQ PLTR")
    print()

    while True:
        try:
            raw = input("  Your selection: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return []

        if not raw:
            print("  Please enter a selection.\n")
            continue

        if raw.lower() == "all":
            return list(portfolios)

        tokens   = [t.strip() for t in raw.replace(",", " ").split() if t.strip()]
        selected = []
        errors   = []

        for tok in tokens:
            if tok.isdigit():
                idx = int(tok)
                if 1 <= idx <= n:
                    selected.append(portfolios[idx - 1])
                else:
                    errors.append(f"'{tok}' out of range (1-{n})")
            elif tok.upper() in symbols_upper:
                selected.append(symbols_upper[tok.upper()])
            else:
                errors.append(f"'{tok}' not recognised")

        if errors:
            print(f"  ✗ Unrecognised: {', '.join(errors)}")
            print(f"    Please use numbers 1-{n}, symbols, or 'all'.\n")
            continue

        if not selected:
            print("  No valid entries — try again.\n")
            continue

        seen   = set()
        unique = []
        for p in selected:
            if p["symbol"] not in seen:
                seen.add(p["symbol"])
                unique.append(p)

        return unique


# =============================================================================
# PORTFOLIO — LAUNCH
# =============================================================================

def launch_portfolio(portfolio: dict) -> "subprocess.Popen | None":
    return _spawn_process(portfolio["path"], portfolio["dir"], f"[{portfolio['symbol']}]")


def launch_portfolios(portfolios: list[dict]) -> "list[tuple[subprocess.Popen, dict]]":
    print()
    print("  Launching portfolios...\n")
    pairs = []
    for i, p in enumerate(portfolios):
        h = launch_portfolio(p)
        if h:
            pairs.append((h, p))
        # Stagger launches to avoid simultaneous reqHistoricalData calls that
        # trigger IBKR HMDS error 162.  Skip the sleep after the last item.
        if i < len(portfolios) - 1:
            print(f"  [HMDS pacing] waiting {HMDS_LAUNCH_STAGGER_S}s before next launch …")
            log.debug("HMDS pacing: sleeping %ds before next portfolio launch", HMDS_LAUNCH_STAGGER_S)
            time.sleep(HMDS_LAUNCH_STAGGER_S)
    return pairs


# =============================================================================
# PORTFOLIO — CONFIRM
# =============================================================================

def confirm_portfolio(selected: list[dict]) -> bool:
    print()
    print(f"  About to launch {len(selected)} portfolio(s):")
    for p in selected:
        print(f"    • {p['symbol']}  →  {p['path'].relative_to(BASE_DIR)}")
    print()

    try:
        answer = input("  Confirm launch? (y/n): ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print("\n  Cancelled.")
        return False

    return answer == "y"


# =============================================================================
# PORTFOLIO — MONITOR
# =============================================================================

def monitor_portfolios(handles: list["subprocess.Popen"], portfolios: list[dict]):
    if not handles:
        return

    reported = set()

    print()
    print("=" * 62)
    print(f"  {len(handles)} portfolio(s) running in separate console windows.")
    print(f"  Running: {', '.join(p['symbol'] for p in portfolios)}")
    print()
    print("  Each strategy window manages its own IBKR connection.")
    print("  Close the individual console windows to stop strategies.")
    print()
    print("  Press Ctrl+C here to exit this launcher (strategies keep running).")
    print("=" * 62)

    try:
        while True:
            time.sleep(10)
            for h, p in zip(handles, portfolios):
                if h.poll() is not None and h.pid not in reported:
                    reported.add(h.pid)
                    print(
                        f"  ⚠  [{p['symbol']}] process exited "
                        f"(code {h.returncode}) — {datetime.now():%H:%M:%S}"
                    )
                    log.warning("EXITED  symbol=%s  pid=%d  code=%d",
                                p["symbol"], h.pid, h.returncode)
            if len(reported) == len(handles):
                print()
                print("  All portfolio processes have exited.")
                log.info("All portfolio processes have exited.")
                break
    except KeyboardInterrupt:
        print("\n\n  Launcher exited.  Strategy processes continue running.")
        log.info("Launcher exited via KeyboardInterrupt; portfolio processes still running.")


# =============================================================================
# SINGLE STRATEGY FLOW
# =============================================================================

def run_single(list_only: bool = False, symbols: list[str] | None = None,
               all_flag: bool = False, interval: str = None):
    print("\n  ── Single Strategy Mode ──────────────────────────────────────\n")

    # Discover and select interval
    if not interval:
        available = discover_single_intervals()
        if not available:
            print(f"  No strategy scripts found in:\n    {DEPLOY_DIR}")
            print("\n  Run generate_deployment_codes_v8.py first to generate strategies.")
            log.error("No strategy scripts found in %s", DEPLOY_DIR)
            sys.exit(1)
        interval = prompt_interval(available)

    by_symbol = scan_strategies(interval=interval)

    if not by_symbol:
        print(f"  No strategy scripts found for interval '{interval}' in:\n    {DEPLOY_DIR}")
        log.error("No strategy scripts found for interval '%s' in %s", interval, DEPLOY_DIR)
        sys.exit(1)

    total_files = sum(len(v) for v in by_symbol.values())
    print(f"  Found {total_files} strateg{'y' if total_files == 1 else 'ies'} "
          f"across {len(by_symbol)} symbol(s) in deploy_ibkr/:\n")
    print_symbol_table(by_symbol)

    if list_only:
        return

    if all_flag:
        selected = [s for strats in by_symbol.values() for s in strats]
        print(f"  --all: deploying all {len(selected)} strategies.\n")

    elif symbols:
        sym_upper = {k.upper(): k for k in by_symbol}
        selected  = []
        for raw in symbols:
            key = raw.upper()
            if key in sym_upper:
                selected.extend(by_symbol[sym_upper[key]])
            else:
                print(f"  ✗ Symbol '{raw}' not found — skipping.")
                log.warning("--symbols: '%s' not found in deploy_ibkr/ — skipped.", raw)
        if not selected:
            print("  No valid symbols specified. Exiting.")
            sys.exit(1)
        print(f"  --symbols: deploying {', '.join(symbols)}.\n")

    else:
        selected = prompt_single_selection(by_symbol)

    if not selected:
        print("  No strategies selected. Exiting.")
        log.info("No strategies selected; exiting.")
        sys.exit(0)

    if not confirm_single(selected):
        print("  Cancelled.")
        log.info("Launch cancelled by user at confirmation prompt.")
        sys.exit(0)

    log.info("Launching %d strategy/strategies: %s",
             len(selected), [s["name"] for s in selected])
    pairs = launch_strategies(selected)

    if not pairs:
        print("\n  No strategies launched successfully.")
        log.error("No strategies launched successfully.")
        sys.exit(1)

    _register_launched(pairs, MODE_SINGLE)

    handles, launched = zip(*pairs)
    monitor_strategies(list(handles), list(launched))


# =============================================================================
# PORTFOLIO FLOW
# =============================================================================

def run_portfolio(list_only: bool = False, symbols: list[str] | None = None,
                  all_flag: bool = False, interval: str = None):
    print("\n  ── Portfolio Strategy Mode ───────────────────────────────────\n")

    # Discover and select interval
    if not interval:
        available = discover_portfolio_intervals()
        if not available:
            print(f"  No portfolio scripts found in:\n    {PORTFOLIO_DIR}")
            print("\n  Run generate_deployment_codes_v8.py --portfolio first to generate portfolios.")
            log.error("No portfolio scripts found in %s", PORTFOLIO_DIR)
            sys.exit(1)
        interval = prompt_interval(available)

    portfolios = scan_portfolios(interval=interval)

    if not portfolios:
        print(f"  No portfolio scripts found for interval '{interval}' in:\n    {PORTFOLIO_DIR}")
        log.error("No portfolio scripts found for interval '%s' in %s", interval, PORTFOLIO_DIR)
        sys.exit(1)

    print(f"  Found {len(portfolios)} portfolio(s) [{interval}] in deploy_ibkr_portfolio/:\n")
    print_portfolio_table(portfolios)

    if list_only:
        return

    if all_flag:
        selected = list(portfolios)
        print(f"  --all flag: deploying all {len(selected)} portfolio(s).\n")

    elif symbols:
        sym_map  = {p["symbol"]: p for p in portfolios}
        selected = []
        for s in symbols:
            key = s.upper()
            if key in sym_map:
                selected.append(sym_map[key])
            else:
                print(f"  ✗ Symbol '{s}' not found — skipping.")
                log.warning("--symbols: '%s' not found in deploy_ibkr_portfolio/ — skipped.", s)
        if not selected:
            print("  No valid symbols specified. Exiting.")
            sys.exit(1)
        print(f"  --symbols flag: deploying {', '.join(p['symbol'] for p in selected)}.\n")

    else:
        selected = prompt_portfolio_selection(portfolios)

    if not selected:
        print("  No portfolios selected. Exiting.")
        log.info("No portfolios selected; exiting.")
        sys.exit(0)

    if not confirm_portfolio(selected):
        print("  Cancelled.")
        log.info("Launch cancelled by user at confirmation prompt.")
        sys.exit(0)

    log.info("Launching %d portfolio(s): %s",
             len(selected), [p["symbol"] for p in selected])
    pairs = launch_portfolios(selected)

    if not pairs:
        print("\n  No portfolios launched successfully.")
        log.error("No portfolios launched successfully.")
        sys.exit(1)

    _register_launched(pairs, MODE_PORTFOLIO)

    handles, launched = zip(*pairs)
    monitor_portfolios(list(handles), list(launched))


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Alpha Launch Pad — deploy single or portfolio strategies"
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--single",    action="store_true",
                            help="Deploy single strategies (skips mode prompt)")
    mode_group.add_argument("--portfolio", action="store_true",
                            help="Deploy portfolio strategies (skips mode prompt)")

    parser.add_argument("--list",    action="store_true",
                        help="List available strategies/portfolios and exit")
    parser.add_argument("--symbols", nargs="+", metavar="SYM",
                        help="Symbols to launch (skips selection prompt)")
    parser.add_argument("--all",     action="store_true",
                        help="Launch all available strategies/portfolios")
    parser.add_argument("--interval", metavar="IVL",
                        help="Interval to deploy (e.g. 1h, 1d). Skips interval prompt.")
    args = parser.parse_args()

    log_file = _setup_logging()
    log.info("=== Alpha Launch Pad started === sys.argv=%s", sys.argv[1:])

    print_banner()
    print(f"  Log: {log_file}\n")

    # ── determine mode ────────────────────────────────────────────────────────
    if args.single:
        mode = MODE_SINGLE
    elif args.portfolio:
        mode = MODE_PORTFOLIO
    else:
        mode = prompt_mode()

    log.info("Mode selected: %s", mode)

    # ── dispatch ──────────────────────────────────────────────────────────────
    if mode == MODE_SINGLE:
        run_single(
            list_only=args.list,
            symbols=args.symbols,
            all_flag=args.all,
            interval=args.interval,
        )
    else:
        run_portfolio(
            list_only=args.list,
            symbols=args.symbols,
            all_flag=args.all,
            interval=args.interval,
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Launcher cancelled.")
        log.info("Launcher cancelled via KeyboardInterrupt.")
        sys.exit(0)
