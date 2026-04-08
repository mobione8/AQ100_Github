#!/usr/bin/env python3
"""
Portfolio Strategy Runner
=========================
Scans deploy_ibkr_portfolio/ for generated portfolio scripts,
prompts for symbol selection, and launches selected strategies
as separate processes (each in its own console window).

Usage:
    python 05_run_portfolio_strategy.py
    python 05_run_portfolio_strategy.py --list        # list available portfolios only
    python 05_run_portfolio_strategy.py --symbols QQQ PLTR   # launch directly (no prompt)
    python 05_run_portfolio_strategy.py --all                # launch all directly

Fixes applied:
    [FIX-1] Symbol extraction now handles interval-suffixed filenames
            e.g. TQQQ_1h_portfolio.py -> TQQQ  (was incorrectly showing TQQQ_1H)
            Supports: _1min, _5min, _15min, _30min, _1h, _4h, _1d, _1w
    [FIX-2] Added 3-second stagger between launches to prevent IBKR HMDS
            error 162 (historical data query cancelled) caused by too many
            simultaneous reqHistoricalData calls on startup.
    [FIX-3] Fixed monitor() bug — process exit was being reported repeatedly
            for the same process on every poll cycle. Now tracks reported PIDs.
"""

import os
import sys
import time
import argparse
import subprocess
from pathlib import Path
from datetime import datetime


# =============================================================================
# PATHS
# =============================================================================

BASE_DIR       = Path(__file__).resolve().parent
PORTFOLIO_DIR  = BASE_DIR / "deploy_ibkr_portfolio"

# [FIX-2] Seconds to wait between consecutive subprocess launches.
# Prevents IBKR HMDS error 162 from simultaneous reqHistoricalData storms.
HMDS_LAUNCH_STAGGER_S = 3

# Known interval suffixes to strip when extracting the symbol from filename.
# Order matters — longer patterns must come before shorter ones.
# e.g. TQQQ_30min_portfolio -> strip _30min_ -> TQQQ
#      TQQQ_1h_portfolio    -> strip _1h_    -> TQQQ
_INTERVAL_SUFFIXES = [
    "_1min", "_5min", "_15min", "_30min",
    "_1h", "_4h", "_1d", "_1w",
]


# =============================================================================
# DISCOVERY
# =============================================================================

def _extract_symbol(stem: str) -> str:
    """
    [FIX-1] Parse the trading symbol from a portfolio script filename stem.

    Handles both legacy and new naming conventions:
        Legacy : QQQ_portfolio          -> QQQ
        New    : TQQQ_1h_portfolio      -> TQQQ
                 SPY_30min_portfolio    -> SPY
                 NVDA_1d_portfolio      -> NVDA

    Strategy:
        1. Strip the trailing _portfolio suffix.
        2. Strip any known interval suffix (_1h, _1d, etc.).
        3. Uppercase and return what remains.
    """
    name = stem

    # Step 1: remove _portfolio
    if name.endswith("_portfolio"):
        name = name[: -len("_portfolio")]

    # Step 2: remove interval suffix if present (longest match first)
    for suffix in _INTERVAL_SUFFIXES:
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break

    return name.upper()


def scan_portfolios() -> list[dict]:
    """
    Scan deploy_ibkr_portfolio/ for *_portfolio.py files.
    Returns list of dicts with keys: symbol, interval, path, dir.
    Sorted alphabetically by symbol then interval.
    """
    found = {}

    if not PORTFOLIO_DIR.exists():
        return []

    for fpath in sorted(PORTFOLIO_DIR.glob("*_portfolio.py")):
        symbol = _extract_symbol(fpath.stem)

        # Derive interval label for display (e.g. "1h", "1d")
        interval = ""
        stem_no_portfolio = fpath.stem
        if stem_no_portfolio.endswith("_portfolio"):
            stem_no_portfolio = stem_no_portfolio[: -len("_portfolio")]
        for suffix in _INTERVAL_SUFFIXES:
            if stem_no_portfolio.endswith(suffix):
                interval = suffix.lstrip("_")
                break

        # Use filename as unique key so multiple intervals for the same
        # symbol are all registered (e.g. SPY_1h and SPY_1d).
        key = fpath.stem
        if key not in found:
            found[key] = {
                "symbol":   symbol,
                "interval": interval,
                "path":     fpath,
                "dir":      fpath.parent,
            }

    return sorted(found.values(), key=lambda x: (x["symbol"], x["interval"]))


# =============================================================================
# DISPLAY
# =============================================================================

def print_banner():
    print("""
╔══════════════════════════════════════════════════════════════╗
║           Portfolio Strategy Runner  ·  AQS100 DemoV3       ║
╚══════════════════════════════════════════════════════════════╝
""")


def print_portfolio_table(portfolios: list[dict]):
    if not portfolios:
        print("  (no portfolio scripts found)")
        return
    print(f"  {'#':<4}  {'Symbol':<12}  {'Interval':<10}  {'File'}")
    print(f"  {'-'*4}  {'-'*12}  {'-'*10}  {'-'*50}")
    for i, p in enumerate(portfolios, 1):
        rel = p["path"].relative_to(BASE_DIR)
        print(f"  {i:<4}  {p['symbol']:<12}  {p['interval']:<10}  {rel}")
    print()


# =============================================================================
# SELECTION PROMPT
# =============================================================================

def prompt_selection(portfolios: list[dict]) -> list[dict]:
    """
    Interactive prompt.  Accepts:
        all              -> all portfolios
        1 3 5            -> space-separated indices
        1,3,5            -> comma-separated indices
        TQQQ SPY         -> symbol names (matches ALL intervals for that symbol)
    """
    n = len(portfolios)

    # Build symbol -> [portfolio entries] map for name-based selection
    by_symbol: dict[str, list[dict]] = {}
    for p in portfolios:
        by_symbol.setdefault(p["symbol"], []).append(p)

    print("  Select portfolios to deploy:")
    print("    • Enter 'all' to deploy everything")
    print("    • Enter numbers: e.g. 1 3  or  1,3")
    print(f"    • Enter symbols: e.g. TQQQ SPY  (selects all intervals)")
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
            elif tok.upper() in by_symbol:
                selected.extend(by_symbol[tok.upper()])
            else:
                errors.append(f"'{tok}' not recognised")

        if errors:
            print(f"  ✗ Unrecognised: {', '.join(errors)}")
            print(f"    Please use numbers 1-{n}, symbol names, or 'all'.\n")
            continue

        if not selected:
            print("  No valid entries — try again.\n")
            continue

        # Deduplicate while preserving order
        seen   = set()
        unique = []
        for p in selected:
            key = str(p["path"])
            if key not in seen:
                seen.add(key)
                unique.append(p)

        return unique


# =============================================================================
# LAUNCH
# =============================================================================

def launch_portfolio(portfolio: dict) -> subprocess.Popen | None:
    """
    Launch a portfolio script as a new subprocess with its own console window.
    Returns the Popen handle, or None on failure.
    """
    script = portfolio["path"]
    cwd    = portfolio["dir"]
    label  = f"{portfolio['symbol']}_{portfolio['interval']}" if portfolio["interval"] else portfolio["symbol"]

    cmd = [sys.executable, str(script)]

    try:
        if sys.platform == "win32":
            proc = subprocess.Popen(
                cmd,
                cwd=str(cwd),
                creationflags=subprocess.CREATE_NEW_CONSOLE,
            )
        else:
            proc = subprocess.Popen(
                ["bash", "-c", f"cd '{cwd}' && python '{script}'; exec bash"],
                cwd=str(cwd),
            )
        print(f"  ✓ [{label}] launched — PID {proc.pid}")
        return proc
    except Exception as e:
        print(f"  ✗ [{label}] failed to launch: {e}")
        return None


def launch_all(portfolios: list[dict]) -> list[subprocess.Popen]:
    """
    [FIX-2] Launch all selected portfolios with a 3-second stagger between
    each launch to prevent IBKR HMDS error 162 from simultaneous
    reqHistoricalData requests flooding the TWS connection.
    """
    print()
    print("  Launching portfolios...")
    print(f"  (staggering {HMDS_LAUNCH_STAGGER_S}s between each to avoid IBKR HMDS limits)")
    print()
    handles = []
    total   = len(portfolios)
    for i, p in enumerate(portfolios):
        h = launch_portfolio(p)
        if h:
            handles.append(h)
        # Stagger all but the last launch
        if i < total - 1:
            print(f"  [HMDS pacing] waiting {HMDS_LAUNCH_STAGGER_S}s before next launch...")
            time.sleep(HMDS_LAUNCH_STAGGER_S)
    return handles


# =============================================================================
# MONITOR
# =============================================================================

def monitor(handles: list[subprocess.Popen], portfolios: list[dict]):
    """
    [FIX-3] After launching, park here so the user can see which processes are
    running.  Tracks already-reported PIDs so each exit is reported only once.
    Press Ctrl+C to stop monitoring (processes keep running).
    """
    if not handles:
        return

    # Build pid -> label map
    pid_map = {}
    for h, p in zip(handles, portfolios):
        label = f"{p['symbol']}_{p['interval']}" if p["interval"] else p["symbol"]
        pid_map[h.pid] = label

    reported = set()

    print()
    print("=" * 62)
    print(f"  {len(handles)} portfolio(s) running in separate console windows.")
    print(f"  Running: {', '.join(pid_map.values())}")
    print()
    print("  Each strategy window manages its own IBKR connection.")
    print("  Close the individual console windows to stop strategies.")
    print()
    print("  Press Ctrl+C here to exit this launcher (strategies keep running).")
    print("=" * 62)

    try:
        while True:
            time.sleep(10)
            for h in handles:
                if h.poll() is not None and h.pid not in reported:
                    reported.add(h.pid)
                    label = pid_map.get(h.pid, f"PID {h.pid}")
                    print(
                        f"  ⚠  [{label}] process exited "
                        f"(code {h.returncode}) — {datetime.now():%H:%M:%S}"
                    )
            if len(reported) == len(handles):
                print()
                print("  All portfolio processes have exited.")
                break
    except KeyboardInterrupt:
        print("\n\n  Launcher exited.  Strategy processes continue running.")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Launch portfolio strategies from deploy_ibkr_portfolio/"
    )
    parser.add_argument("--list",    action="store_true", help="List available portfolios and exit")
    parser.add_argument("--symbols", nargs="+", metavar="SYM",  help="Symbols to launch (skip prompt)")
    parser.add_argument("--all",     action="store_true", help="Launch all portfolios (skip prompt)")
    args = parser.parse_args()

    print_banner()

    # ── discover ─────────────────────────────────────────────────────────────
    portfolios = scan_portfolios()

    if not portfolios:
        print(f"  No portfolio scripts found in:\n    {PORTFOLIO_DIR}")
        print("\n  Run 04_generate_deployment_codes_v2.py --portfolio first.")
        sys.exit(1)

    print(f"  Found {len(portfolios)} portfolio(s) in deploy_ibkr_portfolio/:\n")
    print_portfolio_table(portfolios)

    if args.list:
        sys.exit(0)

    # ── selection ─────────────────────────────────────────────────────────────
    if args.all:
        selected = list(portfolios)
        print(f"  --all flag: deploying all {len(selected)} portfolio(s).\n")
    elif args.symbols:
        # Build symbol -> entries map
        by_symbol: dict[str, list[dict]] = {}
        for p in portfolios:
            by_symbol.setdefault(p["symbol"], []).append(p)

        selected = []
        for s in args.symbols:
            key = s.upper()
            if key in by_symbol:
                selected.extend(by_symbol[key])
            else:
                print(f"  ✗ Symbol '{s}' not found — skipping.")
        if not selected:
            print("  No valid symbols specified. Exiting.")
            sys.exit(1)
        labels = [f"{p['symbol']}_{p['interval']}" if p["interval"] else p["symbol"] for p in selected]
        print(f"  --symbols flag: deploying {', '.join(labels)}.\n")
    else:
        selected = prompt_selection(portfolios)

    if not selected:
        print("  No portfolios selected. Exiting.")
        sys.exit(0)

    # ── confirm ───────────────────────────────────────────────────────────────
    print()
    print(f"  About to launch {len(selected)} portfolio(s):")
    for p in selected:
        print(f"    • {p['symbol']} ({p['interval']})  →  {p['path'].relative_to(BASE_DIR)}")
    print()

    try:
        confirm = input("  Confirm launch? (y/n): ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print("\n  Cancelled.")
        sys.exit(0)

    if confirm != "y":
        print("  Cancelled.")
        sys.exit(0)

    # ── launch ────────────────────────────────────────────────────────────────
    handles = launch_all(selected)

    if not handles:
        print("\n  No portfolios launched successfully.")
        sys.exit(1)

    # ── monitor ───────────────────────────────────────────────────────────────
    monitor(handles, selected)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Launcher cancelled.")
        sys.exit(0)
