#!/usr/bin/env python3
"""
Single Strategy Runner
======================
Scans deploy_ibkr/ recursively for single-strategy scripts,
groups them by symbol, lets the user pick which symbols (or
strategies) to run, then launches each script in its own
console window.

Usage:
    python 05_run_single_strategy.py
    python 05_run_single_strategy.py --list                  # show table, exit
    python 05_run_single_strategy.py --symbols QQQ UVXY      # skip prompt
    python 05_run_single_strategy.py --all                   # launch everything
"""

import os
import sys
import time
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
from collections import defaultdict


# =============================================================================
# PATHS
# =============================================================================

BASE_DIR    = Path(__file__).resolve().parent
DEPLOY_DIR  = BASE_DIR / "deploy_ibkr"

# Subfolders to skip during scan
SKIP_DIRS = {"logs", "__pycache__", ".git"}


# =============================================================================
# DISCOVERY
# =============================================================================

def extract_symbol(filename: str) -> str:
    """
    Parse the trading symbol from a strategy filename.
    Convention: {SYMBOL}_{feature}_{...}.py
    Returns the first underscore-delimited token, uppercased.
    """
    return filename.split("_")[0].upper()


def scan_strategies() -> dict[str, list[dict]]:
    """
    Recursively scan DEPLOY_DIR for *.py files.
    Returns a dict keyed by symbol, each value a sorted list of:
        { name, path, dir, symbol }
    """
    if not DEPLOY_DIR.exists():
        return {}

    by_symbol: dict[str, list[dict]] = defaultdict(list)

    for fpath in sorted(DEPLOY_DIR.rglob("*.py")):
        # Skip ignored directories
        if any(part in SKIP_DIRS for part in fpath.parts):
            continue
        # Skip __init__ and similar non-strategy files
        if fpath.stem.startswith("__"):
            continue

        symbol = extract_symbol(fpath.stem)
        by_symbol[symbol].append({
            "name":   fpath.stem,
            "path":   fpath,
            "dir":    fpath.parent,
            "symbol": symbol,
        })

    return dict(sorted(by_symbol.items()))


# =============================================================================
# DISPLAY
# =============================================================================

def print_banner():
    print("""
╔══════════════════════════════════════════════════════════════╗
║           Single Strategy Runner  ·  AQS100 DemoV3          ║
╚══════════════════════════════════════════════════════════════╝
""")


def print_symbol_table(by_symbol: dict[str, list[dict]]):
    if not by_symbol:
        print("  (no strategy scripts found)")
        return
    print(f"  {'#':<4}  {'Symbol':<12}  {'Strategies':<12}  {'Subfolder(s)'}")
    print(f"  {'-'*4}  {'-'*12}  {'-'*12}  {'-'*40}")
    for i, (symbol, strategies) in enumerate(by_symbol.items(), 1):
        # Collect unique relative subfolders
        folders = sorted({
            str(s["path"].parent.relative_to(DEPLOY_DIR)) for s in strategies
        })
        folders_str = ", ".join(folders) if folders else "."
        print(f"  {i:<4}  {symbol:<12}  {len(strategies):<12}  {folders_str}")
    print()


def print_strategy_list(strategies: list[dict]):
    """Print individual strategy filenames for a symbol."""
    for s in strategies:
        rel = s["path"].relative_to(BASE_DIR)
        print(f"      • {rel}")


# =============================================================================
# SELECTION
# =============================================================================

def prompt_selection(by_symbol: dict[str, list[dict]]) -> list[dict]:
    """
    Interactive symbol-level selection.
    Accepts:
        all          → every strategy across all symbols
        1 3 5        → space-separated row indices
        1,3,5        → comma-separated row indices
        QQQ UVXY     → symbol names (case-insensitive)
    Returns flat list of strategy dicts to launch.
    """
    symbols     = list(by_symbol.keys())
    n           = len(symbols)
    symbol_map  = {s.upper(): s for s in symbols}   # upper → canonical key

    print("  Select symbols to deploy:")
    print("    • 'all'          → every strategy in deploy_ibkr/")
    print("    • numbers        → e.g.  1 3   or   1,3")
    print(f"    • symbol names   → e.g.  QQQ UVXY")
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

        # Deduplicate preserving order
        seen, unique = set(), []
        for s in selected:
            if s["path"] not in seen:
                seen.add(s["path"])
                unique.append(s)

        return unique


# =============================================================================
# LAUNCH
# =============================================================================

def launch_strategy(strategy: dict) -> "subprocess.Popen | None":
    """
    Launch a single strategy script in its own console window.
    Returns the Popen handle, or None on failure.
    """
    script = strategy["path"]
    cwd    = strategy["dir"]
    name   = strategy["name"]

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
        print(f"  ✓  {name}  (PID {proc.pid})")
        return proc
    except Exception as e:
        print(f"  ✗  {name}  ERROR: {e}")
        return None


def launch_all(strategies: list[dict]) -> list["subprocess.Popen"]:
    """Launch all strategies; return list of active Popen handles."""
    print()
    print(f"  Launching {len(strategies)} strategy window(s)...\n")
    handles = []
    for s in strategies:
        h = launch_strategy(s)
        if h:
            handles.append(h)
    return handles


# =============================================================================
# MONITOR
# =============================================================================

def monitor(handles: list["subprocess.Popen"], strategies: list[dict]):
    """
    Park after launch.  Polls every 10 s for unexpected exits.
    Ctrl+C exits the launcher without killing strategy windows.
    """
    if not handles:
        return

    # Map pid -> strategy name for reporting
    pid_map = {h.pid: s["name"] for h, s in zip(handles, strategies)}
    reported = set()

    print()
    print("=" * 62)
    print(f"  {len(handles)} strategy window(s) running.")
    print()

    # Group launched strategies by symbol for a tidy summary
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
            if len(reported) == len(handles):
                print()
                print("  All strategy processes have exited.")
                break
    except KeyboardInterrupt:
        print("\n\n  Launcher exited.  Strategy processes continue running.")


# =============================================================================
# CONFIRM
# =============================================================================

def confirm_launch(strategies: list[dict]) -> bool:
    """Show what will be launched and ask for confirmation."""
    by_sym: dict[str, list[dict]] = defaultdict(list)
    for s in strategies:
        by_sym[s["symbol"]].append(s)

    total = len(strategies)
    print()
    print(f"  About to launch {total} strategy window(s):")
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
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Launch single strategies from deploy_ibkr/"
    )
    parser.add_argument("--list",    action="store_true",
                        help="List available strategies and exit")
    parser.add_argument("--symbols", nargs="+", metavar="SYM",
                        help="Symbols to launch (skips prompt)")
    parser.add_argument("--all",     action="store_true",
                        help="Launch all strategies (skips prompt)")
    args = parser.parse_args()

    print_banner()

    # ── discover ─────────────────────────────────────────────────────────────
    by_symbol = scan_strategies()

    if not by_symbol:
        print(f"  No strategy scripts found in:\n    {DEPLOY_DIR}")
        print("\n  Run generate_deployment_codes_v8.py first to generate strategies.")
        sys.exit(1)

    total_files = sum(len(v) for v in by_symbol.values())
    print(f"  Found {total_files} strateg{'y' if total_files == 1 else 'ies'} "
          f"across {len(by_symbol)} symbol(s) in deploy_ibkr/:\n")
    print_symbol_table(by_symbol)

    if args.list:
        sys.exit(0)

    # ── selection ─────────────────────────────────────────────────────────────
    if args.all:
        selected = [s for strats in by_symbol.values() for s in strats]
        print(f"  --all: deploying all {len(selected)} strategies.\n")

    elif args.symbols:
        sym_upper = {k.upper(): k for k in by_symbol}
        selected  = []
        for raw in args.symbols:
            key = raw.upper()
            if key in sym_upper:
                selected.extend(by_symbol[sym_upper[key]])
            else:
                print(f"  ✗ Symbol '{raw}' not found — skipping.")
        if not selected:
            print("  No valid symbols specified. Exiting.")
            sys.exit(1)
        print(f"  --symbols: deploying {', '.join(args.symbols)}.\n")

    else:
        selected = prompt_selection(by_symbol)

    if not selected:
        print("  No strategies selected. Exiting.")
        sys.exit(0)

    # ── confirm ───────────────────────────────────────────────────────────────
    if not confirm_launch(selected):
        print("  Cancelled.")
        sys.exit(0)

    # ── launch ────────────────────────────────────────────────────────────────
    handles = launch_all(selected)

    if not handles:
        print("\n  No strategies launched successfully.")
        sys.exit(1)

    # ── monitor ───────────────────────────────────────────────────────────────
    monitor(handles, selected)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Launcher cancelled.")
        sys.exit(0)
