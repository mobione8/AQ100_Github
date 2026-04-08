"""
Walk-Forward Validation for Grid Search Results

This script reads configurations from the Alpha_Short worksheet in the
Alpha_GS_Compilation Excel file and runs walk-forward validation on each.

Data source: GridSearch_Data/
Output: AQS_SFGridResults/{symbol_folder}/{feature}/{model}/{strategy}/walk_forward_report.csv
"""

import pandas as pd
import numpy as np
import os
from pathlib import Path
import sys
import glob
import argparse

# Add script directory to path to import util (script is now at project root)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from util_AQS_parallel import generate_all_signals, get_period_from_folder

# Configuration (defaults, can be overridden via command-line)
EXCHANGE = "ibkr"     # Exchange name (lowercase to match folder pattern)
INTERVAL = "1h"       # Time interval (e.g., "1h", "1d", "4h")

# Walk-forward parameters
IN_SAMPLE_RATIO = 0.50  # 50% for IS period
MAX_WARMUP_LENGTH = 300

# FIX: Interval-aware minimum row thresholds.
# Original code had MIN_DATA_LENGTH = 1200 hardcoded (only valid for intraday).
# Weekly data (~1000-1500 rows) and daily data will never hit 1200, so they were
# always skipped. Now each interval has its own sensible minimum.
INTERVAL_MIN_ROWS = {
    '1min':  1200,
    '5min':  1200,
    '15min': 1200,
    '30min': 1200,
    '1h':    1200,
    '4h':    1000,
    '1d':    500,
    '1w':    300,   # 300 weeks = ~6 years, still robust for walk-forward
}
MIN_DATA_LENGTH = 300  # fallback only - real check uses INTERVAL_MIN_ROWS

# Base directories
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = SCRIPT_DIR  # Script is now at project root
DATA_DIR = os.path.join(PROJECT_DIR, "GridSearch_Data")
RESULTS_DIR = os.path.join(PROJECT_DIR, "AQS_SFGridResults")


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Walk-forward validation for Grid Search strategies'
    )
    parser.add_argument('--exchange', type=str, default=EXCHANGE,
                        help=f'Exchange name (default: {EXCHANGE})')
    parser.add_argument('--interval', type=str, default=INTERVAL,
                        help=f'Time interval (default: {INTERVAL})')
    parser.add_argument('--symbols', type=str, default=None,
                        help='Comma-separated list of symbols to filter (default: all from Excel)')
    return parser.parse_args()


def find_latest_compilation_file(base_dir, exchange, interval):
    """
    Find the latest Alpha_GS_Compilation Excel file.

    Args:
        base_dir: Directory to search in
        exchange: Exchange name (e.g., "ibkr")
        interval: Time interval (e.g., "1h")

    Returns:
        str: Path to latest Excel file, or None if not found
    """
    pattern = os.path.join(base_dir, f"Alpha_GS_Compilation_{exchange}_{interval}_*.xlsx")
    files = glob.glob(pattern)

    if not files:
        return None

    # Return the most recently created file
    return max(files, key=os.path.getctime)


def load_configurations_from_excel(excel_path, symbols=None):
    """
    Load configurations from Alpha_Short worksheet.

    Args:
        excel_path: Path to Alpha_GS_Compilation Excel file
        symbols: Optional list of symbols to filter (None = all)

    Returns:
        pd.DataFrame: Configurations with columns: Symbol, feature, model, strategy,
                     length, entry_threshold, exit_threshold
    """
    print(f"Loading configurations from: {excel_path}")

    # Read Alpha_Short worksheet
    df = pd.read_excel(excel_path, sheet_name='Alpha_Short')
    print(f"  Total configurations in Alpha_Short: {len(df)}")

    # Rename columns to match walk-forward script conventions
    df = df.rename(columns={
        'Data Point': 'feature',
        'buy_type': 'strategy'
    })

    # Filter by symbols if specified
    if symbols:
        df = df[df['Symbol'].isin(symbols)]
        print(f"  After filtering by symbols {symbols}: {len(df)}")

    # Select only needed columns (including Variant if present)
    config_columns = ['Symbol', 'feature', 'model', 'strategy',
                      'length', 'entry_threshold', 'exit_threshold']

    # Add Variant column if it exists
    if 'Variant' in df.columns:
        config_columns.insert(1, 'Variant')  # After Symbol
        print(f"  Variant column found - will use for path construction")
    else:
        print(f"  No Variant column - using glob matching for paths")

    df = df[config_columns].copy()

    return df


def construct_symbol_paths(exchange, symbol, interval, variant=None):
    """
    Construct data and results paths for a given symbol.
    If variant is provided, constructs exact path. Otherwise uses glob pattern matching.

    Args:
        exchange: Exchange name (e.g., "ibkr")
        symbol: Base symbol (e.g., "VIXY")
        interval: Time interval (e.g., "1h", "1d")
        variant: Optional variant suffix (e.g., "john_23Jan2026")

    Returns:
        dict: Dictionary with 'data_path' and 'results_dir' keys
    """
    if variant:
        # Construct exact paths using variant
        data_path = os.path.join(DATA_DIR, f"merged_{exchange}_{symbol}_{interval}_{variant}.csv")
        results_dir = os.path.join(RESULTS_DIR, f"merged_{exchange}_{symbol}_{interval}_{variant}")
    else:
        # Use glob pattern matching for data files in GridSearch_Data
        data_pattern = os.path.join(DATA_DIR, f"merged_{exchange}_{symbol}_{interval}_*.csv")
        data_matches = glob.glob(data_pattern)

        # Use first match if found, otherwise return pattern for error handling
        data_path = data_matches[0] if data_matches else data_pattern

        # Use glob pattern matching for results directories in AQS_SFGridResults
        results_pattern = os.path.join(RESULTS_DIR, f"merged_{exchange}_{symbol}_{interval}_*")
        results_matches = glob.glob(results_pattern)

        # Use first match if found, otherwise return pattern for error handling
        results_dir = results_matches[0] if results_matches else results_pattern

    return {
        'data_path': data_path,
        'results_dir': results_dir
    }


def load_and_split_data_walk_forward(data_path, results_dir, in_sample_ratio=0.50, max_warmup_length=300, interval=None):
    """
    Load merged data and split into walk-forward windows with warmup periods.

    Args:
        data_path: Path to merged CSV file
        results_dir: Results directory path (used to extract interval)
        in_sample_ratio: Ratio for in-sample split (default 0.50)
        max_warmup_length: Maximum lookback period for indicators (default 300)

    Returns:
        tuple: (df_is, df_wf1_warmup, wf1_true_start_idx, df_wf2_warmup, wf2_true_start_idx, period)
            - df_is: In-sample data (50%)
            - df_wf1_warmup: WF1 warmup + data
            - wf1_true_start_idx: Index where true WF1 begins in df_wf1_warmup
            - df_wf2_warmup: WF2 warmup + data
            - wf2_true_start_idx: Index where true WF2 begins in df_wf2_warmup
            - period: Annualization period
    """
    print(f"\n{'='*80}")
    print(f"Loading data from: {data_path}")

    # Load data
    df = pd.read_csv(data_path)

    # Sort by time
    df = df.sort_values('datetime').reset_index(drop=True)

    total_rows = len(df)

    # FIX: Use interval-aware minimum row threshold
    min_rows = INTERVAL_MIN_ROWS.get(interval, MIN_DATA_LENGTH) if interval else MIN_DATA_LENGTH
    if total_rows < min_rows:
        raise ValueError(f"Dataset has {total_rows} rows, minimum required: {min_rows} for interval '{interval}'")

    # IS Period (0% to 50%)
    is_end = int(total_rows * in_sample_ratio)
    df_is = df.iloc[0:is_end].copy()

    # WF1 Period (25% to 75%)
    wf1_start = int(total_rows * 0.25)
    wf1_end = int(total_rows * 0.75)
    wf1_warmup_start = max(0, wf1_start - max_warmup_length)
    df_wf1_warmup = df.iloc[wf1_warmup_start:wf1_end].copy().reset_index(drop=True)
    wf1_true_start_idx = wf1_start - wf1_warmup_start

    # WF2 Period (50% to 100%)
    wf2_start = int(total_rows * 0.50)
    wf2_end = total_rows
    wf2_warmup_start = max(0, wf2_start - max_warmup_length)
    df_wf2_warmup = df.iloc[wf2_warmup_start:wf2_end].copy().reset_index(drop=True)
    wf2_true_start_idx = wf2_start - wf2_warmup_start

    # Calculate actual windows for reporting
    df_wf1_true = df.iloc[wf1_start:wf1_end].copy()
    df_wf2_true = df.iloc[wf2_start:wf2_end].copy()

    print(f"Total rows: {total_rows}")
    print(f"IS: rows 0-{is_end} ({len(df_is)} rows)")
    print(f"  Time range: {df_is['datetime'].min()} to {df_is['datetime'].max()}")
    print(f"WF1: rows {wf1_start}-{wf1_end} ({len(df_wf1_true)} rows), warmup from row {wf1_warmup_start}")
    print(f"  Warmup + WF1 total: {len(df_wf1_warmup)} rows")
    print(f"  WF1 starts at index: {wf1_true_start_idx} in warmup dataframe")
    print(f"  Time range: {df_wf1_true['datetime'].min()} to {df_wf1_true['datetime'].max()}")
    print(f"WF2: rows {wf2_start}-{wf2_end} ({len(df_wf2_true)} rows), warmup from row {wf2_warmup_start}")
    print(f"  Warmup + WF2 total: {len(df_wf2_warmup)} rows")
    print(f"  WF2 starts at index: {wf2_true_start_idx} in warmup dataframe")
    print(f"  Time range: {df_wf2_true['datetime'].min()} to {df_wf2_true['datetime'].max()}")

    # Get period from results directory name
    folder_name = os.path.basename(results_dir)
    period = get_period_from_folder(folder_name)
    print(f"Period for annualization: {period} (extracted from folder: {folder_name})")
    print(f"{'='*80}\n")

    return df_is, df_wf1_warmup, wf1_true_start_idx, df_wf2_warmup, wf2_true_start_idx, period


def recalculate_wf_metrics(df_backtest, wf_start_idx, period):
    """
    Recalculate walk-forward metrics after dropping warmup period rows.

    Args:
        df_backtest: Full backtest DataFrame (warmup + WF)
        wf_start_idx: Index where true WF begins
        period: Period for annualization

    Returns:
        dict: WF performance metrics without warmup contamination
    """
    # Drop warmup rows - only keep true WF window
    df_wf_only = df_backtest.iloc[wf_start_idx:].copy().reset_index(drop=True)

    # Recalculate metrics on WF window only
    try:
        # Calculate Sharpe Ratio
        sharpe_ratio = df_wf_only['pnl'].mean() / df_wf_only['pnl'].std() * np.sqrt(period) if df_wf_only['pnl'].std() != 0 else 0

        # Calculate Max Drawdown
        df_wf_only['cumulative_pnl'] = df_wf_only['pnl'].cumsum()
        df_wf_only['dd'] = df_wf_only['cumulative_pnl'] - df_wf_only['cumulative_pnl'].cummax()
        max_drawdown = df_wf_only['dd'].min() if not df_wf_only['dd'].empty else 0

        # Calculate Trade Count
        trade_count = df_wf_only['trade'].sum()

        # Calculate Annualized Return
        annualized_return = df_wf_only['pnl'].mean() * period

        # Calculate Calmar Ratio
        calmar_ratio = annualized_return / abs(max_drawdown) if max_drawdown != 0 else 0

        return {
            'Sharpe Ratio': sharpe_ratio,
            'Max Drawdown': max_drawdown,
            'Trade Count': trade_count,
            'Annualized Return': annualized_return,
            'Calmar Ratio': calmar_ratio
        }

    except Exception as e:
        print(f"  Error recalculating WF metrics: {e}")
        return {
            'Sharpe Ratio': np.nan,
            'Max Drawdown': np.nan,
            'Trade Count': np.nan,
            'Annualized Return': np.nan,
            'Calmar Ratio': np.nan
        }


def run_walk_forward_backtest(df_is, df_wf1_warmup, wf1_start_idx, df_wf2_warmup, wf2_start_idx,
                               feature, model, strategy, params, period):
    """
    Run backtest on IS, WF1, and WF2 periods with warmup.

    Args:
        df_is: In-sample DataFrame
        df_wf1_warmup: WF1 warmup + data DataFrame
        wf1_start_idx: Index where true WF1 begins in df_wf1_warmup
        df_wf2_warmup: WF2 warmup + data DataFrame
        wf2_start_idx: Index where true WF2 begins in df_wf2_warmup
        feature: Feature column name
        model: Normalization model
        strategy: Trading strategy
        params: Dict with keys: length, entry_threshold, exit_threshold
        period: Period for annualization

    Returns:
        dict: Results with IS, WF1, and WF2 metrics
    """
    results = {
        'feature': feature,
        'model': model,
        'buy_type': strategy,
        'length': params['length'],
        'entry_threshold': params['entry_threshold'],
        'exit_threshold': params['exit_threshold']
    }

    try:
        # In-Sample Backtest
        df_is_test = df_is.copy()
        df_is_test, log_is = generate_all_signals(
            df_is_test,
            model=model,
            buy_type=strategy,
            column=feature,
            length=int(params['length']),
            entry_threshold=params['entry_threshold'],
            exit_threshold=params['exit_threshold'],
            period=period
        )

        # Store IS metrics
        results['IS Sharpe Ratio'] = log_is.get('Sharpe Ratio', np.nan)
        results['IS Max Drawdown'] = log_is.get('Max Drawdown', np.nan)
        results['IS Trade Count'] = log_is.get('Trade Count', np.nan)
        results['IS Annualized Return'] = log_is.get('Annualized Return', np.nan)
        results['IS Calmar Ratio'] = log_is.get('Calmar Ratio', np.nan)

    except Exception as e:
        print(f"  Error in IS backtest: {e}")
        results['IS Sharpe Ratio'] = np.nan
        results['IS Max Drawdown'] = np.nan
        results['IS Trade Count'] = np.nan
        results['IS Annualized Return'] = np.nan
        results['IS Calmar Ratio'] = np.nan

    try:
        # WF1 Backtest with warmup period
        df_wf1_test = df_wf1_warmup.copy()
        df_wf1_test, log_wf1_full = generate_all_signals(
            df_wf1_test,
            model=model,
            buy_type=strategy,
            column=feature,
            length=int(params['length']),
            entry_threshold=params['entry_threshold'],
            exit_threshold=params['exit_threshold'],
            period=period
        )

        # Recalculate metrics using only true WF1 window (drop warmup rows)
        log_wf1 = recalculate_wf_metrics(df_wf1_test, wf1_start_idx, period)

        # Store WF1 metrics
        results['WF1 Sharpe Ratio'] = log_wf1.get('Sharpe Ratio', np.nan)
        results['WF1 Max Drawdown'] = log_wf1.get('Max Drawdown', np.nan)
        results['WF1 Trade Count'] = log_wf1.get('Trade Count', np.nan)
        results['WF1 Annualized Return'] = log_wf1.get('Annualized Return', np.nan)
        results['WF1 Calmar Ratio'] = log_wf1.get('Calmar Ratio', np.nan)

    except Exception as e:
        print(f"  Error in WF1 backtest: {e}")
        results['WF1 Sharpe Ratio'] = np.nan
        results['WF1 Max Drawdown'] = np.nan
        results['WF1 Trade Count'] = np.nan
        results['WF1 Annualized Return'] = np.nan
        results['WF1 Calmar Ratio'] = np.nan

    try:
        # WF2 Backtest with warmup period
        df_wf2_test = df_wf2_warmup.copy()
        df_wf2_test, log_wf2_full = generate_all_signals(
            df_wf2_test,
            model=model,
            buy_type=strategy,
            column=feature,
            length=int(params['length']),
            entry_threshold=params['entry_threshold'],
            exit_threshold=params['exit_threshold'],
            period=period
        )

        # Recalculate metrics using only true WF2 window (drop warmup rows)
        log_wf2 = recalculate_wf_metrics(df_wf2_test, wf2_start_idx, period)

        # Store WF2 metrics
        results['WF2 Sharpe Ratio'] = log_wf2.get('Sharpe Ratio', np.nan)
        results['WF2 Max Drawdown'] = log_wf2.get('Max Drawdown', np.nan)
        results['WF2 Trade Count'] = log_wf2.get('Trade Count', np.nan)
        results['WF2 Annualized Return'] = log_wf2.get('Annualized Return', np.nan)
        results['WF2 Calmar Ratio'] = log_wf2.get('Calmar Ratio', np.nan)

    except Exception as e:
        print(f"  Error in WF2 backtest: {e}")
        results['WF2 Sharpe Ratio'] = np.nan
        results['WF2 Max Drawdown'] = np.nan
        results['WF2 Trade Count'] = np.nan
        results['WF2 Annualized Return'] = np.nan
        results['WF2 Calmar Ratio'] = np.nan

    return results


def save_walk_forward_results(results_list, results_dir):
    """
    Save walk-forward results to appropriate strategy folders.
    Creates: {results_dir}/{feature}/{model}/{strategy}/walk_forward_report.csv

    Args:
        results_list: List of result dictionaries
        results_dir: Base results directory for the symbol
    """
    if not results_list:
        return

    df_results = pd.DataFrame(results_list)

    # Group by feature/model/strategy and save each group
    grouped = df_results.groupby(['feature', 'model', 'buy_type'])

    for (feature, model, strategy), group_df in grouped:
        output_dir = os.path.join(results_dir, feature, model, strategy)
        os.makedirs(output_dir, exist_ok=True)

        output_path = os.path.join(output_dir, 'walk_forward_report.csv')

        # Save the group (overwrite if exists)
        group_df.to_csv(output_path, index=False)
        print(f"  Saved: {output_path}")


def generate_summary_report(symbol_stats, symbol_errors, output_path):
    """
    Generate aggregated summary report for all symbols processed.

    Args:
        symbol_stats: Dict with symbol -> {processed, skipped}
        symbol_errors: Dict with symbol -> error_message
        output_path: Where to save summary CSV

    Returns:
        pd.DataFrame: Summary dataframe
    """
    summary_rows = []

    for symbol, stats in symbol_stats.items():
        processed = stats.get('processed', 0)
        skipped = stats.get('skipped', 0)
        total = processed + skipped

        summary_rows.append({
            'Symbol': symbol,
            'Configurations Processed': processed,
            'Configurations Skipped': skipped,
            'Total Configurations': total,
            'Success Rate (%)': (processed / total * 100) if total > 0 else 0.0,
            'Status': symbol_errors.get(symbol, 'Success')
        })

    df_summary = pd.DataFrame(summary_rows)
    df_summary.to_csv(output_path, index=False)
    print(f"\nSummary report saved to: {output_path}")

    return df_summary


def interactive_mode():
    """Interactive CLI for running Stage 2: Walk-Forward Validation"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║    Stage 2: Walk-Forward Validation - Interactive Mode       ║
║  Validate top configurations with rolling IS/OOS windows     ║
╚══════════════════════════════════════════════════════════════╝
    """)

    try:
        symbols_input = input("Enter symbols (comma-separated, e.g., NVDA, AAPL, TECL): ").strip()
        if not symbols_input:
            print("Symbols cannot be empty")
            return
        symbols = [s.strip().upper() for s in symbols_input.split(',')]

        print("\nAvailable intervals:")
        print("  1min, 5min, 15min, 30min, 1h, 1d, 1w")
        interval = input("Enter interval (default: 1h): ").strip().lower()
        if not interval or interval not in ['1min', '5min', '15min', '30min', '1h', '1d', '1w']:
            interval = '1h'
            print("  ✗ Invalid interval. Using default: 1h")

        print(f"\n{'='*60}")
        print("Configuration Summary:")
        print(f"  Symbols:     {', '.join(symbols)}")
        print(f"  Interval:    {interval}")
        print(f"  Exchange:    {EXCHANGE}")
        print(f"{'='*60}")

        proceed = input("\nProceed with execution? (y/n): ").strip().lower()
        if proceed != 'y':
            print("\nExecution cancelled")
            return

        sys.argv = [sys.argv[0], '--symbols', ','.join(symbols), '--interval', interval, '--exchange', EXCHANGE]
        main()

    except KeyboardInterrupt:
        print("\n\nCancelled by user")


def main():
    """
    Main function to orchestrate Grid Search walk-forward validation process.
    """
    # Parse command-line arguments
    args = parse_arguments()
    exchange = args.exchange
    interval = args.interval
    symbols_filter = [s.strip() for s in args.symbols.split(',')] if args.symbols else None

    print("\n" + "="*80)
    print("GRID SEARCH WALK-FORWARD VALIDATION")
    print("="*80)
    print(f"Exchange: {exchange}")
    print(f"Interval: {interval}")
    print(f"Data source: {DATA_DIR}")
    print(f"Results dir: {RESULTS_DIR}")
    if symbols_filter:
        print(f"Symbol filter: {', '.join(symbols_filter)}")
    else:
        print("Symbol filter: None (processing all symbols from Excel)")
    print("="*80)

    # Step 1: Find latest Alpha_GS_Compilation Excel file
    excel_path = find_latest_compilation_file(RESULTS_DIR, exchange, interval)

    if not excel_path:
        print(f"\nERROR: No Alpha_GS_Compilation_{exchange}_{interval}_*.xlsx found in {RESULTS_DIR}")
        return

    print(f"\nUsing compilation file: {excel_path}")

    # Step 2: Load configurations from Alpha_Short worksheet
    try:
        df_configs = load_configurations_from_excel(excel_path, symbols_filter)
    except Exception as e:
        print(f"\nERROR: Failed to load configurations: {e}")
        return

    if df_configs.empty:
        print("\nERROR: No configurations found in Alpha_Short worksheet")
        return

    # Check if Variant column exists
    has_variant = 'Variant' in df_configs.columns

    # Get unique (Symbol, Variant) combinations or just Symbols
    if has_variant:
        # Group by Symbol and Variant
        symbol_variants = df_configs[['Symbol', 'Variant']].drop_duplicates().values.tolist()
        print(f"\nSymbol-Variant combinations to process: {len(symbol_variants)}")
        for sv in symbol_variants:
            print(f"  {sv[0]} / {sv[1]}")
    else:
        # Just symbols (backward compatible)
        symbols = df_configs['Symbol'].unique().tolist()
        symbol_variants = [(s, None) for s in symbols]
        print(f"\nSymbols to process: {', '.join(symbols)}")

    print(f"Total configurations: {len(df_configs)}")

    # Initialize tracking dictionaries
    symbol_stats = {}
    symbol_errors = {}

    # Step 3: Process each symbol-variant combination
    for idx, (symbol, variant) in enumerate(symbol_variants, 1):
        # Create a key for tracking (include variant if present)
        tracking_key = f"{symbol}|{variant}" if variant else symbol

        print(f"\n{'='*80}")
        if variant:
            print(f"[{idx}/{len(symbol_variants)}] Processing: {symbol} / Variant: {variant}")
        else:
            print(f"[SYMBOL {idx}/{len(symbol_variants)}] Processing: {symbol}")
        print(f"{'='*80}")

        # Get configurations for this symbol (and variant if present)
        if has_variant:
            symbol_configs = df_configs[(df_configs['Symbol'] == symbol) & (df_configs['Variant'] == variant)]
        else:
            symbol_configs = df_configs[df_configs['Symbol'] == symbol]
        print(f"Configurations for {tracking_key}: {len(symbol_configs)}")

        # Initialize stats for this symbol-variant
        symbol_stats[tracking_key] = {'processed': 0, 'skipped': 0}

        # Construct paths for this symbol-variant
        paths = construct_symbol_paths(exchange, symbol, interval, variant)
        data_path = paths['data_path']
        results_dir = paths['results_dir']

        # Check if data file exists
        if not os.path.exists(data_path):
            error_msg = f"Data file not found: {data_path}"
            print(f"ERROR: {error_msg}")
            symbol_errors[tracking_key] = error_msg
            symbol_stats[tracking_key]['skipped'] = len(symbol_configs)
            continue

        # Check if results directory exists
        if not os.path.exists(results_dir):
            error_msg = f"Results directory not found: {results_dir}"
            print(f"ERROR: {error_msg}")
            symbol_errors[tracking_key] = error_msg
            symbol_stats[tracking_key]['skipped'] = len(symbol_configs)
            continue

        # Step 4: Load and split data for this symbol-variant
        try:
            df_is, df_wf1_warmup, wf1_start_idx, df_wf2_warmup, wf2_start_idx, period = \
                load_and_split_data_walk_forward(data_path, results_dir, IN_SAMPLE_RATIO, interval=interval)
        except ValueError as e:
            error_msg = f"Dataset validation failed: {str(e)}"
            print(f"ERROR: {error_msg}")
            symbol_errors[tracking_key] = error_msg
            symbol_stats[tracking_key]['skipped'] = len(symbol_configs)
            continue
        except Exception as e:
            error_msg = f"Failed to load data: {str(e)}"
            print(f"ERROR: {error_msg}")
            symbol_errors[tracking_key] = error_msg
            symbol_stats[tracking_key]['skipped'] = len(symbol_configs)
            continue

        # Step 5: Process each configuration for this symbol
        results_list = []

        for config_idx, (_, config_row) in enumerate(symbol_configs.iterrows(), 1):
            feature = config_row['feature']
            model = config_row['model']
            strategy = config_row['strategy']
            params = {
                'length': config_row['length'],
                'entry_threshold': config_row['entry_threshold'],
                'exit_threshold': config_row['exit_threshold']
            }

            print(f"\n  [{config_idx}/{len(symbol_configs)}] {feature}/{model}/{strategy}")
            print(f"    length={params['length']}, entry={params['entry_threshold']:.3f}, exit={params['exit_threshold']:.3f}")

            try:
                result = run_walk_forward_backtest(
                    df_is, df_wf1_warmup, wf1_start_idx, df_wf2_warmup, wf2_start_idx,
                    feature, model, strategy, params, period
                )
                results_list.append(result)
                symbol_stats[tracking_key]['processed'] += 1

                print(f"    IS Sharpe: {result.get('IS Sharpe Ratio', np.nan):.3f}, " +
                      f"WF1 Sharpe: {result.get('WF1 Sharpe Ratio', np.nan):.3f}, " +
                      f"WF2 Sharpe: {result.get('WF2 Sharpe Ratio', np.nan):.3f}")

            except Exception as e:
                print(f"    ERROR: {e}")
                symbol_stats[tracking_key]['skipped'] += 1

        # Step 6: Save results to strategy folders
        if results_list:
            print(f"\nSaving results for {tracking_key}...")
            save_walk_forward_results(results_list, results_dir)

        # Print symbol-variant summary
        print(f"\n{tracking_key} Summary:")
        print(f"  Processed: {symbol_stats[tracking_key]['processed']}")
        print(f"  Skipped: {symbol_stats[tracking_key]['skipped']}")

    # Step 7: Generate and print multi-symbol summary
    print("\n" + "="*80)
    print("GRID SEARCH WALK-FORWARD VALIDATION COMPLETE")
    print("="*80)

    # Generate summary report
    summary_path = os.path.join(RESULTS_DIR, f"gs_walk_forward_summary_{exchange}_{interval}.csv")

    if symbol_stats:
        df_summary = generate_summary_report(symbol_stats, symbol_errors, summary_path)

        print("\nPER-SYMBOL SUMMARY:")
        print(df_summary.to_string(index=False))

        # Calculate grand totals
        total_processed = df_summary['Configurations Processed'].sum()
        total_skipped = df_summary['Configurations Skipped'].sum()
        total_configs = total_processed + total_skipped
        symbols_success = len([s for s in symbol_stats if s not in symbol_errors])
        symbols_failed = len(symbol_errors)

        print(f"\nGRAND TOTALS:")
        print(f"  Total Symbols Processed Successfully: {symbols_success}")
        print(f"  Total Symbols with Errors: {symbols_failed}")
        print(f"  Total Configurations Processed: {total_processed}")
        print(f"  Total Configurations Skipped: {total_skipped}")
        if total_configs > 0:
            print(f"  Overall Success Rate: {(total_processed / total_configs * 100):.1f}%")

        print(f"\nOutput files saved as 'walk_forward_report.csv' in each strategy folder")
        print(f"Summary report saved to: {summary_path}")
    else:
        print("No symbols were processed successfully.")

    print("="*80 + "\n")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        interactive_mode()
    else:
        main()
