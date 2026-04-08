"""
Generate WF Alpha Results from WF_Short Configurations (Grid Search)

This script reads validated strategy configurations from the WF_Short worksheet
in AQS_SFGridResults and generates complete backtest outputs.

Uses glob pattern matching to find data files: merged_{exchange}_{symbol}_{interval}_*

Output structure:
WFAlphaResults/
└── merged_{exchange}_{symbol}_{interval}_linear/
    └── {feature}/
        └── {model}/
            └── {strategy}/
                ├── backtest.csv           # Full timeseries backtest
                ├── metrics.csv            # Performance summary
                ├── summary.csv            # WF_Short configuration row
                └── cumu_pnl_vs_bnh.png   # Equity curve visualization
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
import sys
import glob
import argparse

# Import utility functions
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from util_AQS_parallel import generate_all_signals, get_period_for_interval, config, get_range
from overfit_detector import OverfitDetector

# ============================================================================
# DIRECTORY PATHS
# ============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = SCRIPT_DIR  # Script is in project root
DATA_DIR = os.path.join(PROJECT_DIR, "GridSearch_Data")
RESULTS_DIR = os.path.join(PROJECT_DIR, "AQS_SFGridResults")
OUTPUT_DIR = os.path.join(PROJECT_DIR, "WFAlphaResults")

# ============================================================================
# CONFIGURATION (defaults, can be overridden via command-line)
# ============================================================================
EXCHANGE = "ibkr"
SYMBOLS = ["VIXY", "VIXM"]
INTERVAL = "1h"
WORKSHEET_NAME = "WF_Short"


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='Generate WF alpha results from WF_Short configurations')
    parser.add_argument('--exchange', type=str, default=EXCHANGE,
                        help=f'Exchange name (default: {EXCHANGE})')
    parser.add_argument('--interval', type=str, default=INTERVAL,
                        help=f'Time interval (default: {INTERVAL})')
    parser.add_argument('--symbols', type=str, default=','.join(SYMBOLS),
                        help=f'Comma-separated list of symbols (default: {",".join(SYMBOLS)})')
    return parser.parse_args()

# Processing options
USE_PARALLEL = False           # Future: Set True for parallel processing

# ============================================================================
# HEATMAP CONFIGURATION
# ============================================================================
GENERATE_HEATMAPS = True           # Enable/disable heatmap generation
SAVE_GRID_RESULTS = True           # Save grid_results.csv alongside heatmap
HEATMAP_FIGSIZE = (20, 20)
HEATMAP_DPI = 100
HEATMAP_CMAP = 'Greens'

# ==============================================================================================
# OVERFITTING DETECTION CONFIGURATION (Disabled by default. Feature currently under development)
# ==============================================================================================
DETECT_OVERFITTING = False          # Enable/disable overfitting detection
# Consistent with AQS_SFGrid_parallel.py pct_min_trade_count_threshold = 0.015
OVERFIT_MIN_TRADES_PCT = 0.015     # 1.5% of data rows as minimum trades threshold
OVERFIT_WEIGHTS = {
    'absolute': 0.25,
    'relative': 0.25,
    'worst_case': 0.20,
    'cv': 0.30
}

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def log_error(message, error_log_path):
    """
    Log error message to file with timestamp.

    Args:
        message: Error message to log
        error_log_path: Path to error log file
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"

    # Create directory if needed
    os.makedirs(os.path.dirname(error_log_path), exist_ok=True)

    # Append to log file
    with open(error_log_path, 'a', encoding='utf-8') as f:
        f.write(log_entry)

    # Also print to console
    print(f"  [ERROR] {message}")


def find_latest_compilation_file(results_dir, exchange, interval):
    """
    Find the latest WF_GS_Compilation file by date in filename.

    Args:
        results_dir: Directory containing compilation files
        exchange: Exchange name (e.g., "ibkr")
        interval: Time interval (e.g., "1h")

    Returns:
        Path to latest file, or None if not found
    """
    pattern = os.path.join(results_dir, f'WF_GS_Compilation_{exchange}_{interval}_*.xlsx')
    matches = glob.glob(pattern)

    if not matches:
        return None

    # Sort by filename (date is YYYYMMDD format, so lexicographic sort works)
    matches.sort(reverse=True)
    return matches[0]


def load_wf_short(input_file, worksheet_name, exchange=None, symbols=None, interval=None):
    """
    Load and filter WF_Short configurations from Excel.

    Args:
        input_file: Path to Excel file
        worksheet_name: Name of worksheet to read
        exchange: Filter by exchange (None = all)
        symbols: List of symbols to filter (None = all)
        interval: Filter by interval (None = all)

    Returns:
        DataFrame with filtered WF_Short configurations
    """
    print("\n" + "=" * 80)
    print("LOADING WF_SHORT CONFIGURATIONS")
    print("=" * 80)
    print(f"Input file: {input_file}")
    print(f"Worksheet: {worksheet_name}")

    # Read Excel file
    try:
        df = pd.read_excel(input_file, sheet_name=worksheet_name)
        print(f"Loaded {len(df)} configurations from worksheet")
    except Exception as e:
        print(f"ERROR: Failed to read Excel file: {e}")
        return pd.DataFrame()

    # Apply filters
    original_count = len(df)

    if exchange is not None:
        df = df[df['Exchange'] == exchange]
        print(f"Filtered by exchange '{exchange}': {len(df)} rows")

    if symbols is not None:
        # For IBKR, symbols are already without USDT (e.g., "MBT", "MET")
        # For Binance, symbols in Excel have USDT (e.g., "BTCUSDT")
        df = df[df['Symbol'].isin(symbols)]
        print(f"Filtered by symbols {symbols}: {len(df)} rows")

    if interval is not None:
        df = df[df['Interval'] == interval]
        print(f"Filtered by interval '{interval}': {len(df)} rows")

    print(f"\nFinal dataset: {len(df)} configurations (filtered from {original_count})")
    print("=" * 80)

    return df.reset_index(drop=True)


def construct_csv_path(exchange, symbol, interval, data_dir, variant=None):
    """
    Construct path to merged CSV file.
    If variant is provided, constructs exact path. Otherwise uses glob pattern matching.

    Args:
        exchange: Exchange name
        symbol: Symbol (e.g., "MBT")
        interval: Time interval
        data_dir: Base data directory
        variant: Optional variant suffix (e.g., "john_23Jan2026")

    Returns:
        Path to CSV file (exact or first glob match), or pattern if no match
    """
    if variant:
        # Construct exact path using variant
        csv_path = os.path.join(data_dir, f"merged_{exchange}_{symbol}_{interval}_{variant}.csv")
        return csv_path
    else:
        # Use glob pattern to find matching data files
        pattern = f"merged_{exchange}_{symbol}_{interval}_*.csv"
        pattern_path = os.path.join(data_dir, pattern)
        matches = glob.glob(pattern_path)

        # Return first match if found, otherwise return pattern for error handling
        return matches[0] if matches else pattern_path


def construct_output_path(exchange, symbol, interval, feature, model, strategy, output_dir, variant=None):
    """
    Construct output directory path matching WFAlphaResults structure.

    Args:
        exchange: Exchange name
        symbol: Symbol (e.g., "MBT")
        interval: Time interval
        feature: Feature/Data Point name
        model: Model/transformation name
        strategy: Entry/Exit model name
        output_dir: Base output directory
        variant: Optional variant suffix (e.g., "john_23Jan2026"). If None, uses 'default'.

    Returns:
        Path object for output directory
    """
    # Use dynamic variant instead of hardcoded 'linear'
    variant_suffix = variant if variant else 'default'
    folder_name = f"merged_{exchange}_{symbol}_{interval}_{variant_suffix}"
    path = Path(output_dir) / folder_name / feature / model / strategy
    return path


def save_backtest_csv(df, output_path):
    """
    Save backtest timeseries to CSV.

    Args:
        df: DataFrame with backtest results
        output_path: Path to save CSV
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)


def save_metrics_csv(metrics, output_path):
    """
    Save performance metrics to CSV.

    Args:
        metrics: Dictionary of metric name -> value
        output_path: Path to save CSV
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Convert to DataFrame
    df_metrics = pd.DataFrame([
        {'Metric': k, 'Value': v} for k, v in metrics.items()
    ])

    df_metrics.to_csv(output_path, index=False)


def save_summary_csv(config_row, output_path):
    """
    Save WF_Short configuration row to CSV.

    Args:
        config_row: Series with WF_Short configuration
        output_path: Path to save CSV
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Convert Series to DataFrame (single row)
    df_summary = pd.DataFrame([config_row])

    df_summary.to_csv(output_path, index=False)


def plot_equity_curve(df, output_path):
    """
    Generate equity curve visualization (Cumulative PnL vs Buy-and-Hold).

    Args:
        df: DataFrame with cumulative_pnl and bnh columns
        output_path: Path to save PNG
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    plt.figure(figsize=(12, 6))

    # Plot cumulative PnL and buy-and-hold
    plt.plot(df.index, df['cumulative_pnl'], label='Strategy P&L', linewidth=2)
    plt.plot(df.index, df['bnh'], label='Buy & Hold', linewidth=2, linestyle='--')

    plt.xlabel('Time Index')
    plt.ylabel('Cumulative Return')
    plt.title('Cumulative P&L vs Buy-and-Hold')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()


# ============================================================================
# FULL-PERIOD HEATMAP FUNCTIONS
# ============================================================================

def backtest_single_combo_full_period(df_data, model, strategy, column, length, entry_threshold, period):
    """
    Run single backtest for one parameter combination on full data.

    Args:
        df_data: DataFrame with full dataset
        model: Model name (e.g., 'zscore', 'robust_scaler')
        strategy: Strategy type (e.g., 'trend_long', 'mr')
        column: Feature column name
        length: Length parameter
        entry_threshold: Entry threshold parameter
        period: Annualization period

    Returns:
        Dictionary with results or None if failed
    """
    try:
        df_backtest = df_data.copy()
        df_backtest, log_backtest = generate_all_signals(
            df_backtest,
            model=model,
            buy_type=strategy,
            column=column,
            length=int(length),
            entry_threshold=entry_threshold,
            exit_threshold=0,
            period=period
        )
        return {
            'length': int(length),
            'entry_threshold': entry_threshold,
            'Sharpe Ratio': log_backtest.get('Sharpe Ratio', np.nan),
            'Annualized Return': log_backtest.get('Annualized Return', np.nan),
            'Max Drawdown': log_backtest.get('Max Drawdown', np.nan),
            'Trade Count': log_backtest.get('Trade Count', np.nan),
        }
    except Exception:
        return None


def run_full_period_grid_search(df, model, strategy, column, period):
    """
    Run grid search for all length × entry combinations on full data.

    Args:
        df: DataFrame with full dataset
        model: Model name (must exist in config['models'])
        strategy: Strategy type
        column: Feature column name
        period: Annualization period

    Returns:
        DataFrame with grid search results or None if model not in config
    """
    if model not in config['models']:
        print(f"  [WARNING] Model '{model}' not in config, skipping heatmap")
        return None

    # Get parameter ranges for this model
    lengths = get_range(*config['models'][model]['param1'])
    entry_thresholds = get_range(*config['models'][model]['param2'])

    total_combos = len(lengths) * len(entry_thresholds)
    print(f"  [INFO] Running full-period grid search: {total_combos} combinations")

    results = []
    for length in lengths:
        for entry_threshold in entry_thresholds:
            result = backtest_single_combo_full_period(
                df, model, strategy, column, length, entry_threshold, period
            )
            if result:
                results.append(result)

    if not results:
        return None

    return pd.DataFrame(results)


def generate_sharpe_heatmap(results_df, model, strategy, output_path):
    """
    Generate and save Sharpe Ratio heatmap.

    Args:
        results_df: DataFrame with grid search results (length, entry_threshold, Sharpe Ratio)
        model: Model name for title
        strategy: Strategy name for title
        output_path: Path to save heatmap.png

    Returns:
        True if successful, False if failed
    """
    try:
        pivot_table = results_df.pivot_table(
            index='length',
            columns='entry_threshold',
            values='Sharpe Ratio'
        )

        fig, ax = plt.subplots(figsize=HEATMAP_FIGSIZE)
        sns.heatmap(pivot_table, annot=False, fmt=".1f", cmap=HEATMAP_CMAP, ax=ax)
        ax.invert_yaxis()  # Y-axis ascending from bottom to top
        plt.title(f'Full Period: {model} - {strategy} - Sharpe Ratio Heatmap')
        plt.xlabel('Entry Threshold')
        plt.ylabel('Length')
        plt.tight_layout()
        plt.savefig(output_path, dpi=HEATMAP_DPI, bbox_inches='tight')
        plt.close(fig)
        plt.clf()
        return True
    except Exception as e:
        print(f"  [WARNING] Heatmap generation failed: {e}")
        plt.close('all')
        return False


def run_overfitting_analysis(grid_results_path, output_dir, total_rows):
    """
    Run overfitting detection on grid results.

    Args:
        grid_results_path: Path to grid_results.csv
        output_dir: Directory to save overfitting outputs
        total_rows: Total number of data rows (for dynamic min_trades calculation)

    Returns:
        Tuple of (composite_final score for best params, success boolean)
    """
    try:
        # Calculate min_trades as 1.5% of total rows (consistent with AQS_SFGrid_parallel)
        min_trades = int(OVERFIT_MIN_TRADES_PCT * total_rows)

        detector = OverfitDetector(
            min_trades=min_trades,
            weights=OVERFIT_WEIGHTS,
            col_length='length',
            col_entry='entry_threshold',
            col_sharpe='Sharpe Ratio',
            col_trades='Trade Count'
        )

        detector.load_data(str(grid_results_path))
        results = detector.analyze()

        # Save scores
        scores_path = output_dir / 'overfitting_scores.csv'
        detector.save_results(str(scores_path))

        # Generate robustness heatmap
        heatmap_path = output_dir / 'robustness_heatmap.png'
        detector.generate_heatmap(str(heatmap_path))

        # Get score for the best Sharpe (row with highest sharpe_ratio)
        best_sharpe_row = results.loc[results['sharpe_ratio'].idxmax()]
        best_overfit_score = best_sharpe_row['composite_final']

        return best_overfit_score, True

    except Exception as e:
        print(f"  [WARNING] Overfitting analysis failed: {e}")
        return np.nan, False


def process_strategy(row, data_dir, output_dir, error_log_path):
    """
    Process a single strategy configuration and generate all outputs.

    Args:
        row: Series with WF_Short configuration
        data_dir: Base data directory
        output_dir: Base output directory
        error_log_path: Path to error log file

    Returns:
        True if successful, False if error
    """
    # Extract configuration
    exchange = row['Exchange'].lower()
    symbol = row['Symbol']
    interval = row['Interval']
    # Get variant from row if present, otherwise None (for backward compatibility)
    variant = row.get('Variant', None) if 'Variant' in row.index else None

    # Get annualization period dynamically based on interval
    period = get_period_for_interval(interval)
    feature = row['Data Point']
    model = row['Model']
    strategy = row['Entry / Exit Model']
    length = int(row['Length']) if pd.notna(row['Length']) else None
    entry_threshold = row['Entry']
    exit_threshold = row['Exit']

    # Construct paths (using variant for exact path matching)
    csv_path = construct_csv_path(exchange, symbol, interval, data_dir, variant)
    output_path = construct_output_path(exchange, symbol, interval, feature, model, strategy, output_dir, variant)

    # Check if CSV exists
    if not os.path.exists(csv_path):
        error_msg = f"ERROR: Missing CSV file: {csv_path}"
        log_error(error_msg, error_log_path)
        return False

    try:
        # Load data
        df = pd.read_csv(csv_path)
        print(f"  [OK] Loaded data: {len(df)} rows")

        # Check if feature column exists
        if feature not in df.columns:
            error_msg = f"ERROR: Feature '{feature}' not found in CSV columns"
            log_error(error_msg, error_log_path)
            return False

        # Generate backtest
        df_backtest, metrics = generate_all_signals(
            df=df.copy(),
            model=model,
            buy_type=strategy,
            column=feature,
            length=length,
            entry_threshold=entry_threshold,
            exit_threshold=exit_threshold,
            period=period
        )
        print(f"  [OK] Generated backtest")

        # Add final cumulative values to metrics
        if 'cumulative_pnl' in df_backtest.columns:
            metrics['Final Cumulative PnL'] = df_backtest['cumulative_pnl'].iloc[-1]
        if 'bnh' in df_backtest.columns:
            metrics['Final Buy & Hold'] = df_backtest['bnh'].iloc[-1]
            if metrics['Final Buy & Hold'] != 0:
                metrics['PnL Ratio'] = metrics['Final Cumulative PnL'] / metrics['Final Buy & Hold']

        # Save outputs
        backtest_csv = output_path / 'backtest.csv'
        metrics_csv = output_path / 'metrics.csv'
        summary_csv = output_path / 'summary.csv'
        equity_png = output_path / 'cumu_pnl_vs_bnh.png'

        save_backtest_csv(df_backtest, backtest_csv)
        save_metrics_csv(metrics, metrics_csv)
        save_summary_csv(row, summary_csv)
        plot_equity_curve(df_backtest, equity_png)

        print(f"  [OK] Saved to: {output_path}")

        # Generate full-period heatmap
        if GENERATE_HEATMAPS:
            try:
                heatmap_path = output_path / 'heatmap.png'

                grid_results = run_full_period_grid_search(
                    df=df.copy(),
                    model=model,
                    strategy=strategy,
                    column=feature,
                    period=period
                )

                if grid_results is not None and not grid_results.empty:
                    if SAVE_GRID_RESULTS:
                        grid_results.to_csv(output_path / 'grid_results.csv', index=False)

                    success = generate_sharpe_heatmap(grid_results, model, strategy, heatmap_path)
                    if success:
                        print(f"  [OK] Generated heatmap ({len(grid_results)} combinations)")

                    # Run overfitting detection
                    if DETECT_OVERFITTING:
                        try:
                            overfit_score, overfit_success = run_overfitting_analysis(
                                output_path / 'grid_results.csv',
                                output_path,
                                total_rows=len(df)
                            )
                            if overfit_success:
                                print(f"  [OK] Overfitting analysis complete (best param score: {overfit_score:.3f})")
                        except Exception as e:
                            log_error(f"Overfitting detection failed: {str(e)}", error_log_path)
                else:
                    print(f"  [WARNING] No valid grid results for heatmap")

            except Exception as e:
                log_error(f"Heatmap generation failed: {str(e)}", error_log_path)

        return True

    except Exception as e:
        error_msg = f"ERROR: Backtest generation failed: {str(e)}"
        log_error(error_msg, error_log_path)
        return False


def interactive_mode():
    """Interactive CLI for running Stage 4: WF Alpha Generation"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║     Stage 4: WF Alpha Generation - Interactive Mode          ║
║  Regenerate full backtests for validated strategies           ║
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
    Main function to orchestrate WF alpha results generation.
    """
    # Parse command-line arguments
    args = parse_arguments()
    exchange = args.exchange
    interval = args.interval
    symbols = [s.strip() for s in args.symbols.split(',')]

    # Find latest compilation file
    input_file = find_latest_compilation_file(RESULTS_DIR, exchange, interval)
    error_log_file = os.path.join(OUTPUT_DIR, "error_log.txt")

    if input_file is None:
        print(f"\nERROR: No WF_GS_Compilation_{exchange}_{interval}_*.xlsx found in {RESULTS_DIR}")
        return

    print("\n" + "=" * 80)
    print("WF ALPHA RESULTS GENERATOR (Grid Search)")
    print("=" * 80)
    print(f"Exchange: {exchange}")
    print(f"Interval: {interval}")
    print(f"Symbols: {', '.join(symbols)}")
    print(f"Data directory: {DATA_DIR}")
    print(f"Output directory: {OUTPUT_DIR}")
    print("=" * 80)

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Initialize error log
    if os.path.exists(error_log_file):
        os.remove(error_log_file)

    # Load WF_Short configurations
    df_wf_short = load_wf_short(
        input_file=input_file,
        worksheet_name=WORKSHEET_NAME,
        exchange=exchange,
        symbols=symbols,
        interval=interval
    )

    if df_wf_short.empty:
        print("\nERROR: No configurations to process!")
        return

    # Process each strategy
    total = len(df_wf_short)
    successful = 0
    failed = 0

    print("\n" + "=" * 80)
    print("PROCESSING STRATEGIES")
    print("=" * 80)

    for idx, row in df_wf_short.iterrows():
        symbol = row['Symbol']
        feature = row['Data Point']
        model = row['Model']
        strategy = row['Entry / Exit Model']

        print(f"\nProcessing {idx+1}/{total}: {symbol} - {feature} - {model} - {strategy}")

        success = process_strategy(
            row=row,
            data_dir=DATA_DIR,
            output_dir=OUTPUT_DIR,
            error_log_path=error_log_file
        )

        if success:
            successful += 1
        else:
            failed += 1

    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total strategies: {total}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")

    if failed > 0:
        print(f"\nSee error details in: {error_log_file}")

    print("=" * 80)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        interactive_mode()
    else:
        main()
