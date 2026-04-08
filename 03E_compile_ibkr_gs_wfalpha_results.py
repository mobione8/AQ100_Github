"""
Compile WF Alpha Results (Multi-Symbol Support)

This script scans WFAlphaResults folders, extracts validated alpha configurations from
summary.csv and metrics.csv files, and compiles them into a single Excel file with
one worksheet (WF_Short) containing full-period performance metrics.

Output: WFAlpha_Compilation_{exchange}_{interval}_{YYYYMMDD}.xlsx
"""

import pandas as pd
import numpy as np
import os
import sys
from pathlib import Path
import argparse
import glob
from datetime import datetime

# Directory paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = SCRIPT_DIR  # Script is in project root
OUTPUT_DIR = os.path.join(PROJECT_DIR, "WFAlphaResults")

# Configuration
EXCHANGE = "ibkr"
INTERVAL = "1h"
SYMBOLS = ["VIXY", "VIXM"]  # Base symbols

def construct_folder_paths(exchange, symbol, interval, base_dir):
    """
    Find all folder paths using glob pattern matching.

    Args:
        exchange: Exchange name (e.g., "ibkr")
        symbol: Symbol (e.g., "MBT")
        interval: Interval (e.g., "1h", "1d")
        base_dir: Base directory (WFAlphaResults)

    Returns:
        List of (folder_path, variant) tuples, or empty list if not found
    """
    # FIX: map short-form intervals to long-form folder names using exact match
    interval_map = {"15m": "15min", "30m": "30min"}
    interval_folder = interval_map.get(interval, interval)
    pattern = f"merged_{exchange}_{symbol}_{interval_folder}_*"
    pattern_path = os.path.join(base_dir, pattern)
    matches = glob.glob(pattern_path)

    results = []
    for folder_path in matches:
        # Extract variant from folder name
        folder_name = os.path.basename(folder_path)
        prefix = f"merged_{exchange}_{symbol}_{interval}_"
        variant = folder_name[len(prefix):] if folder_name.startswith(prefix) else 'default'
        results.append((folder_path, variant))

    return results


def extract_alpha_from_summary(summary_path, symbol, exchange, interval, variant=None):
    """
    Extract alpha configuration and metrics from summary.csv file.

    Args:
        summary_path: Path to summary.csv file
        symbol: Symbol (e.g., "MBT")
        exchange: Exchange name
        interval: Time interval
        variant: Variant suffix (e.g., "john_23Jan2026")

    Returns:
        dict with alpha details or None if error
    """
    try:
        # Read summary.csv
        df = pd.read_csv(summary_path)

        if df.empty:
            return None

        # Get first (and only) row
        row = df.iloc[0]

        # Parse path to extract feature, model, and strategy
        parts = Path(summary_path).parts
        feature = parts[-4]  # feature folder name
        model = parts[-3]    # model/transform folder name
        strategy = parts[-2]  # strategy folder name

        # Read metrics.csv for full-period performance metrics
        metrics_path = os.path.join(os.path.dirname(summary_path), 'metrics.csv')
        cumulative_pnl = None
        bnh = None
        pnl_ratio = None
        sharpe_ratio = None
        max_drawdown = None
        trade_count = None
        annualized_return = None
        calmar_ratio = None

        if os.path.exists(metrics_path):
            try:
                df_metrics = pd.read_csv(metrics_path)
                # Extract all metrics
                for idx, metric_row in df_metrics.iterrows():
                    metric_name = metric_row['Metric']
                    if metric_name == 'Final Cumulative PnL':
                        cumulative_pnl = metric_row['Value']
                    elif metric_name == 'Final Buy & Hold':
                        bnh = metric_row['Value']
                    elif metric_name == 'PnL Ratio':
                        pnl_ratio = metric_row['Value']
                    elif metric_name == 'Sharpe Ratio':
                        sharpe_ratio = metric_row['Value']
                    elif metric_name == 'Max Drawdown':
                        max_drawdown = metric_row['Value']
                    elif metric_name == 'Trade Count':
                        trade_count = int(metric_row['Value']) if pd.notna(metric_row['Value']) else None
                    elif metric_name == 'Annualized Return':
                        annualized_return = metric_row['Value']
                    elif metric_name == 'Calmar Ratio':
                        calmar_ratio = metric_row['Value']
            except Exception as e:
                print(f"  WARNING: Error reading metrics.csv: {e}")

        # Read overfitting_scores.csv if exists
        overfit_scores_path = os.path.join(os.path.dirname(summary_path), 'overfitting_scores.csv')
        composite_1hop = None
        composite_2hop = None
        composite_final = None

        if os.path.exists(overfit_scores_path):
            try:
                df_overfit = pd.read_csv(overfit_scores_path)
                # Match by Length and Entry threshold
                length_val = int(row['Length']) if pd.notna(row['Length']) else None
                entry_val = row['Entry']

                if length_val is not None and entry_val is not None:
                    # Find matching row
                    mask = (df_overfit['length'] == length_val) & \
                           (np.isclose(df_overfit['entry_threshold'], entry_val, atol=1e-6))
                    matched = df_overfit[mask]

                    if not matched.empty:
                        overfit_row = matched.iloc[0]
                        composite_1hop = overfit_row.get('composite_1hop', None)
                        composite_2hop = overfit_row.get('composite_2hop', None)
                        composite_final = overfit_row.get('composite_final', None)
            except Exception as e:
                print(f"  WARNING: Error reading overfitting_scores.csv: {e}")

        # Build result dictionary with full-period metrics from metrics.csv
        return {
            # Basic metadata
            'Exchange': exchange,
            'Symbol': symbol,
            'Interval': interval,
            'Variant': variant if variant else 'default',
            'Data Point': feature,
            'Model': model,
            'Entry / Exit Model': strategy,
            'Length': int(row['Length']) if pd.notna(row['Length']) else None,
            'Entry': row['Entry'],
            'Exit': row['Exit'],

            # Full-period performance metrics (from metrics.csv)
            'Sharpe': sharpe_ratio,
            'MDD': max_drawdown,
            'Trade Count': trade_count,
            'Annual Return': annualized_return,
            'Calmar Ratio': calmar_ratio,
            'Cumulative PnL': cumulative_pnl,
            'Buy & Hold': bnh,
            'PnL Ratio': pnl_ratio,

            # Overfitting scores (from overfitting_scores.csv if available)
            'Overfit_1hop': composite_1hop,
            'Overfit_2hop': composite_2hop,
            'Overfit_Final': composite_final
        }

    except Exception as e:
        print(f"  WARNING: Error processing {summary_path}: {e}")
        return None


def process_symbol(symbol_base, exchange, interval, base_dir):
    """
    Process a single symbol and return its compiled alphas.
    Handles multiple variant folders.

    Args:
        symbol_base: Symbol (e.g., "MBT")
        exchange: Exchange name (e.g., "ibkr")
        interval: Interval (e.g., "1h")
        base_dir: Base directory containing WFAlphaResults folders

    Returns:
        tuple: (DataFrame with results, dict with statistics)
    """
    symbol = symbol_base  # No USDT suffix for IBKR
    folder_variants = construct_folder_paths(exchange, symbol, interval, base_dir)

    print(f"\nProcessing {symbol}...")
    print("-" * 80)

    # Check if any folders were found
    if not folder_variants:
        error_msg = f"No folder found matching pattern for {symbol}"
        print(f"WARNING: {error_msg}")
        return pd.DataFrame(), {'symbol': symbol, 'files_found': 0, 'alphas_compiled': 0, 'errors': 1}

    print(f"Found {len(folder_variants)} variant folder(s):")
    for folder_path, variant in folder_variants:
        print(f"  - {os.path.basename(folder_path)} (Variant: {variant})")

    # Find all summary.csv files across all variant folders
    summary_files = []  # List of (summary_path, variant) tuples
    for folder_path, variant in folder_variants:
        if not os.path.exists(folder_path):
            print(f"WARNING: Directory not found: {folder_path}")
            continue

        for root, dirs, files in os.walk(folder_path):
            if 'summary.csv' in files:
                summary_files.append((os.path.join(root, 'summary.csv'), variant))

    print(f"Found {len(summary_files)} summary.csv files for {symbol}")

    if not summary_files:
        print(f"WARNING: No summary.csv files found")
        return pd.DataFrame(), {'symbol': symbol, 'files_found': 0, 'alphas_compiled': 0, 'errors': 0}

    # Extract alphas
    results = []
    skipped = 0

    for summary_path, variant in summary_files:
        alpha = extract_alpha_from_summary(summary_path, symbol, exchange, interval, variant)
        if alpha:
            results.append(alpha)
        else:
            skipped += 1

    # Create DataFrame
    df_result = pd.DataFrame(results)

    # Statistics
    stats = {
        'symbol': symbol,
        'files_found': len(summary_files),
        'alphas_compiled': len(df_result),
        'skipped': skipped,
        'errors': 0
    }

    print(f"Compiled {len(df_result)} alphas for {symbol}")
    if skipped > 0:
        print(f"Skipped {skipped} files due to errors")

    return df_result, stats


def compile_multi_symbol_results(base_dir, exchange, interval, symbols):
    """
    Compile alphas for multiple symbols.

    Args:
        base_dir: Base directory containing WFAlphaResults folders
        exchange: Exchange name
        interval: Time interval
        symbols: List of symbols (e.g., ["MBT", "MET"])

    Returns:
        tuple: (Combined DataFrame, list of symbol statistics)
    """
    print("\n" + "=" * 80)
    print("MULTI-SYMBOL WF ALPHA COMPILATION")
    print("=" * 80)
    print(f"Exchange: {exchange}")
    print(f"Interval: {interval}")
    print(f"Symbols: {', '.join(symbols)}")
    print("=" * 80)

    all_results = []
    all_stats = []

    for symbol in symbols:
        df_symbol, stats = process_symbol(symbol, exchange, interval, base_dir)

        if not df_symbol.empty:
            all_results.append(df_symbol)

        all_stats.append(stats)

    # Combine all results
    if all_results:
        df_combined = pd.concat(all_results, ignore_index=True)

        # Sort by Sharpe Ratio descending
        df_combined = df_combined.sort_values('Sharpe', ascending=False).reset_index(drop=True)

    else:
        df_combined = pd.DataFrame()

    # Print summary
    print("\n" + "=" * 80)
    print("COMPILATION SUMMARY")
    print("=" * 80)

    for stat in all_stats:
        status = "ERROR" if stat.get('errors', 0) > 0 else "OK"
        print(f"{stat['symbol']:12} - Files: {stat['files_found']:3}, Alphas: {stat['alphas_compiled']:4}, Status: {status}")

    print("-" * 80)
    total_alphas = sum(s['alphas_compiled'] for s in all_stats)
    total_files = sum(s['files_found'] for s in all_stats)
    total_errors = sum(s['errors'] for s in all_stats)

    print(f"{'TOTAL':12} - Files: {total_files:3}, Alphas: {total_alphas:4}, Errors: {total_errors:2}")
    print("=" * 80)

    return df_combined, all_stats


def save_to_excel(df, output_file):
    """
    Save DataFrame to Excel with single WF_Short worksheet.
    Uses full-period performance metrics from metrics.csv.

    Args:
        df: DataFrame to save
        output_file: Output Excel file path
    """
    if df.empty:
        print("\nWARNING: No data to save!")
        return

    print("\n" + "=" * 80)
    print(f"Saving to: {output_file}")

    # Create WF_Short DataFrame with basic columns (including Variant)
    columns = ['Exchange', 'Symbol', 'Interval', 'Data Point', 'Model', 'Entry / Exit Model',
               'Length', 'Entry', 'Exit', 'Sharpe', 'MDD', 'Trade Count', 'Annual Return',
               'Calmar Ratio', 'Cumulative PnL', 'Buy & Hold', 'PnL Ratio',
               'Overfit_1hop', 'Overfit_2hop', 'Overfit_Final']

    # Add Variant column if present
    if 'Variant' in df.columns:
        columns.insert(3, 'Variant')  # After Interval

    df_short = df[columns].copy()

    # Deduplicate by strategy combination (including Variant if present)
    dedup_cols = ['Exchange', 'Symbol', 'Interval', 'Data Point', 'Model', 'Entry / Exit Model']
    if 'Variant' in df_short.columns:
        dedup_cols.insert(3, 'Variant')  # After Interval

    df_short = df_short.drop_duplicates(
        subset=dedup_cols,
        keep='first'
    )

    # Add row numbers and Heatmap Checked column
    df_short.insert(0, '#', range(1, len(df_short) + 1))
    df_short['Heatmap Checked'] = None

    print(f"WF_Short rows: {len(df_short)}")

    # Save to Excel
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df_short.to_excel(writer, sheet_name='WF_Short', index=False)

        # Auto-adjust column widths
        worksheet = writer.sheets['WF_Short']
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            worksheet.column_dimensions[column_letter].width = adjusted_width

    print(f"\nExcel file saved with WF_Short worksheet!")
    print("  - Full-period performance metrics from metrics.csv")
    print("  - Deduplicated by unique strategy combinations")
    print("=" * 80)


def parse_arguments():
    """
    Parse command-line arguments.

    Returns:
        Namespace with parsed arguments
    """
    parser = argparse.ArgumentParser(description='Compile WF alpha results from WFAlphaResults (multi-symbol support)')

    parser.add_argument('--exchange', type=str, default=EXCHANGE,
                        help=f'Exchange name (default: {EXCHANGE})')
    parser.add_argument('--interval', type=str, default=INTERVAL,
                        help=f'Time interval (default: {INTERVAL})')
    parser.add_argument('--symbols', type=str, default=','.join(SYMBOLS),
                        help=f'Comma-separated list of symbols (default: {",".join(SYMBOLS)})')

    return parser.parse_args()


def interactive_mode():
    """Interactive CLI for running Stage 5: WF Alpha Compilation"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║     Stage 5: WF Alpha Compilation - Interactive Mode         ║
║  Compile walk-forward alpha results into reference file      ║
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
    """Main function to orchestrate WF alpha compilation."""
    # Parse command-line arguments
    args = parse_arguments()

    # Override config with command-line arguments
    exchange = args.exchange
    interval = args.interval
    symbols = [s.strip() for s in args.symbols.split(',')]

    print("\n" + "=" * 80)
    print("WF ALPHA COMPILATION (MULTI-SYMBOL)")
    print("=" * 80)
    print()

    # Define paths
    today = datetime.now().strftime('%Y%m%d')
    output_file = os.path.join(OUTPUT_DIR, f'WFAlpha_Compilation_{exchange}_{interval}_{today}.xlsx')

    # Compile results for all symbols
    df_results, symbol_stats = compile_multi_symbol_results(OUTPUT_DIR, exchange, interval, symbols)

    if df_results.empty:
        print("\nERROR: No results to compile!")
        return

    # Save to Excel
    save_to_excel(df_results, output_file)

    # Print sample statistics
    print("\nGLOBAL STATISTICS:")
    print("-" * 80)
    print(f"Total Alphas: {len(df_results)}")
    print(f"Top Sharpe Ratio: {df_results['Sharpe'].max():.3f}")
    print(f"Average Sharpe Ratio: {df_results['Sharpe'].mean():.3f}")
    print(f"Sharpe Ratio Range: {df_results['Sharpe'].min():.3f} to {df_results['Sharpe'].max():.3f}")
    print(f"Unique Features: {df_results['Data Point'].nunique()}")
    print(f"Unique Models: {df_results['Model'].nunique()}")
    print(f"Unique Strategies: {df_results['Entry / Exit Model'].nunique()}")

    # Show top 10 by Sharpe
    print("\nTop 10 Configurations by Sharpe Ratio (Full Period):")
    print("-" * 80)
    top_10 = df_results.head(10)[['Symbol', 'Data Point', 'Model', 'Entry / Exit Model', 'Sharpe', 'Trade Count']]
    print(top_10.to_string(index=False))
    print()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        interactive_mode()
    else:
        main()
