"""
Compile Walk-Forward Validation Results with Degradation Metrics (Grid Search)

This script compiles all walk_forward_report.csv files from the AQS_SFGridResults directory
and calculates degradation metrics between In-Sample and Walk-Forward performance.

Supports processing multiple symbols in a single run.
Uses glob pattern matching to find folders: merged_{exchange}_{symbol}_{interval}_*

Output: WF_GS_Compilation_{exchange}_{interval}_{YYYYMMDD}.xlsx with all symbols
"""

import os
import pandas as pd
import sys
import glob
from pathlib import Path
import warnings
from datetime import datetime
import argparse

# Suppress warnings
warnings.filterwarnings('ignore')

# Directory paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = SCRIPT_DIR  # Script is now at project root
RESULTS_DIR = os.path.join(PROJECT_DIR, "AQS_SFGridResults")  # Results directory

# Configuration (defaults, can be overridden via command-line)
EXCHANGE = "ibkr"
INTERVAL = "1h"
SYMBOLS = ["VIXY", "VIXM"]  # Base symbols


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='Compile walk-forward validation results (multi-symbol)')
    parser.add_argument('--exchange', type=str, default=EXCHANGE,
                        help=f'Exchange name (default: {EXCHANGE})')
    parser.add_argument('--interval', type=str, default=INTERVAL,
                        help=f'Time interval (default: {INTERVAL})')
    parser.add_argument('--symbols', type=str, default=','.join(SYMBOLS),
                        help=f'Comma-separated list of symbols (default: {",".join(SYMBOLS)})')
    parser.add_argument('--min-sharpe-ratio', type=float, default=1.0,
                        help='Minimum IS Sharpe Ratio threshold for filtering (default: 1.0)')
    return parser.parse_args()


def construct_folder_pattern(exchange, symbol, interval):
    """
    Construct folder pattern for glob matching: merged_{exchange}_{symbol}_{interval}_*

    Args:
        exchange: Exchange name (e.g., "ibkr")
        symbol: Symbol (e.g., "MBT")
        interval: Interval (e.g., "1h", "1d")

    Returns:
        Folder pattern (e.g., "merged_ibkr_MBT_1h_*")
    """
    # FIX: map short-form intervals to long-form folder names using exact match
    # (cannot use str.replace because "30m" is a substring of "30min")
    interval_map = {"15m": "15min", "30m": "30min"}
    interval_folder = interval_map.get(interval, interval)
    return f"merged_{exchange}_{symbol}_{interval_folder}_*"


def extract_metadata_from_path(file_path):
    """
    Extract Exchange, Symbol, Interval, Variant, Feature, Model, and Buy Type from file path.

    Example path:
    AQS_SFGridResults/merged_ibkr_VIXY_1h_john_23Jan2026/close/zscore/trend_long/walk_forward_report.csv

    Returns:
        dict: {
            'exchange': 'ibkr',
            'symbol': 'VIXY',
            'interval': '1h',
            'variant': 'john_23Jan2026',
            'feature': 'close',
            'model': 'zscore',
            'buy_type': 'trend_long'
        }
    """
    parts = Path(file_path).parts

    # Extract from folder name: merged_binance_BTCUSDT_1h_COMPLETE
    folder_name = None
    for part in parts:
        if part.startswith('merged_'):
            folder_name = part
            break

    if not folder_name:
        return None

    # Parse folder name: merged_ibkr_MBT_1h_john_23Jan2026
    # Format: merged_{EXCHANGE}_{SYMBOL}_{INTERVAL}_{VARIANT}
    middle = folder_name.replace('merged_', '')
    name_parts = middle.split('_')

    # Extract exchange, symbol, interval, and variant using position-based parsing
    if len(name_parts) >= 3:
        exchange = name_parts[0]  # ibkr
        symbol = name_parts[1]    # MBT
        interval = name_parts[2]  # 1h
        # Everything after interval is the variant
        variant = '_'.join(name_parts[3:]) if len(name_parts) > 3 else 'default'
    else:
        exchange = 'ibkr'
        symbol = middle
        interval = '1h'
        variant = 'default'

    # Extract feature, model, buy_type from path
    # Path structure: .../feature/model/buy_type/walk_forward_report.csv
    try:
        idx = parts.index(folder_name)
        feature = parts[idx + 1] if idx + 1 < len(parts) else None
        model = parts[idx + 2] if idx + 2 < len(parts) else None
        buy_type = parts[idx + 3] if idx + 3 < len(parts) else None
    except (ValueError, IndexError):
        feature = model = buy_type = None

    return {
        'exchange': exchange,
        'symbol': symbol,
        'interval': interval,
        'variant': variant,
        'feature': feature,
        'model': model,
        'buy_type': buy_type
    }


def calculate_degradation(is_value, os_value, invert=False):
    """
    Calculate degradation percentage: (OS - IS) / IS

    Args:
        is_value: In-sample value
        os_value: Out-of-sample/Walk-forward value
        invert: If True, multiply result by -1 (for metrics where lower is better, like MDD)

    Returns:
        Degradation as decimal (e.g., -0.30 for -30%)
        Returns -999% if IS value is 0 or very close to 0
    """
    if abs(is_value) < 1e-10:  # Close to zero
        return -9.99  # -999% as decimal

    degradation = (os_value - is_value) / is_value

    if invert:
        degradation = -1 * degradation

    return degradation


def calculate_l_degrade(wf1_degrade, wf2_degrade):
    """
    Calculate L Degrade: the lower (worse) of WF1 and WF2 degradation.

    Args:
        wf1_degrade: WF1 degradation value
        wf2_degrade: WF2 degradation value

    Returns:
        Minimum of the two values (worst-case degradation)
    """
    return min(wf1_degrade, wf2_degrade)


def process_symbol(symbol_base, exchange, interval, base_dir):
    """
    Process a single symbol and return its compiled walk-forward results.
    Uses glob pattern matching to find all folders matching the pattern.

    Args:
        symbol_base: Base symbol (e.g., "MBT")
        exchange: Exchange name (e.g., "ibkr")
        interval: Interval (e.g., "1h")
        base_dir: Base directory containing AQS_SFGridResults

    Returns:
        tuple: (DataFrame with results, dict with statistics)
    """
    full_symbol = symbol_base  # No USDT suffix for IBKR
    folder_pattern = construct_folder_pattern(exchange, symbol_base, interval)

    print(f"\nProcessing {full_symbol}...")
    print("-" * 80)

    # Find all matching folders using glob
    pattern_path = os.path.join(base_dir, folder_pattern)
    matching_folders = glob.glob(pattern_path)

    if not matching_folders:
        print(f"WARNING: No folders found matching pattern: {folder_pattern}")
        return pd.DataFrame(), {'symbol': full_symbol, 'files_found': 0, 'configs_compiled': 0, 'errors': 0}

    print(f"Found {len(matching_folders)} matching folder(s): {[os.path.basename(f) for f in matching_folders]}")

    # Find all walk_forward_report.csv files across all matching folders
    csv_files = []
    for folder_path in matching_folders:
        search_pattern = os.path.join(folder_path, '**', 'walk_forward_report.csv')
        csv_files.extend(glob.glob(search_pattern, recursive=True))

    print(f"Found {len(csv_files)} walk_forward_report.csv files for {full_symbol}")

    if not csv_files:
        print(f"WARNING: No files found for {full_symbol}")
        return pd.DataFrame(), {'symbol': full_symbol, 'files_found': 0, 'configs_compiled': 0, 'errors': 0}

    all_data = []
    skipped_files = []

    for csv_file in csv_files:
        try:
            # Extract metadata from path
            metadata = extract_metadata_from_path(csv_file)
            if not metadata:
                print(f"WARNING: Could not parse path: {csv_file}")
                skipped_files.append(csv_file)
                continue

            # Read CSV file
            df = pd.read_csv(csv_file)

            # Process each row (all configurations)
            for idx, row in df.iterrows():
                # Calculate WF1 degradation metrics (WF1 vs IS)
                wf1_sharpe_degrade = calculate_degradation(row['IS Sharpe Ratio'], row['WF1 Sharpe Ratio'])
                wf1_mdd_degrade = calculate_degradation(row['IS Max Drawdown'], row['WF1 Max Drawdown'], invert=True)
                wf1_annual_return_degrade = calculate_degradation(row['IS Annualized Return'], row['WF1 Annualized Return'])
                wf1_calmar_degrade = calculate_degradation(row['IS Calmar Ratio'], row['WF1 Calmar Ratio'])

                # Calculate WF2 degradation metrics (WF2 vs IS)
                wf2_sharpe_degrade = calculate_degradation(row['IS Sharpe Ratio'], row['WF2 Sharpe Ratio'])
                wf2_mdd_degrade = calculate_degradation(row['IS Max Drawdown'], row['WF2 Max Drawdown'], invert=True)
                wf2_annual_return_degrade = calculate_degradation(row['IS Annualized Return'], row['WF2 Annualized Return'])
                wf2_calmar_degrade = calculate_degradation(row['IS Calmar Ratio'], row['WF2 Calmar Ratio'])

                # Calculate L Degrade (worst-case degradation between WF1 and WF2)
                l_sharpe_degrade = calculate_l_degrade(wf1_sharpe_degrade, wf2_sharpe_degrade)
                l_mdd_degrade = calculate_l_degrade(wf1_mdd_degrade, wf2_mdd_degrade)
                l_annual_return_degrade = calculate_l_degrade(wf1_annual_return_degrade, wf2_annual_return_degrade)
                l_calmar_degrade = calculate_l_degrade(wf1_calmar_degrade, wf2_calmar_degrade)

                # Build result row
                result = {
                    'Exchange': metadata['exchange'],
                    'Symbol': metadata['symbol'],
                    'Interval': metadata['interval'],
                    'Variant': metadata['variant'],
                    'Data Point': row['feature'],
                    'Model': row['model'],
                    'Entry / Exit Model': row['buy_type'],
                    'Length': int(row['length']) if pd.notna(row['length']) else None,
                    'Entry': row['entry_threshold'],
                    'Exit': row['exit_threshold'],
                    # IS Metrics
                    'IS Sharpe': row['IS Sharpe Ratio'],
                    'IS MDD': row['IS Max Drawdown'],
                    'IS Trade Count': int(row['IS Trade Count']) if pd.notna(row['IS Trade Count']) else None,
                    'IS Annual Return': row['IS Annualized Return'],
                    'IS Calmar Ratio': row['IS Calmar Ratio'],
                    # WF1 Metrics
                    'WF1 Sharpe': row['WF1 Sharpe Ratio'],
                    'WF1 MDD': row['WF1 Max Drawdown'],
                    'WF1 Trade Count': int(row['WF1 Trade Count']) if pd.notna(row['WF1 Trade Count']) else None,
                    'WF1 Annual Return': row['WF1 Annualized Return'],
                    'WF1 Calmar Ratio': row['WF1 Calmar Ratio'],
                    # WF2 Metrics
                    'WF2 Sharpe': row['WF2 Sharpe Ratio'],
                    'WF2 MDD': row['WF2 Max Drawdown'],
                    'WF2 Trade Count': int(row['WF2 Trade Count']) if pd.notna(row['WF2 Trade Count']) else None,
                    'WF2 Annual Return': row['WF2 Annualized Return'],
                    'WF2 Calmar Ratio': row['WF2 Calmar Ratio'],
                    # WF1 Degradation
                    'WF1 Sharpe Degrade': wf1_sharpe_degrade,
                    'WF1 MDD Degrade': wf1_mdd_degrade,
                    'WF1 Annual Return Degrade': wf1_annual_return_degrade,
                    'WF1 Calmar Ratio Degrade': wf1_calmar_degrade,
                    # WF2 Degradation
                    'WF2 Sharpe Degrade': wf2_sharpe_degrade,
                    'WF2 MDD Degrade': wf2_mdd_degrade,
                    'WF2 Annual Return Degrade': wf2_annual_return_degrade,
                    'WF2 Calmar Ratio Degrade': wf2_calmar_degrade,
                    # L Degradation (Worst Case)
                    'L Sharpe Degrade': l_sharpe_degrade,
                    'L MDD Degrade': l_mdd_degrade,
                    'L Annual Return Degrade': l_annual_return_degrade,
                    'L Calmar Ratio Degrade': l_calmar_degrade
                }

                all_data.append(result)

        except Exception as e:
            print(f"WARNING: Error processing {csv_file}: {str(e)}")
            skipped_files.append(csv_file)
            continue

    # Create DataFrame
    df_result = pd.DataFrame(all_data)

    # Statistics
    stats = {
        'symbol': full_symbol,
        'files_found': len(csv_files),
        'configs_compiled': len(df_result),
        'errors': len(skipped_files)
    }

    print(f"Compiled {len(df_result)} configurations for {full_symbol}")
    if skipped_files:
        print(f"Skipped {len(skipped_files)} files due to errors")

    return df_result, stats


def compile_multi_symbol_results(base_dir, exchange, interval, symbols):
    """
    Compile walk-forward results for multiple symbols.
    Uses glob pattern matching to find all folders matching: merged_{exchange}_{symbol}_{interval}_*

    Args:
        base_dir: Base directory containing merged_* folders
        exchange: Exchange name
        interval: Time interval
        symbols: List of base symbols (e.g., ["MBT", "MET"])

    Returns:
        tuple: (Combined DataFrame, list of symbol statistics)
    """
    print("\n" + "=" * 80)
    print("MULTI-SYMBOL WALK-FORWARD COMPILATION")
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

        # Sort by IS Sharpe Ratio descending
        df_combined = df_combined.sort_values('IS Sharpe', ascending=False).reset_index(drop=True)

        # Add row number column at the beginning
        df_combined.insert(0, '#', range(1, len(df_combined) + 1))
    else:
        df_combined = pd.DataFrame()

    # Print summary
    print("\n" + "=" * 80)
    print("COMPILATION SUMMARY")
    print("=" * 80)

    for stat in all_stats:
        print(f"{stat['symbol']:12} - Files: {stat['files_found']:3}, Configs: {stat['configs_compiled']:4}, Errors: {stat['errors']:2}")

    print("-" * 80)
    total_configs = sum(s['configs_compiled'] for s in all_stats)
    total_files = sum(s['files_found'] for s in all_stats)
    total_errors = sum(s['errors'] for s in all_stats)

    print(f"{'TOTAL':12} - Files: {total_files:3}, Configs: {total_configs:4}, Errors: {total_errors:2}")
    print("=" * 80)

    return df_combined, all_stats


def apply_worksheet_formatting(worksheet, df, degradation_columns):
    """
    Apply percentage formatting and column width adjustments to a worksheet.

    Args:
        worksheet: openpyxl worksheet object
        df: DataFrame that was written to the worksheet
        degradation_columns: List of column names to format as percentages
    """
    from openpyxl.styles import numbers
    from openpyxl.utils import get_column_letter

    # Get column indices for degradation columns
    header_row = [cell.value for cell in worksheet[1]]

    for col_name in degradation_columns:
        if col_name in header_row:
            col_idx = header_row.index(col_name) + 1  # openpyxl is 1-indexed
            col_letter = get_column_letter(col_idx)

            # Apply percentage format to all data rows
            for row_num in range(2, len(df) + 2):  # Start from row 2 (after header)
                cell = worksheet[f'{col_letter}{row_num}']
                cell.number_format = numbers.FORMAT_PERCENTAGE_00

    # Auto-adjust column widths
    for column in worksheet.columns:
        max_length = 0
        column_letter = column[0].column_letter

        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass

        adjusted_width = min(max_length + 2, 50)  # Cap at 50
        worksheet.column_dimensions[column_letter].width = adjusted_width


def save_to_excel(df, output_file, min_sharpe_ratio):
    """
    Save DataFrame to Excel with three worksheets:
    1. WF_Results: All results (unfiltered)
    2. WF_Filtered: Filtered results (IS Sharpe >= min_sharpe_ratio AND L Sharpe Degrade >= -10%)
    3. WF_Short: Deduplicated WF_Filtered (one row per unique strategy combination)

    Args:
        df: DataFrame to save
        output_file: Output Excel file path
        min_sharpe_ratio: Minimum IS Sharpe Ratio threshold for filtering
    """
    if df.empty:
        print("\nWARNING: No data to save!")
        return

    print("\n" + "=" * 80)
    print(f"Saving to: {output_file}")

    # Create filtered DataFrame
    # Keep rows where: IS Sharpe >= min_sharpe_ratio AND L Sharpe Degrade >= -0.10
    df_filtered = df[(df['IS Sharpe'] >= min_sharpe_ratio) & (df['L Sharpe Degrade'] >= -0.10)].copy()

    # Create deduplicated DataFrame (WF_Short)
    # Deduplicate based on: Exchange, Symbol, Interval, Variant, Data Point, Model, Entry/Exit Model
    # Keep first occurrence (highest IS Sharpe since already sorted)
    dedup_cols = ['Exchange', 'Symbol', 'Interval', 'Data Point', 'Model', 'Entry / Exit Model']
    # Include Variant in deduplication if column exists
    if 'Variant' in df_filtered.columns:
        dedup_cols.insert(3, 'Variant')  # After Interval
    df_short = df_filtered.drop_duplicates(
        subset=dedup_cols,
        keep='first'
    ).copy()

    # Print statistics
    total_rows = len(df)
    filtered_rows = len(df_filtered)
    short_rows = len(df_short)
    removed_by_filter = total_rows - filtered_rows
    removed_by_dedup = filtered_rows - short_rows
    filter_pct = (removed_by_filter / total_rows * 100) if total_rows > 0 else 0
    dedup_pct = (removed_by_dedup / filtered_rows * 100) if filtered_rows > 0 else 0

    print(f"WF_Results rows: {total_rows}")
    print(f"WF_Filtered rows: {filtered_rows} (removed {removed_by_filter} rows, {filter_pct:.1f}%)")
    print(f"WF_Short rows: {short_rows} (removed {removed_by_dedup} duplicates, {dedup_pct:.1f}%)")

    # Degradation columns for percentage formatting
    degradation_columns = [
        'WF1 Sharpe Degrade', 'WF1 MDD Degrade', 'WF1 Annual Return Degrade', 'WF1 Calmar Ratio Degrade',
        'WF2 Sharpe Degrade', 'WF2 MDD Degrade', 'WF2 Annual Return Degrade', 'WF2 Calmar Ratio Degrade',
        'L Sharpe Degrade', 'L MDD Degrade', 'L Annual Return Degrade', 'L Calmar Ratio Degrade'
    ]

    # Create Excel writer
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Write WF_Results worksheet (all data)
        df.to_excel(writer, sheet_name='WF_Results', index=False)

        # Write WF_Filtered worksheet (filtered data)
        df_filtered.to_excel(writer, sheet_name='WF_Filtered', index=False)

        # Write WF_Short worksheet (deduplicated data)
        df_short.to_excel(writer, sheet_name='WF_Short', index=False)

        # Apply formatting to WF_Results
        worksheet_results = writer.sheets['WF_Results']
        apply_worksheet_formatting(worksheet_results, df, degradation_columns)

        # Apply formatting to WF_Filtered
        worksheet_filtered = writer.sheets['WF_Filtered']
        apply_worksheet_formatting(worksheet_filtered, df_filtered, degradation_columns)

        # Apply formatting to WF_Short
        worksheet_short = writer.sheets['WF_Short']
        apply_worksheet_formatting(worksheet_short, df_short, degradation_columns)

    print(f"\nExcel file saved successfully with 3 worksheets!")
    print("  - WF_Results: All results")
    print(f"  - WF_Filtered: Filtered results (IS Sharpe >= {min_sharpe_ratio}, L Sharpe Degrade >= -10%)")
    print("  - WF_Short: Deduplicated (unique strategy combinations)")
    print("=" * 80)


def interactive_mode():
    """Interactive CLI for running Stage 3: WF Results Compilation"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║    Stage 3: WF Results Compilation - Interactive Mode        ║
║  Aggregate walk-forward results and calculate degradation    ║
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
    """Main function to orchestrate multi-symbol walk-forward compilation."""
    # Parse command-line arguments
    args = parse_arguments()
    exchange = args.exchange
    interval = args.interval
    symbols = [s.strip() for s in args.symbols.split(',')]
    min_sharpe_ratio = args.min_sharpe_ratio

    print("\n" + "=" * 80)
    print("WALK-FORWARD VALIDATION RESULTS COMPILATION (MULTI-SYMBOL)")
    print("=" * 80)
    print(f"Exchange: {exchange}")
    print(f"Interval: {interval}")
    print(f"Symbols: {', '.join(symbols)}")
    print()

    # Generate output filename with today's date
    today = datetime.now().strftime("%Y%m%d")
    output_file = os.path.join(RESULTS_DIR, f'WF_GS_Compilation_{exchange}_{interval}_{today}.xlsx')

    # Compile results for all symbols
    df_results, symbol_stats = compile_multi_symbol_results(RESULTS_DIR, exchange, interval, symbols)

    if df_results.empty:
        print("\nERROR: No results to compile!")
        return

    # Save to Excel
    save_to_excel(df_results, output_file, min_sharpe_ratio)

    # Print sample statistics
    print("\nGLOBAL STATISTICS:")
    print("-" * 80)
    print(f"Top IS Sharpe Ratio: {df_results['IS Sharpe'].max():.3f}")
    print(f"Top WF1 Sharpe Ratio: {df_results['WF1 Sharpe'].max():.3f}")
    print(f"Top WF2 Sharpe Ratio: {df_results['WF2 Sharpe'].max():.3f}")
    print(f"Average WF1 Sharpe Degradation: {df_results['WF1 Sharpe Degrade'].mean():.2%}")
    print(f"Average WF2 Sharpe Degradation: {df_results['WF2 Sharpe Degrade'].mean():.2%}")
    print(f"Average L Sharpe Degradation: {df_results['L Sharpe Degrade'].mean():.2%}")
    print(f"Average WF1 MDD Degradation: {df_results['WF1 MDD Degrade'].mean():.2%}")
    print(f"Average WF2 MDD Degradation: {df_results['WF2 MDD Degrade'].mean():.2%}")
    print(f"Average L MDD Degradation: {df_results['L MDD Degrade'].mean():.2%}")
    print(f"Average WF1 Annual Return Degradation: {df_results['WF1 Annual Return Degrade'].mean():.2%}")
    print(f"Average WF2 Annual Return Degradation: {df_results['WF2 Annual Return Degrade'].mean():.2%}")
    print(f"Average L Annual Return Degradation: {df_results['L Annual Return Degrade'].mean():.2%}")
    print(f"Average WF1 Calmar Ratio Degradation: {df_results['WF1 Calmar Ratio Degrade'].mean():.2%}")
    print(f"Average WF2 Calmar Ratio Degradation: {df_results['WF2 Calmar Ratio Degrade'].mean():.2%}")
    print(f"Average L Calmar Ratio Degradation: {df_results['L Calmar Ratio Degrade'].mean():.2%}")

    # Show top 10 by IS Sharpe
    print("\nTop 10 Configurations by IS Sharpe Ratio:")
    print("-" * 80)
    top_10 = df_results.head(10)[['#', 'Symbol', 'Data Point', 'Model', 'Entry / Exit Model',
                                    'IS Sharpe', 'WF1 Sharpe', 'WF2 Sharpe',
                                    'WF1 Sharpe Degrade', 'WF2 Sharpe Degrade', 'L Sharpe Degrade']]
    print(top_10.to_string(index=False))
    print()


if __name__ == '__main__':
    if len(sys.argv) == 1:
        interactive_mode()
    else:
        main()
