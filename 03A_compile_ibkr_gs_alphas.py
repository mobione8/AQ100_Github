"""
Compile Best Alphas from Grid Search IS/OOS Results (Multi-Symbol Support)

This script scans all strategy folders, extracts the top 10 configurations from each
IS_OOS_comparison.csv file, and compiles them into a multi-worksheet Excel file.

Supports processing multiple symbols in a single run.
Uses glob pattern matching to find folders: merged_{exchange}_{symbol}_{interval}_*

Output: Alpha_GS_Compilation_{exchange}_{interval}_{YYYYMMDD}.xlsx
  - Alpha Full_Compilation: All top 10 configs from each IS_OOS_comparison.csv
  - Alpha_Short: Filtered subset meeting quality thresholds
"""

import pandas as pd
import os
import sys
from pathlib import Path
import argparse
import glob
from datetime import datetime

# Configuration
EXCHANGE = "ibkr"
INTERVAL = "1h"
SYMBOLS = ["VIXY", "VIXM"]  # Base symbols

# Top N rows to extract from each IS_OOS_comparison.csv
TOP_N_ROWS = 10

# Filter thresholds for Alpha_Short worksheet
# SHARPE_IS_MIN and SHARPE_OOS_MIN are now passed as --min-sharpe-ratio CLI argument
DEGRADATION_MIN = -10  # >= -10% degradation allowed


def construct_folder_pattern(exchange, symbol, interval):
    """
    Construct folder pattern for glob matching: merged_{exchange}_{symbol}_{interval}_*

    Args:
        exchange: Exchange name (e.g., "ibkr")
        symbol: Symbol (e.g., "VIXY")
        interval: Interval (e.g., "1h", "1d")

    Returns:
        Folder pattern (e.g., "merged_ibkr_VIXY_1h_*")
    """
    return f"merged_{exchange}_{symbol}_{interval}_*"


def extract_variant_from_folder(folder_path, exchange, symbol, interval):
    """
    Extract variant suffix from folder name.

    Folder format: merged_{exchange}_{symbol}_{interval}_{variant}
    Example: merged_ibkr_AAPL_1h_john_23Jan2026 -> 'john_23Jan2026'

    Args:
        folder_path: Full path to the folder
        exchange: Exchange name
        symbol: Symbol name
        interval: Interval

    Returns:
        Variant string (everything after interval in folder name)
    """
    folder_name = os.path.basename(folder_path)
    prefix = f"merged_{exchange}_{symbol}_{interval}_"

    if folder_name.startswith(prefix):
        return folder_name[len(prefix):]
    else:
        # Fallback: split by underscore and take everything after position 4
        parts = folder_name.split('_')
        if len(parts) >= 5:
            return '_'.join(parts[4:])
        return 'unknown'


def extract_top_alphas(comparison_path, symbol, exchange, interval, variant):
    """
    Extract top N alphas from an IS_OOS_comparison.csv file.
    Returns up to TOP_N_ROWS rows sorted by Sharpe Ratio_IS descending.

    Args:
        comparison_path: Path to IS_OOS_comparison.csv file
        symbol: Symbol (e.g., "VIXY")
        exchange: Exchange name
        interval: Time interval
        variant: Variant suffix (e.g., "john_23Jan2026")

    Returns:
        list of dicts with alpha details, or empty list if error
    """
    try:
        df = pd.read_csv(comparison_path)

        if df.empty:
            return []

        # Sort by Sharpe Ratio_IS descending and take top N
        df_sorted = df.sort_values('Sharpe Ratio_IS', ascending=False).head(TOP_N_ROWS)

        # Parse the path to extract feature (Data Point)
        # Path format: merged_{exchange}_{symbol}_{interval}_*/{feature}/{model}/{strategy}/IS_OOS_comparison.csv
        parts = Path(comparison_path).parts
        feature = parts[-4]  # feature folder name (Data Point)

        results = []
        for _, row in df_sorted.iterrows():
            results.append({
                'Exchange': exchange,
                'Symbol': symbol,
                'Interval': interval,
                'Variant': variant,
                'Data Point': feature,
                'model': row['model'],
                'buy_type': row['buy_type'],
                'length': row['length'],
                'entry_threshold': row['entry_threshold'],
                'exit_threshold': row['exit_threshold'],
                'Sharpe Ratio_IS': row['Sharpe Ratio_IS'],
                'Sharpe Ratio_OOS': row['Sharpe Ratio_OOS'],
                'Sharpe_Degradation_%': row['Sharpe_Degradation_%'],
                'Annualized Return_IS': row['Annualized Return_IS'],
                'Annualized Return_OOS': row['Annualized Return_OOS'],
                'Return_Degradation_%': row['Return_Degradation_%'],
                'Max Drawdown_IS': row['Max Drawdown_IS'],
                'Max Drawdown_OOS': row['Max Drawdown_OOS'],
                'Drawdown_Degradation_%': row['Drawdown_Degradation_%'],
                'Calmar Ratio_IS': row['Calmar Ratio_IS'],
                'Calmar Ratio_OOS': row['Calmar Ratio_OOS'],
                'Calmar_Degradation_%': row['Calmar_Degradation_%'],
                'Trade Count_IS': row['Trade Count_IS'],
                'Trade Count_OOS': row['Trade Count_OOS']
            })

        return results

    except Exception as e:
        print(f"  WARNING: Error processing {comparison_path}: {e}")
        return []


def process_symbol(symbol_base, exchange, interval, base_dir):
    """
    Process a single symbol and return its compiled alphas.
    Uses glob pattern matching to find all folders matching the pattern.

    Args:
        symbol_base: Base symbol (e.g., "VIXY")
        exchange: Exchange name (e.g., "ibkr")
        interval: Interval (e.g., "1h")
        base_dir: Base directory containing results folders

    Returns:
        tuple: (DataFrame with results, dict with statistics)
    """
    full_symbol = symbol_base
    folder_pattern = construct_folder_pattern(exchange, symbol_base, interval)
    pattern_path = os.path.join(base_dir, folder_pattern)

    print(f"\nProcessing {full_symbol}...")
    print("-" * 80)

    # Find all matching folders using glob
    matching_folders = glob.glob(pattern_path)

    if not matching_folders:
        error_msg = f"No directories found matching pattern: {folder_pattern}"
        print(f"ERROR: {error_msg}")
        return pd.DataFrame(), {'symbol': full_symbol, 'files_found': 0, 'alphas_compiled': 0, 'errors': 1, 'error_msg': error_msg}

    print(f"Found {len(matching_folders)} matching folder(s): {[os.path.basename(f) for f in matching_folders]}")

    # Extract variants from folder names
    folder_variants = {}
    for folder_path in matching_folders:
        variant = extract_variant_from_folder(folder_path, exchange, symbol_base, interval)
        folder_variants[folder_path] = variant
        print(f"  Folder: {os.path.basename(folder_path)} -> Variant: {variant}")

    # Find all IS_OOS_comparison.csv files across all matching folders
    # Track which folder each file comes from to preserve variant info
    comparison_files = []  # List of (file_path, variant) tuples
    for folder_path in matching_folders:
        variant = folder_variants[folder_path]
        for root, dirs, files in os.walk(folder_path):
            if 'IS_OOS_comparison.csv' in files:
                comparison_files.append((os.path.join(root, 'IS_OOS_comparison.csv'), variant))

    print(f"Found {len(comparison_files)} IS_OOS_comparison.csv files for {full_symbol}")

    if not comparison_files:
        warning_msg = "No IS_OOS_comparison.csv files found"
        print(f"WARNING: {warning_msg}")
        return pd.DataFrame(), {'symbol': full_symbol, 'files_found': 0, 'alphas_compiled': 0, 'errors': 0, 'error_msg': warning_msg}

    # Extract top alphas from each file
    results = []
    files_with_data = 0

    for comparison_path, variant in comparison_files:
        alphas = extract_top_alphas(comparison_path, full_symbol, exchange, interval, variant)
        if alphas:
            results.extend(alphas)
            files_with_data += 1

    # Create DataFrame
    df_result = pd.DataFrame(results)

    # Statistics
    stats = {
        'symbol': full_symbol,
        'files_found': len(comparison_files),
        'files_with_data': files_with_data,
        'alphas_compiled': len(df_result),
        'errors': 0,
        'error_msg': None
    }

    print(f"Compiled {len(df_result)} alphas from {files_with_data} files for {full_symbol}")

    return df_result, stats


def compile_multi_symbol_results(base_dir, exchange, interval, symbols):
    """
    Compile top alphas for multiple symbols.
    Uses glob pattern matching to find all folders matching: merged_{exchange}_{symbol}_{interval}_*

    Args:
        base_dir: Base directory containing merged_* folders
        exchange: Exchange name
        interval: Time interval
        symbols: List of base symbols (e.g., ["VIXY", "VIXM"])

    Returns:
        tuple: (Combined DataFrame, list of symbol statistics)
    """
    print("\n" + "=" * 80)
    print("MULTI-SYMBOL ALPHA GS COMPILATION")
    print("=" * 80)
    print(f"Exchange: {exchange}")
    print(f"Interval: {interval}")
    print(f"Symbols: {', '.join(symbols)}")
    print(f"Top N per file: {TOP_N_ROWS}")
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

        # Sort by Sharpe Ratio_IS descending
        df_combined = df_combined.sort_values('Sharpe Ratio_IS', ascending=False).reset_index(drop=True)

        # Add row number column at the beginning
        df_combined.insert(0, '#', range(1, len(df_combined) + 1))
    else:
        df_combined = pd.DataFrame()

    # Print summary
    print("\n" + "=" * 80)
    print("COMPILATION SUMMARY")
    print("=" * 80)

    for stat in all_stats:
        status = "ERROR" if stat.get('errors', 0) > 0 else "OK"
        print(f"{stat['symbol']:12} - Files: {stat['files_found']:3}, Alphas: {stat['alphas_compiled']:4}, Status: {status}")
        if stat.get('error_msg'):
            print(f"             {stat['error_msg']}")

    print("-" * 80)
    total_alphas = sum(s['alphas_compiled'] for s in all_stats)
    total_files = sum(s['files_found'] for s in all_stats)
    total_errors = sum(s['errors'] for s in all_stats)

    print(f"{'TOTAL':12} - Files: {total_files:3}, Alphas: {total_alphas:4}, Errors: {total_errors:2}")
    print("=" * 80)

    return df_combined, all_stats


def apply_alpha_short_filter(df, min_sharpe_ratio):
    """
    Apply filter criteria to create Alpha_Short worksheet.

    Criteria (ALL must be met):
    - Sharpe Ratio_IS >= min_sharpe_ratio
    - Sharpe Ratio_OOS >= min_sharpe_ratio
    - Sharpe_Degradation_% >= -10

    Args:
        df: DataFrame with full compilation
        min_sharpe_ratio: Minimum Sharpe Ratio threshold for IS and OOS

    Returns:
        Filtered DataFrame with re-numbered # column
    """
    if df.empty:
        return df.copy()

    # Apply all three filter conditions (AND logic)
    mask = (
        (df['Sharpe Ratio_IS'] >= min_sharpe_ratio) &
        (df['Sharpe Ratio_OOS'] >= min_sharpe_ratio) &
        (df['Sharpe_Degradation_%'] >= DEGRADATION_MIN)
    )

    df_filtered = df[mask].copy()

    # Re-number the # column
    if not df_filtered.empty:
        df_filtered['#'] = range(1, len(df_filtered) + 1)

    return df_filtered


def save_to_excel_multi_sheet(df_full, output_file, min_sharpe_ratio):
    """
    Save DataFrame to Excel with two worksheets.
    Handles Excel's row limit (1,048,576) by splitting large datasets across multiple sheets.

    Args:
        df_full: DataFrame with full compilation
        output_file: Output Excel file path
        min_sharpe_ratio: Minimum Sharpe Ratio threshold for filtering
    """
    if df_full.empty:
        print("\nWARNING: No data to save!")
        return False

    print("\n" + "=" * 80)
    print(f"Saving to: {output_file}")

    # Create filtered version for Alpha_Short
    df_short = apply_alpha_short_filter(df_full, min_sharpe_ratio)

    print(f"Alpha Full_Compilation: {len(df_full)} rows")
    print(f"Alpha_Short: {len(df_short)} rows (filtered)")
    print(f"Filter criteria: Sharpe_IS >= {min_sharpe_ratio}, Sharpe_OOS >= {min_sharpe_ratio}, Degradation >= {DEGRADATION_MIN}%")

    # Excel row limit (minus 1 for header)
    EXCEL_MAX_ROWS = 1000000

    # Save to Excel with multiple sheets
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Handle Alpha Full_Compilation - split if exceeds row limit
        if len(df_full) <= EXCEL_MAX_ROWS:
            df_full.to_excel(writer, sheet_name='Alpha Full_Compilation', index=False)
        else:
            # Split into multiple sheets
            num_sheets = (len(df_full) + EXCEL_MAX_ROWS - 1) // EXCEL_MAX_ROWS
            print(f"  Splitting Full_Compilation into {num_sheets} sheets (exceeds Excel row limit)")
            for i in range(num_sheets):
                start_idx = i * EXCEL_MAX_ROWS
                end_idx = min((i + 1) * EXCEL_MAX_ROWS, len(df_full))
                sheet_name = f'Alpha Full_Compilation_{i+1}' if num_sheets > 1 else 'Alpha Full_Compilation'
                df_full.iloc[start_idx:end_idx].to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"    Sheet '{sheet_name}': rows {start_idx+1} to {end_idx}")

        # Handle Alpha_Short - split if exceeds row limit
        if len(df_short) <= EXCEL_MAX_ROWS:
            df_short.to_excel(writer, sheet_name='Alpha_Short', index=False)
        else:
            num_sheets = (len(df_short) + EXCEL_MAX_ROWS - 1) // EXCEL_MAX_ROWS
            print(f"  Splitting Alpha_Short into {num_sheets} sheets (exceeds Excel row limit)")
            for i in range(num_sheets):
                start_idx = i * EXCEL_MAX_ROWS
                end_idx = min((i + 1) * EXCEL_MAX_ROWS, len(df_short))
                sheet_name = f'Alpha_Short_{i+1}' if num_sheets > 1 else 'Alpha_Short'
                df_short.iloc[start_idx:end_idx].to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"    Sheet '{sheet_name}': rows {start_idx+1} to {end_idx}")

    print(f"Excel file saved successfully!")
    print("=" * 80)

    return True


def run_smoke_test(output_file, min_sharpe_ratio):
    """
    Run full validation smoke test on the generated Excel file.

    Args:
        output_file: Path to the Excel file to validate
        min_sharpe_ratio: Minimum Sharpe Ratio threshold used for filtering

    Returns:
        bool: True if all tests pass, False otherwise
    """
    print("\n" + "=" * 80)
    print("RUNNING SMOKE TEST")
    print("=" * 80)

    errors = []

    try:
        # Load all sheet names to handle split sheets
        xl = pd.ExcelFile(output_file)
        sheet_names = xl.sheet_names

        # Find full compilation sheets (may be split: Alpha Full_Compilation_1, _2, etc.)
        full_sheets = [s for s in sheet_names if s.startswith('Alpha Full_Compilation')]
        short_sheets = [s for s in sheet_names if s.startswith('Alpha_Short')]

        if not full_sheets or not short_sheets:
            print(f"[FAIL] Missing required sheets. Found: {sheet_names}")
            return False

        # Load and concatenate all full compilation sheets
        df_full_parts = [pd.read_excel(output_file, sheet_name=s) for s in sorted(full_sheets)]
        df_full = pd.concat(df_full_parts, ignore_index=True) if len(df_full_parts) > 1 else df_full_parts[0]

        # Load and concatenate all short sheets
        df_short_parts = [pd.read_excel(output_file, sheet_name=s) for s in sorted(short_sheets)]
        df_short = pd.concat(df_short_parts, ignore_index=True) if len(df_short_parts) > 1 else df_short_parts[0]

        if len(full_sheets) > 1:
            print(f"[PASS] Loaded {len(full_sheets)} Full_Compilation sheets ({len(df_full)} total rows)")
        else:
            print(f"[PASS] Both worksheets loaded successfully")
    except Exception as e:
        print(f"[FAIL] Could not load worksheets: {e}")
        return False

    # Test 1: Check column count (25 columns including Variant)
    expected_cols = 25
    if len(df_full.columns) == expected_cols:
        print(f"[PASS] Alpha Full_Compilation has {expected_cols} columns")
    else:
        errors.append(f"Alpha Full_Compilation has {len(df_full.columns)} columns, expected {expected_cols}")
        print(f"[FAIL] {errors[-1]}")

    if len(df_short.columns) == expected_cols:
        print(f"[PASS] Alpha_Short has {expected_cols} columns")
    else:
        errors.append(f"Alpha_Short has {len(df_short.columns)} columns, expected {expected_cols}")
        print(f"[FAIL] {errors[-1]}")

    # Test 2: Row numbering is sequential
    if not df_full.empty:
        expected_nums = list(range(1, len(df_full) + 1))
        actual_nums = df_full['#'].tolist()
        if actual_nums == expected_nums:
            print(f"[PASS] Alpha Full_Compilation row numbering is sequential (1 to {len(df_full)})")
        else:
            errors.append("Alpha Full_Compilation row numbering is not sequential")
            print(f"[FAIL] {errors[-1]}")

    if not df_short.empty:
        expected_nums = list(range(1, len(df_short) + 1))
        actual_nums = df_short['#'].tolist()
        if actual_nums == expected_nums:
            print(f"[PASS] Alpha_Short row numbering is sequential (1 to {len(df_short)})")
        else:
            errors.append("Alpha_Short row numbering is not sequential")
            print(f"[FAIL] {errors[-1]}")

    # Test 3: Alpha_Short filter criteria
    if not df_short.empty:
        sharpe_is_ok = (df_short['Sharpe Ratio_IS'] >= min_sharpe_ratio).all()
        sharpe_oos_ok = (df_short['Sharpe Ratio_OOS'] >= min_sharpe_ratio).all()
        degradation_ok = (df_short['Sharpe_Degradation_%'] >= DEGRADATION_MIN).all()

        if sharpe_is_ok:
            print(f"[PASS] All Alpha_Short rows have Sharpe Ratio_IS >= {min_sharpe_ratio}")
        else:
            errors.append(f"Some Alpha_Short rows have Sharpe Ratio_IS < {min_sharpe_ratio}")
            print(f"[FAIL] {errors[-1]}")

        if sharpe_oos_ok:
            print(f"[PASS] All Alpha_Short rows have Sharpe Ratio_OOS >= {min_sharpe_ratio}")
        else:
            errors.append(f"Some Alpha_Short rows have Sharpe Ratio_OOS < {min_sharpe_ratio}")
            print(f"[FAIL] {errors[-1]}")

        if degradation_ok:
            print(f"[PASS] All Alpha_Short rows have Sharpe_Degradation_% >= {DEGRADATION_MIN}")
        else:
            errors.append(f"Some Alpha_Short rows have Sharpe_Degradation_% < {DEGRADATION_MIN}")
            print(f"[FAIL] {errors[-1]}")
    else:
        print(f"[INFO] Alpha_Short is empty (no rows met filter criteria)")

    # Test 4: Sorting (Sharpe Ratio_IS descending)
    if not df_full.empty and len(df_full) > 1:
        is_sorted = df_full['Sharpe Ratio_IS'].is_monotonic_decreasing
        if is_sorted:
            print(f"[PASS] Alpha Full_Compilation is sorted by Sharpe Ratio_IS descending")
        else:
            errors.append("Alpha Full_Compilation is not sorted by Sharpe Ratio_IS descending")
            print(f"[FAIL] {errors[-1]}")

    # Test 5: Alpha_Short is subset of Full
    if not df_short.empty:
        if len(df_short) <= len(df_full):
            print(f"[PASS] Alpha_Short ({len(df_short)} rows) <= Alpha Full_Compilation ({len(df_full)} rows)")
        else:
            errors.append("Alpha_Short has more rows than Alpha Full_Compilation")
            print(f"[FAIL] {errors[-1]}")

    # Test 6: Sample data spot checks
    if not df_full.empty:
        first_row = df_full.iloc[0]
        print(f"[INFO] First row: Symbol={first_row['Symbol']}, Sharpe_IS={first_row['Sharpe Ratio_IS']:.4f}")
        if len(df_full) > 1:
            last_row = df_full.iloc[-1]
            print(f"[INFO] Last row: Symbol={last_row['Symbol']}, Sharpe_IS={last_row['Sharpe Ratio_IS']:.4f}")

    # Summary
    print("-" * 80)
    if errors:
        print(f"SMOKE TEST FAILED: {len(errors)} error(s)")
        for err in errors:
            print(f"  - {err}")
        return False
    else:
        print("SMOKE TEST PASSED: All validations successful")
        return True


def parse_arguments():
    """
    Parse command-line arguments.

    Returns:
        Namespace with parsed arguments
    """
    parser = argparse.ArgumentParser(description='Compile top alphas from IS/OOS comparison results (multi-symbol support)')

    parser.add_argument('--exchange', type=str, default=EXCHANGE,
                        help=f'Exchange name (default: {EXCHANGE})')
    parser.add_argument('--interval', type=str, default=INTERVAL,
                        help=f'Time interval (default: {INTERVAL})')
    parser.add_argument('--symbols', type=str, default=','.join(SYMBOLS),
                        help=f'Comma-separated list of symbols (default: {",".join(SYMBOLS)})')
    parser.add_argument('--min-sharpe-ratio', type=float, default=1.0,
                        help='Minimum Sharpe Ratio threshold for IS and OOS (default: 1.0)')
    parser.add_argument('--smoke', action='store_true',
                        help='Run smoke test after compilation')

    return parser.parse_args()


def interactive_mode():
    """Interactive CLI for running Stage 1: Alpha Compilation"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║       Stage 1: Alpha Compilation - Interactive Mode          ║
║  Extract best configurations from grid search results        ║
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
    """Main function to orchestrate multi-symbol alpha GS compilation."""
    # Parse command-line arguments
    args = parse_arguments()

    # Override config with command-line arguments
    exchange = args.exchange
    interval = args.interval
    symbols = [s.strip() for s in args.symbols.split(',')]
    min_sharpe_ratio = args.min_sharpe_ratio

    print("\n" + "=" * 80)
    print("ALPHA GS COMPILATION (MULTI-SYMBOL)")
    print("=" * 80)
    print()

    # Define paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    results_dir = os.path.join(script_dir, "AQS_SFGridResults")  # Data lives here!

    # Compile results for all symbols (pass results_dir, not script_dir!)
    df_results, symbol_stats = compile_multi_symbol_results(results_dir, exchange, interval, symbols)

    if df_results.empty:
        print("\nERROR: No results to compile!")
        return

    # Generate output filename with today's date
    today = datetime.now().strftime("%Y%m%d")
    output_file = os.path.join(results_dir, f'Alpha_GS_Compilation_{exchange}_{interval}_{today}.xlsx')

    # Save to Excel with multiple sheets
    success = save_to_excel_multi_sheet(df_results, output_file, min_sharpe_ratio)

    if not success:
        return

    # Print sample statistics
    print("\nGLOBAL STATISTICS:")
    print("-" * 80)
    print(f"Total Alphas (Full): {len(df_results)}")
    print(f"Top Sharpe Ratio_IS: {df_results['Sharpe Ratio_IS'].max():.3f}")
    print(f"Average Sharpe Ratio_IS: {df_results['Sharpe Ratio_IS'].mean():.3f}")
    print(f"Top Sharpe Ratio_OOS: {df_results['Sharpe Ratio_OOS'].max():.3f}")
    print(f"Average Sharpe Ratio_OOS: {df_results['Sharpe Ratio_OOS'].mean():.3f}")
    print(f"Unique Symbols: {df_results['Symbol'].nunique()}")
    print(f"Unique Data Points: {df_results['Data Point'].nunique()}")
    print(f"Unique Models: {df_results['model'].nunique()}")
    print(f"Unique Strategies: {df_results['buy_type'].nunique()}")

    # Show top 10 by Sharpe_IS
    print("\nTop 10 Configurations by Sharpe Ratio_IS:")
    print("-" * 80)
    top_10 = df_results.head(10)[['#', 'Symbol', 'Data Point', 'model', 'buy_type', 'Sharpe Ratio_IS', 'Sharpe Ratio_OOS', 'Sharpe_Degradation_%']]
    print(top_10.to_string(index=False))
    print()

    # Run smoke test if requested
    if args.smoke:
        run_smoke_test(output_file, min_sharpe_ratio)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        interactive_mode()
    else:
        main()
