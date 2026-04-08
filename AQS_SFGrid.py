"""
AQS_SFGrid_enhanced.py
======================

Enhanced version of AQS_SFGrid with In-Sample/Out-of-Sample validation.
Sequential execution for easier debugging and understanding.

Key Features:
- IS/OOS split (default 70% IS, 30% OOS)
- Multi-feature processing (all columns)
- Top-N vs Full Grid OOS validation
- Comprehensive IS/OOS reports and comparisons
"""

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from util_AQS_parallel import *
import os

# ============================================================================
# CONFIGURATION
# ============================================================================

# Data source (single file, hardcoded)
DATA_FILE = r"GridSearch_Data/merged_ibkr_NVDA_1h_john_05Dec2025.csv"

# Output directory
OUTPUT_BASE_DIR = "AQS_SFGridResults_Enhanced"

# IS/OOS Split Configuration
IS_OOS_SPLIT = 0.7                # 70% IS, 30% OOS
OOS_TOP_N_VALIDATION = 10         # 0 = full grid on OOS, N>0 = validate top N from IS
OOS_PRE_WARMUP = "Y"              # "Y" = overlap with IS, "N" = no overlap
MAX_WARMUP_LENGTH = 300           # Warmup period length

# Dynamic Trade Count Threshold
pct_min_trade_count_threshold = 0.015  # 1.5% of data rows

# Models to test
model = ['zscore', 'min_max', 'sma_diff']

# Strategy types to test
buy_type = ['trend_long', 'trend_short', 'trend_reverse', 'trend_reverse_long',
            'trend_reverse_short', 'mr', 'mr_reverse']

# Performance filters
sharpe_ratio_threshold = 1.0

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == '__main__':

    print("="*80)
    print("AQS ENHANCED GRID SEARCH WITH IS/OOS VALIDATION")
    print("="*80)
    print(f"Data file: {DATA_FILE}")
    print(f"IS/OOS Split: {IS_OOS_SPLIT*100:.0f}% IS, {(1-IS_OOS_SPLIT)*100:.0f}% OOS")
    print(f"OOS Warmup: {MAX_WARMUP_LENGTH} bars")
    print(f"OOS Validation: {'Full grid' if OOS_TOP_N_VALIDATION==0 else f'Top {OOS_TOP_N_VALIDATION}'}")
    print("="*80)

    # Create output directory
    if not os.path.exists(OUTPUT_BASE_DIR):
        os.makedirs(OUTPUT_BASE_DIR)

    # Load data
    df = pd.read_csv(DATA_FILE)
    df = df.dropna()

    # Calculate dynamic period from filename
    csv_file_name = os.path.splitext(os.path.basename(DATA_FILE))[0]
    try:
        period = get_period_from_folder(csv_file_name)
        print(f"Detected period: {period} (from filename interval)")
    except ValueError as e:
        print(f"ERROR: {e}")
        exit(1)

    # Calculate dynamic trade count thresholds
    total_rows = len(df)
    trade_count_threshold_IS = round(IS_OOS_SPLIT * pct_min_trade_count_threshold * total_rows)
    trade_count_threshold_OOS = round((1 - IS_OOS_SPLIT) * pct_min_trade_count_threshold * total_rows)

    print(f"\nTotal data rows: {total_rows}")
    print(f"IS trade count threshold: {trade_count_threshold_IS}")
    print(f"OOS trade count threshold: {trade_count_threshold_OOS}")

    # Split into IS and OOS
    df_is, df_oos_warmup, oos_start_idx = split_data_is_oos(
        df, IS_OOS_SPLIT, MAX_WARMUP_LENGTH, OOS_PRE_WARMUP
    )

    print(f"\nIS rows: {len(df_is)}")
    print(f"OOS rows (with warmup): {len(df_oos_warmup)}")
    print(f"OOS start index: {oos_start_idx}")
    print(f"Effective OOS rows: {len(df_oos_warmup) - oos_start_idx}")

    # Define columns to exclude
    excluded_columns = ['start_time', 'datetime', 'close', 'Unnamed: 0', 'Unnamed: 0.1',
                       'Unnamed: 0_ibkr', 'Unnamed: 0_binance']
    remaining_columns = [col for col in df.columns if col not in excluded_columns]

    print(f"\nFeature columns ({len(remaining_columns)}): {remaining_columns}")

    # ============================================================================
    # PHASE 1: IN-SAMPLE GRID SEARCH
    # ============================================================================

    # Loop through each feature column
    for column in remaining_columns:

        print(f"\n{'='*80}")
        print(f"Processing column: {column}")
        print(f"{'='*80}")

        output_directory = os.path.join(OUTPUT_BASE_DIR, column)
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        # Loop through each model
        for models in model:

            # Extract parameter ranges
            param1 = config['models'][models]['param1']
            param2 = config['models'][models]['param2']
            param1 = get_range(*param1)
            param2 = get_range(*param2)

            model_directory = os.path.join(output_directory, models)

            print(f"\n  Model: {models} | Param1 range: {len(param1)} | Param2 range: {len(param2)}")

            # Loop through each strategy type
            for buy_types in buy_type:

                print(f"    Strategy: {buy_types}", end=" ")

                # ========================================================
                # IN-SAMPLE GRID SEARCH (Sequential)
                # ========================================================

                is_results_list = []
                total_combos = len(param1) * len(param2)
                combo_count = 0

                print(f"({total_combos} combos) ", end="", flush=True)

                # Sequential grid search
                for length in param1:
                    for entry_threshold in param2:
                        combo_count += 1

                        # Show progress every 10%
                        if combo_count % max(1, total_combos // 10) == 0:
                            print(f"{combo_count}/{total_combos}...", end="", flush=True)

                        # Run backtest
                        df_backtest = df_is.copy()
                        df_backtest, log_backtest = generate_all_signals(
                            df_backtest, model=models, buy_type=buy_types, column=column,
                            length=length, entry_threshold=entry_threshold,
                            exit_threshold=0, period=period
                        )

                        # Add metadata
                        log_backtest['model'] = models
                        log_backtest['buy_type'] = buy_types
                        log_backtest['length'] = length
                        log_backtest['entry_threshold'] = entry_threshold
                        log_backtest['exit_threshold'] = 0

                        is_results_list.append(log_backtest)

                print(" Done", end=" ")

                # Convert to DataFrame
                is_results_df = pd.DataFrame(is_results_list)
                is_results_df = is_results_df.dropna()
                is_results_df = is_results_df.sort_values(by='Sharpe Ratio', ascending=False)

                # Filter by thresholds
                is_results_filtered = is_results_df.loc[
                    (is_results_df['Trade Count'] > trade_count_threshold_IS) &
                    (is_results_df['Sharpe Ratio'] > sharpe_ratio_threshold)
                ]

                if is_results_filtered.empty:
                    print("- No IS results passed filters")
                    continue

                print(f"- IS: {len(is_results_filtered)} passed filters", end=" ")

                # Create directories
                if not os.path.exists(model_directory):
                    os.makedirs(model_directory)

                buy_type_directory = os.path.join(model_directory, buy_types)
                if not os.path.exists(buy_type_directory):
                    os.makedirs(buy_type_directory)

                # ========================================================
                # SAVE IN-SAMPLE RESULTS
                # ========================================================

                # Save IS reports
                is_results_df.to_csv(os.path.join(buy_type_directory, 'report.csv'), index=False)
                is_results_filtered.to_csv(os.path.join(buy_type_directory, 'report_filtered.csv'), index=False)

                # Create IS heatmap
                try:
                    pivot_table = is_results_df.pivot_table(
                        index='length', columns='entry_threshold', values='Sharpe Ratio'
                    )
                    fig, ax = plt.subplots(figsize=(20, 20))
                    sns.heatmap(pivot_table, annot=False, fmt=".1f", cmap='Greens', ax=ax)
                    ax.invert_yaxis()
                    plt.title(f'IS: {models} - {buy_types} - Sharpe Ratio Heatmap')
                    plot_path = os.path.join(buy_type_directory, 'heatmap.png')
                    plt.savefig(plot_path, dpi=100)
                    plt.close(fig)
                    plt.clf()
                except Exception as e:
                    print(f"\n      Warning: IS heatmap failed: {e}")
                    plt.close('all')

                # Get best IS result
                is_best = is_results_filtered.iloc[0]

                # Re-run best IS strategy for visualization
                df_is_plot = df_is.copy()
                df_is_plot, log_is_plot = generate_all_signals(
                    df_is_plot, model=is_best['model'], buy_type=is_best['buy_type'],
                    column=column, length=int(is_best['length']),
                    entry_threshold=is_best['entry_threshold'],
                    exit_threshold=is_best['exit_threshold'], period=period
                )

                # Save IS P&L plot and backtest data
                plot_path = os.path.join(buy_type_directory, 'cumu_pnl_vs_bnh.png')
                plot_cumu_pnl_vs_bnh(df_is_plot, save_path=plot_path)
                df_is_plot.to_csv(os.path.join(buy_type_directory, 'backtest.csv'), index=False)

                # ========================================================
                # PHASE 2: OOS PARAMETER SELECTION
                # ========================================================

                if OOS_TOP_N_VALIDATION == 0:
                    # Full grid search on OOS (same parameters as IS)
                    oos_param_list = [
                        (length, entry_threshold)
                        for length in param1
                        for entry_threshold in param2
                    ]
                    oos_mode = "full grid"
                else:
                    # Top-N from IS filtered results
                    oos_param_list = [
                        (int(row['length']), float(row['entry_threshold']))
                        for _, row in is_results_filtered.head(OOS_TOP_N_VALIDATION).iterrows()
                    ]
                    oos_mode = f"top {len(oos_param_list)}"

                print(f"| OOS: {oos_mode} ({len(oos_param_list)} combos) ", end="", flush=True)

                # ========================================================
                # PHASE 3: OUT-OF-SAMPLE VALIDATION (Sequential)
                # ========================================================

                oos_results_list = []
                oos_combo_count = 0
                total_oos_combos = len(oos_param_list)

                for (length, entry_threshold) in oos_param_list:
                    oos_combo_count += 1

                    # Show progress
                    if oos_combo_count % max(1, total_oos_combos // 5) == 0:
                        print(f"{oos_combo_count}/{total_oos_combos}...", end="", flush=True)

                    # Run backtest on OOS data (with warmup)
                    df_oos_test = df_oos_warmup.copy()
                    df_oos_test, _ = generate_all_signals(
                        df_oos_test, model=models, buy_type=buy_types, column=column,
                        length=int(length), entry_threshold=entry_threshold,
                        exit_threshold=0, period=period
                    )

                    # Recalculate metrics without warmup
                    oos_metrics = recalculate_oos_metrics(df_oos_test, oos_start_idx, period)

                    # Combine with parameters
                    oos_log = {
                        'model': models,
                        'buy_type': buy_types,
                        'length': length,
                        'entry_threshold': entry_threshold,
                        'exit_threshold': 0
                    }
                    oos_log.update(oos_metrics)
                    oos_results_list.append(oos_log)

                print(" Done", end=" ")

                # Convert to DataFrame
                oos_results_df = pd.DataFrame(oos_results_list)
                oos_results_df = oos_results_df.dropna()
                oos_results_df = oos_results_df.sort_values(by='Sharpe Ratio', ascending=False)

                # Filter OOS results
                oos_results_filtered = oos_results_df.loc[
                    (oos_results_df['Trade Count'] > trade_count_threshold_OOS) &
                    (oos_results_df['Sharpe Ratio'] > sharpe_ratio_threshold)
                ]

                print(f"- {len(oos_results_filtered)} passed filters [OK]")

                # ========================================================
                # PHASE 4: SAVE OOS RESULTS
                # ========================================================

                # Save OOS reports
                oos_results_df.to_csv(os.path.join(buy_type_directory, 'OOS_report.csv'), index=False)
                oos_results_filtered.to_csv(os.path.join(buy_type_directory, 'OOS_report_filtered.csv'), index=False)

                # Create OOS heatmap (only if full grid)
                if OOS_TOP_N_VALIDATION == 0:
                    try:
                        pivot_table_oos = oos_results_df.pivot_table(
                            index='length', columns='entry_threshold', values='Sharpe Ratio'
                        )
                        fig, ax = plt.subplots(figsize=(20, 20))
                        sns.heatmap(pivot_table_oos, annot=False, fmt=".1f", cmap='Greens', ax=ax)
                        ax.invert_yaxis()
                        plt.title(f'OOS: {models} - {buy_types} - Sharpe Ratio Heatmap')
                        plot_path = os.path.join(buy_type_directory, 'OOS_heatmap.png')
                        plt.savefig(plot_path, dpi=100)
                        plt.close(fig)
                        plt.clf()
                    except Exception as e:
                        print(f"\n      Warning: OOS heatmap failed: {e}")
                        plt.close('all')

                # Get best OOS result (OOS winner)
                if len(oos_results_filtered) > 0:
                    oos_best = oos_results_filtered.iloc[0]
                else:
                    oos_best = oos_results_df.iloc[0]

                # ========================================================
                # PHASE 5: CREATE IS/OOS COMPARISON
                # ========================================================

                # Merge IS and OOS results on parameter columns
                comparison = pd.merge(
                    is_results_df, oos_results_df,
                    on=['model', 'buy_type', 'length', 'entry_threshold', 'exit_threshold'],
                    suffixes=('_IS', '_OOS'), how='inner'
                )

                # Calculate degradation percentages
                comparison['Sharpe_Degradation_%'] = comparison.apply(
                    lambda row: calculate_degradation(row['Sharpe Ratio_IS'], row['Sharpe Ratio_OOS']),
                    axis=1
                )
                comparison['Return_Degradation_%'] = comparison.apply(
                    lambda row: calculate_degradation(row['Annualized Return_IS'], row['Annualized Return_OOS']),
                    axis=1
                )
                comparison['Drawdown_Degradation_%'] = comparison.apply(
                    lambda row: calculate_degradation(row['Max Drawdown_IS'], row['Max Drawdown_OOS']),
                    axis=1
                )
                comparison['Calmar_Degradation_%'] = comparison.apply(
                    lambda row: calculate_degradation(row['Calmar Ratio_IS'], row['Calmar Ratio_OOS']),
                    axis=1
                )

                # Reorder columns
                col_order = [
                    'model', 'buy_type', 'length', 'entry_threshold', 'exit_threshold',
                    'Sharpe Ratio_IS', 'Sharpe Ratio_OOS', 'Sharpe_Degradation_%',
                    'Annualized Return_IS', 'Annualized Return_OOS', 'Return_Degradation_%',
                    'Max Drawdown_IS', 'Max Drawdown_OOS', 'Drawdown_Degradation_%',
                    'Calmar Ratio_IS', 'Calmar Ratio_OOS', 'Calmar_Degradation_%',
                    'Trade Count_IS', 'Trade Count_OOS'
                ]
                comparison = comparison[col_order]
                comparison = comparison.sort_values('Sharpe Ratio_IS', ascending=False)

                # Save comparison CSV
                comparison.to_csv(os.path.join(buy_type_directory, 'IS_OOS_comparison.csv'), index=False)

                # ========================================================
                # PHASE 6: PLOT OOS WINNER AND IS WINNER ON OOS
                # ========================================================

                # Re-run best OOS strategy for visualization
                df_oos_plot = df_oos_warmup.copy()
                df_oos_plot, log_oos_plot = generate_all_signals(
                    df_oos_plot, model=oos_best['model'], buy_type=oos_best['buy_type'],
                    column=column, length=int(oos_best['length']),
                    entry_threshold=oos_best['entry_threshold'],
                    exit_threshold=oos_best['exit_threshold'], period=period
                )

                # Save OOS winner backtest (drop warmup for visualization)
                df_oos_plot_clean = df_oos_plot.iloc[oos_start_idx:].reset_index(drop=True)
                plot_path = os.path.join(buy_type_directory, 'OOS_cumu_pnl_vs_bnh_OOS_winner.png')
                plot_cumu_pnl_vs_bnh(df_oos_plot_clean, save_path=plot_path)
                df_oos_plot_clean.to_csv(os.path.join(buy_type_directory, 'OOS_backtest_OOS_winner.csv'), index=False)

                # Re-run IS winner on OOS data
                df_oos_is_winner = df_oos_warmup.copy()
                df_oos_is_winner, log_oos_is_winner = generate_all_signals(
                    df_oos_is_winner, model=is_best['model'], buy_type=is_best['buy_type'],
                    column=column, length=int(is_best['length']),
                    entry_threshold=is_best['entry_threshold'],
                    exit_threshold=is_best['exit_threshold'], period=period
                )

                # Save IS winner on OOS (drop warmup for visualization)
                df_oos_is_winner_clean = df_oos_is_winner.iloc[oos_start_idx:].reset_index(drop=True)
                plot_path = os.path.join(buy_type_directory, 'OOS_cumu_pnl_vs_bnh_IS_winner.png')
                plot_cumu_pnl_vs_bnh(df_oos_is_winner_clean, save_path=plot_path)
                df_oos_is_winner_clean.to_csv(os.path.join(buy_type_directory, 'OOS_backtest_IS_winner.csv'), index=False)

                # Print summary
                print(f"      IS Best: Sharpe={is_best['Sharpe Ratio']:.2f}, "
                      f"Length={int(is_best['length'])}, Threshold={is_best['entry_threshold']:.2f}")
                print(f"      OOS Best: Sharpe={oos_best['Sharpe Ratio']:.2f}, "
                      f"Length={int(oos_best['length'])}, Threshold={oos_best['entry_threshold']:.2f}")

    print("\n" + "="*80)
    print("PROCESSING COMPLETE!")
    print("="*80)
    print(f"Results saved to: {OUTPUT_BASE_DIR}/")
    print("="*80)
