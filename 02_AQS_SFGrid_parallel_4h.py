"""
AQS_SFGrid_parallel.py
======================

AQS (Algorithmic Quantitative Strategy) Single-Factor Grid Search with IS/OOS Split

This script processes multiple CSV files from the GridSearch_data directory and runs
parallelized grid search optimization with In-Sample/Out-of-Sample validation.

Key Features:
- Split data into In-Sample (IS) and Out-of-Sample (OOS) periods
- Run full grid search on IS data
- Validate on OOS data (full grid or top-N from IS)
- Generate comprehensive IS and OOS performance reports
- Calculate degradation metrics between IS and OOS

Performance: 6-12x speedup compared to sequential execution using all CPU cores.

Output Directory: AQS_SFGridResults/

Dependencies:
    - joblib: For parallel processing
    - tqdm: For progress bars
    - util_AQS_parallel: Core backtesting functions with IS/OOS support

Usage:
    python AQS_SFGrid_parallel.py
"""

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib

matplotlib.use("Agg")  # Use non-interactive backend to avoid threading issues
import matplotlib.pyplot as plt
from util_AQS_parallel import *
import os
from joblib import Parallel, delayed
from tqdm import tqdm
import multiprocessing as mp

# ============================================================================
# CONFIGURATION
# ============================================================================

# Data source
DATA_DIR = "GridSearch_Data"  # Directory containing CSV files to process

OUTPUT_BASE_DIR = "AQS_SFGridResults"  # NEW output directory for IS/OOS workflow

# ============================================================================
# FILTER CONFIGURATION
# Set FILTER_INTERVALS and/or FILTER_SYMBOLS to process only specific files.
# Leave as None to process ALL files in GridSearch_Data.
# Examples:
#   FILTER_INTERVALS = ["4h"]           → only 4h files
#   FILTER_INTERVALS = ["1h", "4h"]     → 1h and 4h only
#   FILTER_SYMBOLS   = ["AAPL","NVDA"]  → only these symbols
# ============================================================================
FILTER_INTERVALS = ["1h"]  # change to None to process all intervals
# FILTER_SYMBOLS   = None     # change to ["AAPL","NVDA"] to filter symbols
# FILTER_SYMBOLS   = ["FIGR", "CRCL", "DTCX", "USAR", "GLD", "OKLO", "PLTR", "XLC", "ETHA", "MP"]     # change to ["AAPL","NVDA"] to filter symbols
# FILTER_SYMBOLS   = ["PLTR", "DTCX", "MGNI", "XME", "COPX", "REMX", "HSBC", "RY", "NEE", "PM", "LMT", "KLAC", "GDXJ", "KTOS", "IBKR"]     # Cindy update list 2-29Mar2026
# FILTER_SYMBOLS   = ["CAT", "JNJ", "NEM", "USO", "XLE", "GS", "FDX", "MS"]
# FILTER_SYMBOLS   = ["AMZN", "CAT"] completed 1D
# FILTER_SYMBOLS   = ["PLTR", "DTCX", "MGNI", "XME", "COPX", "REMX", "HSBC", "RY", "NEE", "PM", "LMT", "KLAC", "GDXJ", "KTOS", "IBKR"]     # 1dcompleted # Cindy update list 2-29Mar2026 -
FILTER_SYMBOLS = [
    "PLTR",
    "DTCX",
    "MGNI",
    "XME",
    "COPX",
    "REMX",
    "HSBC",
    "RY",
    "NEE",
    "PM",
    "LMT",
    "KLAC",
    "GDXJ",
    "KTOS",
    "IBKR",
]  # doing 1h -completed 2Apr2026 -next run stage 3 scripts # Cindy update list 2-29Mar2026     "NEM",

# FILTER_SYMBOLS = ["VDE","XLV","XLP","KO","MCD","WM",]  # new - 7April2026 - next run stage

# IS/OOS Split Configuration
IS_OOS_SPLIT = 0.7  # 70% IS, 30% OOS (default)
OOS_TOP_N_VALIDATION = 10  # 0 = full grid on OOS, N>0 = validate top N from IS
OOS_PRE_WARMUP = "Y"  # "Y" = overlap with IS, "N" = no overlap
MAX_WARMUP_LENGTH = 300  # Warmup period length

# Dynamic Trade Count Threshold
pct_min_trade_count_threshold = 0.015  # Percentage of data rows for minimum trade count

# Models to test (normalization/signal generation methods)
model = ["zscore", "min_max", "sma_diff", "robust_scaler", "maxabs_norm", "rsi"]
# model = ['zscore', 'rsi']  # Lightweight option for testing

# Strategy types to test
buy_type = [
    "trend_long",
    "trend_short",
    "trend_reverse",
    "trend_reverse_long",
    "trend_reverse_short",
    "mr",
    "mr_reverse",
]

# Performance filters
sharpe_ratio_threshold = 1.0  # Minimum Sharpe ratio required
# Note: trade_count_threshold and period are calculated dynamically per file

# Parallelization settings
N_JOBS = -1  # -1 uses all CPU cores, or specify number (e.g., 8)
VERBOSE = 10  # Verbosity level for joblib (0=silent, 10=detailed)

# ============================================================================
# PARALLEL BACKTEST FUNCTION (FOR IS AND OOS)
# ============================================================================


def backtest_single_combo(
    df_data, models, buy_types, column, length, entry_threshold, period
):
    """
    Run a single backtest for one parameter combination.

    This function is designed to be pickle-able for joblib parallel processing.

    Parameters:
    -----------
    df_data : pd.DataFrame
        Input data (must be passed, not referenced globally)
    models : str
        Model name (e.g., 'zscore', 'min_max')
    buy_types : str
        Strategy type (e.g., 'trend_long', 'mr')
    column : str
        Column name to generate signals from
    length : int
        Rolling window length parameter
    entry_threshold : float
        Entry threshold parameter
    period : int
        Period for annualization

    Returns:
    --------
    dict : Performance metrics and parameters
    """
    try:
        # Create a copy to avoid any reference issues
        df_backtest = df_data.copy()

        # Run backtest
        df_backtest, log_backtest = generate_all_signals(
            df_backtest,
            model=models,
            buy_type=buy_types,
            column=column,
            length=length,
            entry_threshold=entry_threshold,
            exit_threshold=0,
            period=period,
        )

        # Add metadata
        log_backtest.update(
            {
                "model": models,
                "buy_type": buy_types,
                "length": length,
                "entry_threshold": entry_threshold,
                "exit_threshold": 0,
            }
        )

        return log_backtest

    except Exception as e:
        # Return None for failed backtests (will be filtered out)
        return None


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def select_top_n_from_report(report_path, top_n):
    """
    Load report_filtered.csv and select top N configurations by Sharpe Ratio.

    Parameters:
    -----------
    report_path : str
        Path to report_filtered.csv
    top_n : int
        Number of top configurations to select

    Returns:
    --------
    list of tuples : [(length, entry_threshold), ...]
    """
    try:
        df_report = pd.read_csv(report_path)
        df_report = df_report.sort_values("Sharpe Ratio", ascending=False)
        df_top = df_report.head(top_n)

        # Extract parameter tuples
        params = [
            (int(row["length"]), float(row["entry_threshold"]))
            for _, row in df_top.iterrows()
        ]
        return params

    except Exception as e:
        print(f"      Warning: Could not load {report_path}: {e}")
        return []


def create_is_oos_comparison(is_results_df, oos_results_df):
    """
    Create IS/OOS comparison DataFrame with degradation metrics.

    Parameters:
    -----------
    is_results_df : pd.DataFrame
        In-sample results
    oos_results_df : pd.DataFrame
        Out-of-sample results

    Returns:
    --------
    pd.DataFrame : Combined IS/OOS comparison with degradation
    """
    # Merge on parameter columns
    comparison = pd.merge(
        is_results_df,
        oos_results_df,
        on=["model", "buy_type", "length", "entry_threshold", "exit_threshold"],
        suffixes=("_IS", "_OOS"),
        how="inner",
    )

    # Calculate degradation percentages
    comparison["Sharpe_Degradation_%"] = comparison.apply(
        lambda row: calculate_degradation(
            row["Sharpe Ratio_IS"], row["Sharpe Ratio_OOS"]
        ),
        axis=1,
    )
    comparison["Return_Degradation_%"] = comparison.apply(
        lambda row: calculate_degradation(
            row["Annualized Return_IS"], row["Annualized Return_OOS"]
        ),
        axis=1,
    )
    comparison["Drawdown_Degradation_%"] = comparison.apply(
        lambda row: calculate_degradation(
            row["Max Drawdown_IS"], row["Max Drawdown_OOS"]
        ),
        axis=1,
    )
    comparison["Calmar_Degradation_%"] = comparison.apply(
        lambda row: calculate_degradation(
            row["Calmar Ratio_IS"], row["Calmar Ratio_OOS"]
        ),
        axis=1,
    )

    # Reorder columns for better readability
    col_order = [
        "model",
        "buy_type",
        "length",
        "entry_threshold",
        "exit_threshold",
        "Sharpe Ratio_IS",
        "Sharpe Ratio_OOS",
        "Sharpe_Degradation_%",
        "Annualized Return_IS",
        "Annualized Return_OOS",
        "Return_Degradation_%",
        "Max Drawdown_IS",
        "Max Drawdown_OOS",
        "Drawdown_Degradation_%",
        "Calmar Ratio_IS",
        "Calmar Ratio_OOS",
        "Calmar_Degradation_%",
        "Trade Count_IS",
        "Trade Count_OOS",
    ]

    comparison = comparison[col_order]
    comparison = comparison.sort_values("Sharpe Ratio_IS", ascending=False)

    return comparison


# ============================================================================
# MAIN PROCESSING LOOP
# ============================================================================

if __name__ == "__main__":
    # Create base output directory
    if not os.path.exists(OUTPUT_BASE_DIR):
        os.makedirs(OUTPUT_BASE_DIR)

    # Get list of all CSV files in the data directory
    csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]

    # Filter by interval and/or symbol
    def parse_filename(fname):
        parts = fname.replace(".csv", "").split("_")
        if len(parts) >= 4 and parts[0] == "merged":
            return parts[2], parts[3]
        return None, None

    all_count = len(csv_files)
    if FILTER_INTERVALS or FILTER_SYMBOLS:
        filtered = []
        for f in csv_files:
            sym, ivl = parse_filename(f)
            if FILTER_INTERVALS and ivl not in FILTER_INTERVALS:
                continue
            if FILTER_SYMBOLS and sym not in FILTER_SYMBOLS:
                continue
            filtered.append(f)
        csv_files = filtered
        print(f"Filter applied → {len(csv_files)} of {all_count} file(s) selected")
        if FILTER_INTERVALS:
            print(f"  Intervals : {FILTER_INTERVALS}")
        if FILTER_SYMBOLS:
            print(f"  Symbols   : {FILTER_SYMBOLS}")

    print(f"{'=' * 80}")
    print(f"AQS SINGLE-FACTOR GRID SEARCH WITH IS/OOS VALIDATION")
    print(f"{'=' * 80}")
    print(f"Output Directory: {OUTPUT_BASE_DIR}/")
    print(f"Found {len(csv_files)} CSV file(s) to process")
    print(f"Using {mp.cpu_count()} CPU cores for parallel processing")
    print(f"\nIS/OOS Configuration:")
    print(
        f"  IS/OOS Split: {IS_OOS_SPLIT * 100:.0f}% IS, {(1 - IS_OOS_SPLIT) * 100:.0f}% OOS"
    )
    print(f"  OOS Warmup: {MAX_WARMUP_LENGTH} bars")
    print(
        f"  OOS Pre-Warmup: {OOS_PRE_WARMUP} (overlap {'allowed' if OOS_PRE_WARMUP == 'Y' else 'not allowed'})"
    )
    print(
        f"  OOS Validation: {'Full grid search' if OOS_TOP_N_VALIDATION == 0 else f'Top {OOS_TOP_N_VALIDATION} from IS'}"
    )
    print(f"\nModels: {model}")
    print(f"Strategy types: {buy_type}")
    print(
        f"Filters: Trade Count dynamic (IS/OOS specific), Sharpe >= {sharpe_ratio_threshold}"
    )
    print(f"Trade Count %: {pct_min_trade_count_threshold * 100}% of data rows")
    print("=" * 80)

    # Loop through each CSV file
    for csv_file in csv_files:
        print(f"\n{'=' * 80}")
        print(f"Processing: {csv_file}")
        print(f"{'=' * 80}")

        # ====================================================================
        # PHASE 1: DATA LOADING & SPLITTING
        # ====================================================================

        # Load data
        file_path = os.path.join(DATA_DIR, csv_file)
        df = pd.read_csv(file_path)
        df = df.dropna()

        # ====================================================================
        # CALCULATE DYNAMIC PERIOD AND TRADE COUNT THRESHOLDS
        # ====================================================================

        # FIX: Parse interval from position [3] of filename instead of using
        # get_period_from_folder() which false-matches dates (e.g. "18Mar" as "1m").
        # Period values match util_AQS_parallel.py exactly for consistency.
        csv_file_name = os.path.splitext(os.path.basename(file_path))[0]
        INTERVAL_TO_PERIOD = {
            "1min": 391,
            "5min": 391,
            "15min": 391,
            "30min": 391,
            "1h": 252,
            "4h": 504,  # 252 * 2 — matches util_AQS_parallel.py
            "1d": 252,
            "1w": 52,
        }
        try:
            fname_parts = csv_file_name.split("_")
            interval_tag = fname_parts[3] if len(fname_parts) >= 4 else None
            if interval_tag not in INTERVAL_TO_PERIOD:
                raise ValueError(
                    f"Unrecognised interval '{interval_tag}' at position 3 of filename"
                )
            period = INTERVAL_TO_PERIOD[interval_tag]
            print(f"Detected interval: {interval_tag} -> period={period}")
        except (IndexError, ValueError) as e:
            print(f"ERROR: {e}")
            print(f"Skipping file: {csv_file}")
            continue

        # Calculate dynamic trade count thresholds based on data size
        total_rows = len(df)
        trade_count_threshold_IS = round(
            IS_OOS_SPLIT * pct_min_trade_count_threshold * total_rows
        )
        trade_count_threshold_OOS = round(
            (1 - IS_OOS_SPLIT) * pct_min_trade_count_threshold * total_rows
        )

        print(f"Total data rows: {total_rows}")
        print(
            f"IS trade count threshold: {trade_count_threshold_IS} ({pct_min_trade_count_threshold * 100}% of IS window)"
        )
        print(
            f"OOS trade count threshold: {trade_count_threshold_OOS} ({pct_min_trade_count_threshold * 100}% of OOS window)"
        )

        # Split into IS and OOS
        df_is, df_oos_warmup, oos_start_idx = split_data_is_oos(
            df, IS_OOS_SPLIT, MAX_WARMUP_LENGTH, OOS_PRE_WARMUP
        )

        print(f"IS rows: {len(df_is)} ({IS_OOS_SPLIT * 100:.0f}%)")
        print(f"OOS rows (with warmup): {len(df_oos_warmup)}")
        print(f"OOS start index: {oos_start_idx} (warmup bars: {oos_start_idx})")
        print(f"Effective OOS rows: {len(df_oos_warmup) - oos_start_idx}")

        # Define columns to exclude from backtesting
        excluded_columns = [
            "start_time",
            "datetime",
            "close",
            "Unnamed: 0",
            "Unnamed: 0.1",
            "Unnamed: 0_ibkr",
            "Unnamed: 0_binance",
        ]
        remaining_columns = [col for col in df.columns if col not in excluded_columns]

        print(f"Feature columns ({len(remaining_columns)}): {remaining_columns}")

        # Create output directory for this CSV file
        csv_file_name = os.path.splitext(os.path.basename(file_path))[0]
        csv_directory = os.path.join(OUTPUT_BASE_DIR, csv_file_name)

        if not os.path.exists(csv_directory):
            os.makedirs(csv_directory)

        # ====================================================================
        # PHASE 2: IN-SAMPLE GRID SEARCH
        # ====================================================================

        # Loop through each feature column
        for col in remaining_columns:
            print(f"\n--- Processing column: {col} ---")

            output_directory = os.path.join(csv_directory, col)
            column = col

            if not os.path.exists(output_directory):
                os.makedirs(output_directory)

            # Loop through each model
            for models in model:
                # Extract parameter ranges
                param1 = config["models"][models]["param1"]
                param2 = config["models"][models]["param2"]
                param1 = get_range(*param1)
                param2 = get_range(*param2)

                model_directory = os.path.join(output_directory, models)

                print(
                    f"  Model: {models} | Param1 range: {len(param1)} | Param2 range: {len(param2)}"
                )

                # Loop through each strategy type
                for buy_types in buy_type:
                    print(f"    Strategy: {buy_types}", end=" ")

                    # ========================================================
                    # IN-SAMPLE PARALLEL GRID SEARCH
                    # ========================================================

                    # Create all parameter combinations for IS
                    param_combinations = [
                        (
                            df_is,
                            models,
                            buy_types,
                            column,
                            length,
                            entry_threshold,
                            period,
                        )
                        for length in param1
                        for entry_threshold in param2
                    ]

                    total_combos = len(param_combinations)
                    print(f"({total_combos} combos) ", end="", flush=True)

                    # Run parallel backtests on IS data
                    is_results_list = Parallel(n_jobs=N_JOBS, verbose=0)(
                        delayed(backtest_single_combo)(*params)
                        for params in tqdm(
                            param_combinations,
                            desc=f"    IS {models}-{buy_types}",
                            leave=False,
                            disable=True,
                        )
                    )

                    # Filter out None results
                    is_results_list = [r for r in is_results_list if r is not None]

                    if not is_results_list:
                        print("- No valid IS results")
                        continue

                    # Convert to DataFrame
                    is_results_df = pd.DataFrame(is_results_list)
                    is_results_df = is_results_df.dropna()
                    is_results_df = is_results_df.sort_values(
                        by="Sharpe Ratio", ascending=False
                    )

                    # Filter by thresholds
                    is_results_filtered = is_results_df.loc[
                        (is_results_df["Trade Count"] > trade_count_threshold_IS)
                        & (is_results_df["Sharpe Ratio"] > sharpe_ratio_threshold)
                    ]

                    if is_results_filtered.empty:
                        print("- No IS results passed filters")
                        continue

                    print(f"- IS: {len(is_results_filtered)} passed filters", end=" ")

                    # Create directories for output
                    if not os.path.exists(model_directory):
                        os.makedirs(model_directory)

                    buy_type_directory = os.path.join(model_directory, buy_types)
                    if not os.path.exists(buy_type_directory):
                        os.makedirs(buy_type_directory)

                    # ========================================================
                    # SAVE IN-SAMPLE RESULTS
                    # ========================================================

                    # Save IS reports
                    is_results_df.to_csv(
                        os.path.join(buy_type_directory, "report.csv"), index=False
                    )
                    is_results_filtered.to_csv(
                        os.path.join(buy_type_directory, "report_filtered.csv"),
                        index=False,
                    )

                    # Create IS heatmap
                    try:
                        pivot_table = is_results_df.pivot_table(
                            index="length",
                            columns="entry_threshold",
                            values="Sharpe Ratio",
                        )
                        fig, ax = plt.subplots(figsize=(20, 20))
                        sns.heatmap(
                            pivot_table, annot=False, fmt=".1f", cmap="Greens", ax=ax
                        )
                        ax.invert_yaxis()  # Y-axis ascending from bottom to top
                        plt.title(f"IS: {models} - {buy_types} - Sharpe Ratio Heatmap")
                        plot_path = os.path.join(buy_type_directory, "heatmap.png")
                        plt.savefig(plot_path, dpi=100)
                        plt.close(fig)
                        plt.clf()
                    except Exception as e:
                        print(f"\n      Warning: IS heatmap failed: {e}")
                        plt.close("all")

                    # Get best IS result
                    is_best = is_results_filtered.iloc[0]

                    # Re-run best IS strategy for visualization
                    df_is_plot = df_is.copy()
                    df_is_plot, log_is_plot = generate_all_signals(
                        df_is_plot,
                        model=is_best["model"],
                        buy_type=is_best["buy_type"],
                        column=column,
                        length=int(is_best["length"]),
                        entry_threshold=is_best["entry_threshold"],
                        exit_threshold=is_best["exit_threshold"],
                        period=period,
                    )

                    # Save IS P&L plot and backtest data
                    plot_path = os.path.join(buy_type_directory, "cumu_pnl_vs_bnh.png")
                    plot_cumu_pnl_vs_bnh(df_is_plot, save_path=plot_path)
                    df_is_plot.to_csv(
                        os.path.join(buy_type_directory, "backtest.csv"), index=False
                    )

                    # ========================================================
                    # PHASE 3: OOS PARAMETER SELECTION
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
                            (int(row["length"]), float(row["entry_threshold"]))
                            for _, row in is_results_filtered.head(
                                OOS_TOP_N_VALIDATION
                            ).iterrows()
                        ]
                        oos_mode = f"top {len(oos_param_list)}"

                    print(
                        f"| OOS: {oos_mode} ({len(oos_param_list)} combos) ",
                        end="",
                        flush=True,
                    )

                    # ========================================================
                    # PHASE 4: OUT-OF-SAMPLE VALIDATION
                    # ========================================================

                    # Create parameter combinations for OOS
                    oos_param_combinations = [
                        (
                            df_oos_warmup,
                            models,
                            buy_types,
                            column,
                            length,
                            entry_threshold,
                            period,
                        )
                        for (length, entry_threshold) in oos_param_list
                    ]

                    # Run parallel backtests on OOS data (with warmup)
                    oos_results_full_list = Parallel(n_jobs=N_JOBS, verbose=0)(
                        delayed(backtest_single_combo)(*params)
                        for params in tqdm(
                            oos_param_combinations,
                            desc=f"    OOS {models}-{buy_types}",
                            leave=False,
                            disable=True,
                        )
                    )

                    # Recalculate OOS metrics (drop warmup)
                    oos_results_list = []
                    for result in oos_results_full_list:
                        if result is not None:
                            # Need to re-run backtest to get DataFrame, then recalculate metrics
                            df_oos_test = df_oos_warmup.copy()
                            df_oos_test, _ = generate_all_signals(
                                df_oos_test,
                                model=result["model"],
                                buy_type=result["buy_type"],
                                column=column,
                                length=int(result["length"]),
                                entry_threshold=result["entry_threshold"],
                                exit_threshold=result["exit_threshold"],
                                period=period,
                            )

                            # Recalculate metrics without warmup
                            oos_metrics = recalculate_oos_metrics(
                                df_oos_test, oos_start_idx, period
                            )

                            # Combine with parameters
                            oos_log = {
                                "model": result["model"],
                                "buy_type": result["buy_type"],
                                "length": result["length"],
                                "entry_threshold": result["entry_threshold"],
                                "exit_threshold": result["exit_threshold"],
                            }
                            oos_log.update(oos_metrics)
                            oos_results_list.append(oos_log)

                    if not oos_results_list:
                        print("- No valid OOS results")
                        continue

                    # Convert to DataFrame
                    oos_results_df = pd.DataFrame(oos_results_list)
                    oos_results_df = oos_results_df.dropna()

                    # Check if we have any results after dropna
                    if len(oos_results_df) == 0:
                        print("- No valid OOS results after filtering NaN")
                        continue

                    oos_results_df = oos_results_df.sort_values(
                        by="Sharpe Ratio", ascending=False
                    )

                    # Filter OOS results
                    oos_results_filtered = oos_results_df.loc[
                        (oos_results_df["Trade Count"] > trade_count_threshold_OOS)
                        & (oos_results_df["Sharpe Ratio"] > sharpe_ratio_threshold)
                    ]

                    print(f"- {len(oos_results_filtered)} passed filters [OK]")

                    # ========================================================
                    # PHASE 5: SAVE OOS RESULTS
                    # ========================================================

                    # Save OOS reports
                    oos_results_df.to_csv(
                        os.path.join(buy_type_directory, "OOS_report.csv"), index=False
                    )
                    oos_results_filtered.to_csv(
                        os.path.join(buy_type_directory, "OOS_report_filtered.csv"),
                        index=False,
                    )

                    # Create OOS heatmap (only if full grid)
                    if OOS_TOP_N_VALIDATION == 0:
                        try:
                            pivot_table_oos = oos_results_df.pivot_table(
                                index="length",
                                columns="entry_threshold",
                                values="Sharpe Ratio",
                            )
                            fig, ax = plt.subplots(figsize=(20, 20))
                            sns.heatmap(
                                pivot_table_oos,
                                annot=False,
                                fmt=".1f",
                                cmap="Greens",
                                ax=ax,
                            )
                            ax.invert_yaxis()  # Y-axis ascending from bottom to top
                            plt.title(
                                f"OOS: {models} - {buy_types} - Sharpe Ratio Heatmap"
                            )
                            plot_path = os.path.join(
                                buy_type_directory, "OOS_heatmap.png"
                            )
                            plt.savefig(plot_path, dpi=100)
                            plt.close(fig)
                            plt.clf()
                        except Exception as e:
                            print(f"\n      Warning: OOS heatmap failed: {e}")
                            plt.close("all")

                    # Get best OOS result (OOS winner)
                    if len(oos_results_filtered) > 0:
                        oos_best = oos_results_filtered.iloc[0]
                    else:
                        oos_best = oos_results_df.iloc[0]

                    # Re-run best OOS strategy for visualization
                    df_oos_plot = df_oos_warmup.copy()
                    df_oos_plot, log_oos_plot = generate_all_signals(
                        df_oos_plot,
                        model=oos_best["model"],
                        buy_type=oos_best["buy_type"],
                        column=column,
                        length=int(oos_best["length"]),
                        entry_threshold=oos_best["entry_threshold"],
                        exit_threshold=oos_best["exit_threshold"],
                        period=period,
                    )

                    # Save OOS winner backtest (drop warmup for visualization)
                    df_oos_plot_clean = df_oos_plot.iloc[oos_start_idx:].reset_index(
                        drop=True
                    )
                    plot_path = os.path.join(
                        buy_type_directory, "OOS_cumu_pnl_vs_bnh_OOS_winner.png"
                    )
                    plot_cumu_pnl_vs_bnh(df_oos_plot_clean, save_path=plot_path)
                    df_oos_plot_clean.to_csv(
                        os.path.join(buy_type_directory, "OOS_backtest_OOS_winner.csv"),
                        index=False,
                    )

                    # Re-run IS winner on OOS data
                    df_oos_is_winner = df_oos_warmup.copy()
                    df_oos_is_winner, log_oos_is_winner = generate_all_signals(
                        df_oos_is_winner,
                        model=is_best["model"],
                        buy_type=is_best["buy_type"],
                        column=column,
                        length=int(is_best["length"]),
                        entry_threshold=is_best["entry_threshold"],
                        exit_threshold=is_best["exit_threshold"],
                        period=period,
                    )

                    # Save IS winner on OOS (drop warmup for visualization)
                    df_oos_is_winner_clean = df_oos_is_winner.iloc[
                        oos_start_idx:
                    ].reset_index(drop=True)
                    plot_path = os.path.join(
                        buy_type_directory, "OOS_cumu_pnl_vs_bnh_IS_winner.png"
                    )
                    plot_cumu_pnl_vs_bnh(df_oos_is_winner_clean, save_path=plot_path)
                    df_oos_is_winner_clean.to_csv(
                        os.path.join(buy_type_directory, "OOS_backtest_IS_winner.csv"),
                        index=False,
                    )

                    # ========================================================
                    # PHASE 6: CREATE IS/OOS COMPARISON
                    # ========================================================

                    comparison_df = create_is_oos_comparison(
                        is_results_df, oos_results_df
                    )
                    comparison_df.to_csv(
                        os.path.join(buy_type_directory, "IS_OOS_comparison.csv"),
                        index=False,
                    )

                    # Print summary
                    print(
                        f"      IS Best: Sharpe={is_best['Sharpe Ratio']:.2f}, "
                        f"Length={int(is_best['length'])}, Threshold={is_best['entry_threshold']:.2f}"
                    )
                    print(
                        f"      OOS Best: Sharpe={oos_best['Sharpe Ratio']:.2f}, "
                        f"Length={int(oos_best['length'])}, Threshold={oos_best['entry_threshold']:.2f}"
                    )

    print("\n" + "=" * 80)
    print("PROCESSING COMPLETE!")
    print("=" * 80)
    print(f"Results saved to: {OUTPUT_BASE_DIR}/")
    print("=" * 80)
