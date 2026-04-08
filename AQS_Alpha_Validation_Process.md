# AQS_SFGS Alpha Validation Pipeline - Complete Technical Documentation

**Version:** 2.0
**Last Updated:** 2026-01-09
**Author:** CQA Team

---

## Table of Contents

1. [Overview](#1-overview)
2. [Configuration](#2-configuration)
3. [The 7 Stages](#3-the-7-stages)
4. [FunnelTracker Integration](#4-funneltracker-integration)
5. [Execution Model](#5-execution-model)
6. [Data Flow](#6-data-flow)
7. [Command-Line Interface](#7-command-line-interface)
8. [Performance Characteristics](#8-performance-characteristics)
9. [Error Handling and Validation](#9-error-handling-and-validation)
10. [Directory Structure](#10-directory-structure)

---

## 1. Overview

### 1.1 Purpose

The AQS_SFGS (Alpha Quant System - Signal Factory Grid Search) Pipeline is a 7-stage automated system that orchestrates the end-to-end validation and optimization of quantitative trading strategies for Interactive Brokers (IBKR) US equities.

### 1.2 Core Architecture

**Key Characteristics:**
- **Execution Model:** Sequential Interval → Sequential Stage → Parallel Symbol
- **Symbols:** 670+ US equities (NVDA, AAPL, MSFT, AMZN, GOOGL, etc.)
- **Intervals:** 1h, 4h, 1d
- **Checkpoint Support:** Resume from any stage after interruption
- **Error Isolation:** Symbol failures don't block pipeline progress
- **FunnelTracker Integration:** Comprehensive metrics at each stage
- **Grid Search Optimization:** Systematic parameter optimization with IS/OOS validation

### 1.3 Main Orchestration Files

| File | Purpose | Location |
|------|---------|----------|
| [run_ibkr_gs_pipeline.py](run_ibkr_gs_pipeline.py) | Main pipeline orchestrator | Root directory |
| [AQS_SFGrid_parallel.py](AQS_SFGrid_parallel.py) | Grid search parallelization engine | Root directory |
| [util_AQS_parallel.py](util_AQS_parallel.py) | Core utility functions | Root directory |
| [funnel_tracker.py](funnel_tracker.py) | Metrics and reporting | Root directory |

### 1.4 Pipeline Flow

```
Input Data (GridSearch_Data/)
    ↓
Stage 1: Alpha Compilation → AQS_SFGridResults/Alpha_GS_Compilation_*.xlsx
    ↓
Stage 2: Walk-Forward Validation → walk_forward_report.csv (nested)
    ↓
Stage 3: WF Results Compilation → AQS_SFGridResults/WF_GS_Compilation_*.xlsx
    ↓
Stage 4: WF Alpha Generation → WFAlphaResults/ (nested backtests)
    ↓
Stage 5: WF Alpha Compilation → WFAlphaResults/WFAlpha_Compilation_*.xlsx
    ↓
Stage 6: Combination Strategies → WFAlphaResults/Combination_Strategy_Compilation_*.xlsx
    ↓
Stage 7: Final Compilation → WFAlphaResults/Final_Compilation_*.xlsx
```

---

## 2. Configuration

### 2.1 Exchange and Symbols

| Parameter | Value | Notes |
|-----------|-------|-------|
| EXCHANGE | `"ibkr"` | Interactive Brokers data |
| SYMBOLS | `["NVDA", "AAPL", "MSFT", ...]` | 670+ US equity symbols |
| INTERVAL | `"1h"` | Also supports `"4h"`, `"1d"` |

**Symbol Coverage:**
- **Total:** 670+ US equity symbols
- **Major Tech:** NVDA, AAPL, MSFT, AMZN, GOOGL, META, TSLA
- **Financials:** JPM, BAC, GS, MS, C, WFC
- **Healthcare:** UNH, JNJ, MRK, ABT, LLY
- **Other Sectors:** Full S&P 500 coverage plus additional liquid equities
- **Note:** Pipeline designed for IBKR equity market data (8 trading hours/day, 252 trading days/year)

### 2.2 Intervals and Annualization

| Interval | Candles/Year | Min Trade Count | Annualization Period |
|----------|--------------|-----------------|---------------------|
| 1h | 252 × 8 = 2,016 | 260 | 2016 |
| 4h | 252 × 2 = 504 | 130 | 504 |
| 1d | 252 | 75 | 252 |

**Formula:**
```
annualization_period = 252 (trading days) × candles_per_day
```

### 2.3 Grid Search Parameters

| Parameter | Value | Purpose |
|-----------|-------|---------|
| IS_OOS_SPLIT | 0.70 | 70% IS, 30% OOS for grid search |
| OOS_TOP_N_VALIDATION | 10 | Validate top 10 configs from IS on OOS |
| OOS_PRE_WARMUP | "Y" | Overlap warmup with IS period |
| MAX_WARMUP_LENGTH | 300 | Indicator lookback period |

### 2.4 Walk-Forward Parameters

| Parameter | Value | Purpose |
|-----------|-------|---------|
| IN_SAMPLE_RATIO | 0.50 | 50% of data for training |
| MAX_WARMUP_LENGTH | 300 | Indicator lookback period |
| MIN_DATA_LENGTH | 1200 | Minimum rows required |
| TOP_N_ROWS | 10 | Top configurations per strategy (from IS_OOS_comparison) |
| DEGRADATION_MIN | -10% | Minimum allowed Sharpe degradation |

**Data Split Strategy:**
```
Total Data = 100%
├── IS Period:     0% to 50%   (training)
├── WF1 Period:   25% to 75%   (validation 1, with warmup)
└── WF2 Period:   50% to 100%  (validation 2, with warmup)
```

This creates overlapping windows for independent validation on different data subsets.

### 2.5 Correlation and Portfolio Parameters

| Parameter | Value | Purpose |
|-----------|-------|---------|
| CORRELATION_THRESHOLDS | `[0.7, 0.6, 0.5]` | Portfolio diversification levels |
| FINAL_THRESHOLD | `0.5` | Used in Stage 7 for final portfolio |
| ALLOCATION_METHOD | Equal-weighted | Avoids overfitting weights |

### 2.6 Quality Thresholds

| Metric | Threshold | Applied At |
|--------|-----------|-----------|
| Minimum Trade Count | 1.5% of data rows | Stage 1 (pct_min_trade_count_threshold) |
| Minimum Sharpe Ratio | 1.0 | Grid search filtering |
| Maximum Sharpe Degrade | -10% | Alpha compilation (DEGRADATION_MIN) |
| Minimum Data Rows | 1200 | Stage 2 validation |

---

## 3. The 7 Stages

### Stage 1: Alpha Compilation

**Script:** [AQS_SFGridResults/compile_ibkr_gs_alphas.py](AQS_SFGridResults/compile_ibkr_gs_alphas.py)

**Purpose:** Extract the best 10 configurations from each strategy's IS/OOS grid search results.

#### Input
- **Source:** `AQS_SFGridResults/merged_{exchange}_{symbol}_{interval}_*` directories
- **Pattern:** Uses glob to find folders like `merged_ibkr_NVDA_1h_wiho_15Dec2025`
- **Files:** `IS_OOS_comparison.csv` files nested in `feature/model/strategy/` hierarchy
- **Format:** CSV with columns: IS_Sharpe, OOS_Sharpe, Sharpe_Degradation_%, Trade Count, etc.

#### Processing Logic
```python
For each symbol:
  1. Discover all folders matching: merged_{exchange}_{symbol}_{interval}_*
  2. Recursively find all IS_OOS_comparison.csv files
  3. For each IS_OOS_comparison.csv:
     - Filter entries with Sharpe_Degradation_% >= DEGRADATION_MIN (-10%)
     - Extract top TOP_N_ROWS (10) configurations
     - Parse path to extract feature, model, strategy
     - Calculate performance metrics
  4. Combine all results into single DataFrame
  5. Sort by OOS Sharpe Ratio (descending)
```

#### Output
- **File:** `AQS_SFGridResults/Alpha_GS_Compilation_ibkr_1h_{YYYYMMDD}.xlsx`
- **Worksheets:**
  - Alpha Full_Compilation: All configurations meeting criteria
  - Alpha_Short: Deduplicated best per strategy type
- **Columns:**
  - Metadata: Exchange, Symbol, Interval, Data Point, Model, Entry/Exit Model
  - Parameters: Length, Entry, Exit
  - Metrics: IS Sharpe, OOS Sharpe, Sharpe_Degradation_%, MDD, Trade Count, Annual Return, Calmar Ratio
  - Performance: Cumulative PnL, Buy & Hold, PnL Ratio
- **Sorting:** By OOS Sharpe Ratio (descending)

#### Configuration
```python
EXCHANGE = "ibkr"
INTERVAL = "1h"
TOP_N_ROWS = 10
DEGRADATION_MIN = -10  # >= -10% degradation allowed
```

#### Performance
- **Runtime:** ~100 seconds per symbol (parallel execution)
- **Output Size:** ~263 MB (670+ symbols)

---

### Stage 2: Walk-Forward Validation

**Script:** [AQS_SFGridResults/validate_ibkr_gs_walk_forward.py](AQS_SFGridResults/validate_ibkr_gs_walk_forward.py)

**Purpose:** Perform walk-forward validation on configurations to test robustness on unseen data.

#### Input
- **Source 1:** Alpha_GS_Compilation from Stage 1
- **Source 2:** Original OHLCV data from `GridSearch_Data/merged_ibkr_{symbol}_{interval}_*.csv`

#### Processing Logic
```python
For each configuration from Alpha Compilation:
  1. Load merged data CSV using glob pattern
  2. Validate minimum data length (1200 rows)
  3. Split data into walk-forward windows:
     - IS Period:  rows[0:50%]
     - WF1 Period: rows[25%-warmup:75%] (with warmup from 25%-300 candles)
     - WF2 Period: rows[50%-warmup:100%] (with warmup from 50%-300 candles)
  4. For each period (IS, WF1, WF2):
     - Generate trading signals using util_AQS_parallel.generate_all_signals()
     - Calculate metrics:
       * Trade Count
       * Sharpe Ratio (annualized)
       * Max Drawdown
       * Annualized Return
       * Calmar Ratio
  5. Validate minimum trade counts
  6. Write walk_forward_report.csv to strategy folder
```

#### Output
- **Location:** `AQS_SFGridResults/merged_ibkr_{symbol}_{interval}_*/feature/model/strategy/walk_forward_report.csv`
- **Columns:**
  - IS Metrics: IS Trade Count, IS Sharpe Ratio, IS Max Drawdown, IS Annualized Return, IS Calmar Ratio
  - WF1 Metrics: WF1 Trade Count, WF1 Sharpe Ratio, WF1 Max Drawdown, WF1 Annualized Return, WF1 Calmar Ratio
  - WF2 Metrics: WF2 Trade Count, WF2 Sharpe Ratio, WF2 Max Drawdown, WF2 Annualized Return, WF2 Calmar Ratio
- **Format:** CSV files scattered across folder hierarchy (one per strategy)

#### Data Split Diagram
```
Total Dataset: 1200+ rows
│
├─ IS Period (0-50%): 600 rows
│  └─ Used for: Training metrics
│
├─ WF1 Period (25%-75%): 600 rows
│  ├─ Warmup (25%-300 candles): Indicator initialization
│  └─ Validation (actual period): Performance testing
│
└─ WF2 Period (50%-100%): 600 rows
   ├─ Warmup (50%-300 candles): Indicator initialization
   └─ Validation (actual period): Performance testing
```

#### Configuration
```python
IN_SAMPLE_RATIO = 0.50
TOP_N = 5
MAX_WARMUP_LENGTH = 300
MIN_DATA_LENGTH = 1200
ANNUALIZATION_PERIOD = 252 * 8  # 2016 for 1h
```

#### Performance
- **Runtime:** ~290 seconds per symbol
- **Output:** One CSV per strategy (distributed across folders)

---

### Stage 3: WF Results Compilation

**Script:** [AQS_SFGridResults/compile_ibkr_gs_walk_forward_results.py](AQS_SFGridResults/compile_ibkr_gs_walk_forward_results.py)

**Purpose:** Aggregate all walk-forward results and calculate degradation metrics.

#### Input
- **Source:** All `walk_forward_report.csv` files from Stage 2
- **Pattern:** Recursive search in `AQS_SFGridResults/merged_{exchange}_{symbol}_{interval}_*/**/walk_forward_report.csv`

#### Processing Logic
```python
For each symbol:
  1. Find all walk_forward_report.csv files using glob
  2. Extract metadata from file path:
     - Exchange, Symbol, Interval from folder name
     - Feature, Model, Buy_Type from directory structure
  3. For each row in each CSV:
     # Calculate degradation metrics
     WF1 Sharpe Degrade = (WF1_Sharpe - IS_Sharpe) / IS_Sharpe
     WF2 Sharpe Degrade = (WF2_Sharpe - IS_Sharpe) / IS_Sharpe
     L Sharpe Degrade = min(WF1_Sharpe_Degrade, WF2_Sharpe_Degrade)  # Worst case

     # Similar for MDD, Annual Return, Calmar (MDD is inverted: lower is better)
  4. Combine all results
  5. Sort by L Sharpe Degrade (descending)
```

#### Degradation Formula
```python
def calculate_degradation(os_value, is_value, invert=False):
    """
    Calculate degradation percentage.

    Args:
        os_value: Out-of-sample metric value
        is_value: In-sample metric value
        invert: True for metrics where lower is better (e.g., MDD)

    Returns:
        Degradation percentage (positive = improvement, negative = degradation)
    """
    degradation = (os_value - is_value) / is_value
    if invert:
        degradation = -1 * degradation
    return degradation

# L Degrade (Least/Worst degradation)
l_degrade = min(wf1_degrade, wf2_degrade)
```

#### Output
- **File:** `AQS_SFGridResults/WF_GS_Compilation_ibkr_1h_{YYYYMMDD}.xlsx`
- **Worksheets:**
  1. **WF_Results:** All configurations with IS/WF1/WF2/Degradation metrics
  2. **WF_Filtered:** Configurations meeting degradation criteria
  3. **WF_Short:** Best configurations per strategy type
- **Columns:**
  - Metadata: #, Exchange, Symbol, Interval, Data Point, Model, Entry/Exit Model, Length, Entry, Exit
  - IS Metrics: IS Sharpe, IS MDD, IS Trade Count, IS Annual Return, IS Calmar Ratio
  - WF1 Metrics: WF1 Sharpe, WF1 MDD, WF1 Trade Count, WF1 Annual Return, WF1 Calmar Ratio
  - WF2 Metrics: WF2 Sharpe, WF2 MDD, WF2 Trade Count, WF2 Annual Return, WF2 Calmar Ratio
  - Degradation: WF1/WF2 Sharpe/MDD/Annual Return/Calmar Degrade
  - L Metrics: L Sharpe Degrade, L MDD Degrade, L Annual Return Degrade, L Calmar Degrade
- **Sorting:** By L Sharpe Degrade (descending = best maintained performance)

#### Configuration
- No additional parameters (processes all walk_forward_report.csv files)

#### Performance
- **Runtime:** ~12 seconds per symbol (fast aggregation)
- **Output Size:** ~94 MB (~12,000 configurations for 670+ symbols)

---

### Stage 4: WF Alpha Generation

**Script:** [generate_ibkr_gs_wf_alpha_results.py](generate_ibkr_gs_wf_alpha_results.py)

**Purpose:** Regenerate full backtests for validated strategies from WF_Short worksheet.

#### Input
- **Source 1:** `AQS_SFGridResults/WF_GS_Compilation_ibkr_1h_{YYYYMMDD}.xlsx` from Stage 3
- **Worksheet:** "WF_Short" (subset of best configurations)
- **Source 2:** Original OHLCV data from `GridSearch_Data/merged_ibkr_{symbol}_{interval}_*.csv`

#### Processing Logic
```python
For each configuration in WF_Short:
  1. Load configuration row (Exchange, Symbol, Interval, Data Point, Model, etc.)
  2. Load original OHLCV data using glob pattern
  3. Generate trading signals using util_AQS_parallel.generate_all_signals():
     - Apply technical indicators (model)
     - Generate entry/exit signals (strategy)
     - Calculate P&L timeseries
  4. Calculate cumulative metrics and Buy & Hold comparison
  5. Create outputs:
     a) backtest.csv: Full timeseries data
     b) metrics.csv: Summary statistics
     c) summary.csv: Original configuration row
     d) cumu_pnl_vs_bnh.png: Equity curve visualization
  6. Save to: WFAlphaResults/merged_{exchange}_{symbol}_{interval}_linear/
              feature/model/strategy/
```

#### Output Structure
```
WFAlphaResults/
└── merged_ibkr_{symbol}_{interval}_linear/
    └── {feature}/
        └── {model}/
            └── {strategy}/
                ├── backtest.csv           # Full timeseries backtest
                ├── metrics.csv            # Performance summary
                ├── summary.csv            # WF_Short configuration row
                └── cumu_pnl_vs_bnh.png   # Equity curve visualization
```

#### backtest.csv Structure
```csv
datetime,close,signal,pnl,cumulative_pnl,bnh,drawdown
2024-01-01 00:00:00,102.50,1,0.005,0.005,0.002,-0.000
2024-01-01 01:00:00,103.10,-1,-0.003,0.002,0.008,-0.006
...
```

**Columns:**
- `datetime`: Trading timestamp (ISO format, timezone-naive)
- `close`: Close price from original data
- `signal`: Trading signal (+1 = long, -1 = short, 0 = neutral)
- `pnl`: Period P&L (percentage return)
- `cumulative_pnl`: Running cumulative P&L
- `bnh`: Buy & Hold cumulative return
- `drawdown`: Drawdown from peak cumulative P&L

#### Configuration
```python
EXCHANGE = "ibkr"
INTERVAL = "1h"
DATA_DIR = "GridSearch_Data"
OUTPUT_DIR = "WFAlphaResults"
ANNUALIZATION_PERIOD = 252 * 8  # 2016 for 1h
OVERFIT_MIN_TRADES_PCT = 0.015  # 1.5% minimum trades threshold
```

#### Performance
- **Runtime:** ~675 seconds per symbol (longest stage)
- **Output:** Nested directory structure with 4 files per strategy

---

### Stage 5: WF Alpha Compilation

**Script:** [compile_ibkr_gs_wfalpha_results.py](compile_ibkr_gs_wfalpha_results.py)

**Purpose:** Compile WFAlpha results into reference file with three worksheets.

#### Input
- **Source:** `WFAlphaResults/merged_ibkr_{symbol}_{interval}_*` directories from Stage 4
- **Files:** `summary.csv` and `metrics.csv` from each strategy folder
- **Pattern:** Glob search for `merged_ibkr_{symbol}_{interval}_*`

#### Processing Logic
```python
For each symbol:
  1. Find all folders: WFAlphaResults/merged_{exchange}_{symbol}_{interval}_*
  2. Recursively search for summary.csv files
  3. For each summary.csv:
     - Extract configuration (feature, model, strategy) from path
     - Read summary.csv for WF metrics
     - Read metrics.csv for additional performance data
     - Extract Cumulative PnL, Buy & Hold, PnL Ratio
     - Build complete result row
  4. Create three worksheets:
     - WF_Alphas: Basic metrics (like Alpha_Compilation format)
     - WF_Extended: Full walk-forward breakdown (IS/WF1/WF2 separate)
     - WF_Short: Deduplicated best per strategy type
```

#### Output
- **File:** `WFAlphaResults/WFAlpha_Compilation_ibkr_1h_{YYYYMMDD}.xlsx`
- **Worksheets:**

1. **WF_Alphas** (Basic Metrics)
   - Columns: Exchange, Symbol, Interval, Data Point, Model, Entry/Exit Model, Length, Entry, Exit, Sharpe (IS), MDD (IS), Trade Count (IS), Annual Return (IS), Calmar Ratio (IS), Cumulative PnL, Buy & Hold, PnL Ratio

2. **WF_Extended** (Complete Walk-Forward Breakdown)
   - All IS/WF1/WF2 metrics separately
   - All degradation columns
   - Complete performance history

3. **WF_Short** (Best Configurations)
   - Deduplicated subset
   - Used by Stage 6 & 7
   - Filtering logic: Best configuration per strategy type based on L Sharpe Degrade

#### Configuration
```python
EXCHANGE = "ibkr"
INTERVAL = "1h"
BASE_DIR = "WFAlphaResults"
```

#### Performance
- **Runtime:** ~6 seconds per symbol (fast compilation)
- **Output Size:** Variable (depends on number of validated strategies)

---

### Stage 6: Combination Strategies (Portfolio Construction)

**Script:** [compile_ibkr_gs_combination_strategies.py](compile_ibkr_gs_combination_strategies.py)

**Purpose:** Portfolio construction with correlation optimization across multiple symbols.

#### Input
- **Source 1:** `WFAlphaResults/WFAlpha_Compilation_ibkr_1h_{YYYYMMDD}.xlsx` from Stage 5
- **Worksheet:** "WF_Short" (best configurations)
- **Source 2:** Backtest timeseries from `WFAlphaResults/merged_{exchange}_{symbol}_{interval}_*/feature/model/strategy/backtest.csv`
- **Source 3:** Close prices from `GridSearch_Data/merged_ibkr_{symbol}_{interval}_*.csv`

#### Processing Logic
```python
For each symbol in WF_Short:
  1. Load strategy configurations from WF_Short
  2. For each configuration:
     - Load backtest.csv (datetime, pnl, signal columns)
     - Extract strategy P&L timeseries
     - Construct strategy name: {symbol}_{feature}_{model}_{strategy}
  3. Merge all strategy P&Ls using INNER join on datetime
  4. Calculate correlation matrix between all strategy P&Ls
  5. For each correlation threshold (0.7, 0.6, 0.5):
     # Greedy correlation optimization
     a) Sort strategies by Sharpe Ratio (descending)
     b) Start with highest Sharpe strategy
     c) For each remaining strategy:
        - Calculate max correlation with already-selected strategies
        - If max_corr < threshold: add to portfolio
     d) Calculate portfolio metrics for selected strategies:
        - Equal-weighted portfolio P&L
        - Cumulative P&L
        - Sharpe Ratio
        - Max Drawdown
        - Annualized Return
        - Calmar Ratio
  6. Load Buy & Hold returns from close prices
  7. Generate output worksheets
```

#### Correlation Optimization Algorithm
```python
def greedy_correlation_optimization(corr_matrix, strategy_metrics, threshold):
    """
    Select strategies using greedy algorithm based on correlation threshold.

    Algorithm:
    1. Sort strategies by Sharpe Ratio (descending)
    2. Select highest Sharpe strategy first
    3. For each remaining strategy:
       - Calculate max correlation with already-selected strategies
       - If max_correlation < threshold: add to portfolio
    4. Return selected strategies and portfolio metrics
    """
    sorted_strategies = sorted(strategy_metrics, key=lambda x: x['Sharpe'], reverse=True)
    selected = [sorted_strategies[0]]

    for candidate in sorted_strategies[1:]:
        max_corr = max([abs(corr_matrix.loc[candidate, s]) for s in selected])
        if max_corr < threshold:
            selected.append(candidate)

    return selected
```

#### Output
- **File:** `WFAlphaResults/Combination_Strategy_Compilation_ibkr_1h_{YYYYMMDD}.xlsx`
- **Worksheets:**

1. **Corr Summary** (Optimization Results)
   - Columns: Symbol, Threshold, Total Strategies, # Strategies Selected, Portfolio Sharpe, Avg Correlation, Max Correlation, Min Correlation, Portfolio MDD, Portfolio Annual Return, Portfolio Calmar Ratio, Cumulative PnL, Buy & Hold, PnL Ratio, Strategy List
   - Shows results for all correlation thresholds (0.7, 0.6, 0.5)

2. **{Symbol} Portfolio** (Timeseries Data)
   - Columns: datetime, {strategy1}_pnl, {strategy1}_signal, {strategy2}_pnl, {strategy2}_signal, ..., portfolio_pnl, cumulative_pnl, bnh, drawdown
   - Full portfolio timeseries with all strategy P&Ls

3. **{Symbol} Corr** (Correlation Matrix)
   - N × N correlation matrix showing pairwise correlations
   - Summary statistics (avg, max, min correlation, strategy count)

#### Merge Method
- **Type:** INNER join (Stage 7 uses OUTER)
- **Reason:** Ensures all strategies have data at each timestamp
- **Impact:** May reduce datetime coverage if strategies have different start dates

#### Configuration
```python
EXCHANGE = "ibkr"
INTERVAL = "1h"
CORRELATION_THRESHOLDS = [0.7, 0.6, 0.5]
ANNUALIZATION_PERIOD = 252 * 8  # 2016 for 1h
DATA_DIR = "GridSearch_Data"
OUTPUT_DIR = "WFAlphaResults"
```

#### Performance
- **Runtime:** ~182 seconds per symbol
- **Output Size:** ~653 MB (depends on number of strategies and timeseries length)

---

### Stage 7: Final Compilation (OUTER Merge)

**Script:** [generate_ibkr_gs_final_compilation.py](generate_ibkr_gs_final_compilation.py)

**Purpose:** Generate final portfolios using OUTER merge to preserve maximum historical data.

#### Input
- **Source 1:** `WFAlphaResults/Combination_Strategy_Compilation_ibkr_1h_{YYYYMMDD}.xlsx` from Stage 6
- **Worksheet:** "Corr Summary" (extracts 0.5 threshold strategies only)
- **Source 2:** Backtest timeseries from `WFAlphaResults/merged_{exchange}_{symbol}_{interval}_*/feature/model/strategy/backtest.csv`
- **Source 3:** Close prices from `GridSearch_Data/merged_ibkr_{symbol}_{interval}_*.csv`

#### Processing Logic
```python
For each symbol:
  1. Load Corr Summary (filter for Threshold = 0.5)
  2. Extract strategy list for this symbol
  3. For each strategy in list:
     - Map strategy name to backtest.csv path using WF_Short metadata
     - Load backtest.csv
     - Extract datetime and P&L columns
  4. Merge all strategy backtests using OUTER join:
     # Key difference from Stage 6
     - Preserves ALL datetime rows from ALL strategies
     - Fills NaN with 0 for missing strategy data (neutral position)
     - Result: Maximum historical coverage
  5. Calculate equal-weighted portfolio P&L:
     portfolio_pnl = mean(strategy1_pnl, strategy2_pnl, ..., strategyN_pnl)
     cumulative_pnl = cumsum(portfolio_pnl)
  6. Load Buy & Hold returns from close prices
  7. Calculate portfolio metrics:
     - Sharpe Ratio (annualized)
     - Max Drawdown
     - Annualized Return
     - Calmar Ratio
     - Cumulative PnL vs Buy & Hold comparison
  8. Generate output worksheets
```

#### Key Difference from Stage 6
| Aspect | Stage 6 | Stage 7 |
|--------|---------|---------|
| Merge Type | INNER join | OUTER join |
| Missing Data | Excluded | Filled with 0 |
| Datetime Coverage | Intersection of all strategies | Union of all strategies |
| Data Preservation | Minimum | Maximum |
| Use Case | Initial analysis | Final production portfolio |

#### Output
- **File:** `WFAlphaResults/Final_Compilation_ibkr_1h_{YYYYMMDD}.xlsx`
- **Worksheets:**

1. **Corr Summary** (0.5 Threshold Only)
   - Same format as Stage 6, but only includes 0.5 threshold results

2. **{Symbol} Portfolio** (Final Timeseries)
   - Columns: datetime, {strategy1}_pnl, {strategy2}_pnl, ..., portfolio_pnl, cumulative_pnl, bnh, drawdown
   - Maximum historical coverage (OUTER merge)

3. **{Symbol} Corr** (Correlation Matrix)
   - N × N correlation matrix for final selected strategies

#### Configuration
```python
EXCHANGE = "ibkr"
INTERVAL = "1h"
CORRELATION_THRESHOLD = 0.5  # Fixed to 0.5
ANNUALIZATION_PERIOD = 252 * 8  # 2016 for 1h
BASE_DIR = "WFAlphaResults"
```

#### Performance
- **Runtime:** ~15 seconds per symbol (fast merge)
- **Output Size:** Variable (depends on final portfolio composition)

---

### Stage Summary Table

| Stage | Script | Input | Output | Runtime | Key Action |
|-------|--------|-------|--------|---------|-----------|
| 1 | compile_ibkr_gs_alphas.py | IS_OOS_comparison.csv files | Alpha_GS_Compilation Excel | 100s | Extract best configs |
| 2 | validate_ibkr_gs_walk_forward.py | Excel + OHLCV CSV | walk_forward_report CSVs | 290s | Walk-forward validation |
| 3 | compile_ibkr_gs_walk_forward_results.py | walk_forward_report CSVs | WF_GS_Compilation Excel | 12s | Calculate degradation |
| 4 | generate_ibkr_gs_wf_alpha_results.py | Excel + OHLCV CSV | Nested backtest files | 675s | Regenerate full backtests |
| 5 | compile_ibkr_gs_wfalpha_results.py | Nested backtest files | WFAlpha_Compilation Excel | 6s | Compile into 3 worksheets |
| 6 | compile_ibkr_gs_combination_strategies.py | WFAlpha Excel + backtests | Combination Excel | 182s | Portfolio + correlation |
| 7 | generate_ibkr_gs_final_compilation.py | Combination Excel | Final_Compilation Excel | 15s | OUTER merge final portfolio |

**Total Runtime:** ~1,280 seconds (21 minutes) per symbol, 1 interval

---

## 4. FunnelTracker Integration

### 4.1 Overview

**File:** [funnel_tracker.py](funnel_tracker.py)

The FunnelTracker module provides comprehensive metrics collection across all 7 pipeline stages, enabling performance monitoring, error tracking, and quality analysis.

### 4.2 FunnelTracker Class

```python
class FunnelTracker:
    """Track funnel metrics across pipeline stages"""

    def __init__(self, config: Dict[str, Any]):
        """Initialize with pipeline configuration"""
        self.metrics = {
            'stages': [],
            'symbols': {},
            'overall': {
                'start_time': datetime.now().isoformat(),
                'end_time': None,
                'total_runtime_seconds': None
            }
        }
```

### 4.3 Metrics Collected

#### Overall Pipeline Metrics
- `start_time`: Pipeline execution start timestamp
- `end_time`: Pipeline execution end timestamp
- `total_runtime_seconds`: Total pipeline runtime

#### Per-Stage Metrics
- `stage_num`: Stage number (1-7)
- `stage_name`: Human-readable stage name
- `script`: Script filename
- `start_time`: Stage start timestamp
- `end_time`: Stage end timestamp
- `runtime_seconds`: Stage execution duration
- `input_volume`: Number of input configurations
- `output_volume`: Number of output configurations
- `pass_rate`: Output volume / Input volume
- `input_data_rows`: Total input data rows (for data processing stages)
- `output_data_rows`: Total output data rows
- `errors`: List of error dictionaries (symbol, error message, timestamp)
- `symbol_metrics`: Dictionary of per-symbol custom metrics

#### Per-Symbol Metrics
- `runtime_seconds`: Symbol-specific execution time
- Custom metrics recorded by stage scripts (e.g., strategies_processed, files_found)

### 4.4 Core Methods

```python
# Stage tracking
funnel_tracker.start_stage(stage_num, stage_name, script_name)
funnel_tracker.end_stage(stage_num)

# Volume tracking
funnel_tracker.set_stage_volume(stage_num, input_vol, output_vol)
funnel_tracker.set_stage_data_rows(stage_num, input_rows, output_rows)

# Per-symbol tracking
funnel_tracker.record_symbol_metric(stage_num, symbol, metric_name, value)
funnel_tracker.record_error(stage_num, symbol, error_msg)

# Finalization
funnel_tracker.finalize()
funnel_tracker.generate_report(output_path)

# Checkpointing
funnel_tracker.save_checkpoint(checkpoint_path)
funnel_tracker.load_checkpoint(checkpoint_path)
```

### 4.5 Report Generation

**Output File:** `pipeline_runs/run_{timestamp}/funnel_report.xlsx`

**Worksheets:**

#### 1. Summary
Pipeline-level overview:
- **Pipeline Execution**
  - Start Time
  - End Time
  - Total Runtime (hours)
- **Configuration Volume Funnel**
  - Total Input Volume
  - Total Output Volume
  - Overall Reduction (%)
- **Data Row Funnel**
  - Total Input Data Rows
  - Total Output Data Rows
- **Execution Summary**
  - Stages Completed
  - Total Errors
  - Symbols Processed

#### 2. Alpha_Summary
Excel output tracking across pipeline stages:
- **Stage**: Stage name (e.g., "1. Alpha Compilation", "6. Combination Strategies")
- **Excel Report**: Excel filename generated by the stage
- **Worksheet**: Specific worksheet within the Excel file
- **MBT**: Count of strategies/configurations for MBT symbol
- **MET**: Count of strategies/configurations for MET symbol
- **Total**: Sum of MBT + MET counts
- **Remarks**: Additional notes (e.g., "Based on 0.7 Threshold")

**Coverage:** Tracks Excel outputs from Stages 1, 3, 4, 6, and 7.

**Special Parsing Logic:**
- **Standard Worksheets** (Stages 1, 3, 4): Counts rows where `Symbol` column equals 'MBT' or 'MET'
- **Corr Summary Worksheets** (Stages 6, 7): Extracts specific column values filtered by threshold:
  - **Stage 6**: Filters `Threshold == 0.7`, extracts `# Strategies Selected` column
  - **Stage 7**: Filters `Threshold == 0.5`, extracts `Total Strategies` column

This worksheet provides a comprehensive view of how many strategies/configurations survive at each stage, making it easy to visualize the funnel progression.

#### 3. Symbol_Breakdown
Per-symbol status and metrics:
- Symbol
- Status (Success/Failed/Skipped)
- Error Count
- Notes

#### 4. Stage_Details
Detailed stage-by-stage metrics:
- Stage (number and name)
- Script
- Input Volume
- Output Volume
- Pass Rate (%)
- Input Data Rows
- Output Data Rows
- Runtime (minutes)
- Error Count
- Start Time
- End Time

#### 5. Quality_Metrics
Quality analysis (placeholder for future implementation):
- Sharpe Ratio distributions
- Degradation statistics
- Strategy quality trends

#### 6. Errors
Complete error log:
- Stage (number and name)
- Symbol
- Error Message
- Timestamp

### 4.6 Alpha Summary Feature

The Alpha Summary worksheet provides automatic tracking of Excel outputs across all pipeline stages, enabling quick visualization of the strategy funnel.

#### How It Works

**1. Excel Output Registration:**

During pipeline execution, each stage that produces Excel reports calls `record_excel_output()` to register its files:

```python
# In run_ibkr_pipeline.py - register_excel_outputs() function
def register_excel_outputs(stage_num, interval, funnel_tracker, config, logger):
    """Register Excel outputs for a completed stage with FunnelTracker"""

    # Stage 1: Alpha Compilation
    if stage_num == 1:
        excel_file = f"BruteForceResults/Alpha_Compilation_{EXCHANGE}_{interval}_{today}.xlsx"
        funnel_tracker.record_excel_output(
            stage_num=1,
            excel_file=excel_file,
            worksheet="Alpha Compilation",
            remarks=""
        )

    # Stage 3: WF Results Compilation (multiple worksheets)
    elif stage_num == 3:
        excel_file = f"BruteForceResults/WF_Compilation_{EXCHANGE}_{interval}_{today}.xlsx"
        funnel_tracker.record_excel_output(3, excel_file, "WF_Results", "")
        funnel_tracker.record_excel_output(3, excel_file, "WF_Filtered", "")
        funnel_tracker.record_excel_output(3, excel_file, "WF_Short", "")

    # Stage 4: WF Alpha Generation
    elif stage_num == 4:
        excel_file = f"WFAlphaResults/WFAlpha_Compilation_{EXCHANGE}_{interval}_{today}.xlsx"
        funnel_tracker.record_excel_output(4, excel_file, "WF_Short", "")

    # Stage 6: Combination Strategies
    elif stage_num == 6:
        excel_file = f"WFAlphaResults/Combination_Strategy_Compilation_{EXCHANGE}_{interval}_{today}.xlsx"
        funnel_tracker.record_excel_output(6, excel_file, "Corr Summary", "Based on 0.7 Threshold")

    # Stage 7: Final Compilation
    elif stage_num == 7:
        excel_file = f"WFAlphaResults/Final_Compilation_{EXCHANGE}_{interval}_{today}.xlsx"
        funnel_tracker.record_excel_output(7, excel_file, "Corr Summary", "Based on 0.5 Threshold")
```

**2. Automatic Parsing:**

The `record_excel_output()` method automatically:
- Opens the Excel file
- Reads the specified worksheet
- Counts strategies by symbol (MBT, MET)
- Stores results for later report generation

**3. Parsing Logic:**

There are two parsing strategies based on worksheet type:

**Row Counting (Standard Worksheets):**
```python
# For most worksheets (Stages 1, 3, 4)
if 'Symbol' in df.columns:
    mbt_count = len(df[df['Symbol'] == 'MBT'])
    met_count = len(df[df['Symbol'] == 'MET'])
```

**Column Extraction (Corr Summary Worksheets):**
```python
# For Corr Summary worksheets (Stages 6, 7)
if worksheet == "Corr Summary" and 'Symbol' in df.columns and 'Threshold' in df.columns:

    # Stage 6: Extract "# Strategies Selected" for threshold 0.7
    if stage_num == 6:
        df_filtered = df[df['Threshold'] == 0.7]
        mbt_row = df_filtered[df_filtered['Symbol'] == 'MBT']
        met_row = df_filtered[df_filtered['Symbol'] == 'MET']
        mbt_count = int(mbt_row['# Strategies Selected'].iloc[0])
        met_count = int(met_row['# Strategies Selected'].iloc[0])

    # Stage 7: Extract "Total Strategies" for threshold 0.5
    elif stage_num == 7:
        df_filtered = df[df['Threshold'] == 0.5]
        mbt_row = df_filtered[df_filtered['Symbol'] == 'MBT']
        met_row = df_filtered[df_filtered['Symbol'] == 'MET']
        mbt_count = int(mbt_row['Total Strategies'].iloc[0])
        met_count = int(met_row['Total Strategies'].iloc[0])
```

#### Why Different Parsing Strategies?

**Stages 1, 3, 4** produce worksheets where each row represents one strategy/configuration. The Symbol column identifies which symbol the row belongs to, so row counting is appropriate.

**Stages 6, 7** produce Corr Summary worksheets with:
- Multiple threshold rows per symbol (0.7, 0.6, 0.5)
- Column values that represent strategy counts at each threshold
- Need to extract specific column values, not count rows

Example Corr Summary structure (Stage 6):
```
Symbol  Threshold  Total Strategies  # Strategies Selected  Portfolio Sharpe  ...
MBT     0.7        100              46                      2.45
MBT     0.6        100              62                      2.38
MBT     0.5        100              78                      2.31
MET     0.7        150              73                      2.52
MET     0.6        150              95                      2.44
MET     0.5        150              118                     2.37
```

For Stage 6, we want the **# Strategies Selected** value for threshold 0.7 (46 for MBT, 73 for MET), not the row count (which would be 3 for each symbol).

#### Expected Output Example

Alpha_Summary worksheet:
```
Stage                        Excel Report                                    Worksheet         MBT   MET   Total  Remarks
1. Alpha Compilation         Alpha_Compilation_ibkr_1h_20251121.xlsx        Alpha Compilation 512   475   987
3. WF Results Compilation    WF_Compilation_ibkr_1h_20251121.xlsx           WF_Results        512   475   987
3. WF Results Compilation    WF_Compilation_ibkr_1h_20251121.xlsx           WF_Filtered       210   198   408
3. WF Results Compilation    WF_Compilation_ibkr_1h_20251121.xlsx           WF_Short          105   99    204
4. WF Alpha Generation       WFAlpha_Compilation_ibkr_1h_20251121.xlsx      WF_Short          105   99    204
6. Combination Strategies    Combination_Strategy_Compilation_ibkr_1h_*.xlsx Corr Summary      46    73    119    Based on 0.7 Threshold
7. Final Compilation         Final_Compilation_ibkr_1h_*.xlsx               Corr Summary      21    29    50     Based on 0.5 Threshold
```

This provides a clear visualization of the strategy funnel: starting with 987 total configurations, progressively filtering through walk-forward validation, and ultimately selecting 50 final strategies for production deployment.

### 4.7 Integration Example

```python
# In run_ibkr_pipeline.py
from funnel_tracker import FunnelTracker

# Initialize
funnel_tracker = FunnelTracker(config.to_dict())

# Stage execution
funnel_tracker.start_stage(1, "Alpha Compilation", "compile_ibkr_alphas.py")

# Run stage for each symbol in parallel
for symbol in symbols:
    result = run_stage_for_symbol(stage_num=1, symbol=symbol, ...)

    # Record metrics
    funnel_tracker.record_symbol_metric(1, symbol, 'runtime_seconds', result['elapsed_time'])

    if result['status'] == 'failed':
        funnel_tracker.record_error(1, symbol, result['error'])

# End stage
funnel_tracker.set_stage_volume(1, input_volume=1000, output_volume=850)
funnel_tracker.end_stage(1)

# Register Excel outputs for Alpha Summary tracking
register_excel_outputs(stage_num=1, interval=interval, funnel_tracker=funnel_tracker,
                      config=config, logger=logger)

# After all stages complete
funnel_tracker.finalize()
funnel_tracker.generate_report(f"{config.run_dir}/funnel_report.xlsx")
```

### 4.8 Console Status

Real-time status display during execution:

```python
status = funnel_tracker.get_console_status()
# Returns: "Stage 4: WF Alpha Generation (Running 345.2 min)"
```

---

## 5. Execution Model

### 5.1 Sequential Interval, Parallel Symbol Architecture

The pipeline uses a hierarchical execution model:

```python
for interval in config.intervals:                    # Level 1: Sequential
    print(f"Processing interval: {interval}")

    for stage_num in range(1, 8):                    # Level 2: Sequential
        print(f"Executing Stage {stage_num}")

        # Level 3: Parallel (across symbols)
        results = run_stage_parallel(
            stage_num,
            interval,
            config.symbols,  # ["MBT", "MET"]
            funnel_tracker,
            logger
        )

        # Wait for all symbols to complete before next stage
        save_checkpoint(config, interval, stage_num, results)
```

**Execution Flow:**
```
Interval: 1h
│
├─ Stage 1: Alpha Compilation
│  ├─ MBT (parallel) ──┐
│  └─ MET (parallel) ──┴─ Wait for both → Checkpoint
│
├─ Stage 2: Walk-Forward Validation
│  ├─ MBT (parallel) ──┐
│  └─ MET (parallel) ──┴─ Wait for both → Checkpoint
│
├─ Stages 3-7 (same pattern)
│
Interval: 4h (repeat pattern)
Interval: 1d (repeat pattern)
```

### 5.2 Parallelization Details

#### ThreadPoolExecutor Implementation

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def run_stage_parallel(stage_num, interval, symbols, config, funnel_tracker, logger):
    """Run stage for all symbols in parallel"""

    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        # Submit all symbol tasks
        futures = {
            executor.submit(
                run_stage_for_symbol,
                stage_num,
                symbol,
                interval,
                config,
                logger
            ): symbol
            for symbol in symbols
        }

        # Collect results as they complete
        results = []
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
                results.append(result)

                # Record metrics
                funnel_tracker.record_symbol_metric(
                    stage_num,
                    symbol,
                    'runtime_seconds',
                    result['elapsed_time']
                )
            except Exception as e:
                funnel_tracker.record_error(stage_num, symbol, str(e))

        return results
```

#### Max Workers Configuration
- **Default:** `len(symbols)` (2 for MBT, MET)
- **Configurable:** `--max-workers N` CLI argument
- **Recommendation:** Set to number of CPU cores for optimal performance

### 5.3 Checkpoint and Resume

#### Checkpoint Format

**File:** `pipeline_runs/run_{timestamp}/checkpoint.json`

```json
{
    "timestamp": "2025-11-20T16:19:24.123456",
    "config": {
        "intervals": ["1h"],
        "symbols": ["MBT", "MET"],
        "variants": null,
        "start_stage": 1,
        "end_stage": 7,
        "max_workers": 2,
        "run_dir": "pipeline_runs/run_20251120_161924",
        "execution_date": "20251120"
    },
    "current_interval": "1h",
    "current_stage": 3,
    "completed_stages": [1, 2],
    "results": [
        {"stage": 1, "symbol": "MBT", "status": "success", "elapsed_time": 98.5},
        {"stage": 1, "symbol": "MET", "status": "success", "elapsed_time": 102.3},
        {"stage": 2, "symbol": "MBT", "status": "success", "elapsed_time": 285.7},
        {"stage": 2, "symbol": "MET", "status": "success", "elapsed_time": 291.2}
    ]
}
```

#### Resume Logic

```python
def resume_from_checkpoint(checkpoint_path):
    """Resume pipeline from checkpoint"""

    # Load checkpoint
    with open(checkpoint_path, 'r') as f:
        checkpoint = json.load(f)

    # Determine resume point
    if checkpoint['current_stage'] < 7:
        # Resume from next stage in same interval
        resume_stage = checkpoint['current_stage'] + 1
        resume_interval = checkpoint['current_interval']
    else:
        # Move to next interval, reset to stage 1
        intervals = checkpoint['config']['intervals']
        current_idx = intervals.index(checkpoint['current_interval'])

        if current_idx + 1 < len(intervals):
            resume_stage = 1
            resume_interval = intervals[current_idx + 1]
        else:
            print("Pipeline already completed!")
            return

    # Update config and restart
    config = PipelineConfig.from_dict(checkpoint['config'])
    config.start_stage = resume_stage
    config.current_interval = resume_interval

    run_pipeline(config)
```

#### Usage

```bash
# Resume from checkpoint
python run_ibkr_gs_pipeline.py --resume pipeline_runs/run_20251120_161924/checkpoint.json
```

### 5.4 Error Isolation

**Key Principle:** Symbol failures don't block other symbols or subsequent stages.

```python
def run_stage_for_symbol(stage_num, symbol, interval, config, logger):
    """Run stage for single symbol with error isolation"""

    try:
        # Execute stage script
        result = execute_stage_script(...)

        return {
            'status': 'success',
            'symbol': symbol,
            'stage_num': stage_num,
            'elapsed_time': elapsed_time,
            'error': None
        }

    except Exception as e:
        logger.error(f"[Stage {stage_num}] [{symbol}] Failed: {str(e)}")

        return {
            'status': 'failed',
            'symbol': symbol,
            'stage_num': stage_num,
            'elapsed_time': elapsed_time,
            'error': str(e)
        }

# In parallel execution
results = run_stage_parallel(stage_num, interval, symbols, ...)

# Continue even if some symbols failed
successful_symbols = [r['symbol'] for r in results if r['status'] == 'success']
failed_symbols = [r['symbol'] for r in results if r['status'] == 'failed']

print(f"Stage {stage_num} completed: {len(successful_symbols)} succeeded, {len(failed_symbols)} failed")
# Pipeline continues to next stage
```

---

## 6. Data Flow

### 6.1 Input Data Structure

**Location:** `GridSearch_Data/`

**Files:**
- 670+ OHLCV data files for US equities
- Pattern: `merged_{exchange}_{symbol}_{interval}_{variant}.csv`
- Example: `merged_ibkr_NVDA_1h_wiho_15Dec2025.csv`

**CSV Structure:**
```csv
datetime,open,high,low,close,volume,...
2024-01-01 00:00:00,102.50,103.20,102.30,103.10,1000000
2024-01-01 01:00:00,103.10,103.80,103.00,103.70,950000
2024-01-01 02:00:00,103.70,104.10,103.50,103.90,890000
...
```

**Columns Used:**
- `datetime`: Timestamp (ISO 8601 format, timezone-naive for Excel compatibility)
- `open`, `high`, `low`, `close`: OHLC prices
- `volume`: Trading volume
- Additional columns: Technical indicators, returns, volatility, spreads (added by data_generator.py)

### 6.2 Intermediate Outputs

#### Stage 1-3: AQS_SFGridResults/

```
AQS_SFGridResults/
├── merged_ibkr_NVDA_1h_wiho_15Dec2025/     # Symbol-specific folder (670+ folders)
│   ├── feature1/                            # Feature dimension (e.g., "high", "close")
│   │   ├── model1/                          # Model dimension (e.g., "sma_diff", "zscore")
│   │   │   ├── strategy1/                   # Strategy dimension (e.g., "trend_long", "mr")
│   │   │   │   ├── IS_OOS_comparison.csv    # Grid search IS/OOS results
│   │   │   │   ├── backtest.csv             # Full timeseries
│   │   │   │   └── walk_forward_report.csv  # WF validation results (Stage 2)
│   │   │   ├── strategy2/
│   │   │   └── strategyN/
│   │   ├── model2/
│   │   └── modelN/
│   ├── feature2/
│   └── featureN/
├── merged_ibkr_AAPL_1h_wiho_15Dec2025/     # Additional symbol folders
│   └── [similar structure]
├── Alpha_GS_Compilation_ibkr_1h_20260101.xlsx # Stage 1 output (263 MB)
└── WF_GS_Compilation_ibkr_1h_20260101.xlsx    # Stage 3 output (94 MB)
```

#### Stage 4-7: WFAlphaResults/

```
WFAlphaResults/
├── merged_ibkr_NVDA_1h_linear/
│   ├── feature1/
│   │   ├── model1/
│   │   │   ├── strategy1/
│   │   │   │   ├── backtest.csv             # Full timeseries backtest
│   │   │   │   ├── metrics.csv              # Summary metrics
│   │   │   │   ├── summary.csv              # Configuration row
│   │   │   │   └── cumu_pnl_vs_bnh.png     # Equity curve chart
│   │   │   ├── strategy2/
│   │   │   └── strategyN/
│   │   ├── model2/
│   │   └── modelN/
│   ├── feature2/
│   └── featureN/
├── merged_ibkr_AAPL_1h_linear/
│   └── [similar structure]
├── WFAlpha_Compilation_ibkr_1h_20260103.xlsx          # Stage 5 output
├── Combination_Strategy_Compilation_ibkr_1h_20260103.xlsx  # Stage 6 output (653 MB)
└── Final_Compilation_ibkr_1h_20260103.xlsx            # Stage 7 output (280 MB)
```

### 6.3 Final Output Files

| File | Stage | Size | Typical Rows | Worksheets |
|------|-------|------|--------------|------------|
| Alpha_GS_Compilation_ibkr_1h_{DATE}.xlsx | 1 | 263 MB | ~100,000+ | 2 |
| WF_GS_Compilation_ibkr_1h_{DATE}.xlsx | 3 | 94 MB | ~12,000+ | 3 |
| WFAlpha_Compilation_ibkr_1h_{DATE}.xlsx | 5 | Variable | Variable | 3 |
| Combination_Strategy_Compilation_ibkr_1h_{DATE}.xlsx | 6 | 653 MB | Variable | 3+ |
| Final_Compilation_ibkr_1h_{DATE}.xlsx | 7 | 280 MB | Variable | 2+ |

### 6.4 Naming Conventions

**Pattern:** `{OutputType}_GS_{Exchange}_{Interval}_{YYYYMMDD}.xlsx`

**Examples:**
- `Alpha_GS_Compilation_ibkr_1h_20260101.xlsx`
- `WF_GS_Compilation_ibkr_1h_20260101.xlsx`
- `WFAlpha_Compilation_ibkr_1h_20260103.xlsx`
- `Combination_Strategy_Compilation_ibkr_1h_20260103.xlsx`
- `Final_Compilation_ibkr_1h_20260103.xlsx`

**Date Synchronization:**
- All outputs from same pipeline run use same date suffix
- Date set via environment variable: `PIPELINE_EXECUTION_DATE`
- Format: YYYYMMDD (e.g., "20260101")

### 6.5 Glob Patterns Used

Throughout the pipeline, glob patterns enable flexible file discovery:

```python
# Directory pattern
pattern = f"merged_{exchange}_{symbol}_{interval}_*"
# Matches: merged_ibkr_NVDA_1h_wiho_15Dec2025, merged_ibkr_AAPL_1h_linear, etc.

# File pattern
pattern = f"Alpha_GS_Compilation_{exchange}_{interval}_*.xlsx"
# Matches: Alpha_GS_Compilation_ibkr_1h_20260101.xlsx

# Recursive search for IS/OOS results
pattern = os.path.join(base_dir, "**", "IS_OOS_comparison.csv")
matches = glob.glob(pattern, recursive=True)
# Finds all IS_OOS_comparison.csv files in subdirectories

# Recursive search for WF reports
pattern = os.path.join(base_dir, "**", "walk_forward_report.csv")
matches = glob.glob(pattern, recursive=True)
# Finds all walk_forward_report.csv files in subdirectories
```

---

## 7. Command-Line Interface

### 7.1 Basic Usage

```bash
# Run full pipeline with defaults
python run_ibkr_gs_pipeline.py

# Run specific intervals
python run_ibkr_gs_pipeline.py --intervals 1h 4h

# Run specific symbols (example with US equities)
python run_ibkr_gs_pipeline.py --symbols NVDA AAPL MSFT

# Run partial pipeline (stages 3-7)
python run_ibkr_gs_pipeline.py --start-stage 3 --end-stage 7

# Run single stage
python run_ibkr_gs_pipeline.py --start-stage 5 --end-stage 5

# Resume from checkpoint
python run_ibkr_gs_pipeline.py --resume pipeline_runs/run_20251120_161924/checkpoint.json

# Custom output directory
python run_ibkr_gs_pipeline.py --output-dir my_custom_run

# Enable verbose logging
python run_ibkr_gs_pipeline.py --verbose

# Combine multiple options
python run_ibkr_gs_pipeline.py --intervals 1h --symbols MBT MET --start-stage 1 --end-stage 7 --max-workers 2 --verbose
```

### 7.2 CLI Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--intervals` | List[str] | `["1h"]` | Time intervals to process (1h, 4h, 1d) |
| `--symbols` | List[str] | `["MBT", "MET"]` | Symbols to process |
| `--variants` | List[str] | `None` | Data variants (auto-discover if None) |
| `--start-stage` | int | `1` | Starting stage number (1-7) |
| `--end-stage` | int | `7` | Ending stage number (1-7) |
| `--max-workers` | int | `len(symbols)` | Maximum parallel workers |
| `--output-dir` | str | `None` | Custom output directory (default: auto-generated) |
| `--resume` | str | `None` | Path to checkpoint file for resuming |
| `--verbose` | bool | `False` | Enable DEBUG-level logging |

### 7.3 Output Structure

**Run Directory:** `pipeline_runs/run_{YYYYMMDD}_{HHMMSS}/`

```
pipeline_runs/run_20251120_161924/
├── pipeline.log                             # Execution log
├── checkpoint.json                          # Resume data
├── funnel_report.xlsx                       # Metrics report
├── config.json                              # Pipeline configuration
├── stage1_1h/                               # Stage outputs (optional)
│   └── Alpha_Compilation_ibkr_1h_20251120.xlsx
├── stage2_1h/
├── stage3_1h/
│   └── WF_Compilation_ibkr_1h_20251120.xlsx
└── ...
```

### 7.4 Environment Variables

The pipeline sets environment variables for date synchronization:

```python
# Set in run_ibkr_pipeline.py
env = os.environ.copy()
env['PIPELINE_EXECUTION_DATE'] = config.execution_date  # "20251120"

# Used in stage scripts to ensure consistent output naming
today = os.environ.get('PIPELINE_EXECUTION_DATE', datetime.now().strftime('%Y%m%d'))
output_file = f"Alpha_Compilation_ibkr_1h_{today}.xlsx"
```

### 7.5 Logging Levels

```python
# Default (INFO)
python run_ibkr_gs_pipeline.py
# Output: Stage starts/ends, progress updates

# Verbose (DEBUG)
python run_ibkr_gs_pipeline.py --verbose
# Output: Detailed execution info, file paths, data shapes, etc.
```

**Log File Contents:**
```
2025-11-20 16:19:24 - INFO - [Pipeline] Starting IBKR pipeline
2025-11-20 16:19:24 - INFO - [Config] Intervals: ['1h'], Symbols: ['MBT', 'MET'], Stages: 1-7
2025-11-20 16:19:24 - INFO - [Stage 1] [1h] Starting: Alpha Compilation
2025-11-20 16:19:24 - INFO - [Stage 1] [1h] [MBT] Starting: Alpha Compilation
2025-11-20 16:21:03 - INFO - [Stage 1] [1h] [MBT] Completed in 98.5s
2025-11-20 16:21:06 - INFO - [Stage 1] [1h] [MET] Completed in 102.3s
2025-11-20 16:21:06 - INFO - [Stage 1] [1h] Summary: 2 succeeded, 0 failed
...
```

---

## 8. Performance Characteristics

### 8.1 Runtime Analysis

**Per-Stage Timing (2 symbols, 1 interval):**

| Stage | Avg Runtime | Bottleneck | Parallelizable | Scalability |
|-------|-------------|-----------|----------------|-------------|
| 1 | 100s | File I/O | ✓ (symbols) | Linear |
| 2 | 290s | Signal generation | ✓ (symbols) | Linear |
| 3 | 12s | Aggregation | ✓ (symbols) | Linear |
| 4 | 675s | Backtesting | ✓ (symbols) | Linear |
| 5 | 6s | Compilation | ✓ (symbols) | Linear |
| 6 | 182s | Correlation calc | ✓ (symbols) | Linear |
| 7 | 15s | Final merge | ✓ (symbols) | Linear |
| **Total** | **1,280s** | **~21 min** | **✓** | **Linear** |

### 8.2 Scaling Behavior

**Symbols:**
```
2 symbols (MBT, MET):      ~21 minutes
4 symbols:                 ~42 minutes (linear)
8 symbols:                 ~84 minutes (linear)
```

**Intervals:**
```
1 interval (1h):           ~21 minutes
2 intervals (1h, 4h):      ~42 minutes
3 intervals (1h, 4h, 1d):  ~63 minutes
```

**Total Time Formula:**
```
total_time ≈ num_intervals × num_symbols × 640 seconds
           = num_intervals × num_symbols × 10.7 minutes
```

### 8.3 Resource Requirements

#### Memory
- **Stage 1-3:** ~100 MB per symbol (CSV data loading)
- **Stage 4:** ~200 MB per symbol (backtest generation, peak usage)
- **Stage 5-7:** ~100 MB per symbol (portfolio construction)
- **Peak:** ~500 MB total for 2 symbols

#### Disk Space
- **Input Data:** ~10 MB per symbol per interval
- **BruteForceResults:** ~50 MB per symbol per interval
- **WFAlphaResults:** ~100 MB per symbol per interval
- **Final Outputs:** ~5 MB per interval
- **Total (2 symbols, 1 interval):** ~350 MB

#### CPU
- **Parallel Execution:** 2 workers (one per symbol)
- **CPU Utilization:** ~150% average (stages use CPU + I/O)
- **Recommended:** 4+ cores for optimal performance

### 8.4 Performance Optimization Tips

1. **Increase Max Workers:**
   ```bash
   python run_ibkr_gs_pipeline.py --max-workers 4
   ```
   - Benefit: Faster symbol processing if CPU allows
   - Trade-off: Higher memory usage

2. **Run Partial Pipelines:**
   ```bash
   # Run only fast stages (1, 3, 5, 7)
   python run_ibkr_gs_pipeline.py --start-stage 5 --end-stage 7
   ```

3. **Process Intervals Separately:**
   ```bash
   # Run intervals in parallel terminals
   python run_ibkr_gs_pipeline.py --intervals 1h &
   python run_ibkr_gs_pipeline.py --intervals 4h &
   python run_ibkr_gs_pipeline.py --intervals 1d &
   ```

4. **Use SSD Storage:**
   - Stages 1, 2, 4 are I/O-bound
   - SSD can reduce runtime by 20-30%

---

## 9. Error Handling and Validation

### 9.1 Environment Validation

**Checks performed at pipeline start:**

```python
def validate_environment(config):
    """Validate environment before pipeline execution"""

    # 1. Python version check
    if sys.version_info < (3, 7):
        raise EnvironmentError("Python 3.7+ required")

    # 2. Required modules
    required = ['pandas', 'numpy', 'openpyxl', 'matplotlib']
    for module in required:
        try:
            __import__(module)
        except ImportError:
            raise EnvironmentError(f"Missing required module: {module}")

    # 3. Stage scripts existence
    for stage_num, stage_info in STAGES.items():
        if not os.path.exists(stage_info['script']):
            raise FileNotFoundError(f"Stage {stage_num} script not found: {stage_info['script']}")

    # 4. Disk space check
    free_space_gb = shutil.disk_usage('.').free / (1024**3)
    if free_space_gb < 1:
        logging.warning(f"Low disk space: {free_space_gb:.2f} GB free")

    # 5. BruteForceResults directory
    if not os.path.exists('BruteForceResults'):
        raise FileNotFoundError("BruteForceResults directory not found")

    # 6. Input data check
    for symbol in config.symbols:
        for interval in config.intervals:
            pattern = f"Bruteforce_data/merged_{config.exchange}_{symbol}_{interval}_*.csv"
            if not glob.glob(pattern):
                raise FileNotFoundError(f"No input data found: {pattern}")
```

### 9.2 Error Isolation

**Symbol-Level Isolation:**

```python
# In run_stage_parallel()
for future in as_completed(futures):
    symbol = futures[future]
    try:
        result = future.result()
        results.append(result)
    except Exception as e:
        # Log error but continue with other symbols
        logger.error(f"[Stage {stage_num}] [{symbol}] Failed: {str(e)}")
        funnel_tracker.record_error(stage_num, symbol, str(e))
        results.append({
            'status': 'failed',
            'symbol': symbol,
            'error': str(e)
        })

# Pipeline continues even if some symbols failed
```

**Stage-Level Isolation:**

```python
# In main pipeline loop
for stage_num in range(config.start_stage, config.end_stage + 1):
    try:
        results = run_stage_parallel(stage_num, interval, ...)

        # Continue even if stage partially failed
        successful = [r for r in results if r['status'] == 'success']
        if len(successful) == 0:
            logger.error(f"Stage {stage_num} failed for all symbols!")
            # Option: Continue or abort based on configuration
        else:
            logger.info(f"Stage {stage_num} completed: {len(successful)} succeeded")

    except Exception as e:
        logger.error(f"Stage {stage_num} exception: {str(e)}")
        # Option: Continue or abort
```

### 9.3 Logging System

**Log File:** `pipeline_runs/run_{timestamp}/pipeline.log`

**Log Levels:**
- **DEBUG:** Detailed execution info (with `--verbose`)
- **INFO:** Stage execution, progress updates
- **WARNING:** Missing files, validation issues
- **ERROR:** Stage failures, exceptions
- **CRITICAL:** Fatal errors that stop pipeline

**Example Log Output:**
```
2025-11-20 16:19:24 - INFO - ================================================================================
2025-11-20 16:19:24 - INFO - IBKR ALPHA VALIDATION PIPELINE
2025-11-20 16:19:24 - INFO - ================================================================================
2025-11-20 16:19:24 - INFO - Run Directory: pipeline_runs/run_20251120_161924
2025-11-20 16:19:24 - INFO - Intervals: ['1h']
2025-11-20 16:19:24 - INFO - Symbols: ['MBT', 'MET']
2025-11-20 16:19:24 - INFO - Stages: 1 to 7
2025-11-20 16:19:24 - INFO - Max Workers: 2
2025-11-20 16:19:24 - INFO - ================================================================================
2025-11-20 16:19:24 - INFO -
2025-11-20 16:19:24 - INFO - [Interval 1/1] Processing: 1h
2025-11-20 16:19:24 - INFO - ================================================================================
2025-11-20 16:19:24 - INFO -
2025-11-20 16:19:24 - INFO - [Stage 1] Starting: Alpha Compilation
2025-11-20 16:19:24 - INFO - [Stage 1] [1h] [MBT] Starting: Alpha Compilation
2025-11-20 16:21:03 - INFO - [Stage 1] [1h] [MBT] Completed in 98.5s
2025-11-20 16:21:06 - INFO - [Stage 1] [1h] [MET] Completed in 102.3s
2025-11-20 16:21:06 - INFO - [Stage 1] [1h] Summary: 2 succeeded, 0 failed
2025-11-20 16:21:06 - INFO - [Stage 1] Checkpoint saved
...
```

### 9.4 Common Errors and Solutions

| Error | Cause | Solution |
|-------|-------|----------|
| `FileNotFoundError: No input data found` | Missing CSV files | Verify GridSearch_Data/ contains merged_*.csv files |
| `UnicodeEncodeError` | Unicode characters in output | Fixed in latest version (replaced with ASCII) |
| `MemoryError` | Insufficient RAM | Reduce max_workers or process fewer symbols |
| `TimeoutExpired` | Stage taking too long | Increase timeout or check data size |
| `ValueError: Minimum data length not met` | Insufficient historical data | Requires at least 1200 rows per symbol |
| `KeyError: 'Symbol' column not found` | Incorrect input format | Verify WF_Short worksheet exists and has correct columns |

---

## 10. Directory Structure

### 10.1 Complete Directory Tree

```
d:\Backup\AQS_SFGS\
│
├── [ORCHESTRATORS]
├── run_ibkr_gs_pipeline.py                  # Main pipeline orchestrator
├── AQS_SFGrid_parallel.py                   # Grid search parallelization engine
├── funnel_tracker.py                        # Metrics tracking
│
├── [UTILITIES]
├── util_AQS_parallel.py                     # Core utility functions
├── data_generator.py                        # IBKR data fetcher
├── start_generator.py                       # Data generator launcher
├── overfit_detector.py                      # Overfitting analysis
├── compile_overfitting_scores.py            # Overfitting compilation
│
├── [STAGE SCRIPTS - ROOT LEVEL]
├── generate_ibkr_gs_wf_alpha_results.py     # Stage 4
├── compile_ibkr_gs_wfalpha_results.py       # Stage 5
├── compile_ibkr_gs_combination_strategies.py # Stage 6
├── generate_ibkr_gs_final_compilation.py    # Stage 7
│
├── [INPUT DATA]
├── GridSearch_Data/                         # INPUT: 670+ OHLCV data files
│   ├── merged_ibkr_NVDA_1h_wiho_15Dec2025.csv
│   ├── merged_ibkr_AAPL_1h_wiho_15Dec2025.csv
│   ├── merged_ibkr_MSFT_1h_wiho_15Dec2025.csv
│   └── ... (670+ symbol files)
│
├── [STAGE 1-3 OUTPUTS]
├── AQS_SFGridResults/
│   ├── compile_ibkr_gs_alphas.py            # Stage 1 script
│   ├── validate_ibkr_gs_walk_forward.py     # Stage 2 script
│   ├── compile_ibkr_gs_walk_forward_results.py # Stage 3 script
│   │
│   ├── merged_ibkr_NVDA_1h_wiho_15Dec2025/  # 670+ symbol folders
│   ├── merged_ibkr_AAPL_1h_wiho_15Dec2025/
│   └── ... [more symbols]
│       └── {feature}/{model}/{strategy}/
│           ├── IS_OOS_comparison.csv        # Grid search results
│           ├── backtest.csv
│           └── walk_forward_report.csv      # Stage 2 output
│   │
│   ├── Alpha_GS_Compilation_ibkr_1h_20260101.xlsx (263 MB)
│   └── WF_GS_Compilation_ibkr_1h_20260101.xlsx (94 MB)
│
├── [STAGE 4-7 OUTPUTS]
├── WFAlphaResults/
│   ├── merged_ibkr_NVDA_1h_linear/          # Regenerated alphas
│   ├── merged_ibkr_AAPL_1h_linear/
│   └── ... [more symbols]
│       └── {feature}/{model}/{strategy}/
│           ├── backtest.csv                 # Stage 4: Full timeseries
│           ├── metrics.csv                  # Stage 4: Summary metrics
│           ├── summary.csv                  # Stage 4: Configuration
│           └── cumu_pnl_vs_bnh.png         # Stage 4: Equity curve
│   │
│   ├── WFAlpha_Compilation_ibkr_1h_20260103.xlsx
│   ├── Combination_Strategy_Compilation_ibkr_1h_20260103.xlsx (653 MB)
│   └── Final_Compilation_ibkr_1h_20260103.xlsx (280 MB)
│
├── [DEPLOYMENT]
├── deploy_ibkr/                             # 3,530 deployed strategy files
│   ├── NVDA_high_min_max_mr.py
│   ├── AAPL_close_QQQ_spread_maxabs_norm_trend_reverse.py
│   └── ... [3,528 more strategy files]
│
├── deploy_ibkr_portfolio/                   # Portfolio deployment
│
├── [RUN TRACKING]
└── pipeline_runs/
    └── run_20260101_161924/
        ├── pipeline.log                     # Execution log
        ├── checkpoint.json                  # Resume data
        ├── funnel_report.xlsx               # Metrics report
        └── config.json                      # Pipeline configuration
```

### 10.2 Key File Types

| Extension | Purpose | Generated By |
|-----------|---------|--------------|
| `.csv` | Data files (OHLCV, backtests, IS_OOS_comparison) | All stages |
| `.xlsx` | Compilation files (multi-worksheet) | Stages 1, 3, 5, 6, 7 |
| `.png` | Equity curve visualizations | Stage 4 |
| `.py` | Deployed strategy files | Deployment process |
| `.log` | Execution logs | run_ibkr_gs_pipeline.py |
| `.json` | Configuration and checkpoints | run_ibkr_pipeline.py |

### 10.3 Glob Pattern Summary

| Pattern | Matches | Used In |
|---------|---------|---------|
| `merged_{exchange}_{symbol}_{interval}_*` | Data directories | All stages |
| `merged_{exchange}_{symbol}_{interval}_*.csv` | OHLCV CSV files | Stages 2, 4, 6, 7 |
| `**/IS_OOS_comparison.csv` | Grid search IS/OOS results | Stage 1 |
| `**/walk_forward_report.csv` | WF validation reports | Stage 3 |
| `**/summary.csv` | Strategy summaries | Stage 5 |
| `**/backtest.csv` | Full backtest timeseries | Stages 4, 6, 7 |

---

## Appendix A: Execution Example

**Recent Run:** `pipeline_runs/run_20251120_161924/`

### Configuration
- **Run Directory:** `pipeline_runs/run_20260101_161924`
- **Execution Date:** 2026-01-01
- **Intervals:** `['1h']`
- **Symbols:** `670+ US equities (NVDA, AAPL, MSFT, ...)`
- **Stages:** 1 to 7
- **Max Workers:** 8

### Execution Summary

| Stage | Duration | Symbols | Status |
|-------|----------|---------|--------|
| 1: Alpha Compilation | ~45 min | 670+ | SUCCESS |
| 2: Walk-Forward Validation | ~2 hrs | 670+ | SUCCESS |
| 3: WF Results Compilation | ~5 min | 670+ | SUCCESS |
| 4: WF Alpha Generation | ~3 hrs | 670+ | SUCCESS |
| 5: WF Alpha Compilation | ~10 min | 670+ | SUCCESS |
| 6: Combination Strategies | ~1 hr | 670+ | SUCCESS |
| 7: Final Compilation | ~20 min | 670+ | SUCCESS |
| **TOTAL** | **~8 hrs** | **4,690+ tasks** | **SUCCESS** |

### Metrics
- **Total Tasks:** 4,690+ (7 stages × 670+ symbols)
- **Successful:** 4,690+
- **Failed:** 0
- **Success Rate:** 100.0%

### Output Files Generated
1. `AQS_SFGridResults/Alpha_GS_Compilation_ibkr_1h_20260101.xlsx` (263 MB)
2. `AQS_SFGridResults/WF_GS_Compilation_ibkr_1h_20260101.xlsx` (94 MB)
3. `WFAlphaResults/WFAlpha_Compilation_ibkr_1h_20260103.xlsx`
4. `WFAlphaResults/Combination_Strategy_Compilation_ibkr_1h_20260103.xlsx` (653 MB)
5. `WFAlphaResults/Final_Compilation_ibkr_1h_20260103.xlsx` (280 MB)
6. `pipeline_runs/run_20260101_161924/funnel_report.xlsx`

---

## Appendix B: Quick Reference

### Command Cheat Sheet

```bash
# Standard execution
python run_ibkr_gs_pipeline.py

# Specific configuration
python run_ibkr_gs_pipeline.py --intervals 1h --symbols MBT MET --start-stage 1 --end-stage 7

# Resume from checkpoint
python run_ibkr_gs_pipeline.py --resume pipeline_runs/run_20251120_161924/checkpoint.json

# Partial execution
python run_ibkr_gs_pipeline.py --start-stage 5 --end-stage 7

# Verbose logging
python run_ibkr_gs_pipeline.py --verbose
```

### Stage Quick Reference

| Stage | Input | Output | Runtime |
|-------|-------|--------|---------|
| 1 | Strategy folders | Alpha_Compilation Excel | 100s |
| 2 | Excel + CSV | walk_forward_report CSVs | 290s |
| 3 | walk_forward_report CSVs | WF_Compilation Excel | 12s |
| 4 | Excel + CSV | Nested backtest files | 675s |
| 5 | Nested backtest files | WFAlpha_Compilation Excel | 6s |
| 6 | WFAlpha Excel | Combination Excel | 182s |
| 7 | Combination Excel | Final_Compilation Excel | 15s |

### File Path Quick Reference

```python
# Input data
data_path = "GridSearch_Data/merged_ibkr_{symbol}_1h_wiho_{date}.csv"

# Stage 1 output
stage1_output = "AQS_SFGridResults/Alpha_GS_Compilation_ibkr_1h_{date}.xlsx"

# Stage 3 output
stage3_output = "AQS_SFGridResults/WF_GS_Compilation_ibkr_1h_{date}.xlsx"

# Stage 4 output (nested)
stage4_output = "WFAlphaResults/merged_ibkr_{symbol}_1h_linear/{feature}/{model}/{strategy}/backtest.csv"

# Stage 5 output
stage5_output = "WFAlphaResults/WFAlpha_Compilation_ibkr_1h_{date}.xlsx"

# Stage 6 output
stage6_output = "WFAlphaResults/Combination_Strategy_Compilation_ibkr_1h_{date}.xlsx"

# Stage 7 output
stage7_output = "WFAlphaResults/Final_Compilation_ibkr_1h_{date}.xlsx"

# Funnel report
funnel_report = "pipeline_runs/run_{timestamp}/funnel_report.xlsx"
```

---

## Appendix C: Troubleshooting

### Common Issues

**Issue:** Pipeline hangs at Stage 2
- **Cause:** Insufficient data for walk-forward split
- **Solution:** Verify CSV files have >= 1200 rows

**Issue:** Unicode encoding errors
- **Cause:** Windows console doesn't support Unicode
- **Solution:** Latest version uses ASCII-safe characters ([OK], [ERROR])

**Issue:** Out of memory
- **Cause:** Too many symbols processed in parallel
- **Solution:** Reduce `--max-workers` or process fewer symbols

**Issue:** Missing output files
- **Cause:** Stage failed silently
- **Solution:** Check `pipeline.log` for errors

**Issue:** Checkpoint resume fails
- **Cause:** Checkpoint file corrupted
- **Solution:** Restart from beginning or manual stage selection

---

## 11. Data Generator

The AQS_SFGS pipeline includes a data generation component for fetching historical OHLCV data from Interactive Brokers.

### 11.1 Core Scripts

| Script | Purpose |
|--------|---------|
| `data_generator.py` | Main data fetcher for IBKR historical data |
| `start_generator.py` | Launcher script with scheduling capability |

### 11.2 Features

- **Symbol Coverage:** 670+ US equities across multiple sectors
- **Data Integration:**
  - VIX data integration for volatility context
  - Benchmark mapping (SPY, QQQ sector indices)
  - Technical indicator computation (moving averages, RSI, etc.)
- **Output Format:** CSV files with OHLCV + features, saved to `GridSearch_Data/`
- **Naming Convention:** `merged_ibkr_{symbol}_{interval}_wiho_{date}.csv`

### 11.3 Usage

```bash
# Start data generator
python start_generator.py

# Generator fetches data for all configured symbols
# Output: GridSearch_Data/merged_ibkr_{symbol}_1h_wiho_{date}.csv
```

---

## 12. Deployment

After pipeline completion, validated strategies are deployed for live trading.

### 12.1 Deployment Folders

| Folder | Purpose | Contents |
|--------|---------|----------|
| `deploy_ibkr/` | Individual strategy files | 3,530+ .py files |
| `deploy_ibkr_portfolio/` | Portfolio-level deployment | Combined strategy configurations |

### 12.2 Strategy File Format

Each deployed strategy file (e.g., `NVDA_high_min_max_mr.py`) contains:
- Symbol and interval configuration
- Feature extraction logic
- Model parameters (optimized from grid search)
- Entry/exit signal generation
- Position sizing rules

### 12.3 Deployment Process

1. **Selection:** Top strategies from `Final_Compilation_*.xlsx` are selected
2. **Code Generation:** Strategy logic exported to standalone Python files
3. **Portfolio Construction:** Correlation-filtered strategies grouped into portfolios
4. **Live Execution:** Deployed to IBKR trading infrastructure

---

## 13. Overfitting Detection

The pipeline includes utilities for detecting potential overfitting in grid search optimization.

### 13.1 Detection Scripts

| Script | Purpose |
|--------|---------|
| `overfit_detector.py` | Analyzes IS vs OOS performance degradation |
| `compile_overfitting_scores.py` | Aggregates overfitting metrics across strategies |

### 13.2 Key Metrics

- **Performance Degradation:** Compares IS Sharpe to OOS Sharpe ratio
- **Threshold:** `DEGRADATION_MIN = -10%` (strategies with >10% degradation flagged)
- **Parameter Sensitivity:** Identifies strategies overly sensitive to parameter changes

---

## Document Revision History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-11-20 | Initial comprehensive documentation |
| 1.1 | 2025-11-21 | Added Alpha Summary feature documentation in Section 4.6 |
| 2.0 | 2026-01-09 | Updated for AQS_SFGS: 252-based annualization, US equities, Grid Search workflow |

---

**End of Documentation**
