"""
util_AQS_parallel.py
====================

Enhanced utility functions for AQS (Algorithmic Quantitative Strategy) backtesting
with In-Sample/Out-of-Sample split support.

Based on util_parallel.py with additional functions for IS/OOS validation:
- split_data_is_oos(): Split data into IS and OOS periods with warmup
- recalculate_oos_metrics(): Recalculate metrics after dropping warmup
- get_period_from_folder(): Extract annualization period from folder name
- calculate_degradation(): Calculate performance degradation between periods
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend to avoid threading issues
import matplotlib.pyplot as plt
import seaborn as sns
import requests
from functools import reduce

# ============================================================================
# PARAMETER CONFIGURATION
# ============================================================================

# Generate a range of values for parameter grid search
def get_range(start, end, step):
    # Create numpy array from start to end with step size, convert to list
    return np.arange(start, end, step).tolist()

# Configuration dictionary defining parameter ranges for different normalization models
config = {
    "models": {
        "zscore": {
            "param1": [10, 200, 10],      # length: start=10, end=200, step=10
            "param2": [-3, 3, 0.25]        # entry_threshold: start=0, end=3, step=0.25
        },
        "min_max": {
            "param1": [10, 70, 5],       # length range
            "param2": [-1, 1, 0.05]        # threshold range for min-max scaled values
        },
        "sma_diff": {
            "param1": [5, 255, 5],       # SMA length range
            "param2": [-2.0, 2.0, 0.1]    # percentage difference threshold range
        },
        "robust_scaler": {
            "param1": [10, 260, 10],       # rolling window length
            "param2": [0.00, 1.50, 0.05]        # IQR-based threshold range
        },
        "maxabs_norm": {
            "param1": [20, 300, 10],       # rolling window length
            "param2": [-1.2, 1.2, 0.04]        # normalized value threshold range
        },
        "rsi": {
            "param1": [7, 51, 2],       # RSI period range
            "param2": [20, 80, 5]        # RSI level threshold (RSI is 0-100)
        },
    }
}

# ============================================================================
# NEW: IS/OOS SPLIT FUNCTIONS
# ============================================================================

def split_data_is_oos(df, is_ratio=0.6, warmup_length=300, oos_pre_warmup="Y"):
    """
    Split data into In-Sample and Out-of-Sample periods with optional warmup.

    Parameters:
    -----------
    df : pd.DataFrame
        Full dataset to split
    is_ratio : float
        Ratio for in-sample split (default 0.6 = 60% IS, 40% OOS)
    warmup_length : int
        Number of bars to use for warmup period (default 300)
    oos_pre_warmup : str
        "Y" = Allow warmup overlap with IS data
        "N" = No overlap (use first N OOS bars as warmup)

    Returns:
    --------
    tuple: (df_is, df_oos_warmup, oos_start_idx)
        df_is : pd.DataFrame
            In-sample data (first is_ratio% of data)
        df_oos_warmup : pd.DataFrame
            OOS data with warmup (includes warmup period for indicators)
        oos_start_idx : int
            Index where true OOS begins in df_oos_warmup (for metric calculation)
    """
    total_rows = len(df)
    split_idx = int(total_rows * is_ratio)

    # In-Sample data (clean split)
    df_is = df.iloc[:split_idx].copy()

    if oos_pre_warmup == "Y":
        # Allow warmup overlap with IS data (existing pattern)
        warmup_start = max(0, split_idx - warmup_length)
        df_oos_warmup = df.iloc[warmup_start:].copy().reset_index(drop=True)
        oos_start_idx = split_idx - warmup_start
    else:
        # No overlap - use first N OOS bars as warmup
        # This reduces effective OOS size but maintains clean separation
        df_oos_warmup = df.iloc[split_idx:].copy().reset_index(drop=True)
        oos_start_idx = min(warmup_length, len(df_oos_warmup))

    return df_is, df_oos_warmup, oos_start_idx


def recalculate_oos_metrics(df_backtest, oos_start_idx, period):
    """
    Recalculate OOS metrics after dropping warmup period rows.

    This function prevents warmup contamination by:
    1. Dropping warmup rows from the backtest results
    2. Recalculating all metrics on the true OOS window only

    Parameters:
    -----------
    df_backtest : pd.DataFrame
        Full backtest DataFrame (warmup + OOS) with 'pnl', 'trade' columns
    oos_start_idx : int
        Index where true OOS begins (skip warmup)
    period : int
        Period for annualization (e.g., 365*24 for hourly data)

    Returns:
    --------
    dict : OOS performance metrics without warmup contamination
        'Sharpe Ratio', 'Max Drawdown', 'Trade Count',
        'Annualized Return', 'Calmar Ratio'
    """
    # Drop warmup rows - only keep true OOS window
    df_oos_only = df_backtest.iloc[oos_start_idx:].copy().reset_index(drop=True)

    # Recalculate metrics on OOS window only
    try:
        # Calculate Sharpe Ratio
        if df_oos_only['pnl'].std() > 0:
            sharpe_ratio = (df_oos_only['pnl'].mean() / df_oos_only['pnl'].std()) * np.sqrt(period)
        else:
            sharpe_ratio = 0.0

        # Calculate Max Drawdown
        df_oos_only['cumulative_pnl'] = df_oos_only['pnl'].cumsum()
        df_oos_only['dd'] = df_oos_only['cumulative_pnl'] - df_oos_only['cumulative_pnl'].cummax()
        max_drawdown = df_oos_only['dd'].min() if not df_oos_only['dd'].empty else 0

        # Calculate Trade Count
        trade_count = df_oos_only['trade'].sum()

        # Calculate Annualized Return
        annualized_return = df_oos_only['pnl'].mean() * period

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
        print(f"  Error recalculating OOS metrics: {e}")
        return {
            'Sharpe Ratio': np.nan,
            'Max Drawdown': np.nan,
            'Trade Count': np.nan,
            'Annualized Return': np.nan,
            'Calmar Ratio': np.nan
        }


def get_period_from_folder(folder_name):
    """
    Extract interval from folder name and return appropriate period for annualization.
    Uses non-crypto market assumptions (252 trading days, 8-hour sessions).

    Parameters:
    -----------
    folder_name : str
        Folder name containing interval info (e.g., "merged_binance_BTCUSDT_1h_COMPLETE")

    Returns:
    --------
    int : Number of periods per year for annualization

    Raises:
    -------
    ValueError : If no interval found or multiple intervals detected
    """
    folder_lower = folder_name.lower()

    # Check for all interval patterns using underscore delimiters to avoid
    # substring collisions (e.g., "15min" matching "5min", "1min")
    intervals_found = []
    for interval in ['15min', '30min', '5min', '1min', '1h', '4h', '1d', '1w']:
        if f"_{interval}_" in folder_lower or folder_lower.endswith(f"_{interval}"):
            intervals_found.append(interval)

    # Error if multiple intervals detected
    if len(intervals_found) > 1:
        raise ValueError(f"Multiple intervals detected in '{folder_name}': {intervals_found}. "
                        "Filename should contain only one interval pattern.")

    # Error if no interval detected
    if len(intervals_found) == 0:
        raise ValueError(f"No interval pattern detected in '{folder_name}'. "
                        "Expected one of: 1min, 5min, 15min, 30min, 1h, 4h, 1d, 1w")

    # Calculate period based on interval (non-crypto market assumptions)
    interval = intervals_found[0]

    if interval == "1w":
        return 52  # 52 trading weeks per year
    if interval == "1d":
        return 252  # 252 trading days per year
    elif interval == "4h":
        return 252 * 2  # 2 periods per day
    elif interval == "1h":
        return 252 * 8  # 8 hours per session
    elif interval == "30min":
        return 252 * 8 * 2  # = 4032
    elif interval == "15min":
        return 252 * 8 * 4  # = 8064
    elif interval == "5min":
        return 252 * 8 * 12  
    elif interval == "1min":
        return 252 * 8 * 60  



def get_period_for_interval(interval):
    """
    Return appropriate period for annualization based on interval string.

    Parameters:
    -----------
    interval : str
        Interval string (e.g., "1min", "5min", "15min", "30min", "1h", "4h", "1d", "1w")

    Returns:
    --------
    int : Number of periods per year for annualization

    Raises:
    -------
    ValueError : If interval is not recognized
    """
    interval_lower = interval.lower()

    if interval_lower == "1w":
        return 52
    elif interval_lower == "1d":
        return 252
    elif interval_lower == "4h":
        return 252 * 2
    elif interval_lower == "1h":
        return 252 * 8
    elif interval_lower == "30min":
        return 252 * 8 * 2
    elif interval_lower == "15min":
        return 252 * 8 * 4
    elif interval_lower == "5min":
        return 252 * 8 * 12
    elif interval_lower == "1min":
        return 252 * 8 * 60

    else:
        raise ValueError(f"Unrecognized interval: '{interval}'. Expected one of: 1min, 5min, 15min, 30min, 1h, 4h, 1d")


def calculate_degradation(is_val, oos_val):
    """
    Calculate performance degradation from IS to OOS period.

    Parameters:
    -----------
    is_val : float
        In-sample metric value
    oos_val : float
        Out-of-sample metric value

    Returns:
    --------
    float : Degradation percentage
        Positive = improvement, Negative = degradation
    """
    if is_val != 0:
        return ((oos_val - is_val) / is_val) * 100
    return 0.0

# ============================================================================
# NORMALIZATION MODELS
# ============================================================================

# Calculate z-score normalization on a rolling window
def zscore(df, length, column):
    # Calculate rolling simple moving average
    df['sma'] = df[column].rolling(window=length).mean()
    # Calculate rolling standard deviation
    df['std'] = df[column].rolling(window=length).std()
    # Calculate z-score: (value - mean) / std
    df['zscore'] = (df[column] - df['sma']) / df['std']
    return df

# Calculate simple moving average
def sma(df, length, column='close'):
    # Calculate rolling mean over specified window length
    df['sma'] = df[column].rolling(window=length).mean()
    return df

# Min-max normalization to scale values between -1 and 1
def min_max(df, length, column):
    # Calculate rolling minimum over window
    df['min'] = df[column].rolling(length).min()
    # Calculate rolling maximum over window
    df['max'] = df[column].rolling(length).max()

    # Min-max scaling formula: 2 * ((value - min) / (max - min)) - 1
    # Maps [min, max] to [-1, 1]
    df['min_max'] = 2 * ((df[column] - df['min']) / (df['max'] - df['min'])) - 1
    return df

# Calculate percentage difference from SMA
def sma_diff(df, length, column='close'):
    # Calculate rolling simple moving average
    df['sma'] = df[column].rolling(length).mean()
    # Calculate percentage difference: (price/sma) - 1
    df['sma_diff'] = (df[column]/df['sma'] - 1)
    return df

# Min-Max Normalization (scales between 0 and 1 within a rolling window)
def minmax_norm(df, length, column):
    # Calculate rolling minimum
    df['min'] = df[column].rolling(window=length).min()
    # Calculate rolling maximum
    df['max'] = df[column].rolling(window=length).max()
    # Normalize to [0, 1]: (value - min) / (max - min)
    df['minmax_norm'] = (df[column] - df['min']) / (df['max'] - df['min'])
    return df

# Mean Normalization (scales between -1 and 1 around rolling mean)
def mean_norm(df, length, column):
    # Calculate rolling minimum
    df['min'] = df[column].rolling(window=length).min()
    # Calculate rolling maximum
    df['max'] = df[column].rolling(window=length).max()
    # Calculate rolling mean
    df['mean'] = df[column].rolling(window=length).mean()
    # Normalize around mean: (value - mean) / (max - min)
    df['mean_norm'] = (df[column] - df['mean']) / (df['max'] - df['min'])
    return df

# Max Absolute Scaling (rolling max abs)
def maxabs_norm(df, length, column):
    # Calculate rolling maximum of absolute values
    df['max_abs'] = df[column].rolling(window=length).apply(lambda x: np.abs(x).max(), raw=True)
    # Scale by max absolute value: value / max_abs
    df['maxabs_norm'] = df[column] / df['max_abs']
    return df

# Robust Scaling (median and IQR based, rolling)
def robust_scaler(df, length, column):
    # Calculate rolling median
    df['median'] = df[column].rolling(window=length).median()
    # Calculate first quartile (25th percentile)
    df['q1'] = df[column].rolling(window=length).quantile(0.25)
    # Calculate third quartile (75th percentile)
    df['q3'] = df[column].rolling(window=length).quantile(0.75)
    # Calculate interquartile range (IQR)
    df['iqr'] = df['q3'] - df['q1']
    # Scale using median and IQR: (value - median) / IQR
    df['robust_scaler'] = (df[column] - df['median']) / df['iqr']
    return df

# Calculate Relative Strength Index (RSI) - OPTIMIZED VERSION
def rsi(df, length, column):
    # Calculate price change from previous period
    delta = df[column].diff()
    # Extract gains (positive changes, 0 otherwise) - OPTIMIZED with clip
    gain = delta.clip(lower=0)
    # Extract losses (absolute negative changes, 0 otherwise) - OPTIMIZED with clip
    loss = -delta.clip(upper=0)

    # Calculate average gain over rolling window
    avg_gain = gain.rolling(window=length, min_periods=length).mean()
    # Calculate average loss over rolling window
    avg_loss = loss.rolling(window=length, min_periods=length).mean()

    # Calculate relative strength (RS)
    rs = avg_gain / avg_loss
    # Calculate RSI using formula: 100 - (100 / (1 + RS))
    df['rsi'] = 100 - (100 / (1 + rs))
    return df

# ============================================================================
# SIGNAL GENERATION
# ============================================================================

# Generate trading signals based on threshold conditions
def signal_threshold(df, buy_type, column, entry_threshold, exit_threshold=0):

    # Trend-following long: Enter long when indicator >= entry_threshold, exit when <= exit_threshold
    if buy_type == 'trend_long':
        df['signal'] = np.where(df[column].shift(1) >= entry_threshold , 1, np.where(df[column].shift(1) <= exit_threshold, 0, np.nan))
        df['signal'] = df['signal'].ffill().fillna(0)  # Carry forward the last signal

    # Trend-following short: Enter short when indicator >= entry_threshold, exit when <= exit_threshold
    elif buy_type == 'trend_short':
        df['signal'] = np.where(df[column].shift(1) >= entry_threshold , -1, np.where(df[column].shift(1) <= exit_threshold, 0, np.nan))
        df['signal'] = df['signal'].ffill().fillna(0)

    # Reverse trend long: Enter long when indicator <= entry_threshold, exit when >= exit_threshold
    elif buy_type == 'trend_reverse_long':
        df['signal'] = np.where(df[column].shift(1) <= entry_threshold , 1, np.where(df[column].shift(1) >= exit_threshold, 0, np.nan))
        df['signal'] = df['signal'].ffill().fillna(0)

    # Reverse trend short: Enter short when indicator <= entry_threshold, exit when >= exit_threshold
    elif buy_type == 'trend_reverse_short':
        df['signal'] = np.where(df[column].shift(1) <= entry_threshold , -1, np.where(df[column].shift(1) >= exit_threshold, 0, np.nan))
        df['signal'] = df['signal'].ffill().fillna(0)

    # Bidirectional trend: Long when >= entry, short when <= exit
    # elif buy_type == "trend":
    #     df['signal'] = np.where(df[column].shift(1) >= entry_threshold, 1, np.where(df[column].shift(1) <= exit_threshold, -1, np.nan))
    #     df['signal'] = df['signal'].ffill().fillna(0)
    # Bidirectional trend: Long when >= entry, short when <= -entry
    elif buy_type == "trend":
        df['signal'] = np.where(df[column].shift(1) >= entry_threshold, 1, np.where(df[column].shift(1) <= -entry_threshold, -1, np.nan))
        df['signal'] = df['signal'].ffill().fillna(0)

    # Reverse bidirectional trend: Short when >= entry, long when <= exit
    # elif buy_type == "trend_reverse":
    #     df['signal'] = np.where(df[column].shift(1) >= entry_threshold, -1, np.where(df[column].shift(1) <= exit_threshold, 1, np.nan))
    #     df['signal'] = df['signal'].ffill().fillna(0)
    # Reverse bidirectional trend: Short when >= entry, long when <= -entry
    elif buy_type == "trend_reverse":
        df['signal'] = np.where(df[column].shift(1) >= entry_threshold, -1, np.where(df[column].shift(1) <= -entry_threshold, 1, np.nan))
        df['signal'] = df['signal'].ffill().fillna(0)

    # Mean reversion: Short when > threshold (expect reversion down), long when < -threshold (expect reversion up)
    elif buy_type == 'mr':
        # Condition 1: Short signal when indicator > entry_threshold, exit when crosses 0
        cond1 = np.where(
            df[column].shift(1) > entry_threshold,
            1,
            np.where(df[column] .shift(1) < 0, 0, np.nan)
        )

        # Condition 2: Long signal when indicator < -entry_threshold, exit when crosses 0
        cond2 = np.where(
            df[column].shift(1) < -entry_threshold,
            -1,
            np.where(df[column].shift(1) > 0, 0, np.nan)
        )

        # Combine both conditions (sum of 1 and -1 signals)
        df['signal'] = pd.Series(cond1).ffill() + pd.Series(cond2).ffill()

    # Reverse mean reversion: Long when > threshold, short when < -threshold
    elif buy_type == 'mr_reverse':
        # Condition 1: Long signal when indicator > entry_threshold
        cond1 = np.where(
            df[column].shift(1) > entry_threshold,
            -1,
            np.where(df[column].shift(1) < 0, 0, np.nan)
        )

        # Condition 2: Short signal when indicator < -entry_threshold
        cond2 = np.where(
            df[column].shift(1) < -entry_threshold,
            1,
            np.where(df[column].shift(1) > 0, 0, np.nan)
        )

        # Combine both conditions
        df['signal'] = pd.Series(cond1).ffill() + pd.Series(cond2).ffill()

    # Forward fill any remaining NaN values and fill initial NaNs with 0
    df['signal'] = df['signal'].ffill().fillna(0)

    return df

# ============================================================================
# BACKTEST ENGINE
# ============================================================================

# Calculate backtest performance metrics
def calculate(df, fee, period=365):

    # Calculate trade occurrences (signal change = trade)
    df['trade'] = abs(df['signal'].shift(1) - df['signal'])
    # Calculate trading fees (fee per trade)
    df['fee'] = df['trade'] * fee
    # Calculate PnL: position * return - fees
    df['pnl'] = df['signal'].shift(1)*(df['close']/df['close'].shift(1) - 1) - df['fee']
    # Calculate cumulative PnL over time
    df['cumulative_pnl'] = df['pnl'].cumsum()
    # Drop rows with NaN values
    df.dropna(inplace=True)
    # Calculate buy-and-hold cumulative returns
    df['bnh'] = (df['close']/df['close'].shift(1) - 1).cumsum()
    # Calculate drawdown (distance from peak cumulative PnL)
    df['dd'] = df['cumulative_pnl'] - df['cumulative_pnl'].cummax()

    # Calculate Sharpe ratio (risk-adjusted return) - annualized
    Sharpe_ratio = df['pnl'].mean() / df['pnl'].std() * np.sqrt(period)  if df['pnl'].std() != 0 else 0
    # Get maximum drawdown (most negative value)
    max_drawdown = df['dd'].min() if not df['dd'].empty else 0
    # Count total number of trades
    trade_count = df['trade'].sum()
    # Calculate annualized return
    annualized_return = (df['pnl'].mean()) * period
    # Calculate Calmar ratio (return / max drawdown)
    calmar_ratio = annualized_return / abs(max_drawdown) if max_drawdown != 0 else 0

    # Create performance metrics dictionary
    log = {
        'Sharpe Ratio': Sharpe_ratio,
        'Max Drawdown': max_drawdown,
        'Trade Count': trade_count,
        'Annualized Return': annualized_return,
        'Calmar Ratio': calmar_ratio,
        }

    return df, log

# Master function to generate signals and run backtest for a specific model
def generate_all_signals(df, model, buy_type, column, length, entry_threshold, exit_threshold=0, period=365):

    # Create copy to avoid modifying original DataFrame
    df = df.copy()

    # Apply z-score normalization model
    if model == 'zscore':
        df = zscore(df, length, column=column)
        df = signal_threshold(df, buy_type, column='zscore', entry_threshold= entry_threshold, exit_threshold= exit_threshold)
    # Apply min-max normalization model
    if model == 'min_max':
        df = min_max(df, length, column=column)
        df = signal_threshold(df, buy_type, column='min_max', entry_threshold= entry_threshold, exit_threshold= exit_threshold)
    # Apply SMA difference model
    if model == 'sma_diff':
        df = sma_diff(df, length, column=column)
        df = signal_threshold(df, buy_type, column='sma_diff', entry_threshold= entry_threshold, exit_threshold= exit_threshold)
    # Apply robust scaler model
    if model == 'robust_scaler':
        df = robust_scaler(df, length, column=column)
        df = signal_threshold(df, buy_type, column='robust_scaler', entry_threshold= entry_threshold, exit_threshold= exit_threshold)
    # Apply max absolute scaling model
    if model == 'maxabs_norm':
        df = maxabs_norm(df, length, column= column)
        df = signal_threshold(df, buy_type, column='maxabs_norm', entry_threshold= entry_threshold, exit_threshold= exit_threshold)
    # Apply RSI model
    if model == 'rsi':
        df = rsi(df, length, column)
        df =signal_threshold(df, buy_type, column='rsi', entry_threshold= entry_threshold, exit_threshold= exit_threshold)


    # Run backtest calculation with fee of 0.06% (6 basis points)
    df, log = calculate(df, fee=0.06/100, period=period)
    return df, log

# ============================================================================
# RISK PARAMETER BACKTEST ENGINE
# ============================================================================

def calculate_with_risk_params(df, fee, period=365, sl_pct=0, tp_pct=0, tsl_pct=0, track_risk_levels=False):
    """
    Bar-by-bar backtest engine with Stop Loss, Take Profit, and Trailing Stop Loss overlays.

    Applies risk management exits on top of pre-computed signals. When a risk exit
    triggers, the position is forced flat until the original signal changes.

    Exit priority (conservative): SL → TSL → TP → signal_exit

    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame with columns: signal, open, high, low, close
    fee : float
        Transaction fee per trade (e.g., 0.0006 for 6 bps)
    period : int
        Annualization period (e.g., 252 for daily, 2016 for hourly)
    sl_pct : float
        Stop loss percentage (0 = disabled). E.g., 0.05 = 5%
    tp_pct : float
        Take profit percentage (0 = disabled). E.g., 0.10 = 10%
    tsl_pct : float
        Trailing stop loss percentage (0 = disabled). E.g., 0.03 = 3%
    track_risk_levels : bool
        If True, add SL/TSL/TP price-level columns to output DataFrame.
        Default False to avoid overhead during grid search.

    Returns:
    --------
    tuple: (pd.DataFrame, dict)
        DataFrame with columns: [SL, TSL, TP,] signal, trade, fee, pnl, cumulative_pnl, bnh, dd, exit_type
        Dict with metrics: Sharpe Ratio, Max Drawdown, Trade Count, Annualized Return, Calmar Ratio
    """
    df_out = df.copy()
    n = len(df_out)

    if n <= 1:
        if track_risk_levels:
            signal_idx = list(df_out.columns).index('signal')
            df_out.insert(signal_idx, 'TP', 0.0)
            df_out.insert(signal_idx, 'TSL', 0.0)
            df_out.insert(signal_idx, 'SL', 0.0)
        df_out['trade'] = 0.0
        df_out['fee'] = 0.0
        df_out['pnl'] = 0.0
        df_out['cumulative_pnl'] = 0.0
        df_out['bnh'] = 0.0
        df_out['dd'] = 0.0
        df_out['exit_type'] = ''
        return df_out, {'Sharpe Ratio': 0, 'Max Drawdown': 0, 'Trade Count': 0,
                        'Annualized Return': 0, 'Calmar Ratio': 0}

    # If all risk params are zero, delegate to original calculate() for consistency
    if sl_pct == 0 and tp_pct == 0 and tsl_pct == 0:
        df_out, log = calculate(df_out, fee, period)
        df_out['exit_type'] = ''
        if track_risk_levels:
            signal_idx = list(df_out.columns).index('signal')
            df_out.insert(signal_idx, 'TP', 0.0)
            df_out.insert(signal_idx, 'TSL', 0.0)
            df_out.insert(signal_idx, 'SL', 0.0)
        return df_out, log

    # Extract arrays for fast iteration
    original_signals = df_out['signal'].values.astype(float)
    highs = df_out['high'].values.astype(float)
    lows = df_out['low'].values.astype(float)
    closes = df_out['close'].values.astype(float)

    # Output arrays
    effective_signals = np.zeros(n, dtype=float)
    exit_types = np.empty(n, dtype=object)
    exit_types[:] = ''

    # Tracking arrays for risk price levels (only when requested)
    if track_risk_levels:
        sl_levels = np.zeros(n)
        tsl_levels = np.zeros(n)
        tp_levels = np.zeros(n)

    # State variables
    position = 0.0
    entry_price = 0.0
    watermark = 0.0
    forced_flat = False

    for t in range(n):
        orig_sig = original_signals[t]
        prev_orig = original_signals[t - 1] if t > 0 else 0.0

        # --- FORCED FLAT LOGIC ---
        if forced_flat:
            if orig_sig != prev_orig:
                forced_flat = False
            else:
                effective_signals[t] = 0.0
                continue

        # --- POSITION ENTRY / EXIT DETECTION ---
        prev_eff = effective_signals[t - 1] if t > 0 else 0.0
        desired = orig_sig

        if desired != prev_eff and desired != 0:
            # New entry or reversal
            entry_price = closes[t - 1] if t > 0 else closes[t]
            watermark = entry_price
            position = desired
        elif desired == 0 and prev_eff != 0:
            # Signal-driven exit
            exit_types[t] = 'signal_exit'
            position = 0.0
        else:
            # Continuation or flat
            position = desired

        # --- RISK EXIT CHECKS ---
        if position != 0 and entry_price > 0:
            high_t = highs[t]
            low_t = lows[t]

            # Update TSL watermark BEFORE checking exits
            if position == 1:
                if high_t > watermark:
                    watermark = high_t
            elif position == -1:
                if low_t < watermark:
                    watermark = low_t

            # Record risk price levels after watermark update, before exit checks
            if track_risk_levels:
                if position == 1:  # Long
                    sl_levels[t] = entry_price * (1 - sl_pct) if sl_pct > 0 else 0
                    tsl_levels[t] = watermark * (1 - tsl_pct) if tsl_pct > 0 else 0
                    tp_levels[t] = entry_price * (1 + tp_pct) if tp_pct > 0 else 0
                elif position == -1:  # Short
                    sl_levels[t] = entry_price * (1 + sl_pct) if sl_pct > 0 else 0
                    tsl_levels[t] = watermark * (1 + tsl_pct) if tsl_pct > 0 else 0
                    tp_levels[t] = entry_price * (1 - tp_pct) if tp_pct > 0 else 0

            triggered = None

            # Priority: SL → TSL → TP (conservative)
            if sl_pct > 0 and triggered is None:
                if position == 1 and low_t <= entry_price * (1 - sl_pct):
                    triggered = 'sl_exit'
                elif position == -1 and high_t >= entry_price * (1 + sl_pct):
                    triggered = 'sl_exit'

            if tsl_pct > 0 and triggered is None:
                if position == 1 and low_t <= watermark * (1 - tsl_pct):
                    triggered = 'tsl_exit'
                elif position == -1 and high_t >= watermark * (1 + tsl_pct):
                    triggered = 'tsl_exit'

            if tp_pct > 0 and triggered is None:
                if position == 1 and high_t >= entry_price * (1 + tp_pct):
                    triggered = 'tp_exit'
                elif position == -1 and low_t <= entry_price * (1 - tp_pct):
                    triggered = 'tp_exit'

            if triggered:
                exit_types[t] = triggered
                position = 0.0
                forced_flat = True

        effective_signals[t] = position

    # --- COMPUTE PnL (same formula as calculate()) ---
    df_out['signal'] = effective_signals
    # Add tracking columns BEFORE dropna so they stay aligned
    if track_risk_levels:
        df_out['SL'] = sl_levels
        df_out['TSL'] = tsl_levels
        df_out['TP'] = tp_levels
    df_out['trade'] = np.abs(np.diff(effective_signals, prepend=0))
    df_out['fee'] = df_out['trade'] * fee
    df_out['pnl'] = pd.Series(effective_signals).shift(1).values * (closes / np.roll(closes, 1) - 1) - df_out['fee'].values
    df_out.iloc[0, df_out.columns.get_loc('pnl')] = 0.0  # First row has no prior bar
    df_out['cumulative_pnl'] = df_out['pnl'].cumsum()
    df_out.dropna(inplace=True)
    df_out['bnh'] = (df_out['close'] / df_out['close'].shift(1) - 1).cumsum()
    df_out['dd'] = df_out['cumulative_pnl'] - df_out['cumulative_pnl'].cummax()
    df_out['exit_type'] = exit_types[:len(df_out)]

    # Reorder tracking columns to appear before 'signal'
    if track_risk_levels:
        cols = list(df_out.columns)
        for c in ['SL', 'TSL', 'TP']:
            cols.remove(c)
        signal_idx = cols.index('signal')
        for i, c in enumerate(['SL', 'TSL', 'TP']):
            cols.insert(signal_idx + i, c)
        df_out = df_out[cols]

    # --- COMPUTE METRICS (same as calculate()) ---
    sharpe_ratio = df_out['pnl'].mean() / df_out['pnl'].std() * np.sqrt(period) if df_out['pnl'].std() != 0 else 0
    max_drawdown = df_out['dd'].min() if not df_out['dd'].empty else 0
    trade_count = df_out['trade'].sum()
    annualized_return = df_out['pnl'].mean() * period
    calmar_ratio = annualized_return / abs(max_drawdown) if max_drawdown != 0 else 0

    log = {
        'Sharpe Ratio': sharpe_ratio,
        'Max Drawdown': max_drawdown,
        'Trade Count': trade_count,
        'Annualized Return': annualized_return,
        'Calmar Ratio': calmar_ratio,
    }

    return df_out, log


def filter_by_min_trade_count(results_df, min_trades):
    """
    Remove grid search results with Trade Count below the minimum threshold.

    Parameters:
    -----------
    results_df : pd.DataFrame
        Grid search results with 'Trade Count' column
    min_trades : int
        Minimum required trade count

    Returns:
    --------
    pd.DataFrame : Filtered results (rows with Trade Count >= min_trades)
    """
    return results_df[results_df['Trade Count'] >= min_trades].copy()


def select_best_combination(results_df, tiebreak_cols):
    """
    Select the best combination by max Sharpe Ratio with conservative tie-breaking.

    Parameters:
    -----------
    results_df : pd.DataFrame
        Filtered grid search results with 'Sharpe Ratio' column
    tiebreak_cols : list of str
        Column names for tie-breaking (sorted ascending = more conservative)
        E.g., ['SL', 'TP'] means prefer lowest SL, then lowest TP

    Returns:
    --------
    pd.Series or None : Best combination row, or None if results_df is empty
    """
    if results_df.empty:
        return None

    sort_cols = ['Sharpe Ratio'] + tiebreak_cols
    sort_asc = [False] + [True] * len(tiebreak_cols)
    sorted_df = results_df.sort_values(by=sort_cols, ascending=sort_asc)
    return sorted_df.iloc[0]


# ============================================================================
# VISUALIZATION
# ============================================================================

# Plot cumulative PnL vs Buy-and-Hold comparison
def plot_cumu_pnl_vs_bnh(df, save_path='cumu_pnl_vs_bnh.png'):
    """
    Plots the cumulative PnL and Buy and Hold (bnh) on the same figure.

    Parameters:
    df (pd.DataFrame): DataFrame that contains 'cumu_pnl' and 'bnh' columns.
    save_path (str): Path to save the plot as a PNG file. Defaults to 'cumu_pnl_vs_bnh.png'.
    """
    # Create figure with specified size
    fig = plt.figure(figsize=(16, 8))

    # Plot cumulative PnL line in blue
    df['cumulative_pnl'].plot(label='Cumulative PnL', color='blue')

    # Plot Buy and Hold (bnh) line in orange
    df['bnh'].plot(label='Buy and Hold', color='orange')

    # Set chart title and axis labels
    plt.title('Cumulative PnL vs. Buy and Hold')
    plt.xlabel('Time')
    plt.ylabel('Value')

    # Add a legend to distinguish the two lines
    plt.legend()

    # Save the plot as PNG image to specified path
    plt.savefig(save_path)

    # Close the plot to free memory
    plt.close(fig)
    plt.clf()  # Clear the current figure
