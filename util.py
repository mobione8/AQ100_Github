import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import requests
import pandas as pd
from functools import reduce

# Generate a range of values for parameter grid search
def get_range(start, end, step):
    # Create numpy array from start to end with step size, convert to list
    return np.arange(start, end, step).tolist()

# Configuration dictionary defining parameter ranges for different normalization models
config = {
"models": {
        "zscore": {
            "param1": [10, 200, 10],      # length: start=10, end=200, step=10
            "param2": [-3, 3, 0.25]        # entry_threshold: start=-3, end=3, step=0.25
        },
        "min_max": {
            "param1": [10, 70, 5],       # length range
            "param2": [-1, 1, 0.05]        # threshold range for min-max scaled values
        },
        "sma_diff": {
            "param1": [5, 255, 5],       # SMA length range
            "param2": [-1.0, 2.0, 0.1]    # percentage difference threshold range
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
            "param2": [20, 80, 5]        # RSI level threshold (unused, RSI is 0-100)
        },

    }
}


def get_period_from_interval(interval_or_filename):
    """
    Get annualization period from interval string or filename.

    Args:
        interval_or_filename: Either interval ('1h', '1d') or filename containing interval

    Returns:
        int: Number of periods per year (based on 252 trading days)
    """
    text = str(interval_or_filename).lower()

    if "1m" in text:
        return 252 * 8 * 60  # 8064 (15-min bars per trading year)
    if "5m" in text:
        return 252 * 8 * 12  # 8064 (15-min bars per trading year)
    if "15m" in text:
        return 252 * 8 * 4  # 8064 (15-min bars per trading year)
    elif "30m" in text:
        return 252 * 8 * 2  # 4032
    elif "1h" in text:
        return 252 * 8   # 2016
    elif "4h" in text:
        return 252 * 2   # 504
    elif "1d" in text:
        return 252       # Daily bars
    elif "1w" in text:
        return 52       # Weekly bars
    else:
        return 252       # Default to daily


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

# Calculate Relative Strength Index (RSI)
def rsi(df, length, column):
    # Calculate price change from previous period
    delta = df[column].diff()
    # Extract gains (positive changes, 0 otherwise)
    gain = np.where(delta > 0, delta, 0)
    # Extract losses (absolute negative changes, 0 otherwise)
    loss = np.where(delta < 0, -delta, 0)

    # Calculate average gain over rolling window
    avg_gain = pd.Series(gain).rolling(window=length, min_periods=length).mean()
    # Calculate average loss over rolling window
    avg_loss = pd.Series(loss).rolling(window=length, min_periods=length).mean()

    # Calculate relative strength (RS)
    rs = avg_gain / avg_loss
    # Calculate RSI using formula: 100 - (100 / (1 + RS))
    df['rsi'] = 100 - (100 / (1 + rs))
    return df


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
    elif buy_type == "trend":
        df['signal'] = np.where(df[column].shift(1) >= entry_threshold, 1, np.where(df[column].shift(1) <= exit_threshold, -1, np.nan))
        df['signal'] = df['signal'].ffill().fillna(0)

    # Reverse bidirectional trend: Short when >= entry, long when <= exit
    elif buy_type == "trend_reverse":
        df['signal'] = np.where(df[column].shift(1) >= entry_threshold, -1, np.where(df[column].shift(1) <= exit_threshold, 1, np.nan))
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

# Calculate backtest performance metrics
def calculate(df, fee, period=252):

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
def generate_all_signals(df, model, buy_type, column, length, entry_threshold, exit_threshold=0, period=252):

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



# Plot cumulative PnL vs Buy-and-Hold comparison
def plot_cumu_pnl_vs_bnh(df, save_path='cumu_pnl_vs_bnh.png'):
    """
    Plots the cumulative PnL and Buy and Hold (bnh) on the same figure.

    Parameters:
    df (pd.DataFrame): DataFrame that contains 'cumu_pnl' and 'bnh' columns.
    save_path (str): Path to save the plot as a PNG file. Defaults to 'cumu_pnl_vs_bnh.png'.
    """
    # Create figure with specified size
    plt.figure(figsize=(16, 8))

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
    plt.close()
