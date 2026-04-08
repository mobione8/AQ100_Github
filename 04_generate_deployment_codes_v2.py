"""
Generate IBKR Deployment Codes for AQ100  - IBKR-Native for US Equities
"""

import os
import sys
import glob
import re
import json
import argparse
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# --- Path anchors (script lives at project root) ---
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
IBKR_DIR = os.path.join(ROOT_DIR, 'ibkr_deployment')

# Client ID manager lives in ibkr_deployment/
sys.path.insert(0, IBKR_DIR)
try:
    from client_id_manager import get_or_allocate_client_id as _alloc_client_id
    _CLIENT_ID_MGR_AVAILABLE = True
except ImportError:
    _CLIENT_ID_MGR_AVAILABLE = False

# ===== TRANSFORMATION FUNCTIONS (to be inlined) =====
TRANSFORM_FUNCTIONS = {
    'zscore': '''def transform(series, length):
    """Z-score normalization"""
    sma = series.rolling(window=length).mean()
    std = series.rolling(window=length).std().replace(0, float('nan'))
    return (series - sma) / std''',

    'min_max': '''def transform(series, length):
    """Min-max scaling to [-1, 1]"""
    min_val = series.rolling(length).min()
    max_val = series.rolling(length).max()
    rng = (max_val - min_val).replace(0, float('nan'))
    return 2 * ((series - min_val) / rng) - 1''',

    'sma_diff': '''def transform(series, length):
    """Percentage difference from SMA"""
    sma = series.rolling(length).mean().replace(0, float('nan'))
    return (series / sma - 1)''',

    'robust_scaler': '''def transform(series, length):
    """Robust scaling using median and IQR"""
    median = series.rolling(window=length).median()
    q1 = series.rolling(window=length).quantile(0.25)
    q3 = series.rolling(window=length).quantile(0.75)
    iqr = (q3 - q1).replace(0, float('nan'))
    return (series - median) / iqr''',

    'maxabs_norm': '''def transform(series, length):
    """Max absolute value scaling"""
    max_abs = series.rolling(window=length).apply(lambda x: np.abs(x).max(), raw=True).replace(0, float('nan'))
    return series / max_abs''',

    'rsi': '''def transform(series, length):
    """Relative Strength Index"""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=length, min_periods=length).mean()
    avg_loss = loss.rolling(window=length, min_periods=length).mean().replace(0, float('nan'))
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))'''
}


# ===== SIGNAL FUNCTIONS (to be inlined) =====
SIGNAL_FUNCTIONS = {
    'trend_long': '''def generate_signal(transformed, entry, exit_th):
    """Long when >= entry, exit when <= exit"""
    signal = np.where(transformed.shift(1) >= entry, 1,
                     np.where(transformed.shift(1) <= exit_th, 0, np.nan))
    return pd.Series(signal).ffill().fillna(0).astype(int)''',

    'trend_short': '''def generate_signal(transformed, entry, exit_th):
    """Short when >= entry, exit when <= exit"""
    signal = np.where(transformed.shift(1) >= entry, -1,
                     np.where(transformed.shift(1) <= exit_th, 0, np.nan))
    return pd.Series(signal).ffill().fillna(0).astype(int)''',

    'trend_reverse_long': '''def generate_signal(transformed, entry, exit_th):
    """Long when <= entry, exit when >= exit"""
    signal = np.where(transformed.shift(1) <= entry, 1,
                     np.where(transformed.shift(1) >= exit_th, 0, np.nan))
    return pd.Series(signal).ffill().fillna(0).astype(int)''',

    'trend_reverse_short': '''def generate_signal(transformed, entry, exit_th):
    """Short when <= entry, exit when >= exit_th"""
    signal = np.where(transformed.shift(1) <= entry, -1,
                     np.where(transformed.shift(1) >= exit_th, 0, np.nan))
    return pd.Series(signal).ffill().fillna(0).astype(int)''',

    # Legacy typo support
    'trend_revese_long': '''def generate_signal(transformed, entry, exit_th):
    """Long when <= entry, exit when >= exit"""
    signal = np.where(transformed.shift(1) <= entry, 1,
                     np.where(transformed.shift(1) >= exit_th, 0, np.nan))
    return pd.Series(signal).ffill().fillna(0).astype(int)''',

    'trend_revese_short': '''def generate_signal(transformed, entry, exit_th):
    """Short when <= entry, exit when >= exit_th"""
    signal = np.where(transformed.shift(1) <= entry, -1,
                     np.where(transformed.shift(1) >= exit_th, 0, np.nan))
    return pd.Series(signal).ffill().fillna(0).astype(int)''',

    'trend': '''def generate_signal(transformed, entry, exit_th):
    """Bidirectional: Long >= entry, Short <= -entry"""
    signal = np.where(transformed.shift(1) >= entry, 1,
                     np.where(transformed.shift(1) <= -entry, -1, np.nan))
    return pd.Series(signal).ffill().fillna(0).astype(int)''',

    'trend_reverse': '''def generate_signal(transformed, entry, exit_th):
    """Reverse bidirectional: Short >= entry, Long <= -entry"""
    signal = np.where(transformed.shift(1) >= entry, -1,
                     np.where(transformed.shift(1) <= -entry, 1, np.nan))
    return pd.Series(signal).ffill().fillna(0).astype(int)''',

    'mr': '''def generate_signal(transformed, entry, exit_th):
    """Mean reversion: Long when > entry, Short when < -entry"""
    cond1 = np.where(transformed.shift(1) > entry, 1,
                    np.where(transformed.shift(1) < 0, 0, np.nan))
    cond2 = np.where(transformed.shift(1) < -entry, -1,
                    np.where(transformed.shift(1) > 0, 0, np.nan))
    return (pd.Series(cond1).ffill() + pd.Series(cond2).ffill()).fillna(0).astype(int)''',

    'mr_reverse': '''def generate_signal(transformed, entry, exit_th):
    """Reverse MR: Short when > entry, Long when < -entry"""
    cond1 = np.where(transformed.shift(1) > entry, -1,
                    np.where(transformed.shift(1) < 0, 0, np.nan))
    cond2 = np.where(transformed.shift(1) < -entry, 1,
                    np.where(transformed.shift(1) > 0, 0, np.nan))
    return (pd.Series(cond1).ffill() + pd.Series(cond2).ffill()).fillna(0).astype(int)'''
}




# ===== CODE INJECTION SANITIZER =====
def _sanitize_injected_block(code: str) -> str:
    """
    Make injected code safe to embed into templates.

    Primary fix: convert triple double-quotes to triple single-quotes.
    This prevents accidental termination of outer module docstrings if a
    template includes injected code inside a triple-double-quoted block.

    We intentionally *do not* attempt full parsing — we apply a conservative,
    idempotent text transform.
    """
    if code is None:
        return ""
    # Normalize line endings
    code = code.replace("\r\n", "\n").replace("\r", "\n")
    # Critical: avoid nested triple double-quotes inside outer docstrings
    code = code.replace('"""', "'''")
    return code.strip() + "\n"


# ===== DATE SUFFIX REPLACEMENT (V8) =====
_OLD_DATE_SUFFIX = re.compile(
    r"_(\d{2}(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\d{4})$",
    re.IGNORECASE,
)


def replace_date_suffix(strategy_name: str, new_suffix: str) -> str:
    """Replace trailing ddMMMYYYY date token with a new suffix (e.g. YYYYMMDDhhmm)."""
    result, count = _OLD_DATE_SUFFIX.subn(f"_{new_suffix}", strategy_name)
    if count == 0:
        print(f"  WARNING: No date suffix found in '{strategy_name}', keeping original name")
    return result


# ===== INTERACTIVE MODE HELPER FUNCTIONS =====

def auto_detect_excel_file(interval: str, exchange: str) -> Optional[str]:
    """
    Auto-detect the latest Final_Compilation Excel file.

    Args:
        interval: Time interval (1h, 1d, 4h)
        exchange: Exchange name (default: ibkr)

    Returns:
        Path to latest Excel file or None if not found
    """
    wfalpha_dir = os.path.join(ROOT_DIR, "WFAlphaResults")
    pattern = os.path.join(wfalpha_dir, f"Final_Compilation_{exchange}_{interval}_*.xlsx")
    excel_files = sorted(glob.glob(pattern), reverse=True)
    return excel_files[0] if excel_files else None


def auto_detect_risk_optimized_file(interval: str, exchange: str) -> Optional[str]:
    """
    Auto-detect the latest Risk_Optimized_Compilation Excel file.

    Args:
        interval: Time interval (1h, 1d, 4h)
        exchange: Exchange name (default: ibkr)

    Returns:
        Path to latest Risk_Optimized_Compilation Excel file or None if not found
    """
    wfalpha_dir = os.path.join(ROOT_DIR, "WFAlphaResults")
    pattern = os.path.join(wfalpha_dir, f"Risk_Optimized_Compilation_{exchange}_{interval}_*.xlsx")
    excel_files = sorted(glob.glob(pattern), reverse=True)
    return excel_files[0] if excel_files else None


def get_available_symbols(excel_path: str, sheet_name: str = 'Corr Summary') -> List[str]:
    """
    Extract available symbols from compilation Excel file.

    Args:
        excel_path: Path to compilation Excel file
        sheet_name: Sheet name to read symbols from

    Returns:
        Sorted list of unique symbols
    """
    try:
        df_corr = pd.read_excel(excel_path, sheet_name=sheet_name)
        symbols = sorted(df_corr['Symbol'].unique().tolist())
        return symbols
    except Exception as e:
        print(f"  WARNING: Could not read symbols from Excel: {e}")
        return []


def validate_interval(interval_input: str) -> str:
    """
    Validate interval input.

    Args:
        interval_input: User input for interval

    Returns:
        Valid interval string

    Raises:
        ValueError: If interval is invalid
    """
    interval = interval_input.strip().lower()
    if not interval:
        return '1d'  # Default
    if interval not in ['1h', '4h', '1d']:
        raise ValueError(f"Invalid interval: {interval}. Must be 1h, 4h, or 1d")
    return interval


def validate_symbol(symbol_input: str, available_symbols: List[str]) -> Optional[List[str]]:
    """
    Validate symbol input against available symbols.

    Args:
        symbol_input: User input for symbol (supports comma-separated multiple symbols)
        available_symbols: List of available symbols from Excel

    Returns:
        None for "all symbols" mode (empty input), or list of validated symbol strings

    Raises:
        ValueError: If user cancels after symbol not found warning
    """
    symbol_input_stripped = symbol_input.strip().upper()

    if not symbol_input_stripped:
        return None  # All symbols mode

    # Support comma-separated multiple symbols
    symbols = [s.strip() for s in symbol_input_stripped.split(',') if s.strip()]

    not_found = [s for s in symbols if s not in available_symbols]
    if not_found:
        print(f"  WARNING: Symbol(s) not found in Excel file: {', '.join(not_found)}")
        print(f"  Available symbols: {', '.join(sorted(available_symbols)[:10])}...")
        proceed = input("  Continue anyway? (y/n): ").strip().lower()
        if proceed != 'y':
            raise ValueError("Symbol not found in compilation file")

    return symbols


def validate_generation_type(type_input: str) -> Tuple[bool, bool]:
    """
    Validate generation type choice.

    Args:
        type_input: User input for generation type (1, 2, 3 or empty)

    Returns:
        Tuple of (generate_single, generate_portfolio)
        - 1 or empty: (True, False)  - single strategies only (default)
        - 2:          (False, True)  - portfolio only
        - 3:          (True, True)   - both

    Raises:
        ValueError: If input is invalid
    """
    choice = type_input.strip() or '1'  # Default to single strategies

    if choice == '1':
        return True, False
    elif choice == '2':
        return False, True
    elif choice == '3':
        return True, True
    else:
        raise ValueError("Invalid choice. Enter 1, 2, or 3")


def safe_input(prompt: str, validator, max_attempts: int = 3):
    """
    Get user input with validation and retry logic.

    Args:
        prompt: Input prompt to display
        validator: Function that validates and transforms input
        max_attempts: Maximum retry attempts

    Returns:
        Validated input value

    Raises:
        ValueError: If max attempts exceeded
    """
    for attempt in range(max_attempts):
        try:
            user_input = input(prompt).strip()
            return validator(user_input)
        except ValueError as e:
            print(f"  ERROR: {e}")
            if attempt < max_attempts - 1:
                print(f"  Please try again ({max_attempts - attempt - 1} attempts remaining)")
            else:
                raise ValueError(f"Max attempts ({max_attempts}) exceeded")
        except Exception as e:
            print(f"  UNEXPECTED ERROR: {e}")
            raise


def display_summary(config: Dict) -> None:
    """
    Display configuration summary before execution.

    Args:
        config: Dictionary containing all configuration parameters
    """
    print("\n" + "=" * 80)
    print("Configuration Summary:")
    print("=" * 80)
    print(f"  Interval:        {config['interval']}")
    print(f"  Trading Mode:    {config['trading_mode'].capitalize()} Trading (Port: {config['trading_port']})")
    if 'dry_run' in config:
        print(f"  Dry Run:         {'Enabled' if config['dry_run'] else 'Disabled'}")
    if 'source' in config:
        print(f"  Source:          {config['source']}")
    print(f"  Excel File:      {config['excel_file']}")
    print(f"  Symbol(s):       {config['symbols']}")
    print(f"  Generate:")
    if config.get('generate_single'):
        print(f"    - Single strategies: {config['output_single']}")
    if config.get('generate_portfolio'):
        print(f"    - Portfolio files:   {config['output_portfolio']}")
    if 'mdd_mode' in config:
        if config['mdd_mode'] == 'fixed':
            print(f"  Kill Switch:     Fixed ({config['mdd_single_pct']}% single / {config['mdd_portfolio_pct']}% portfolio)")
        else:
            print(f"  Kill Switch:     Dynamic ({config['mdd_multiplier']}x Max DD, capped at 100%)")
    print()
    print("  Note: Existing strategy deployment files can be overwritten.")
    print("  Please backup before generating these strategy deployment files.")
    print("=" * 80)


def detect_benchmarks_from_backtest(backtest_path: str, symbol: str = '') -> List[str]:
    """
    Detect which benchmarks were used in the backtest by reading CSV columns.

    Excludes the primary symbol from the benchmark list (V4b fix).
    Previously, a symbol like GLD would appear as its own benchmark because
    backtest.csv contains a GLD_close column, causing close_GLD_spread = 0 always.

    Args:
        backtest_path: Path to backtest.csv
        symbol: Primary asset symbol to exclude from benchmarks (e.g. 'GLD')

    Returns:
        List of benchmark symbols (e.g., ['SPY']) excluding the primary symbol
    """
    try:
        df = pd.read_csv(backtest_path, nrows=1)
        benchmarks = []
        for col in df.columns:
            # Pattern: "{BENCHMARK}_close" where BENCHMARK is uppercase
            if col.endswith('_close') and col != 'close':
                bench = col.replace('_close', '')
                # Exclude VIX (handled separately), lowercase columns,
                # and the primary symbol itself
                if bench != 'VIX' and bench != symbol and bench.isupper():
                    benchmarks.append(bench)
        return benchmarks
    except Exception as e:
        print(f"  WARNING: Could not detect benchmarks: {e}")
        return []


def parse_strategy_name(strategy_name: str) -> Tuple[str, str, str, str]:
    """
    Parse strategy name into components.

    Example inputs:
      "GLD_volatility_20_zscore_trend_reverse_long_christine_04Feb2026"
      "GLD_average_min_max_trend_long_christine_04Feb2026"

    Returns:
      (symbol, feature, model, buy_type)
    """
    KNOWN_MODELS = ['zscore', 'min_max', 'sma_diff', 'robust_scaler', 'maxabs_norm', 'rsi']
    KNOWN_BUY_TYPES = [
        'trend_long', 'trend_short',
        'trend_reverse_long', 'trend_reverse_short',
        'trend_revese_long', 'trend_revese_short',  # legacy typo support
        'trend', 'trend_reverse',
        'mr', 'mr_reverse'
    ]

    parts = strategy_name.split('_')
    if len(parts) < 4:
        raise ValueError(f"Invalid strategy name format: {strategy_name}")

    symbol = parts[0]

    if '_' in symbol:
        import warnings
        warnings.warn(f"Symbol '{symbol}' may be misparsed — underscores in symbols are not supported")

    # Find model (longest match first to handle min_max, sma_diff, robust_scaler, maxabs_norm)
    # Must check two-word combos before single-word to avoid partial matches.
    model = None
    model_index = -1
    for i in range(1, len(parts)):
        # Try two-word match first (e.g. 'robust_scaler', 'min_max', 'sma_diff')
        if i + 1 < len(parts):
            two_word = f"{parts[i]}_{parts[i+1]}"
            if two_word in KNOWN_MODELS:
                model = two_word
                model_index = i + 1  # Points to second word; feature ends at i
                break
        # Fall back to single-word match (zscore, rsi)
        if parts[i] in KNOWN_MODELS:
            model = parts[i]
            model_index = i
            break

    if model is None:
        raise ValueError(f"No known model found in: {strategy_name}")

    # Feature is everything between symbol and the START of the model token.
    # For two-word models (e.g. robust_scaler), model_index points to the second
    # word, so the model starts at (model_index - 1).
    model_start = model_index - 1 if '_' in model else model_index
    feature = '_'.join(parts[1:model_start])

    # Everything after model could be buy_type + optional date/name suffix
    remaining = parts[model_index + 1:]
    remaining_joined = '_'.join(remaining)

    # Pick the LONGEST known buy_type that matches the prefix of remaining_joined
    buy_type = None
    for bt in sorted(KNOWN_BUY_TYPES, key=len, reverse=True):
        if remaining_joined.startswith(bt):
            buy_type = bt
            break

    if buy_type is None:
        raise ValueError(f"No known buy_type found in: {strategy_name} (after model '{model}')")

    return symbol, feature, model, buy_type


def compute_max_drawdown_pct(historical_mdd: float, mdd_mode: str,
                              fixed_pct: float = 0.02, mdd_multiplier: float = 1.0) -> float:
    """
    Compute kill switch threshold based on mode.

    Args:
        historical_mdd: Raw historical MDD (positive ratio, 0 if unavailable)
        mdd_mode: 'fixed' or 'dynamic'
        fixed_pct: User-specified threshold for fixed mode (ratio, e.g. 0.02 = 2%)
        mdd_multiplier: Multiplier for dynamic mode (default 1.0)

    Returns:
        max_drawdown_pct as positive ratio
    """
    if mdd_mode == 'fixed':
        return fixed_pct
    else:  # dynamic
        if historical_mdd > 0:
            return min(historical_mdd * mdd_multiplier, 1.0)
        else:
            return fixed_pct  # Fallback if no MDD data


def load_strategy_config(symbol: str, feature: str, model: str, buy_type: str,
                         base_path: str = "WFAlphaResults", interval: str = "1h") -> Dict:
    """
    Load strategy configuration from summary.csv with benchmark detection.

    Args:
        symbol: IBKR symbol (e.g., "PLTR")
        feature: Feature column name
        model: Transformation model
        buy_type: Entry/exit logic
        base_path: Base path to WFAlphaResults folder
        interval: Time interval (1h, 1d, etc.)

    Returns:
        Dict with strategy configuration including benchmarks
    """
    # Use glob to find matching folder (handles date suffix)
    pattern = os.path.join(base_path, f"merged_ibkr_{symbol}_{interval}_*", feature, model, buy_type)
    matches = glob.glob(pattern)

    if not matches:
        raise FileNotFoundError(f"No strategy folder found for pattern: {pattern}")

    strategy_dir = matches[0]  # Use first match
    summary_path = os.path.join(strategy_dir, "summary.csv")
    backtest_path = os.path.join(strategy_dir, "backtest.csv")
    metrics_path = os.path.join(strategy_dir, "metrics.csv")

    if not os.path.exists(summary_path):
        raise FileNotFoundError(f"Summary file not found: {summary_path}")

    # Load summary.csv
    df_summary = pd.read_csv(summary_path)

    required_cols = ['Symbol', 'Exchange', 'Interval', 'Data Point', 'Model',
                     'Entry / Exit Model', 'Length', 'Entry', 'Exit']
    missing_cols = [c for c in required_cols if c not in df_summary.columns]
    if missing_cols:
        raise ValueError(f"summary.csv missing required columns: {missing_cols}")

    if len(df_summary) == 0:
        raise ValueError(f"Empty summary file: {summary_path}")

    row = df_summary.iloc[0]

    # Detect benchmarks from backtest.csv — pass symbol to exclude it
    benchmarks = (
        detect_benchmarks_from_backtest(backtest_path, symbol=symbol)
        if os.path.exists(backtest_path) else []
    )

    # Build configuration dict
    config = {
        'symbol': str(row['Symbol']),
        'exchange': str(row['Exchange']),
        'interval': str(row['Interval']),
        'feature': str(row['Data Point']),
        'model': str(row['Model']),
        'buy_type': str(row['Entry / Exit Model']),
        'length': int(row['Length']),
        'entry_threshold': float(row['Entry']),
        'exit_threshold': float(row['Exit']),
        'benchmarks': benchmarks,

        # Performance metrics (for reference)
        'is_sharpe': float(row['IS Sharpe']) if 'IS Sharpe' in row.index and pd.notna(row['IS Sharpe']) else 0.0,
        'is_trade_count': int(float(row['IS Trade Count'])) if 'IS Trade Count' in row.index and pd.notna(row['IS Trade Count']) else 0,

        # Risk optimization parameters (defaults, overridden by risk_params if available)
        'sl_pct': 0.0,
        'tp_pct': 0.0,
        'tsl_pct': 0.0,
    }

    # Read MDD from metrics.csv (file has header row: Metric,Value)
    mdd = 0.0
    if os.path.exists(metrics_path):
        try:
            df_metrics = pd.read_csv(metrics_path)
            mdd_row = df_metrics[df_metrics['Metric'] == 'Max Drawdown']
            if len(mdd_row) > 0:
                mdd = abs(float(mdd_row.iloc[0]['Value']))
        except Exception:
            pass
    config['max_drawdown'] = mdd

    return config


def parse_final_compilation(excel_path: str, sheet_name: str = 'Corr Summary') -> Tuple[Dict[str, List[str]], Dict[str, float]]:
    """
    Parse compilation Excel to extract strategy lists per symbol and portfolio MDD.

    Args:
        excel_path: Path to compilation Excel file
        sheet_name: Sheet name to read from ('Corr Summary' or 'Portfolio')

    Returns:
        Tuple of (strategies_by_symbol, portfolio_mdd_by_symbol)
    """
    print(f"Parsing compilation: {excel_path} (sheet: {sheet_name})")

    df_corr = pd.read_excel(excel_path, sheet_name=sheet_name)

    strategies_by_symbol = {}
    portfolio_mdd_by_symbol = {}

    # Determine MDD column based on sheet
    mdd_col = None
    if sheet_name == 'Corr Summary' and 'Portfolio MDD' in df_corr.columns:
        mdd_col = 'Portfolio MDD'
    elif sheet_name == 'Portfolio' and 'Risk Opt Portfolio MDD' in df_corr.columns:
        mdd_col = 'Risk Opt Portfolio MDD'

    for idx, row in df_corr.iterrows():
        symbol = row['Symbol']
        # Use full symbol for new pipeline (PLTR vs MBT)
        symbol_short = symbol

        strategy_list_str = row['Strategy List']
        if pd.isna(strategy_list_str) or not str(strategy_list_str).strip():
            print(f"  Skipping {symbol}: empty Strategy List")
            continue
        strategies = [s.strip() for s in str(strategy_list_str).split(',')]

        strategies_by_symbol[symbol_short] = strategies
        print(f"  {symbol_short}: {len(strategies)} strategies")

        # Extract portfolio MDD if column exists
        if mdd_col and mdd_col in row.index:
            try:
                portfolio_mdd_by_symbol[symbol_short] = abs(float(row[mdd_col]))
            except (ValueError, TypeError):
                pass

    return strategies_by_symbol, portfolio_mdd_by_symbol


def load_risk_params_from_excel(excel_path: str) -> Dict[str, Dict]:
    """
    Load risk optimization parameters from Risk_Optimized_Strategies sheet.

    Args:
        excel_path: Path to Risk_Optimized_Compilation Excel file

    Returns:
        Dict mapping {strategy_name: {sl_pct, tp_pct, tsl_pct, selected_approach}}
    """
    df = pd.read_excel(excel_path, sheet_name='Risk_Optimized_Strategies')
    risk_params = {}
    for _, row in df.iterrows():
        strategy_id = str(row['Strategy_ID'])
        risk_params[strategy_id] = {
            'sl_pct': float(row.get('Optimal_SL', 0)),
            'tp_pct': float(row.get('Optimal_TP', 0)),
            'tsl_pct': float(row.get('Optimal_TSL', 0)),
            'selected_approach': str(row.get('Selected_Approach', 'Baseline')),
            'optimized_mdd': abs(float(row['Optimized_MDD'])) if pd.notna(row.get('Optimized_MDD')) else 0.0,
            'optimized_sharpe': float(row.get('Optimized_Sharpe', 0.0)) if pd.notna(row.get('Optimized_Sharpe')) else 0.0,
            'optimized_annual_return': float(row.get('Optimized_Annual_Return', 0.0)) if pd.notna(row.get('Optimized_Annual_Return')) else 0.0,
            'optimized_calmar': float(row.get('Optimized_Calmar', 0.0)) if pd.notna(row.get('Optimized_Calmar')) else 0.0,
        }
    return risk_params


def load_all_strategy_configs(strategies_by_symbol: Dict[str, List[str]],
                               base_path: str = "WFAlphaResults",
                               interval: str = "1h",
                               risk_params: Optional[Dict[str, Dict]] = None) -> Dict[str, List[Dict]]:
    """
    Load configurations for all strategies.

    Args:
        strategies_by_symbol: Dict from parse_final_compilation
        base_path: Base path to WFAlphaResults folder
        interval: Time interval
        risk_params: Optional dict of {strategy_name: {sl_pct, tp_pct, tsl_pct}} from load_risk_params_from_excel

    Returns:
        Dict mapping {symbol: [list of config dicts]}
    """
    configs_by_symbol = {}

    for symbol, strategies in strategies_by_symbol.items():
        print(f"\nLoading configurations for {symbol}...")
        configs = []

        for strategy_name in strategies:
            try:
                sym, feature, model, buy_type = parse_strategy_name(strategy_name)
                config = load_strategy_config(sym, feature, model, buy_type, base_path, interval)
                config['strategy_name'] = strategy_name

                # Merge risk params if available
                if risk_params and strategy_name in risk_params:
                    rp = risk_params[strategy_name]
                    config['sl_pct'] = rp['sl_pct']
                    config['tp_pct'] = rp['tp_pct']
                    config['tsl_pct'] = rp['tsl_pct']
                    if rp.get('optimized_mdd', 0) > 0:
                        config['max_drawdown'] = rp['optimized_mdd']
                    config['optimized_sharpe'] = rp.get('optimized_sharpe', 0.0)
                    config['optimized_annual_return'] = rp.get('optimized_annual_return', 0.0)
                    config['optimized_calmar'] = rp.get('optimized_calmar', 0.0)
                    config['selected_approach'] = rp.get('selected_approach', 'Baseline')

                configs.append(config)

                # Print risk param info if available
                if risk_params and strategy_name in risk_params:
                    rp = risk_params[strategy_name]
                    print(f"  SUCCESS: {strategy_name} (benchmarks: {config['benchmarks']})")
                    print(f"    Risk params: SL={rp['sl_pct']*100:.1f}%, TP={rp['tp_pct']*100:.1f}%, "
                          f"TSL={rp['tsl_pct']*100:.1f}% ({rp['selected_approach']})")
                else:
                    print(f"  SUCCESS: {strategy_name} (benchmarks: {config['benchmarks']})")

            except Exception as e:
                print(f"  ERROR: {strategy_name}: {e}")
                continue

        configs_by_symbol[symbol] = configs
        print(f"  Total: {len(configs)}/{len(strategies)} strategies loaded")

    return configs_by_symbol


def get_interval_config(interval: str) -> Dict:
    """
    Get IBKR-specific configuration for interval.

    Args:
        interval: Time interval (1h, 1d, etc.)

    Returns:
        Dict with IBKR bar settings
    """
    configs = {
        '1h': {
            'barSizeSetting': '1 hour',
            'durationStr': '2 D',
            'sleep_seconds': 30,
            'check_minute': 1
        },
        '4h': {
            'barSizeSetting': '4 hours',
            'durationStr': '5 D',   # Covers Mon open after weekend gap
            'sleep_seconds': 60,
            'check_minute': 5
        },
        '1d': {
            'barSizeSetting': '1 day',
            'durationStr': '5 D',   # '2 D' risks 0 trading days over a weekend
            'sleep_seconds': 60,
            'check_minute': 0  # Check at market close
        },
    }
    return configs.get(interval, configs['1h'])


def generate_single_strategy_code_v3(config: Dict, output_dir: str, client_id_base: int,
                                      trading_port: Optional[int] = None,
                                      mdd_mode: str = 'fixed', fixed_single_pct: float = 0.02,
                                      mdd_multiplier: float = 1.0,
                                      dry_run: bool = True) -> str:
    """
    Generate IBKR-native single strategy file.
    No external API dependencies.

    Args:
        config: Strategy configuration dictionary
        output_dir: Output directory for generated file
        client_id_base: IBKR client ID
        trading_port: TWS port number (7497 for paper, 7496 for live, default: 7497)
        mdd_mode: Kill switch mode - 'fixed' or 'dynamic'
        fixed_single_pct: Fixed mode threshold (ratio, e.g. 0.02 = 2%)
        mdd_multiplier: Dynamic mode multiplier

    Returns:
        Path to generated file
    """
    template_path = os.path.join(IBKR_DIR, 'templates', 'single_strategy_refactor_template_v6.py')
    with open(template_path, 'r', encoding='utf-8') as f:
        template = f.read()

    model = config['model']
    buy_type = config['buy_type']
    feature = config['feature']
    interval = config.get('interval', '1h')
    benchmarks = config.get('benchmarks', [])

    transform_function = TRANSFORM_FUNCTIONS.get(model, TRANSFORM_FUNCTIONS['zscore'])
    transform_function = _sanitize_injected_block(transform_function)
    signal_function = SIGNAL_FUNCTIONS.get(buy_type, SIGNAL_FUNCTIONS['mr'])
    signal_function = _sanitize_injected_block(signal_function)
    interval_config = get_interval_config(interval)

    filename = f"{config['strategy_name']}.py"
    output_path = os.path.join(output_dir, filename)

    code = template.format(
        symbol=config['symbol'],
        feature=feature,
        model=model,
        buy_type=buy_type,
        length=config['length'],
        entry_threshold=config['entry_threshold'],
        exit_threshold=config['exit_threshold'],
        client_id=client_id_base,
        strategy_name=config['strategy_name'],
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        transform_function=transform_function,
        signal_function=signal_function,
        benchmarks=benchmarks,
        interval=interval,
        bar_size_setting=interval_config['barSizeSetting'],
        duration_str=interval_config['durationStr'],
        sleep_seconds=interval_config['sleep_seconds'],
        rolling_window=config.get('rolling_window', 20),
        has_vix='VIX' in str(feature).upper(),
        tws_port=7497,
        paper_trading_mode=True,
        dry_run=True,
        max_position_size=1,
        sl_pct=config.get('sl_pct', 0.0),
        tp_pct=config.get('tp_pct', 0.0),
        tsl_pct=config.get('tsl_pct', 0.0),
        max_drawdown_pct=compute_max_drawdown_pct(
            config.get('max_drawdown', 0), mdd_mode,
            fixed_pct=fixed_single_pct, mdd_multiplier=mdd_multiplier),
        backtest_selected_approach=config.get('selected_approach', 'Baseline'),
        backtest_optimized_sharpe=config.get('optimized_sharpe', 0.0),
        backtest_optimized_mdd=config.get('max_drawdown', 0.0),
        backtest_optimized_annual_return=config.get('optimized_annual_return', 0.0),
        backtest_optimized_calmar=config.get('optimized_calmar', 0.0),
    )

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(code)

    return output_path


def generate_portfolio_code_v3(symbol: str, configs: List[Dict], output_dir: str,
                                client_id_base: int, total_capital: float = 100000,
                                trading_port: Optional[int] = None,
                                portfolio_mdd: float = 0.0,
                                mdd_mode: str = 'fixed', fixed_portfolio_pct: float = 0.05,
                                fixed_single_pct: float = 0.02,
                                mdd_multiplier: float = 1.0,
                                dry_run: bool = True) -> str:
    """
    Generate IBKR-native portfolio file.
    Single IBKR connection, shared data fetching.

    Args:
        symbol: Primary trading symbol
        configs: List of strategy configuration dictionaries
        output_dir: Output directory for generated file
        client_id_base: IBKR client ID
        total_capital: Total capital for portfolio (default: 100000)
        trading_port: TWS port number (7497 for paper, 7496 for live, default: 7497)
        portfolio_mdd: Historical portfolio MDD (positive ratio)
        mdd_mode: Kill switch mode - 'fixed' or 'dynamic'
        fixed_portfolio_pct: Fixed mode threshold for portfolio (ratio, e.g. 0.05 = 5%)
        fixed_single_pct: Fixed mode threshold for single strategies (ratio, e.g. 0.02 = 2%)
        mdd_multiplier: Dynamic mode multiplier

    Returns:
        Path to generated file
    """
    num_strategies = len(configs)
    capital_per_strategy = total_capital / num_strategies
    interval = configs[0].get('interval', '1h') if configs else '1d'
    interval_config = get_interval_config(interval)

    # Collect all unique benchmarks across strategies
    all_benchmarks = set()
    for cfg in configs:
        all_benchmarks.update(cfg.get('benchmarks', []))
    all_benchmarks = sorted(list(all_benchmarks))

    filename = f"{symbol}_{interval}_portfolio.py"
    output_path = os.path.join(output_dir, filename)

    template_path = os.path.join(IBKR_DIR, 'templates', 'portfolio_refactor_template_v4.py')
    with open(template_path, 'r', encoding='utf-8') as f:
        template = f.read()

    # Build strategies list string
    strategies_list = "[\n"
    for cfg in configs:
        strategies_list += f'''    {{
        'name': {repr(cfg['strategy_name'])},
        'feature': {repr(cfg['feature'])},
        'model': {repr(cfg['model'])},
        'buy_type': {repr(cfg['buy_type'])},
        'length': {cfg['length']},
        'entry_threshold': {cfg['entry_threshold']},
        'exit_threshold': {cfg['exit_threshold']},
        'weight': {1.0/num_strategies:.4f},
        'sl_pct': {cfg.get('sl_pct', 0.0)},
        'tp_pct': {cfg.get('tp_pct', 0.0)},
        'tsl_pct': {cfg.get('tsl_pct', 0.0)},
        'max_drawdown_pct': {compute_max_drawdown_pct(cfg.get('max_drawdown', 0), mdd_mode, fixed_pct=fixed_single_pct, mdd_multiplier=mdd_multiplier)},
        'backtest_selected_approach': {repr(cfg.get('selected_approach', 'Baseline'))},
        'backtest_optimized_sharpe': {cfg.get('optimized_sharpe', 0.0)},
        'backtest_optimized_mdd': {cfg.get('max_drawdown', 0.0)},
        'backtest_optimized_annual_return': {cfg.get('optimized_annual_return', 0.0)},
        'backtest_optimized_calmar': {cfg.get('optimized_calmar', 0.0)},
    }},
'''
    strategies_list += "]"

    # Sanitize strategies_list (convert """ to ''' to prevent docstring collision)
    strategies_list = _sanitize_injected_block(strategies_list)

    # Get max rolling_window across all strategies (default 20)
    max_rolling_window = max((cfg.get('rolling_window', 20) for cfg in configs), default=20)

    # Check if any strategy uses VIX in its feature name
    has_vix = any('VIX' in str(cfg.get('feature', '')).upper() for cfg in configs)

    code = template.format(
        symbol=symbol,
        benchmarks=all_benchmarks,
        num_strategies=num_strategies,
        total_capital=total_capital,
        capital_per_strategy=capital_per_strategy,
        client_id=client_id_base,
        strategies_list=strategies_list,
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        interval=interval,
        bar_size_setting=interval_config['barSizeSetting'],
        sleep_seconds=interval_config['sleep_seconds'],
        rolling_window=max_rolling_window,
        has_vix=has_vix,
        tws_port=7497,
        paper_trading_mode=True,
        dry_run=True,
        max_position_size=1,
        max_drawdown_pct=compute_max_drawdown_pct(
            portfolio_mdd, mdd_mode,
            fixed_pct=fixed_portfolio_pct, mdd_multiplier=mdd_multiplier),
    )

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(code)

    return output_path


def run_generation(
    interval: str,
    exchange: str,
    excel_path: str,
    symbol_filter: Optional[List[str]],
    generate_single: bool,
    generate_portfolio: bool,
    output_single: Optional[str] = None,
    output_portfolio: Optional[str] = None,
    trading_port: Optional[int] = None,
    source: str = 'final',
    mdd_mode: str = 'fixed',
    mdd_single_pct: float = 2.0,
    mdd_portfolio_pct: float = 5.0,
    mdd_multiplier: float = 1.0,
    dry_run: bool = True,
) -> Tuple[int, int]:
    """
    Core generation logic shared by CLI and interactive modes.

    Args:
        interval: Time interval (1h, 1d, 4h)
        exchange: Exchange name (default: ibkr)
        excel_path: Path to compilation Excel file
        symbol_filter: Generate for specific symbol(s) only (None = all symbols)
        generate_single: Whether to generate single strategy files
        generate_portfolio: Whether to generate portfolio files
        output_single: Output directory for single strategy files
        output_portfolio: Output directory for portfolio files
        trading_port: TWS port number (7497 for paper, 7496 for live)
        source: Data source - 'final' or 'risk_optimized'
        mdd_mode: Kill switch mode - 'fixed' or 'dynamic'
        mdd_single_pct: Max DD % for single strategies (user %, e.g. 2.0 = 2%)
        mdd_portfolio_pct: Max DD % for portfolios (user %, e.g. 5.0 = 5%)
        mdd_multiplier: Multiplier for dynamic mode (default 1.0)

    Returns:
        Tuple of (total_single_generated, total_portfolio_generated)
    """
    wfalpha_dir = os.path.join(ROOT_DIR, "WFAlphaResults")

    # Validate Excel file exists
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"Excel file not found: {excel_path}")

    print(f"Excel file: {excel_path}")

    # Determine sheet_name and risk_params based on source
    if source == 'risk_optimized':
        sheet_name = 'Portfolio'
        risk_params = load_risk_params_from_excel(excel_path)
        print(f"Loaded risk parameters for {len(risk_params)} strategies")
    else:
        sheet_name = 'Corr Summary'
        risk_params = None

    # Parse and load configurations
    strategies_by_symbol, portfolio_mdd_by_symbol = parse_final_compilation(excel_path, sheet_name=sheet_name)
    configs_by_symbol = load_all_strategy_configs(
        strategies_by_symbol,
        base_path=wfalpha_dir,
        interval=interval,
        risk_params=risk_params
    )

    # Filter to specific symbol(s) if requested
    if symbol_filter:
        not_found = [s for s in symbol_filter if s not in configs_by_symbol]
        if not_found:
            raise ValueError(f"Symbol(s) {', '.join(not_found)} not found in Excel file")
        configs_by_symbol = {s: configs_by_symbol[s] for s in symbol_filter}
        print(f"\nGenerating for symbol(s): {', '.join(symbol_filter)}")

    # V8: Replace date suffix in all strategy names with generation timestamp
    gen_ts = datetime.now().strftime("%Y%m%d%H%M")
    print(f"\nV8: Replacing date suffixes with generation timestamp: {gen_ts}")
    for symbol, configs in configs_by_symbol.items():
        for config in configs:
            original_name = config['strategy_name']
            config['strategy_name'] = replace_date_suffix(original_name, gen_ts)
            if config['strategy_name'] != original_name:
                print(f"  {original_name} -> {config['strategy_name']}")

    # Inject interval into strategy names: GLD_... -> GLD_1h_...
    for symbol, configs in configs_by_symbol.items():
        for config in configs:
            old_name = config['strategy_name']
            sym = config['symbol']
            if old_name.startswith(sym + '_'):
                config['strategy_name'] = f"{sym}_{interval}_{old_name[len(sym)+1:]}"

    # Create output directories
    single_output_dir = output_single or os.path.join(ROOT_DIR, 'deploy_ibkr')
    portfolio_output_dir = output_portfolio or os.path.join(ROOT_DIR, 'deploy_ibkr_portfolio')

    if generate_single:
        os.makedirs(single_output_dir, exist_ok=True)
        os.makedirs(os.path.join(single_output_dir, 'logs'), exist_ok=True)
    if generate_portfolio:
        os.makedirs(portfolio_output_dir, exist_ok=True)
        os.makedirs(os.path.join(portfolio_output_dir, 'logs'), exist_ok=True)

    total_single = 0
    total_portfolio = 0

    # Generate single strategy codes
    if generate_single:
        print("\n" + "=" * 80)
        print("GENERATING SINGLE STRATEGY CODES (V8 - IBKR-Native)")
        print("=" * 80)

        client_id_base = 10000
        client_id_offset = 0

        for symbol, configs in configs_by_symbol.items():
            print(f"\n[{symbol}] Generating {len(configs)} strategies...")

            for i, config in enumerate(configs):
                # Allocate client ID via manager (persistent, collision-free)
                preferred_id = client_id_base + client_id_offset + i
                if _CLIENT_ID_MGR_AVAILABLE:
                    try:
                        script_name = config['strategy_name']
                        allocated_id = _alloc_client_id(
                            name=script_name, role="single_strategy", preferred=preferred_id
                        )
                    except Exception as e:
                        print(f"  WARNING: client_id_manager failed for {config['strategy_name']}: {e} — using {preferred_id}")
                        allocated_id = preferred_id
                else:
                    allocated_id = preferred_id

                try:
                    output_path = generate_single_strategy_code_v3(
                        config=config,
                        output_dir=single_output_dir,
                        client_id_base=allocated_id,
                        trading_port=trading_port,
                        mdd_mode=mdd_mode,
                        fixed_single_pct=mdd_single_pct / 100.0,
                        mdd_multiplier=mdd_multiplier,
                        dry_run=dry_run,
                    )
                    print(f"  SUCCESS: {os.path.basename(output_path)} (client_id={allocated_id})")
                    total_single += 1
                except Exception as e:
                    print(f"  ERROR: {config['strategy_name']}: {e}")

            client_id_offset += len(configs)

        print(f"\nTotal single strategies generated: {total_single}")

    # Generate portfolio codes
    if generate_portfolio:
        print("\n" + "=" * 80)
        print("GENERATING PORTFOLIO CODES (V8 - IBKR-Native)")
        print("=" * 80)

        client_id_portfolio_base = 20000

        for i, (symbol, configs) in enumerate(configs_by_symbol.items()):
            if len(configs) == 0:
                print(f"\n[{symbol}] Skipping (no strategies)")
                continue

            # Allocate client ID via manager (persistent, collision-free)
            preferred_id = client_id_portfolio_base + i
            if _CLIENT_ID_MGR_AVAILABLE:
                try:
                    script_name = f"{symbol}_portfolio"
                    allocated_id = _alloc_client_id(
                        name=script_name, role="portfolio", preferred=preferred_id
                    )
                except Exception as e:
                    print(f"  WARNING: client_id_manager failed for {symbol}_portfolio: {e} — using {preferred_id}")
                    allocated_id = preferred_id
            else:
                allocated_id = preferred_id

            try:
                output_path = generate_portfolio_code_v3(
                    symbol=symbol,
                    configs=configs,
                    output_dir=portfolio_output_dir,
                    client_id_base=allocated_id,
                    total_capital=100000,
                    trading_port=trading_port,
                    portfolio_mdd=portfolio_mdd_by_symbol.get(symbol, 0.0),
                    mdd_mode=mdd_mode,
                    fixed_portfolio_pct=mdd_portfolio_pct / 100.0,
                    fixed_single_pct=mdd_single_pct / 100.0,
                    mdd_multiplier=mdd_multiplier,
                    dry_run=dry_run,
                )
                print(f"  SUCCESS: {os.path.basename(output_path)} ({len(configs)} strategies, client_id={allocated_id})")
                total_portfolio += 1
            except Exception as e:
                print(f"  ERROR: {symbol}_portfolio: {e}")

        print(f"\nTotal portfolio strategies generated: {total_portfolio}")

    # --- Auto-populate trade_mode_assignments.json ---
    assignments_path = os.path.join(IBKR_DIR, 'trade_mode_assignments.json')

    if os.path.exists(assignments_path):
        with open(assignments_path) as f:
            assignments = json.load(f)
    else:
        assignments = {"default_mode": "trade_mode_dry_run.json", "strategies": {}}

    for symbol, configs in configs_by_symbol.items():
        for config in configs:
            strategy_name = config['strategy_name']
            assignments["strategies"][strategy_name] = "trade_mode_dry_run.json"
        portfolio_name = f"{symbol}_{interval}_portfolio"
        assignments["strategies"][portfolio_name] = "trade_mode_dry_run.json"

    with open(assignments_path, 'w') as f:
        json.dump(assignments, f, indent=2)

    print(f"\nUpdated {assignments_path}: assigned strategies -> trade_mode_dry_run.json")

    return total_single, total_portfolio


def interactive_mode():
    """
    Interactive CLI for generating IBKR deployment codes.
    Collects user inputs step-by-step and generates strategy files.
    """
    # Display banner
    print("""
╔═════════════════════════════════════════════════════════════════════╗
║     IBKR Deployment Code Generator for AQ100 - Interactive Mode     ║
║              Generate Strategy Deployment Files                     ║
╚═════════════════════════════════════════════════════════════════════╝
    """)

    # Display context info
    print("This tool generates deployment files from compilation Excel files.")
    print("All data is fetched directly from IBKR TWS using ib_insync.")
    print()
    print("Available options:")
    print("  - Intervals:      1h, 4h, 1d")
    print("  - Trading Modes:  Paper (port 7497), Live (port 7496)")
    print("  - Source:         Risk Optimized Compilation")
    print("  - Generation:     Single strategies, Portfolio files, or Both")
    print()
    print("=" * 80)
    print()

    try:
        # Step 1: Collect interval
        print("STEP 1: Interval Selection")
        print("-" * 80)
        interval = safe_input(
            "Enter interval (1h/4h/1d, default: 1d): ",
            validate_interval
        )
        print(f"  Selected: {interval}")
        print()

        # STEP 2: Trade Mode (always Dry Run)
        print("\n" + "="*60)
        print("STEP 2: Trade Mode")
        print("="*60)
        print("All strategies are generated in Dry Run mode (Paper TWS, simulated orders).")
        print("To promote a strategy, edit trade_mode_assignments.json:")
        print("  dry_run -> paper -> live_1x -> live_2x -> live_4x -> live_full")

        trading_port = 7497          # Always paper
        dry_run_mode = True          # Always dry run
        dry_run = True
        print()

        # Source: always risk_optimized
        source = 'risk_optimized'

        # Step 3: Auto-detect Excel file
        print("STEP 3: Excel File Selection")
        print("-" * 80)
        exchange = 'ibkr'
        excel_path = auto_detect_risk_optimized_file(interval, exchange)
        file_label = "Risk_Optimized_Compilation"

        if not excel_path:
            print(f"  ERROR: No {file_label} file found for {exchange} {interval}")
            print(f"  Expected pattern: WFAlphaResults/{file_label}_{exchange}_{interval}_*.xlsx")
            print()
            print("  Please ensure:")
            print("    1. The pipeline has completed stage 8 (Risk Optimization)")
            print("    2. The WFAlphaResults directory contains the expected files")
            return

        print(f"  Auto-detected: {os.path.basename(excel_path)}")
        confirm_excel = input("  Use this file? (y/n, default: y): ").strip().lower()

        if confirm_excel == 'n':
            manual_path = input("  Enter full path to Excel file: ").strip()
            if os.path.exists(manual_path):
                excel_path = manual_path
                print(f"  Using: {os.path.basename(excel_path)}")
            else:
                print(f"  ERROR: File not found: {manual_path}")
                return
        print()

        # Step 4: Get available symbols and collect symbol filter
        print("STEP 4: Symbol Selection")
        print("-" * 80)
        sheet_name = 'Portfolio'
        available_symbols = get_available_symbols(excel_path, sheet_name=sheet_name)

        if not available_symbols:
            print("  ERROR: Could not read symbols from Excel file")
            return

        print(f"  Found {len(available_symbols)} symbols in Excel file")
        print(f"  Examples: {', '.join(available_symbols[:5])}")
        print()

        symbol_filter = safe_input(
            f"Enter symbol(s) comma-separated (or press Enter for ALL {len(available_symbols)} symbols): ",
            lambda s: validate_symbol(s, available_symbols)
        )

        if symbol_filter:
            print(f"  Selected: {', '.join(symbol_filter)} ({len(symbol_filter)} symbol{'s' if len(symbol_filter) > 1 else ''})")
        else:
            print(f"  Selected: ALL symbols ({len(available_symbols)} total)")
        print()

        # Step 5: Generation type
        print("STEP 5: Generation Type")
        print("-" * 80)
        print("  1. Single strategies only (default)")
        print("  2. Portfolio only")
        print("  3. Both single + portfolio")
        print()

        generate_single, generate_portfolio = safe_input(
            "Enter choice (1/2/3, default: 1): ",
            validate_generation_type
        )

        gen_desc = []
        if generate_single:
            gen_desc.append("Single strategies")
        if generate_portfolio:
            gen_desc.append("Portfolio")
        print(f"  Selected: {' + '.join(gen_desc)}")
        print()

        # Step 6: Drawdown Kill Switch
        print("STEP 6: Drawdown Kill Switch")
        print("-" * 80)
        print("  1. Fixed Kill Switch (user-specified thresholds)")
        print("  2. Dynamic Kill Switch (multiplier × historical Max DD)")
        print()
        mdd_choice = input("Enter choice (1/2, default: 1): ").strip()
        if mdd_choice == '2':
            mdd_mode = 'dynamic'
            mdd_single_pct = 2.0   # fallback only
            mdd_portfolio_pct = 5.0  # fallback only
            print("  Selected: Dynamic Kill Switch")
            print()
            mult_input = input("  Enter Max DD multiplier (default: 1.0): ").strip()
            try:
                mdd_multiplier = float(mult_input) if mult_input else 1.0
            except ValueError:
                mdd_multiplier = 1.0
                print("  Invalid input, using default: 1.0")
            print(f"  Selected: {mdd_multiplier}x historical Max DD (capped at 100%)")
        else:
            mdd_mode = 'fixed'
            mdd_multiplier = 1.0
            print("  Selected: Fixed Kill Switch")
            print()
            single_input = input("  Enter max drawdown % for single strategies (default: 2.0): ").strip()
            try:
                mdd_single_pct = float(single_input) if single_input else 2.0
            except ValueError:
                mdd_single_pct = 2.0
                print("  Invalid input, using default: 2.0")
            portfolio_input = input("  Enter max drawdown % for portfolios (default: 5.0): ").strip()
            try:
                mdd_portfolio_pct = float(portfolio_input) if portfolio_input else 5.0
            except ValueError:
                mdd_portfolio_pct = 5.0
                print("  Invalid input, using default: 5.0")
            print(f"  Selected: {mdd_single_pct}% single / {mdd_portfolio_pct}% portfolio")
        print()

        # Calculate output directories
        single_output_dir = os.path.join(ROOT_DIR, 'deploy_ibkr')
        portfolio_output_dir = os.path.join(ROOT_DIR, 'deploy_ibkr_portfolio')

        # Display configuration summary
        source_label = 'Risk Optimized Compilation' if source == 'risk_optimized' else 'Final Compilation'
        summary_config = {
            'interval': interval,
            'trading_mode': 'paper',
            'trading_port': trading_port,
            'source': source_label,
            'excel_file': os.path.basename(excel_path),
            'symbols': ', '.join(symbol_filter) if symbol_filter else f"ALL ({len(available_symbols)} symbols)",
            'generate_single': generate_single,
            'generate_portfolio': generate_portfolio,
            'output_single': single_output_dir,
            'output_portfolio': portfolio_output_dir,
            'mdd_mode': mdd_mode,
            'mdd_single_pct': mdd_single_pct,
            'mdd_portfolio_pct': mdd_portfolio_pct,
            'mdd_multiplier': mdd_multiplier,
            'dry_run': dry_run,
        }
        display_summary(summary_config)

        # Final confirmation
        print()
        proceed = input("Proceed with generation? (y/n): ").strip().lower()
        if proceed != 'y':
            print("\nGeneration cancelled by user")
            return

        # Execute generation
        print("\n" + "=" * 80)
        print("Starting generation...")
        print("=" * 80 + "\n")

        total_single, total_portfolio = run_generation(
            interval=interval,
            exchange=exchange,
            excel_path=excel_path,
            symbol_filter=symbol_filter,
            generate_single=generate_single,
            generate_portfolio=generate_portfolio,
            output_single=None,  # Use defaults
            output_portfolio=None,  # Use defaults
            trading_port=trading_port,
            source=source,
            mdd_mode=mdd_mode,
            mdd_single_pct=mdd_single_pct,
            mdd_portfolio_pct=mdd_portfolio_pct,
            mdd_multiplier=mdd_multiplier,
            dry_run=dry_run
        )

        # Success summary
        print("\n" + "=" * 80)
        print("GENERATION COMPLETE")
        print("=" * 80)
        if generate_single:
            print(f"Single strategies: {total_single} files in {single_output_dir}")
        if generate_portfolio:
            print(f"Portfolio files:   {total_portfolio} files in {portfolio_output_dir}")
        print("=" * 80)
        print()
        print("Next steps:")
        print("  1. Review generated files in the output directories")
        print("  2. Ensure IBKR TWS is running on the selected port")
        print("  3. Run a strategy file: python <strategy_file>.py")
        print("=" * 80)

    except KeyboardInterrupt:
        print("\n\nGeneration cancelled by user (Ctrl+C)")
        print("No files were generated")
    except Exception as e:
        print(f"\nERROR: {e}")
        print("\nGeneration failed. Please check the error message above.")


def main():
    """Main entry point with CLI argument parsing or interactive mode."""

    # Check for interactive mode (no arguments)
    if len(sys.argv) == 1:
        interactive_mode()
        return

    # CLI mode with argparse
    parser = argparse.ArgumentParser(
        description='Generate IBKR-native deployment codes for US equities (V8)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python generate_deployment_codes_v4b.py --interval 1d
  python generate_deployment_codes_v4b.py --interval 1h --symbol PLTR
  python generate_deployment_codes_v4b.py --excel Final_Compilation_ibkr_1d_20251201.xlsx
        """
    )

    parser.add_argument('--interval', default='1d', choices=['1h', '1d', '4h'],
                        help='Time interval (default: 1d)')
    parser.add_argument('--exchange', default='ibkr',
                        help='Exchange name (default: ibkr)')
    parser.add_argument('--excel', default=None,
                        help='Path to Final_Compilation Excel file')
    parser.add_argument('--symbol', default=None,
                        help='Generate for specific symbol only (test mode)')
    parser.add_argument('--output-single', default=None,
                        help='Output directory for single strategy files')
    parser.add_argument('--output-portfolio', default=None,
                        help='Output directory for portfolio files')
    parser.add_argument('--portfolio', action='store_true', default=False,
                        help='Also generate portfolio files')
    parser.add_argument('--source', default='final', choices=['final', 'risk_optimized'],
                        help='Data source: final (Final_Compilation) or risk_optimized (Risk_Optimized_Compilation)')
    parser.add_argument('--mdd-mode', default='fixed', choices=['fixed', 'dynamic'],
                        help='Drawdown kill switch mode: fixed (user pct) or dynamic (multiplier × Max DD)')
    parser.add_argument('--mdd-single-pct', type=float, default=2.0,
                        help='Max drawdown %% for single strategies in fixed mode (default: 2.0)')
    parser.add_argument('--mdd-portfolio-pct', type=float, default=5.0,
                        help='Max drawdown %% for portfolios in fixed mode (default: 5.0)')
    parser.add_argument('--mdd-multiplier', type=float, default=1.0,
                        help='Max DD multiplier for dynamic mode (default: 1.0)')
    parser.add_argument('--no-dry-run', action='store_true', default=False,
                        help='Disable dry-run mode (enable real order placement)')

    args = parser.parse_args()
    dry_run = not args.no_dry_run

    source_label = 'Risk Optimized Compilation' if args.source == 'risk_optimized' else 'Final Compilation'

    print("=" * 80)
    print("IBKR DEPLOYMENT CODE GENERATOR for AQ100 (IBKR-Native)")
    print("=" * 80)
    print(f"Interval: {args.interval}")
    print(f"Exchange: {args.exchange}")
    print(f"Source: {source_label}")

    # Find Excel file
    if args.excel:
        excel_path = args.excel
    elif args.source == 'risk_optimized':
        excel_path = auto_detect_risk_optimized_file(args.interval, args.exchange)
        if not excel_path:
            print(f"ERROR: No Risk_Optimized_Compilation file found for {args.exchange} {args.interval}")
            sys.exit(1)
    else:
        excel_path = auto_detect_excel_file(args.interval, args.exchange)
        if not excel_path:
            print(f"ERROR: No Final_Compilation file found for {args.exchange} {args.interval}")
            sys.exit(1)

    # Call run_generation with CLI parameters
    try:
        total_single, total_portfolio = run_generation(
            interval=args.interval,
            exchange=args.exchange,
            excel_path=excel_path,
            symbol_filter=[args.symbol] if args.symbol else None,
            generate_single=True,
            generate_portfolio=args.portfolio,
            output_single=args.output_single,
            output_portfolio=args.output_portfolio,
            trading_port=None,  # CLI mode uses default paper trading port
            source=args.source,
            mdd_mode=args.mdd_mode,
            mdd_single_pct=args.mdd_single_pct,
            mdd_portfolio_pct=args.mdd_portfolio_pct,
            mdd_multiplier=args.mdd_multiplier,
            dry_run=dry_run,
        )

        # Display final summary
        single_output_dir = args.output_single or os.path.join(ROOT_DIR, 'deploy_ibkr')
        portfolio_output_dir = args.output_portfolio or os.path.join(ROOT_DIR, 'deploy_ibkr_portfolio')

        print("\n" + "=" * 80)
        print("GENERATION COMPLETE (V8 - IBKR-Native)")
        print("=" * 80)
        print(f"Single strategies: {total_single} files in {single_output_dir}")
        if args.portfolio:
            print(f"Portfolio files:   {total_portfolio} files in {portfolio_output_dir}")
        print("=" * 80)

    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
