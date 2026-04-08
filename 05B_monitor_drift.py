#!/usr/bin/env python3
"""
Performance Drift Visualization Dashboard
==========================================
Compares live audit CSV data against historical backtest baselines to detect
strategy drift: rolling metrics, signal distributions, and feature-level shifts.

Pre-requisite: Script requires 60 bars to fill the rolling window 

Run with:  streamlit run 05B_monitor_drift.py
CLI mode:  python 05B_monitor_drift.py --html --strategy <name> --output report.html
"""

# =============================================================================
# IMPORTS
# =============================================================================

import streamlit as st
import pandas as pd
import numpy as np
import glob
import re
from pathlib import Path
from datetime import datetime

try:
    import plotly.graph_objects as go
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

# =============================================================================
# CONSTANTS
# =============================================================================

PROJECT_DIR = Path(__file__).resolve().parent
WF_DIR = PROJECT_DIR / "WFAlphaResults"
DEPLOY_DIRS = [
    PROJECT_DIR / "deploy_ibkr",
    PROJECT_DIR / "deploy_ibkr_portfolio",
]
TRADES_DIRS = [
    PROJECT_DIR / "deploy_ibkr" / "trades",
    PROJECT_DIR / "deploy_ibkr_portfolio" / "trades",
]

META_COLUMNS = {
    "datetime", "open", "high", "low", "close", "volume", "average", "barCount",
    "start_time", "signal", "trade", "fee", "pnl", "cumulative_pnl", "bnh", "dd",
    "position", "entry_price", "exit_type", "commission", "trade_mode",
    "returns", "peak_pnl",
}

BARS_PER_YEAR = {
    "1h":    252 * 7,     # 1764
    "4h":    252 * 2,     # 504
    "1d":    252,
    "15min": 252 * 26,    # 6552
    "5min":  252 * 78,    # 19656
}

# Ordered longest-first so greedy matching works
KNOWN_BUY_TYPES = [
    "trend_reverse_long", "trend_reverse_short", "trend_reverse",
    "trend_long", "trend_short", "trend",
    "mr_reverse", "mr",
]

KNOWN_MODELS = [
    "maxabs_norm", "robust_scaler", "minmax_norm", "standard_scaler",
    "sma_diff", "ema_diff", "percent_rank", "log_return",
]

# Dark plotly template for quant aesthetic
PLOTLY_TEMPLATE = "plotly_dark"


# =============================================================================
# STEP 1 — SCAFFOLDING + DATA DISCOVERY
# =============================================================================

def discover_audit_strategies(trades_dirs=None):
    """Discover strategy names from audit_*.csv files across all trade directories."""
    if trades_dirs is None:
        trades_dirs = TRADES_DIRS
    strategies = set()
    for trades_dir in trades_dirs:
        trades_dir = Path(trades_dir)
        if not trades_dir.exists():
            continue
        pattern = str(trades_dir / "audit_*.csv")
        files = glob.glob(pattern)
        for f in files:
            name = Path(f).stem  # e.g. audit_GLD_1h_volatility_20_rsi_trend_reverse_long_christine_202603250037
            if name.startswith("audit_"):
                strategies.add(name[len("audit_"):])
    return sorted(strategies)


def extract_config(strategy_name, base_dir=None):
    """
    Extract CONFIG dict from deployed .py files via regex (no exec/eval).
    Searches deploy_ibkr/ and deploy_ibkr_portfolio/ for matching files.
    Returns dict with keys: symbol, feature, model, buy_type, interval,
    and optionally backtest_optimized_* for v6 strategies.
    """
    if base_dir is None:
        base_dir = PROJECT_DIR
    base_dir = Path(base_dir)

    config = {}
    deploy_dirs = [
        base_dir / "deploy_ibkr",
        base_dir / "deploy_ibkr_portfolio",
    ]

    # Search for .py file matching strategy name
    target_file = None
    for d in deploy_dirs:
        if not d.exists():
            continue
        # Try exact match first
        candidate = d / f"{strategy_name}.py"
        if candidate.exists():
            target_file = candidate
            break
        # Try glob for partial matches
        for py_file in d.glob("*.py"):
            if strategy_name in py_file.stem:
                target_file = py_file
                break
        if target_file:
            break

    if target_file is None:
        return infer_config_from_name(strategy_name)

    try:
        content = target_file.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return infer_config_from_name(strategy_name)

    # Regex extraction of CONFIG keys
    key_patterns = {
        "symbol":   r'"symbol"\s*:\s*"([^"]+)"',
        "feature":  r'"feature"\s*:\s*"([^"]+)"',
        "model":    r'"model"\s*:\s*"([^"]+)"',
        "buy_type": r'"buy_type"\s*:\s*"([^"]+)"',
        "interval": r'"interval"\s*:\s*"([^"]+)"',
    }
    for key, pat in key_patterns.items():
        m = re.search(pat, content)
        if m:
            config[key] = m.group(1)

    # v6 baseline metrics from CONFIG
    baseline_keys = {
        "backtest_optimized_sharpe":        r'"backtest_optimized_sharpe"\s*:\s*([\d.eE+-]+)',
        "backtest_optimized_mdd":           r'"backtest_optimized_mdd"\s*:\s*([\d.eE+-]+)',
        "backtest_optimized_annual_return": r'"backtest_optimized_annual_return"\s*:\s*([\d.eE+-]+)',
        "backtest_optimized_calmar":        r'"backtest_optimized_calmar"\s*:\s*([\d.eE+-]+)',
    }
    for key, pat in baseline_keys.items():
        m = re.search(pat, content)
        if m:
            config[key] = float(m.group(1))

    # Detect v6 vs v5
    config["version"] = "v6" if "backtest_optimized_sharpe" in config else "v5"

    # Fill missing keys from name inference
    inferred = infer_config_from_name(strategy_name)
    for k, v in inferred.items():
        if k not in config:
            config[k] = v

    return config


def infer_config_from_name(strategy_name):
    """
    Fallback parser: extract config from strategy name using known lists.
    Pattern: {SYMBOL}_{INTERVAL}_{FEATURE}_{MODEL}_{BUYTYPE}_{NAME}_{DATE}  (v6)
    Pattern: {SYMBOL}_{FEATURE}_{MODEL}_{BUYTYPE}_{NAME}_{DATE}             (v5)
    """
    config = {}

    # Strip trailing _personname_date suffix (handles any name e.g. _wiho_08Feb2026, _christine_202603250037)
    m = re.search(r'_([A-Za-z]+)_(\d{2}[A-Za-z]{3}\d{4}|\d{10,12})$', strategy_name)
    if m:
        config["wiho_date"] = m.group(2)
        name_part = strategy_name[:m.start()]
    else:
        name_part = strategy_name

    tokens = name_part.split("_")
    if not tokens:
        return config

    # Symbol is always first token (uppercase ticker)
    config["symbol"] = tokens[0]

    # Check if second token is an interval
    if len(tokens) > 1 and tokens[1] in BARS_PER_YEAR:
        config["interval"] = tokens[1]
        remainder = "_".join(tokens[2:])
    else:
        config["interval"] = "1h"  # default
        remainder = "_".join(tokens[1:])

    # Match buy_type from the end (longest first)
    matched_buy_type = None
    feature_model_part = remainder
    for bt in KNOWN_BUY_TYPES:
        if remainder.endswith("_" + bt) or remainder == bt:
            matched_buy_type = bt
            if remainder == bt:
                feature_model_part = ""
            else:
                feature_model_part = remainder[:-(len(bt) + 1)]
            break
    if matched_buy_type:
        config["buy_type"] = matched_buy_type

    # Match model from what remains
    if feature_model_part:
        matched_model = None
        for model in KNOWN_MODELS:
            if feature_model_part.endswith("_" + model) or feature_model_part == model:
                matched_model = model
                if feature_model_part == model:
                    config["feature"] = ""
                else:
                    config["feature"] = feature_model_part[:-(len(model) + 1)]
                break
        if matched_model:
            config["model"] = matched_model
        else:
            # No model matched — entire remainder is feature
            config["feature"] = feature_model_part

    config.setdefault("version", "v5")
    return config


# =============================================================================
# STEP 2 — DATA LOADING
# =============================================================================

def load_audit_csv(strategy_name, trades_dirs=None):
    """Load audit CSV for a strategy, searching across all trade directories."""
    if trades_dirs is None:
        trades_dirs = TRADES_DIRS
    for trades_dir in trades_dirs:
        trades_dir = Path(trades_dir)
        path = trades_dir / f"audit_{strategy_name}.csv"
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path, on_bad_lines="skip")
            if "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True).dt.tz_localize(None)
                df = df.dropna(subset=["datetime"])
                df = df.sort_values("datetime").reset_index(drop=True)
                df = df.set_index("datetime")
            return df
        except Exception:
            continue
    return None


def locate_backtest_csv(config, wf_dir=None):
    """
    Locate the backtest CSV using config keys.
    Pattern: WFAlphaResults/merged_ibkr_{symbol}_{interval}_{name}_{date}/{feature}/{model}/{buy_type}*/backtest.csv
    Resolution: exact -> _long -> _short -> glob fallback.
    Multiple date folders: take latest by parsed date.
    """
    if wf_dir is None:
        wf_dir = WF_DIR
    wf_dir = Path(wf_dir)
    if not wf_dir.exists():
        return None

    symbol = config.get("symbol", "")
    interval = config.get("interval", "1h")
    feature = config.get("feature", "")
    model = config.get("model", "")
    buy_type = config.get("buy_type", "")

    if not symbol or not feature or not model or not buy_type:
        return None

    # Find all matching date folders (any person name between interval and date)
    folder_pattern = f"merged_ibkr_{symbol}_{interval}_*"
    date_folders = list(wf_dir.glob(folder_pattern))
    if not date_folders:
        return None

    # Sort by parsed date, take latest
    def parse_folder_date(folder_path):
        name = folder_path.name
        m = re.search(r'_[A-Za-z]+_(\d{2}[A-Za-z]{3}\d{4})$', name)
        if m:
            try:
                return datetime.strptime(m.group(1), "%d%b%Y")
            except ValueError:
                pass
        return datetime.min

    date_folders.sort(key=parse_folder_date, reverse=True)

    # Try each date folder (latest first)
    for folder in date_folders:
        base = folder / feature / model

        if not base.exists():
            continue

        # Resolution order for buy_type
        candidates = [
            base / buy_type / "backtest.csv",
            base / f"{buy_type}_long" / "backtest.csv",
            base / f"{buy_type}_short" / "backtest.csv",
        ]

        for candidate in candidates:
            if candidate.exists():
                return candidate

        # Glob fallback
        glob_matches = list(base.glob(f"{buy_type}*/backtest.csv"))
        if glob_matches:
            return glob_matches[0]

    return None


def load_backtest_csv(path):
    """Load backtest CSV, returning DataFrame with datetime index."""
    if path is None:
        return None
    path = Path(path)
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path)
        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True).dt.tz_localize(None)
            df = df.dropna(subset=["datetime"])
            df = df.sort_values("datetime").reset_index(drop=True)
            df = df.set_index("datetime")
        return df
    except Exception:
        return None


def load_baseline_metrics(config, backtest_path=None):
    """
    Load baseline metrics.
    Priority: v6 CONFIG keys -> v5 metrics.csv -> compute from backtest pnl.
    MDD sign convention: normalize to NEGATIVE internally.
    """
    baseline = {}

    # v6: from CONFIG keys (negate MDD since CONFIG stores positive)
    if config.get("version") == "v6":
        if "backtest_optimized_sharpe" in config:
            baseline["sharpe"] = config["backtest_optimized_sharpe"]
            baseline["mdd"] = -abs(config.get("backtest_optimized_mdd", 0))
            baseline["annual_return"] = config.get("backtest_optimized_annual_return", 0)
            baseline["calmar"] = config.get("backtest_optimized_calmar", 0)
            return baseline

    # v5: from metrics.csv alongside backtest.csv
    if backtest_path is not None:
        metrics_path = Path(backtest_path).parent / "metrics.csv"
        if metrics_path.exists():
            try:
                mdf = pd.read_csv(metrics_path)
                metrics_dict = dict(zip(mdf["Metric"], mdf["Value"]))
                baseline["sharpe"] = float(metrics_dict.get("Sharpe Ratio", 0))
                baseline["mdd"] = float(metrics_dict.get("Max Drawdown", 0))  # already negative
                baseline["annual_return"] = float(metrics_dict.get("Annualized Return", 0))
                baseline["calmar"] = float(metrics_dict.get("Calmar Ratio", 0))
                return baseline
            except Exception:
                pass

    # Fallback: compute from backtest CSV pnl column
    if backtest_path is not None:
        bt_df = load_backtest_csv(backtest_path)
        if bt_df is not None and "pnl" in bt_df.columns:
            interval = config.get("interval", "1h")
            bpy = BARS_PER_YEAR.get(interval, 1764)
            baseline = compute_overall_metrics(
                bt_df["pnl"],
                bt_df["cumulative_pnl"] if "cumulative_pnl" in bt_df.columns else bt_df["pnl"].cumsum(),
                bpy,
            )
            return baseline

    return baseline


def discover_common_features(audit_df, backtest_df):
    """Find numeric columns present in both DataFrames, minus META_COLUMNS."""
    if audit_df is None or backtest_df is None:
        return []
    audit_numeric = set(audit_df.select_dtypes(include=[np.number]).columns)
    bt_numeric = set(backtest_df.select_dtypes(include=[np.number]).columns)
    common = (audit_numeric & bt_numeric) - META_COLUMNS
    # Case-insensitive exclusion for safety
    common = {c for c in common if c.lower() not in META_COLUMNS}
    return sorted(common)


# =============================================================================
# STEP 3 — METRIC COMPUTATION
# =============================================================================

def compute_rolling_sharpe(pnl, window, bars_per_year):
    """Vectorized rolling Sharpe ratio."""
    rolling_mean = pnl.rolling(window, min_periods=window).mean()
    rolling_std = pnl.rolling(window, min_periods=window).std()
    return (rolling_mean / rolling_std.replace(0, np.nan)) * np.sqrt(bars_per_year)


def compute_rolling_mdd(cumulative_pnl, window):
    """
    Rolling max drawdown using np.maximum.accumulate within each window.
    Returns negative values (drawdown convention).
    """
    def _window_mdd(window_data):
        peak = np.maximum.accumulate(window_data)
        dd = window_data - peak
        return dd.min()

    return cumulative_pnl.rolling(window, min_periods=2).apply(_window_mdd, raw=True)


def compute_rolling_annual_return(pnl, window, bars_per_year):
    """Vectorized rolling annualized return."""
    return pnl.rolling(window, min_periods=window).sum() * (bars_per_year / window)


def compute_rolling_calmar(rolling_return, rolling_mdd):
    """Vectorized rolling Calmar ratio with NaN guard."""
    mdd_abs = rolling_mdd.abs().replace(0, np.nan)
    return rolling_return / mdd_abs


def compute_signal_distribution(df):
    """
    Count signal values and return ratios.
    Returns dict like {1: 0.45, 0: 0.30, -1: 0.25}.
    """
    if df is None or "signal" not in df.columns:
        return {}
    counts = df["signal"].value_counts()
    total = counts.sum()
    if total == 0:
        return {}
    result = {}
    for val in [-1, 0, 1]:
        result[val] = counts.get(val, 0) / total
    return result


def compute_overall_metrics(pnl, cumulative_pnl, bars_per_year):
    """Compute full-series (non-rolling) metrics for summary cards."""
    metrics = {}
    n = len(pnl)
    if n == 0:
        return {"sharpe": 0, "mdd": 0, "annual_return": 0, "calmar": 0}

    mean_pnl = pnl.mean()
    std_pnl = pnl.std()
    if std_pnl != 0 and not np.isnan(std_pnl):
        metrics["sharpe"] = (mean_pnl / std_pnl) * np.sqrt(bars_per_year)
    else:
        metrics["sharpe"] = 0.0

    # MDD from cumulative PnL
    peak = cumulative_pnl.expanding().max()
    dd = cumulative_pnl - peak
    metrics["mdd"] = dd.min() if len(dd) > 0 else 0.0

    # Annual return
    total_return = pnl.sum()
    if n > 0:
        metrics["annual_return"] = total_return * (bars_per_year / n)
    else:
        metrics["annual_return"] = 0.0

    # Calmar
    mdd_abs = abs(metrics["mdd"]) if metrics["mdd"] != 0 else np.nan
    if not np.isnan(mdd_abs) and mdd_abs != 0:
        metrics["calmar"] = metrics["annual_return"] / mdd_abs
    else:
        metrics["calmar"] = 0.0

    return metrics


# =============================================================================
# STEP 4 — VISUALIZATION (PLOTLY)
# =============================================================================

def plot_rolling_metric(series, baseline=None, title="", y_label="", datetime_index=None):
    """
    Line chart of a rolling metric with optional baseline horizontal line.
    Uses dark theme.
    """
    fig = go.Figure()

    x_vals = datetime_index if datetime_index is not None else series.index

    fig.add_trace(go.Scatter(
        x=x_vals,
        y=series.values,
        mode="lines",
        name="Live (rolling)",
        line=dict(color="#00d4ff", width=1.5),
    ))

    if baseline is not None and not np.isnan(baseline):
        fig.add_hline(
            y=baseline,
            line_dash="dash",
            line_color="#ff6b6b",
            annotation_text=f"Baseline: {baseline:.4f}",
            annotation_position="top left",
            annotation_font_color="#ff6b6b",
        )

    fig.update_layout(
        title=dict(text=title, font=dict(size=14)),
        yaxis_title=y_label,
        template=PLOTLY_TEMPLATE,
        height=300,
        margin=dict(l=50, r=20, t=40, b=30),
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    return fig


def plot_signal_comparison(bt_dist, live_dist):
    """Grouped bar chart comparing signal distributions (backtest vs live)."""
    labels = ["Short (-1)", "Flat (0)", "Long (1)"]
    signal_vals = [-1, 0, 1]

    bt_values = [bt_dist.get(v, 0) for v in signal_vals]
    live_values = [live_dist.get(v, 0) for v in signal_vals]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Backtest",
        x=labels,
        y=bt_values,
        marker_color="#636efa",
    ))
    fig.add_trace(go.Bar(
        name="Live",
        x=labels,
        y=live_values,
        marker_color="#00cc96",
    ))

    fig.update_layout(
        title=dict(text="Signal Distribution", font=dict(size=14)),
        yaxis_title="Ratio",
        barmode="group",
        template=PLOTLY_TEMPLATE,
        height=350,
        margin=dict(l=50, r=20, t=40, b=30),
    )

    return fig


def plot_model_score_distribution(bt_scores, live_scores, model_name="model_score"):
    """Overlaid histogram comparing model score distributions."""
    fig = go.Figure()

    if bt_scores is not None and len(bt_scores) > 0:
        fig.add_trace(go.Histogram(
            x=bt_scores,
            name="Backtest",
            opacity=0.6,
            marker_color="#636efa",
            nbinsx=50,
        ))

    if live_scores is not None and len(live_scores) > 0:
        fig.add_trace(go.Histogram(
            x=live_scores,
            name="Live",
            opacity=0.6,
            marker_color="#00cc96",
            nbinsx=50,
        ))

    fig.update_layout(
        title=dict(text=f"Model Score Distribution: {model_name}", font=dict(size=14)),
        xaxis_title="Score",
        yaxis_title="Count",
        barmode="overlay",
        template=PLOTLY_TEMPLATE,
        height=350,
        margin=dict(l=50, r=20, t=40, b=30),
    )

    return fig


def plot_feature_drift(bt_col, live_col, feature_name="feature"):
    """Overlaid histogram comparing a feature distribution (backtest vs live)."""
    fig = go.Figure()

    if bt_col is not None and len(bt_col) > 0:
        fig.add_trace(go.Histogram(
            x=bt_col.dropna(),
            name="Backtest",
            opacity=0.6,
            marker_color="#636efa",
            nbinsx=50,
        ))

    if live_col is not None and len(live_col) > 0:
        fig.add_trace(go.Histogram(
            x=live_col.dropna(),
            name="Live",
            opacity=0.6,
            marker_color="#00cc96",
            nbinsx=50,
        ))

    fig.update_layout(
        title=dict(text=f"Feature Drift: {feature_name}", font=dict(size=14)),
        xaxis_title="Value",
        yaxis_title="Count",
        barmode="overlay",
        template=PLOTLY_TEMPLATE,
        height=300,
        margin=dict(l=50, r=20, t=40, b=30),
    )

    return fig


# =============================================================================
# STEP 6 — HTML EXPORT
# =============================================================================

def generate_html_report(strategy_name, figures, metadata):
    """
    Generate a dark-themed standalone HTML report with all Plotly figures.
    First figure uses include_plotlyjs='cdn', rest use include_plotlyjs=False.
    """
    symbol = metadata.get("symbol", "")
    interval = metadata.get("interval", "")
    report_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    audit_bars = metadata.get("audit_bars", 0)
    backtest_bars = metadata.get("backtest_bars", 0)

    header_html = f"""
    <div style="background: #1e1e2f; color: #e0e0e0; padding: 20px;
                font-family: 'Segoe UI', Tahoma, sans-serif; border-radius: 8px;
                margin-bottom: 20px;">
        <h1 style="color: #00d4ff; margin: 0;">Drift Report: {strategy_name}</h1>
        <p style="color: #aaa; margin: 5px 0;">
            Symbol: <b>{symbol}</b> | Interval: <b>{interval}</b> |
            Generated: <b>{report_date}</b>
        </p>
        <p style="color: #aaa; margin: 5px 0;">
            Audit bars: <b>{audit_bars}</b> | Backtest bars: <b>{backtest_bars}</b>
        </p>
    </div>
    """

    body_parts = [header_html]
    plotly_js_included = False

    for section_title, fig in figures:
        section_html = f"""
        <div style="background: #1e1e2f; color: #e0e0e0; padding: 10px 20px;
                    margin: 10px 0; border-radius: 8px;">
            <h2 style="color: #00d4ff; border-bottom: 1px solid #333; padding-bottom: 8px;">
                {section_title}
            </h2>
        """
        if not plotly_js_included:
            fig_html = fig.to_html(
                full_html=False,
                include_plotlyjs="cdn",
                config={"displayModeBar": True},
            )
            plotly_js_included = True
        else:
            fig_html = fig.to_html(
                full_html=False,
                include_plotlyjs=False,
                config={"displayModeBar": True},
            )
        section_html += fig_html + "</div>"
        body_parts.append(section_html)

    full_html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Drift Report - {strategy_name}</title>
    <style>
        body {{
            background: #0e0e1a;
            color: #e0e0e0;
            font-family: 'Segoe UI', Tahoma, sans-serif;
            margin: 20px;
            padding: 0;
        }}
    </style>
</head>
<body>
{"".join(body_parts)}
</body>
</html>"""

    return full_html


# =============================================================================
# STEP 5 — STREAMLIT ASSEMBLY
# =============================================================================

def init_session_state():
    """Initialize session state with defaults."""
    defaults = {
        "drift_strategy":      None,
        "drift_window":        60,
        "drift_date_start":    None,
        "drift_date_end":      None,
        "drift_figures":       [],
        "drift_metadata":      {},
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def main():
    """Main Streamlit application."""
    st.set_page_config(page_title="Drift Monitor", layout="wide")
    init_session_state()

    # ── Check Plotly ────────────────────────────────────────────────────────
    if not HAS_PLOTLY:
        st.error(
            "Plotly is required but not installed. "
            "Please run: `pip install plotly`"
        )
        return

    # ── Discover strategies ─────────────────────────────────────────────────
    strategies = discover_audit_strategies()

    # ── Sidebar ─────────────────────────────────────────────────────────────
    st.sidebar.header("Drift Monitor")

    if not strategies:
        st.sidebar.info("No audit CSVs found in trades/")
        selected_strategy = None
    else:
        selected_strategy = st.sidebar.selectbox(
            "Strategy",
            options=strategies,
            index=0,
        )

    st.sidebar.divider()

    rolling_window = st.sidebar.slider(
        "Rolling Window (bars)",
        min_value=20,
        max_value=200,
        value=st.session_state.drift_window,
        step=5,
    )
    st.session_state.drift_window = rolling_window

    # ── No strategies → early return ────────────────────────────────────────
    if selected_strategy is None:
        st.title("Performance Drift Monitor")
        st.info(
            "No audit CSVs found. Deploy strategies with v6 template to generate "
            "audit data, then revisit this dashboard."
        )
        return

    # ── Load Data ───────────────────────────────────────────────────────────
    st.title("Performance Drift Monitor")

    config = extract_config(selected_strategy)
    interval = config.get("interval", "1h")
    bars_per_year = BARS_PER_YEAR.get(interval, 1764)

    audit_df = load_audit_csv(selected_strategy)
    backtest_path = locate_backtest_csv(config)
    backtest_df = load_backtest_csv(backtest_path)
    baseline = load_baseline_metrics(config, backtest_path)
    model_col = config.get("model", "")

    # ── Date range filter ───────────────────────────────────────────────────
    if audit_df is not None and len(audit_df) > 0:
        min_date = audit_df.index.min().date()
        max_date = audit_df.index.max().date()

        st.sidebar.divider()
        st.sidebar.subheader("Date Range")
        date_start = st.sidebar.date_input("Start", value=min_date, min_value=min_date, max_value=max_date)
        date_end = st.sidebar.date_input("End", value=max_date, min_value=min_date, max_value=max_date)

        # Filter audit data by date range
        mask = (audit_df.index.date >= date_start) & (audit_df.index.date <= date_end)
        audit_df = audit_df.loc[mask]

    # ── Sidebar data info ───────────────────────────────────────────────────
    common_features = discover_common_features(audit_df, backtest_df)

    st.sidebar.divider()
    st.sidebar.subheader("Data Info")
    st.sidebar.text(f"Strategy: {selected_strategy}")
    st.sidebar.text(f"Symbol: {config.get('symbol', 'N/A')}")
    st.sidebar.text(f"Interval: {interval}")
    st.sidebar.text(f"Template: {config.get('version', 'N/A')}")
    st.sidebar.text(f"Audit bars: {len(audit_df) if audit_df is not None else 0}")
    st.sidebar.text(f"Backtest bars: {len(backtest_df) if backtest_df is not None else 0}")
    st.sidebar.text(f"Common features: {len(common_features)}")

    # ── Handle edge case: no audit data ─────────────────────────────────────
    if audit_df is None or len(audit_df) == 0:
        st.warning(
            f"No audit data available for **{selected_strategy}**. "
            "The strategy may not have generated any audit rows yet."
        )
        if backtest_df is not None:
            with st.expander("Backtest Data (preview)"):
                st.dataframe(backtest_df.head(20))
        return

    # ── Compute live overall metrics ────────────────────────────────────────
    if "pnl" not in audit_df.columns:
        st.error("Audit CSV is missing the 'pnl' column. Cannot compute metrics.")
        return

    audit_pnl = audit_df["pnl"].astype(float)
    audit_cum_pnl = (
        audit_df["cumulative_pnl"].astype(float)
        if "cumulative_pnl" in audit_df.columns
        else audit_pnl.cumsum()
    )

    live_metrics = compute_overall_metrics(audit_pnl, audit_cum_pnl, bars_per_year)

    # ── Summary Metrics (2 rows x 4 columns) ───────────────────────────────
    st.subheader("Summary Metrics")

    # Row 1: Live metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        delta_s = live_metrics["sharpe"] - baseline.get("sharpe", 0) if baseline else None
        st.metric("Live Sharpe", f"{live_metrics['sharpe']:.4f}",
                  delta=f"{delta_s:.4f}" if delta_s is not None else None)
    with col2:
        delta_m = live_metrics["mdd"] - baseline.get("mdd", 0) if baseline else None
        st.metric("Live MDD", f"{live_metrics['mdd']:.4f}",
                  delta=f"{delta_m:.4f}" if delta_m is not None else None)
    with col3:
        delta_r = live_metrics["annual_return"] - baseline.get("annual_return", 0) if baseline else None
        st.metric("Live Ann. Return", f"{live_metrics['annual_return']:.4f}",
                  delta=f"{delta_r:.4f}" if delta_r is not None else None)
    with col4:
        delta_c = live_metrics["calmar"] - baseline.get("calmar", 0) if baseline else None
        st.metric("Live Calmar", f"{live_metrics['calmar']:.4f}",
                  delta=f"{delta_c:.4f}" if delta_c is not None else None)

    # Row 2: Baseline metrics
    col5, col6, col7, col8 = st.columns(4)
    with col5:
        st.metric("Baseline Sharpe", f"{baseline.get('sharpe', 0):.4f}" if baseline else "N/A")
    with col6:
        st.metric("Baseline MDD", f"{baseline.get('mdd', 0):.4f}" if baseline else "N/A")
    with col7:
        st.metric("Baseline Ann. Return", f"{baseline.get('annual_return', 0):.4f}" if baseline else "N/A")
    with col8:
        st.metric("Baseline Calmar", f"{baseline.get('calmar', 0):.4f}" if baseline else "N/A")

    # ── Rolling Metrics (2x2 grid) ──────────────────────────────────────────
    st.subheader("Rolling Metrics")

    if len(audit_df) < rolling_window:
        st.warning(
            f"Insufficient data for rolling window={rolling_window}. "
            f"Only {len(audit_df)} bars available. Reduce the window size or wait for more data."
        )
    else:
        rolling_sharpe = compute_rolling_sharpe(audit_pnl, rolling_window, bars_per_year)
        rolling_mdd_val = compute_rolling_mdd(audit_cum_pnl, rolling_window)
        rolling_ret = compute_rolling_annual_return(audit_pnl, rolling_window, bars_per_year)
        rolling_calmar_val = compute_rolling_calmar(rolling_ret, rolling_mdd_val)

        dt_index = audit_df.index

        # Collect figures for HTML export
        report_figures = []

        r1c1, r1c2 = st.columns(2)
        with r1c1:
            fig_sharpe = plot_rolling_metric(
                rolling_sharpe, baseline.get("sharpe"),
                "Rolling Sharpe Ratio", "Sharpe", dt_index,
            )
            st.plotly_chart(fig_sharpe, use_container_width=True)
            report_figures.append(("Rolling Sharpe Ratio", fig_sharpe))

        with r1c2:
            fig_mdd = plot_rolling_metric(
                rolling_mdd_val, baseline.get("mdd"),
                "Rolling Max Drawdown", "MDD", dt_index,
            )
            st.plotly_chart(fig_mdd, use_container_width=True)
            report_figures.append(("Rolling Max Drawdown", fig_mdd))

        r2c1, r2c2 = st.columns(2)
        with r2c1:
            fig_ret = plot_rolling_metric(
                rolling_ret, baseline.get("annual_return"),
                "Rolling Annual Return", "Return", dt_index,
            )
            st.plotly_chart(fig_ret, use_container_width=True)
            report_figures.append(("Rolling Annual Return", fig_ret))

        with r2c2:
            fig_calmar = plot_rolling_metric(
                rolling_calmar_val, baseline.get("calmar"),
                "Rolling Calmar Ratio", "Calmar", dt_index,
            )
            st.plotly_chart(fig_calmar, use_container_width=True)
            report_figures.append(("Rolling Calmar Ratio", fig_calmar))

        # ── Signal Drift ────────────────────────────────────────────────────
        st.subheader("Signal Drift")

        live_signal_dist = compute_signal_distribution(audit_df)
        bt_signal_dist = compute_signal_distribution(backtest_df)

        sig_c1, sig_c2 = st.columns(2)

        with sig_c1:
            if bt_signal_dist or live_signal_dist:
                fig_sig = plot_signal_comparison(bt_signal_dist, live_signal_dist)
                st.plotly_chart(fig_sig, use_container_width=True)
                report_figures.append(("Signal Distribution", fig_sig))
            else:
                st.info("No signal data available for comparison.")

        with sig_c2:
            if model_col and model_col in audit_df.columns:
                live_scores = audit_df[model_col].dropna()
                bt_scores = (
                    backtest_df[model_col].dropna()
                    if backtest_df is not None and model_col in backtest_df.columns
                    else pd.Series(dtype=float)
                )
                fig_score = plot_model_score_distribution(bt_scores, live_scores, model_col)
                st.plotly_chart(fig_score, use_container_width=True)
                report_figures.append(("Model Score Distribution", fig_score))
            else:
                st.info(
                    f"Model score column '{model_col}' not found in audit data. "
                    "Skipping model score distribution."
                )

        # ── Feature Drift ───────────────────────────────────────────────────
        st.subheader("Feature Drift")

        if common_features:
            for feat in common_features:
                if feat == model_col:
                    continue  # already shown in Model Score Distribution
                with st.expander(f"Feature: {feat}"):
                    bt_feat = (
                        backtest_df[feat].dropna()
                        if backtest_df is not None and feat in backtest_df.columns
                        else pd.Series(dtype=float)
                    )
                    live_feat = audit_df[feat].dropna() if feat in audit_df.columns else pd.Series(dtype=float)
                    fig_feat = plot_feature_drift(bt_feat, live_feat, feat)
                    st.plotly_chart(fig_feat, use_container_width=True)
                    report_figures.append((f"Feature Drift: {feat}", fig_feat))
        else:
            if backtest_df is None:
                st.info("No backtest data found. Cannot compare feature distributions.")
            else:
                st.info("No common numeric features found between audit and backtest data.")

        # ── Raw Data ────────────────────────────────────────────────────────
        st.subheader("Raw Data")
        with st.expander("Audit Data (head)"):
            st.dataframe(audit_df.head(50))
        if backtest_df is not None:
            with st.expander("Backtest Data (head)"):
                st.dataframe(backtest_df.head(50))

        # ── HTML Export (sidebar) ───────────────────────────────────────────
        metadata = {
            "symbol": config.get("symbol", ""),
            "interval": interval,
            "audit_bars": len(audit_df),
            "backtest_bars": len(backtest_df) if backtest_df is not None else 0,
        }
        st.session_state.drift_figures = report_figures
        st.session_state.drift_metadata = metadata

    # ── Sidebar HTML download button ────────────────────────────────────────
    st.sidebar.divider()
    if st.session_state.drift_figures:
        html_content = generate_html_report(
            selected_strategy,
            st.session_state.drift_figures,
            st.session_state.drift_metadata,
        )
        st.sidebar.download_button(
            label="Download HTML Report",
            data=html_content,
            file_name=f"drift_report_{selected_strategy}.html",
            mime="text/html",
        )


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Performance Drift Report")
    parser.add_argument("--html", action="store_true", help="Generate HTML report")
    parser.add_argument("--strategy", type=str, help="Strategy name (from audit CSV)")
    parser.add_argument("--output", type=str, default="drift_report.html", help="Output HTML file path")
    parser.add_argument("--window", type=int, default=60, help="Rolling window size in bars")

    # When launched via `streamlit run`, sys.argv contains streamlit-specific args.
    # Only parse our args when NOT running under streamlit.
    if "streamlit" in sys.modules and hasattr(sys.modules["streamlit"], "runtime"):
        # Running under Streamlit — call main() directly
        main()
    else:
        args = parser.parse_args()

        if args.html:
            if not args.strategy:
                print("Error: --strategy is required for HTML generation.")
                sys.exit(1)

            if not HAS_PLOTLY:
                print("Error: plotly is required. Install with: pip install plotly")
                sys.exit(1)

            print(f"Loading data for strategy: {args.strategy}")
            config = extract_config(args.strategy)
            interval = config.get("interval", "1h")
            bpy = BARS_PER_YEAR.get(interval, 1764)

            audit_df = load_audit_csv(args.strategy)
            backtest_path = locate_backtest_csv(config)
            backtest_df = load_backtest_csv(backtest_path)
            baseline = load_baseline_metrics(config, backtest_path)
            model_col = config.get("model", "")

            figures = []
            metadata = {
                "symbol": config.get("symbol", ""),
                "interval": interval,
                "audit_bars": len(audit_df) if audit_df is not None else 0,
                "backtest_bars": len(backtest_df) if backtest_df is not None else 0,
            }

            if audit_df is not None and "pnl" in audit_df.columns and len(audit_df) >= args.window:
                pnl = audit_df["pnl"].astype(float)
                cum_pnl = (
                    audit_df["cumulative_pnl"].astype(float)
                    if "cumulative_pnl" in audit_df.columns
                    else pnl.cumsum()
                )
                dt_idx = audit_df.index

                # Rolling metrics
                rs = compute_rolling_sharpe(pnl, args.window, bpy)
                rm = compute_rolling_mdd(cum_pnl, args.window)
                rr = compute_rolling_annual_return(pnl, args.window, bpy)
                rc = compute_rolling_calmar(rr, rm)

                figures.append(("Rolling Sharpe Ratio",
                                plot_rolling_metric(rs, baseline.get("sharpe"), "Rolling Sharpe", "Sharpe", dt_idx)))
                figures.append(("Rolling Max Drawdown",
                                plot_rolling_metric(rm, baseline.get("mdd"), "Rolling MDD", "MDD", dt_idx)))
                figures.append(("Rolling Annual Return",
                                plot_rolling_metric(rr, baseline.get("annual_return"), "Rolling Return", "Return", dt_idx)))
                figures.append(("Rolling Calmar Ratio",
                                plot_rolling_metric(rc, baseline.get("calmar"), "Rolling Calmar", "Calmar", dt_idx)))

                # Signal drift
                live_sig = compute_signal_distribution(audit_df)
                bt_sig = compute_signal_distribution(backtest_df)
                if live_sig or bt_sig:
                    figures.append(("Signal Distribution", plot_signal_comparison(bt_sig, live_sig)))

                # Model score
                if model_col and model_col in audit_df.columns:
                    ls = audit_df[model_col].dropna()
                    bs = (
                        backtest_df[model_col].dropna()
                        if backtest_df is not None and model_col in backtest_df.columns
                        else pd.Series(dtype=float)
                    )
                    figures.append(("Model Score Distribution",
                                    plot_model_score_distribution(bs, ls, model_col)))

                # Feature drift
                common = discover_common_features(audit_df, backtest_df)
                for feat in common:
                    bf = backtest_df[feat].dropna() if backtest_df is not None and feat in backtest_df.columns else pd.Series(dtype=float)
                    lf = audit_df[feat].dropna() if feat in audit_df.columns else pd.Series(dtype=float)
                    figures.append((f"Feature Drift: {feat}", plot_feature_drift(bf, lf, feat)))

            elif audit_df is not None and "pnl" in audit_df.columns:
                print(f"Warning: Only {len(audit_df)} bars, need {args.window} for rolling metrics.")
            else:
                print("Warning: No audit data or missing pnl column.")

            if figures:
                html = generate_html_report(args.strategy, figures, metadata)
                output_path = Path(args.output)
                output_path.write_text(html, encoding="utf-8")
                print(f"Report saved to: {output_path.resolve()}")
            else:
                print("No figures generated. Report not created.")
        else:
            print("Use --html to generate a report, or run with: streamlit run 05B_monitor_drift.py")
