"""
IBKR Pipeline Orchestrator
Automates the end-to-end 7-stage IBKR trading strategy workflow.

Execution Model: Sequential Interval, Parallel Symbol
- For each interval (1h, 4h, 1d):
    - For each stage (1-7):
        - Process all symbols in parallel
        - Wait for completion before next stage

Features:
- Multi-interval and multi-symbol support
- Symbol-level parallelism within stages
- FunnelTracker integration for metrics
- Resume capability from checkpoints
- Error isolation per symbol
- Timestamped output directories
- Environment validation

Usage:
    python run_ibkr_gs_pipeline.py
    python run_ibkr_gs_pipeline.py --intervals 1h 4h --symbols AAPL
    python run_ibkr_gs_pipeline.py --resume checkpoint_20250120_143022.json
    python run_ibkr_gs_pipeline.py --start-stage 3 --intervals 1h
"""

import os
import sys
import subprocess
import argparse
import json
import glob
import logging
import shutil
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)

# Get script name dynamically for resume messages
SCRIPT_NAME = os.path.basename(__file__)

# Import FunnelTracker
from funnel_tracker import FunnelTracker


# ============================================================================
# CONFIGURATION
# ============================================================================

# Default configuration
DEFAULT_INTERVALS = ["1h"] # Support multiple intervals
# DEFAULT_SYMBOLS = ["AAPL","AMZN","BITO","DIS","GLD","IBM","PFE","PLTR","TECL"] # Support multiple symbols
DEFAULT_SYMBOLS = ["NVDA","AAPL","MSFT","AMZN","GOOGL","GOOG","AVGO","META","TSLA","BRK","LLY","JPM","WMT","V","ORCL","XOM","MA","JNJ","NFLX","PLTR","ABBV","COST","AMD","BAC","HD","PG","GE","CVX","CSCO","KO","UNH","IBM","MU","WFC","MS","CAT","AXP","TMUS","PM","GS","RTX","CRM","MRK","ABT","TMO","MCD","PEP","LIN","ISRG","DIS","UBER","APP","QCOM","LRCX","INTU","AMGN","T","AMAT","C","NOW","NEE","VZ","INTC","SCHW","ANET","APH","BLK","BKNG","TJX","GEV","DHR","GILD","BSX","ACN","SPGI","KLAC","BA","TXN","PFE","PANW","ADBE","SYK","ETN","CRWD","COF","UNP","WELL","PGR","DE","LOW","HON","MDT","CB","ADI","PLD","COP","VRTX","HOOD","BX","LMT","KKR","HCA","CEG","PH","MCK","CME","ADP","SO","CMCSA","CVS","MO","SBUX","NEM","DUK","BMY","NKE","GD","TT","DELL","DASH","MMC","MMM","ICE","AMT","CDNS","MCO","WM","ORLY","SHW","HWM","UPS","NOC","JCI","BK","EQIX","MAR","COIN","APO","CTAS","TDG","AON","ABNB","WMB","USB","ECL","MDLZ","REGN","SNPS","ELV","PNC","CI","EMR","ITW","GLW","TEL","COR","MNST","RCL","AJG","GM","AEP","CSX","DDOG","PWR","CMI","AZO","TRV","RSG","ADSK","NSC","FDX","MSI","CL","HLT","WDAY","FTNT","KMI","MPC","EOG","SRE","AFL","SPG","VST","PYPL","FCX","APD","PSX","TFC","WBD","ALL","SLB","VLO","BDX","STX","IDXX","LHX","DLR","WDC","URI","ZTS","F","O","ROST","MET","D","PCAR","EA","EW","PSA","NDAQ","NXPI","CAH","ROP","BKR","XEL","EXC","FAST","CARR","CBRE","CTVA","AME","OKE","KR","MPWR","LVS","GWW","AXON","TTWO","ETR","AMP","MSCI","ROK","OXY","AIG","FANG","DHI","CMG","A","YUM","FICO","PEG","TGT","CCI","PAYX","CPRT","DAL","EBAY","PRU","IQV","EQT","GRMN","HIG","VMC","KDP","XYZ","VTR","ED","HSY","TRGP","PCG","SYY","MLM","WEC","RMD","CTSH","WAB","XYL","OTIS","KMB","CCL","FISV","NUE","FIS","ACGL","GEHC","STT","VICI","EXPE","KVUE","EL","NRG","LYV","RJF","LEN","UAL","WTW","KEYS","HPE","VRSK","IR","KHC","IBKR","TSCO","WRB","DTE","K","MCHP","CSGP","MTB","MTD","HUM","AEE","ADM","EXR","ROL","FITB","EXE","ATO","ES","EME","ODFL","BRO","PPL","FSLR","CBOE","IRM","TER","FE","BR","SYF","CHTR","CNP","AWK","AVB","EFX","CINF","GIS","DOV","LDOS","STE","HBAN","BIIB","NTRS","VLTO","ULTA","TDY","VRSN","PODD","TPL","HUBB","PHM","DG","HAL","HPQ","STLD","DXCM","WAT","EQR","EIX","STZ","CMS","DVN","CFG","WSM","PTC","TROW","LH","NTAP","SMCI","JBL","PPG","RF","L","DLTR","SBAC","DGX","TPR","INCY","TTD","NI","DRI","CHD","LULU","TYL","RL","CTRA","NVR","IP","AMCR","CPAY","KEY","ON","TSN","CDW","WST","BG","PFG","TRMB","J","EXPD","CHRW","SW","CNC","Q","ZBH","PKG","GPC","GPN","EVRG","GDDY","INVH","MKC","LNT","PSKY","IFF","SNA","PNR","APTV","LUV","IT","GEN","LII","DD","HOLX","ESS","FTV","DOW","WY","BBY","JBHT","MAA","ERIE","LYB","TKO","COO","TXT","UHS","DPZ","OMC","ALLE","KIM","FOX","EG","FOXA","ALB","FFIV","AVY","CF","BF.B","SOLV","NDSN","BALL","REG","CLX","AKAM","MAS","WYNN","HRL","IEX","VTRS","HII","ZBRA","DOC","HST","DECK","JKHY","SJM","BEN","BLDR","UDR","AIZ","BXP","DAY","CPT","HAS","PNW","RVTY","GL","IVZ","FDS","SWK","EPAM","SWKS","ALGN","AES","NWSA","MRNA","IPG","BAX","CPB","TECH","TAP","PAYC","ARE","AOS","POOL","GNRC","MGM","APA","DVA","HSIC","FRT","NCLH","CAG","MOS","CRL","LW","LKQ","MTCH","MOH","SOLS","MHK","NWS"] # Support multiple symbols
DEFAULT_VARIANTS = None  # Auto-discover if None. ["linear","28Dec2025"] to process files with named suffix 

# Exchange
EXCHANGE = "ibkr"

# Stage definitions
STAGES = {
    1: {
        "name": "Alpha Compilation",
        "script": "03A_compile_ibkr_gs_alphas.py",
        "input_pattern": "merged_{exchange}_{symbol}_{interval}_*",
        "output_pattern": "AQS_SFGridResults/Alpha_GS_Compilation_{exchange}_{interval}_*.xlsx",
        "output_dir": "AQS_SFGridResults",
        "description": "Extract best configurations per strategy from grid search results"
    },
    2: {
        "name": "Walk-Forward Validation",
        "script": "03B_validate_ibkr_gs_walk_forward.py",
        "input_pattern": "Alpha_GS_Compilation_{exchange}_{interval}_*.xlsx",
        "output_pattern": "walk_forward_report.csv",  # In each strategy folder
        "output_dir": "AQS_SFGridResults/merged_{exchange}_{symbol}_{interval}_*/",
        "description": "Perform walk-forward validation on top configurations"
    },
    3: {
        "name": "WF Results Compilation",
        "script": "03C_compile_ibkr_gs_walk_forward_results.py",
        "input_pattern": "walk_forward_report.csv",
        "output_pattern": "AQS_SFGridResults/WF_GS_Compilation_{exchange}_{interval}_*.xlsx",
        "output_dir": "AQS_SFGridResults",
        "description": "Aggregate WF results and calculate degradation metrics"
    },
    4: {
        "name": "WF Alpha Generation",
        "script": "03D_generate_ibkr_gs_wf_alpha_results.py",
        "input_pattern": "WF_GS_Compilation_{exchange}_{interval}_*.xlsx",
        "output_pattern": "WFAlphaResults/merged_{exchange}_{symbol}_{interval}_*/",
        "output_dir": "WFAlphaResults",
        "description": "Regenerate full backtests for validated strategies"
    },
    5: {
        "name": "WF Alpha Compilation",
        "script": "03E_compile_ibkr_gs_wfalpha_results.py",
        "input_pattern": "WFAlphaResults/merged_{exchange}_{symbol}_{interval}_*/",
        "output_pattern": "WFAlpha_Compilation_{exchange}_{interval}_*.xlsx",
        "output_dir": ".",
        "description": "Compile WFAlpha results into reference file"
    },
    6: {
        "name": "Combination Strategies",
        "script": "03F_compile_ibkr_gs_combination_strategies.py",
        "input_pattern": "WFAlpha_Compilation_{exchange}_{interval}_*.xlsx",
        "output_pattern": "Combination_Strategy_Compilation_{exchange}_{interval}_*.xlsx",
        "output_dir": ".",
        "description": "Portfolio construction with correlation optimization"
    },
    7: {
        "name": "Final Compilation",
        "script": "03G_generate_ibkr_gs_final_compilation.py",
        "input_pattern": "Combination_Strategy_Compilation_{exchange}_{interval}_*.xlsx",
        "output_pattern": "WFAlphaResults/Final_Compilation_{exchange}_{interval}_*.xlsx",
        "output_dir": "WFAlphaResults",
        "description": "Final portfolios with OUTER merge"
    }
}

# Stage execution modes
# - 'consolidated': Run once with all symbols (produces single cross-symbol output)
# - 'parallel': Run once per symbol in parallel (produces per-symbol outputs)
STAGE_EXECUTION_MODES = {
    1: 'consolidated',  # Alpha Compilation - cross-symbol Excel
    2: 'parallel',      # Walk-Forward Validation - per-symbol processing
    3: 'consolidated',  # WF Results Compilation - cross-symbol Excel
    4: 'parallel',      # WF Alpha Generation - per-symbol backtest generation
    5: 'consolidated',  # WF Alpha Compilation - cross-symbol Excel
    6: 'consolidated',  # Combination Strategies - cross-symbol portfolio
    7: 'consolidated',  # Final Compilation - cross-symbol portfolio
}

# Logging configuration
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


# ============================================================================
# PIPELINE CONFIGURATION CLASS
# ============================================================================

class PipelineConfig:
    """Centralized pipeline configuration management."""

    def __init__(self,
                 intervals: List[str] = None,
                 symbols: List[str] = None,
                 variants: Optional[List[str]] = None,
                 start_stage: int = 1,
                 end_stage: int = 7,
                #  max_workers: int = None,
                 max_workers: int = 1,
                 resume_checkpoint: str = None,
                 output_base_dir: str = None,
                 min_sharpe_ratio: float = 1.0):
        """
        Initialize pipeline configuration.

        Args:
            intervals: List of intervals to process
            symbols: List of symbols to process
            variants: List of variants to process (None = auto-discover)
            start_stage: Starting stage number
            end_stage: Ending stage number
            max_workers: Max parallel workers (None = len(symbols))
            resume_checkpoint: Path to checkpoint file to resume from
            output_base_dir: Base directory for run outputs
            min_sharpe_ratio: Minimum Sharpe Ratio threshold for filtering (default: 1.0)
        """
        self.intervals = intervals or DEFAULT_INTERVALS
        self.symbols = symbols or DEFAULT_SYMBOLS
        self.variants = variants or DEFAULT_VARIANTS
        self.start_stage = start_stage
        self.end_stage = end_stage
        self.max_workers = max_workers or len(self.symbols)
        self.resume_checkpoint = resume_checkpoint
        self.min_sharpe_ratio = min_sharpe_ratio

        # Create timestamped run directory
        if output_base_dir is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.run_dir = f"pipeline_runs/run_{timestamp}"
        else:
            self.run_dir = output_base_dir

        os.makedirs(self.run_dir, exist_ok=True)

        # Checkpoint file
        self.checkpoint_file = os.path.join(self.run_dir, "checkpoint.json")

        # Log file
        self.log_file = os.path.join(self.run_dir, "pipeline.log")

        # Execution date (for filename matching)
        self.execution_date = datetime.now().strftime('%Y%m%d')

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary."""
        return {
            'intervals': self.intervals,
            'symbols': self.symbols,
            'variants': self.variants,
            'start_stage': self.start_stage,
            'end_stage': self.end_stage,
            'max_workers': self.max_workers,
            'run_dir': self.run_dir,
            'execution_date': self.execution_date,
            'min_sharpe_ratio': self.min_sharpe_ratio,
        }


# ============================================================================
# ENVIRONMENT VALIDATION
# ============================================================================

def validate_environment(config: PipelineConfig, logger: logging.Logger) -> Tuple[bool, List[str]]:
    """
    Validate environment before pipeline execution.

    Args:
        config: Pipeline configuration
        logger: Logger instance

    Returns:
        Tuple of (all_valid, warnings_list)
    """
    warnings = []

    logger.info("Validating environment...")

    # 1. Check Python version
    if sys.version_info < (3, 7):
        warnings.append(f"Python version {sys.version_info.major}.{sys.version_info.minor} may be incompatible (recommend 3.7+)")

    # 2. Check required modules
    required_modules = ['pandas', 'numpy', 'openpyxl', 'matplotlib']
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            warnings.append(f"Required module '{module}' not found")

    # 3. Check all stage scripts exist
    for stage_num, stage_info in STAGES.items():
        script_path = stage_info['script']
        if not os.path.exists(script_path):
            warnings.append(f"Stage {stage_num} script not found: {script_path}")

    # 4. Check disk space (warn if < 1GB)
    try:
        import shutil
        total, used, free = shutil.disk_usage(".")
        free_gb = free / (1024**3)
        if free_gb < 1.0:
            warnings.append(f"Low disk space: {free_gb:.2f} GB available")
    except Exception as e:
        warnings.append(f"Could not check disk space: {e}")

    # 5. Check AQS_SFGridResults directory exists
    if not os.path.exists("AQS_SFGridResults"):
        warnings.append("AQS_SFGridResults directory not found")

    # 6. Check GridSearch_Data directory exists
    if not os.path.exists("GridSearch_Data"):
        warnings.append("GridSearch_Data directory not found")

    # 7. Check for expected input folders (Stage 1)
    for symbol in config.symbols:
        for interval in config.intervals:
            pattern = f"AQS_SFGridResults/merged_{EXCHANGE}_{symbol}_{interval}_*"
            matches = glob.glob(pattern)
            if not matches:
                warnings.append(f"No input folders found for {symbol} {interval}: {pattern}")

    if warnings:
        logger.warning(f"Environment validation found {len(warnings)} warnings:")
        for warning in warnings:
            logger.warning(f"  - {warning}")
        return False, warnings
    else:
        logger.info("Environment validation passed.")
        return True, []


# ============================================================================
# FILE DISCOVERY UTILITIES
# ============================================================================

def find_latest_file(pattern: str, logger: logging.Logger) -> Optional[str]:
    """
    Find most recent file matching glob pattern.

    Args:
        pattern: Glob pattern
        logger: Logger instance

    Returns:
        Path to most recent file, or None if not found
    """
    matches = glob.glob(pattern)
    if not matches:
        logger.warning(f"No files found matching pattern: {pattern}")
        return None

    # Sort by modification time, most recent first
    matches.sort(key=os.path.getmtime, reverse=True)
    latest = matches[0]
    logger.debug(f"Found latest file: {latest}")
    return latest


def discover_variants(symbol: str, interval: str, logger: logging.Logger) -> List[str]:
    """
    Auto-discover variants for a symbol/interval combination.

    Args:
        symbol: Symbol name
        interval: Interval
        logger: Logger instance

    Returns:
        List of variant names (e.g., ['linear', 'nonlinear'])
    """
    pattern = f"AQS_SFGridResults/merged_{EXCHANGE}_{symbol}_{interval}_*"
    matches = glob.glob(pattern)

    if not matches:
        logger.warning(f"No folders found for variant discovery: {pattern}")
        return []

    # Extract variant from folder name
    # e.g., merged_ibkr_MBT_1h_linear -> 'linear'
    variants = []
    for match in matches:
        folder_name = os.path.basename(match)
        parts = folder_name.split('_')
        if len(parts) >= 5:
            variant = '_'.join(parts[4:])  # Everything after interval
            if variant and variant not in variants:
                variants.append(variant)

    logger.info(f"Discovered variants for {symbol} {interval}: {variants}")
    return variants


# ============================================================================
# STAGE EXECUTION
# ============================================================================

def run_stage_for_symbol(stage_num: int,
                        symbol: str,
                        interval: str,
                        config: PipelineConfig,
                        logger: logging.Logger) -> Dict[str, Any]:
    """
    Execute a single stage for one symbol/interval.

    Args:
        stage_num: Stage number (1-7)
        symbol: Symbol name
        interval: Interval
        config: Pipeline configuration
        logger: Logger instance

    Returns:
        Result dictionary with status, elapsed_time, error
    """
    stage_info = STAGES[stage_num]
    script_path = stage_info['script']

    start_time = time.time()

    logger.info(f"[Stage {stage_num}] [{interval}] [{symbol}] Starting: {stage_info['name']}")
    print(f"\n{'='*80}")
    print(f"[Stage {stage_num}] [{interval}] [{symbol}] Executing: {script_path}")
    print(f"{'='*80}\n")

    try:
        # Set environment variable for date synchronization
        env = os.environ.copy()
        env['PIPELINE_EXECUTION_DATE'] = config.execution_date

        # Build command with arguments
        cmd = [sys.executable, script_path]
        cmd.extend(['--symbols', symbol])
        cmd.extend(['--interval', interval])
        cmd.extend(['--exchange', EXCHANGE])
        if stage_num in [1, 3]:  # Only Stages 1 and 3 use Sharpe filtering
            cmd.extend(['--min-sharpe-ratio', str(config.min_sharpe_ratio)])

        # Run script as subprocess with real-time output streaming
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            bufsize=1,
            universal_newlines=True
        )

        # Stream output in real-time
        output_lines = []
        for line in process.stdout:
            print(line, end='')  # Print to console in real-time
            output_lines.append(line)

        # Wait for process to complete
        return_code = process.wait(timeout=3600)
        elapsed_time = time.time() - start_time

        if return_code == 0:
            logger.info(f"[Stage {stage_num}] [{interval}] [{symbol}] Completed in {elapsed_time:.1f}s")
            print(f"\n{'='*80}")
            print(f"[Stage {stage_num}] [{interval}] [{symbol}] SUCCESS - Completed in {elapsed_time:.1f}s")
            print(f"{'='*80}\n")
            return {
                'status': 'success',
                'symbol': symbol,
                'interval': interval,
                'stage_num': stage_num,
                'elapsed_time': elapsed_time,
                'error': None
            }
        else:
            error_msg = ''.join(output_lines[-10:]) if output_lines else "Unknown error"
            logger.error(f"[Stage {stage_num}] [{interval}] [{symbol}] Failed with code {return_code}")
            print(f"\n{'='*80}")
            print(f"[Stage {stage_num}] [{interval}] [{symbol}] FAILED with return code {return_code}")
            print(f"{'='*80}\n")
            return {
                'status': 'failed',
                'symbol': symbol,
                'interval': interval,
                'stage_num': stage_num,
                'elapsed_time': elapsed_time,
                'error': error_msg[:500]
            }

    except subprocess.TimeoutExpired:
        elapsed_time = time.time() - start_time
        error_msg = f"Stage timeout after {elapsed_time:.1f}s"
        logger.error(f"[Stage {stage_num}] [{interval}] [{symbol}] {error_msg}")
        print(f"\n{'='*80}")
        print(f"[Stage {stage_num}] [{interval}] [{symbol}] TIMEOUT after {elapsed_time:.1f}s")
        print(f"{'='*80}\n")
        try:
            process.kill()
        except:
            pass
        return {
            'status': 'timeout',
            'symbol': symbol,
            'interval': interval,
            'stage_num': stage_num,
            'elapsed_time': elapsed_time,
            'error': error_msg
        }

    except Exception as e:
        elapsed_time = time.time() - start_time
        error_msg = str(e)
        logger.error(f"[Stage {stage_num}] [{interval}] [{symbol}] Exception: {error_msg}")
        print(f"\n{'='*80}")
        print(f"[Stage {stage_num}] [{interval}] [{symbol}] EXCEPTION: {error_msg}")
        print(f"{'='*80}\n")
        return {
            'status': 'exception',
            'symbol': symbol,
            'interval': interval,
            'stage_num': stage_num,
            'elapsed_time': elapsed_time,
            'error': error_msg
        }


def run_stage_for_all_symbols(stage_num: int,
                               symbols: List[str],
                               interval: str,
                               config: PipelineConfig,
                               logger: logging.Logger) -> Dict[str, Any]:
    """
    Execute a consolidated stage for ALL symbols at once.
    Used for stages that produce cross-symbol compilation outputs.

    Args:
        stage_num: Stage number (1-7)
        symbols: List of all symbols to process
        interval: Interval
        config: Pipeline configuration
        logger: Logger instance

    Returns:
        Result dictionary with status, elapsed_time, error
    """
    stage_info = STAGES[stage_num]
    script_path = stage_info['script']

    start_time = time.time()

    # Format symbols for logging
    symbols_str = ', '.join(symbols)

    logger.info(f"[Stage {stage_num}] [{interval}] [ALL SYMBOLS: {symbols_str}] Starting: {stage_info['name']}")
    print(f"\n{'='*80}")
    print(f"[Stage {stage_num}] [{interval}] [ALL SYMBOLS: {symbols_str}]")
    print(f"Executing: {script_path}")
    print(f"{'='*80}\n")

    try:
        # Set environment variable for date synchronization
        env = os.environ.copy()
        env['PIPELINE_EXECUTION_DATE'] = config.execution_date

        # Build command with ALL symbols as comma-separated list
        cmd = [sys.executable, script_path]
        cmd.extend(['--symbols', ','.join(symbols)])  # KEY FIX: comma-separated list
        cmd.extend(['--interval', interval])
        cmd.extend(['--exchange', EXCHANGE])
        if stage_num in [1, 3]:  # Only Stages 1 and 3 use Sharpe filtering
            cmd.extend(['--min-sharpe-ratio', str(config.min_sharpe_ratio)])

        # Run script as subprocess with real-time output streaming
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            bufsize=1,
            universal_newlines=True
        )

        # Stream output in real-time
        output_lines = []
        for line in process.stdout:
            print(line, end='')  # Print to console in real-time
            output_lines.append(line)

        # Wait for process to complete
        return_code = process.wait(timeout=3600)
        elapsed_time = time.time() - start_time

        if return_code == 0:
            logger.info(f"[Stage {stage_num}] [{interval}] [ALL SYMBOLS] Completed in {elapsed_time:.1f}s")
            print(f"\n{'='*80}")
            print(f"[Stage {stage_num}] [{interval}] [ALL SYMBOLS] SUCCESS - Completed in {elapsed_time:.1f}s")
            print(f"{'='*80}\n")
            return {
                'status': 'success',
                'symbol': 'ALL',  # Special marker for consolidated execution
                'symbols': symbols,  # Full list for tracking
                'interval': interval,
                'stage_num': stage_num,
                'elapsed_time': elapsed_time,
                'error': None
            }
        else:
            error_msg = ''.join(output_lines[-10:]) if output_lines else "Unknown error"
            logger.error(f"[Stage {stage_num}] [{interval}] [ALL SYMBOLS] Failed with code {return_code}")
            print(f"\n{'='*80}")
            print(f"[Stage {stage_num}] [{interval}] [ALL SYMBOLS] FAILED with return code {return_code}")
            print(f"{'='*80}\n")
            return {
                'status': 'failed',
                'symbol': 'ALL',
                'symbols': symbols,
                'interval': interval,
                'stage_num': stage_num,
                'elapsed_time': elapsed_time,
                'error': error_msg[:500]
            }

    except subprocess.TimeoutExpired:
        elapsed_time = time.time() - start_time
        error_msg = f"Stage timeout after {elapsed_time:.1f}s"
        logger.error(f"[Stage {stage_num}] [{interval}] [ALL SYMBOLS] {error_msg}")
        print(f"\n{'='*80}")
        print(f"[Stage {stage_num}] [{interval}] [ALL SYMBOLS] TIMEOUT after {elapsed_time:.1f}s")
        print(f"{'='*80}\n")
        try:
            process.kill()
        except:
            pass
        return {
            'status': 'timeout',
            'symbol': 'ALL',
            'symbols': symbols,
            'interval': interval,
            'stage_num': stage_num,
            'elapsed_time': elapsed_time,
            'error': error_msg
        }

    except Exception as e:
        elapsed_time = time.time() - start_time
        error_msg = str(e)
        logger.error(f"[Stage {stage_num}] [{interval}] [ALL SYMBOLS] Exception: {error_msg}")
        print(f"\n{'='*80}")
        print(f"[Stage {stage_num}] [{interval}] [ALL SYMBOLS] EXCEPTION: {error_msg}")
        print(f"{'='*80}\n")
        return {
            'status': 'exception',
            'symbol': 'ALL',
            'symbols': symbols,
            'interval': interval,
            'stage_num': stage_num,
            'elapsed_time': elapsed_time,
            'error': error_msg
        }


def get_excel_row_count(excel_file: str, worksheet: str, logger: logging.Logger) -> int:
    """
    Count total data rows in an Excel worksheet.
    Handles split sheets (e.g., Alpha Full_Compilation_1, _2) by summing all matching sheets.

    Args:
        excel_file: Path to Excel file
        worksheet: Worksheet name (base name, will also match numbered suffixes)
        logger: Logger instance

    Returns:
        Row count (0 if file/worksheet not found)
    """
    try:
        if os.path.exists(excel_file):
            xl = pd.ExcelFile(excel_file)
            # Find matching sheets (exact or split: worksheet_1, worksheet_2, etc.)
            matching_sheets = [s for s in xl.sheet_names if s == worksheet or s.startswith(f"{worksheet}_")]

            if not matching_sheets:
                logger.warning(f"No sheets matching '{worksheet}' in {excel_file}")
                return 0

            total_rows = 0
            for sheet in matching_sheets:
                df = pd.read_excel(excel_file, sheet_name=sheet)
                total_rows += len(df)
            return total_rows
        return 0
    except Exception as e:
        logger.warning(f"Failed to count rows in {excel_file}/{worksheet}: {e}")
        return 0


def find_excel_file(pattern: str, logger: logging.Logger) -> Optional[str]:
    """
    Find most recent Excel file matching a glob pattern.

    For multi-day pipeline runs, files may be created with different dates
    than the pipeline start date. This function finds the most recently
    modified file matching the pattern.

    Args:
        pattern: Glob pattern (e.g., 'WFAlphaResults/Final_Compilation_ibkr_1h_*.xlsx')
        logger: Logger instance

    Returns:
        Path to most recent matching file, or None if no matches
    """
    matches = glob.glob(pattern)
    if not matches:
        logger.debug(f"No files matching pattern: {pattern}")
        return None
    # Return most recently modified file
    result = max(matches, key=os.path.getmtime)
    logger.debug(f"Found {len(matches)} files matching {pattern}, using: {result}")
    return result


def register_excel_outputs(stage_num: int,
                          interval: str,
                          funnel_tracker: FunnelTracker,
                          config: PipelineConfig,
                          logger: logging.Logger):
    """
    Register Excel outputs for a completed stage with FunnelTracker.

    Args:
        stage_num: Stage number (1-7)
        interval: Interval
        funnel_tracker: FunnelTracker instance
        config: Pipeline configuration
        logger: Logger instance
    """
    # Only specific stages produce Excel reports
    excel_stages = {1, 3, 5, 6, 7}

    if stage_num not in excel_stages:
        return

    # Get execution date for filename matching
    today = config.execution_date

    # Stage-specific Excel output registration
    if stage_num == 1:
        # Stage 1: Alpha Compilation
        excel_file = f"AQS_SFGridResults/Alpha_GS_Compilation_{EXCHANGE}_{interval}_{today}.xlsx"
        if os.path.exists(excel_file):
            funnel_tracker.record_excel_output(
                stage_num=1,
                excel_file=excel_file,
                worksheet="Alpha Full_Compilation",
                remarks="Unfiltered (IS Sharpe >= threshold)"
            )
            funnel_tracker.record_excel_output(
                stage_num=1,
                excel_file=excel_file,
                worksheet="Alpha_Short",
                remarks="Filtered (IS & OOS Sharpe >= threshold)"
            )
            # Set data row counts: input = Full_Compilation, output = Alpha_Short
            input_rows = get_excel_row_count(excel_file, "Alpha Full_Compilation", logger)
            output_rows = get_excel_row_count(excel_file, "Alpha_Short", logger)
            funnel_tracker.set_stage_data_rows(1, input_rows, output_rows)
            logger.debug(f"Registered Stage 1 Excel output: {excel_file} (input: {input_rows}, output: {output_rows})")

    elif stage_num == 3:
        # Stage 3: WF Results Compilation - Multiple worksheets
        excel_file = f"AQS_SFGridResults/WF_GS_Compilation_{EXCHANGE}_{interval}_{today}.xlsx"
        if os.path.exists(excel_file):
            funnel_tracker.record_excel_output(
                stage_num=3,
                excel_file=excel_file,
                worksheet="WF_Results",
                remarks="Unfiltered"
            )
            funnel_tracker.record_excel_output(
                stage_num=3,
                excel_file=excel_file,
                worksheet="WF_Filtered",
                remarks="Robust (IS Sharpe >= threshold, L Sharpe Degrade >= -10%)"
            )
            funnel_tracker.record_excel_output(
                stage_num=3,
                excel_file=excel_file,
                worksheet="WF_Short",
                remarks="Top Performers (IS Sharpe >= threshold, L Sharpe Degrade >= -10%)"
            )
            # Set data row counts: input = WF_Results, output = WF_Filtered
            input_rows = get_excel_row_count(excel_file, "WF_Results", logger)
            output_rows = get_excel_row_count(excel_file, "WF_Filtered", logger)
            funnel_tracker.set_stage_data_rows(3, input_rows, output_rows)
            logger.debug(f"Registered Stage 3 Excel output: {excel_file} (input: {input_rows}, output: {output_rows})")

    elif stage_num == 5:
        # Stage 5: WF Alpha Compilation - THIS creates WFAlpha_Compilation file
        # Use glob pattern to find file (handles multi-day pipelines where date may differ)
        pattern = f"WFAlphaResults/WFAlpha_Compilation_{EXCHANGE}_{interval}_*.xlsx"
        excel_file = find_excel_file(pattern, logger)
        if excel_file:
            funnel_tracker.record_excel_output(
                stage_num=5,
                excel_file=excel_file,
                worksheet="WF_Short",
                remarks="Validated (Full Sharpe)"
            )
            logger.info(f"Registered Stage 5 Excel output: {excel_file}")
        else:
            logger.warning(f"Stage 5 Excel file not found matching: {pattern}")

    elif stage_num == 6:
        # Stage 6: Combination Strategies
        # Use glob pattern to find file (handles multi-day pipelines where date may differ)
        pattern = f"WFAlphaResults/Combination_Strategy_Compilation_{EXCHANGE}_{interval}_*.xlsx"
        excel_file = find_excel_file(pattern, logger)
        if excel_file:
            funnel_tracker.record_excel_output(
                stage_num=6,
                excel_file=excel_file,
                worksheet="Corr Summary",
                remarks="Based on 0.7 Threshold"
            )
            logger.debug(f"Registered Stage 6 Excel output: {excel_file}")
        else:
            logger.warning(f"Stage 6 Excel file not found matching: {pattern}")

    elif stage_num == 7:
        # Stage 7: Final Compilation
        # Use glob pattern to find file (handles multi-day pipelines where date may differ)
        pattern = f"WFAlphaResults/Final_Compilation_{EXCHANGE}_{interval}_*.xlsx"
        excel_file = find_excel_file(pattern, logger)
        if excel_file:
            funnel_tracker.record_excel_output(
                stage_num=7,
                excel_file=excel_file,
                worksheet="Corr Summary",
                remarks="Based on 0.5 Threshold"
            )
            logger.debug(f"Registered Stage 7 Excel output: {excel_file}")
        else:
            logger.warning(f"Stage 7 Excel file not found matching: {pattern}")


def run_stage_parallel(stage_num: int,
                      interval: str,
                      config: PipelineConfig,
                      funnel_tracker: FunnelTracker,
                      logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Execute a stage for all symbols (either consolidated or parallel).

    Args:
        stage_num: Stage number (1-7)
        interval: Interval
        config: Pipeline configuration
        funnel_tracker: FunnelTracker instance
        logger: Logger instance

    Returns:
        List of result dictionaries
    """
    stage_info = STAGES[stage_num]

    # Determine execution mode (consolidated vs parallel)
    execution_mode = STAGE_EXECUTION_MODES.get(stage_num, 'parallel')

    # Start stage tracking
    funnel_tracker.start_stage(stage_num, stage_info['name'], stage_info['script'])

    logger.info(f"\n{'='*80}")
    logger.info(f"Stage {stage_num}: {stage_info['name']} - Interval: {interval}")
    logger.info(f"Description: {stage_info['description']}")
    logger.info(f"Execution mode: {execution_mode.upper()}")
    logger.info(f"Processing symbols: {config.symbols}")
    logger.info(f"{'='*80}\n")

    results = []

    if execution_mode == 'consolidated':
        # CONSOLIDATED MODE: Run ONCE with all symbols
        logger.info(f"[Stage {stage_num}] [{interval}] Running in CONSOLIDATED mode - single execution with all {len(config.symbols)} symbols")

        result = run_stage_for_all_symbols(stage_num, config.symbols, interval, config, logger)
        results = [result]

        # Record metrics for consolidated execution
        if result['status'] == 'success':
            # Record runtime for the consolidated execution
            funnel_tracker.record_symbol_metric(
                stage_num, 'ALL_SYMBOLS', 'runtime_seconds', result['elapsed_time']
            )
        else:
            # Record error for consolidated execution
            funnel_tracker.record_error(
                stage_num, 'ALL_SYMBOLS', result['error']
            )

    else:
        # PARALLEL MODE: Run ONCE PER symbol in parallel
        logger.info(f"[Stage {stage_num}] [{interval}] Running in PARALLEL mode - {len(config.symbols)} symbols with max_workers={config.max_workers}")

        with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
            futures = {
                executor.submit(run_stage_for_symbol, stage_num, symbol, interval, config, logger): symbol
                for symbol in config.symbols
            }

            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    result = future.result()
                    results.append(result)

                    # Record metrics in FunnelTracker
                    if result['status'] == 'success':
                        funnel_tracker.record_symbol_metric(
                            stage_num, symbol, 'runtime_seconds', result['elapsed_time']
                        )
                    else:
                        funnel_tracker.record_error(
                            stage_num, symbol, result['error']
                        )

                except Exception as e:
                    logger.error(f"Failed to get result for {symbol}: {e}")
                    results.append({
                        'status': 'exception',
                        'symbol': symbol,
                        'interval': interval,
                        'stage_num': stage_num,
                        'elapsed_time': 0,
                        'error': str(e)
                    })

    # End stage tracking
    funnel_tracker.end_stage(stage_num)

    # Update volume metrics based on execution mode
    input_vol = len(config.symbols)
    if execution_mode == 'consolidated':
        # Consolidated: Input is N symbols, output is 1 consolidated result
        output_vol = 1 if results[0]['status'] == 'success' else 0
    else:
        # Parallel: Input is N symbols, output is count of successful symbols
        output_vol = sum(1 for r in results if r['status'] == 'success')

    funnel_tracker.set_stage_volume(stage_num, input_vol, output_vol)

    # Register Excel outputs for Alpha Summary tracking
    register_excel_outputs(stage_num, interval, funnel_tracker, config, logger)

    # Summary
    success_count = sum(1 for r in results if r['status'] == 'success')
    failed_count = len(results) - success_count

    if execution_mode == 'consolidated':
        logger.info(f"\n[Stage {stage_num}] [{interval}] Summary: Consolidated execution {'SUCCEEDED' if success_count > 0 else 'FAILED'} ({len(config.symbols)} symbols processed)")
    else:
        logger.info(f"\n[Stage {stage_num}] [{interval}] Summary: {success_count} succeeded, {failed_count} failed")

    return results


# ============================================================================
# CHECKPOINT AND RESUME
# ============================================================================

def save_checkpoint(config: PipelineConfig,
                   current_interval: str,
                   current_stage: int,
                   results: List[Dict[str, Any]],
                   logger: logging.Logger):
    """
    Save checkpoint to JSON file.

    Args:
        config: Pipeline configuration
        current_interval: Current interval being processed
        current_stage: Current stage number
        results: All results so far
        logger: Logger instance
    """
    checkpoint_data = {
        'timestamp': datetime.now().isoformat(),
        'config': config.to_dict(),
        'current_interval': current_interval,
        'current_stage': current_stage,
        'results': results,
    }

    with open(config.checkpoint_file, 'w') as f:
        json.dump(checkpoint_data, f, indent=2)

    logger.debug(f"Checkpoint saved: {config.checkpoint_file}")


def load_checkpoint(checkpoint_path: str, logger: logging.Logger) -> Dict[str, Any]:
    """
    Load checkpoint from JSON file.

    Args:
        checkpoint_path: Path to checkpoint file
        logger: Logger instance

    Returns:
        Checkpoint data dictionary
    """
    with open(checkpoint_path, 'r') as f:
        checkpoint_data = json.load(f)

    logger.info(f"Loaded checkpoint from {checkpoint_path}")
    logger.info(f"  Checkpoint timestamp: {checkpoint_data['timestamp']}")
    logger.info(f"  Last completed: Interval {checkpoint_data['current_interval']}, Stage {checkpoint_data['current_stage']}")

    return checkpoint_data


def validate_resume_state(checkpoint_data: Dict[str, Any], logger: logging.Logger) -> bool:
    """
    Validate that outputs from checkpoint still exist.

    Args:
        checkpoint_data: Checkpoint data
        logger: Logger instance

    Returns:
        True if valid, False otherwise
    """
    logger.info("Validating checkpoint state (checking outputs exist)...")

    # For each successful result, verify output files exist
    results = checkpoint_data.get('results', [])
    missing_outputs = []

    for result in results:
        if result['status'] != 'success':
            continue

        stage_num = result['stage_num']
        interval = result['interval']
        symbol = result['symbol']

        # Get expected output pattern
        stage_info = STAGES[stage_num]

        # Handle consolidated stages (symbol == 'ALL')
        if symbol == 'ALL':
            # Consolidated stage - check for cross-symbol output file
            # Use wildcard pattern to find any matching output
            output_pattern = stage_info['output_pattern'].format(
                exchange=EXCHANGE,
                interval=interval,
                symbol='*'
            )
        else:
            # Per-symbol stage - check for symbol-specific output
            output_pattern = stage_info['output_pattern'].format(
                exchange=EXCHANGE,
                interval=interval,
                symbol=symbol
            )

        # Check if file/folder exists
        matches = glob.glob(output_pattern)
        if not matches:
            missing_outputs.append(f"Stage {stage_num} {interval} {symbol}: {output_pattern}")

    if missing_outputs:
        logger.warning(f"Found {len(missing_outputs)} missing outputs from checkpoint:")
        for missing in missing_outputs[:10]:  # Show first 10
            logger.warning(f"  - {missing}")
        if len(missing_outputs) > 10:
            logger.warning(f"  ... and {len(missing_outputs) - 10} more")
        logger.warning("Proceeding with resume (outputs may be regenerated)")
    else:
        logger.info("All checkpoint outputs validated.")

    return True


# ============================================================================
# OUTPUT MANAGEMENT
# ============================================================================

def copy_outputs_to_run_dir(config: PipelineConfig,
                           interval: str,
                           stage_num: int,
                           logger: logging.Logger):
    """
    Copy stage outputs to timestamped run directory.

    Args:
        config: Pipeline configuration
        interval: Interval
        stage_num: Stage number
        logger: Logger instance
    """
    stage_info = STAGES[stage_num]
    output_pattern = stage_info['output_pattern'].format(
        exchange=EXCHANGE,
        interval=interval,
        symbol='*'  # Wildcard for all symbols
    )

    # Find output files
    matches = glob.glob(output_pattern)

    if not matches:
        logger.warning(f"No outputs found to copy for Stage {stage_num} {interval}: {output_pattern}")
        return

    # Create stage directory in run_dir
    stage_dir = os.path.join(config.run_dir, f"stage{stage_num}_{interval}")
    os.makedirs(stage_dir, exist_ok=True)

    # Copy files
    for match in matches:
        try:
            if os.path.isfile(match):
                shutil.copy2(match, stage_dir)
                logger.debug(f"Copied: {match} -> {stage_dir}")
            elif os.path.isdir(match):
                dest = os.path.join(stage_dir, os.path.basename(match))
                shutil.copytree(match, dest, dirs_exist_ok=True)
                logger.debug(f"Copied directory: {match} -> {dest}")
        except Exception as e:
            logger.error(f"Failed to copy {match}: {e}")

    logger.info(f"Copied {len(matches)} outputs to {stage_dir}")


# ============================================================================
# MAIN PIPELINE EXECUTION
# ============================================================================

def run_pipeline(config: PipelineConfig, logger: logging.Logger):
    """
    Execute the full pipeline.

    Args:
        config: Pipeline configuration
        logger: Logger instance
    """
    logger.info(f"\n{'#'*80}")
    logger.info(f"# IBKR PIPELINE EXECUTION")
    logger.info(f"{'#'*80}")
    logger.info(f"Run Directory: {config.run_dir}")
    logger.info(f"Intervals: {config.intervals}")
    logger.info(f"Symbols: {config.symbols}")
    logger.info(f"Variants: {config.variants if config.variants else 'Auto-discover'}")
    logger.info(f"Stages: {config.start_stage} to {config.end_stage}")
    logger.info(f"Max Workers: {config.max_workers}")
    logger.info(f"{'#'*80}\n")

    # Environment validation
    validation_passed, warnings = validate_environment(config, logger)
    if not validation_passed:
        logger.warning("Continuing despite validation warnings...")

    # Initialize FunnelTracker
    funnel_config = {
        'pipeline_name': 'IBKR Pipeline',
        'intervals': config.intervals,
        'symbols': config.symbols,
    }
    funnel_tracker = FunnelTracker(funnel_config)

    # Track all results
    all_results = []

    # Main execution loop: Sequential Interval, Parallel Symbol
    try:
        for interval in config.intervals:
            logger.info(f"\n{'='*80}")
            logger.info(f"PROCESSING INTERVAL: {interval}")
            logger.info(f"{'='*80}\n")

            for stage_num in range(config.start_stage, config.end_stage + 1):
                # Run stage in parallel across symbols
                results = run_stage_parallel(
                    stage_num,
                    interval,
                    config,
                    funnel_tracker,
                    logger
                )

                all_results.extend(results)

                # Save checkpoint after each stage
                save_checkpoint(config, interval, stage_num, all_results, logger)

                # Copy outputs to run directory
                copy_outputs_to_run_dir(config, interval, stage_num, logger)

                # Check for critical failures (all symbols failed)
                success_count = sum(1 for r in results if r['status'] == 'success')
                if success_count == 0:
                    logger.error(f"All symbols failed in Stage {stage_num} for {interval}")
                    logger.error("Stopping pipeline execution due to complete stage failure")
                    raise RuntimeError(f"Stage {stage_num} complete failure for {interval}")

            logger.info(f"\n{'='*80}")
            logger.info(f"COMPLETED INTERVAL: {interval}")
            logger.info(f"{'='*80}\n")

        # Finalize tracking
        funnel_tracker.finalize()

        # Generate funnel report
        report_path = os.path.join(config.run_dir, "funnel_report.xlsx")
        funnel_tracker.generate_report(report_path)

        # Final summary
        logger.info(f"\n{'#'*80}")
        logger.info(f"# PIPELINE EXECUTION COMPLETED")
        logger.info(f"{'#'*80}")

        total_tasks = len(all_results)
        success_tasks = sum(1 for r in all_results if r['status'] == 'success')
        failed_tasks = total_tasks - success_tasks

        logger.info(f"Total Tasks: {total_tasks}")
        logger.info(f"  Successful: {success_tasks}")
        logger.info(f"  Failed: {failed_tasks}")
        logger.info(f"  Success Rate: {success_tasks/total_tasks*100:.1f}%")
        logger.info(f"\nOutputs saved to: {config.run_dir}")
        logger.info(f"Funnel report: {report_path}")
        logger.info(f"{'#'*80}\n")

    except KeyboardInterrupt:
        logger.warning("\n\nPipeline interrupted by user (Ctrl+C)")
        logger.info(f"Checkpoint saved: {config.checkpoint_file}")
        logger.info(f"Resume with: python {SCRIPT_NAME} --resume {config.checkpoint_file}")
        sys.exit(1)

    except Exception as e:
        logger.error(f"\n\nPipeline failed with exception: {e}")
        logger.info(f"Checkpoint saved: {config.checkpoint_file}")
        logger.info(f"Resume with: python {SCRIPT_NAME} --resume {config.checkpoint_file}")
        raise


# ============================================================================
# CLI INTERFACE
# ============================================================================

def setup_logging(log_file: str, verbose: bool = False) -> logging.Logger:
    """
    Setup logging to both file and console.

    Args:
        log_file: Path to log file
        verbose: Enable verbose (DEBUG) logging

    Returns:
        Logger instance
    """
    logger = logging.getLogger('ibkr_pipeline')
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # File handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


def interactive_mode():
    """Interactive CLI for running the IBKR pipeline"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║           IBKR Pipeline Orchestrator - Interactive Mode      ║
║                  7-Stage Strategy Workflow                   ║
╚══════════════════════════════════════════════════════════════╝
    """)

    # Display stage information
    print("Pipeline Stages:")
    for num, info in STAGES.items():
        print(f"  {num}. {info['name']}")
    print()

    try:
        # 1. Symbols input (required)
        print("Please provide the following information:\n")
        symbols_input = input("Enter symbols (comma-separated, e.g., NVDA,AAPL): ").strip()
        if not symbols_input:
            print("Symbols cannot be empty")
            return
        symbols = [s.strip().upper() for s in symbols_input.split(',')]

        # 2. Interval input
        print("\nAvailable intervals:")
        print("  1min, 5min, 15min, 30min, 1h, 1d, 1w")
        interval = input("\nEnter interval (default: 1h): ").strip().lower()
        if not interval or interval not in ['1min', '5min', '15min', '30min', '1h', '4h', '1d', '1w']:
            interval = '1h'
            print("  ✗ Invalid interval. Using default: 1h")
        intervals = [interval]

        # 3. Stage selection
        run_all = input("\nRun all stages 1-7? (y/n, default: y): ").strip().lower()
        if run_all == 'n' or run_all == 'no':
            start_stage = input("Enter starting stage (1-7): ").strip()
            start_stage = int(start_stage) if start_stage.isdigit() and 1 <= int(start_stage) <= 7 else 1
        else:
            start_stage = 1
        end_stage = 7

        # Configuration summary
        print(f"\n{'='*60}")
        print("Configuration Summary:")
        print(f"  Symbols:     {', '.join(symbols)}")
        print(f"  Interval:    {interval}")
        print(f"  Stages:      {start_stage} -> {end_stage}")
        print(f"{'='*60}")

        # Confirmation
        proceed = input("\nProceed with pipeline execution? (y/n): ").strip().lower()
        if proceed != 'y':
            print("\nPipeline execution cancelled")
            return

        # Create configuration and run
        config = PipelineConfig(
            intervals=intervals,
            symbols=symbols,
            start_stage=start_stage,
            end_stage=end_stage,
            max_workers=1
        )

        logger = setup_logging(config.log_file, verbose=False)

        print(f"\n{'='*60}")
        print("Starting pipeline execution...")
        print(f"  Run directory: {config.run_dir}")
        print(f"{'='*60}\n")

        run_pipeline(config, logger)

        print(f"\n{'='*60}")
        print("SUCCESS! Pipeline execution completed.")
        print(f"{'='*60}")
        print(f"\nOutputs saved to: {config.run_dir}")
        print("\nNext steps:")
        print("  1. Review the funnel_report.xlsx")
        print("  2. Check Final_Compilation for portfolio strategies")
        print("  3. Deploy selected strategies via ibkr_deployment/")

    except KeyboardInterrupt:
        print("\n\nPipeline cancelled by user")
    except Exception as e:
        print(f"\nPipeline error: {e}")
        raise


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description='IBKR Pipeline Orchestrator - Automate 7-stage trading strategy workflow',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline with defaults
  python run_ibkr_gs_pipeline.py

  # Run specific intervals and symbols
  python run_ibkr_gs_pipeline.py --intervals 1h 4h --symbols MBT MET

  # Run partial pipeline (stages 3-7)
  python run_ibkr_gs_pipeline.py --start-stage 3

  # Resume from checkpoint
  python run_ibkr_gs_pipeline.py --resume checkpoint_20250120_143022.json

  # Run with custom output directory
  python run_ibkr_gs_pipeline.py --output-dir custom_run_20250120

  # Verbose logging
  python run_ibkr_gs_pipeline.py --verbose
        """
    )

    parser.add_argument('--intervals', nargs='+', default=None,
                       help=f'Intervals to process (default: {DEFAULT_INTERVALS})')
    parser.add_argument('--symbols', nargs='+', default=None,
                       help=f'Symbols to process (default: {DEFAULT_SYMBOLS})')
    parser.add_argument('--variants', nargs='+', default=None,
                       help='Variants to process (default: auto-discover)')
    parser.add_argument('--start-stage', type=int, default=1,
                       help='Starting stage number (default: 1)')
    parser.add_argument('--end-stage', type=int, default=7,
                       help='Ending stage number (default: 7)')
    parser.add_argument('--max-workers', type=int, default=None,
                       help='Max parallel workers (default: number of symbols)')
    parser.add_argument('--min-sharpe-ratio', type=float, default=1.0,
                       help='Minimum Sharpe Ratio threshold for filtering (default: 1.0)')
    parser.add_argument('--output-dir', type=str, default=None,
                       help='Custom output directory (default: timestamped run directory)')
    parser.add_argument('--resume', type=str, default=None,
                       help='Resume from checkpoint file')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose (DEBUG) logging')

    args = parser.parse_args()

    # Create configuration
    config = PipelineConfig(
        intervals=args.intervals,
        symbols=args.symbols,
        variants=args.variants,
        start_stage=args.start_stage,
        end_stage=args.end_stage,
        max_workers=args.max_workers,
        output_base_dir=args.output_dir,
        resume_checkpoint=args.resume,
        min_sharpe_ratio=args.min_sharpe_ratio
    )

    # Setup logging
    logger = setup_logging(config.log_file, args.verbose)

    # Resume handling
    if args.resume:
        logger.info(f"Resume mode: Loading checkpoint from {args.resume}")
        checkpoint_data = load_checkpoint(args.resume, logger)

        # Validate resume state
        if not validate_resume_state(checkpoint_data, logger):
            logger.error("Checkpoint validation failed. Aborting resume.")
            sys.exit(1)

        # Update config from checkpoint
        checkpoint_config = checkpoint_data['config']
        config.intervals = checkpoint_config['intervals']
        config.symbols = checkpoint_config['symbols']
        config.variants = checkpoint_config['variants']
        config.min_sharpe_ratio = checkpoint_config.get('min_sharpe_ratio', 1.0)

        # Resume from next stage/interval
        current_interval = checkpoint_data['current_interval']
        current_stage = checkpoint_data['current_stage']

        # Determine resume point
        if current_stage < 7:
            # Resume from next stage in same interval
            config.start_stage = current_stage + 1
            interval_idx = config.intervals.index(current_interval)
            config.intervals = config.intervals[interval_idx:]
        else:
            # Current interval complete, move to next
            interval_idx = config.intervals.index(current_interval)
            if interval_idx + 1 < len(config.intervals):
                config.intervals = config.intervals[interval_idx + 1:]
                config.start_stage = 1
            else:
                logger.info("Checkpoint indicates pipeline already completed.")
                sys.exit(0)

        logger.info(f"Resuming from Interval {config.intervals[0]}, Stage {config.start_stage}")

    # Run pipeline
    try:
        run_pipeline(config, logger)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        # No arguments = interactive mode
        interactive_mode()
    else:
        # Arguments provided = CLI mode
        main()
