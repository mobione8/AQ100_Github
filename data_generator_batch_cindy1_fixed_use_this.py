"""
Automated Merged Data Generator for Bruteforce Backtesting
==========================================================

FIXED VERSION - Changes from original:
  FIX 1: dropna() was too aggressive - now only drops rows where primary OHLCV
          is missing, then forward-fills benchmark/VIX gaps. Prevents losing
          ~500 weekly bars on merge, which was causing Stage 2 to fail
          (1054 rows instead of ~1500+ needed).

  FIX 2: min_rows validation was hardcoded to 9060 (1min calibrated).
          Now uses interval-aware thresholds so 1d/1w don't false-fail.

  FIX 3: validate_output now receives the interval so it can pick the
          correct minimum row count.

Usage:
    python data_generator_batch_cindy1_fixed.py

Features:
    - Interactive CLI with smart defaults
    - BATCH MODE: Generate all intervals at once
    - MULTI-SYMBOL: Process multiple symbols in one run
    - Automatic benchmark selection based on asset type
    - VIX integration for volatility context
    - Derived features: returns, volatility, spreads, volume ratios
    - Data validation against bruteforce requirements
    - Proper output naming convention
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from ib_insync import IB, Stock, Index, util
import pytz
import os
import sys
import time

# FIX: Always save files relative to this script's own folder (AQS100V2),
# regardless of which terminal directory the script is launched from.
os.chdir(os.path.dirname(os.path.abspath(__file__)))


class DataGenerator:
    """Generate merged CSV files for bruteforce backtesting"""

    # Asset type to benchmark mapping
    BENCHMARK_MAP = {
        'tech_stock':       ['QQQ', 'XLK'],
        'financial_stock':  ['XLF', 'SPY'],
        'healthcare_stock': ['XLV', 'SPY'],
        'energy_stock':     ['XLE', 'SPY'],
        'consumer_stock':   ['XLY', 'SPY'],
        'industrial_stock': ['XLI', 'SPY'],
        'materials_stock':  ['XLB', 'SPY'],
        'utilities_stock':  ['XLU', 'SPY'],
        'realestate_stock': ['XLRE', 'SPY'],
        'crypto_etf':       ['QQQ', 'GBTC'],
        'commodity_etf':    ['GLD', 'SPY'],
        'leveraged_etf':    ['QQQ', 'SPY'],
        'generic':          ['SPY', 'QQQ']
    }

    # Tech stock symbols for auto-detection
    TECH_SYMBOLS = ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'GOOG', 'META', 'AMZN',
                    'TSLA', 'AMD', 'NFLX', 'INTC', 'CSCO', 'ADBE', 'CRM',
                    'AVGO', 'QCOM', 'TXN', 'ORCL', 'NOW', 'INTU']

    # Interval mapping for IB API
    INTERVAL_MAP = {
        '1min':  '1 min',
        '5min':  '5 mins',
        '15min': '15 mins',
        '30min': '30 mins',
        '1h':    '1 hour',
        '4h':    '4 hours',
        '1d':    '1 day',
        '1w':    '1 week'
    }

    # Duration mapping based on interval
    DURATION_MAP = {
        '1min':  '20 D',
        '5min':  '80 D',
        '15min': '200 D',
        '30min': '360 D',
        '1h':    '3 Y',
        '4h':    '10 Y',
        '1d':    '30 Y',
        '1w':    '30 Y'
    }

    # ----------------------------------------------------------------
    # FIX 2: Interval-aware minimum row thresholds
    # Original had min_rows = 9060 hardcoded (only valid for 1min).
    # This caused false validation failures for 1d/1w data.
    # ----------------------------------------------------------------
    MIN_ROWS_MAP = {
        '1min':  9060,
        '5min':  2000,
        '15min': 1500,
        '30min': 1200,
        '1h':    1200,
        '4h':    1000,
        '1d':    500,
        '1w':    300
    }

    def __init__(self):
        self.ib = IB()
        self.connected = False

    def connect(self, host='127.0.0.1', port=7497, clientId=201):
        """Connect to Interactive Brokers"""
        try:
            self.ib.connect(host, port, clientId)
            self.connected = True
            print(f"✓ Connected to IB Gateway/TWS at {host}:{port}")
            return True
        except Exception as e:
            print(f"✗ Failed to connect to IB: {e}")
            print("  Make sure TWS/IB Gateway is running and API connections are enabled.")
            return False

    def disconnect(self):
        """Disconnect from Interactive Brokers"""
        if self.connected:
            self.ib.disconnect()
            self.connected = False
            print("✓ Disconnected from IB")

    def detect_asset_type(self, symbol):
        """Automatically detect asset type for benchmark selection"""
        symbol_upper = symbol.upper()

        if symbol_upper in self.TECH_SYMBOLS:
            return 'tech_stock'
        if any(pattern in symbol_upper for pattern in ['3X', '2X', 'BULL', 'BEAR']):
            return 'leveraged_etf'
        if any(pattern in symbol_upper for pattern in ['BIT', 'BTC', 'ETH', 'CRYPTO']):
            return 'crypto_etf'
        if any(pattern in symbol_upper for pattern in ['GLD', 'SLV', 'USO', 'UNG']):
            return 'commodity_etf'
        return 'generic'

    def get_benchmarks(self, symbol):
        """Get appropriate benchmark tickers for the asset"""
        asset_type = self.detect_asset_type(symbol)
        benchmarks = self.BENCHMARK_MAP.get(asset_type, self.BENCHMARK_MAP['generic'])
        print(f"  Asset type detected: {asset_type}")
        print(f"  Benchmarks selected: {', '.join(benchmarks)}")
        return benchmarks

    def create_contract(self, symbol, sec_type='STK'):
        """Create IB contract object"""
        if sec_type in ('STK', 'ETF'):
            return Stock(symbol, 'SMART', 'USD')
        elif sec_type == 'IND':
            return Index(symbol, 'CBOE')
        else:
            return Stock(symbol, 'SMART', 'USD')

    def fetch_historical_data(self, symbol, interval, duration, sec_type='STK'):
        """Fetch historical OHLCV data from IB"""
        try:
            contract = self.create_contract(symbol, sec_type)

            qualified = self.ib.qualifyContracts(contract)
            if not qualified:
                print(f"  ✗ Could not qualify contract for {symbol}")
                return None

            contract = qualified[0]

            bar_size = self.INTERVAL_MAP.get(interval, '1 hour')
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime='',
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow='TRADES',
                useRTH=False,
                formatDate=1
            )

            if not bars:
                print(f"  ✗ No data received for {symbol}")
                return None

            df = util.df(bars)
            df = df.rename(columns={'date': 'datetime', 'volume': 'volume'})

            print(f"  ✓ Fetched {len(df)} bars for {symbol}")
            return df

        except Exception as e:
            print(f"  ✗ Error fetching data for {symbol}: {e}")
            return None

    def calculate_derived_features(self, df, primary_col='close', benchmark_cols=None):
        """Calculate derived technical features"""
        print("  Calculating derived features...")

        df['returns'] = df[primary_col].pct_change()
        df['volatility_20'] = df['returns'].rolling(window=20).std()
        df['volume_ma_ratio'] = df['volume'] / df['volume'].rolling(window=20).mean()

        if benchmark_cols:
            for bench_col in benchmark_cols:
                if bench_col in df.columns:
                    spread_col = f"{primary_col}_{bench_col.replace('_close', '')}_spread"
                    df[spread_col] = df[primary_col] - df[bench_col]

        return df

    def merge_data(self, primary_df, vix_df, benchmark_dfs, benchmark_symbols):
        """Merge primary asset data with VIX and benchmarks"""
        print("  Merging datasets...")

        merged = primary_df.copy()
        merged['start_time'] = merged['datetime']

        if vix_df is not None:
            vix_df = vix_df[['datetime', 'close']].rename(columns={'close': 'VIX'})
            merged = pd.merge(merged, vix_df, on='datetime', how='left')

        for symbol, bench_df in zip(benchmark_symbols, benchmark_dfs):
            if bench_df is not None:
                col_name = f"{symbol}_close"
                bench_df = bench_df[['datetime', 'close']].rename(columns={'close': col_name})
                merged = pd.merge(merged, bench_df, on='datetime', how='left')

        return merged

    def validate_output(self, df, interval='1h'):
        """
        Validate data against bruteforce requirements.

        FIX 2 applied here: uses interval-aware min_rows instead of
        the original hardcoded 9060 which was only valid for 1min data.
        """
        print("  Validating output...")

        issues = []

        if 'close' not in df.columns:
            issues.append("Missing required 'close' column")

        excluded = ['start_time', 'datetime', 'close', 'Unnamed: 0']
        feature_cols = [col for col in df.columns if col not in excluded]
        if len(feature_cols) == 0:
            issues.append("No feature columns found")

        nan_cols = df.columns[df.isna().any()].tolist()
        if nan_cols:
            issues.append(f"NaN values found in columns: {', '.join(nan_cols)}")

        # FIX 2: use interval-aware minimum
        min_rows = self.MIN_ROWS_MAP.get(interval, 500)
        if len(df) < min_rows:
            issues.append(f"Only {len(df)} rows (minimum recommended for {interval}: {min_rows})")

        non_numeric = [col for col in feature_cols
                       if not pd.api.types.is_numeric_dtype(df[col])]
        if non_numeric:
            issues.append(f"Non-numeric feature columns: {', '.join(non_numeric)}")

        if issues:
            print("  ⚠ Validation warnings:")
            for issue in issues:
                print(f"    - {issue}")
            return False, issues
        else:
            print("  ✓ All validation checks passed")
            return True, []

    def save_output(self, df, symbol, interval, creator_name):
        """Save merged data with proper naming convention"""
        output_dir = "GridSearch_Data"
        os.makedirs(output_dir, exist_ok=True)

        date_str = datetime.now().strftime("%d%b%Y")
        filename = f"merged_ibkr_{symbol}_{interval}_{creator_name}_{date_str}.csv"
        filepath = os.path.join(output_dir, filename)

        df.to_csv(filepath, index=False)
        print(f"  ✓ Saved: {os.path.basename(filepath)}")
        print(f"    Rows: {len(df)}, Columns: {len(df.columns)}")

        return filepath

    def generate(self, symbol, interval, creator_name, custom_benchmarks=None, auto_continue=False):
        """Main generation workflow"""
        print(f"\n{'='*60}")
        print(f"Generating Merged Data for {symbol} ({interval})")
        print(f"{'='*60}\n")

        if not self.connected:
            if not self.connect():
                return None

        duration = self.DURATION_MAP.get(interval, '2 Y')

        # Step 1: Fetch primary asset data
        print("1. Fetching primary asset data...")
        primary_df = self.fetch_historical_data(symbol, interval, duration)
        if primary_df is None:
            return None

        # Step 2: Fetch VIX data
        print("\n2. Fetching VIX data...")
        vix_df = self.fetch_historical_data('VIX', interval, duration, sec_type='IND')

        # Step 3: Determine and fetch benchmarks
        print("\n3. Determining benchmarks...")
        if custom_benchmarks:
            benchmark_symbols = custom_benchmarks
            print(f"  Using custom benchmarks: {', '.join(benchmark_symbols)}")
        else:
            benchmark_symbols = self.get_benchmarks(symbol)

        print("  Fetching benchmark data...")
        benchmark_dfs = []
        for bench_symbol in benchmark_symbols:
            bench_df = self.fetch_historical_data(bench_symbol, interval, duration, sec_type='ETF')
            benchmark_dfs.append(bench_df)

        # Step 4: Merge all data
        print("\n4. Merging data...")
        merged_df = self.merge_data(primary_df, vix_df, benchmark_dfs, benchmark_symbols)

        # Step 5: Calculate derived features
        print("\n5. Calculating features...")
        benchmark_cols = [f"{sym}_close" for sym in benchmark_symbols]
        merged_df = self.calculate_derived_features(merged_df, benchmark_cols=benchmark_cols)

        # ----------------------------------------------------------------
        # FIX 1: Smart cleaning instead of blanket dropna()
        #
        # Original code:
        #   merged_df = merged_df.dropna()
        #
        # Problem: dropna() dropped ANY row where benchmark/VIX had a gap
        # on that timestamp. For weekly AAPL this wiped ~500 rows, leaving
        # only 1054 instead of ~1500+, causing Stage 2 to fail its 1200-row
        # minimum check.
        #
        # Fix:
        #   1. Only hard-drop rows where primary OHLCV data is missing.
        #   2. Forward-fill benchmark and VIX columns to bridge small gaps.
        #   3. Then drop any remaining NaN (e.g., rolling window warmup).
        # ----------------------------------------------------------------
        print("\n6. Cleaning data...")
        initial_rows = len(merged_df)

        primary_cols = ['open', 'high', 'low', 'close', 'volume']
        existing_primary = [c for c in primary_cols if c in merged_df.columns]

        # Drop rows only where the main OHLCV data is actually missing
        merged_df = merged_df.dropna(subset=existing_primary)

        # Forward-fill benchmark and VIX columns to cover small calendar gaps
        fill_cols = [c for c in merged_df.columns
                     if c not in existing_primary + ['datetime', 'start_time']]
        if fill_cols:
            merged_df[fill_cols] = merged_df[fill_cols].ffill()
            print(f"  Forward-filled {len(fill_cols)} benchmark/VIX/derived columns")

        # Final drop for any remaining NaN (rolling window warmup rows at the start)
        merged_df = merged_df.dropna()

        dropped_rows = initial_rows - len(merged_df)
        if dropped_rows > 0:
            print(f"  Dropped {dropped_rows} rows (warmup/missing primary data)")
        print(f"  Final row count: {len(merged_df)}")

        # Step 7: Validate — FIX 3: pass interval so thresholds are correct
        print("\n7. Validating output...")
        valid, issues = self.validate_output(merged_df, interval=interval)

        # Step 8: Save
        if valid or auto_continue or (not valid and input("\n  Continue despite warnings? (y/n): ").lower() == 'y'):
            print("\n8. Saving output...")
            filepath = self.save_output(merged_df, symbol, interval, creator_name)
            return filepath
        else:
            print("\n  ✗ Output not saved due to validation issues")
            return None

    def generate_all_intervals(self, symbol, creator_name, custom_benchmarks=None, intervals=None):
        """Generate data for all intervals for a single symbol"""
        if intervals is None:
            intervals = list(self.INTERVAL_MAP.keys())

        print(f"\n{'#'*70}")
        print(f"# BATCH GENERATION: {symbol} - ALL INTERVALS")
        print(f"# Total intervals to process: {len(intervals)}")
        print(f"# Intervals: {', '.join(intervals)}")
        print(f"{'#'*70}\n")

        results = {'success': [], 'failed': []}

        for idx, interval in enumerate(intervals, 1):
            print(f"\n[{idx}/{len(intervals)}] Processing interval: {interval}")
            print("-" * 70)

            try:
                filepath = self.generate(
                    symbol=symbol,
                    interval=interval,
                    creator_name=creator_name,
                    custom_benchmarks=custom_benchmarks,
                    auto_continue=True  # Auto-continue past validation warnings in batch mode
                )

                if filepath:
                    results['success'].append((interval, filepath))
                    print(f"✓ SUCCESS: {interval}")
                else:
                    results['failed'].append((interval, "Generation returned None"))
                    print(f"✗ FAILED: {interval}")

            except Exception as e:
                results['failed'].append((interval, str(e)))
                print(f"✗ FAILED: {interval} - {e}")

            if idx < len(intervals):
                time.sleep(2)

        self._print_batch_summary(symbol, results)
        return results

    def generate_multi_symbol(self, symbols, creator_name, custom_benchmarks=None, intervals=None):
        """Generate data for multiple symbols across all intervals"""
        if intervals is None:
            intervals = list(self.INTERVAL_MAP.keys())

        print(f"\n{'#'*70}")
        print(f"# MULTI-SYMBOL BATCH GENERATION")
        print(f"# Symbols: {', '.join(symbols)}")
        print(f"# Intervals per symbol: {len(intervals)}")
        print(f"# Total files to generate: {len(symbols) * len(intervals)}")
        print(f"{'#'*70}\n")

        all_results = {}

        for symbol_idx, symbol in enumerate(symbols, 1):
            print(f"\n{'='*70}")
            print(f"SYMBOL {symbol_idx}/{len(symbols)}: {symbol}")
            print(f"{'='*70}")

            results = self.generate_all_intervals(
                symbol=symbol,
                creator_name=creator_name,
                custom_benchmarks=custom_benchmarks,
                intervals=intervals
            )

            all_results[symbol] = results

            if symbol_idx < len(symbols):
                print(f"\nPausing 5 seconds before next symbol...")
                time.sleep(5)

        self._print_multi_summary(all_results)
        return all_results

    def _print_batch_summary(self, symbol, results):
        """Print summary for single symbol batch generation"""
        print(f"\n{'='*70}")
        print(f"BATCH GENERATION SUMMARY: {symbol}")
        print(f"{'='*70}")
        total = len(results['success']) + len(results['failed'])
        print(f"✓ Successful: {len(results['success'])}/{total}")
        print(f"✗ Failed:     {len(results['failed'])}/{total}")

        if results['success']:
            print(f"\nSuccessful intervals:")
            for interval, filepath in results['success']:
                print(f"  ✓ {interval:6s} → {os.path.basename(filepath)}")

        if results['failed']:
            print(f"\nFailed intervals:")
            for interval, error in results['failed']:
                print(f"  ✗ {interval:6s} → {error}")

        print(f"{'='*70}\n")

    def _print_multi_summary(self, all_results):
        """Print summary for multi-symbol batch generation"""
        total_success = sum(len(r['success']) for r in all_results.values())
        total_failed  = sum(len(r['failed'])  for r in all_results.values())
        total_files   = total_success + total_failed

        print(f"\n{'#'*70}")
        print(f"MULTI-SYMBOL GENERATION COMPLETE")
        print(f"{'#'*70}")
        print(f"Total symbols processed: {len(all_results)}")
        print(f"Total files generated:   {total_success}/{total_files}")
        if total_files > 0:
            print(f"Success rate:            {(total_success/total_files*100):.1f}%")

        print(f"\nPer-symbol breakdown:")
        for symbol, results in all_results.items():
            success_count = len(results['success'])
            total_count   = success_count + len(results['failed'])
            print(f"  {symbol:6s}: {success_count}/{total_count} successful")

        print(f"{'#'*70}\n")


# =============================================================================
# INTERACTIVE CLI
# =============================================================================

def interactive_mode():
    """Interactive CLI for generating merged data"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║     Automated Merged Data Generator for Bruteforce          ║
║              Backtesting System - BATCH MODE                 ║
╚══════════════════════════════════════════════════════════════╝
    """)

    generator = DataGenerator()

    try:
        print("Select mode:")
        print("  1. Single symbol, single interval")
        print("  2. Single symbol, ALL intervals (batch mode)")
        print("  3. Multiple symbols, ALL intervals (multi-symbol batch)")
        print("  4. Multiple symbols, ONE specific interval  ← NEW")

        mode = input("\nEnter mode (1/2/3/4, default: 2): ").strip() or '2'

        if mode == '1':
            symbol = input("\nEnter asset symbol (e.g., NVDA, AAPL, SOXL): ").strip().upper()
            if not symbol:
                print("✗ Symbol cannot be empty")
                return

            print("\nAvailable intervals: 1min, 5min, 15min, 30min, 1h, 4h, 1d, 1w")
            interval = input("Enter interval (default: 1h): ").strip().lower() or '1h'
            if interval not in DataGenerator.INTERVAL_MAP:
                print(f"✗ Invalid interval. Using default: 1h")
                interval = '1h'

            creator_name = input("Enter your name/identifier (e.g., cindy): ").strip().lower() or "user"

            custom = input("\nUse custom benchmarks? (y/n, default: n): ").strip().lower()
            custom_benchmarks = None
            if custom == 'y':
                bench_input = input("Enter benchmark symbols (e.g., SPY,QQQ): ").strip()
                if bench_input:
                    custom_benchmarks = [b.strip().upper() for b in bench_input.split(',')]

            generator.generate(symbol, interval, creator_name, custom_benchmarks)

        elif mode == '2':
            symbol = input("\nEnter asset symbol (e.g., SOXL, NVDA): ").strip().upper()
            if not symbol:
                print("✗ Symbol cannot be empty")
                return

            creator_name = input("Enter your name/identifier (e.g., cindy): ").strip().lower() or "user"

            custom = input("\nUse custom benchmarks? (y/n, default: n): ").strip().lower()
            custom_benchmarks = None
            if custom == 'y':
                bench_input = input("Enter benchmark symbols (e.g., SPY,QQQ): ").strip()
                if bench_input:
                    custom_benchmarks = [b.strip().upper() for b in bench_input.split(',')]

            custom_intervals = input("\nGenerate specific intervals only? (y/n, default: n): ").strip().lower()
            intervals = None
            if custom_intervals == 'y':
                print("Available: 1min, 5min, 15min, 30min, 1h, 4h, 1d, 1w")
                interval_input = input("Enter intervals (e.g., 1h,1d,1w): ").strip()
                if interval_input:
                    intervals = [i.strip().lower() for i in interval_input.split(',')]

            generator.generate_all_intervals(symbol, creator_name, custom_benchmarks, intervals)

        elif mode == '3':
            symbols_input = input("\nEnter symbols (e.g., SOXL,TQQQ,NVDA): ").strip().upper()
            if not symbols_input:
                print("✗ Symbols cannot be empty")
                return

            symbols = [s.strip() for s in symbols_input.split(',')]
            creator_name = input("Enter your name/identifier (e.g., cindy): ").strip().lower() or "user"

            custom = input("\nUse custom benchmarks? (y/n, default: n): ").strip().lower()
            custom_benchmarks = None
            if custom == 'y':
                bench_input = input("Enter benchmark symbols (e.g., SPY,QQQ): ").strip()
                if bench_input:
                    custom_benchmarks = [b.strip().upper() for b in bench_input.split(',')]

            custom_intervals = input("\nGenerate specific intervals only? (y/n, default: n): ").strip().lower()
            intervals = None
            if custom_intervals == 'y':
                print("Available: 1min, 5min, 15min, 30min, 1h, 4h, 1d, 1w")
                interval_input = input("Enter intervals (e.g., 1h,1d,1w): ").strip()
                if interval_input:
                    intervals = [i.strip().lower() for i in interval_input.split(',')]

            generator.generate_multi_symbol(symbols, creator_name, custom_benchmarks, intervals)

        elif mode == '4':
            # NEW: Multiple symbols, ONE specific interval
            symbols_input = input("\nEnter symbols (e.g., AEHR,ALGM,NVDA or paste full list): ").strip().upper()
            if not symbols_input:
                print("✗ Symbols cannot be empty")
                return

            symbols = [s.strip() for s in symbols_input.split(',') if s.strip()]

            print("\nAvailable intervals: 1min, 5min, 15min, 30min, 1h, 4h, 1d, 1w")
            interval = input("Enter interval (default: 4h): ").strip().lower() or '4h'
            if interval not in DataGenerator.INTERVAL_MAP:
                print(f"✗ Invalid interval '{interval}'. Using default: 4h")
                interval = '4h'

            creator_name = input("Enter your name/identifier (e.g., cindy): ").strip().lower() or "user"

            custom = input("\nUse custom benchmarks? (y/n, default: n): ").strip().lower()
            custom_benchmarks = None
            if custom == 'y':
                bench_input = input("Enter benchmark symbols (e.g., SPY,QQQ): ").strip()
                if bench_input:
                    custom_benchmarks = [b.strip().upper() for b in bench_input.split(',')]

            print(f"\n{'#'*70}")
            print(f"# MULTI-SYMBOL SINGLE INTERVAL: {len(symbols)} symbols @ {interval}")
            print(f"{'#'*70}\n")

            results_all = {}
            for idx, symbol in enumerate(symbols, 1):
                print(f"\n[{idx}/{len(symbols)}] {symbol}")
                print("-" * 50)
                try:
                    filepath = generator.generate(
                        symbol=symbol,
                        interval=interval,
                        creator_name=creator_name,
                        custom_benchmarks=custom_benchmarks,
                        auto_continue=True
                    )
                    results_all[symbol] = '✅ OK' if filepath else '❌ Failed'
                except Exception as e:
                    results_all[symbol] = f'❌ Error: {e}'
                    print(f"✗ {symbol} failed: {e}")

                if idx < len(symbols):
                    import time
                    time.sleep(2)

            print(f"\n{'='*70}")
            print(f"SUMMARY — {interval} data for {len(symbols)} symbols")
            print(f"{'='*70}")
            for sym, status in results_all.items():
                print(f"  {sym:8s}: {status}")
            print(f"{'='*70}")

        else:
            print("✗ Invalid mode selected")
            return

        print(f"\n{'='*60}")
        print("✓ Data generation completed!")
        print(f"{'='*60}")
        print("\nYour files are in the GridSearch_Data/ directory")
        print("Next steps:")
        print("  1. Review the generated CSV files")
        print("  2. Run: python 03_run_ibkr_gs_pipeline.py --intervals 1w --symbols AAPL")

    except KeyboardInterrupt:
        print("\n\n✗ Operation cancelled by user")
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        generator.disconnect()


if __name__ == "__main__":
    interactive_mode()
