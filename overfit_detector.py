#!/usr/bin/env python3
"""
Overfitting Detector for Backtesting Parameter Optimization

Analyzes parameter grid results to identify potentially overfitted parameter combinations
by examining Sharpe ratio deterioration in neighboring parameter regions.

Author: Quant Research Assistant
Version: 1.0.0
"""

import argparse
import sys
from pathlib import Path
from typing import Optional, Tuple, Dict, List
import warnings

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from scipy import stats


class OverfitDetector:
    """
    Detects overfitting in backtesting parameter optimization results.
    
    Analyzes the parameter surface to identify sharp peaks that may indicate
    overfitting rather than robust alpha discovery.
    """
    
    def __init__(
        self,
        min_trades: int = 200,
        weights: Dict[str, float] = None,
        col_length: str = "length",
        col_entry: str = "entry_threshold",
        col_sharpe: str = "Sharpe Ratio",
        col_trades: str = "Trade Count"
    ):
        """
        Initialize the OverfitDetector.
        
        Args:
            min_trades: Minimum trade count threshold for graduated penalty
            weights: Dict with keys 'absolute', 'relative', 'worst_case', 'cv'
            col_length: Column name for length parameter
            col_entry: Column name for entry threshold parameter
            col_sharpe: Column name for Sharpe ratio
            col_trades: Column name for trade count
        """
        self.min_trades = min_trades
        self.weights = weights or {
            'absolute': 0.25,
            'relative': 0.25,
            'worst_case': 0.20,
            'cv': 0.30
        }
        self.col_length = col_length
        self.col_entry = col_entry
        self.col_sharpe = col_sharpe
        self.col_trades = col_trades
        
        # Validate weights sum to 1
        weight_sum = sum(self.weights.values())
        if not np.isclose(weight_sum, 1.0):
            warnings.warn(f"Weights sum to {weight_sum}, normalizing to 1.0")
            for k in self.weights:
                self.weights[k] /= weight_sum
        
        # Will be populated during analysis
        self.df = None
        self.grid = None
        self.sharpe_matrix = None
        self.trades_matrix = None
        self.length_values = None
        self.entry_values = None
        self.results = None
    
    def load_data(self, filepath: str) -> pd.DataFrame:
        """Load and validate input CSV data."""
        self.df = pd.read_csv(filepath)
        
        required_cols = [self.col_length, self.col_entry, self.col_sharpe, self.col_trades]
        missing = [c for c in required_cols if c not in self.df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        print(f"Loaded {len(self.df)} parameter combinations")
        return self.df
    
    def _build_grid(self) -> None:
        """Build 2D grid structure from parameter combinations."""
        self.length_values = np.sort(self.df[self.col_length].unique())
        self.entry_values = np.sort(self.df[self.col_entry].unique())
        
        n_length = len(self.length_values)
        n_entry = len(self.entry_values)
        
        print(f"Grid dimensions: {n_length} x {n_entry} (length x entry)")
        
        # Create lookup dictionaries for index mapping
        self.length_to_idx = {v: i for i, v in enumerate(self.length_values)}
        self.entry_to_idx = {v: i for i, v in enumerate(self.entry_values)}
        
        # Initialize matrices with NaN
        self.sharpe_matrix = np.full((n_length, n_entry), np.nan)
        self.trades_matrix = np.full((n_length, n_entry), np.nan)
        
        # Populate matrices
        for _, row in self.df.iterrows():
            i = self.length_to_idx[row[self.col_length]]
            j = self.entry_to_idx[row[self.col_entry]]
            self.sharpe_matrix[i, j] = row[self.col_sharpe]
            self.trades_matrix[i, j] = row[self.col_trades]
    
    def _get_neighbors(self, i: int, j: int, radius: int) -> List[Tuple[int, int]]:
        """
        Get all neighbor indices within specified radius (disk topology).
        
        Uses Chebyshev distance (king moves) - a cell is within radius r
        if max(|di|, |dj|) <= r.
        
        Args:
            i: Row index (length)
            j: Column index (entry)
            radius: Maximum Chebyshev distance
            
        Returns:
            List of (i, j) tuples for valid neighbors
        """
        neighbors = []
        n_rows, n_cols = self.sharpe_matrix.shape
        
        for di in range(-radius, radius + 1):
            for dj in range(-radius, radius + 1):
                if di == 0 and dj == 0:
                    continue  # Skip center cell
                
                ni, nj = i + di, j + dj
                
                # Check bounds
                if 0 <= ni < n_rows and 0 <= nj < n_cols:
                    # Check if cell has valid data
                    if not np.isnan(self.sharpe_matrix[ni, nj]):
                        neighbors.append((ni, nj))
        
        return neighbors
    
    def _compute_metrics(
        self, 
        center_sharpe: float, 
        neighbor_sharpes: np.ndarray
    ) -> Dict[str, float]:
        """
        Compute overfitting metrics for a single parameter combination.
        
        Args:
            center_sharpe: Sharpe ratio at the center cell
            neighbor_sharpes: Array of Sharpe ratios for neighbors
            
        Returns:
            Dict with absolute_drop, relative_drop, worst_case_drop, cv
        """
        if len(neighbor_sharpes) == 0:
            return {
                'absolute_drop': np.nan,
                'relative_drop': np.nan,
                'worst_case_drop': np.nan,
                'cv': np.nan
            }
        
        neighbor_mean = np.mean(neighbor_sharpes)
        neighbor_std = np.std(neighbor_sharpes, ddof=1) if len(neighbor_sharpes) > 1 else 0.0
        neighbor_min = np.min(neighbor_sharpes)
        
        # Absolute drop: center - mean(neighbors)
        # Positive value means center is higher (potential overfit)
        absolute_drop = center_sharpe - neighbor_mean
        
        # Relative drop: normalized by center magnitude
        # Handle near-zero center values
        if np.abs(center_sharpe) > 1e-6:
            relative_drop = absolute_drop / np.abs(center_sharpe)
        else:
            relative_drop = absolute_drop  # Fall back to absolute
        
        # Worst-case drop: center - min(neighbors)
        worst_case_drop = center_sharpe - neighbor_min
        
        # Coefficient of variation in neighborhood
        # High CV indicates unstable/noisy parameter surface
        if np.abs(neighbor_mean) > 1e-6:
            cv = neighbor_std / np.abs(neighbor_mean)
        else:
            cv = neighbor_std  # Fall back to std
        
        return {
            'absolute_drop': absolute_drop,
            'relative_drop': relative_drop,
            'worst_case_drop': worst_case_drop,
            'cv': cv
        }
    
    def _normalize_metrics(self, results_df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize each metric to [0, 1] range across all parameter combinations.
        
        Higher normalized value = more overfitted.
        """
        df = results_df.copy()
        
        metrics_1hop = ['absolute_drop_1hop', 'relative_drop_1hop', 
                        'worst_case_drop_1hop', 'cv_1hop']
        metrics_2hop = ['absolute_drop_2hop', 'relative_drop_2hop',
                        'worst_case_drop_2hop', 'cv_2hop']
        
        for metric in metrics_1hop + metrics_2hop:
            col = df[metric]
            valid_mask = ~col.isna()
            
            if valid_mask.sum() > 0:
                min_val = col[valid_mask].min()
                max_val = col[valid_mask].max()
                
                if max_val > min_val:
                    df[f'{metric}_norm'] = (col - min_val) / (max_val - min_val)
                else:
                    df[f'{metric}_norm'] = 0.5  # All same value
            else:
                df[f'{metric}_norm'] = np.nan
        
        return df
    
    def _compute_composite_score(self, results_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute weighted composite overfitting score.
        
        Combines normalized metrics with specified weights.
        Applies graduated trade count penalty.
        """
        df = results_df.copy()
        
        # Compute composite for 1-hop
        df['composite_1hop'] = (
            self.weights['absolute'] * df['absolute_drop_1hop_norm'] +
            self.weights['relative'] * df['relative_drop_1hop_norm'] +
            self.weights['worst_case'] * df['worst_case_drop_1hop_norm'] +
            self.weights['cv'] * df['cv_1hop_norm']
        )
        
        # Compute composite for 2-hop
        df['composite_2hop'] = (
            self.weights['absolute'] * df['absolute_drop_2hop_norm'] +
            self.weights['relative'] * df['relative_drop_2hop_norm'] +
            self.weights['worst_case'] * df['worst_case_drop_2hop_norm'] +
            self.weights['cv'] * df['cv_2hop_norm']
        )
        
        # Combined composite (average of 1-hop and 2-hop)
        df['composite_combined'] = (df['composite_1hop'] + df['composite_2hop']) / 2
        
        # Apply graduated trade count penalty
        # penalty_factor approaches 1 as trade_count approaches min_trades
        # For low trade counts, we INCREASE the overfit score
        trade_reliability = np.minimum(1.0, df['trade_count'] / self.min_trades)
        
        # Penalty increases overfit score for low trade counts
        # score_final = score + (1 - reliability) * (1 - score)
        # This pushes low-trade combinations toward maximum overfit score
        df['trade_penalty'] = 1 - trade_reliability
        
        df['composite_1hop_penalized'] = (
            df['composite_1hop'] + df['trade_penalty'] * (1 - df['composite_1hop'])
        )
        df['composite_2hop_penalized'] = (
            df['composite_2hop'] + df['trade_penalty'] * (1 - df['composite_2hop'])
        )
        df['composite_final'] = (
            df['composite_combined'] + df['trade_penalty'] * (1 - df['composite_combined'])
        )
        
        # Handle NaN composites (edge cases with no valid neighbors)
        df['composite_final'] = df['composite_final'].fillna(1.0)
        
        return df
    
    def analyze(self) -> pd.DataFrame:
        """
        Run full overfitting analysis on loaded data.
        
        Returns:
            DataFrame with all metrics and composite scores
        """
        if self.df is None:
            raise ValueError("No data loaded. Call load_data() first.")
        
        self._build_grid()
        
        results = []
        
        for _, row in self.df.iterrows():
            length = row[self.col_length]
            entry = row[self.col_entry]
            center_sharpe = row[self.col_sharpe]
            trade_count = row[self.col_trades]
            
            i = self.length_to_idx[length]
            j = self.entry_to_idx[entry]
            
            # Get neighbors at different radii
            neighbors_1hop = self._get_neighbors(i, j, radius=1)
            neighbors_2hop = self._get_neighbors(i, j, radius=2)  # Disk includes 1-hop
            
            # Extract Sharpe values
            sharpes_1hop = np.array([self.sharpe_matrix[ni, nj] for ni, nj in neighbors_1hop])
            sharpes_2hop = np.array([self.sharpe_matrix[ni, nj] for ni, nj in neighbors_2hop])
            
            # Compute metrics
            metrics_1hop = self._compute_metrics(center_sharpe, sharpes_1hop)
            metrics_2hop = self._compute_metrics(center_sharpe, sharpes_2hop)
            
            results.append({
                self.col_length: length,
                self.col_entry: entry,
                'sharpe_ratio': center_sharpe,
                'trade_count': trade_count,
                'n_neighbors_1hop': len(neighbors_1hop),
                'n_neighbors_2hop': len(neighbors_2hop),
                'absolute_drop_1hop': metrics_1hop['absolute_drop'],
                'relative_drop_1hop': metrics_1hop['relative_drop'],
                'worst_case_drop_1hop': metrics_1hop['worst_case_drop'],
                'cv_1hop': metrics_1hop['cv'],
                'absolute_drop_2hop': metrics_2hop['absolute_drop'],
                'relative_drop_2hop': metrics_2hop['relative_drop'],
                'worst_case_drop_2hop': metrics_2hop['worst_case_drop'],
                'cv_2hop': metrics_2hop['cv'],
            })
        
        results_df = pd.DataFrame(results)
        
        # Normalize metrics
        results_df = self._normalize_metrics(results_df)
        
        # Compute composite scores
        results_df = self._compute_composite_score(results_df)
        
        # Sort by final composite score (most overfitted first)
        results_df = results_df.sort_values('composite_final', ascending=False)
        
        self.results = results_df
        
        print(f"Analysis complete. {len(results_df)} parameter combinations scored.")
        
        return results_df
    
    def save_results(self, filepath: str) -> None:
        """Save results to CSV."""
        if self.results is None:
            raise ValueError("No results to save. Call analyze() first.")
        
        self.results.to_csv(filepath, index=False)
        print(f"Results saved to {filepath}")
    
    def generate_heatmap(
        self, 
        output_path: str,
        figsize: Tuple[int, int] = (14, 10),
        cmap: str = 'RdYlGn_r'  # Red = high overfit, Green = robust
    ) -> None:
        """
        Generate robustness heatmap visualization.
        
        Args:
            output_path: Path to save the heatmap image
            figsize: Figure size tuple
            cmap: Colormap (reversed RdYlGn so red = overfitted)
        """
        if self.results is None:
            raise ValueError("No results to visualize. Call analyze() first.")
        
        # Build matrix for visualization
        n_length = len(self.length_values)
        n_entry = len(self.entry_values)
        
        overfit_matrix = np.full((n_length, n_entry), np.nan)
        
        for _, row in self.results.iterrows():
            i = self.length_to_idx[row[self.col_length]]
            j = self.entry_to_idx[row[self.col_entry]]
            overfit_matrix[i, j] = row['composite_final']
        
        # Create figure with two subplots
        fig, axes = plt.subplots(1, 2, figsize=figsize)
        
        # Plot 1: Original Sharpe Ratio heatmap
        im1 = axes[0].imshow(
            self.sharpe_matrix, 
            aspect='auto', 
            cmap='Greens',
            origin='lower'
        )
        axes[0].set_title('Original Sharpe Ratio Heatmap', fontsize=12, fontweight='bold')
        axes[0].set_xlabel(self.col_entry)
        axes[0].set_ylabel(self.col_length)
        
        # Set tick labels
        x_ticks = np.arange(0, n_entry, max(1, n_entry // 10))
        y_ticks = np.arange(0, n_length, max(1, n_length // 10))
        axes[0].set_xticks(x_ticks)
        axes[0].set_yticks(y_ticks)
        axes[0].set_xticklabels([f'{self.entry_values[i]:.2f}' for i in x_ticks], rotation=45)
        axes[0].set_yticklabels([f'{self.length_values[i]:.0f}' for i in y_ticks])
        
        plt.colorbar(im1, ax=axes[0], label='Sharpe Ratio')
        
        # Plot 2: Overfitting Score heatmap
        im2 = axes[1].imshow(
            overfit_matrix,
            aspect='auto',
            cmap=cmap,
            origin='lower',
            vmin=0,
            vmax=1
        )
        axes[1].set_title('Overfitting Score Heatmap\n(Higher = More Overfitted)', 
                          fontsize=12, fontweight='bold')
        axes[1].set_xlabel(self.col_entry)
        axes[1].set_ylabel(self.col_length)
        
        axes[1].set_xticks(x_ticks)
        axes[1].set_yticks(y_ticks)
        axes[1].set_xticklabels([f'{self.entry_values[i]:.2f}' for i in x_ticks], rotation=45)
        axes[1].set_yticklabels([f'{self.length_values[i]:.0f}' for i in y_ticks])
        
        plt.colorbar(im2, ax=axes[1], label='Overfitting Score')
        
        # Mark top 5 most overfitted points
        top_5 = self.results.head(5)
        for _, row in top_5.iterrows():
            i = self.length_to_idx[row[self.col_length]]
            j = self.entry_to_idx[row[self.col_entry]]
            axes[1].plot(j, i, 'ko', markersize=10, markerfacecolor='none', markeredgewidth=2)
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        print(f"Heatmap saved to {output_path}")
    
    def print_summary(self, top_n: int = 10) -> None:
        """Print summary of most and least overfitted parameters."""
        if self.results is None:
            raise ValueError("No results to summarize. Call analyze() first.")
        
        print("\n" + "="*80)
        print("OVERFITTING ANALYSIS SUMMARY")
        print("="*80)
        
        print(f"\nConfiguration:")
        print(f"  - Minimum trades threshold: {self.min_trades}")
        print(f"  - Weights: {self.weights}")
        
        print(f"\n{'TOP ' + str(top_n) + ' MOST OVERFITTED PARAMETERS':^80}")
        print("-"*80)
        
        cols_display = [self.col_length, self.col_entry, 'sharpe_ratio', 'trade_count', 
                       'composite_final', 'trade_penalty']
        
        print(self.results[cols_display].head(top_n).to_string(index=False))
        
        print(f"\n{'TOP ' + str(top_n) + ' MOST ROBUST PARAMETERS':^80}")
        print("-"*80)
        
        print(self.results[cols_display].tail(top_n).iloc[::-1].to_string(index=False))
        
        # Statistics
        print(f"\n{'DISTRIBUTION STATISTICS':^80}")
        print("-"*80)
        print(f"Composite score - Mean: {self.results['composite_final'].mean():.4f}, "
              f"Std: {self.results['composite_final'].std():.4f}")
        print(f"Composite score - Min: {self.results['composite_final'].min():.4f}, "
              f"Max: {self.results['composite_final'].max():.4f}")
        
        # Count by trade threshold
        low_trade = (self.results['trade_count'] < self.min_trades).sum()
        print(f"\nParameter combinations with < {self.min_trades} trades: "
              f"{low_trade} ({100*low_trade/len(self.results):.1f}%)")


def main():
    """Main entry point for command-line usage."""
    parser = argparse.ArgumentParser(
        description='Detect overfitting in backtesting parameter optimization results',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python overfit_detector.py --input report.csv --output scores.csv
  python overfit_detector.py --input report.csv --output scores.csv --min-trades 100 --generate-heatmap
  python overfit_detector.py --input data.csv --col-length "lookback" --col-entry "threshold"
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Input CSV file with backtest results'
    )
    
    parser.add_argument(
        '--output', '-o',
        required=True,
        help='Output CSV file for overfitting scores'
    )
    
    parser.add_argument(
        '--min-trades',
        type=int,
        default=200,
        help='Minimum trade count threshold for penalty (default: 200)'
    )
    
    parser.add_argument(
        '--col-length',
        default='length',
        help='Column name for length parameter (default: length)'
    )
    
    parser.add_argument(
        '--col-entry',
        default='entry_threshold',
        help='Column name for entry threshold parameter (default: entry_threshold)'
    )
    
    parser.add_argument(
        '--col-sharpe',
        default='Sharpe Ratio',
        help='Column name for Sharpe ratio (default: "Sharpe Ratio")'
    )
    
    parser.add_argument(
        '--col-trades',
        default='Trade Count',
        help='Column name for trade count (default: "Trade Count")'
    )
    
    parser.add_argument(
        '--generate-heatmap',
        action='store_true',
        help='Generate robustness heatmap visualization'
    )
    
    parser.add_argument(
        '--heatmap-output',
        default=None,
        help='Output path for heatmap (default: <output>_heatmap.png)'
    )
    
    parser.add_argument(
        '--weights',
        type=str,
        default='0.25,0.25,0.20,0.30',
        help='Weights for absolute,relative,worst_case,cv (default: 0.25,0.25,0.20,0.30)'
    )
    
    parser.add_argument(
        '--top-n',
        type=int,
        default=10,
        help='Number of top/bottom results to show in summary (default: 10)'
    )
    
    args = parser.parse_args()
    
    # Parse weights
    try:
        w = [float(x) for x in args.weights.split(',')]
        if len(w) != 4:
            raise ValueError("Must provide exactly 4 weights")
        weights = {
            'absolute': w[0],
            'relative': w[1],
            'worst_case': w[2],
            'cv': w[3]
        }
    except Exception as e:
        print(f"Error parsing weights: {e}")
        sys.exit(1)
    
    # Initialize detector
    detector = OverfitDetector(
        min_trades=args.min_trades,
        weights=weights,
        col_length=args.col_length,
        col_entry=args.col_entry,
        col_sharpe=args.col_sharpe,
        col_trades=args.col_trades
    )
    
    # Run analysis
    try:
        detector.load_data(args.input)
        detector.analyze()
        detector.save_results(args.output)
        detector.print_summary(top_n=args.top_n)
        
        if args.generate_heatmap:
            heatmap_path = args.heatmap_output or args.output.replace('.csv', '_heatmap.png')
            detector.generate_heatmap(heatmap_path)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
