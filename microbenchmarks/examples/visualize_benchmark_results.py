#!/usr/bin/env python3
"""
Visualization script for join benchmark results.

This script reads the TSV output from the benchmark and creates various plots
to help analyze the data and understand relationships between metrics.

Usage:
    python visualize_benchmark_results.py [/path/to/benchmark_results.tsv] [output_directory]
    
    Arguments:
        tsv_path (optional): Path to benchmark results TSV file
            Default: /data/tmp/simple_hash_vs_sort/benchmark_results.tsv
        
        output_directory (optional): Directory to save plots
            Default: visualizations/ directory next to the TSV file
    
    Examples:
        # Use defaults (TSV and output directory)
        python visualize_benchmark_results.py
        
        # Specify TSV path (output goes to visualizations/ next to TSV)
        python visualize_benchmark_results.py /path/to/results.tsv
        
        # Specify both TSV and output directory
        python visualize_benchmark_results.py /path/to/results.tsv /path/to/output

Requirements:
    pip install pandas matplotlib seaborn numpy scipy
    
    Optional (for better trend line smoothing):
    pip install statsmodels
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from scipy.signal import savgol_filter

# Try to import LOWESS for smoothing, fall back to polynomial if not available
try:
    from statsmodels.nonparametric.smoothers_lowess import lowess
    HAS_LOWESS = True
except ImportError:
    HAS_LOWESS = False

# Default paths
DEFAULT_TSV_PATH = "/data/tmp/simple_hash_vs_sort/benchmark_results.tsv"

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)

def load_benchmark_data(tsv_path):
    """Load and prepare benchmark data from TSV."""
    print(f"Loading data from: {tsv_path}")
    
    if not os.path.exists(tsv_path):
        print(f"ERROR: File not found: {tsv_path}")
        sys.exit(1)
    
    # Read TSV
    df = pd.read_csv(tsv_path, sep='\t')
    print(f"Loaded {len(df)} rows")
    
    # Filter to successful runs only
    if 'Status' in df.columns:
        df = df[df['Status'] == 'SUCCESS'].copy()
        print(f"Successful runs: {len(df)}")
    else:
        print("WARNING: No 'Status' column found, using all rows")
    
    # Filter to hash_object and sort_object_post only
    if 'JoinStrategy' in df.columns:
        df = df[df['JoinStrategy'].isin(['hash_object', 'sort_object_post'])].copy()
        print(f"Hash/Sort runs: {len(df)}")
    
    # Convert numeric columns
    numeric_cols = ['MedianTimeMs', 'AvgTimeMs', 'BuildTimeMs', 'ProbeTimeMs',
                   'BuildMaxKeyCount', 'ProbeMaxKeyCount', 'BuildRows', 'ProbeRows',
                   'LeftRows', 'RightRows', 'OutputRows']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def add_trend_line(ax, x_data, y_data, color, label_prefix='', min_points=5):
    """
    Add a trend line to the plot using LOWESS smoothing or polynomial fit.
    
    Args:
        ax: Matplotlib axis
        x_data: X values (pandas Series or array)
        y_data: Y values (pandas Series or array)
        color: Color for the trend line
        label_prefix: Prefix for the legend label
        min_points: Minimum number of points required to draw a trend line
    """
    if len(x_data) < min_points:
        return
    
    # Remove any NaN values
    mask = ~(np.isnan(x_data) | np.isnan(y_data))
    x_clean = np.array(x_data[mask])
    y_clean = np.array(y_data[mask])
    
    if len(x_clean) < min_points:
        return
    
    # Sort by x for smooth line plotting
    sort_idx = np.argsort(x_clean)
    x_sorted = x_clean[sort_idx]
    y_sorted = y_clean[sort_idx]
    
    # Work in log space since we're using log scales
    log_x = np.log10(x_sorted)
    log_y = np.log10(y_sorted)
    
    try:
        if HAS_LOWESS and len(x_sorted) >= 10:
            # Use LOWESS smoothing (more robust, no assumptions about functional form)
            # frac controls the amount of smoothing (smaller = less smooth)
            frac = min(0.4, max(0.1, 10.0 / len(x_sorted)))
            smoothed = lowess(log_y, log_x, frac=frac, return_sorted=False)
            trend_x = x_sorted
            trend_y = 10 ** smoothed
        else:
            # Fall back to polynomial fit (degree 2)
            degree = min(2, len(x_sorted) - 1)
            coeffs = np.polyfit(log_x, log_y, degree)
            poly = np.poly1d(coeffs)
            
            # Generate smooth curve
            log_x_smooth = np.linspace(log_x.min(), log_x.max(), 100)
            log_y_smooth = poly(log_x_smooth)
            trend_x = 10 ** log_x_smooth
            trend_y = 10 ** log_y_smooth
        
        # Plot trend line
        ax.plot(trend_x, trend_y, '--', color=color, linewidth=2.5, 
                alpha=0.8, label=f'{label_prefix} Trend', zorder=10)
        
    except Exception as e:
        # If trend line computation fails, silently skip
        pass

def determine_max_key_count(df):
    """Determine MaxKeyCount (using BuildMaxKeyCount only)."""
    if 'BuildMaxKeyCount' in df.columns:
        df['MaxKeyCount'] = df['BuildMaxKeyCount']
    else:
        print("WARNING: BuildMaxKeyCount column not found")
        df['MaxKeyCount'] = np.nan
    
    return df

def categorize_by_key_type(df):
    """
    Categorize rows by key type:
    - single_fixed: Single fixed-width key (int, long, decimal)
    - single_string: Single variable-width key (string)
    - multiple: Multiple keys (composite keys)
    """
    df = df.copy()
    
    # Determine build key type (smaller side is build)
    def get_build_key_type(row):
        if row.get('LeftRows', 0) <= row.get('RightRows', 0):
            return str(row.get('LeftKeyType', ''))
        else:
            return str(row.get('RightKeyType', ''))
    
    df['BuildKeyType'] = df.apply(get_build_key_type, axis=1)
    
    # Categorize
    def categorize_key_type(key_type_str):
        if pd.isna(key_type_str) or key_type_str == '':
            return 'unknown'
        
        key_type_str = str(key_type_str)
        
        # Check if it's a composite key (contains comma)
        if ',' in key_type_str:
            # Check if comma is inside parentheses (e.g., decimal(18,2))
            paren_depth = 0
            has_comma_outside = False
            for char in key_type_str:
                if char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
                elif char == ',' and paren_depth == 0:
                    has_comma_outside = True
                    break
            
            if has_comma_outside:
                return 'multiple'
            else:
                # Single type with comma in parentheses (e.g., decimal(18,2))
                if 'string' in key_type_str.lower():
                    return 'single_string'
                else:
                    return 'single_fixed'
        else:
            # Single key type
            if 'string' in key_type_str.lower():
                return 'single_string'
            else:
                return 'single_fixed'
    
    df['KeyTypeCategory'] = df['BuildKeyType'].apply(categorize_key_type)
    
    return df

def plot_max_key_count_vs_time_multiplot(df, output_dir='.'):
    """
    Create a 2x2 multi-plot: BuildMaxKeyCount vs Time for all categories.
    
    All plots share the same axis ranges for easy comparison.
    Shows: All data, Single fixed-width, Single string, Multiple keys.
    """
    print("\n" + "="*80)
    print("Multi-Plot: BuildMaxKeyCount vs Time (All Categories)")
    print("="*80)
    
    # Determine MaxKeyCount (using BuildMaxKeyCount)
    df = determine_max_key_count(df)
    
    # Categorize by key type
    df = categorize_by_key_type(df)
    
    # Determine time column (prefer MedianTimeMs, fallback to AvgTimeMs)
    if 'MedianTimeMs' in df.columns:
        time_col = 'MedianTimeMs'
    elif 'AvgTimeMs' in df.columns:
        time_col = 'AvgTimeMs'
    else:
        print("ERROR: No time column found (MedianTimeMs or AvgTimeMs)")
        return
    
    # Filter out NaN values
    plot_df = df[df['MaxKeyCount'].notna() & df[time_col].notna()].copy()
    
    if len(plot_df) == 0:
        print("ERROR: No valid data points after filtering NaN values")
        return
    
    # Determine global axis ranges from all data
    x_min = plot_df['MaxKeyCount'].min()
    x_max = plot_df['MaxKeyCount'].max()
    y_min = plot_df[time_col].min()
    y_max = plot_df[time_col].max()
    
    # Add small padding (10% on each side)
    x_range = x_max - x_min
    y_range = y_max - y_min
    x_min_padded = max(1, x_min - 0.1 * x_range)  # Ensure >= 1 for log scale
    x_max_padded = x_max + 0.1 * x_range
    y_min_padded = max(0.1, y_min - 0.1 * y_range)  # Ensure > 0 for log scale
    y_max_padded = y_max + 0.1 * y_range
    
    print(f"Global axis ranges:")
    print(f"  X-axis (MaxKeyCount): {x_min_padded:.1f} to {x_max_padded:.1f}")
    print(f"  Y-axis (Time): {y_min_padded:.1f} to {y_max_padded:.1f}")
    print()
    
    # Create 2x2 subplot figure
    fig, axes = plt.subplots(2, 2, figsize=(20, 16))
    axes = axes.flatten()
    
    # Categories to plot
    categories = [
        (None, 'All Data'),
        ('single_fixed', 'Single Fixed-Width Key'),
        ('single_string', 'Single String Key'),
        ('multiple', 'Multiple Keys')
    ]
    
    all_correlations = {}
    
    for idx, (category, title) in enumerate(categories):
        ax = axes[idx]
        
        # Filter data for this category
        if category:
            cat_df = plot_df[plot_df['KeyTypeCategory'] == category].copy()
            print(f"  {title}: {len(cat_df)} data points")
        else:
            cat_df = plot_df.copy()
            print(f"  {title}: {len(cat_df)} data points")
        
        if len(cat_df) == 0:
            ax.text(0.5, 0.5, f'No data for\n{title}', 
                   ha='center', va='center', fontsize=14, transform=ax.transAxes)
            ax.set_title(title, fontsize=13, fontweight='bold')
            continue
        
        # Separate by strategy
        hash_df = cat_df[cat_df['JoinStrategy'] == 'hash_object']
        sort_df = cat_df[cat_df['JoinStrategy'] == 'sort_object_post']
        
        # Plot hash joins
        if len(hash_df) > 0:
            ax.scatter(hash_df['MaxKeyCount'], hash_df[time_col], 
                      alpha=0.6, s=50, label='Hash Join', color='#2E86AB', 
                      edgecolors='black', linewidth=0.5)
        
        # Plot sort joins
        if len(sort_df) > 0:
            ax.scatter(sort_df['MaxKeyCount'], sort_df[time_col], 
                      alpha=0.6, s=50, label='Sort Join', color='#A23B72', 
                      edgecolors='black', linewidth=0.5)
        
        # Set same axis ranges for all plots
        ax.set_xlim(x_min_padded, x_max_padded)
        ax.set_ylim(y_min_padded, y_max_padded)
        
        # Labels
        if idx >= 2:  # Bottom row
            ax.set_xlabel('Build Max Key Count', fontsize=11, fontweight='bold')
        if idx % 2 == 0:  # Left column
            ax.set_ylabel(f'Time ({time_col})', fontsize=11, fontweight='bold')
        
        # Title
        ax.set_title(title, fontsize=13, fontweight='bold')
        
        # Log scale
        ax.set_xscale('log')
        ax.set_yscale('log')
        
        # Grid
        ax.grid(True, alpha=0.3, linestyle='--')
        
        # Add trend lines
        if len(hash_df) > 0:
            add_trend_line(ax, hash_df['MaxKeyCount'], hash_df[time_col], 
                          '#1a5c7a', 'Hash')
        if len(sort_df) > 0:
            add_trend_line(ax, sort_df['MaxKeyCount'], sort_df[time_col], 
                          '#6b1f47', 'Sort')
        
        # Legend (only on first plot to avoid clutter)
        if idx == 0:
            ax.legend(loc='upper left', fontsize=10, framealpha=0.9)
        
        # Statistics
        if len(hash_df) > 0:
            hash_corr = hash_df['MaxKeyCount'].corr(hash_df[time_col])
            all_correlations[f'{title} - Hash'] = hash_corr
        
        if len(sort_df) > 0:
            sort_corr = sort_df['MaxKeyCount'].corr(sort_df[time_col])
            all_correlations[f'{title} - Sort'] = sort_corr
    
    plt.suptitle('Join Execution Time vs Build Max Key Count\n(Colored by Strategy, Same Axis Ranges)', 
                 fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout(rect=[0, 0, 1, 0.98])
    
    # Save
    output_path = os.path.join(output_dir, 'max_key_count_vs_time_multiplot.png')
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"\nSaved multi-plot to: {output_path}")
    
    # Print correlations
    print("\nCorrelations by category:")
    for key, corr in sorted(all_correlations.items(), key=lambda x: abs(x[1]), reverse=True):
        print(f"  {key}: {corr:.3f}")
    
    plt.close()

def plot_max_key_count_vs_time(df, output_dir='.', category=None):
    """
    Create scatter plot: BuildMaxKeyCount vs Time, colored by strategy.
    
    This plot shows how the build-side maximum key count correlates with join execution time,
    and whether hash or sort joins behave differently.
    
    Args:
        df: DataFrame with benchmark data
        output_dir: Directory to save the plot
        category: Optional category filter ('single_fixed', 'single_string', 'multiple', or None for all)
    """
    plot_name = 'max_key_count_vs_time'
    title_suffix = ''
    if category:
        plot_name += f'_{category}'
        title_suffix = f' ({category.replace("_", " ").title()})'
    
    print(f"\nPlot: BuildMaxKeyCount vs Time{title_suffix}")
    print("-" * 80)
    
    # Determine MaxKeyCount (using BuildMaxKeyCount)
    df = determine_max_key_count(df)
    
    # Categorize by key type if needed
    if category:
        df = categorize_by_key_type(df)
        df = df[df['KeyTypeCategory'] == category].copy()
        print(f"Filtered to {category}: {len(df)} rows")
    
    # Determine time column (prefer MedianTimeMs, fallback to AvgTimeMs)
    if 'MedianTimeMs' in df.columns:
        time_col = 'MedianTimeMs'
    elif 'AvgTimeMs' in df.columns:
        time_col = 'AvgTimeMs'
    else:
        print("ERROR: No time column found (MedianTimeMs or AvgTimeMs)")
        return
    
    # Filter out NaN values
    plot_df = df[df['MaxKeyCount'].notna() & df[time_col].notna()].copy()
    
    if len(plot_df) == 0:
        print(f"ERROR: No valid data points after filtering NaN values")
        return
    
    print(f"Plotting {len(plot_df)} data points")
    
    # Create figure
    fig, ax = plt.subplots(figsize=(14, 10))
    
    # Separate by strategy
    hash_df = plot_df[plot_df['JoinStrategy'] == 'hash_object']
    sort_df = plot_df[plot_df['JoinStrategy'] == 'sort_object_post']
    
    # Plot hash joins
    if len(hash_df) > 0:
        ax.scatter(hash_df['MaxKeyCount'], hash_df[time_col], 
                  alpha=0.6, s=50, label='Hash Join', color='#2E86AB', edgecolors='black', linewidth=0.5)
    
    # Plot sort joins
    if len(sort_df) > 0:
        ax.scatter(sort_df['MaxKeyCount'], sort_df[time_col], 
                  alpha=0.6, s=50, label='Sort Join', color='#A23B72', edgecolors='black', linewidth=0.5)
    
    # Labels and title
    ax.set_xlabel('Build Max Key Count', fontsize=12, fontweight='bold')
    ax.set_ylabel(f'Time ({time_col})', fontsize=12, fontweight='bold')
    
    title = 'Join Execution Time vs Build Max Key Count\n(Colored by Strategy)'
    if title_suffix:
        title = title.replace('\n', f'{title_suffix}\n')
    ax.set_title(title, fontsize=14, fontweight='bold')
    
    # Log scale for x-axis (key counts can vary widely)
    ax.set_xscale('log')
    ax.set_yscale('log')
    
    # Grid
    ax.grid(True, alpha=0.3, linestyle='--')
    
    # Add trend lines
    if len(hash_df) > 0:
        add_trend_line(ax, hash_df['MaxKeyCount'], hash_df[time_col], 
                      '#1a5c7a', 'Hash')
    if len(sort_df) > 0:
        add_trend_line(ax, sort_df['MaxKeyCount'], sort_df[time_col], 
                      '#6b1f47', 'Sort')
    
    # Legend
    ax.legend(loc='upper left', fontsize=11, framealpha=0.9)
    
    # Statistics
    if len(hash_df) > 0:
        hash_corr = hash_df['MaxKeyCount'].corr(hash_df[time_col])
        print(f"  Hash join correlation: {hash_corr:.3f} ({len(hash_df)} points)")
    
    if len(sort_df) > 0:
        sort_corr = sort_df['MaxKeyCount'].corr(sort_df[time_col])
        print(f"  Sort join correlation: {sort_corr:.3f} ({len(sort_df)} points)")
    
    plt.tight_layout()
    
    # Save
    output_path = os.path.join(output_dir, f'{plot_name}.png')
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"Saved plot to: {output_path}")
    
    plt.close()

def plot_build_time_multiplot(df, output_dir='.'):
    """
    Create a 2x2 multi-plot: BuildMaxKeyCount vs BuildTimeMs for all categories.
    
    All plots share the same axis ranges for easy comparison.
    Shows: All data, Single fixed-width, Single string, Multiple keys.
    """
    print("\n" + "="*80)
    print("Multi-Plot: BuildMaxKeyCount vs BuildTimeMs (All Categories)")
    print("="*80)
    
    # Check if BuildTimeMs column exists
    if 'BuildTimeMs' not in df.columns:
        print("WARNING: BuildTimeMs column not found, skipping build time plots")
        return
    
    # Determine MaxKeyCount (using BuildMaxKeyCount)
    df = determine_max_key_count(df)
    
    # Categorize by key type
    df = categorize_by_key_type(df)
    
    # Filter out NaN values
    plot_df = df[df['MaxKeyCount'].notna() & df['BuildTimeMs'].notna()].copy()
    
    if len(plot_df) == 0:
        print("ERROR: No valid data points after filtering NaN values")
        return
    
    # Determine global axis ranges from all data
    x_min = plot_df['MaxKeyCount'].min()
    x_max = plot_df['MaxKeyCount'].max()
    y_min = plot_df['BuildTimeMs'].min()
    y_max = plot_df['BuildTimeMs'].max()
    
    # Add small padding (10% on each side)
    x_range = x_max - x_min
    y_range = y_max - y_min
    x_min_padded = max(1, x_min - 0.1 * x_range)  # Ensure >= 1 for log scale
    x_max_padded = x_max + 0.1 * x_range
    y_min_padded = max(0.1, y_min - 0.1 * y_range)  # Ensure > 0 for log scale
    y_max_padded = y_max + 0.1 * y_range
    
    print(f"Global axis ranges:")
    print(f"  X-axis (MaxKeyCount): {x_min_padded:.1f} to {x_max_padded:.1f}")
    print(f"  Y-axis (BuildTimeMs): {y_min_padded:.1f} to {y_max_padded:.1f}")
    print()
    
    # Create 2x2 subplot figure
    fig, axes = plt.subplots(2, 2, figsize=(20, 16))
    axes = axes.flatten()
    
    # Categories to plot
    categories = [
        (None, 'All Data'),
        ('single_fixed', 'Single Fixed-Width Key'),
        ('single_string', 'Single String Key'),
        ('multiple', 'Multiple Keys')
    ]
    
    all_correlations = {}
    
    for idx, (category, title) in enumerate(categories):
        ax = axes[idx]
        
        # Filter data for this category
        if category:
            cat_df = plot_df[plot_df['KeyTypeCategory'] == category].copy()
            print(f"  {title}: {len(cat_df)} data points")
        else:
            cat_df = plot_df.copy()
            print(f"  {title}: {len(cat_df)} data points")
        
        if len(cat_df) == 0:
            ax.text(0.5, 0.5, f'No data for\n{title}', 
                   ha='center', va='center', fontsize=14, transform=ax.transAxes)
            ax.set_title(title, fontsize=13, fontweight='bold')
            continue
        
        # Separate by strategy
        hash_df = cat_df[cat_df['JoinStrategy'] == 'hash_object']
        sort_df = cat_df[cat_df['JoinStrategy'] == 'sort_object_post']
        
        # Plot hash joins
        if len(hash_df) > 0:
            ax.scatter(hash_df['MaxKeyCount'], hash_df['BuildTimeMs'], 
                      alpha=0.6, s=50, label='Hash Join', color='#2E86AB', 
                      edgecolors='black', linewidth=0.5)
        
        # Plot sort joins
        if len(sort_df) > 0:
            ax.scatter(sort_df['MaxKeyCount'], sort_df['BuildTimeMs'], 
                      alpha=0.6, s=50, label='Sort Join', color='#A23B72', 
                      edgecolors='black', linewidth=0.5)
        
        # Set same axis ranges for all plots
        ax.set_xlim(x_min_padded, x_max_padded)
        ax.set_ylim(y_min_padded, y_max_padded)
        
        # Labels
        if idx >= 2:  # Bottom row
            ax.set_xlabel('Build Max Key Count', fontsize=11, fontweight='bold')
        if idx % 2 == 0:  # Left column
            ax.set_ylabel('Build Time (ms)', fontsize=11, fontweight='bold')
        
        # Title
        ax.set_title(title, fontsize=13, fontweight='bold')
        
        # Log scale
        ax.set_xscale('log')
        ax.set_yscale('log')
        
        # Grid
        ax.grid(True, alpha=0.3, linestyle='--')
        
        # Add trend lines
        if len(hash_df) > 0:
            add_trend_line(ax, hash_df['MaxKeyCount'], hash_df['BuildTimeMs'], 
                          '#1a5c7a', 'Hash')
        if len(sort_df) > 0:
            add_trend_line(ax, sort_df['MaxKeyCount'], sort_df['BuildTimeMs'], 
                          '#6b1f47', 'Sort')
        
        # Legend (only on first plot to avoid clutter)
        if idx == 0:
            ax.legend(loc='upper left', fontsize=10, framealpha=0.9)
        
        # Statistics
        if len(hash_df) > 0:
            hash_corr = hash_df['MaxKeyCount'].corr(hash_df['BuildTimeMs'])
            all_correlations[f'{title} - Hash'] = hash_corr
        
        if len(sort_df) > 0:
            sort_corr = sort_df['MaxKeyCount'].corr(sort_df['BuildTimeMs'])
            all_correlations[f'{title} - Sort'] = sort_corr
    
    plt.suptitle('Build Time vs Build Max Key Count\n(Colored by Strategy, Same Axis Ranges)', 
                 fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout(rect=[0, 0, 1, 0.98])
    
    # Save
    output_path = os.path.join(output_dir, 'build_time_multiplot.png')
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"\nSaved multi-plot to: {output_path}")
    
    # Print correlations
    print("\nCorrelations by category:")
    for key, corr in sorted(all_correlations.items(), key=lambda x: abs(x[1]), reverse=True):
        print(f"  {key}: {corr:.3f}")
    
    plt.close()

def plot_probe_time_multiplot(df, output_dir='.'):
    """
    Create a 2x2 multi-plot: ProbeMaxKeyCount vs ProbeTimeMs for all categories.
    
    All plots share the same axis ranges for easy comparison.
    Shows: All data, Single fixed-width, Single string, Multiple keys.
    """
    print("\n" + "="*80)
    print("Multi-Plot: ProbeMaxKeyCount vs ProbeTimeMs (All Categories)")
    print("="*80)
    
    # Check if ProbeTimeMs column exists
    if 'ProbeTimeMs' not in df.columns or 'ProbeMaxKeyCount' not in df.columns:
        print("WARNING: ProbeTimeMs or ProbeMaxKeyCount column not found, skipping probe time plots")
        return
    
    # Categorize by key type (using probe side key type)
    df = df.copy()
    
    # Determine probe key type (larger side is probe)
    def get_probe_key_type(row):
        if row.get('LeftRows', 0) > row.get('RightRows', 0):
            return str(row.get('LeftKeyType', ''))
        else:
            return str(row.get('RightKeyType', ''))
    
    df['ProbeKeyType'] = df.apply(get_probe_key_type, axis=1)
    
    # Categorize
    def categorize_key_type(key_type_str):
        if pd.isna(key_type_str) or key_type_str == '':
            return 'unknown'
        
        key_type_str = str(key_type_str)
        
        # Check if it's a composite key (contains comma)
        if ',' in key_type_str:
            # Check if comma is inside parentheses (e.g., decimal(18,2))
            paren_depth = 0
            has_comma_outside = False
            for char in key_type_str:
                if char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
                elif char == ',' and paren_depth == 0:
                    has_comma_outside = True
                    break
            
            if has_comma_outside:
                return 'multiple'
            else:
                # Single type with comma in parentheses (e.g., decimal(18,2))
                if 'string' in key_type_str.lower():
                    return 'single_string'
                else:
                    return 'single_fixed'
        else:
            # Single key type
            if 'string' in key_type_str.lower():
                return 'single_string'
            else:
                return 'single_fixed'
    
    df['KeyTypeCategory'] = df['ProbeKeyType'].apply(categorize_key_type)
    
    # Filter out NaN values
    plot_df = df[df['ProbeMaxKeyCount'].notna() & df['ProbeTimeMs'].notna()].copy()
    
    if len(plot_df) == 0:
        print("ERROR: No valid data points after filtering NaN values")
        return
    
    # Determine global axis ranges from all data
    x_min = plot_df['ProbeMaxKeyCount'].min()
    x_max = plot_df['ProbeMaxKeyCount'].max()
    y_min = plot_df['ProbeTimeMs'].min()
    y_max = plot_df['ProbeTimeMs'].max()
    
    # Add small padding (10% on each side)
    x_range = x_max - x_min
    y_range = y_max - y_min
    x_min_padded = max(1, x_min - 0.1 * x_range)  # Ensure >= 1 for log scale
    x_max_padded = x_max + 0.1 * x_range
    y_min_padded = max(0.1, y_min - 0.1 * y_range)  # Ensure > 0 for log scale
    y_max_padded = y_max + 0.1 * y_range
    
    print(f"Global axis ranges:")
    print(f"  X-axis (ProbeMaxKeyCount): {x_min_padded:.1f} to {x_max_padded:.1f}")
    print(f"  Y-axis (ProbeTimeMs): {y_min_padded:.1f} to {y_max_padded:.1f}")
    print()
    
    # Create 2x2 subplot figure
    fig, axes = plt.subplots(2, 2, figsize=(20, 16))
    axes = axes.flatten()
    
    # Categories to plot
    categories = [
        (None, 'All Data'),
        ('single_fixed', 'Single Fixed-Width Key'),
        ('single_string', 'Single String Key'),
        ('multiple', 'Multiple Keys')
    ]
    
    all_correlations = {}
    
    for idx, (category, title) in enumerate(categories):
        ax = axes[idx]
        
        # Filter data for this category
        if category:
            cat_df = plot_df[plot_df['KeyTypeCategory'] == category].copy()
            print(f"  {title}: {len(cat_df)} data points")
        else:
            cat_df = plot_df.copy()
            print(f"  {title}: {len(cat_df)} data points")
        
        if len(cat_df) == 0:
            ax.text(0.5, 0.5, f'No data for\n{title}', 
                   ha='center', va='center', fontsize=14, transform=ax.transAxes)
            ax.set_title(title, fontsize=13, fontweight='bold')
            continue
        
        # Separate by strategy
        hash_df = cat_df[cat_df['JoinStrategy'] == 'hash_object']
        sort_df = cat_df[cat_df['JoinStrategy'] == 'sort_object_post']
        
        # Plot hash joins
        if len(hash_df) > 0:
            ax.scatter(hash_df['ProbeMaxKeyCount'], hash_df['ProbeTimeMs'], 
                      alpha=0.6, s=50, label='Hash Join', color='#2E86AB', 
                      edgecolors='black', linewidth=0.5)
        
        # Plot sort joins
        if len(sort_df) > 0:
            ax.scatter(sort_df['ProbeMaxKeyCount'], sort_df['ProbeTimeMs'], 
                      alpha=0.6, s=50, label='Sort Join', color='#A23B72', 
                      edgecolors='black', linewidth=0.5)
        
        # Set same axis ranges for all plots
        ax.set_xlim(x_min_padded, x_max_padded)
        ax.set_ylim(y_min_padded, y_max_padded)
        
        # Labels
        if idx >= 2:  # Bottom row
            ax.set_xlabel('Probe Max Key Count', fontsize=11, fontweight='bold')
        if idx % 2 == 0:  # Left column
            ax.set_ylabel('Probe Time (ms)', fontsize=11, fontweight='bold')
        
        # Title
        ax.set_title(title, fontsize=13, fontweight='bold')
        
        # Log scale
        ax.set_xscale('log')
        ax.set_yscale('log')
        
        # Grid
        ax.grid(True, alpha=0.3, linestyle='--')
        
        # Add trend lines
        if len(hash_df) > 0:
            add_trend_line(ax, hash_df['ProbeMaxKeyCount'], hash_df['ProbeTimeMs'], 
                          '#1a5c7a', 'Hash')
        if len(sort_df) > 0:
            add_trend_line(ax, sort_df['ProbeMaxKeyCount'], sort_df['ProbeTimeMs'], 
                          '#6b1f47', 'Sort')
        
        # Legend (only on first plot to avoid clutter)
        if idx == 0:
            ax.legend(loc='upper left', fontsize=10, framealpha=0.9)
        
        # Statistics
        if len(hash_df) > 0:
            hash_corr = hash_df['ProbeMaxKeyCount'].corr(hash_df['ProbeTimeMs'])
            all_correlations[f'{title} - Hash'] = hash_corr
        
        if len(sort_df) > 0:
            sort_corr = sort_df['ProbeMaxKeyCount'].corr(sort_df['ProbeTimeMs'])
            all_correlations[f'{title} - Sort'] = sort_corr
    
    plt.suptitle('Probe Time vs Probe Max Key Count\n(Colored by Strategy, Same Axis Ranges)', 
                 fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout(rect=[0, 0, 1, 0.98])
    
    # Save
    output_path = os.path.join(output_dir, 'probe_time_multiplot.png')
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"\nSaved multi-plot to: {output_path}")
    
    # Print correlations
    print("\nCorrelations by category:")
    for key, corr in sorted(all_correlations.items(), key=lambda x: abs(x[1]), reverse=True):
        print(f"  {key}: {corr:.3f}")
    
    plt.close()

def main():
    """Main execution."""
    # Determine TSV path
    if len(sys.argv) > 1:
        tsv_path = sys.argv[1]
    else:
        tsv_path = DEFAULT_TSV_PATH
    
    # Determine output directory
    if len(sys.argv) > 2:
        # Use provided output directory
        output_dir = sys.argv[2]
    else:
        # Create visualizations directory next to TSV file
        tsv_dir = os.path.dirname(os.path.abspath(tsv_path))
        if tsv_dir:
            output_dir = os.path.join(tsv_dir, 'visualizations')
        else:
            output_dir = 'visualizations'
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    print("="*80)
    print("BENCHMARK RESULTS VISUALIZATION")
    print("="*80)
    print()
    print(f"TSV path: {tsv_path}")
    print(f"Output directory: {output_dir}")
    print()
    
    # Load data
    df = load_benchmark_data(tsv_path)
    
    if len(df) == 0:
        print("ERROR: No data to plot")
        sys.exit(1)
    
    # Generate plots
    # 1. Multi-plot with all categories (same axis ranges for comparison)
    print("\n" + "="*80)
    print("TOTAL EXECUTION TIME PLOTS")
    print("="*80)
    plot_max_key_count_vs_time_multiplot(df, output_dir)
    
    # 2. Build time plots
    print("\n" + "="*80)
    print("BUILD TIME PLOTS")
    print("="*80)
    plot_build_time_multiplot(df, output_dir)
    
    # 3. Probe time plots
    print("\n" + "="*80)
    print("PROBE TIME PLOTS")
    print("="*80)
    plot_probe_time_multiplot(df, output_dir)
    
    # 4. Individual plots (for detailed inspection)
    print("\n" + "="*80)
    print("INDIVIDUAL PLOTS (Total Time)")
    print("="*80)
    # All data together
    plot_max_key_count_vs_time(df, output_dir, category=None)
    
    # Single fixed-width key
    plot_max_key_count_vs_time(df, output_dir, category='single_fixed')
    
    # Single variable-width key (string)
    plot_max_key_count_vs_time(df, output_dir, category='single_string')
    
    # Multiple keys (composite)
    plot_max_key_count_vs_time(df, output_dir, category='multiple')
    
    print("\n" + "="*80)
    print("Visualization complete!")
    print("="*80)

if __name__ == "__main__":
    main()

