#!/usr/bin/env python3
"""
Build Side Sweep Analysis Script

This script analyzes the TSV output from build_side_sweep_benchmark.scala
and generates visualizations showing the impact of build side selection.

Usage:
    python analyze_build_side_sweep.py /path/to/build_side_sweep.tsv

Output:
    - Multiple graphs showing performance across different dimensions
    - Analysis summary printed to console
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import os
from pathlib import Path

# Set style for better-looking plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10


def load_data(tsv_path):
    """Load and preprocess the benchmark TSV file."""
    print(f"Loading data from: {tsv_path}")
    
    df = pd.read_csv(tsv_path, sep='\t')
    
    # Filter out error cases for main analysis
    df_success = df[df['Status'] == 'SUCCESS'].copy()
    
    print(f"Total tests: {len(df)}")
    print(f"Successful tests: {len(df_success)}")
    print(f"Failed tests: {len(df) - len(df_success)}")
    
    if len(df_success) == 0:
        print("\nERROR: No successful tests found in the data!")
        print("The benchmark may not have completed yet, or all tests failed.")
        sys.exit(1)
    
    # Extract metadata from TestName
    # Format: L{leftPct}_R{rightPct}_{cardinality}_{overlap}_{strategy}_{buildSide}
    def parse_test_name(name):
        try:
            parts = name.split('_')
            return {
                'LeftPct': int(parts[0][1:]),  # Remove 'L' prefix
                'RightPct': int(parts[1][1:]),  # Remove 'R' prefix
                'Cardinality': parts[2] + '_' + parts[3],
                'Overlap': parts[4] + '_' + parts[5],
                'Strategy': parts[6],
                'BuildSide': parts[7]
            }
        except (IndexError, ValueError) as e:
            print(f"Warning: Could not parse test name: {name}")
            return {
                'LeftPct': 0,
                'RightPct': 0,
                'Cardinality': 'unknown',
                'Overlap': 'unknown',
                'Strategy': 'unknown',
                'BuildSide': 'unknown'
            }
    
    parsed = df_success['TestName'].apply(parse_test_name)
    parsed_df = pd.DataFrame(parsed.tolist())
    
    # Drop any existing columns that we're about to add from parsing
    # This prevents duplicate column names
    columns_to_drop = [col for col in parsed_df.columns if col in df_success.columns]
    if columns_to_drop:
        print(f"\nDropping existing columns to avoid duplicates: {columns_to_drop}")
        df_success = df_success.drop(columns=columns_to_drop)
    
    df_success = pd.concat([df_success, parsed_df], axis=1)
    
    # Debug: Print column names to see what we have
    print(f"\nParsed columns: {parsed_df.columns.tolist()}")
    print(f"Sample parsed data:\n{parsed_df.head()}")
    print(f"\nFinal dataframe columns: {df_success.columns.tolist()}")
    
    # Convert numeric columns to proper types
    numeric_columns = ['LeftRows', 'RightRows', 'OutputRows', 'AvgTimeMs', 'MedianTimeMs', 
                       'MinTimeMs', 'MaxTimeMs', 'StdDevMs', 'WallClockMs',
                       'CreateBuildObjectMs', 'ExecuteJoinMs', 'LeftPct', 'RightPct']
    
    for col in numeric_columns:
        if col in df_success.columns:
            df_success[col] = pd.to_numeric(df_success[col], errors='coerce')
    
    # Calculate left/right ratio for plotting (avoid division by zero)
    df_success['LeftRightRatio'] = df_success.apply(
        lambda row: row['LeftRows'] / row['RightRows'] if row['RightRows'] > 0 else float('inf'),
        axis=1
    )
    
    return df_success


def plot_build_side_comparison(df, cardinality, overlap, strategy, output_dir):
    """
    Plot performance comparison between LeftBuild and RightBuild
    as the distribution of rows changes.
    """
    subset = df[
        (df['Cardinality'] == cardinality) &
        (df['Overlap'] == overlap) &
        (df['Strategy'] == strategy)
    ].copy()
    
    if len(subset) == 0:
        print(f"No data for {cardinality}, {overlap}, {strategy}")
        return
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    # Plot 1: Median time vs left percentage
    for build_side in ['LeftBuild', 'RightBuild']:
        data = subset[subset['BuildSide'] == build_side].sort_values('LeftPct')
        ax1.plot(data['LeftPct'], data['MedianTimeMs'], 
                marker='o', label=build_side, linewidth=2, markersize=6)
    
    ax1.set_xlabel('Left Table Percentage (%)', fontsize=12)
    ax1.set_ylabel('Median Time (ms)', fontsize=12)
    ax1.set_title(f'Build Side Performance: {strategy} Join\n'
                  f'{cardinality}, {overlap}', fontsize=14, fontweight='bold')
    ax1.legend(fontsize=11)
    ax1.grid(True, alpha=0.3)
    ax1.set_xscale('log')
    
    # Add vertical line at 50% (balanced)
    ax1.axvline(x=50, color='gray', linestyle='--', alpha=0.5, label='Balanced (50/50)')
    
    # Plot 2: Speedup of choosing optimal build side
    left_build = subset[subset['BuildSide'] == 'LeftBuild'].sort_values('LeftPct')
    right_build = subset[subset['BuildSide'] == 'RightBuild'].sort_values('LeftPct')
    
    # Merge on LeftPct to compare
    merged = pd.merge(left_build[['LeftPct', 'MedianTimeMs']], 
                     right_build[['LeftPct', 'MedianTimeMs']], 
                     on='LeftPct', suffixes=('_Left', '_Right'))
    
    # Calculate which is better and the speedup
    merged['OptimalTime'] = merged[['MedianTimeMs_Left', 'MedianTimeMs_Right']].min(axis=1)
    merged['SuboptimalTime'] = merged[['MedianTimeMs_Left', 'MedianTimeMs_Right']].max(axis=1)
    merged['Speedup'] = merged['SuboptimalTime'] / merged['OptimalTime']
    merged['PercentSlower'] = (merged['Speedup'] - 1) * 100
    
    # Color by which is optimal
    merged['OptimalSide'] = merged.apply(
        lambda x: 'Left' if x['MedianTimeMs_Left'] < x['MedianTimeMs_Right'] else 'Right', axis=1)
    
    colors = merged['OptimalSide'].map({'Left': 'blue', 'Right': 'red'})
    
    ax2.bar(range(len(merged)), merged['PercentSlower'], color=colors, alpha=0.7)
    ax2.set_xlabel('Test Index (sorted by Left %)', fontsize=12)
    ax2.set_ylabel('Penalty for Wrong Build Side (%)', fontsize=12)
    ax2.set_title('Performance Penalty for Suboptimal Build Side Selection', 
                  fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Add legend for colors
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='blue', alpha=0.7, label='Left is optimal'),
        Patch(facecolor='red', alpha=0.7, label='Right is optimal')
    ]
    ax2.legend(handles=legend_elements, fontsize=11)
    
    plt.tight_layout()
    filename = f"build_side_comparison_{strategy}_{cardinality}_{overlap}.png"
    filepath = os.path.join(output_dir, filename)
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    print(f"  Saved: {filename}")
    plt.close()
    
    # Also create a simpler version with just the performance comparison (no penalty chart)
    # This avoids noise from percentage calculations when times are very small
    fig_simple, ax_simple = plt.subplots(1, 1, figsize=(14, 6))
    
    for build_side in ['LeftBuild', 'RightBuild']:
        data = subset[subset['BuildSide'] == build_side].sort_values('LeftPct')
        ax_simple.plot(data['LeftPct'], data['MedianTimeMs'], 
                marker='o', label=build_side, linewidth=2, markersize=6)
    
    ax_simple.set_xlabel('Left Table Percentage (%)', fontsize=12)
    ax_simple.set_ylabel('Median Time (ms)', fontsize=12)
    ax_simple.set_title(f'Build Side Performance: {strategy} Join\n'
                  f'{cardinality}, {overlap}', fontsize=14, fontweight='bold')
    ax_simple.legend(fontsize=11)
    ax_simple.grid(True, alpha=0.3)
    ax_simple.set_xscale('log')
    ax_simple.axvline(x=50, color='gray', linestyle='--', alpha=0.5, label='Balanced (50/50)')
    
    plt.tight_layout()
    filename_simple = f"build_side_performance_{strategy}_{cardinality}_{overlap}.png"
    filepath_simple = os.path.join(output_dir, filename_simple)
    plt.savefig(filepath_simple, dpi=150, bbox_inches='tight')
    print(f"  Saved: {filename_simple}")
    plt.close()
    
    return merged


def plot_detailed_timing_breakdown(df, cardinality, overlap, output_dir):
    """
    Plot the breakdown of build vs probe time from detailed timings.
    """
    subset = df[
        (df['Cardinality'] == cardinality) &
        (df['Overlap'] == overlap) &
        (df['CreateBuildObjectMs'].notna())  # Only rows with detailed timings
    ].copy()
    
    if len(subset) == 0:
        print(f"No detailed timing data for {cardinality}, {overlap}")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    for idx, strategy in enumerate(['Hash', 'Sort']):
        for jdx, build_side in enumerate(['LeftBuild', 'RightBuild']):
            ax = axes[idx][jdx]
            
            data = subset[
                (subset['Strategy'] == strategy) &
                (subset['BuildSide'] == build_side)
            ].sort_values('LeftPct')
            
            if len(data) == 0:
                continue
            
            # Stack the timing components
            x = range(len(data))
            
            # Note: Since no remapping, remap times should be 0
            build_time = data['CreateBuildObjectMs'].values
            probe_time = data['ExecuteJoinMs'].values
            
            ax.bar(x, build_time, label='Build Object Creation', color='steelblue', alpha=0.8)
            ax.bar(x, probe_time, bottom=build_time, label='Join Execution (Probe)', 
                  color='coral', alpha=0.8)
            
            ax.set_xlabel('Test Index (sorted by Left %)', fontsize=11)
            ax.set_ylabel('Time (ms)', fontsize=11)
            ax.set_title(f'{strategy} Join - {build_side}\n{cardinality}, {overlap}', 
                        fontsize=12, fontweight='bold')
            ax.legend(fontsize=10)
            ax.grid(True, alpha=0.3, axis='y')
            
            # Add left percentage labels
            if len(data) <= 20:  # Only add labels if not too many points
                ax.set_xticks(x[::2])  # Every other label
                ax.set_xticklabels(data['LeftPct'].values[::2], rotation=45)
    
    plt.suptitle('Detailed Timing Breakdown: Build vs Probe Phase', 
                 fontsize=16, fontweight='bold', y=1.00)
    plt.tight_layout()
    
    filename = f"timing_breakdown_{cardinality}_{overlap}.png"
    filepath = os.path.join(output_dir, filename)
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    print(f"  Saved: {filename}")
    plt.close()


def plot_hash_vs_sort(df, cardinality, overlap, output_dir):
    """Compare Hash vs Sort strategies."""
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    for idx, build_side in enumerate(['LeftBuild', 'RightBuild']):
        ax = axes[idx]
        
        for strategy in ['Hash', 'Sort']:
            data = df[
                (df['Cardinality'] == cardinality) &
                (df['Overlap'] == overlap) &
                (df['BuildSide'] == build_side) &
                (df['Strategy'] == strategy)
            ].sort_values('LeftPct')
            
            if len(data) > 0:
                ax.plot(data['LeftPct'], data['MedianTimeMs'], 
                       marker='o', label=strategy, linewidth=2, markersize=6)
        
        ax.set_xlabel('Left Table Percentage (%)', fontsize=12)
        ax.set_ylabel('Median Time (ms)', fontsize=12)
        ax.set_title(f'{build_side}\n{cardinality}, {overlap}', 
                    fontsize=13, fontweight='bold')
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3)
        ax.set_xscale('log')
        ax.axvline(x=50, color='gray', linestyle='--', alpha=0.5)
    
    plt.suptitle('Hash vs Sort Join Performance', fontsize=16, fontweight='bold')
    plt.tight_layout()
    
    filename = f"hash_vs_sort_{cardinality}_{overlap}.png"
    filepath = os.path.join(output_dir, filename)
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    print(f"  Saved: {filename}")
    plt.close()


def print_summary_statistics(df):
    """Print summary statistics about the speedup from optimal build side selection."""
    print("\n" + "="*80)
    print("SUMMARY STATISTICS: Build Side Selection Impact")
    print("="*80)
    
    # Get unique values safely
    def safe_unique(series_or_df):
        if isinstance(series_or_df, pd.DataFrame):
            series_or_df = series_or_df.iloc[:, 0]
        return series_or_df.dropna().unique()
    
    for cardinality in safe_unique(df['Cardinality']):
        for overlap in safe_unique(df['Overlap']):
            for strategy in safe_unique(df['Strategy']):
                subset = df[
                    (df['Cardinality'] == cardinality) &
                    (df['Overlap'] == overlap) &
                    (df['Strategy'] == strategy)
                ]
                
                if len(subset) == 0:
                    continue
                
                left_build = subset[subset['BuildSide'] == 'LeftBuild'].sort_values('LeftPct')
                right_build = subset[subset['BuildSide'] == 'RightBuild'].sort_values('LeftPct')
                
                if len(left_build) == 0 or len(right_build) == 0:
                    continue
                
                merged = pd.merge(left_build[['LeftPct', 'MedianTimeMs']], 
                                 right_build[['LeftPct', 'MedianTimeMs']], 
                                 on='LeftPct', suffixes=('_Left', '_Right'))
                
                merged['Speedup'] = merged[['MedianTimeMs_Left', 'MedianTimeMs_Right']].max(axis=1) / \
                                   merged[['MedianTimeMs_Left', 'MedianTimeMs_Right']].min(axis=1)
                
                print(f"\n{strategy} - {cardinality} - {overlap}:")
                print(f"  Average speedup from optimal build side (based on median times): {merged['Speedup'].mean():.2f}x")
                print(f"  Max speedup: {merged['Speedup'].max():.2f}x")
                print(f"  Min speedup: {merged['Speedup'].min():.2f}x")
                print(f"  Median speedup: {merged['Speedup'].median():.2f}x")


def main():
    if len(sys.argv) != 2:
        print("Usage: python analyze_build_side_sweep.py <path_to_tsv_file>")
        sys.exit(1)
    
    tsv_path = sys.argv[1]
    
    if not os.path.exists(tsv_path):
        print(f"Error: File not found: {tsv_path}")
        sys.exit(1)
    
    # Load data
    df = load_data(tsv_path)
    
    # Verify we have the required columns
    required_cols = ['Cardinality', 'Overlap', 'Strategy', 'BuildSide']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"\nERROR: Missing required columns: {missing_cols}")
        print("Available columns:", df.columns.tolist())
        sys.exit(1)
    
    # Create output directory
    output_dir = os.path.join(os.path.dirname(tsv_path), "analysis_plots")
    os.makedirs(output_dir, exist_ok=True)
    print(f"\nSaving plots to: {output_dir}\n")
    
    # Get unique values for each dimension (filter out NaN and 'unknown')
    def get_valid_unique_values(column_name, exclude_unknown=True):
        """Get unique values from a column, excluding NaN and optionally 'unknown'."""
        if column_name not in df.columns:
            print(f"ERROR: Column '{column_name}' not found in dataframe")
            print(f"Available columns: {df.columns.tolist()}")
            return []
        
        series = df[column_name]
        # Make sure it's a Series
        if isinstance(series, pd.DataFrame):
            print(f"WARNING: '{column_name}' returned a DataFrame instead of Series")
            series = series.iloc[:, 0]  # Take first column
        
        unique_vals = series.dropna().unique()
        if exclude_unknown:
            unique_vals = [v for v in unique_vals if v != 'unknown']
        # Convert to strings for consistent sorting
        return sorted([str(v) for v in unique_vals])
    
    cardinalities = get_valid_unique_values('Cardinality')
    overlaps = get_valid_unique_values('Overlap')
    strategies = get_valid_unique_values('Strategy')
    
    print(f"Found data for:")
    print(f"  Cardinalities: {cardinalities}")
    print(f"  Overlaps: {overlaps}")
    print(f"  Strategies: {strategies}")
    print()
    
    # Generate plots for each cardinality and overlap combination
    print("Generating build side comparison plots...")
    all_speedup_data = []
    
    for cardinality in cardinalities:
        for overlap in overlaps:
            for strategy in strategies:
                try:
                    speedup_data = plot_build_side_comparison(df, cardinality, overlap, strategy, output_dir)
                    if speedup_data is not None:
                        speedup_data['Cardinality'] = cardinality
                        speedup_data['Overlap'] = overlap
                        speedup_data['Strategy'] = strategy
                        all_speedup_data.append(speedup_data)
                except Exception as e:
                    print(f"  Warning: Could not generate plot for {cardinality}/{overlap}/{strategy}: {e}")
    
    print("\nGenerating detailed timing breakdown plots...")
    for cardinality in cardinalities:
        for overlap in overlaps:
            try:
                plot_detailed_timing_breakdown(df, cardinality, overlap, output_dir)
            except Exception as e:
                print(f"  Warning: Could not generate timing breakdown for {cardinality}/{overlap}: {e}")
    
    print("\nGenerating hash vs sort comparison plots...")
    for cardinality in cardinalities:
        for overlap in overlaps:
            try:
                plot_hash_vs_sort(df, cardinality, overlap, output_dir)
            except Exception as e:
                print(f"  Warning: Could not generate hash vs sort plot for {cardinality}/{overlap}: {e}")
    
    # Print summary statistics
    try:
        print_summary_statistics(df)
    except Exception as e:
        print(f"\nWarning: Could not generate summary statistics: {e}")
    
    print("\n" + "="*80)
    print(f"Analysis complete! All plots saved to: {output_dir}")
    print("="*80)


if __name__ == "__main__":
    main()

