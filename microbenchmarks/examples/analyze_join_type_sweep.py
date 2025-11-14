#!/usr/bin/env python3
"""
Join Type Sweep Analysis Script

This script analyzes the TSV output from join_type_sweep_benchmark.scala
and generates visualizations showing the performance of different join types.

Usage:
    python analyze_join_type_sweep.py /path/to/join_type_sweep.tsv
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import os

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10


def load_data(tsv_path):
    """Load and preprocess the benchmark TSV file."""
    print(f"Loading data from: {tsv_path}")
    
    df = pd.read_csv(tsv_path, sep='\t')
    df_success = df[df['Status'] == 'SUCCESS'].copy()
    
    print(f"Total tests: {len(df)}")
    print(f"Successful tests: {len(df_success)}")
    print(f"Failed tests: {len(df) - len(df_success)}")
    
    if len(df_success) == 0:
        print("\nERROR: No successful tests found!")
        sys.exit(1)
    
    # Parse test names
    def parse_test_name(name):
        try:
            parts = name.split('_')
            return {
                'LeftPct': int(parts[0][1:]),
                'RightPct': int(parts[1][1:]),
                'Cardinality': parts[2] + '_' + parts[3],
                'Overlap': parts[4] + '_' + parts[5],
                'JoinTypeName': parts[6],
                'BuildSide': parts[7]
            }
        except (IndexError, ValueError) as e:
            print(f"Warning: Could not parse test name: {name}")
            return {
                'LeftPct': 0,
                'RightPct': 0,
                'Cardinality': 'unknown',
                'Overlap': 'unknown',
                'JoinTypeName': 'unknown',
                'BuildSide': 'unknown'
            }
    
    parsed = df_success['TestName'].apply(parse_test_name)
    parsed_df = pd.DataFrame(parsed.tolist())
    
    # Drop duplicates if they exist
    columns_to_drop = [col for col in parsed_df.columns if col in df_success.columns]
    if columns_to_drop:
        df_success = df_success.drop(columns=columns_to_drop)
    
    df_success = pd.concat([df_success, parsed_df], axis=1)
    
    # Convert numeric columns
    numeric_columns = ['LeftRows', 'RightRows', 'OutputRows', 'AvgTimeMs', 'MedianTimeMs',
                       'MinTimeMs', 'MaxTimeMs', 'StdDevMs', 'WallClockMs',
                       'CreateBuildObjectMs', 'ExecuteJoinMs', 'LeftPct', 'RightPct']
    
    for col in numeric_columns:
        if col in df_success.columns:
            df_success[col] = pd.to_numeric(df_success[col], errors='coerce')
    
    df_success['LeftRightRatio'] = df_success.apply(
        lambda row: row['LeftRows'] / row['RightRows'] if row['RightRows'] > 0 else float('inf'),
        axis=1
    )
    
    return df_success


def plot_join_types_by_overlap(df, overlap, output_dir):
    """
    Plot performance of different join types for a given overlap scenario.
    """
    subset = df[df['Overlap'] == overlap].copy()
    
    if len(subset) == 0:
        print(f"No data for {overlap}")
        return
    
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    axes = axes.flatten()
    
    join_types = sorted(subset['JoinTypeName'].dropna().unique())
    
    for idx, join_type in enumerate(join_types):
        if idx >= 6:  # Max 6 subplots
            break
        
        ax = axes[idx]
        join_data = subset[subset['JoinTypeName'] == join_type]
        
        # Group by build side if there are multiple
        build_sides = sorted(join_data['BuildSide'].dropna().unique())
        
        for build_side in build_sides:
            data = join_data[join_data['BuildSide'] == build_side].sort_values('LeftPct')
            if len(data) > 0:
                label = f"{build_side}" if len(build_sides) > 1 else join_type
                ax.plot(data['LeftPct'], data['MedianTimeMs'],
                       marker='o', label=label, linewidth=2, markersize=6)
        
        ax.set_xlabel('Left Table Percentage (%)', fontsize=10)
        ax.set_ylabel('Median Time (ms)', fontsize=10)
        ax.set_title(f'{join_type}', fontsize=12, fontweight='bold')
        if len(build_sides) > 1:
            ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.set_xscale('log')
        ax.axvline(x=50, color='gray', linestyle='--', alpha=0.5)
    
    # Hide unused subplots
    for idx in range(len(join_types), 6):
        axes[idx].set_visible(False)
    
    plt.suptitle(f'Join Type Performance: {overlap}\n(100% Cardinality, HashObject)',
                 fontsize=16, fontweight='bold')
    plt.tight_layout()
    
    filename = f"join_types_{overlap}.png"
    filepath = os.path.join(output_dir, filename)
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    print(f"  Saved: {filename}")
    plt.close()


def plot_join_type_comparison(df, overlap, output_dir):
    """
    Compare all join types on a single plot for easier comparison.
    """
    subset = df[df['Overlap'] == overlap].copy()
    
    if len(subset) == 0:
        return
    
    fig, ax = plt.subplots(1, 1, figsize=(14, 8))
    
    join_types = sorted(subset['JoinTypeName'].dropna().unique())
    colors = plt.cm.tab10(range(len(join_types)))
    
    for idx, join_type in enumerate(join_types):
        join_data = subset[subset['JoinTypeName'] == join_type]
        
        # If multiple build sides, pick one (prefer RightBuild for consistency)
        build_sides = sorted(join_data['BuildSide'].dropna().unique())
        chosen_build_side = 'RightBuild' if 'RightBuild' in build_sides else build_sides[0]
        
        data = join_data[join_data['BuildSide'] == chosen_build_side].sort_values('LeftPct')
        
        if len(data) > 0:
            label = join_type
            if len(build_sides) > 1:
                label += f" ({chosen_build_side})"
            ax.plot(data['LeftPct'], data['MedianTimeMs'],
                   marker='o', label=label, linewidth=2, markersize=6, color=colors[idx])
    
    ax.set_xlabel('Left Table Percentage (%)', fontsize=12)
    ax.set_ylabel('Median Time (ms)', fontsize=12)
    ax.set_title(f'Join Type Comparison: {overlap}\n(100% Cardinality, HashObject)',
                fontsize=14, fontweight='bold')
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.set_xscale('log')
    ax.axvline(x=50, color='gray', linestyle='--', alpha=0.5)
    
    plt.tight_layout()
    
    filename = f"join_types_comparison_{overlap}.png"
    filepath = os.path.join(output_dir, filename)
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    print(f"  Saved: {filename}")
    plt.close()


def plot_build_side_comparison_for_join_type(df, join_type, overlap, output_dir):
    """
    For join types that support both build sides (Inner, FullOuter),
    compare LeftBuild vs RightBuild.
    """
    subset = df[
        (df['JoinTypeName'] == join_type) &
        (df['Overlap'] == overlap)
    ].copy()
    
    if len(subset) == 0:
        return
    
    build_sides = sorted(subset['BuildSide'].dropna().unique())
    if len(build_sides) < 2:
        return  # Only one build side tested
    
    fig, ax = plt.subplots(1, 1, figsize=(14, 6))
    
    for build_side in build_sides:
        data = subset[subset['BuildSide'] == build_side].sort_values('LeftPct')
        ax.plot(data['LeftPct'], data['MedianTimeMs'],
               marker='o', label=build_side, linewidth=2, markersize=6)
    
    ax.set_xlabel('Left Table Percentage (%)', fontsize=12)
    ax.set_ylabel('Median Time (ms)', fontsize=12)
    ax.set_title(f'{join_type} Join: Build Side Comparison\n{overlap} (100% Cardinality, HashObject)',
                fontsize=14, fontweight='bold')
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.set_xscale('log')
    ax.axvline(x=50, color='gray', linestyle='--', alpha=0.5)
    
    plt.tight_layout()
    
    filename = f"build_side_comparison_{join_type}_{overlap}.png"
    filepath = os.path.join(output_dir, filename)
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    print(f"  Saved: {filename}")
    plt.close()


def print_summary_statistics(df):
    """Print summary statistics about join type performance."""
    print("\n" + "="*80)
    print("SUMMARY STATISTICS: Join Type Performance")
    print("="*80)
    
    overlaps = sorted(df['Overlap'].dropna().unique())
    join_types = sorted(df['JoinTypeName'].dropna().unique())
    
    for overlap in overlaps:
        print(f"\n{overlap.upper()}:")
        print("-" * 40)
        
        for join_type in join_types:
            subset = df[
                (df['JoinTypeName'] == join_type) &
                (df['Overlap'] == overlap)
            ]
            
            if len(subset) == 0:
                continue
            
            median_times = subset['MedianTimeMs']
            print(f"  {join_type}:")
            print(f"    Avg median time: {median_times.mean():.2f} ms")
            print(f"    Min median time: {median_times.min():.2f} ms")
            print(f"    Max median time: {median_times.max():.2f} ms")


def main():
    if len(sys.argv) != 2:
        print("Usage: python analyze_join_type_sweep.py <path_to_tsv_file>")
        sys.exit(1)
    
    tsv_path = sys.argv[1]
    
    if not os.path.exists(tsv_path):
        print(f"Error: File not found: {tsv_path}")
        sys.exit(1)
    
    # Load data
    df = load_data(tsv_path)
    
    # Create output directory
    output_dir = os.path.join(os.path.dirname(tsv_path), "analysis_plots")
    os.makedirs(output_dir, exist_ok=True)
    print(f"\nSaving plots to: {output_dir}\n")
    
    overlaps = sorted(df['Overlap'].dropna().unique())
    
    print("Generating join type performance plots...")
    for overlap in overlaps:
        plot_join_types_by_overlap(df, overlap, output_dir)
        plot_join_type_comparison(df, overlap, output_dir)
    
    print("\nGenerating build side comparison plots...")
    for overlap in overlaps:
        for join_type in ['Inner', 'FullOuter']:
            plot_build_side_comparison_for_join_type(df, join_type, overlap, output_dir)
    
    # Print summary
    print_summary_statistics(df)
    
    print("\n" + "="*80)
    print(f"Analysis complete! All plots saved to: {output_dir}")
    print("="*80)


if __name__ == "__main__":
    main()

