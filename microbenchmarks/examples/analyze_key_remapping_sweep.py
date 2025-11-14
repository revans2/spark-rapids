#!/usr/bin/env python3
"""
Key Remapping Sweep Analysis Script

This script analyzes the output from key_remapping_sweep_benchmark.scala and generates
visualizations to understand the performance impact of key remapping with string keys.

Usage:
    python3 analyze_key_remapping_sweep.py /path/to/key_remapping_sweep.tsv

Output:
    - Multiple PNG charts in <input_dir>/analysis_plots/
    - Summary statistics printed to console

Charts Generated:
    1. Remapping comparison (with vs without) for each strategy/cardinality/overlap
    2. Hash vs Sort comparison for remapping enabled vs disabled
    3. Timing breakdown showing remapping overhead vs join execution
    4. Speedup factors for different cardinality scenarios
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import os
from pathlib import Path

# Set style for all plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10


def load_data(tsv_path):
    """Load and preprocess the benchmark results."""
    print(f"Loading data from: {tsv_path}")
    
    df = pd.read_csv(tsv_path, sep='\t')
    
    # Filter out skipped and error rows
    df = df[df['Status'] == 'SUCCESS'].copy()
    
    print(f"Loaded {len(df)} successful benchmark results")
    
    # Add computed columns
    df['LeftPct'] = (df['LeftRows'] / (df['LeftRows'] + df['RightRows']) * 100).round(1)
    df['TotalRows'] = df['LeftRows'] + df['RightRows']
    
    # Parse optimizations to extract remapping flag
    df['HasRemapping'] = df['Optimizations'].str.contains('remap', na=False)
    df['RemappingLabel'] = df['HasRemapping'].map({True: 'With Remapping', False: 'Without Remapping'})
    
    # Extract cardinality name
    df['CardinalityLabel'] = df['TestName'].str.extract(r'_(100pct_distinct|50pct_cardinality|100pct_cardinality)_')[0]
    df['CardinalityLabel'] = df['CardinalityLabel'].map({
        '100pct_distinct': '100% Distinct',
        '50pct_cardinality': '50% Cardinality',
        '100pct_cardinality': '1 Key (100% card.)'
    })
    
    # Extract overlap
    df['OverlapLabel'] = df['TestName'].str.extract(r'_(no_overlap|full_overlap)_')[0]
    df['OverlapLabel'] = df['OverlapLabel'].map({
        'no_overlap': 'No Overlap',
        'full_overlap': 'Full Overlap'
    })
    
    # Strategy label
    df['StrategyLabel'] = df['Strategy'].map({
        'HashObjectPost': 'Hash',
        'SortObjectPost': 'Sort'
    })
    
    return df


def create_output_dir(tsv_path):
    """Create output directory for plots."""
    input_dir = os.path.dirname(tsv_path)
    output_dir = os.path.join(input_dir, 'analysis_plots')
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory: {output_dir}")
    return output_dir


def plot_remapping_comparison(df, output_dir):
    """
    Plot comparison of with vs without remapping for each strategy/cardinality/overlap.
    """
    print("\nGenerating remapping comparison plots...")
    
    for strategy in df['StrategyLabel'].unique():
        for cardinality in df['CardinalityLabel'].unique():
            for overlap in df['OverlapLabel'].unique():
                subset = df[
                    (df['StrategyLabel'] == strategy) & 
                    (df['CardinalityLabel'] == cardinality) &
                    (df['OverlapLabel'] == overlap)
                ]
                
                if len(subset) == 0:
                    continue
                
                fig, ax = plt.subplots(figsize=(14, 8))
                
                for has_remap, group in subset.groupby('HasRemapping'):
                    label = 'With Remapping' if has_remap else 'Without Remapping'
                    marker = 'o' if has_remap else '^'
                    
                    # Sort by LeftPct
                    group_sorted = group.sort_values('LeftPct')
                    
                    ax.plot(group_sorted['LeftPct'], group_sorted['AvgTimeMs'], 
                           marker=marker, label=label, markersize=6, linewidth=2)
                
                ax.set_xlabel('Left Table Percentage (%)', fontsize=12, fontweight='bold')
                ax.set_ylabel('Average Time (ms)', fontsize=12, fontweight='bold')
                ax.set_title(f'Key Remapping Performance: {strategy} Join\n'
                           f'{cardinality}, {overlap}',
                           fontsize=14, fontweight='bold')
                ax.legend(fontsize=11)
                ax.grid(True, alpha=0.3)
                
                # Add vertical line at 50%
                ax.axvline(x=50, color='red', linestyle='--', alpha=0.3, label='Balanced (50/50)')
                
                filename = f"remapping_comparison_{strategy}_{cardinality.replace(' ', '_')}_{overlap.replace(' ', '_')}.png"
                filepath = os.path.join(output_dir, filename)
                plt.tight_layout()
                plt.savefig(filepath, dpi=150)
                plt.close()
                print(f"  Saved: {filename}")


def plot_speedup_analysis(df, output_dir):
    """
    Calculate and plot speedup from remapping (time_without / time_with).
    """
    print("\nGenerating speedup analysis plots...")
    
    # Pivot to get with vs without remapping side by side
    for strategy in df['StrategyLabel'].unique():
        for overlap in df['OverlapLabel'].unique():
            subset = df[
                (df['StrategyLabel'] == strategy) & 
                (df['OverlapLabel'] == overlap)
            ]
            
            if len(subset) == 0:
                continue
            
            # Create pivot table
            pivot = subset.pivot_table(
                index=['LeftPct', 'CardinalityLabel'],
                columns='HasRemapping',
                values='AvgTimeMs'
            )
            
            # Calculate speedup
            if False in pivot.columns and True in pivot.columns:
                pivot['Speedup'] = pivot[False] / pivot[True]
            else:
                continue
            
            # Reset index for plotting
            pivot_reset = pivot.reset_index()
            
            fig, ax = plt.subplots(figsize=(14, 8))
            
            for cardinality in pivot_reset['CardinalityLabel'].unique():
                card_data = pivot_reset[pivot_reset['CardinalityLabel'] == cardinality]
                card_data_sorted = card_data.sort_values('LeftPct')
                
                ax.plot(card_data_sorted['LeftPct'], card_data_sorted['Speedup'], 
                       marker='o', label=cardinality, markersize=6, linewidth=2)
            
            ax.axhline(y=1.0, color='black', linestyle='--', alpha=0.5, label='No Change (1.0x)')
            ax.set_xlabel('Left Table Percentage (%)', fontsize=12, fontweight='bold')
            ax.set_ylabel('Speedup Factor (Without / With Remapping)', fontsize=12, fontweight='bold')
            ax.set_title(f'Key Remapping Speedup: {strategy} Join, {overlap}',
                       fontsize=14, fontweight='bold')
            ax.legend(fontsize=11)
            ax.grid(True, alpha=0.3)
            
            filename = f"speedup_{strategy}_{overlap.replace(' ', '_')}.png"
            filepath = os.path.join(output_dir, filename)
            plt.tight_layout()
            plt.savefig(filepath, dpi=150)
            plt.close()
            print(f"  Saved: {filename}")


def plot_timing_breakdown(df, output_dir):
    """
    Plot timing breakdown showing remapping overhead vs join execution time.
    """
    print("\nGenerating timing breakdown plots...")
    
    # Only plot for cases with remapping enabled
    df_remap = df[df['HasRemapping'] == True].copy()
    
    if len(df_remap) == 0:
        print("  No remapping data found, skipping timing breakdown")
        return
    
    # Calculate total remapping time
    df_remap['TotalRemapMs'] = (
        df_remap['RemapStructureBuildMs'].fillna(0) +
        df_remap['RemapBuildKeysMs'].fillna(0) +
        df_remap['RemapProbeKeysMs'].fillna(0)
    )
    
    for strategy in df_remap['StrategyLabel'].unique():
        for cardinality in df_remap['CardinalityLabel'].unique():
            for overlap in df_remap['OverlapLabel'].unique():
                subset = df_remap[
                    (df_remap['StrategyLabel'] == strategy) & 
                    (df_remap['CardinalityLabel'] == cardinality) &
                    (df_remap['OverlapLabel'] == overlap)
                ]
                
                if len(subset) == 0:
                    continue
                
                subset_sorted = subset.sort_values('LeftPct')
                
                fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12))
                
                # Plot 1: Stacked bar showing breakdown
                width = 0.8
                x_pos = range(len(subset_sorted))
                
                ax1.bar(x_pos, subset_sorted['TotalRemapMs'], width, 
                       label='Remapping Overhead', color='lightcoral')
                ax1.bar(x_pos, subset_sorted['CreateBuildObjectMs'], width,
                       bottom=subset_sorted['TotalRemapMs'],
                       label='Build Object Creation', color='skyblue')
                ax1.bar(x_pos, subset_sorted['ExecuteJoinMs'], width,
                       bottom=subset_sorted['TotalRemapMs'] + subset_sorted['CreateBuildObjectMs'],
                       label='Join Execution', color='lightgreen')
                
                ax1.set_xticks(x_pos)
                ax1.set_xticklabels([f"{int(pct)}%" for pct in subset_sorted['LeftPct']], rotation=45)
                ax1.set_xlabel('Left Table Percentage', fontsize=12, fontweight='bold')
                ax1.set_ylabel('Time (ms)', fontsize=12, fontweight='bold')
                ax1.set_title(f'Timing Breakdown with Remapping: {strategy} Join\n'
                            f'{cardinality}, {overlap}',
                            fontsize=14, fontweight='bold')
                ax1.legend(fontsize=11)
                ax1.grid(True, alpha=0.3, axis='y')
                
                # Plot 2: Remapping overhead as percentage
                total_time = subset_sorted['AvgTimeMs']
                remap_pct = (subset_sorted['TotalRemapMs'] / total_time * 100)
                
                ax2.plot(subset_sorted['LeftPct'], remap_pct, 
                        marker='o', color='red', linewidth=2, markersize=6)
                ax2.set_xlabel('Left Table Percentage (%)', fontsize=12, fontweight='bold')
                ax2.set_ylabel('Remapping Overhead (%)', fontsize=12, fontweight='bold')
                ax2.set_title('Remapping Overhead as Percentage of Total Time',
                            fontsize=12, fontweight='bold')
                ax2.grid(True, alpha=0.3)
                ax2.axhline(y=10, color='orange', linestyle='--', alpha=0.5, label='10% threshold')
                ax2.legend(fontsize=10)
                
                filename = f"timing_breakdown_{strategy}_{cardinality.replace(' ', '_')}_{overlap.replace(' ', '_')}.png"
                filepath = os.path.join(output_dir, filename)
                plt.tight_layout()
                plt.savefig(filepath, dpi=150)
                plt.close()
                print(f"  Saved: {filename}")


def plot_hash_vs_sort(df, output_dir):
    """
    Compare hash vs sort strategies for remapping enabled and disabled.
    """
    print("\nGenerating hash vs sort comparison plots...")
    
    for has_remap in [False, True]:
        remap_label = 'With Remapping' if has_remap else 'Without Remapping'
        
        for cardinality in df['CardinalityLabel'].unique():
            for overlap in df['OverlapLabel'].unique():
                subset = df[
                    (df['HasRemapping'] == has_remap) &
                    (df['CardinalityLabel'] == cardinality) &
                    (df['OverlapLabel'] == overlap)
                ]
                
                if len(subset) == 0:
                    continue
                
                fig, ax = plt.subplots(figsize=(14, 8))
                
                for strategy in subset['StrategyLabel'].unique():
                    strategy_data = subset[subset['StrategyLabel'] == strategy]
                    strategy_sorted = strategy_data.sort_values('LeftPct')
                    
                    marker = 'o' if strategy == 'Hash' else 's'
                    ax.plot(strategy_sorted['LeftPct'], strategy_sorted['AvgTimeMs'],
                           marker=marker, label=strategy, markersize=6, linewidth=2)
                
                ax.set_xlabel('Left Table Percentage (%)', fontsize=12, fontweight='bold')
                ax.set_ylabel('Average Time (ms)', fontsize=12, fontweight='bold')
                ax.set_title(f'Hash vs Sort Strategies: {remap_label}\n'
                           f'{cardinality}, {overlap}',
                           fontsize=14, fontweight='bold')
                ax.legend(fontsize=11)
                ax.grid(True, alpha=0.3)
                ax.axvline(x=50, color='red', linestyle='--', alpha=0.3)
                
                filename = f"hash_vs_sort_{'remap' if has_remap else 'noremap'}_{cardinality.replace(' ', '_')}_{overlap.replace(' ', '_')}.png"
                filepath = os.path.join(output_dir, filename)
                plt.tight_layout()
                plt.savefig(filepath, dpi=150)
                plt.close()
                print(f"  Saved: {filename}")


def print_summary_statistics(df):
    """Print summary statistics about the benchmark results."""
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)
    
    print(f"\nTotal successful tests: {len(df)}")
    print(f"Strategies tested: {', '.join(df['StrategyLabel'].unique())}")
    print(f"Cardinality scenarios: {', '.join(df['CardinalityLabel'].unique())}")
    print(f"Overlap scenarios: {', '.join(df['OverlapLabel'].unique())}")
    
    print("\n" + "-"*80)
    print("REMAPPING SPEEDUP ANALYSIS")
    print("-"*80)
    
    # Calculate average speedup for each combination
    for strategy in df['StrategyLabel'].unique():
        for cardinality in df['CardinalityLabel'].unique():
            for overlap in df['OverlapLabel'].unique():
                subset = df[
                    (df['StrategyLabel'] == strategy) &
                    (df['CardinalityLabel'] == cardinality) &
                    (df['OverlapLabel'] == overlap)
                ]
                
                if len(subset) == 0:
                    continue
                
                # Pivot to calculate speedup
                pivot = subset.pivot_table(
                    index='LeftPct',
                    columns='HasRemapping',
                    values='AvgTimeMs'
                )
                
                if False in pivot.columns and True in pivot.columns:
                    speedups = pivot[False] / pivot[True]
                    avg_speedup = speedups.mean()
                    max_speedup = speedups.max()
                    min_speedup = speedups.min()
                    
                    print(f"\n{strategy} | {cardinality} | {overlap}")
                    print(f"  Average speedup: {avg_speedup:.2f}x")
                    print(f"  Max speedup:     {max_speedup:.2f}x")
                    print(f"  Min speedup:     {min_speedup:.2f}x")
                    
                    # Find configuration with max speedup
                    max_idx = speedups.idxmax()
                    print(f"  Max at left%: {max_idx:.1f}%")
    
    print("\n" + "-"*80)
    print("REMAPPING OVERHEAD ANALYSIS")
    print("-"*80)
    
    df_remap = df[df['HasRemapping'] == True].copy()
    if len(df_remap) > 0:
        df_remap['TotalRemapMs'] = (
            df_remap['RemapStructureBuildMs'].fillna(0) +
            df_remap['RemapBuildKeysMs'].fillna(0) +
            df_remap['RemapProbeKeysMs'].fillna(0)
        )
        df_remap['RemapOverheadPct'] = (df_remap['TotalRemapMs'] / df_remap['AvgTimeMs'] * 100)
        
        print(f"\nAverage remapping overhead: {df_remap['RemapOverheadPct'].mean():.1f}%")
        print(f"Max remapping overhead: {df_remap['RemapOverheadPct'].max():.1f}%")
        print(f"Min remapping overhead: {df_remap['RemapOverheadPct'].min():.1f}%")
        
        for strategy in df_remap['StrategyLabel'].unique():
            strategy_data = df_remap[df_remap['StrategyLabel'] == strategy]
            print(f"\n{strategy} average overhead: {strategy_data['RemapOverheadPct'].mean():.1f}%")


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 analyze_key_remapping_sweep.py <tsv_file>")
        print("\nExample:")
        print("  python3 analyze_key_remapping_sweep.py /data/tmp/key_remapping_sweep/key_remapping_sweep.tsv")
        sys.exit(1)
    
    tsv_path = sys.argv[1]
    
    if not os.path.exists(tsv_path):
        print(f"Error: File not found: {tsv_path}")
        sys.exit(1)
    
    # Load data
    df = load_data(tsv_path)
    
    # Create output directory
    output_dir = create_output_dir(tsv_path)
    
    # Generate plots
    plot_remapping_comparison(df, output_dir)
    plot_speedup_analysis(df, output_dir)
    plot_timing_breakdown(df, output_dir)
    plot_hash_vs_sort(df, output_dir)
    
    # Print summary statistics
    print_summary_statistics(df)
    
    print("\n" + "="*80)
    print(f"Analysis complete! Plots saved to: {output_dir}")
    print("="*80)


if __name__ == '__main__':
    main()

