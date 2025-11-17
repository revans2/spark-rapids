#!/usr/bin/env python3
"""
Analyze AST Post-Processing Sweep Benchmark Results

This script visualizes results from ast_post_processing_sweep.scala to compare:
1. HashDirectStrategy vs HashObjectWithPostStrategy with AST filtering
2. How each strategy scales with AST complexity (NoAst → Simple → Medium → Complex)
3. Build-side swapping effectiveness when enabled
4. Join type behavior across strategies and AST levels

Key Visualizations:
- Strategy comparison plots (solid = HashDirect, dashed = HashObjectPost)
- AST complexity overhead for each strategy
- Build-side flexibility analysis
- Performance heatmaps
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import sys
from pathlib import Path

# Set plotting style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 11

def load_data(tsv_path):
    """Load benchmark results from TSV file."""
    print(f"Loading data from: {tsv_path}")
    
    if not Path(tsv_path).exists():
        print(f"ERROR: File not found: {tsv_path}")
        sys.exit(1)
    
    df = pd.read_csv(tsv_path, sep='\t')
    
    # Filter successful tests only
    df = df[df['Status'] == 'SUCCESS'].copy()
    
    # Extract strategy from TestName if not in columns
    if 'Strategy' not in df.columns:
        def extract_strategy(name):
            if 'HashObjectPost' in name:
                return 'HashObjectPost'
            elif 'HashDirect' in name:
                return 'HashDirect'
            else:
                return 'Unknown'
        df['Strategy'] = df['TestName'].apply(extract_strategy)
    
    print(f"Loaded {len(df)} successful test results")
    print(f"Strategies: {df['Strategy'].unique()}")
    print(f"Join types: {df['JoinType'].unique()}")
    print(f"AST complexities: {df['AstComplexity'].unique()}")
    print(f"Build sides: {df['ActualBuildSide'].unique()}")
    
    return df

def plot_progressive_overhead_by_join_type(df, output_dir):
    """
    Show progressive overhead comparing HashDirect vs HashObjectPost strategies.
    For each strategy, shows NoAst → Simple → Medium → Complex.
    """
    print("\nGenerating progressive overhead plot...")
    
    fig, axes = plt.subplots(2, 3, figsize=(20, 12))
    axes = axes.flatten()
    
    join_types = ['Inner', 'LeftOuter', 'RightOuter', 'FullOuter', 'LeftSemi', 'LeftAnti']
    ast_order = ['NoAst', 'Simple', 'Medium', 'Complex']
    strategies = ['HashDirect', 'HashObjectPost']
    
    for idx, join_type in enumerate(join_types):
        ax = axes[idx]
        
        # Filter for this join type
        jt_data = df[df['JoinType'] == join_type].copy()
        
        if len(jt_data) == 0:
            ax.text(0.5, 0.5, f'No data for {join_type}', 
                   ha='center', va='center', transform=ax.transAxes)
            ax.set_title(f'{join_type} Join', fontsize=13, fontweight='bold')
            continue
        
        # For each strategy and AST complexity combination
        for strategy in strategies:
            for ast_complexity in ast_order:
                strat_ast_data = jt_data[
                    (jt_data['Strategy'] == strategy) & 
                    (jt_data['AstComplexity'] == ast_complexity)
                ].copy()
                
                if len(strat_ast_data) == 0:
                    continue
                
                # Average across build sides for cleaner visualization
                grouped = strat_ast_data.groupby('LeftPct')['MedianTimeMs'].mean().reset_index()
                grouped = grouped.sort_values('LeftPct')
                
                # Choose line style and color based on strategy and complexity
                if strategy == 'HashDirect':
                    # Solid lines for HashDirect
                    if ast_complexity == 'NoAst':
                        color = 'steelblue'
                        linestyle = '-'
                        linewidth = 3
                        marker = 'o'
                        label = 'HashDirect: NoAst'
                    elif ast_complexity == 'Simple':
                        color = 'coral'
                        linestyle = '-'
                        linewidth = 2.5
                        marker = '^'
                        label = 'HashDirect: Simple'
                    elif ast_complexity == 'Medium':
                        color = 'orange'
                        linestyle = '-'
                        linewidth = 2.5
                        marker = 's'
                        label = 'HashDirect: Medium'
                    else:  # Complex
                        color = 'red'
                        linestyle = '-'
                        linewidth = 2.5
                        marker = 'D'
                        label = 'HashDirect: Complex'
                else:  # HashObjectPost
                    # Dashed lines for HashObjectPost
                    if ast_complexity == 'NoAst':
                        color = 'steelblue'
                        linestyle = '--'
                        linewidth = 3
                        marker = 'o'
                        label = 'HashObjectPost: NoAst'
                    elif ast_complexity == 'Simple':
                        color = 'coral'
                        linestyle = '--'
                        linewidth = 2.5
                        marker = '^'
                        label = 'HashObjectPost: Simple'
                    elif ast_complexity == 'Medium':
                        color = 'orange'
                        linestyle = '--'
                        linewidth = 2.5
                        marker = 's'
                        label = 'HashObjectPost: Medium'
                    else:  # Complex
                        color = 'red'
                        linestyle = '--'
                        linewidth = 2.5
                        marker = 'D'
                        label = 'HashObjectPost: Complex'
                
                ax.plot(grouped['LeftPct'], grouped['MedianTimeMs'],
                       marker=marker, label=label, color=color,
                       linestyle=linestyle, linewidth=linewidth, markersize=6, alpha=0.8)
        
        ax.set_xlabel('Left Table Percentage (%)', fontsize=11, fontweight='bold')
        ax.set_ylabel('Median Time (ms)', fontsize=11, fontweight='bold')
        ax.set_title(f'{join_type} Join', fontsize=13, fontweight='bold')
        ax.legend(fontsize=7, loc='best', ncol=2)  # Smaller font, 2 columns for 8 lines
        ax.grid(True, alpha=0.3)
        ax.set_xscale('log')
        ax.axvline(x=50, color='gray', linestyle='--', alpha=0.5, linewidth=1)
    
    plt.suptitle('Strategy Comparison: HashDirect vs HashObjectPost with AST\n' +
                 'Solid lines = HashDirect, Dashed lines = HashObjectPost',
                 fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout()
    
    filename = os.path.join(output_dir, 'strategy_comparison_by_join_type.png')
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    print(f"  Saved: strategy_comparison_by_join_type.png")
    plt.close()

def plot_output_rows_by_join_type(df, output_dir):
    """
    Show how output rows vary by join type, AST complexity, and table distribution.
    Similar layout to strategy comparison plot.
    """
    print("\nGenerating output rows plot...")
    
    fig, axes = plt.subplots(2, 3, figsize=(20, 12))
    axes = axes.flatten()
    
    join_types = ['Inner', 'LeftOuter', 'RightOuter', 'FullOuter', 'LeftSemi', 'LeftAnti']
    ast_order = ['NoAst', 'Simple', 'Medium', 'Complex']
    
    # Color scheme for AST complexity
    colors = {
        'NoAst': 'steelblue',
        'Simple': 'coral',
        'Medium': 'orange',
        'Complex': 'red'
    }
    
    markers = {
        'NoAst': 'o',
        'Simple': '^',
        'Medium': 's',
        'Complex': 'D'
    }
    
    for idx, join_type in enumerate(join_types):
        ax = axes[idx]
        
        # Filter for this join type
        jt_data = df[df['JoinType'] == join_type].copy()
        
        if len(jt_data) == 0:
            ax.text(0.5, 0.5, f'No data for {join_type}', 
                   ha='center', va='center', transform=ax.transAxes)
            ax.set_title(f'{join_type}')
            continue
        
        # Plot each AST complexity
        for ast_complexity in ast_order:
            ast_data = jt_data[jt_data['AstComplexity'] == ast_complexity]
            
            if len(ast_data) > 0:
                # Average across strategies and build sides
                grouped = ast_data.groupby('LeftPct')['OutputRows'].mean().reset_index()
                grouped = grouped.sort_values('LeftPct')
                
                color = colors[ast_complexity]
                marker = markers[ast_complexity]
                linewidth = 3 if ast_complexity == 'NoAst' else 2.5
                
                ax.plot(grouped['LeftPct'], grouped['OutputRows'],
                       marker=marker, label=ast_complexity, color=color,
                       linestyle='-', linewidth=linewidth, markersize=6, alpha=0.8)
        
        ax.set_title(f'{join_type}', fontsize=12, fontweight='bold')
        ax.set_xlabel('Left Table %', fontsize=10)
        ax.set_ylabel('Output Rows', fontsize=10)
        ax.legend(fontsize=9, loc='best')
        ax.grid(True, alpha=0.3)
        ax.set_xscale('log')
        ax.axvline(x=50, color='gray', linestyle='--', alpha=0.5, linewidth=1)
        
        # Format y-axis to show numbers nicely
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    
    plt.suptitle('Output Rows by Join Type and AST Complexity\n' +
                 'Shows how AST filtering reduces output size across different table distributions',
                 fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout()
    
    filename = os.path.join(output_dir, 'output_rows_by_join_type.png')
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    print(f"  Saved: output_rows_by_join_type.png")
    plt.close()

def plot_left_outer_build_side_flexibility(df, output_dir):
    """
    Focused plot for Left Outer comparing HashDirect vs HashObjectPost strategies.
    Shows how each strategy performs across AST complexities and table distributions.
    """
    print("\nGenerating Left Outer strategy comparison plot...")
    
    # Filter for LeftOuter only
    lo_data = df[df['JoinType'] == 'LeftOuter'].copy()
    
    if len(lo_data) == 0:
        print("  No Left Outer data found, skipping")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    axes = axes.flatten()
    
    ast_complexities = ['NoAst', 'Simple', 'Medium', 'Complex']
    
    for idx, ast_complexity in enumerate(ast_complexities):
        ax = axes[idx]
        
        ast_data = lo_data[lo_data['AstComplexity'] == ast_complexity].copy()
        
        if len(ast_data) == 0:
            ax.text(0.5, 0.5, f'No data for {ast_complexity}',
                   ha='center', va='center', transform=ax.transAxes)
            ax.set_title(f'{ast_complexity} AST', fontsize=13, fontweight='bold')
            continue
        
        # Plot HashDirect (solid line)
        hash_direct = ast_data[ast_data['Strategy'] == 'HashDirect'].copy()
        if len(hash_direct) > 0:
            # Average across build sides
            hd_grouped = hash_direct.groupby('LeftPct')['MedianTimeMs'].mean().reset_index()
            hd_grouped = hd_grouped.sort_values('LeftPct')
            ax.plot(hd_grouped['LeftPct'], hd_grouped['MedianTimeMs'],
                   marker='o', label='HashDirect',
                   linewidth=3, markersize=7, color='blue', linestyle='-', alpha=0.8)
        
        # Plot HashObjectPost (dashed line)
        hash_object_post = ast_data[ast_data['Strategy'] == 'HashObjectPost'].copy()
        if len(hash_object_post) > 0:
            # Average across build sides
            hop_grouped = hash_object_post.groupby('LeftPct')['MedianTimeMs'].mean().reset_index()
            hop_grouped = hop_grouped.sort_values('LeftPct')
            ax.plot(hop_grouped['LeftPct'], hop_grouped['MedianTimeMs'],
                   marker='s', label='HashObjectPost',
                   linewidth=3, markersize=7, color='green', linestyle='--', alpha=0.8)
        
        ax.set_xlabel('Left Table Percentage (%)', fontsize=11, fontweight='bold')
        ax.set_ylabel('Median Time (ms)', fontsize=11, fontweight='bold')
        ax.set_title(f'{ast_complexity} AST', fontsize=13, fontweight='bold')
        ax.legend(fontsize=10, loc='best')
        ax.grid(True, alpha=0.3)
        ax.set_xscale('log')
        ax.axvline(x=50, color='gray', linestyle='--', alpha=0.5, linewidth=2)
    
    plt.suptitle('Left Outer Join: HashDirect vs HashObjectPost Strategy Comparison\n' +
                 'Comparing direct AST evaluation vs post-processing approach',
                 fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout()
    
    filename = os.path.join(output_dir, 'left_outer_build_flexibility.png')
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    print(f"  Saved: left_outer_build_flexibility.png")
    plt.close()

def plot_overhead_heatmap(df, output_dir):
    """
    Heatmap showing overhead percentage for each (JoinType, AstComplexity) combination.
    """
    print("\nGenerating overhead heatmap...")
    
    # Calculate average overhead for each combination
    # Use NoAst as baseline
    baseline = df[df['AstComplexity'] == 'NoAst'].copy()
    baseline_avg = baseline.groupby(['JoinType', 'LeftPct'])['MedianTimeMs'].mean().reset_index()
    baseline_avg.rename(columns={'MedianTimeMs': 'BaselineMs'}, inplace=True)
    
    # Calculate overhead for each AST complexity
    overhead_data = []
    
    for join_type in df['JoinType'].unique():
        for ast_complexity in ['Simple', 'Medium', 'Complex']:
            ast_data = df[(df['JoinType'] == join_type) & (df['AstComplexity'] == ast_complexity)].copy()
            
            if len(ast_data) == 0:
                continue
            
            # Merge with baseline
            ast_avg = ast_data.groupby('LeftPct')['MedianTimeMs'].mean().reset_index()
            merged = pd.merge(ast_avg, 
                            baseline_avg[baseline_avg['JoinType'] == join_type],
                            on='LeftPct')
            
            # Calculate overhead
            merged['overhead_pct'] = (merged['MedianTimeMs'] - merged['BaselineMs']) / merged['BaselineMs'] * 100
            avg_overhead = merged['overhead_pct'].mean()
            
            overhead_data.append({
                'JoinType': join_type,
                'AstComplexity': ast_complexity,
                'AvgOverhead': avg_overhead
            })
    
    if len(overhead_data) == 0:
        print("  No overhead data to plot")
        return
    
    overhead_df = pd.DataFrame(overhead_data)
    
    # Create pivot table for heatmap
    pivot = overhead_df.pivot(index='JoinType', columns='AstComplexity', values='AvgOverhead')
    
    # Reorder columns
    pivot = pivot[['Simple', 'Medium', 'Complex']]
    
    fig, ax = plt.subplots(1, 1, figsize=(10, 6))
    
    sns.heatmap(pivot, annot=True, fmt='.1f', cmap='YlOrRd', 
               cbar_kws={'label': 'Average Overhead (%)'}, ax=ax,
               linewidths=1, linecolor='white', vmin=0)
    
    ax.set_xlabel('AST Complexity', fontsize=13, fontweight='bold')
    ax.set_ylabel('Join Type', fontsize=13, fontweight='bold')
    ax.set_title('Average AST Post-Processing Overhead by Join Type\n' +
                '(Percentage increase vs NoAst baseline)',
                fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    
    filename = os.path.join(output_dir, 'ast_overhead_heatmap.png')
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    print(f"  Saved: ast_overhead_heatmap.png")
    plt.close()

def plot_ast_complexity_comparison(df, output_dir):
    """
    Bar chart comparing average overhead across AST complexities for all join types.
    """
    print("\nGenerating AST complexity comparison...")
    
    fig, ax = plt.subplots(1, 1, figsize=(14, 8))
    
    # Calculate average times for each join type and AST complexity
    summary = df.groupby(['JoinType', 'AstComplexity'])['MedianTimeMs'].mean().reset_index()
    
    join_types = summary['JoinType'].unique()
    ast_complexities = ['NoAst', 'Simple', 'Medium', 'Complex']
    
    x = np.arange(len(join_types))
    width = 0.2
    
    for idx, ast_complexity in enumerate(ast_complexities):
        ast_data = summary[summary['AstComplexity'] == ast_complexity]
        values = [ast_data[ast_data['JoinType'] == jt]['MedianTimeMs'].values[0] 
                 if len(ast_data[ast_data['JoinType'] == jt]) > 0 else 0
                 for jt in join_types]
        
        offset = (idx - 1.5) * width
        
        if ast_complexity == 'NoAst':
            color = 'steelblue'
            label = 'No AST (baseline)'
        elif ast_complexity == 'Simple':
            color = 'coral'
            label = 'Simple AST'
        elif ast_complexity == 'Medium':
            color = 'orange'
            label = 'Medium AST'
        else:
            color = 'red'
            label = 'Complex AST'
        
        bars = ax.bar(x + offset, values, width, label=label, color=color, alpha=0.8)
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{height:.1f}',
                       ha='center', va='bottom', fontsize=8)
    
    ax.set_xlabel('Join Type', fontsize=13, fontweight='bold')
    ax.set_ylabel('Average Median Time (ms)', fontsize=13, fontweight='bold')
    ax.set_title('AST Complexity Comparison Across Join Types\n' +
                '(Averaged across all table size distributions)',
                fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(join_types, fontsize=11)
    ax.legend(fontsize=11, loc='upper left')
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    
    filename = os.path.join(output_dir, 'ast_complexity_comparison.png')
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    print(f"  Saved: ast_complexity_comparison.png")
    plt.close()

def generate_summary_report(df, output_dir):
    """Generate text summary report."""
    print("\nGenerating summary report...")
    
    report_path = os.path.join(output_dir, 'ast_sweep_summary.txt')
    
    with open(report_path, 'w') as f:
        f.write("="*80 + "\n")
        f.write("AST POST-PROCESSING SWEEP BENCHMARK SUMMARY\n")
        f.write("="*80 + "\n\n")
        
        # Overall statistics
        f.write("OVERALL STATISTICS\n")
        f.write("-"*80 + "\n")
        f.write(f"Total successful tests: {len(df)}\n")
        f.write(f"Strategies tested: {', '.join(df['Strategy'].unique())}\n")
        f.write(f"Join types tested: {', '.join(df['JoinType'].unique())}\n")
        f.write(f"AST complexities: {', '.join(df['AstComplexity'].unique())}\n")
        f.write(f"Table size range: {df['LeftRows'].min():,} - {df['LeftRows'].max():,} rows\n\n")
        
        # Strategy comparison
        f.write("STRATEGY COMPARISON (HashDirect vs HashObjectPost)\n")
        f.write("-"*80 + "\n")
        for strategy in ['HashDirect', 'HashObjectPost']:
            strat_data = df[df['Strategy'] == strategy]
            if len(strat_data) > 0:
                avg_time = strat_data['MedianTimeMs'].mean()
                f.write(f"{strategy:20s}: {avg_time:8.2f} ms (average)\n")
        f.write("\n")
        
        # Average overhead by AST complexity (for each strategy)
        f.write("AVERAGE OVERHEAD BY AST COMPLEXITY (per strategy)\n")
        f.write("-"*80 + "\n")
        
        for strategy in ['HashDirect', 'HashObjectPost']:
            strat_data = df[df['Strategy'] == strategy]
            if len(strat_data) == 0:
                continue
                
            f.write(f"\n{strategy}:\n")
            baseline = strat_data[strat_data['AstComplexity'] == 'NoAst']['MedianTimeMs'].mean()
            f.write(f"  NoAst (baseline): {baseline:8.2f} ms\n")
            
            for ast_complexity in ['Simple', 'Medium', 'Complex']:
                ast_avg = strat_data[strat_data['AstComplexity'] == ast_complexity]['MedianTimeMs'].mean()
                if pd.notna(ast_avg) and pd.notna(baseline):
                    overhead_pct = (ast_avg - baseline) / baseline * 100
                    f.write(f"  {ast_complexity:10s}: {ast_avg:8.2f} ms | {overhead_pct:+6.1f}% overhead\n")
        
        f.write("\n")
        
        # Best and worst performers
        f.write("BEST AND WORST PERFORMERS\n")
        f.write("-"*80 + "\n")
        
        for ast_complexity in ['Simple', 'Medium', 'Complex']:
            ast_data = df[df['AstComplexity'] == ast_complexity].copy()
            
            if len(ast_data) == 0:
                continue
            
            best = ast_data.loc[ast_data['MedianTimeMs'].idxmin()]
            worst = ast_data.loc[ast_data['MedianTimeMs'].idxmax()]
            
            f.write(f"\n{ast_complexity} AST:\n")
            f.write(f"  Best:  {best['JoinType']:10s} L={best['LeftPct']:3d}% Build={best['ActualBuildSide']:5s} " +
                   f"Time={best['MedianTimeMs']:7.2f} ms\n")
            f.write(f"  Worst: {worst['JoinType']:10s} L={worst['LeftPct']:3d}% Build={worst['ActualBuildSide']:5s} " +
                   f"Time={worst['MedianTimeMs']:7.2f} ms\n")
        
        f.write("\n")
        
        # Build-side flexibility analysis for Left Outer
        f.write("LEFT OUTER JOIN: BUILD-SIDE FLEXIBILITY ANALYSIS\n")
        f.write("-"*80 + "\n")
        
        lo_data = df[df['JoinType'] == 'LeftOuter'].copy()
        
        if len(lo_data) > 0:
            for ast_complexity in df['AstComplexity'].unique():
                ast_lo = lo_data[lo_data['AstComplexity'] == ast_complexity].copy()
                
                if len(ast_lo) == 0:
                    continue
                
                # Compare LeftBuild vs RightBuild
                left_build_avg = ast_lo[ast_lo['ActualBuildSide'] == 'Left']['MedianTimeMs'].mean()
                right_build_avg = ast_lo[ast_lo['ActualBuildSide'] == 'Right']['MedianTimeMs'].mean()
                
                f.write(f"\n{ast_complexity}:\n")
                if pd.notna(left_build_avg) and pd.notna(right_build_avg):
                    diff_pct = (left_build_avg - right_build_avg) / right_build_avg * 100
                    f.write(f"  LeftBuild avg:  {left_build_avg:7.2f} ms\n")
                    f.write(f"  RightBuild avg: {right_build_avg:7.2f} ms\n")
                    if abs(diff_pct) > 5:
                        better = "LeftBuild" if left_build_avg < right_build_avg else "RightBuild"
                        f.write(f"  → {better} is {abs(diff_pct):.1f}% faster on average\n")
                    else:
                        f.write(f"  → Build side choice has minimal impact (<5%)\n")
                else:
                    f.write(f"  Insufficient data for comparison\n")
    
    print(f"  Saved: ast_sweep_summary.txt")

def main():
    """Main entry point."""
    # Parse command line arguments
    if len(sys.argv) > 1:
        tsv_path = sys.argv[1]
    else:
        tsv_path = '/data/tmp/ast_post_sweep/ast_post_sweep.tsv'
    
    if len(sys.argv) > 2:
        output_dir = sys.argv[2]
    else:
        # Default: create visualizations/ directory next to TSV
        tsv_dir = os.path.dirname(os.path.abspath(tsv_path))
        output_dir = os.path.join(tsv_dir, 'visualizations')
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir}\n")
    
    # Load data
    df = load_data(tsv_path)
    
    # Generate visualizations
    print("\n" + "="*80)
    print("GENERATING VISUALIZATIONS")
    print("="*80)
    
    plot_progressive_overhead_by_join_type(df, output_dir)
    plot_output_rows_by_join_type(df, output_dir)
    plot_left_outer_build_side_flexibility(df, output_dir)
    plot_overhead_heatmap(df, output_dir)
    plot_ast_complexity_comparison(df, output_dir)
    
    # Generate summary report
    generate_summary_report(df, output_dir)
    
    print("\n" + "="*80)
    print("VISUALIZATION COMPLETE!")
    print("="*80)
    print(f"\nAll outputs saved to: {output_dir}")
    print("\nGenerated files:")
    print("  • strategy_comparison_by_join_type.png  (PRIMARY - HashDirect vs HashObjectPost with AST)")
    print("  • output_rows_by_join_type.png          (Output row counts by join type & AST complexity)")
    print("  • left_outer_build_flexibility.png      (Left Outer: HashDirect vs HashObjectPost)")
    print("  • ast_overhead_heatmap.png              (Overhead matrix)")
    print("  • ast_complexity_comparison.png         (Bar chart comparison)")
    print("  • ast_sweep_summary.txt                 (Text summary with strategy comparison)")

if __name__ == '__main__':
    main()

