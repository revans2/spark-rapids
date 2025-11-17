#!/usr/bin/env python3
"""
Visualization script for AST Post-Processing Benchmark Results

This script reads the TSV output from post_processing_analysis.scala and creates
visualizations showing the performance impact of AST filtering on inner joins.

Usage:
    python visualize_ast_post_processing.py [/path/to/output.tsv] [output_directory]
    
    Arguments:
        tsv_path (optional): Path to benchmark results TSV file
            Default: /data/tmp/post_processing_benchmark/results.tsv
        
        output_directory (optional): Directory to save plots
            Default: visualizations/ directory next to the TSV file
    
    Examples:
        # Use default paths
        python visualize_ast_post_processing.py
        
        # Specify TSV path
        python visualize_ast_post_processing.py /path/to/results.tsv
        
        # Specify both paths
        python visualize_ast_post_processing.py /path/to/results.tsv /path/to/output

Requirements:
    pip install pandas matplotlib seaborn numpy
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import re

# Default paths
DEFAULT_TSV_PATH = "/data/tmp/post_processing_benchmark/results.tsv"

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10

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
    
    # Parse test names to extract metadata
    df['test_type'] = df['TestName'].apply(extract_test_type)
    df['with_ast'] = df['TestName'].str.contains('with_post')
    df['base_name'] = df['TestName'].apply(extract_base_name)
    
    # Parse AST-specific parameters
    df['cardinality_pct'] = df['TestName'].apply(extract_cardinality)
    df['selectivity_pct'] = df['TestName'].apply(extract_selectivity)
    df['complexity'] = df['TestName'].apply(extract_complexity)
    df['input_rows'] = df['TestName'].apply(extract_input_rows)
    
    return df

def extract_test_type(test_name):
    """Extract whether this is an AST test or join type test."""
    if 'AST_' in test_name:
        return 'ast_filtering'
    elif any(x in test_name for x in ['LEFT_OUTER', 'RIGHT_OUTER', 'FULL_OUTER', 'LEFT_SEMI', 'LEFT_ANTI']):
        return 'join_type'
    return 'unknown'

def extract_base_name(test_name):
    """Extract base test name (without _with_post or _inner_only suffix)."""
    return test_name.replace('_with_post', '').replace('_inner_only', '').replace('_direct', '')

def extract_cardinality(test_name):
    """Extract cardinality percentage from test name."""
    match = re.search(r'CARD(\d+)_', test_name)
    if match:
        return int(match.group(1))
    
    # Default cardinality for complexity/selectivity tests (from test config)
    if any(x in test_name for x in ['SIMPLE', 'MEDIUM', 'COMPLEX', 'VCOMPLEX']):
        if 'CARD' not in test_name:  # Only if not explicitly set
            return 50  # Fixed at 50% cardinality per test design
    return None

def extract_selectivity(test_name):
    """Extract selectivity percentage from test name."""
    match = re.search(r'(\d+)pct_', test_name)
    if match:
        return int(match.group(1))
    
    # Default selectivity for cardinality tests (from test config)
    if 'CARD' in test_name and 'pct' not in test_name:
        return 25  # Fixed at 25% selectivity per test design
    return None

def extract_complexity(test_name):
    """Extract AST complexity from test name."""
    if 'SIMPLE' in test_name:
        return 'simple'
    elif 'MEDIUM' in test_name:
        return 'medium'
    elif 'VCOMPLEX' in test_name:
        return 'very_complex'
    elif 'COMPLEX' in test_name:
        return 'complex'
    return 'simple'  # default for CARD tests

def extract_input_rows(test_name):
    """Extract input row count from test name."""
    match = re.search(r'(\d+)M_', test_name)
    if match:
        return int(match.group(1)) * 1_000_000
    return None

def pair_baseline_and_ast(df):
    """Pair baseline (inner_only) and AST (with_post) results."""
    ast_tests = df[df['test_type'] == 'ast_filtering'].copy()
    
    pairs = []
    for base_name in ast_tests['base_name'].unique():
        base_group = ast_tests[ast_tests['base_name'] == base_name]
        
        with_post = base_group[base_group['with_ast'] == True]
        inner_only = base_group[base_group['with_ast'] == False]
        
        if len(with_post) == 1 and len(inner_only) == 1:
            wp = with_post.iloc[0]
            io = inner_only.iloc[0]
            
            overhead_ms = wp['MedianTimeMs'] - io['MedianTimeMs']
            overhead_pct = (overhead_ms / io['MedianTimeMs']) * 100 if io['MedianTimeMs'] > 0 else 0
            
            pairs.append({
                'base_name': base_name,
                'cardinality_pct': wp['cardinality_pct'],
                'selectivity_pct': wp['selectivity_pct'],
                'complexity': wp['complexity'],
                'input_rows': wp['input_rows'],
                'with_ast_ms': wp['MedianTimeMs'],
                'without_ast_ms': io['MedianTimeMs'],
                'overhead_ms': overhead_ms,
                'overhead_pct': overhead_pct,
                'left_rows': wp['LeftRows'],
                'right_rows': wp['RightRows'],
                'output_rows_ast': wp['OutputRows']
            })
    
    return pd.DataFrame(pairs)

def plot_overhead_by_cardinality(paired_df, output_dir):
    """Plot overhead vs cardinality (inner join size scaling)."""
    # Filter for simple complexity, 25% selectivity
    df = paired_df[
        (paired_df['complexity'] == 'simple') & 
        (paired_df['selectivity_pct'] == 25)
    ].copy()
    
    if len(df) == 0:
        print("No data for cardinality analysis")
        return
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Plot 1: Overhead % vs Cardinality
    for rows in sorted(df['input_rows'].unique()):
        subset = df[df['input_rows'] == rows].sort_values('cardinality_pct')
        ax1.plot(subset['cardinality_pct'], subset['overhead_pct'], 
                marker='o', linewidth=2, markersize=8, 
                label=f'{rows/1_000_000:.0f}M rows')
    
    ax1.set_xlabel('Cardinality %', fontsize=12, fontweight='bold')
    ax1.set_ylabel('AST Overhead %', fontsize=12, fontweight='bold')
    ax1.set_title('AST Post-Processing Overhead vs Inner Join Size\n(Simple condition, 25% selectivity)', 
                  fontsize=13, fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.axhline(y=0, color='black', linestyle='--', alpha=0.5)
    ax1.axhline(y=10, color='red', linestyle='--', alpha=0.3, label='10% threshold')
    
    # Plot 2: Absolute times
    x_pos = np.arange(len(df))
    width = 0.35
    
    df_sorted = df.sort_values(['input_rows', 'cardinality_pct'])
    labels = [f"{row['input_rows']/1_000_000:.0f}M\n{row['cardinality_pct']}%" 
              for _, row in df_sorted.iterrows()]
    
    ax2.bar(x_pos - width/2, df_sorted['without_ast_ms'], width, 
            label='Inner Only', alpha=0.8, color='steelblue')
    ax2.bar(x_pos + width/2, df_sorted['with_ast_ms'], width, 
            label='Inner + AST', alpha=0.8, color='coral')
    
    ax2.set_xlabel('Config (Rows / Cardinality)', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Median Time (ms)', fontsize=12, fontweight='bold')
    ax2.set_title('Absolute Performance: Inner vs Inner+AST', fontsize=13, fontweight='bold')
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels(labels, fontsize=9)
    ax2.legend()
    ax2.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/ast_overhead_by_cardinality.png', dpi=300, bbox_inches='tight')
    print(f"Saved: {output_dir}/ast_overhead_by_cardinality.png")
    plt.close()

def plot_overhead_by_selectivity(paired_df, output_dir):
    """Plot overhead vs selectivity (filter passing rate)."""
    # Filter for simple complexity, 50% cardinality, excluding 25% selectivity
    df = paired_df[
        (paired_df['complexity'] == 'simple') & 
        (paired_df['cardinality_pct'] == 50) &
        (paired_df['selectivity_pct'] != 25)
    ].copy()
    
    if len(df) == 0:
        print("No data for selectivity analysis")
        return
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Plot 1: Overhead % vs Selectivity
    for rows in sorted(df['input_rows'].unique()):
        subset = df[df['input_rows'] == rows].sort_values('selectivity_pct')
        ax1.plot(subset['selectivity_pct'], subset['overhead_pct'], 
                marker='o', linewidth=2, markersize=8, 
                label=f'{rows/1_000_000:.0f}M rows')
    
    ax1.set_xlabel('Filter Selectivity (% rows passing)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('AST Overhead %', fontsize=12, fontweight='bold')
    ax1.set_title('AST Overhead vs Filter Selectivity\n(Simple condition, 50% cardinality)', 
                  fontsize=13, fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.axhline(y=0, color='black', linestyle='--', alpha=0.5)
    ax1.axhline(y=10, color='red', linestyle='--', alpha=0.3, label='10% threshold')
    
    # Plot 2: Breakdown by selectivity bucket
    df_sorted = df.sort_values(['selectivity_pct', 'input_rows'])
    x_pos = np.arange(len(df_sorted))
    
    ax2.bar(x_pos, df_sorted['overhead_pct'], color=plt.cm.RdYlGn_r(df_sorted['overhead_pct']/100))
    
    labels = [f"{row['selectivity_pct']}%\n{row['input_rows']/1_000_000:.0f}M" 
              for _, row in df_sorted.iterrows()]
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels(labels, fontsize=9)
    ax2.set_xlabel('Selectivity / Input Size', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Overhead %', fontsize=12, fontweight='bold')
    ax2.set_title('Overhead Distribution by Selectivity', fontsize=13, fontweight='bold')
    ax2.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax2.axhline(y=10, color='red', linestyle='--', alpha=0.5)
    ax2.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/ast_overhead_by_selectivity.png', dpi=300, bbox_inches='tight')
    print(f"Saved: {output_dir}/ast_overhead_by_selectivity.png")
    plt.close()

def plot_overhead_by_complexity(paired_df, output_dir):
    """Plot overhead vs AST complexity."""
    # Filter for 25% selectivity, 50% cardinality
    df = paired_df[
        (paired_df['selectivity_pct'] == 25) & 
        (paired_df['cardinality_pct'] == 50)
    ].copy()
    
    if len(df) == 0:
        print("No data for complexity analysis")
        return
    
    # Use the largest available dataset size
    max_rows = df['input_rows'].max()
    df = df[df['input_rows'] == max_rows].copy()
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Define complexity order
    complexity_order = ['simple', 'medium', 'complex', 'very_complex']
    df['complexity_cat'] = pd.Categorical(df['complexity'], categories=complexity_order, ordered=True)
    df_sorted = df.sort_values('complexity_cat')
    
    # Plot 1: Overhead % by complexity
    colors = plt.cm.viridis(np.linspace(0, 0.8, len(df_sorted)))
    bars1 = ax1.bar(range(len(df_sorted)), df_sorted['overhead_pct'], color=colors)
    
    ax1.set_xticks(range(len(df_sorted)))
    ax1.set_xticklabels(df_sorted['complexity'], fontsize=11)
    ax1.set_xlabel('AST Condition Complexity', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Overhead %', fontsize=12, fontweight='bold')
    ax1.set_title(f'AST Overhead vs Condition Complexity\n({max_rows/1_000_000:.0f}M rows, 50% cardinality, 25% selectivity)', 
                  fontsize=13, fontweight='bold')
    ax1.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax1.axhline(y=10, color='red', linestyle='--', alpha=0.5, label='10% threshold')
    ax1.grid(True, alpha=0.3, axis='y')
    ax1.legend()
    
    # Add value labels on bars
    for bar, val in zip(bars1, df_sorted['overhead_pct']):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{val:.1f}%', ha='center', va='bottom', fontweight='bold')
    
    # Plot 2: Absolute times comparison
    x_pos = np.arange(len(df_sorted))
    width = 0.35
    
    ax2.bar(x_pos - width/2, df_sorted['without_ast_ms'], width, 
            label='Inner Only', alpha=0.8, color='steelblue')
    ax2.bar(x_pos + width/2, df_sorted['with_ast_ms'], width, 
            label='Inner + AST', alpha=0.8, color='coral')
    
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels(df_sorted['complexity'], fontsize=11)
    ax2.set_xlabel('AST Condition Complexity', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Median Time (ms)', fontsize=12, fontweight='bold')
    ax2.set_title('Absolute Performance by Complexity', fontsize=13, fontweight='bold')
    ax2.legend()
    ax2.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/ast_overhead_by_complexity.png', dpi=300, bbox_inches='tight')
    print(f"Saved: {output_dir}/ast_overhead_by_complexity.png")
    plt.close()

def plot_overhead_heatmap(paired_df, output_dir):
    """Create heatmap of overhead across multiple dimensions."""
    # Create pivot table: cardinality vs selectivity (simple complexity only)
    df = paired_df[
        (paired_df['complexity'] == 'simple') &
        (paired_df['cardinality_pct'].notna()) &
        (paired_df['selectivity_pct'].notna())
    ].copy()
    
    if len(df) == 0:
        print("No data for heatmap")
        return
    
    fig, axes = plt.subplots(1, 2, figsize=(18, 7))
    
    for idx, rows in enumerate(sorted(df['input_rows'].unique())):
        subset = df[df['input_rows'] == rows]
        
        # Create pivot table
        pivot = subset.pivot_table(
            values='overhead_pct', 
            index='selectivity_pct', 
            columns='cardinality_pct',
            aggfunc='mean'
        )
        
        # Plot heatmap
        sns.heatmap(pivot, annot=True, fmt='.1f', cmap='RdYlGn_r', 
                   center=10, vmin=-5, vmax=25,
                   cbar_kws={'label': 'Overhead %'}, ax=axes[idx])
        
        axes[idx].set_title(f'AST Overhead Heatmap - {rows/1_000_000:.0f}M Input Rows\n(Simple condition)', 
                           fontsize=13, fontweight='bold')
        axes[idx].set_xlabel('Cardinality % (affects inner join size)', fontsize=11, fontweight='bold')
        axes[idx].set_ylabel('Selectivity % (% passing filter)', fontsize=11, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/ast_overhead_heatmap.png', dpi=300, bbox_inches='tight')
    print(f"Saved: {output_dir}/ast_overhead_heatmap.png")
    plt.close()

def plot_ast_strategy_comparison_sweep(paired_df, df, output_dir):
    """
    Create a plot similar to analyze_join_type_sweep.py's plot_left_outer_strategy_comparison.
    
    Shows performance across different configurations (cardinality or size sweep) with:
    1. Baseline: Inner join without AST
    2. Inner + AST: Shows AST overhead
    3. Join types with post-processing: Shows additional overhead but enables flexibility
    
    This is the sweep plot that shows where crossovers occur.
    """
    # Get join type tests (LEFT_OUTER, etc.)
    join_type_tests = df[df['test_type'] == 'join_type'].copy()
    
    if len(join_type_tests) == 0:
        print("No join type data for progressive overhead plot")
        return
    
    # Filter for tests with post-processing and direct
    join_type_tests['is_post'] = join_type_tests['TestName'].str.contains('with_post')
    join_type_tests['is_direct'] = join_type_tests['TestName'].str.contains('direct')
    join_type_tests['base_name'] = join_type_tests['TestName'].str.replace('_with_post', '').str.replace('_direct', '')
    
    # Extract join type from test name
    def extract_join_type(name):
        for jt in ['LEFT_OUTER', 'RIGHT_OUTER', 'FULL_OUTER', 'LEFT_SEMI', 'LEFT_ANTI']:
            if jt in name:
                return jt.replace('_', ' ').title()
        return 'Unknown'
    
    join_type_tests['join_type'] = join_type_tests['TestName'].apply(extract_join_type)
    
    # Get a representative AST test (25% selectivity, 50% cardinality, 5M rows)
    ast_example = paired_df[
        (paired_df['selectivity_pct'] == 25) & 
        (paired_df['cardinality_pct'] == 50) &
        (paired_df['input_rows'] == 5_000_000) &
        (paired_df['complexity'] == 'simple')
    ]
    
    if len(ast_example) == 0:
        # Fall back to any simple test
        ast_example = paired_df[paired_df['complexity'] == 'simple']
    
    if len(ast_example) == 0:
        print("No AST data for progressive overhead plot")
        return
    
    ast_example = ast_example.iloc[0]
    
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    
    # Plot 1: Progressive overhead bar chart for a single configuration
    ax1 = axes[0, 0]
    
    # Find a representative join type test (5M x 5M, 10% cardinality)
    representative = join_type_tests[
        (join_type_tests['LeftRows'] == 5000000) &
        (join_type_tests['RightRows'] == 5000000) &
        (join_type_tests['TestName'].str.contains('10pct'))
    ]
    
    if len(representative) > 0:
        # Get one of each join type
        join_types_to_show = ['Left Outer', 'Right Outer', 'Full Outer', 'Left Semi', 'Left Anti']
        
        baseline = ast_example['without_ast_ms']
        bars_data = {
            'Baseline\n(Inner Only)': baseline,
            'Inner + AST\n(Simple)': ast_example['with_ast_ms']
        }
        
        for jt in join_types_to_show:
            jt_tests = representative[representative['join_type'] == jt]
            if len(jt_tests) > 0:
                # Get post-processing version
                post = jt_tests[jt_tests['is_post']]
                if len(post) > 0:
                    bars_data[f'{jt}\n(Post)'] = post.iloc[0]['MedianTimeMs']
        
        x_pos = np.arange(len(bars_data))
        values = list(bars_data.values())
        colors = ['steelblue', 'coral'] + ['salmon'] * (len(bars_data) - 2)
        
        bars = ax1.bar(x_pos, values, color=colors, alpha=0.7, edgecolor='black', linewidth=1.5)
        ax1.set_xticks(x_pos)
        ax1.set_xticklabels(bars_data.keys(), fontsize=10)
        ax1.set_ylabel('Median Time (ms)', fontsize=12, fontweight='bold')
        ax1.set_title('Progressive Overhead: Inner → Inner+AST → Join Types\n(5M x 5M, 10% cardinality)', 
                      fontsize=13, fontweight='bold')
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Add percentage labels
        for i, (bar, val) in enumerate(zip(bars, values)):
            overhead_pct = (val - baseline) / baseline * 100
            label = f'{val:.1f}ms'
            if i > 0:
                label += f'\n(+{overhead_pct:.1f}%)'
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                    label, ha='center', va='bottom', fontweight='bold', fontsize=9)
    
    # Plot 2: Left Outer Join - Post vs Direct with different build sides
    ax2 = axes[0, 1]
    
    left_outer = join_type_tests[join_type_tests['join_type'] == 'Left Outer']
    
    if len(left_outer) > 0:
        # Group by configuration and get post vs direct
        configs = left_outer.groupby(['LeftRows', 'RightRows', 'base_name'])
        
        data_to_plot = []
        for (left_r, right_r, base), group in configs:
            post_tests = group[group['is_post']]
            direct_tests = group[group['is_direct']]
            
            if len(post_tests) > 0 and len(direct_tests) > 0:
                post_time = post_tests['MedianTimeMs'].iloc[0]
                direct_time = direct_tests['MedianTimeMs'].iloc[0]
                
                # Get build sides
                post_build = post_tests['ActualBuildSide'].iloc[0] if 'ActualBuildSide' in post_tests.columns else 'N/A'
                direct_build = direct_tests['ActualBuildSide'].iloc[0] if 'ActualBuildSide' in direct_tests.columns else 'Right'
                
                data_to_plot.append({
                    'config': f'{left_r/1e6:.0f}M x {right_r/1e6:.0f}M',
                    'post_time': post_time,
                    'direct_time': direct_time,
                    'post_build': post_build,
                    'direct_build': direct_build,
                    'overhead_pct': (post_time - direct_time) / direct_time * 100
                })
        
        if data_to_plot:
            df_plot = pd.DataFrame(data_to_plot)
            x_pos = np.arange(len(df_plot))
            width = 0.35
            
            bars1 = ax2.bar(x_pos - width/2, df_plot['direct_time'], width, 
                           label='Direct (RightBuild only)', alpha=0.7, color='steelblue')
            bars2 = ax2.bar(x_pos + width/2, df_plot['post_time'], width, 
                           label='Post-processing (Any build)', alpha=0.7, color='coral')
            
            ax2.set_xlabel('Configuration', fontsize=12, fontweight='bold')
            ax2.set_ylabel('Median Time (ms)', fontsize=12, fontweight='bold')
            ax2.set_title('Left Outer Join: Direct vs Post-Processing\n(Post-processing enables build-side flexibility)', 
                          fontsize=13, fontweight='bold')
            ax2.set_xticks(x_pos)
            ax2.set_xticklabels(df_plot['config'], fontsize=9, rotation=45, ha='right')
            ax2.legend()
            ax2.grid(True, alpha=0.3, axis='y')
            
            # Add overhead percentages
            for i, row in df_plot.iterrows():
                if abs(row['overhead_pct']) > 2:  # Only show if > 2%
                    ax2.text(i, max(row['post_time'], row['direct_time']),
                            f'{row["overhead_pct"]:+.1f}%', ha='center', va='bottom',
                            fontsize=8, fontweight='bold')
    
    # Plot 3: All join types - Post vs Direct comparison
    ax3 = axes[1, 0]
    
    # Get summary by join type
    join_type_summary = []
    for jt in join_type_tests['join_type'].unique():
        jt_data = join_type_tests[join_type_tests['join_type'] == jt]
        
        post_times = jt_data[jt_data['is_post']]['MedianTimeMs']
        direct_times = jt_data[jt_data['is_direct']]['MedianTimeMs']
        
        if len(post_times) > 0 and len(direct_times) > 0:
            avg_post = post_times.mean()
            avg_direct = direct_times.mean()
            overhead = (avg_post - avg_direct) / avg_direct * 100
            
            join_type_summary.append({
                'join_type': jt,
                'avg_direct': avg_direct,
                'avg_post': avg_post,
                'overhead_pct': overhead
            })
    
    if join_type_summary:
        df_summary = pd.DataFrame(join_type_summary).sort_values('join_type')
        x_pos = np.arange(len(df_summary))
        width = 0.35
        
        bars1 = ax3.bar(x_pos - width/2, df_summary['avg_direct'], width, 
                       label='Direct', alpha=0.7, color='steelblue')
        bars2 = ax3.bar(x_pos + width/2, df_summary['avg_post'], width, 
                       label='Post-processing', alpha=0.7, color='coral')
        
        ax3.set_xlabel('Join Type', fontsize=12, fontweight='bold')
        ax3.set_ylabel('Average Median Time (ms)', fontsize=12, fontweight='bold')
        ax3.set_title('All Join Types: Direct vs Post-Processing\n(Average across all configurations)', 
                      fontsize=13, fontweight='bold')
        ax3.set_xticks(x_pos)
        ax3.set_xticklabels(df_summary['join_type'], fontsize=10)
        ax3.legend()
        ax3.grid(True, alpha=0.3, axis='y')
        
        # Add overhead labels
        for i, row in df_summary.iterrows():
            max_val = max(row['avg_direct'], row['avg_post'])
            color = 'red' if row['overhead_pct'] > 10 else 'green' if row['overhead_pct'] < 0 else 'orange'
            ax3.text(i, max_val, f'{row["overhead_pct"]:+.1f}%',
                    ha='center', va='bottom', fontsize=9, fontweight='bold', color=color)
    
    # Plot 4: Stacked overhead breakdown
    ax4 = axes[1, 1]
    
    # Show cumulative overhead: baseline → +AST → +join type
    if len(representative) > 0 and len(ast_example) > 0:
        baseline = ast_example['without_ast_ms']
        ast_overhead = ast_example['overhead_ms']
        
        breakdown_data = []
        for jt in ['Left Outer', 'Right Outer', 'Full Outer', 'Left Semi', 'Left Anti']:
            jt_tests = representative[representative['join_type'] == jt]
            if len(jt_tests) > 0:
                post = jt_tests[jt_tests['is_post']]
                if len(post) > 0:
                    total_time = post.iloc[0]['MedianTimeMs']
                    join_overhead = total_time - ast_example['with_ast_ms']
                    
                    breakdown_data.append({
                        'join_type': jt,
                        'baseline': baseline,
                        'ast_overhead': ast_overhead,
                        'join_overhead': max(0, join_overhead),  # Ensure non-negative
                        'total': total_time
                    })
        
        if breakdown_data:
            df_breakdown = pd.DataFrame(breakdown_data)
            x_pos = np.arange(len(df_breakdown))
            
            p1 = ax4.bar(x_pos, df_breakdown['baseline'], label='Inner Join (baseline)', color='steelblue', alpha=0.7)
            p2 = ax4.bar(x_pos, df_breakdown['ast_overhead'], bottom=df_breakdown['baseline'],
                        label='AST Overhead', color='coral', alpha=0.7)
            p3 = ax4.bar(x_pos, df_breakdown['join_overhead'], 
                        bottom=df_breakdown['baseline'] + df_breakdown['ast_overhead'],
                        label='Join Type Overhead', color='salmon', alpha=0.7)
            
            ax4.set_xlabel('Join Type with Post-Processing', fontsize=12, fontweight='bold')
            ax4.set_ylabel('Median Time (ms)', fontsize=12, fontweight='bold')
            ax4.set_title('Stacked Overhead Breakdown\n(Base Inner → +AST → +Join Type)', 
                          fontsize=13, fontweight='bold')
            ax4.set_xticks(x_pos)
            ax4.set_xticklabels(df_breakdown['join_type'], fontsize=10, rotation=45, ha='right')
            ax4.legend(loc='upper left')
            ax4.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/progressive_overhead_analysis.png', dpi=300, bbox_inches='tight')
    print(f"Saved: {output_dir}/progressive_overhead_analysis.png")
    plt.close()

def plot_summary_dashboard(paired_df, output_dir):
    """Create a comprehensive summary dashboard."""
    fig = plt.figure(figsize=(20, 12))
    gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
    
    # Overall statistics
    ax_stats = fig.add_subplot(gs[0, :])
    ax_stats.axis('off')
    
    stats_text = f"""
    AST POST-PROCESSING PERFORMANCE SUMMARY
    
    Total Test Configurations: {len(paired_df)}
    
    Overall Statistics:
    • Average Overhead: {paired_df['overhead_pct'].mean():.2f}%
    • Median Overhead: {paired_df['overhead_pct'].median():.2f}%
    • Min Overhead: {paired_df['overhead_pct'].min():.2f}% (best case)
    • Max Overhead: {paired_df['overhead_pct'].max():.2f}% (worst case)
    • Overhead Std Dev: {paired_df['overhead_pct'].std():.2f}%
    
    Overhead Distribution:
    • < 5% overhead: {len(paired_df[paired_df['overhead_pct'] < 5])} tests ({len(paired_df[paired_df['overhead_pct'] < 5])/len(paired_df)*100:.1f}%)
    • 5-10% overhead: {len(paired_df[(paired_df['overhead_pct'] >= 5) & (paired_df['overhead_pct'] < 10)])} tests
    • 10-20% overhead: {len(paired_df[(paired_df['overhead_pct'] >= 10) & (paired_df['overhead_pct'] < 20)])} tests
    • > 20% overhead: {len(paired_df[paired_df['overhead_pct'] >= 20])} tests ({len(paired_df[paired_df['overhead_pct'] >= 20])/len(paired_df)*100:.1f}%)
    """
    
    ax_stats.text(0.1, 0.5, stats_text, fontsize=11, family='monospace',
                  verticalalignment='center')
    
    # Plot 1: Overhead distribution histogram
    ax1 = fig.add_subplot(gs[1, 0])
    ax1.hist(paired_df['overhead_pct'], bins=20, color='steelblue', alpha=0.7, edgecolor='black')
    ax1.axvline(paired_df['overhead_pct'].mean(), color='red', linestyle='--', 
               linewidth=2, label=f'Mean: {paired_df["overhead_pct"].mean():.1f}%')
    ax1.axvline(paired_df['overhead_pct'].median(), color='green', linestyle='--', 
               linewidth=2, label=f'Median: {paired_df["overhead_pct"].median():.1f}%')
    ax1.set_xlabel('Overhead %', fontweight='bold')
    ax1.set_ylabel('Frequency', fontweight='bold')
    ax1.set_title('Overhead Distribution', fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Overhead by cardinality (box plot)
    ax2 = fig.add_subplot(gs[1, 1])
    df_card = paired_df[paired_df['cardinality_pct'].notna()].copy()
    if len(df_card) > 0:
        df_card['card_str'] = df_card['cardinality_pct'].astype(int).astype(str) + '%'
        sns.boxplot(data=df_card, x='card_str', y='overhead_pct', hue='card_str', ax=ax2, 
                   palette='Set2', legend=False)
        ax2.set_xlabel('Cardinality %', fontweight='bold')
        ax2.set_ylabel('Overhead %', fontweight='bold')
        ax2.set_title('Overhead by Cardinality', fontweight='bold')
        ax2.axhline(y=10, color='red', linestyle='--', alpha=0.5)
        ax2.grid(True, alpha=0.3, axis='y')
    
    # Plot 3: Overhead by selectivity (box plot)
    ax3 = fig.add_subplot(gs[1, 2])
    df_sel = paired_df[paired_df['selectivity_pct'].notna()].copy()
    if len(df_sel) > 0:
        df_sel['sel_str'] = df_sel['selectivity_pct'].astype(int).astype(str) + '%'
        sns.boxplot(data=df_sel, x='sel_str', y='overhead_pct', hue='sel_str', ax=ax3, 
                   palette='Set3', legend=False)
        ax3.set_xlabel('Selectivity %', fontweight='bold')
        ax3.set_ylabel('Overhead %', fontweight='bold')
        ax3.set_title('Overhead by Selectivity', fontweight='bold')
        ax3.axhline(y=10, color='red', linestyle='--', alpha=0.5)
        ax3.grid(True, alpha=0.3, axis='y')
    
    # Plot 4: Overhead by complexity (violin plot)
    ax4 = fig.add_subplot(gs[2, 0])
    df_comp = paired_df[paired_df['complexity'].notna()].copy()
    if len(df_comp) > 0:
        complexity_order = ['simple', 'medium', 'complex', 'very_complex']
        df_comp['complexity_cat'] = pd.Categorical(df_comp['complexity'], 
                                                   categories=complexity_order, ordered=True)
        sns.violinplot(data=df_comp, x='complexity', y='overhead_pct', hue='complexity', ax=ax4, 
                      order=complexity_order, palette='muted', legend=False)
        ax4.set_xlabel('Complexity', fontweight='bold')
        ax4.set_ylabel('Overhead %', fontweight='bold')
        ax4.set_title('Overhead by Complexity', fontweight='bold')
        ax4.axhline(y=10, color='red', linestyle='--', alpha=0.5)
        ax4.grid(True, alpha=0.3, axis='y')
    
    # Plot 5: Scatter - overhead vs input size
    ax5 = fig.add_subplot(gs[2, 1])
    df_rows = paired_df[paired_df['input_rows'].notna()].copy()
    if len(df_rows) > 0:
        scatter = ax5.scatter(df_rows['input_rows']/1_000_000, df_rows['overhead_pct'], 
                            c=df_rows['cardinality_pct'], cmap='viridis', 
                            s=100, alpha=0.6, edgecolors='black')
        ax5.set_xlabel('Input Size (M rows)', fontweight='bold')
        ax5.set_ylabel('Overhead %', fontweight='bold')
        ax5.set_title('Overhead vs Input Size', fontweight='bold')
        ax5.axhline(y=10, color='red', linestyle='--', alpha=0.5)
        ax5.grid(True, alpha=0.3)
        plt.colorbar(scatter, ax=ax5, label='Cardinality %')
    
    # Plot 6: Recommendations
    ax6 = fig.add_subplot(gs[2, 2])
    ax6.axis('off')
    
    avg_overhead = paired_df['overhead_pct'].mean()
    
    if avg_overhead < 10:
        recommendation = "✓ AST post-filtering is EFFICIENT\n\nRecommendations:\n• Use for complex conditions\n• Safe for production\n• Low overhead observed"
        color = 'green'
    elif avg_overhead < 20:
        recommendation = "~ AST post-filtering is ACCEPTABLE\n\nRecommendations:\n• Use when necessary\n• Monitor performance\n• Consider alternatives for\n  high-throughput paths"
        color = 'orange'
    else:
        recommendation = "⚠ AST post-filtering has HIGH overhead\n\nRecommendations:\n• Prefer filter pushdown\n• Consider rewriting queries\n• Use only when essential"
        color = 'red'
    
    ax6.text(0.5, 0.5, recommendation, fontsize=12, family='monospace',
            verticalalignment='center', horizontalalignment='center',
            bbox=dict(boxstyle='round', facecolor=color, alpha=0.3))
    
    plt.savefig(f'{output_dir}/ast_summary_dashboard.png', dpi=300, bbox_inches='tight')
    print(f"Saved: {output_dir}/ast_summary_dashboard.png")
    plt.close()

def generate_report(paired_df, output_dir):
    """Generate a text report with key findings."""
    report_path = f'{output_dir}/ast_analysis_report.txt'
    
    with open(report_path, 'w') as f:
        f.write("="*80 + "\n")
        f.write("AST POST-PROCESSING PERFORMANCE ANALYSIS REPORT\n")
        f.write("="*80 + "\n\n")
        
        f.write("OVERALL STATISTICS\n")
        f.write("-"*80 + "\n")
        f.write(f"Total Configurations Tested: {len(paired_df)}\n")
        f.write(f"Average Overhead: {paired_df['overhead_pct'].mean():.2f}%\n")
        f.write(f"Median Overhead: {paired_df['overhead_pct'].median():.2f}%\n")
        f.write(f"Std Dev: {paired_df['overhead_pct'].std():.2f}%\n")
        f.write(f"Min Overhead: {paired_df['overhead_pct'].min():.2f}%\n")
        f.write(f"Max Overhead: {paired_df['overhead_pct'].max():.2f}%\n\n")
        
        f.write("OVERHEAD BY CARDINALITY\n")
        f.write("-"*80 + "\n")
        for card in sorted(paired_df['cardinality_pct'].dropna().unique()):
            subset = paired_df[paired_df['cardinality_pct'] == card]
            f.write(f"{int(card)}% cardinality: avg={subset['overhead_pct'].mean():.2f}%, "
                   f"median={subset['overhead_pct'].median():.2f}%\n")
        f.write("\n")
        
        f.write("OVERHEAD BY SELECTIVITY\n")
        f.write("-"*80 + "\n")
        for sel in sorted(paired_df['selectivity_pct'].dropna().unique()):
            subset = paired_df[paired_df['selectivity_pct'] == sel]
            f.write(f"{int(sel)}% selectivity: avg={subset['overhead_pct'].mean():.2f}%, "
                   f"median={subset['overhead_pct'].median():.2f}%\n")
        f.write("\n")
        
        f.write("OVERHEAD BY COMPLEXITY\n")
        f.write("-"*80 + "\n")
        for comp in ['simple', 'medium', 'complex', 'very_complex']:
            subset = paired_df[paired_df['complexity'] == comp]
            if len(subset) > 0:
                f.write(f"{comp}: avg={subset['overhead_pct'].mean():.2f}%, "
                       f"median={subset['overhead_pct'].median():.2f}%\n")
        f.write("\n")
        
        f.write("KEY FINDINGS\n")
        f.write("-"*80 + "\n")
        
        # Finding 1: Cardinality impact
        high_card = paired_df[paired_df['cardinality_pct'] >= 50]['overhead_pct'].mean()
        low_card = paired_df[paired_df['cardinality_pct'] <= 10]['overhead_pct'].mean()
        
        if abs(high_card - low_card) > 10:
            f.write(f"1. CARDINALITY IMPACT: SIGNIFICANT\n")
            f.write(f"   High cardinality (≥50%): {high_card:.2f}% avg overhead\n")
            f.write(f"   Low cardinality (≤10%): {low_card:.2f}% avg overhead\n")
            f.write(f"   → AST overhead scales with inner join result size\n\n")
        else:
            f.write(f"1. CARDINALITY IMPACT: MINIMAL\n")
            f.write(f"   → AST overhead is relatively constant\n\n")
        
        # Finding 2: Selectivity impact
        high_sel = paired_df[paired_df['selectivity_pct'] >= 75]['overhead_pct'].mean()
        low_sel = paired_df[paired_df['selectivity_pct'] <= 25]['overhead_pct'].mean()
        
        if abs(high_sel - low_sel) > 10:
            f.write(f"2. SELECTIVITY IMPACT: SIGNIFICANT\n")
            f.write(f"   High selectivity (≥75%): {high_sel:.2f}% avg overhead\n")
            f.write(f"   Low selectivity (≤25%): {low_sel:.2f}% avg overhead\n\n")
        else:
            f.write(f"2. SELECTIVITY IMPACT: MINIMAL\n")
            f.write(f"   → Filter passing rate has little effect on overhead\n\n")
        
        f.write("RECOMMENDATIONS\n")
        f.write("-"*80 + "\n")
        avg = paired_df['overhead_pct'].mean()
        if avg < 10:
            f.write("✓ AST post-filtering is generally EFFICIENT (<10% overhead)\n")
            f.write("→ Safe to use for complex conditions\n")
        elif avg < 20:
            f.write("~ AST post-filtering has MODERATE overhead (10-20%)\n")
            f.write("→ Use when necessary, monitor performance\n")
        else:
            f.write("⚠ AST post-filtering has HIGH overhead (>20%)\n")
            f.write("→ Prefer filter pushdown or query rewriting\n")
    
    print(f"Saved: {report_path}")

def plot_size_sweep_analysis(df, output_dir):
    """
    Plot the size sweep showing:
    1. Inner join baseline (no AST)
    2. Inner join + AST (showing AST overhead)
    3. Left Outer + post-processing (showing build-side flexibility)
    
    Similar to join_type_sweep_benchmark.scala visualization.
    """
    # Filter for sweep tests
    sweep_tests = df[df['TestName'].str.contains('SWEEP_', na=False)].copy()
    
    if len(sweep_tests) == 0:
        print("No sweep data found, skipping size sweep visualization")
        return
    
    # Extract left percentage from test name
    def extract_left_pct(name):
        import re
        match = re.search(r'L(\d+)pct', name)
        if match:
            return int(match.group(1))
        return None
    
    sweep_tests['left_pct'] = sweep_tests['TestName'].apply(extract_left_pct)
    sweep_tests = sweep_tests[sweep_tests['left_pct'].notna()].copy()
    
    # Categorize tests
    sweep_tests['test_category'] = 'unknown'
    sweep_tests.loc[sweep_tests['TestName'].str.contains('no_ast'), 'test_category'] = 'inner_no_ast'
    sweep_tests.loc[sweep_tests['TestName'].str.contains('with_ast'), 'test_category'] = 'inner_with_ast'
    sweep_tests.loc[sweep_tests['TestName'].str.contains('LEFT_OUTER'), 'test_category'] = 'left_outer_post'
    
    # Extract build side for left outer tests
    if 'ActualBuildSide' in sweep_tests.columns:
        sweep_tests['build_side'] = sweep_tests['ActualBuildSide']
    else:
        sweep_tests['build_side'] = 'Unknown'
    
    # Create the plot
    fig, ax = plt.subplots(1, 1, figsize=(16, 10))
    
    # Plot inner join baseline (no AST)
    inner_no_ast = sweep_tests[sweep_tests['test_category'] == 'inner_no_ast'].sort_values('left_pct')
    if len(inner_no_ast) > 0:
        ax.plot(inner_no_ast['left_pct'], inner_no_ast['MedianTimeMs'],
               marker='o', label='Inner Join (no AST) - Baseline', 
               linewidth=3, markersize=10, color='steelblue', linestyle='-')
    
    # Plot inner join with AST
    inner_with_ast = sweep_tests[sweep_tests['test_category'] == 'inner_with_ast'].sort_values('left_pct')
    if len(inner_with_ast) > 0:
        ax.plot(inner_with_ast['left_pct'], inner_with_ast['MedianTimeMs'],
               marker='^', label='Inner Join + AST (25% selectivity)', 
               linewidth=3, markersize=10, color='coral', linestyle='--')
    
    # Plot left outer with post-processing, colored by build side
    left_outer = sweep_tests[sweep_tests['test_category'] == 'left_outer_post'].sort_values('left_pct')
    if len(left_outer) > 0:
        # Group by build side
        for build_side in left_outer['build_side'].unique():
            lo_build = left_outer[left_outer['build_side'] == build_side].sort_values('left_pct')
            
            if build_side == 'LeftBuild' or build_side == 'Left':
                color = 'green'
                marker = 's'
                label = f'Left Outer + Post (LeftBuild)'
                linestyle = '-.'
            elif build_side == 'RightBuild' or build_side == 'Right':
                color = 'purple'
                marker = 'D'
                label = f'Left Outer + Post (RightBuild)'
                linestyle = ':'
            else:
                color = 'gray'
                marker = 'x'
                label = f'Left Outer + Post ({build_side})'
                linestyle = '-'
            
            if len(lo_build) > 0:
                ax.plot(lo_build['left_pct'], lo_build['MedianTimeMs'],
                       marker=marker, label=label, 
                       linewidth=2.5, markersize=9, color=color, linestyle=linestyle)
    
    # Formatting
    ax.set_xlabel('Left Table Percentage (%)', fontsize=14, fontweight='bold')
    ax.set_ylabel('Median Time (ms)', fontsize=14, fontweight='bold')
    ax.set_title('AST Post-Processing Size Sweep: Inner vs Left Outer\n' +
                'Total ~92,680 rows, 100% cardinality (1 key), AST 25% selectivity',
                fontsize=16, fontweight='bold')
    ax.legend(fontsize=12, loc='best')
    ax.grid(True, alpha=0.3)
    ax.set_xscale('log')
    
    # Add 50% vertical line
    ax.axvline(x=50, color='red', linestyle='--', alpha=0.5, linewidth=2)
    
    # Annotate key observations
    if len(inner_no_ast) > 0 and len(inner_with_ast) > 0:
        # Calculate average overhead
        merged = pd.merge(inner_no_ast[['left_pct', 'MedianTimeMs']], 
                         inner_with_ast[['left_pct', 'MedianTimeMs']], 
                         on='left_pct', suffixes=('_no_ast', '_with_ast'))
        merged['overhead_pct'] = (merged['MedianTimeMs_with_ast'] - merged['MedianTimeMs_no_ast']) / merged['MedianTimeMs_no_ast'] * 100
        avg_overhead = merged['overhead_pct'].mean()
        
        ax.text(0.02, 0.98, f'Avg AST Overhead: {avg_overhead:+.1f}%',
               transform=ax.transAxes, fontsize=12, fontweight='bold',
               verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/size_sweep_analysis.png', dpi=300, bbox_inches='tight')
    print(f"Saved: {output_dir}/size_sweep_analysis.png")
    plt.close()
    
    # Print summary table
    print("\n" + "="*80)
    print("SIZE SWEEP SUMMARY")
    print("="*80)
    print(f"\n{'Left%':<8} {'Inner(no AST)':<16} {'Inner+AST':<16} {'Overhead%':<12} {'LeftOuter+Post':<16} {'Build':<10}")
    print("-"*80)
    
    for pct in sorted(sweep_tests['left_pct'].unique()):
        row_data = sweep_tests[sweep_tests['left_pct'] == pct]
        
        inner_no = row_data[row_data['test_category'] == 'inner_no_ast']
        inner_yes = row_data[row_data['test_category'] == 'inner_with_ast']
        left_out = row_data[row_data['test_category'] == 'left_outer_post']
        
        inner_no_time = inner_no['MedianTimeMs'].values[0] if len(inner_no) > 0 else None
        inner_yes_time = inner_yes['MedianTimeMs'].values[0] if len(inner_yes) > 0 else None
        left_out_time = left_out['MedianTimeMs'].values[0] if len(left_out) > 0 else None
        left_out_build = left_out['build_side'].values[0] if len(left_out) > 0 else 'N/A'
        
        if inner_no_time and inner_yes_time:
            overhead = (inner_yes_time - inner_no_time) / inner_no_time * 100
            print(f"{pct:<8} {inner_no_time:<16.2f} {inner_yes_time:<16.2f} {overhead:<12.1f} {left_out_time if left_out_time else 'N/A':<16} {left_out_build:<10}")

def main():
    # Parse command line arguments
    if len(sys.argv) > 1:
        tsv_path = sys.argv[1]
    else:
        tsv_path = DEFAULT_TSV_PATH
    
    if len(sys.argv) > 2:
        output_dir = sys.argv[2]
    else:
        # Default: create visualizations/ directory next to TSV
        tsv_dir = os.path.dirname(os.path.abspath(tsv_path))
        output_dir = os.path.join(tsv_dir, 'visualizations')
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir}")
    print()
    
    # Load data
    df = load_benchmark_data(tsv_path)
    
    # Generate size sweep visualization FIRST (this is the PRIMARY plot requested by user)
    print("\nGenerating size sweep analysis (PRIMARY PLOT)...")
    print()
    plot_size_sweep_analysis(df, output_dir)
    
    # Pair baseline and AST results
    print("\nPairing baseline and AST results...")
    paired_df = pair_baseline_and_ast(df)
    print(f"Created {len(paired_df)} paired comparisons")
    print()
    
    if len(paired_df) == 0:
        print("WARNING: No paired results found. Skipping paired visualizations.")
    else:
        # Generate visualizations
        print("Generating visualizations...")
        print()
        
        plot_overhead_by_cardinality(paired_df, output_dir)
        plot_overhead_by_selectivity(paired_df, output_dir)
        plot_overhead_by_complexity(paired_df, output_dir)
        plot_overhead_heatmap(paired_df, output_dir)
        plot_summary_dashboard(paired_df, output_dir)
        
        # Generate report
        print("\nGenerating analysis report...")
        generate_report(paired_df, output_dir)
    
    print("\n" + "="*80)
    print("VISUALIZATION COMPLETE!")
    print("="*80)
    print(f"\nAll outputs saved to: {output_dir}")
    print("\nGenerated files:")
    print("  • size_sweep_analysis.png  (PRIMARY - shows Inner baseline → Inner+AST → Left Outer build-side flexibility)")
    print("  • ast_overhead_by_cardinality.png")
    print("  • ast_overhead_by_selectivity.png")
    print("  • ast_overhead_by_complexity.png")
    print("  • ast_overhead_heatmap.png")
    print("  • ast_summary_dashboard.png")
    print("  • ast_analysis_report.txt")

if __name__ == '__main__':
    main()

