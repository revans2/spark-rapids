#!/usr/bin/env python3
"""
Key Remapping Performance Analysis

This script analyzes the detailed timing data from key_remapping_benchmark.scala
to understand the performance impact and breakdown of key remapping optimization.

It provides:
- Timing breakdown analysis (structure build, remap build/probe, join creation, execution)
- Performance comparison (with vs without remapping)
- Analysis by key type, distribution, table size, and cardinality
- Decision tree models to predict when remapping should be enabled
- Visualizations of performance patterns

Usage:
    python key_remapping_analysis.py /path/to/remapping_benchmark.tsv

Requirements:
    pip install pandas numpy matplotlib seaborn scipy scikit-learn
"""

import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.tree import DecisionTreeClassifier, export_text
from sklearn.metrics import accuracy_score, classification_report
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)

def load_data(tsv_path):
    """Load and prepare benchmark data."""
    print("="*80)
    print("LOADING KEY REMAPPING BENCHMARK DATA")
    print("="*80)
    print()
    
    df = pd.read_csv(tsv_path, sep='\t')
    print(f"Loaded {len(df)} benchmark results")
    
    # Filter to successful runs only
    success_df = df[df['Status'] == 'SUCCESS'].copy()
    print(f"Successful runs: {len(success_df)} ({len(success_df)/len(df)*100:.1f}%)")
    print()
    
    if len(success_df) == 0:
        print("ERROR: No successful benchmark runs found!")
        sys.exit(1)
    
    # Extract test configuration from test name
    # Format: Ncol_keytype_bXXXk_pXXXk_cardXX_distname_strategy_swap[_remap]
    def parse_test_name(name):
        import re
        
        # Check if has remapping by looking at the end
        has_remap = name.endswith('_swap_remap')
        
        # Extract strategy (hash or sort) - it's right before _swap or _swap_remap
        if has_remap:
            # Remove _swap_remap from the end
            name_without_suffix = name[:-len('_swap_remap')]
            strategy_match = re.search(r'_(hash|sort)$', name_without_suffix)
        else:
            # Remove _swap from the end
            name_without_suffix = name[:-len('_swap')]
            strategy_match = re.search(r'_(hash|sort)$', name_without_suffix)
        
        strategy = strategy_match.group(1) if strategy_match else 'unknown'
        name_without_strategy = name_without_suffix[:strategy_match.start()] if strategy_match else name_without_suffix
        
        # Parse from the beginning: Ncol_keytype_bXXXk_pXXXk_cardXX_distribution
        # Extract num columns
        num_cols_match = re.match(r'^(\d+)col_', name_without_strategy)
        num_key_columns = int(num_cols_match.group(1)) if num_cols_match else 0
        
        # Extract build size
        build_match = re.search(r'_b(\d+)k_', name_without_strategy)
        build_size = f"b{build_match.group(1)}k" if build_match else 'unknown'
        
        # Extract probe size
        probe_match = re.search(r'_p(\d+)k_', name_without_strategy)
        probe_size = f"p{probe_match.group(1)}k" if probe_match else 'unknown'
        
        # Extract cardinality
        card_match = re.search(r'_card(\d+)_', name_without_strategy)
        cardinality = int(card_match.group(1)) / 100.0 if card_match else 0.0
        
        # Extract key type (between Ncol_ and _bXXXk)
        if num_cols_match and build_match:
            key_type_start = num_cols_match.end()
            key_type_end = build_match.start()
            key_type = name_without_strategy[key_type_start:key_type_end]
        else:
            key_type = 'unknown'
        
        # Extract distribution (between _cardXX_ and _strategy)
        if card_match:
            dist_start = card_match.end()
            distribution = name_without_strategy[dist_start:]
        else:
            distribution = 'unknown'
        
        return {
            'num_key_columns': num_key_columns,
            'key_type': key_type,
            'build_size': build_size,
            'probe_size': probe_size,
            'cardinality': cardinality,
            'distribution': distribution,
            'strategy': strategy,
            'has_remap': has_remap
        }
    
    # Parse test names
    parsed = success_df['TestName'].apply(parse_test_name)
    for col in parsed.iloc[0].keys():
        success_df[col] = [p[col] for p in parsed]
    
    print("Data Overview:")
    print(f"  Test configurations: {success_df['TestName'].nunique()}")
    print(f"  Key column counts: {sorted(success_df['num_key_columns'].unique())}")
    print(f"  Key types: {sorted(success_df['key_type'].unique())}")
    print(f"  Distributions: {sorted(success_df['distribution'].unique())}")
    print(f"  Strategies: {sorted(success_df['strategy'].unique())}")
    print(f"  With remapping: {len(success_df[success_df['has_remap']])}")
    print(f"  Without remapping: {len(success_df[~success_df['has_remap']])}")
    print()
    
    return success_df

def analyze_timing_breakdown(df):
    """Analyze the detailed timing breakdown for remapping."""
    print("="*80)
    print("TIMING BREAKDOWN ANALYSIS")
    print("="*80)
    print()
    
    # Filter to runs with remapping enabled and detailed timings
    remap_df = df[df['has_remap'] & df['RemapStructureBuildMs'].notna()].copy()
    
    if len(remap_df) == 0:
        print("No detailed timing data available")
        return
    
    # Calculate timing components
    remap_df['TotalRemapMs'] = (remap_df['RemapStructureBuildMs'] + 
                                  remap_df['RemapBuildKeysMs'] + 
                                  remap_df['RemapProbeKeysMs'])
    remap_df['RemapOverheadPct'] = (remap_df['TotalRemapMs'] / remap_df['MedianTimeMs']) * 100
    
    # Analyze each strategy separately
    for strategy in sorted(remap_df['strategy'].unique()):
        strat_df = remap_df[remap_df['strategy'] == strategy]
        
        print(f"{'='*80}")
        print(f"TIMING BREAKDOWN: {strategy.upper()} STRATEGY")
        print(f"{'='*80}")
        print()
        
        print(f"Overall Timing ({strategy}):")
        print(f"  Remap structure build:  {strat_df['RemapStructureBuildMs'].mean():.3f}ms ± {strat_df['RemapStructureBuildMs'].std():.3f}ms")
        print(f"  Remap build keys:       {strat_df['RemapBuildKeysMs'].mean():.3f}ms ± {strat_df['RemapBuildKeysMs'].std():.3f}ms")
        print(f"  Remap probe keys:       {strat_df['RemapProbeKeysMs'].mean():.3f}ms ± {strat_df['RemapProbeKeysMs'].std():.3f}ms")
        print(f"  Create build object:    {strat_df['CreateBuildObjectMs'].mean():.3f}ms ± {strat_df['CreateBuildObjectMs'].std():.3f}ms")
        print(f"  Execute join:           {strat_df['ExecuteJoinMs'].mean():.3f}ms ± {strat_df['ExecuteJoinMs'].std():.3f}ms")
        print(f"  Total remapping time:   {strat_df['TotalRemapMs'].mean():.3f}ms ± {strat_df['TotalRemapMs'].std():.3f}ms")
        print(f"  Remapping overhead:     {strat_df['RemapOverheadPct'].mean():.1f}% of total time")
        print()
        
        # Breakdown by key columns for this strategy
        print(f"Breakdown by Number of Key Columns ({strategy}):")
        for num_cols in sorted(strat_df['num_key_columns'].unique()):
            col_df = strat_df[strat_df['num_key_columns'] == num_cols]
            print(f"\n  {num_cols} column(s):")
            print(f"    Remap structure: {col_df['RemapStructureBuildMs'].mean():.3f}ms")
            print(f"    Remap build:     {col_df['RemapBuildKeysMs'].mean():.3f}ms")
            print(f"    Remap probe:     {col_df['RemapProbeKeysMs'].mean():.3f}ms")
            print(f"    Total remap:     {col_df['TotalRemapMs'].mean():.3f}ms")
            print(f"    Overhead:        {col_df['RemapOverheadPct'].mean():.1f}%")
        print()
        print()

def analyze_performance_impact(df):
    """Analyze the performance impact of key remapping."""
    print("="*80)
    print("PERFORMANCE IMPACT ANALYSIS")
    print("="*80)
    print()
    
    # Pair up with/without remapping runs
    # Extract base test name (without the swap/remap suffix)
    # The base name includes the strategy (hash or sort)
    # Format: base_strategy_swap or base_strategy_swap_remap
    def get_base_name(test_name):
        # Remove _remap if present, then remove _swap
        without_remap = test_name.replace('_swap_remap', '_swap')
        without_swap = without_remap.replace('_swap', '')
        return without_swap
    
    df['base_test'] = df['TestName'].apply(get_base_name)
    
    # Get tests that have both with and without remapping (should have pairs)
    test_counts = df.groupby('base_test').size()
    paired_tests = test_counts[test_counts == 2].index
    
    paired_df = df[df['base_test'].isin(paired_tests)].copy()
    
    print(f"Found {len(paired_tests)} tests with both remapping on/off")
    print()
    
    # Calculate speedup for each paired test
    speedups = []
    for test in paired_tests:
        test_df = paired_df[paired_df['base_test'] == test]
        without = test_df[~test_df['has_remap']]['MedianTimeMs'].values[0]
        with_remap = test_df[test_df['has_remap']]['MedianTimeMs'].values[0]
        
        speedup = without / with_remap
        speedups.append({
            'test': test,
            'without_remap_ms': without,
            'with_remap_ms': with_remap,
            'speedup': speedup,
            'improvement_pct': (speedup - 1.0) * 100,
            **test_df.iloc[0][['num_key_columns', 'key_type', 'distribution', 
                               'strategy', 'cardinality', 'build_size', 'probe_size']].to_dict()
        })
    
    speedup_df = pd.DataFrame(speedups)
    
    # Analyze each strategy separately
    for strategy in sorted(speedup_df['strategy'].unique()):
        strat_df = speedup_df[speedup_df['strategy'] == strategy]
        
        print(f"{'='*80}")
        print(f"PERFORMANCE IMPACT: {strategy.upper()} STRATEGY")
        print(f"{'='*80}")
        print()
        
        print(f"Statistics ({len(strat_df)} test pairs):")
        print(f"  Mean speedup:       {strat_df['speedup'].mean():.2f}x")
        print(f"  Median speedup:     {strat_df['speedup'].median():.2f}x")
        print(f"  Best speedup:       {strat_df['speedup'].max():.2f}x")
        print(f"  Worst speedup:      {strat_df['speedup'].min():.2f}x")
        print(f"  Std dev:            {strat_df['speedup'].std():.2f}x")
        print()
        
        improvements = strat_df[strat_df['speedup'] > 1.05]  # >5% improvement
        regressions = strat_df[strat_df['speedup'] < 0.95]   # >5% regression
        neutral = strat_df[(strat_df['speedup'] >= 0.95) & (strat_df['speedup'] <= 1.05)]
        
        print(f"  Tests improved (>5%):   {len(improvements)} ({len(improvements)/len(strat_df)*100:.1f}%)")
        print(f"  Tests regressed (>5%):  {len(regressions)} ({len(regressions)/len(strat_df)*100:.1f}%)")
        print(f"  Tests neutral (±5%):    {len(neutral)} ({len(neutral)/len(strat_df)*100:.1f}%)")
        print()
        
        # Key type analysis for this strategy
        print(f"Performance by Key Type ({strategy}):")
        key_type_stats = strat_df.groupby('key_type').agg({
            'speedup': ['mean', 'median', 'count']
        }).round(2)
        key_type_stats = key_type_stats.sort_values(('speedup', 'mean'), ascending=False)
        
        for key_type in key_type_stats.index:
            mean_spd = key_type_stats.loc[key_type, ('speedup', 'mean')]
            median_spd = key_type_stats.loc[key_type, ('speedup', 'median')]
            count = int(key_type_stats.loc[key_type, ('speedup', 'count')])
            print(f"  {key_type:30s}: {mean_spd:.2f}x mean, {median_spd:.2f}x median ({count} tests)")
        print()
        print()
    
    # Worst cases (regressions)
    if len(regressions) > 0:
        print("Top 10 Regressions (worst slowdowns):")
        worst_regressions = speedup_df.nsmallest(10, 'speedup')
        for idx, row in worst_regressions.iterrows():
            print(f"  {row['speedup']:.2f}x - {row['strategy']} - {row['key_type']} - "
                  f"{row['num_key_columns']}col - {row['distribution']} - "
                  f"{row['build_size']} / {row['probe_size']}")
        print()
    
    return speedup_df

def analyze_scaling(df):
    """Analyze how remapping performance scales with data size."""
    print("="*80)
    print("SCALING ANALYSIS")
    print("="*80)
    print()
    
    # Parse size strings to numeric values
    def parse_size(size_str):
        # Format: bXXXk or similar
        val = int(''.join(filter(str.isdigit, size_str)))
        if 'k' in size_str.lower():
            return val * 1000
        elif 'm' in size_str.lower():
            return val * 1000000
        return val
    
    df['build_rows'] = df['build_size'].apply(parse_size)
    df['probe_rows'] = df['probe_size'].apply(parse_size)
    df['total_rows'] = df['build_rows'] + df['probe_rows']
    
    # Analyze remapping time scaling
    remap_df = df[df['has_remap'] & df['RemapStructureBuildMs'].notna()].copy()
    
    if len(remap_df) == 0:
        print("No detailed timing data for scaling analysis")
        return
    
    # Analyze each strategy separately
    for strategy in sorted(remap_df['strategy'].unique()):
        strat_df = remap_df[remap_df['strategy'] == strategy]
        
        print(f"{'='*80}")
        print(f"SCALING ANALYSIS: {strategy.upper()} STRATEGY")
        print(f"{'='*80}")
        print()
        
        # Group by table size and calculate average times
        size_groups = strat_df.groupby('total_rows').agg({
            'RemapStructureBuildMs': 'mean',
            'RemapBuildKeysMs': 'mean',
            'RemapProbeKeysMs': 'mean',
            'TotalRemapMs': 'mean',
            'MedianTimeMs': 'mean'
        }).reset_index()
        
        size_groups = size_groups.sort_values('total_rows')
        
        print(f"Average times by total row count ({strategy}):")
        print(size_groups.to_string(index=False))
        print()
        
        # Calculate scaling factor
        if len(size_groups) > 1:
            first = size_groups.iloc[0]
            last = size_groups.iloc[-1]
            size_ratio = last['total_rows'] / first['total_rows']
            time_ratio = last['TotalRemapMs'] / first['TotalRemapMs']
            
            print(f"Scaling from {first['total_rows']:,} to {last['total_rows']:,} rows ({size_ratio:.1f}x):")
            print(f"  Remapping time increased by {time_ratio:.1f}x")
            print(f"  Scaling efficiency: {(time_ratio/size_ratio)*100:.1f}% (100% = linear)")
            print()
        print()

def analyze_cardinality_impact(df):
    """Analyze how cardinality affects remapping performance."""
    print("="*80)
    print("CARDINALITY IMPACT ANALYSIS")
    print("="*80)
    print()
    
    remap_df = df[df['has_remap'] & df['RemapStructureBuildMs'].notna()].copy()
    
    if len(remap_df) == 0:
        print("No detailed timing data for cardinality analysis")
        return
    
    # Analyze each strategy separately
    for strategy in sorted(remap_df['strategy'].unique()):
        strat_df = remap_df[remap_df['strategy'] == strategy]
        
        print(f"{'='*80}")
        print(f"CARDINALITY IMPACT: {strategy.upper()} STRATEGY")
        print(f"{'='*80}")
        print()
        
        cardinality_groups = strat_df.groupby('cardinality').agg({
            'RemapStructureBuildMs': ['mean', 'std'],
            'RemapBuildKeysMs': ['mean', 'std'],
            'RemapProbeKeysMs': ['mean', 'std'],
            'TotalRemapMs': ['mean', 'std'],
            'RemapOverheadPct': ['mean', 'std']
        }).round(3)
        
        print(f"Remapping Performance by Cardinality ({strategy}):")
        print(cardinality_groups.to_string())
        print()
        
        # Compare low vs high cardinality
        low_card_df = strat_df[strat_df['cardinality'] <= 0.25]
        high_card_df = strat_df[strat_df['cardinality'] >= 0.75]
        
        if len(low_card_df) > 0 and len(high_card_df) > 0:
            low_card = low_card_df['TotalRemapMs'].mean()
            high_card = high_card_df['TotalRemapMs'].mean()
            
            print(f"Low cardinality (≤25%): {low_card:.3f}ms average remapping time")
            print(f"High cardinality (≥75%): {high_card:.3f}ms average remapping time")
            if low_card > 0:
                print(f"Difference: {((high_card - low_card) / low_card * 100):.1f}% increase")
            print()
        print()

def generate_summary(df, speedup_df):
    """Generate a comprehensive summary."""
    print("="*80)
    print("KEY REMAPPING BENCHMARK SUMMARY")
    print("="*80)
    print()
    
    remap_df = df[df['has_remap'] & df['RemapStructureBuildMs'].notna()]
    
    print("Test Coverage:")
    print(f"  Total tests:              {len(df)}")
    print(f"  With detailed timings:    {len(remap_df)}")
    print(f"  Key types tested:         {df['key_type'].nunique()}")
    print(f"  Distributions tested:     {df['distribution'].nunique()}")
    print(f"  Strategies tested:        {df['strategy'].nunique()}")
    print()
    
    # Summary by strategy
    if len(remap_df) > 0:
        for strategy in sorted(remap_df['strategy'].unique()):
            strat_remap_df = remap_df[remap_df['strategy'] == strategy]
            
            print(f"{strategy.upper()} Strategy - Remapping Overhead:")
            print(f"  Average overhead:         {strat_remap_df['RemapOverheadPct'].mean():.1f}%")
            print(f"  Min overhead:             {strat_remap_df['RemapOverheadPct'].min():.1f}%")
            print(f"  Max overhead:             {strat_remap_df['RemapOverheadPct'].max():.1f}%")
            print()
    
    if speedup_df is not None and len(speedup_df) > 0:
        for strategy in sorted(speedup_df['strategy'].unique()):
            strat_speedup_df = speedup_df[speedup_df['strategy'] == strategy]
            
            print(f"{strategy.upper()} Strategy - Performance Impact:")
            print(f"  Average speedup:          {strat_speedup_df['speedup'].mean():.2f}x")
            print(f"  Median speedup:           {strat_speedup_df['speedup'].median():.2f}x")
            print(f"  Best speedup:             {strat_speedup_df['speedup'].max():.2f}x")
            improved = len(strat_speedup_df[strat_speedup_df['speedup'] > 1.05])
            print(f"  Tests improved (>5%):     {improved} / {len(strat_speedup_df)} ({improved/len(strat_speedup_df)*100:.1f}%)")
            print()
            
            # Recommendations for this strategy
            best_scenarios = strat_speedup_df[strat_speedup_df['speedup'] > 1.2]
            if len(best_scenarios) > 0:
                print(f"  Recommendations for {strategy}:")
                print(f"    Significant benefits in {len(best_scenarios)} tests (>1.2x speedup)")
                
                top_key_types = best_scenarios.groupby('key_type')['speedup'].mean().nlargest(3)
                print(f"    Best performing key types:")
                for kt, spd in top_key_types.items():
                    print(f"      - {kt}: {spd:.2f}x average")
            else:
                print(f"  Recommendations for {strategy}:")
                print(f"    No significant benefits observed (no tests >1.2x speedup)")
            
            print()

def simplify_tree_text(tree_text):
    """
    Simplify decision tree text by collapsing branches where all children lead to same class.
    
    For example, if we have:
    |--- feature <= 0.5
    |   |--- class: 0
    |--- feature > 0.5
    |   |--- class: 0
    
    This simplifies to:
    |--- [All branches] → class: 0
    """
    lines = tree_text.split('\n')
    simplified_lines = []
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Check if this is a split node (has '---' but no 'class:')
        if '---' in line and 'class:' not in line:
            # Get the indentation level
            indent = len(line) - len(line.lstrip())
            
            # Look ahead to see if all children have the same class
            child_classes = set()
            j = i + 1
            max_child_indent = indent + 4
            
            # Collect all immediate child classes
            while j < len(lines):
                child_line = lines[j]
                if not child_line.strip():
                    j += 1
                    continue
                    
                child_indent = len(child_line) - len(child_line.lstrip())
                
                # If we've gone back to same or lower indent, we're done with children
                if child_indent <= indent:
                    break
                
                # Only look at immediate children (one level deeper)
                if child_indent == max_child_indent and 'class:' in child_line:
                    # Extract class number
                    if 'class: 0' in child_line:
                        child_classes.add(0)
                    elif 'class: 1' in child_line:
                        child_classes.add(1)
                
                j += 1
            
            # If all children have same class, simplify
            if len(child_classes) == 1:
                the_class = list(child_classes)[0]
                class_text = "DISABLE remapping" if the_class == 0 else "ENABLE remapping"
                simplified_lines.append(line.rstrip() + f"  → [All branches lead to: {class_text}]")
                # Skip all the children
                i = j
                continue
        
        simplified_lines.append(line)
        i += 1
    
    return '\n'.join(simplified_lines)

def calculate_runtime_metrics(strat_df, y_pred, features_df, strategy):
    """
    Calculate total runtime for different selection strategies.
    
    Args:
        strat_df: DataFrame filtered to one strategy (hash or sort) - not used anymore
        y_pred: Model predictions (1 = enable remap, 0 = disable)
        features_df: DataFrame with features, labels, and runtime data (aligned with y_pred)
        strategy: 'hash' or 'sort'
    """
    print("Runtime Comparison:")
    print("-" * 80)
    
    # features_df now contains WithRemapTimeMs, WithoutRemapTimeMs, and base_test
    # It's already filtered and aligned with the training data
    # We just need to add the predictions
    
    runtime_df = features_df[['base_test', 'WithRemapTimeMs', 'WithoutRemapTimeMs']].copy()
    runtime_df['prediction'] = y_pred
    
    # Rename columns for clarity
    runtime_df = runtime_df.rename(columns={
        'WithRemapTimeMs': 'with_remap_time',
        'WithoutRemapTimeMs': 'without_remap_time'
    })
    
    # Deduplicate by base_test (since each test appears twice in the original data)
    runtime_df = runtime_df.drop_duplicates(subset=['base_test'])
    
    # Calculate total runtime for each scenario
    # Scenario 1: Perfect oracle - always pick the faster time
    perfect_oracle_time = runtime_df.apply(
        lambda row: min(row['with_remap_time'], row['without_remap_time']), 
        axis=1
    ).sum()
    
    # Scenario 2: Always enable remapping
    always_remap_time = runtime_df['with_remap_time'].sum()
    
    # Scenario 3: Always disable remapping
    always_no_remap_time = runtime_df['without_remap_time'].sum()
    
    # Scenario 4: Use model prediction
    model_time = runtime_df.apply(
        lambda row: row['with_remap_time'] if row['prediction'] == 1 else row['without_remap_time'],
        axis=1
    ).sum()
    
    # Print results
    print(f"\nTotal Runtime (sum of all test runtimes in ms):")
    print(f"  1. Perfect Oracle (always fastest):     {perfect_oracle_time:,.1f} ms")
    print(f"  2. Always Remapping ON:                 {always_remap_time:,.1f} ms")
    print(f"  3. Always Remapping OFF:                {always_no_remap_time:,.1f} ms")
    print(f"  4. Model Prediction:                    {model_time:,.1f} ms")
    print()
    
    # Calculate how close each strategy is to perfect
    print(f"Relative Performance (vs Perfect Oracle = 100%):")
    print(f"  1. Perfect Oracle:                      100.0%")
    print(f"  2. Always Remapping ON:                 {(perfect_oracle_time / always_remap_time * 100):.1f}%")
    print(f"  3. Always Remapping OFF:                {(perfect_oracle_time / always_no_remap_time * 100):.1f}%")
    print(f"  4. Model Prediction:                    {(perfect_oracle_time / model_time * 100):.1f}%")
    print()
    
    # Calculate speedup vs worst baseline
    worst_baseline = max(always_remap_time, always_no_remap_time)
    best_simple_baseline = min(always_remap_time, always_no_remap_time)
    
    print(f"Speedup Metrics:")
    print(f"  Model vs Worst Baseline:                {(worst_baseline / model_time):.3f}x")
    print(f"  Model vs Best Simple Baseline:          {(best_simple_baseline / model_time):.3f}x")
    print(f"  Model vs Perfect Oracle:                {(perfect_oracle_time / model_time):.3f}x")
    print()
    
    # Calculate time saved/lost compared to best simple baseline
    time_saved = best_simple_baseline - model_time
    print(f"Time Difference vs Best Simple Baseline:")
    if time_saved > 0:
        print(f"  Model SAVES:                            {time_saved:,.1f} ms ({(time_saved/best_simple_baseline*100):.1f}%)")
    else:
        print(f"  Model COSTS:                            {-time_saved:,.1f} ms ({(-time_saved/best_simple_baseline*100):.1f}%)")
    print()
    
    # Show how many decisions the model got right vs simple strategies
    always_remap_correct = (runtime_df['with_remap_time'] <= runtime_df['without_remap_time']).sum()
    always_no_remap_correct = (runtime_df['without_remap_time'] <= runtime_df['with_remap_time']).sum()
    model_correct = ((runtime_df['prediction'] == 1) & (runtime_df['with_remap_time'] <= runtime_df['without_remap_time']) |
                     (runtime_df['prediction'] == 0) & (runtime_df['without_remap_time'] <= runtime_df['with_remap_time'])).sum()
    
    print(f"Decision Correctness (choosing the faster option):")
    print(f"  Always Remapping ON:                    {always_remap_correct}/{len(runtime_df)} ({always_remap_correct/len(runtime_df)*100:.1f}%)")
    print(f"  Always Remapping OFF:                   {always_no_remap_correct}/{len(runtime_df)} ({always_no_remap_correct/len(runtime_df)*100:.1f}%)")
    print(f"  Model Prediction:                       {model_correct}/{len(runtime_df)} ({model_correct/len(runtime_df)*100:.1f}%)")
    print()

def train_remapping_decision_model(df):
    """
    Train decision tree models to predict when key remapping should be enabled.
    Separate models are trained for hash and sort strategies.
    """
    print("="*80)
    print("DECISION TREE MODEL: WHEN TO ENABLE KEY REMAPPING")
    print("="*80)
    print()
    
    # We need paired data (with and without remapping) to determine which is better
    # Filter to only tests where we have both variants
    df['base_test'] = df['TestName'].apply(lambda x: x.replace('_swap_remap', '_swap').replace('_swap', ''))
    test_counts = df.groupby(['base_test', 'strategy']).size()
    paired_tests = test_counts[test_counts == 2].index
    
    if len(paired_tests) == 0:
        print("No paired data (tests with both remapping on/off) found.")
        print("Cannot train decision model.")
        print()
        return
    
    print(f"Found {len(paired_tests)} paired test configurations")
    print()
    
    # Train separate models for each strategy
    for strategy in ['hash', 'sort']:
        print(f"{'='*80}")
        print(f"STRATEGY: {strategy.upper()}")
        print(f"{'='*80}")
        print()
        
        strat_df = df[df['strategy'] == strategy].copy()
        
        if len(strat_df) < 20:
            print(f"  Insufficient data for {strategy} (need >= 20 samples, have {len(strat_df)})")
            print()
            continue
        
        # Prepare features
        features_for_tree = pd.DataFrame()
        features_for_tree['base_test'] = strat_df['base_test'].values  # Keep track of which test this is
        features_for_tree['NumKeyColumns'] = strat_df['num_key_columns'].values
        
        # Parse row counts from build_size and probe_size (format: bXXXk, pXXXk)
        features_for_tree['BuildRows'] = strat_df['build_size'].apply(
            lambda x: int(x.replace('b', '').replace('k', '')) * 1000
        ).values
        features_for_tree['ProbeRows'] = strat_df['probe_size'].apply(
            lambda x: int(x.replace('p', '').replace('k', '')) * 1000
        ).values
        features_for_tree['TotalRows'] = features_for_tree['BuildRows'] + features_for_tree['ProbeRows']
        features_for_tree['BuildCardinalityPct'] = strat_df['cardinality'].values
        
        # Map key types to complexity scores
        # Simple types (int, long, decimal) = lower scores
        # String types = higher scores
        # Composite = highest scores
        def key_complexity_score(key_type, num_cols):
            if num_cols == 1:
                if key_type == 'int':
                    return 1
                elif key_type == 'long':
                    return 2
                elif key_type.startswith('decimal'):
                    if '32' in key_type:
                        return 1
                    elif '64' in key_type:
                        return 2
                    else:  # 128
                        return 3
                elif key_type.startswith('string'):
                    return 4
                else:
                    return 2
            elif num_cols == 2:
                return 5  # Composite, all fixed width likely
            else:  # 3+ columns
                return 6  # Composite with variable width likely
        
        features_for_tree['KeyComplexityScore'] = strat_df.apply(
            lambda row: key_complexity_score(row['key_type'], row['num_key_columns']),
            axis=1
        )
        
        # Create binary label: should enable remapping?
        # For each config, compare performance with and without remapping
        labels = []
        with_remap_times = []
        without_remap_times = []
        
        for idx, row in strat_df.iterrows():
            base_test = row['base_test']
            
            # Find matching configs with and without remapping
            matching = strat_df[strat_df['base_test'] == base_test]
            
            with_remap = matching[matching['has_remap'] == True]
            without_remap = matching[matching['has_remap'] == False]
            
            if len(with_remap) > 0 and len(without_remap) > 0:
                with_remap_time = with_remap['MedianTimeMs'].iloc[0]
                without_remap_time = without_remap['MedianTimeMs'].iloc[0]
                
                # Label = 1 if remapping is faster (at least 5% improvement)
                speedup = without_remap_time / with_remap_time
                labels.append(1 if speedup >= 1.05 else 0)
                with_remap_times.append(with_remap_time)
                without_remap_times.append(without_remap_time)
            else:
                labels.append(None)
                with_remap_times.append(None)
                without_remap_times.append(None)
        
        features_for_tree['ShouldRemap'] = labels
        features_for_tree['WithRemapTimeMs'] = with_remap_times
        features_for_tree['WithoutRemapTimeMs'] = without_remap_times
        features_for_tree = features_for_tree.dropna()
        
        if len(features_for_tree) < 10:
            print(f"  Insufficient paired data for {strategy} (need >= 10 pairs, have {len(features_for_tree)})")
            print()
            continue
        
        # Extract features (drop labels and metadata)
        X = features_for_tree.drop(['ShouldRemap', 'WithRemapTimeMs', 'WithoutRemapTimeMs', 'base_test'], axis=1)
        y = features_for_tree['ShouldRemap'].astype(int)
        
        print(f"Training samples: {len(X)}")
        print(f"Features: {list(X.columns)}")
        print(f"Label distribution: Enable remapping={y.sum()}, Disable={len(y)-y.sum()}")
        print()
        
        # Check if we have both classes - need at least some of each to train
        if len(y.unique()) < 2:
            print(f"  WARNING: Only one class present in the data!")
            if y.iloc[0] == 0:
                print(f"  All tests show that remapping does NOT improve performance for {strategy}.")
                print(f"  This suggests remapping may not be beneficial for this strategy,")
                print(f"  or the test coverage doesn't include scenarios where it helps.")
            else:
                print(f"  All tests show that remapping ALWAYS improves performance for {strategy}.")
                print(f"  Remapping should likely be enabled for all {strategy} joins.")
            print()
            print(f"  Cannot train a decision tree with only one class.")
            print(f"  Need examples of both 'Enable' and 'Disable' cases to learn decision boundaries.")
            print()
            continue
        
        # Train decision tree with cost-complexity pruning for simpler trees
        # First train a full tree to find optimal pruning parameter
        dt_full = DecisionTreeClassifier(
            max_depth=5, 
            min_samples_split=10, 
            min_samples_leaf=5, 
            random_state=42
        )
        dt_full.fit(X, y)
        
        # Find optimal ccp_alpha through pruning path
        path = dt_full.cost_complexity_pruning_path(X, y)
        ccp_alphas = path.ccp_alphas[:-1]  # Exclude the last (trivial tree)
        
        # Use a moderate alpha to get a simpler tree
        if len(ccp_alphas) > 5:
            # Use alpha at ~60% through the sequence (prune more aggressively)
            optimal_alpha = ccp_alphas[int(len(ccp_alphas) * 0.6)]
        elif len(ccp_alphas) > 0:
            optimal_alpha = ccp_alphas[len(ccp_alphas) // 2]
        else:
            optimal_alpha = 0.0
        
        # Train final pruned tree
        dt = DecisionTreeClassifier(
            max_depth=5, 
            min_samples_split=10, 
            min_samples_leaf=5,
            min_impurity_decrease=0.01,  # Require meaningful splits
            ccp_alpha=optimal_alpha,
            random_state=42
        )
        dt.fit(X, y)
        
        # Evaluate
        y_pred = dt.predict(X)
        accuracy = accuracy_score(y, y_pred)
        
        print(f"Model Performance:")
        print(f"  Accuracy: {accuracy:.1%}")
        print(f"  Tree complexity (ccp_alpha): {optimal_alpha:.6f}")
        print(f"  Tree depth: {dt.get_depth()}, Leaves: {dt.get_n_leaves()}")
        print()
        
        # Print tree rules with enhanced readability
        tree_rules = export_text(dt, feature_names=list(X.columns))
        
        # Replace class labels with descriptive text
        tree_rules = tree_rules.replace('class: 0', 'class: 0 (DISABLE remapping)')
        tree_rules = tree_rules.replace('class: 1', 'class: 1 (ENABLE remapping)')
        
        # Add inline comments for KeyComplexityScore thresholds
        tree_rules = tree_rules.replace('KeyComplexityScore <= 1.50', 
                                         'KeyComplexityScore <= 1.50  [int, decimal_32]')
        tree_rules = tree_rules.replace('KeyComplexityScore <= 2.50', 
                                         'KeyComplexityScore <= 2.50  [long, decimal_64 or simpler]')
        tree_rules = tree_rules.replace('KeyComplexityScore <= 3.50', 
                                         'KeyComplexityScore <= 3.50  [decimal_128 or simpler]')
        tree_rules = tree_rules.replace('KeyComplexityScore <= 4.50', 
                                         'KeyComplexityScore <= 4.50  [string or simpler]')
        tree_rules = tree_rules.replace('KeyComplexityScore <= 5.50', 
                                         'KeyComplexityScore <= 5.50  [2-column composite or simpler]')
        
        tree_rules = tree_rules.replace('KeyComplexityScore >  1.50', 
                                         'KeyComplexityScore >  1.50  [> int/decimal_32]')
        tree_rules = tree_rules.replace('KeyComplexityScore >  2.50', 
                                         'KeyComplexityScore >  2.50  [> long/decimal_64]')
        tree_rules = tree_rules.replace('KeyComplexityScore >  3.50', 
                                         'KeyComplexityScore >  3.50  [> decimal_128 (i.e., string/composite)]')
        tree_rules = tree_rules.replace('KeyComplexityScore >  4.50', 
                                         'KeyComplexityScore >  4.50  [composite keys]')
        tree_rules = tree_rules.replace('KeyComplexityScore >  5.50', 
                                         'KeyComplexityScore >  5.50  [3+ column composite]')
        
        # Add inline comment for NumKeyColumns
        tree_rules = tree_rules.replace('NumKeyColumns <= 1.50', 
                                         'NumKeyColumns <= 1.50  [single column]')
        tree_rules = tree_rules.replace('NumKeyColumns >  1.50', 
                                         'NumKeyColumns >  1.50  [composite: 2+ columns]')
        
        # Simplify by collapsing redundant branches
        tree_rules = simplify_tree_text(tree_rules)
        
        print("Decision Tree Rules:")
        print("-" * 80)
        print(tree_rules)
        print()
        
        # Print classification report
        print("Classification Report:")
        print("-" * 80)
        print(classification_report(y, y_pred, 
                                   target_names=['Disable Remapping', 'Enable Remapping'],
                                   digits=3))
        print()
        
        # Calculate runtime metrics
        calculate_runtime_metrics(strat_df, y_pred, features_for_tree, strategy)
        print()

def main():
    if len(sys.argv) < 2:
        print("Usage: python key_remapping_analysis.py <benchmark_tsv_file>")
        sys.exit(1)
    
    tsv_path = sys.argv[1]
    
    # Load data
    df = load_data(tsv_path)
    
    # Add computed columns for remapping
    if 'RemapStructureBuildMs' in df.columns:
        remap_mask = df['RemapStructureBuildMs'].notna()
        df.loc[remap_mask, 'TotalRemapMs'] = (
            df.loc[remap_mask, 'RemapStructureBuildMs'] +
            df.loc[remap_mask, 'RemapBuildKeysMs'] +
            df.loc[remap_mask, 'RemapProbeKeysMs']
        )
        df.loc[remap_mask, 'RemapOverheadPct'] = (
            df.loc[remap_mask, 'TotalRemapMs'] / df.loc[remap_mask, 'MedianTimeMs'] * 100
        )
    
    # Run analyses
    analyze_timing_breakdown(df)
    speedup_df = analyze_performance_impact(df)
    analyze_scaling(df)
    analyze_cardinality_impact(df)
    generate_summary(df, speedup_df)
    train_remapping_decision_model(df)
    
    print("="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()

