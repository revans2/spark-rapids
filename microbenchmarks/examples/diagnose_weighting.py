#!/usr/bin/env python3
"""
Quick diagnostic script to understand your data distribution
and help choose the right weighting strategy.

Usage: python diagnose_weighting.py /path/to/benchmark_results.tsv
"""

import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def diagnose_data(tsv_path):
    """Analyze the data to understand time distributions."""
    
    print("="*80)
    print("DATA DISTRIBUTION DIAGNOSTIC")
    print("="*80)
    print()
    
    # Load data
    df = pd.read_csv(tsv_path, sep='\t')
    df = df[df['Status'] == 'SUCCESS']
    df = df[df['JoinStrategy'].isin(['hash_object', 'sort_object_post'])]
    
    print(f"Total successful runs: {len(df)}")
    print()
    
    # Group by config to get best strategy
    config_cols = ['LeftRows', 'LeftDistinctKeys', 'LeftKeyType',
                   'RightRows', 'RightDistinctKeys', 'RightKeyType',
                   'NumKeyColumns']
    
    best_configs = []
    for config, group in df.groupby(config_cols, dropna=False):
        hash_time = group[group['JoinStrategy'] == 'hash_object']['MedianTimeMs'].min()
        sort_time = group[group['JoinStrategy'] == 'sort_object_post']['MedianTimeMs'].min()
        
        if pd.isna(hash_time) or pd.isna(sort_time):
            continue
        
        best_strategy = 'sort_object_post' if sort_time < hash_time else 'hash_object'
        best_time = min(sort_time, hash_time)
        alt_time = max(sort_time, hash_time)
        
        # Calculate percentage difference (handle division by zero)
        if alt_time > 0:
            time_diff_pct = ((alt_time - best_time) / alt_time) * 100
        else:
            time_diff_pct = 0.0
        
        best_configs.append({
            'best_strategy': best_strategy,
            'best_time': best_time,
            'alt_time': alt_time,
            'time_diff_ms': alt_time - best_time,
            'time_diff_pct': time_diff_pct
        })
    
    best_df = pd.DataFrame(best_configs)
    print(f"Unique configs with both strategies: {len(best_df)}")
    print()
    
    # Analyze time distributions
    print("="*80)
    print("ABSOLUTE TIME ANALYSIS")
    print("="*80)
    print()
    
    print("Query times (milliseconds):")
    print(f"  Min:    {best_df['best_time'].min():.2f} ms")
    print(f"  Max:    {best_df['best_time'].max():.2f} ms")
    print(f"  Mean:   {best_df['best_time'].mean():.2f} ms")
    print(f"  Median: {best_df['best_time'].median():.2f} ms")
    print()
    
    print("Absolute time differences (ms):")
    print(f"  Min:    {best_df['time_diff_ms'].min():.2f} ms")
    print(f"  Max:    {best_df['time_diff_ms'].max():.2f} ms")
    print(f"  Mean:   {best_df['time_diff_ms'].mean():.2f} ms")
    print(f"  Median: {best_df['time_diff_ms'].median():.2f} ms")
    print()
    
    # Percentiles
    print("Time difference percentiles:")
    for p in [10, 25, 50, 75, 90, 95, 99]:
        val = np.percentile(best_df['time_diff_ms'], p)
        print(f"  {p:2d}th: {val:>8.2f} ms")
    print()
    
    # Count high-impact cases
    thresholds = [10, 20, 50, 100, 200, 500]
    print("High-impact case counts (by absolute time):")
    for thresh in thresholds:
        count = (best_df['time_diff_ms'] > thresh).sum()
        pct = count / len(best_df) * 100
        print(f"  >{thresh:4d}ms: {count:4d} cases ({pct:5.1f}%)")
    print()
    
    # Percentage analysis
    print("="*80)
    print("PERCENTAGE DIFFERENCE ANALYSIS")
    print("="*80)
    print()
    
    print("Percentage differences:")
    print(f"  Min:    {best_df['time_diff_pct'].min():.2f}%")
    print(f"  Max:    {best_df['time_diff_pct'].max():.2f}%")
    print(f"  Mean:   {best_df['time_diff_pct'].mean():.2f}%")
    print(f"  Median: {best_df['time_diff_pct'].median():.2f}%")
    print()
    
    # Compare weighting strategies
    print("="*80)
    print("WEIGHTING STRATEGY COMPARISON")
    print("="*80)
    print()
    
    # Exponential with different scales
    print("Exponential weighting weight ranges (normalized to mean=1):")
    for scale in [10, 25, 50, 100, 200]:
        weights = np.exp(best_df['time_diff_ms'] / scale)
        weights = weights / weights.mean()
        print(f"  Scale={scale:3d}ms: min={weights.min():.3f}, max={weights.max():.3f}, ratio={weights.max()/weights.min():.1f}x")
    print()
    
    # Percentage-based for comparison
    print("Percentage-based exponential weighting:")
    for scale in [10, 20, 30, 50]:
        weights = np.exp(best_df['time_diff_pct'] / scale)
        weights = weights / weights.mean()
        print(f"  Scale={scale:2d}%: min={weights.min():.3f}, max={weights.max():.3f}, ratio={weights.max()/weights.min():.1f}x")
    print()
    
    # Correlation analysis
    print("="*80)
    print("CORRELATION ANALYSIS")
    print("="*80)
    print()
    
    print("Are absolute time differences correlated with percentages?")
    # Handle NaN in correlation (can happen if one variable has no variance)
    try:
        corr = np.corrcoef(best_df['time_diff_ms'], best_df['time_diff_pct'])[0,1]
        if np.isnan(corr):
            print(f"  Correlation: N/A (insufficient variance in data)")
            corr = 0.0
        else:
            print(f"  Correlation: {corr:.3f}")
    except:
        print(f"  Correlation: N/A (could not compute)")
        corr = 0.0
    
    if corr > 0.8:
        print("  → High correlation: Absolute and percentage weighting will be similar")
    elif corr > 0.5:
        print("  → Moderate correlation: Some difference between strategies")
    else:
        print("  → Low correlation: Absolute and percentage weighting will be very different")
    print()
    
    # Strategy winners
    print("="*80)
    print("STRATEGY ANALYSIS")
    print("="*80)
    print()
    
    hash_wins = (best_df['best_strategy'] == 'hash_object').sum()
    sort_wins = (best_df['best_strategy'] == 'sort_object_post').sum()
    print(f"Hash wins: {hash_wins} ({hash_wins/len(best_df)*100:.1f}%)")
    print(f"Sort wins: {sort_wins} ({sort_wins/len(best_df)*100:.1f}%)")
    print()
    
    if sort_wins < 20:
        print("⚠ WARNING: Very few sort wins! Model will struggle to learn when to use sort.")
        print("  → Collect more low-cardinality data (<5% cardinality)")
        print()
    
    # Recommendations
    print("="*80)
    print("RECOMMENDATIONS")
    print("="*80)
    print()
    
    median_time = best_df['best_time'].median()
    median_diff = best_df['time_diff_ms'].median()
    p75_diff = np.percentile(best_df['time_diff_ms'], 75)
    p95_diff = np.percentile(best_df['time_diff_ms'], 95)
    
    print("Based on your data:")
    print()
    
    if median_time < 50:
        print("1. QUERIES ARE FAST (median < 50ms)")
        print("   → Absolute time weighting might over-emphasize small differences")
        print("   → Consider percentage-based weighting or hybrid approach")
        print()
    elif median_time > 500:
        print("1. QUERIES ARE SLOW (median > 500ms)")
        print("   → Absolute time weighting is appropriate")
        print("   → Use scale=100 or scale=200 for exponential weighting")
        print()
    else:
        print("1. QUERIES ARE MODERATE (50-500ms)")
        print("   → Absolute time weighting should work well")
        print("   → Current scale=50 is reasonable")
        print()
    
    if p95_diff < 50:
        print("2. TIME DIFFERENCES ARE SMALL (95th percentile < 50ms)")
        print("   → Few truly expensive mistakes in your data")
        print("   → Use smaller scale (10-25ms) or percentage weighting")
        print()
    elif p95_diff > 200:
        print("2. TIME DIFFERENCES ARE LARGE (95th percentile > 200ms)")
        print("   → Many expensive mistakes possible")
        print("   → Use larger scale (100-200ms) for exponential weighting")
        print()
    else:
        print("2. TIME DIFFERENCES ARE MODERATE")
        print("   → Current scale=50 should work")
        print()
    
    if sort_wins < 50:
        print("3. IMBALANCED DATA")
        print("   → Need more sort win cases")
        print("   → Collect targeted low-cardinality data")
        print()
    
    # Suggested config
    print("SUGGESTED CONFIGURATION:")
    print()
    
    if median_time < 50 and median_diff < 20:
        print("WEIGHT_STRATEGY = 'exponential'  # or try 'quadratic'")
        print("# Use percentage-based instead:")
        print("# time_diff_pct = best_df['TimeDiffPct'].values")
        print("# weights = np.exp(time_diff_pct / 30.0)")
        scale = 10
    elif median_time > 500:
        scale = max(100, int(p75_diff))
    else:
        scale = max(25, int(median_diff))
    
    print(f"# Or adjust absolute time scale:")
    print(f"scale = {scale}  # in compute_sample_weights(), line ~298")
    print()
    
    high_impact_thresh = min(100, max(20, int(p75_diff)))
    print(f"HIGH_IMPACT_THRESHOLD_MS = {high_impact_thresh}  # line ~54")
    print()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        tsv_path = sys.argv[1]
    else:
        tsv_path = "/data/tmp/simple_hash_vs_sort/benchmark_results.tsv"
    
    diagnose_data(tsv_path)

