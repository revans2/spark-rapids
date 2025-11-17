#!/usr/bin/env python3
"""
Diagnostic script to analyze why string and multi-key columns don't have high max key counts.

This script analyzes the benchmark data to test hypotheses about what's causing
low max key counts for strings and multi-key columns.

Usage:
    python analyze_key_count_by_type.py [/path/to/benchmark_results.tsv]
"""

import sys
import os
import pandas as pd
import numpy as np
from pathlib import Path

# Default paths
DEFAULT_TSV_PATH = "/data/tmp/simple_hash_vs_sort/benchmark_results.tsv"

def load_data(tsv_path):
    """Load benchmark data."""
    print(f"Loading data from: {tsv_path}")
    df = pd.read_csv(tsv_path, sep='\t')
    
    # Filter to successful runs
    if 'Status' in df.columns:
        df = df[df['Status'] == 'SUCCESS'].copy()
    
    # Filter to hash/sort strategies
    if 'JoinStrategy' in df.columns:
        df = df[df['JoinStrategy'].isin(['hash_object', 'sort_object_post'])].copy()
    
    # Convert numeric columns
    numeric_cols = ['BuildMaxKeyCount', 'LeftRows', 'RightRows', 'LeftDistinctKeys', 
                   'RightDistinctKeys', 'LeftCardinalityPct', 'RightCardinalityPct',
                   'NumKeyColumns', 'LeftAvgKeyBytes', 'RightAvgKeyBytes']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def categorize_key_type(key_type_str):
    """Categorize key type, handling commas in parentheses."""
    if pd.isna(key_type_str) or key_type_str == '':
        return 'unknown'
    
    key_type_str = str(key_type_str)
    
    # Check if it's a composite key
    if ',' in key_type_str:
        # Check if comma is inside parentheses
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

def determine_build_side(df):
    """Determine build side metrics."""
    df = df.copy()
    
    # Determine which side is build (smaller side)
    df['IsLeftBuild'] = df['LeftRows'] <= df['RightRows']
    
    # Build side metrics
    df['BuildRows'] = df.apply(
        lambda row: row['LeftRows'] if row['IsLeftBuild'] else row['RightRows'], axis=1
    )
    df['BuildDistinctKeys'] = df.apply(
        lambda row: row['LeftDistinctKeys'] if row['IsLeftBuild'] else row['RightDistinctKeys'], axis=1
    )
    df['BuildCardinalityPct'] = df.apply(
        lambda row: row['LeftCardinalityPct'] if row['IsLeftBuild'] else row['RightCardinalityPct'], axis=1
    )
    df['BuildKeyType'] = df.apply(
        lambda row: row['LeftKeyType'] if row['IsLeftBuild'] else row['RightKeyType'], axis=1
    )
    df['BuildAvgKeyBytes'] = df.apply(
        lambda row: row['LeftAvgKeyBytes'] if row['IsLeftBuild'] else row['RightAvgKeyBytes'], axis=1
    )
    
    # BuildMaxKeyCount (already in dataframe)
    if 'BuildMaxKeyCount' not in df.columns:
        print("WARNING: BuildMaxKeyCount not found")
        df['BuildMaxKeyCount'] = np.nan
    
    # Categorize key types
    df['KeyTypeCategory'] = df['BuildKeyType'].apply(categorize_key_type)
    
    # Calculate derived metrics
    df['RowsPerDistinctKey'] = df['BuildRows'] / df['BuildDistinctKeys'].replace(0, np.nan)
    df['MaxKeyCountRatio'] = df['BuildMaxKeyCount'] / df['RowsPerDistinctKey'].replace(0, np.nan)
    
    return df

def analyze_hypotheses(df):
    """Analyze data to test hypotheses.
    
    Returns:
        tuple: (analysis_df, comp_df, above_floor_df) for further analysis
    """
    print("\n" + "="*80)
    print("HYPOTHESIS TESTING: Why Low Max Key Counts for Strings/Multi-Keys?")
    print("="*80)
    print()
    
    # Filter to valid data
    analysis_df = df[
        df['BuildMaxKeyCount'].notna() & 
        df['BuildRows'].notna() & 
        df['BuildDistinctKeys'].notna() &
        (df['BuildDistinctKeys'] > 0)
    ].copy()
    
    print(f"Analyzing {len(analysis_df)} valid data points")
    print()
    
    # Hypothesis 1: Fewer rows due to memory constraints
    print("="*80)
    print("HYPOTHESIS 1: Memory constraints → Fewer rows for strings/multi-keys")
    print("="*80)
    print()
    print("If strings/multi-keys have larger bytesPerRow, they get fewer rows")
    print("for the same memory target, which could limit max key count.")
    print()
    
    for category in ['single_fixed', 'single_string', 'multiple']:
        cat_df = analysis_df[analysis_df['KeyTypeCategory'] == category]
        if len(cat_df) == 0:
            print(f"{category}: No data")
            continue
        
        print(f"{category.upper()}:")
        print(f"  Count: {len(cat_df)}")
        print(f"  BuildRows - Mean: {cat_df['BuildRows'].mean():,.0f}, Median: {cat_df['BuildRows'].median():,.0f}")
        print(f"  BuildRows - Min: {cat_df['BuildRows'].min():,.0f}, Max: {cat_df['BuildRows'].max():,.0f}")
        print(f"  BuildAvgKeyBytes - Mean: {cat_df['BuildAvgKeyBytes'].mean():.1f}, Median: {cat_df['BuildAvgKeyBytes'].median():.1f}")
        print()
    
    # Compare row counts
    fixed_df = analysis_df[analysis_df['KeyTypeCategory'] == 'single_fixed']
    string_df = analysis_df[analysis_df['KeyTypeCategory'] == 'single_string']
    multi_df = analysis_df[analysis_df['KeyTypeCategory'] == 'multiple']
    
    if len(fixed_df) > 0 and len(string_df) > 0:
        row_ratio = string_df['BuildRows'].median() / fixed_df['BuildRows'].median()
        print(f"  String rows vs Fixed rows (median): {row_ratio:.2f}x")
        print(f"  → Strings have {1/row_ratio:.2f}x FEWER rows on average")
        print()
    
    # Hypothesis 2: Higher cardinality for strings/multi-keys
    print("="*80)
    print("HYPOTHESIS 2: Higher cardinality → More distinct keys → Lower max count")
    print("="*80)
    print()
    print("If strings/multi-keys have higher cardinality percentages, they have")
    print("more distinct keys relative to rows, reducing max key count.")
    print()
    
    for category in ['single_fixed', 'single_string', 'multiple']:
        cat_df = analysis_df[analysis_df['KeyTypeCategory'] == category]
        if len(cat_df) == 0:
            continue
        
        print(f"{category.upper()}:")
        print(f"  BuildCardinalityPct - Mean: {cat_df['BuildCardinalityPct'].mean()*100:.2f}%, Median: {cat_df['BuildCardinalityPct'].median()*100:.2f}%")
        print(f"  BuildDistinctKeys - Mean: {cat_df['BuildDistinctKeys'].mean():,.0f}, Median: {cat_df['BuildDistinctKeys'].median():,.0f}")
        print(f"  RowsPerDistinctKey - Mean: {cat_df['RowsPerDistinctKey'].mean():.1f}, Median: {cat_df['RowsPerDistinctKey'].median():.1f}")
        print()
    
    # Hypothesis 3: Distribution differences
    print("="*80)
    print("HYPOTHESIS 3: Different distributions → Different max key counts")
    print("="*80)
    print()
    print("Skewed distributions (Zipf) create higher max key counts.")
    print("Uniform distributions create lower max key counts.")
    print()
    
    if 'LeftDistribution' in analysis_df.columns:
        # Determine build distribution
        analysis_df['BuildDistribution'] = analysis_df.apply(
            lambda row: row['LeftDistribution'] if row['IsLeftBuild'] else row['RightDistribution'], axis=1
        )
        
        for category in ['single_fixed', 'single_string', 'multiple']:
            cat_df = analysis_df[analysis_df['KeyTypeCategory'] == category]
            if len(cat_df) == 0:
                continue
            
            print(f"{category.upper()}:")
            dist_counts = cat_df['BuildDistribution'].value_counts()
            for dist, count in dist_counts.items():
                pct = count / len(cat_df) * 100
                print(f"  {dist}: {count} ({pct:.1f}%)")
            
            # Check max key count by distribution
            for dist in cat_df['BuildDistribution'].unique():
                dist_df = cat_df[cat_df['BuildDistribution'] == dist]
                if len(dist_df) > 0:
                    print(f"    {dist} - MaxKeyCount: Mean={dist_df['BuildMaxKeyCount'].mean():,.0f}, Median={dist_df['BuildMaxKeyCount'].median():,.0f}")
            print()
    
    # Hypothesis 4: Max key count vs rows/distinctKeys ratio
    print("="*80)
    print("HYPOTHESIS 4: Max Key Count vs Rows/DistinctKeys Ratio")
    print("="*80)
    print()
    print("Max key count should correlate with rows/distinctKeys ratio.")
    print("If strings/multi-keys have lower ratios, they'll have lower max counts.")
    print()
    
    for category in ['single_fixed', 'single_string', 'multiple']:
        cat_df = analysis_df[analysis_df['KeyTypeCategory'] == category]
        if len(cat_df) == 0:
            continue
        
        print(f"{category.upper()}:")
        print(f"  MaxKeyCount - Mean: {cat_df['BuildMaxKeyCount'].mean():,.0f}, Median: {cat_df['BuildMaxKeyCount'].median():,.0f}")
        print(f"  MaxKeyCount - Min: {cat_df['BuildMaxKeyCount'].min():,.0f}, Max: {cat_df['BuildMaxKeyCount'].max():,.0f}")
        print(f"  RowsPerDistinctKey - Mean: {cat_df['RowsPerDistinctKey'].mean():.1f}, Median: {cat_df['RowsPerDistinctKey'].median():.1f}")
        
        # Correlation
        corr = cat_df['BuildMaxKeyCount'].corr(cat_df['RowsPerDistinctKey'])
        print(f"  Correlation (MaxKeyCount vs RowsPerDistinctKey): {corr:.3f}")
        
        # MaxKeyCountRatio (how many times larger than uniform expectation)
        print(f"  MaxKeyCountRatio (vs uniform) - Mean: {cat_df['MaxKeyCountRatio'].mean():.2f}, Median: {cat_df['MaxKeyCountRatio'].median():.2f}")
        print()
    
    # Summary comparison
    print("="*80)
    print("SUMMARY COMPARISON")
    print("="*80)
    print()
    
    comparison_data = []
    for category in ['single_fixed', 'single_string', 'multiple']:
        cat_df = analysis_df[analysis_df['KeyTypeCategory'] == category]
        if len(cat_df) == 0:
            continue
        
        comparison_data.append({
            'Category': category,
            'Count': len(cat_df),
            'MedianRows': cat_df['BuildRows'].median(),
            'MedianDistinctKeys': cat_df['BuildDistinctKeys'].median(),
            'MedianCardinalityPct': cat_df['BuildCardinalityPct'].median() * 100,
            'MedianRowsPerDistinct': cat_df['RowsPerDistinctKey'].median(),
            'MedianMaxKeyCount': cat_df['BuildMaxKeyCount'].median(),
            'MaxMaxKeyCount': cat_df['BuildMaxKeyCount'].max(),
            'MedianAvgKeyBytes': cat_df['BuildAvgKeyBytes'].median()
        })
    
    comp_df = pd.DataFrame(comparison_data)
    print(comp_df.to_string(index=False))
    print()
    
    # CRITICAL ANALYSIS: Tests above the 10k floor
    print("="*80)
    print("CRITICAL ANALYSIS: Tests Above 10k Row Floor")
    print("="*80)
    print()
    print("Many tests hit 10k rows (likely a minimum constraint).")
    print("Let's analyze only tests with >10k rows to see memory effects:")
    print()
    
    above_floor_df = analysis_df[analysis_df['BuildRows'] > 10000].copy()
    print(f"Tests with >10k rows: {len(above_floor_df)} ({len(above_floor_df)/len(analysis_df)*100:.1f}%)")
    print()
    
    if len(above_floor_df) > 0:
        for category in ['single_fixed', 'single_string', 'multiple']:
            cat_df = above_floor_df[above_floor_df['KeyTypeCategory'] == category]
            if len(cat_df) == 0:
                print(f"{category.upper()}: No tests above 10k floor")
                continue
            
            print(f"{category.upper()} (>10k rows only):")
            print(f"  Count: {len(cat_df)}")
            print(f"  BuildRows - Mean: {cat_df['BuildRows'].mean():,.0f}, Median: {cat_df['BuildRows'].median():,.0f}")
            print(f"  BuildRows - Min: {cat_df['BuildRows'].min():,.0f}, Max: {cat_df['BuildRows'].max():,.0f}")
            print(f"  BuildDistinctKeys - Mean: {cat_df['BuildDistinctKeys'].mean():,.0f}, Median: {cat_df['BuildDistinctKeys'].median():,.0f}")
            print(f"  RowsPerDistinctKey - Mean: {cat_df['RowsPerDistinctKey'].mean():.1f}, Median: {cat_df['RowsPerDistinctKey'].median():.1f}")
            print(f"  MaxKeyCount - Mean: {cat_df['BuildMaxKeyCount'].mean():,.0f}, Median: {cat_df['BuildMaxKeyCount'].median():,.0f}")
            print(f"  MaxKeyCount - Max: {cat_df['BuildMaxKeyCount'].max():,.0f}")
            print()
        
        # Compare ratios
        fixed_above = above_floor_df[above_floor_df['KeyTypeCategory'] == 'single_fixed']
        string_above = above_floor_df[above_floor_df['KeyTypeCategory'] == 'single_string']
        multi_above = above_floor_df[above_floor_df['KeyTypeCategory'] == 'multiple']
        
        if len(fixed_above) > 0 and len(string_above) > 0:
            print("COMPARISON (above 10k floor only):")
            print(f"  Row ratio (fixed/string): {fixed_above['BuildRows'].median() / string_above['BuildRows'].median():.2f}x")
            print(f"  MaxKeyCount ratio (fixed/string): {fixed_above['BuildMaxKeyCount'].median() / string_above['BuildMaxKeyCount'].median():.2f}x")
            print(f"  RowsPerDistinct ratio (fixed/string): {fixed_above['RowsPerDistinctKey'].median() / string_above['RowsPerDistinctKey'].median():.2f}x")
            print()
    
    # Analyze why multi-keys have such low max counts
    print("="*80)
    print("MULTI-KEY DEEP DIVE")
    print("="*80)
    print()
    multi_df = analysis_df[analysis_df['KeyTypeCategory'] == 'multiple']
    if len(multi_df) > 0:
        print(f"Multi-key tests: {len(multi_df)}")
        print(f"  Median MaxKeyCount: {multi_df['BuildMaxKeyCount'].median():.0f}")
        print(f"  Mean MaxKeyCount: {multi_df['BuildMaxKeyCount'].mean():,.0f}")
        print(f"  Max MaxKeyCount: {multi_df['BuildMaxKeyCount'].max():,.0f}")
        print()
        print("Why so low? Let's check:")
        print(f"  Median RowsPerDistinctKey: {multi_df['RowsPerDistinctKey'].median():.1f}")
        print(f"  Median DistinctKeys: {multi_df['BuildDistinctKeys'].median():,.0f}")
        print(f"  Median Rows: {multi_df['BuildRows'].median():,.0f}")
        print()
        print("For multi-keys, the distinct key space is HUGE:")
        print("  - 2 keys: distinct combinations = distinct1 × distinct2")
        print("  - 3 keys: distinct combinations = distinct1 × distinct2 × distinct3")
        print("  → Even with same cardinality %, multi-keys have WAY more distinct combinations")
        print("  → Fewer duplicates per combination → Lower max key count")
        print()
        
        # Check if multi-keys have more distinct keys than expected
        if 'NumKeyColumns' in multi_df.columns:
            print("Multi-key column counts:")
            col_counts = multi_df['NumKeyColumns'].value_counts().sort_index()
            for cols, count in col_counts.items():
                pct = count / len(multi_df) * 100
                subset = multi_df[multi_df['NumKeyColumns'] == cols]
                print(f"  {cols} columns: {count} ({pct:.1f}%) - Median MaxKeyCount: {subset['BuildMaxKeyCount'].median():.0f}")
            print()
    
    # Key insights
    print("="*80)
    print("KEY INSIGHTS")
    print("="*80)
    print()
    
    if len(comp_df) >= 2:
        fixed_row = comp_df[comp_df['Category'] == 'single_fixed']
        string_row = comp_df[comp_df['Category'] == 'single_string']
        
        if len(fixed_row) > 0 and len(string_row) > 0:
            fixed_rows = fixed_row.iloc[0]['MedianRows']
            string_rows = string_row.iloc[0]['MedianRows']
            fixed_max = fixed_row.iloc[0]['MedianMaxKeyCount']
            string_max = string_row.iloc[0]['MedianMaxKeyCount']
            
            print(f"1. Row Count Comparison:")
            print(f"   Fixed-width: {fixed_rows:,.0f} rows (median)")
            print(f"   String:      {string_rows:,.0f} rows (median)")
            print(f"   Ratio:       {fixed_rows/string_rows:.2f}x more rows for fixed-width")
            print()
            
            print(f"2. Max Key Count Comparison:")
            print(f"   Fixed-width: {fixed_max:,.0f} (median)")
            print(f"   String:      {string_max:,.0f} (median)")
            print(f"   Ratio:       {fixed_max/string_max:.2f}x higher max count for fixed-width")
            print()
            
            fixed_rpd = fixed_row.iloc[0]['MedianRowsPerDistinct']
            string_rpd = string_row.iloc[0]['MedianRowsPerDistinct']
            
            print(f"3. Rows Per Distinct Key:")
            print(f"   Fixed-width: {fixed_rpd:.1f} rows/distinct key")
            print(f"   String:      {string_rpd:.1f} rows/distinct key")
            print(f"   → This directly affects max key count!")
            print()
            
            # Calculate expected max key count if same rows/distinct ratio
            if string_rpd > 0:
                expected_string_max = (fixed_max / fixed_rpd) * string_rpd
                print(f"4. Expected Max Key Count for Strings (if same distribution):")
                print(f"   Actual:   {string_max:,.0f}")
                print(f"   Expected: {expected_string_max:,.0f}")
                print(f"   → {'LOWER than expected' if string_max < expected_string_max else 'HIGHER than expected'}")
                print()
    
    # Return dataframes for further analysis
    return analysis_df, comp_df, above_floor_df

def main():
    """Main execution."""
    if len(sys.argv) > 1:
        tsv_path = sys.argv[1]
    else:
        tsv_path = DEFAULT_TSV_PATH
    
    print("="*80)
    print("KEY COUNT ANALYSIS BY KEY TYPE")
    print("="*80)
    print()
    
    df = load_data(tsv_path)
    print(f"Loaded {len(df)} rows")
    print()
    
    df = determine_build_side(df)
    
    analysis_df, comp_df, above_floor_df = analyze_hypotheses(df)
    
    # Root cause analysis
    print("="*80)
    print("ROOT CAUSE ANALYSIS")
    print("="*80)
    print()
    print("Based on the data, here are the likely root causes:")
    print()
    
    # Count tests at 10k floor
    at_floor = len(analysis_df[analysis_df['BuildRows'] == 10000])
    pct_at_floor = at_floor / len(analysis_df) * 100
    
    print(f"1. MINIMUM ROW CONSTRAINT:")
    print(f"   {at_floor} tests ({pct_at_floor:.1f}%) are at exactly 10,000 rows")
    print(f"   This suggests many tests hit a minimum constraint, masking memory effects.")
    print(f"   → For strings/multi-keys: Even when memory allows more rows, many tests")
    print(f"     are constrained to 10k, preventing high max key counts.")
    print()
    
    # Distinct keys comparison
    fixed_distinct = comp_df[comp_df['Category'] == 'single_fixed']
    string_distinct = comp_df[comp_df['Category'] == 'single_string']
    multi_distinct = comp_df[comp_df['Category'] == 'multiple']
    
    if len(fixed_distinct) > 0 and len(string_distinct) > 0:
        print(f"2. DISTINCT KEY SPACE EXPLOSION:")
        print(f"   Fixed-width: {fixed_distinct.iloc[0]['MedianDistinctKeys']:,.0f} distinct keys (median)")
        print(f"   String:      {string_distinct.iloc[0]['MedianDistinctKeys']:,.0f} distinct keys (median)")
        if len(multi_distinct) > 0:
            print(f"   Multiple:    {multi_distinct.iloc[0]['MedianDistinctKeys']:,.0f} distinct keys (median)")
        print(f"   → Strings have {string_distinct.iloc[0]['MedianDistinctKeys'] / fixed_distinct.iloc[0]['MedianDistinctKeys']:.1f}x MORE distinct keys")
        print(f"   → With same rows, fewer duplicates per key → lower max key count")
        print()
    
    if len(multi_distinct) > 0:
        print(f"3. MULTI-KEY COMPOSITE EXPLOSION:")
        print(f"   Multi-keys create a CARTESIAN PRODUCT of distinct key spaces:")
        print(f"   - 2 columns: distinct combinations = distinct_col1 × distinct_col2")
        print(f"   - 3 columns: distinct combinations = distinct_col1 × distinct_col2 × distinct_col3")
        print(f"   → Even with same cardinality %, multi-keys have EXPONENTIALLY more distinct combinations")
        print(f"   → Median MaxKeyCount of {multi_distinct.iloc[0]['MedianMaxKeyCount']:.0f} is extremely low")
        print(f"   → This is EXPECTED behavior, not a bug!")
        print()
    
    print("4. MEMORY CONSTRAINT EFFECTS (when above 10k floor):")
    if len(above_floor_df) > 0:
        fixed_above = above_floor_df[above_floor_df['KeyTypeCategory'] == 'single_fixed']
        string_above = above_floor_df[above_floor_df['KeyTypeCategory'] == 'single_string']
        
        if len(fixed_above) > 0 and len(string_above) > 0:
            row_ratio = fixed_above['BuildRows'].median() / string_above['BuildRows'].median()
            print(f"   For tests >10k rows:")
            print(f"   - Fixed-width gets {row_ratio:.1f}x more rows than strings")
            print(f"   - This amplifies the max key count difference")
            print(f"   → Memory constraints DO matter, but many tests hit the floor first")
    else:
        print("   No tests above 10k floor to analyze")
    print()
    
    print("="*80)
    print("RECOMMENDATIONS")
    print("="*80)
    print()
    print("To get high max key counts for strings/multi-keys:")
    print()
    print("1. REMOVE OR INCREASE MINIMUM ROW CONSTRAINT:")
    print("   - Current: Many tests hit 10k row minimum")
    print("   - Fix: Ensure minMemoryTarget is respected, not a hardcoded 10k")
    print("   - For strings (60 bytes/row): minMemoryTarget (1MB) = ~17k rows minimum")
    print("   - For multi-keys: Even higher minimum needed")
    print()
    print("2. GENERATE MORE TESTS WITH HIGHER MEMORY TARGETS:")
    print("   - Current maxMemoryTarget: 1GB")
    print("   - For strings: 1GB = ~17M rows max")
    print("   - For multi-keys: Even fewer rows")
    print("   - Consider: Increase maxMemoryTarget OR add targeted high-memory tests")
    print()
    print("3. USE LOWER CARDINALITY FOR STRINGS/MULTI-KEYS:")
    print("   - Current: Similar cardinality % across all key types")
    print("   - Fix: Weight strings/multi-keys toward LOWER cardinality")
    print("   - This reduces distinct key space explosion")
    print()
    print("4. ACCEPT MULTI-KEY LIMITATIONS:")
    print("   - Multi-keys inherently have huge distinct key spaces")
    print("   - Low max key counts are EXPECTED for multi-keys")
    print("   - Focus on single-key string tests for high max key counts")
    print()
    
    print("="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()

