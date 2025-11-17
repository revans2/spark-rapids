#!/usr/bin/env python3
"""
Iterative Model Trainer for Hash vs Sort Join Strategy Selection

This script provides an iterative workflow for training decision tree models to
predict when SortObjectPost beats HashObject for Inner joins.

Usage:
    python iterative_model_trainer.py [/path/to/benchmark_results.tsv]
    
    If no path is provided, defaults to:
        /data/tmp/simple_hash_vs_sort/benchmark_results.tsv

Requirements:
    pip install pandas scikit-learn scipy matplotlib seaborn numpy

Workflow:
    1. Load benchmark results (HashObject vs SortObjectPost only)
    2. Train decision tree model with train/test split
    3. Show model performance and confidence metrics
    4. Identify regions where more data is needed
    5. Generate refinement config JSON for scala benchmark to target those regions
    6. User runs scala benchmark again (reads refinement config)
    7. Repeat until model converges with good generalization
"""

import sys
import os
import json
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier, export_text
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from sklearn.model_selection import cross_val_score
import warnings
warnings.filterwarnings('ignore')

# Default paths
DEFAULT_TSV_PATH = "/data/tmp/simple_hash_vs_sort/benchmark_results.tsv"
REFINEMENT_CONFIG_PATH = "/data/tmp/simple_hash_vs_sort/refinement_config.json"

# ============================================================================
# Configuration
# ============================================================================

# Sample Weighting Strategy
# This is KEY to avoiding pathologically bad cases!
# Options:
#   'none'            - No weighting (baseline)
#   'exponential'     - Weight by absolute time (good for mixed workloads)
#   'exponential_pct' - Weight by percentage (good for fast queries)
#   'hybrid'          - Combine both (RECOMMENDED - balances concerns)
#   'linear'          - Gentler weighting by absolute time
#   'quadratic'       - Strong weighting by absolute time
#   'threshold'       - Binary weighting
#
# If results are worse with weighting, try:
#   1. WEIGHT_STRATEGY = 'none' (to see baseline)
#   2. WEIGHT_STRATEGY = 'exponential_pct' (go back to percentage)
#   3. WEIGHT_STRATEGY = 'hybrid' (balance both)
#   4. Run diagnose_weighting.py to understand your data
#
# NOW WITH AUTO-SCALING: Automatically adjusts to your data to prevent overflow
WEIGHT_STRATEGY = 'exponential_pct'  # RECOMMENDED: Start with percentage (most stable)

# High-impact case threshold (in milliseconds)
# Cases with time differences > this are considered "high impact"
HIGH_IMPACT_THRESHOLD_MS = 100.0  # Based on typical median time difference (50-107ms)

# Confidence thresholds
# INCREASED: Higher minimums for better generalization
MIN_SAMPLES_PER_STRATEGY = 100  # Minimum samples needed for each strategy (was 50)
MIN_TEST_ACCURACY = 0.80        # Minimum test accuracy to consider model good (was 0.75)
MAX_OVERFITTING_GAP = 0.10      # Maximum train-test gap before flagging overfitting (was 0.15)
MIN_CROSS_VAL_SCORE = 0.75      # Minimum cross-validation score (was 0.70)

# Test set size
TEST_SIZE = 0.10  # 10% holdout for testing (increases as we get more data)

# ============================================================================
# Data Loading and Preparation
# ============================================================================

def load_and_filter_data(tsv_path):
    """Load TSV and filter to HashObject vs SortObjectPost only."""
    print("="*80)
    print("LOADING AND FILTERING DATA")
    print("="*80)
    print()
    
    if not os.path.exists(tsv_path):
        print(f"ERROR: File not found: {tsv_path}")
        sys.exit(1)
    
    # Expected column names (from Scala benchmark)
    expected_columns = [
        'TestName', 'Status', 'JoinStrategy', 'ActualBuildSide',
        'LeftRows', 'LeftDistinctKeys', 'LeftCardinalityPct', 'LeftKeyType',
        'LeftAvgKeyBytes', 'LeftMemoryMB', 'LeftDistribution',
        'RightRows', 'RightDistinctKeys', 'RightCardinalityPct', 'RightKeyType',
        'RightAvgKeyBytes', 'RightMemoryMB', 'RightDistribution',
        'NumKeyColumns', 'KeyOverlapPct',
        'MedianTimeMs', 'AvgTimeMs', 'StdDevMs', 'OutputRows', 'Iterations', 'ErrorMessage',
        'BuildTimeMs', 'ProbeTimeMs', 'RemapStructureMs', 'RemapBuildKeysMs',
        'RemapProbeKeysMs', 'CreateBuildObjectMs', 'ExecuteJoinMs',
        'BuildMaxKeyCount', 'BuildKeyCountStdDev', 'BuildKeyCountP99', 'BuildKeyCountP95',
        'BuildKeyCountP90', 'BuildKeyCountP75', 'BuildKeyCountP50',
        'ProbeMaxKeyCount', 'ProbeKeyCountStdDev', 'ProbeKeyCountP99', 'ProbeKeyCountP95',
        'ProbeKeyCountP90', 'ProbeKeyCountP75', 'ProbeKeyCountP50'
    ]
    
    # Try reading with headers first to check
    df_test = pd.read_csv(tsv_path, sep='\t', nrows=1)
    
    # Check if the first column looks like a header or data
    first_col = str(df_test.columns[0]).lower()
    
    # Detection logic:
    # - If first column contains 'test_' it's definitely data (no headers)
    # - If first column is exactly one of the expected header names, it has headers
    # - Otherwise, check if it looks like a header name
    if 'test_' in first_col or first_col in ['success', 'hash_object', 'sort_object_post']:
        has_headers = False
    elif first_col in ['testname', 'status', 'joinstategy', 'actualbuildside']:
        has_headers = True
    else:
        # Ambiguous - assume no headers if it looks like data
        has_headers = False
    
    if not has_headers:
        print("Detected TSV file without headers. Reading with expected column names...")
        print()
        
        # Read without headers
        df = pd.read_csv(tsv_path, sep='\t', header=None)
        
        # Assign column names (use expected columns, pad with Unnamed if needed)
        num_cols = len(df.columns)
        if num_cols <= len(expected_columns):
            df.columns = expected_columns[:num_cols]
        else:
            # More columns than expected - use expected names + Unnamed for extras
            df.columns = expected_columns + [f'Unnamed_{i}' for i in range(len(expected_columns), num_cols)]
        
        print(f"Assigned {len(df.columns)} column names")
    else:
        # Read with headers
        df = pd.read_csv(tsv_path, sep='\t')
        print(f"Loaded TSV with headers")
    
    print(f"Loaded {len(df)} benchmark results")
    
    # Show available columns for debugging
    print(f"Available columns ({len(df.columns)}): {', '.join(df.columns.tolist()[:10])}...")
    print()
    
    # Check for Status column (case-insensitive)
    status_col = None
    for col in df.columns:
        if col.lower() == 'status':
            status_col = col
            break
    
    if status_col is None:
        print("WARNING: 'Status' column not found in TSV file.")
        print("         Assuming all rows are successful runs.")
        print("         If you have failed runs, they will be included in the analysis.")
        print()
        success_df = df.copy()
        success_df['Status'] = 'SUCCESS'  # Add Status column for consistency
    else:
        # Filter to successful runs only
        success_df = df[df[status_col] == 'SUCCESS'].copy()
        print(f"Successful runs: {len(success_df)} ({len(success_df)/len(df)*100:.1f}%)")
    
    # Check for JoinStrategy column (case-insensitive with fallback to direct access)
    strategy_col = None
    
    # First try case-insensitive search
    for col in success_df.columns:
        col_lower = col.lower().strip()  # Strip whitespace just in case
        if col_lower == 'joinstategy' or col_lower == 'join_strategy':
            strategy_col = col
            break
    
    # Fallback to direct access (handles pandas Index issues)
    if strategy_col is None:
        if 'JoinStrategy' in success_df.columns:
            strategy_col = 'JoinStrategy'
        elif 'joinStrategy' in success_df.columns:
            strategy_col = 'joinStrategy'
    
    if strategy_col is None:
        print("ERROR: 'JoinStrategy' column not found in TSV file.")
        print(f"       Available columns ({len(success_df.columns)}): {', '.join(success_df.columns.tolist())}")
        print()
        print("DEBUG: Checking column names more carefully...")
        for col in success_df.columns:
            col_lower = col.lower().strip()
            col_repr = repr(col)  # Show exact representation
            print(f"  Column: {col_repr} -> normalized: '{col_lower}' -> matches: {col_lower in ['joinstategy', 'join_strategy']}")
        print()
        print("This usually means:")
        print("  1. The TSV file doesn't have headers (we tried to detect this)")
        print("  2. The column name is different than expected")
        print("  3. The file format is different")
        print()
        print("Please check the TSV file format. Expected columns include:")
        print("  TestName, Status, JoinStrategy, ActualBuildSide, ...")
        sys.exit(1)
    
    # Filter to HashObject and SortObjectPost only
    target_strategies = ['hash_object', 'sort_object_post']
    filtered_df = success_df[success_df[strategy_col].isin(target_strategies)].copy()
    print(f"HashObject/SortObjectPost runs: {len(filtered_df)} ({len(filtered_df)/len(success_df)*100:.1f}%)")
    
    if len(filtered_df) == 0:
        print("ERROR: No HashObject or SortObjectPost results found!")
        sys.exit(1)
    
    # Show distribution
    print()
    print("Strategy Distribution:")
    print(filtered_df[strategy_col].value_counts().to_string())
    print()
    
    # Rename columns to standard names for rest of script
    if status_col and status_col != 'Status':
        filtered_df = filtered_df.rename(columns={status_col: 'Status'})
    elif not status_col:
        filtered_df['Status'] = 'SUCCESS'  # Add Status column for consistency
    
    if strategy_col != 'JoinStrategy':
        filtered_df = filtered_df.rename(columns={strategy_col: 'JoinStrategy'})
    
    # Ensure optional timing columns exist (legacy TSVs may not include these)
    timing_columns = [
        'BuildTimeMs',
        'ProbeTimeMs',
        'RemapStructureMs',
        'RemapBuildKeysMs',
        'RemapProbeKeysMs',
        'CreateBuildObjectMs',
        'ExecuteJoinMs'
    ]
    for col in timing_columns:
        if col not in filtered_df.columns:
            filtered_df[col] = np.nan
        else:
            filtered_df[col] = pd.to_numeric(filtered_df[col], errors='coerce')
    
    return filtered_df

def create_build_probe_features(df):
    """
    Create feature matrix using build/probe terminology.
    
    PRODUCTION FEASIBILITY:
    All features here are production-feasible:
    - FREE: NumKeyColumns, KeyTypeScores, MixedKeys, AvgKeyBytes (from schema/metadata)
    - CHEAP: BuildRows, ProbeRows (from table stats)
    - MODERATE: BuildDistinctKeys (requires approx_count_distinct on build side)
    - EXPENSIVE: ProbeDistinctKeys (requires approx_count_distinct on probe side - larger table!)
    
    Note: ProbeDistinctKeys requires scanning the LARGER table, which is more expensive.
    However, it provides valuable information about join selectivity and cardinality
    relationships that can significantly improve model accuracy.
    """
    features = pd.DataFrame()
    
    # Determine build and probe sides (smaller side is build side)
    # Build side features
    features['BuildRows'] = df.apply(
        lambda row: row['LeftRows'] if row['LeftRows'] <= row['RightRows'] else row['RightRows'],
        axis=1
    )
    # BuildDistinctKeys: In production, use approx_count_distinct() on build table
    # This requires scanning the build side, but it's essential for the model
    features['BuildDistinctKeys'] = df.apply(
        lambda row: row['LeftDistinctKeys'] if row['LeftRows'] <= row['RightRows'] else row['RightDistinctKeys'],
        axis=1
    )
    features['BuildCardinalityPct'] = df.apply(
        lambda row: row['LeftCardinalityPct'] if row['LeftRows'] <= row['RightRows'] else row['RightCardinalityPct'],
        axis=1
    )
    features['BuildAvgKeyBytes'] = df.apply(
        lambda row: row['LeftAvgKeyBytes'] if row['LeftRows'] <= row['RightRows'] else row['RightAvgKeyBytes'],
        axis=1
    )
    
    # Probe side features
    features['ProbeRows'] = df.apply(
        lambda row: row['RightRows'] if row['LeftRows'] <= row['RightRows'] else row['LeftRows'],
        axis=1
    )
    # ProbeDistinctKeys: In production, requires approx_count_distinct() on probe table
    # This is more expensive (scanning the larger table) but gives the model more information
    features['ProbeDistinctKeys'] = df.apply(
        lambda row: row['RightDistinctKeys'] if row['LeftRows'] <= row['RightRows'] else row['LeftDistinctKeys'],
        axis=1
    )
    features['ProbeCardinalityPct'] = df.apply(
        lambda row: row['RightCardinalityPct'] if row['LeftRows'] <= row['RightRows'] else row['LeftCardinalityPct'],
        axis=1
    )
    features['ProbeAvgKeyBytes'] = df.apply(
        lambda row: row['RightAvgKeyBytes'] if row['LeftRows'] <= row['RightRows'] else row['LeftAvgKeyBytes'],
        axis=1
    )
    
    # Key columns
    if 'NumKeyColumns' in df.columns:
        features['NumKeyColumns'] = df['NumKeyColumns']
    
    # NOTE: KeyOverlapPct is NOT included (requires doing the join to calculate)
    # It's available in synthetic benchmarks but not production-feasible
    
    # Derived features
    features['TotalRows'] = features['BuildRows'] + features['ProbeRows']
    features['BuildProbeRatio'] = features['BuildRows'] / features['ProbeRows']
    
    # Cardinality relationship features (helpful for understanding join selectivity)
    features['BuildProbeCardinalityRatio'] = features['BuildCardinalityPct'] / features['ProbeCardinalityPct'].replace(0, 1)
    features['MaxCardinality'] = features[['BuildCardinalityPct', 'ProbeCardinalityPct']].max(axis=1)
    features['MinCardinality'] = features[['BuildCardinalityPct', 'ProbeCardinalityPct']].min(axis=1)
    
    # Probe-side cardinality flags (similar to build side)
    features['ProbeHighCardinality'] = (features['ProbeCardinalityPct'] > 0.5).astype(int)
    features['ProbeLowCardinality'] = (features['ProbeCardinalityPct'] < 0.1).astype(int)
    
    # Key type complexity based on BIT WIDTH (handles both single and mixed keys)
    # For mixed keys, KeyType column contains comma-separated types like "int,string,long"
    # Use BIT FLAGS to preserve information about which bit widths are present
    key_type_flags = {
        # 32-bit types
        'int': 1,           # 0b0001 = 32-bit integer
        'decimal(9,2)': 1,  # 0b0001 = 32-bit decimal (same flag as int)
        
        # 64-bit types
        'long': 2,          # 0b0010 = 64-bit integer
        'decimal(18,2)': 2, # 0b0010 = 64-bit decimal (same flag as long)
        
        # 128-bit types
        'decimal(38,2)': 4, # 0b0100 = 128-bit decimal (high precision)
        
        # Variable-width types
        'string': 8         # 0b1000 = variable-width (most complex)
    }
    
    def compute_key_type_score(key_type_str):
        """
        Compute score based on bit width using bitwise OR flags.
        
        Bit width grouping:
        - 32-bit (flag=1): int, decimal(9,2)
        - 64-bit (flag=2): long, decimal(18,2)
        - 128-bit (flag=4): decimal(38,2)
        - variable (flag=8): string
        
        Examples:
        - 'int' → 1 (32-bit)
        - 'long' → 2 (64-bit)
        - 'string' → 8 (variable-width)
        - 'int,long' → 1 | 2 = 3 (32-bit + 64-bit)
        - 'int,string' → 1 | 8 = 9 (32-bit + variable)
        - 'int,long,string' → 1 | 2 | 8 = 11 (all three widths)
        - 'decimal(18,2)' → 2 (single type, comma is inside parentheses)
        
        This preserves information about which bit widths are present,
        unlike taking max().
        """
        if ',' not in key_type_str:
            # Single key type
            return key_type_flags.get(key_type_str, 1)
        
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
        
        # If no comma outside parentheses, it's a single type (e.g., decimal(18,2))
        if not has_comma_outside:
            return key_type_flags.get(key_type_str, 1)
        
        # Mixed keys: split only on commas outside parentheses
        types = []
        current = []
        paren_depth = 0
        for char in key_type_str:
            if char == '(':
                paren_depth += 1
                current.append(char)
            elif char == ')':
                paren_depth -= 1
                current.append(char)
            elif char == ',' and paren_depth == 0:
                types.append(''.join(current).strip())
                current = []
            else:
                current.append(char)
        if current:
            types.append(''.join(current).strip())
        
        # Bitwise OR of all type flags
        score = 0
        for t in types:
            score |= key_type_flags.get(t, 1)
        return score
    
    features['BuildKeyTypeScore'] = df.apply(
        lambda row: compute_key_type_score(
            row['LeftKeyType'] if row['LeftRows'] <= row['RightRows'] else row['RightKeyType']
        ),
        axis=1
    )
    features['ProbeKeyTypeScore'] = df.apply(
        lambda row: compute_key_type_score(
            row['RightKeyType'] if row['LeftRows'] <= row['RightRows'] else row['LeftKeyType']
        ),
        axis=1
    )
    features['MaxKeyTypeScore'] = features[['BuildKeyTypeScore', 'ProbeKeyTypeScore']].max(axis=1)
    
    # Mixed key indicator: 1 if multi-column keys have DIFFERENT types (heterogeneous)
    # 0 if single column OR all columns have same type (homogeneous)
    def is_mixed_keys(key_type_str):
        """
        Check if composite key has mixed (heterogeneous) types.
        
        Examples:
        - 'int' → 0 (single column, not mixed)
        - 'int,int' → 0 (multiple columns, but all int - homogeneous)
        - 'int,string' → 1 (multiple columns with different types - heterogeneous!)
        - 'int,int,string' → 1 (not all same type)
        - 'decimal(18,2)' → 0 (single type, comma is inside parentheses)
        """
        if ',' not in key_type_str:
            return 0  # Single column, can't be mixed
        
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
        
        # If no comma outside parentheses, it's a single type
        if not has_comma_outside:
            return 0
        
        # Split only on commas outside parentheses
        types = []
        current = []
        paren_depth = 0
        for char in key_type_str:
            if char == '(':
                paren_depth += 1
                current.append(char)
            elif char == ')':
                paren_depth -= 1
                current.append(char)
            elif char == ',' and paren_depth == 0:
                types.append(''.join(current).strip())
                current = []
            else:
                current.append(char)
        if current:
            types.append(''.join(current).strip())
        
        unique_types = set(types)
        
        # Mixed if more than one unique type
        return 1 if len(unique_types) > 1 else 0
    
    features['MixedKeys'] = df['LeftKeyType'].apply(is_mixed_keys)
    
    # Cardinality flags
    features['BuildHighCardinality'] = (features['BuildCardinalityPct'] > 0.5).astype(int)
    features['BuildLowCardinality'] = (features['BuildCardinalityPct'] < 0.1).astype(int)
    features['BuildVeryLowCardinality'] = (features['BuildCardinalityPct'] < 0.05).astype(int)
    
    # Key count statistics (if available)
    # Map left/right to build/probe based on which side is smaller
    key_count_cols = [
        'BuildMaxKeyCount', 'BuildKeyCountStdDev', 'BuildKeyCountP99', 'BuildKeyCountP95',
        'BuildKeyCountP90', 'BuildKeyCountP75', 'BuildKeyCountP50',
        'ProbeMaxKeyCount', 'ProbeKeyCountStdDev', 'ProbeKeyCountP99', 'ProbeKeyCountP95',
        'ProbeKeyCountP90', 'ProbeKeyCountP75', 'ProbeKeyCountP50'
    ]
    
    # Check if key count columns exist (they may not be in older TSV files)
    if all(col in df.columns for col in key_count_cols):
        # Key count stats are already mapped to build/probe in the TSV
        features['BuildMaxKeyCount'] = pd.to_numeric(df['BuildMaxKeyCount'], errors='coerce').fillna(0)
        features['BuildKeyCountStdDev'] = pd.to_numeric(df['BuildKeyCountStdDev'], errors='coerce').fillna(0)
        features['BuildKeyCountP99'] = pd.to_numeric(df['BuildKeyCountP99'], errors='coerce').fillna(0)
        features['BuildKeyCountP95'] = pd.to_numeric(df['BuildKeyCountP95'], errors='coerce').fillna(0)
        features['BuildKeyCountP90'] = pd.to_numeric(df['BuildKeyCountP90'], errors='coerce').fillna(0)
        features['BuildKeyCountP75'] = pd.to_numeric(df['BuildKeyCountP75'], errors='coerce').fillna(0)
        features['BuildKeyCountP50'] = pd.to_numeric(df['BuildKeyCountP50'], errors='coerce').fillna(0)
        
        features['ProbeMaxKeyCount'] = pd.to_numeric(df['ProbeMaxKeyCount'], errors='coerce').fillna(0)
        features['ProbeKeyCountStdDev'] = pd.to_numeric(df['ProbeKeyCountStdDev'], errors='coerce').fillna(0)
        features['ProbeKeyCountP99'] = pd.to_numeric(df['ProbeKeyCountP99'], errors='coerce').fillna(0)
        features['ProbeKeyCountP95'] = pd.to_numeric(df['ProbeKeyCountP95'], errors='coerce').fillna(0)
        features['ProbeKeyCountP90'] = pd.to_numeric(df['ProbeKeyCountP90'], errors='coerce').fillna(0)
        features['ProbeKeyCountP75'] = pd.to_numeric(df['ProbeKeyCountP75'], errors='coerce').fillna(0)
        features['ProbeKeyCountP50'] = pd.to_numeric(df['ProbeKeyCountP50'], errors='coerce').fillna(0)
        
        # Derived features from key count stats
        # Ratio of max to median (skew indicator)
        features['BuildKeyCountSkew'] = features['BuildMaxKeyCount'] / features['BuildKeyCountP50'].replace(0, 1)
        features['ProbeKeyCountSkew'] = features['ProbeMaxKeyCount'] / features['ProbeKeyCountP50'].replace(0, 1)
        
        # Coefficient of variation (stddev / mean, using P50 as proxy for mean)
        features['BuildKeyCountCV'] = features['BuildKeyCountStdDev'] / features['BuildKeyCountP50'].replace(0, 1)
        features['ProbeKeyCountCV'] = features['ProbeKeyCountStdDev'] / features['ProbeKeyCountP50'].replace(0, 1)
        
        # Ratio of high percentiles to median (tail heaviness)
        features['BuildKeyCountTailHeavy'] = features['BuildKeyCountP99'] / features['BuildKeyCountP50'].replace(0, 1)
        features['ProbeKeyCountTailHeavy'] = features['ProbeKeyCountP99'] / features['ProbeKeyCountP50'].replace(0, 1)
    else:
        # Key count stats not available - fill with zeros
        print("WARNING: Key count statistics columns not found in TSV. These features will be set to 0.")
        for col in key_count_cols:
            features[col] = 0
        features['BuildKeyCountSkew'] = 0
        features['ProbeKeyCountSkew'] = 0
        features['BuildKeyCountCV'] = 0
        features['ProbeKeyCountCV'] = 0
        features['BuildKeyCountTailHeavy'] = 0
        features['ProbeKeyCountTailHeavy'] = 0
    
    # Handle any NaN or inf values
    features = features.replace([np.inf, -np.inf], np.nan)
    features = features.fillna(0)
    
    return features

def find_best_strategy_per_config(df):
    """For each unique test configuration, find which strategy is faster."""
    print("Finding best strategy for each configuration...")
    
    # Group by test configuration (left and right side characteristics)
    config_cols = [
        'LeftRows', 'LeftDistinctKeys', 'LeftKeyType',
        'RightRows', 'RightDistinctKeys', 'RightKeyType',
        'NumKeyColumns'
    ]
    
    best_strategies = []
    
    for config, group in df.groupby(config_cols, dropna=False):
        # Get times for each strategy
        hash_time = group[group['JoinStrategy'] == 'hash_object']['MedianTimeMs'].min()
        sort_time = group[group['JoinStrategy'] == 'sort_object_post']['MedianTimeMs'].min()
        
        # Skip if either strategy is missing
        if pd.isna(hash_time) or pd.isna(sort_time):
            continue
        
        # Find which is faster
        if sort_time < hash_time:
            best_strategy = 'sort_object_post'
            best_time = sort_time
        else:
            best_strategy = 'hash_object'
            best_time = hash_time
        
        # Store the best run
        best_row = group[group['JoinStrategy'] == best_strategy].iloc[0].copy()
        best_row['BestStrategy'] = best_strategy
        best_row['BestTime'] = best_time
        best_row['AlternativeTime'] = sort_time if best_strategy == 'hash_object' else hash_time
        best_row['TimeDiffPct'] = ((best_row['AlternativeTime'] - best_time) / best_row['AlternativeTime']) * 100
        best_strategies.append(best_row)
    
    result_df = pd.DataFrame(best_strategies)
    print(f"Found best strategies for {len(result_df)} unique configurations")
    print()
    
    # Show distribution of winners
    print("Best Strategy Distribution:")
    print(result_df['BestStrategy'].value_counts().to_string())
    print()
    
    # Show average time difference
    hash_wins = result_df[result_df['BestStrategy'] == 'hash_object']
    sort_wins = result_df[result_df['BestStrategy'] == 'sort_object_post']
    
    if len(hash_wins) > 0:
        print(f"When hash wins: average {hash_wins['TimeDiffPct'].mean():.1f}% faster than sort")
    if len(sort_wins) > 0:
        print(f"When sort wins: average {sort_wins['TimeDiffPct'].mean():.1f}% faster than hash")
    print()
    
    return result_df

# ============================================================================
# Timing Outlier Analysis
# ============================================================================

def analyze_timing_outliers(df):
    """
    Analyze outliers in join timing, comparing build vs probe time.
    Split analysis between hash joins and sort joins.
    """
    print("="*80)
    print("TIMING OUTLIER ANALYSIS")
    print("="*80)
    print()
    
    # Filter to successful runs with timing data
    timing_df = df[df['Status'] == 'SUCCESS'].copy()
    
    # Check if timing columns exist and have data
    has_build_time = 'BuildTimeMs' in timing_df.columns and timing_df['BuildTimeMs'].notna().any()
    has_probe_time = 'ProbeTimeMs' in timing_df.columns and timing_df['ProbeTimeMs'].notna().any()
    
    if not has_build_time or not has_probe_time:
        print("WARNING: Detailed timing data (BuildTimeMs/ProbeTimeMs) not available.")
        print("         Skipping timing outlier analysis.")
        print("         (This is normal for legacy TSV files without timing breakdown)")
        print()
        return
    
    # Calculate total time from build + probe
    timing_df['TotalTimeMs'] = timing_df['BuildTimeMs'] + timing_df['ProbeTimeMs']
    
    # Use MedianTimeMs as fallback if TotalTimeMs is missing
    timing_df['TotalTimeMs'] = timing_df['TotalTimeMs'].fillna(timing_df['MedianTimeMs'])
    
    # Filter out any remaining NaN values
    timing_df = timing_df[timing_df['TotalTimeMs'].notna() & 
                         timing_df['BuildTimeMs'].notna() & 
                         timing_df['ProbeTimeMs'].notna()].copy()
    
    if len(timing_df) == 0:
        print("WARNING: No valid timing data found after filtering.")
        print()
        return
    
    print(f"Analyzing {len(timing_df)} successful runs with timing data")
    print()
    
    # Split by strategy
    hash_df = timing_df[timing_df['JoinStrategy'] == 'hash_object'].copy()
    sort_df = timing_df[timing_df['JoinStrategy'] == 'sort_object_post'].copy()
    
    print(f"Hash joins: {len(hash_df)} samples")
    print(f"Sort joins: {len(sort_df)} samples")
    print()
    
    # Function to identify outliers using IQR method
    def identify_outliers(series, multiplier=1.5):
        """Identify outliers using IQR method."""
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - multiplier * IQR
        upper_bound = Q3 + multiplier * IQR
        outliers = (series < lower_bound) | (series > upper_bound)
        return outliers, lower_bound, upper_bound, Q1, Q3
    
    # Analyze each strategy separately
    for strategy_name, strategy_df in [("HASH", hash_df), ("SORT", sort_df)]:
        if len(strategy_df) == 0:
            continue
            
        print("="*80)
        print(f"{strategy_name} JOIN TIMING ANALYSIS")
        print("="*80)
        print()
        
        # Determine build and probe sides (smaller side is build side)
        strategy_df['BuildRows'] = strategy_df.apply(
            lambda row: row['LeftRows'] if row['LeftRows'] <= row['RightRows'] else row['RightRows'],
            axis=1
        )
        strategy_df['ProbeRows'] = strategy_df.apply(
            lambda row: row['RightRows'] if row['LeftRows'] <= row['RightRows'] else row['LeftRows'],
            axis=1
        )
        strategy_df['BuildDistinctKeys'] = strategy_df.apply(
            lambda row: row['LeftDistinctKeys'] if row['LeftRows'] <= row['RightRows'] else row['RightDistinctKeys'],
            axis=1
        )
        strategy_df['ProbeDistinctKeys'] = strategy_df.apply(
            lambda row: row['RightDistinctKeys'] if row['LeftRows'] <= row['RightRows'] else row['LeftDistinctKeys'],
            axis=1
        )
        strategy_df['BuildCardinalityPct'] = strategy_df.apply(
            lambda row: row['LeftCardinalityPct'] if row['LeftRows'] <= row['RightRows'] else row['RightCardinalityPct'],
            axis=1
        )
        strategy_df['ProbeCardinalityPct'] = strategy_df.apply(
            lambda row: row['RightCardinalityPct'] if row['LeftRows'] <= row['RightRows'] else row['LeftCardinalityPct'],
            axis=1
        )
        
        # Calculate ratios
        strategy_df['BuildProbeRowRatio'] = strategy_df['BuildRows'] / strategy_df['ProbeRows'].replace(0, np.nan)
        strategy_df['BuildProbeCardinalityRatio'] = strategy_df['BuildCardinalityPct'] / strategy_df['ProbeCardinalityPct'].replace(0, np.nan)
        strategy_df['BuildProbeRatio'] = strategy_df['BuildTimeMs'] / strategy_df['ProbeTimeMs'].replace(0, np.nan)
        strategy_df['BuildTimePct'] = (strategy_df['BuildTimeMs'] / strategy_df['TotalTimeMs'] * 100)
        strategy_df['ProbeTimePct'] = (strategy_df['ProbeTimeMs'] / strategy_df['TotalTimeMs'] * 100)
        
        # Overall statistics
        print("Overall Timing Statistics:")
        print(f"  Total Time (ms):")
        print(f"    Mean:   {strategy_df['TotalTimeMs'].mean():.2f}")
        print(f"    Median: {strategy_df['TotalTimeMs'].median():.2f}")
        print(f"    Min:    {strategy_df['TotalTimeMs'].min():.2f}")
        print(f"    Max:    {strategy_df['TotalTimeMs'].max():.2f}")
        print(f"    StdDev: {strategy_df['TotalTimeMs'].std():.2f}")
        print()
        
        print(f"  Build Time (ms):")
        print(f"    Mean:   {strategy_df['BuildTimeMs'].mean():.2f}")
        print(f"    Median: {strategy_df['BuildTimeMs'].median():.2f}")
        print(f"    Min:    {strategy_df['BuildTimeMs'].min():.2f}")
        print(f"    Max:    {strategy_df['BuildTimeMs'].max():.2f}")
        print(f"    StdDev: {strategy_df['BuildTimeMs'].std():.2f}")
        print()
        
        print(f"  Probe Time (ms):")
        print(f"    Mean:   {strategy_df['ProbeTimeMs'].mean():.2f}")
        print(f"    Median: {strategy_df['ProbeTimeMs'].median():.2f}")
        print(f"    Min:    {strategy_df['ProbeTimeMs'].min():.2f}")
        print(f"    Max:    {strategy_df['ProbeTimeMs'].max():.2f}")
        print(f"    StdDev: {strategy_df['ProbeTimeMs'].std():.2f}")
        print()
        
        print(f"  Build/Probe Ratio:")
        valid_ratios = strategy_df['BuildProbeRatio'].dropna()
        if len(valid_ratios) > 0:
            print(f"    Mean:   {valid_ratios.mean():.3f}")
            print(f"    Median: {valid_ratios.median():.3f}")
            print(f"    Min:    {valid_ratios.min():.3f}")
            print(f"    Max:    {valid_ratios.max():.3f}")
            print(f"    Interpretation:")
            print(f"      < 1.0: Probe takes longer (typical for large probe tables)")
            print(f"      = 1.0: Build and probe take equal time")
            print(f"      > 1.0: Build takes longer (unusual, may indicate issues)")
        print()
        
        print(f"  Time Distribution:")
        print(f"    Build time % of total: Mean={strategy_df['BuildTimePct'].mean():.1f}%, Median={strategy_df['BuildTimePct'].median():.1f}%")
        print(f"    Probe time % of total:  Mean={strategy_df['ProbeTimePct'].mean():.1f}%, Median={strategy_df['ProbeTimePct'].median():.1f}%")
        print()
        
        # Row count statistics
        print("Row Count Statistics:")
        print(f"  Build Rows:")
        print(f"    Mean:   {strategy_df['BuildRows'].mean():,.0f}")
        print(f"    Median: {strategy_df['BuildRows'].median():,.0f}")
        print(f"    Min:    {strategy_df['BuildRows'].min():,.0f}")
        print(f"    Max:    {strategy_df['BuildRows'].max():,.0f}")
        print()
        print(f"  Probe Rows:")
        print(f"    Mean:   {strategy_df['ProbeRows'].mean():,.0f}")
        print(f"    Median: {strategy_df['ProbeRows'].median():,.0f}")
        print(f"    Min:    {strategy_df['ProbeRows'].min():,.0f}")
        print(f"    Max:    {strategy_df['ProbeRows'].max():,.0f}")
        print()
        valid_row_ratios = strategy_df['BuildProbeRowRatio'].dropna()
        if len(valid_row_ratios) > 0:
            print(f"  Build/Probe Row Ratio:")
            print(f"    Mean:   {valid_row_ratios.mean():.4f}")
            print(f"    Median: {valid_row_ratios.median():.4f}")
            print(f"    Min:    {valid_row_ratios.min():.4f}")
            print(f"    Max:    {valid_row_ratios.max():.4f}")
            print(f"    Interpretation: < 1.0 means build side is smaller (typical)")
        print()
        
        # Cardinality statistics
        print("Cardinality Statistics:")
        print(f"  Build Cardinality %:")
        print(f"    Mean:   {strategy_df['BuildCardinalityPct'].mean()*100:.3f}%")
        print(f"    Median: {strategy_df['BuildCardinalityPct'].median()*100:.3f}%")
        print(f"    Min:    {strategy_df['BuildCardinalityPct'].min()*100:.3f}%")
        print(f"    Max:    {strategy_df['BuildCardinalityPct'].max()*100:.3f}%")
        print()
        print(f"  Probe Cardinality %:")
        print(f"    Mean:   {strategy_df['ProbeCardinalityPct'].mean()*100:.3f}%")
        print(f"    Median: {strategy_df['ProbeCardinalityPct'].median()*100:.3f}%")
        print(f"    Min:    {strategy_df['ProbeCardinalityPct'].min()*100:.3f}%")
        print(f"    Max:    {strategy_df['ProbeCardinalityPct'].max()*100:.3f}%")
        print()
        valid_card_ratios = strategy_df['BuildProbeCardinalityRatio'].dropna()
        if len(valid_card_ratios) > 0:
            print(f"  Build/Probe Cardinality Ratio:")
            print(f"    Mean:   {valid_card_ratios.mean():.4f}")
            print(f"    Median: {valid_card_ratios.median():.4f}")
            print(f"    Min:    {valid_card_ratios.min():.4f}")
            print(f"    Max:    {valid_card_ratios.max():.4f}")
        print()
        
        # Output rows statistics (join result size)
        if 'OutputRows' in strategy_df.columns:
            # Calculate join selectivity metrics
            strategy_df['OutputRows'] = pd.to_numeric(strategy_df['OutputRows'], errors='coerce')
            strategy_df['OutputBuildRatio'] = strategy_df['OutputRows'] / strategy_df['BuildRows'].replace(0, np.nan)
            strategy_df['OutputProbeRatio'] = strategy_df['OutputRows'] / strategy_df['ProbeRows'].replace(0, np.nan)
            strategy_df['JoinSelectivity'] = strategy_df['OutputRows'] / (strategy_df['BuildRows'] * strategy_df['ProbeRows']).replace(0, np.nan)
            
            print("Output Rows Statistics (Join Result Size):")
            print(f"  Output Rows:")
            print(f"    Mean:   {strategy_df['OutputRows'].mean():,.0f}")
            print(f"    Median: {strategy_df['OutputRows'].median():,.0f}")
            print(f"    Min:    {strategy_df['OutputRows'].min():,.0f}")
            print(f"    Max:    {strategy_df['OutputRows'].max():,.0f}")
            print()
            
            valid_output_build = strategy_df['OutputBuildRatio'].dropna()
            if len(valid_output_build) > 0:
                print(f"  Output/Build Ratio (rows produced per build row):")
                print(f"    Mean:   {valid_output_build.mean():.4f}")
                print(f"    Median: {valid_output_build.median():.4f}")
                print(f"    Min:    {valid_output_build.min():.4f}")
                print(f"    Max:    {valid_output_build.max():.4f}")
                print(f"    Interpretation: > 1.0 means each build row produces multiple output rows")
            print()
            
            valid_output_probe = strategy_df['OutputProbeRatio'].dropna()
            if len(valid_output_probe) > 0:
                print(f"  Output/Probe Ratio (rows produced per probe row):")
                print(f"    Mean:   {valid_output_probe.mean():.4f}")
                print(f"    Median: {valid_output_probe.median():.4f}")
                print(f"    Min:    {valid_output_probe.min():.4f}")
                print(f"    Max:    {valid_output_probe.max():.4f}")
            print()
            
            valid_selectivity = strategy_df['JoinSelectivity'].dropna()
            if len(valid_selectivity) > 0:
                print(f"  Join Selectivity (OutputRows / (BuildRows × ProbeRows)):")
                print(f"    Mean:   {valid_selectivity.mean():.6f}")
                print(f"    Median: {valid_selectivity.median():.6f}")
                print(f"    Min:    {valid_selectivity.min():.6f}")
                print(f"    Max:    {valid_selectivity.max():.6f}")
                print(f"    Interpretation: Lower values = more selective join (fewer matches)")
            print()
        
        # Correlation analysis - Combined all metrics into single ranked lists
        print("="*80)
        print("CORRELATION ANALYSIS: How Metrics Impact Build and Probe Times")
        print("="*80)
        print()
        print("  All metrics (basic stats, key type/size, key count statistics, and derived features)")
        print("  are combined into single ranked lists to show which features are most useful for prediction.")
        print()
        
        # Collect ALL metrics for build time correlation
        build_correlations = {}
        
        # Basic metrics
        build_metrics = ['BuildRows', 'BuildDistinctKeys', 'BuildCardinalityPct', 
                        'ProbeRows', 'ProbeDistinctKeys', 'ProbeCardinalityPct',
                        'BuildProbeRowRatio', 'BuildProbeCardinalityRatio']
        # Add output rows if available
        if 'OutputRows' in strategy_df.columns:
            build_metrics.extend(['OutputRows', 'OutputBuildRatio', 'OutputProbeRatio', 'JoinSelectivity'])
        
        # Key type and key size metrics (if available)
        key_type_metrics = ['BuildKeyTypeScore', 'ProbeKeyTypeScore', 'MaxKeyTypeScore', 
                           'BuildAvgKeyBytes', 'ProbeAvgKeyBytes', 'MixedKeys']
        available_key_type_metrics = [m for m in key_type_metrics if m in strategy_df.columns]
        
        # Key count statistics (if available)
        key_count_metrics = [
            'BuildMaxKeyCount', 'BuildKeyCountStdDev', 'BuildKeyCountP99', 'BuildKeyCountP95',
            'BuildKeyCountP90', 'BuildKeyCountP75', 'BuildKeyCountP50',
            'ProbeMaxKeyCount', 'ProbeKeyCountStdDev', 'ProbeKeyCountP99', 'ProbeKeyCountP95',
            'ProbeKeyCountP90', 'ProbeKeyCountP75', 'ProbeKeyCountP50'
        ]
        available_key_count_metrics = [m for m in key_count_metrics if m in strategy_df.columns]
        
        # Derived key count features (if available)
        derived_features = ['BuildKeyCountSkew', 'ProbeKeyCountSkew', 'BuildKeyCountCV', 
                          'ProbeKeyCountCV', 'BuildKeyCountTailHeavy', 'ProbeKeyCountTailHeavy']
        available_derived = [f for f in derived_features if f in strategy_df.columns]
        
        # Calculate correlations for all metrics with build time
        all_build_metrics = build_metrics + available_key_type_metrics + available_key_count_metrics + available_derived
        for metric in all_build_metrics:
            if metric in strategy_df.columns:
                corr = strategy_df['BuildTimeMs'].corr(strategy_df[metric])
                if pd.notna(corr):
                    build_correlations[metric] = corr
        
        # Collect ALL metrics for probe time correlation
        probe_correlations = {}
        
        # Basic metrics (same as build, plus some probe-specific)
        probe_metrics = ['BuildRows', 'BuildDistinctKeys', 'BuildCardinalityPct', 
                        'ProbeRows', 'ProbeDistinctKeys', 'ProbeCardinalityPct',
                        'BuildProbeRowRatio', 'BuildProbeCardinalityRatio']
        # Add output rows if available
        if 'OutputRows' in strategy_df.columns:
            probe_metrics.extend(['OutputRows', 'OutputBuildRatio', 'OutputProbeRatio', 'JoinSelectivity'])
        
        # Calculate correlations for all metrics with probe time
        all_probe_metrics = probe_metrics + available_key_type_metrics + available_key_count_metrics + available_derived
        for metric in all_probe_metrics:
            if metric in strategy_df.columns:
                corr = strategy_df['ProbeTimeMs'].corr(strategy_df[metric])
                if pd.notna(corr):
                    probe_correlations[metric] = corr
        
        # Helper function to format metric names
        def format_metric_name(metric):
            """Format metric name for display."""
            name = metric.replace('Build', 'Build ').replace('Probe', 'Probe ')
            name = name.replace('Pct', '%').replace('Ratio', 'Ratio')
            name = name.replace('KeyCount', 'Key Count ')
            name = name.replace('StdDev', 'Std Dev')
            name = name.replace('CV', 'CV').replace('Skew', 'Skew')
            name = name.replace('TailHeavy', 'Tail Heavy')
            return name
        
        # Print combined build time correlations (all metrics ranked together)
        print("Build Time Correlations (All Metrics Ranked):")
        print("  (What impacts how long it takes to build the join object?)")
        print("  Metrics include: basic stats, key type/size, key count statistics, and derived features")
        print()
        sorted_build_corr = sorted(build_correlations.items(), key=lambda x: abs(x[1]), reverse=True)
        for rank, (metric, corr) in enumerate(sorted_build_corr, 1):
            metric_name = format_metric_name(metric)
            direction = "positive" if corr > 0 else "negative"
            strength = "strong" if abs(corr) > 0.7 else "moderate" if abs(corr) > 0.4 else "weak"
            print(f"  {rank:<2d}. {metric_name:35s}: {corr:>7.3f} ({strength} {direction})")
        print()
        
        # Print combined probe time correlations (all metrics ranked together)
        print("Probe Time Correlations (All Metrics Ranked):")
        print("  (What impacts how long it takes to probe the join object?)")
        print("  NOTE: Probe time may be impacted by build-side metrics and output size!")
        print("  Metrics include: basic stats, key type/size, key count statistics, and derived features")
        print()
        sorted_probe_corr = sorted(probe_correlations.items(), key=lambda x: abs(x[1]), reverse=True)
        for rank, (metric, corr) in enumerate(sorted_probe_corr, 1):
            metric_name = format_metric_name(metric)
            direction = "positive" if corr > 0 else "negative"
            strength = "strong" if abs(corr) > 0.7 else "moderate" if abs(corr) > 0.4 else "weak"
            print(f"  {rank:<2d}. {metric_name:35s}: {corr:>7.3f} ({strength} {direction})")
        print()
        
        # Summary insights
        print("Summary Insights:")
        if sorted_build_corr:
            top_build = sorted_build_corr[0]
            top_name = format_metric_name(top_build[0])
            print(f"  Build time: Most correlated with {top_name} (r={top_build[1]:.3f})")
        if sorted_probe_corr:
            top_probe = sorted_probe_corr[0]
            top_name = format_metric_name(top_probe[0])
            print(f"  Probe time: Most correlated with {top_name} (r={top_probe[1]:.3f})")
        
        # Count metrics by category
        build_basic = sum(1 for m in sorted_build_corr if m[0] not in available_key_count_metrics and m[0] not in available_derived)
        build_key_count = sum(1 for m in sorted_build_corr if m[0] in available_key_count_metrics)
        build_derived = sum(1 for m in sorted_build_corr if m[0] in available_derived)
        probe_basic = sum(1 for m in sorted_probe_corr if m[0] not in available_key_count_metrics and m[0] not in available_derived)
        probe_key_count = sum(1 for m in sorted_probe_corr if m[0] in available_key_count_metrics)
        probe_derived = sum(1 for m in sorted_probe_corr if m[0] in available_derived)
        
        print()
        print("  Metric counts by category:")
        print(f"    Build time: {build_basic} basic, {build_key_count} key count, {build_derived} derived")
        print(f"    Probe time: {probe_basic} basic, {probe_key_count} key count, {probe_derived} derived")
        
        if not available_key_count_metrics:
            print()
            print("  NOTE: Key count statistics not available in this dataset.")
            print("        Run the updated benchmark to include these metrics.")
        print()
        
        # Identify outliers in total time
        total_outliers, total_lower, total_upper, total_q1, total_q3 = identify_outliers(strategy_df['TotalTimeMs'])
        outlier_count = total_outliers.sum()
        
        print(f"Outlier Detection (IQR method, multiplier=1.5):")
        print(f"  Total Time Outliers: {outlier_count} / {len(strategy_df)} ({outlier_count/len(strategy_df)*100:.1f}%)")
        print(f"    Q1: {total_q1:.2f} ms")
        print(f"    Q3: {total_q3:.2f} ms")
        print(f"    IQR: {total_q3 - total_q1:.2f} ms")
        print(f"    Lower bound: {total_lower:.2f} ms")
        print(f"    Upper bound: {total_upper:.2f} ms")
        print()
        
        if outlier_count > 0:
            outlier_df = strategy_df[total_outliers].copy()
            
            print(f"  Outlier Details (showing top 20 by total time):")
            print()
            
            # Sort by total time descending
            outlier_df_sorted = outlier_df.nlargest(20, 'TotalTimeMs')
            
            # Check if OutputRows is available
            has_output_rows = 'OutputRows' in strategy_df.columns
            has_build_max_key_count = 'BuildMaxKeyCount' in strategy_df.columns
            
            if has_output_rows:
                if has_build_max_key_count:
                    print(f"{'Test Name':<23s} {'Total':>10s} {'Build':>10s} {'Probe':>10s} {'B/P':>7s} {'B Rows':>13s} {'P Rows':>13s} {'Out Rows':>13s} {'B Card%':>9s} {'P Card%':>9s} {'B MaxKC':>10s}")
                    print("-" * 150)
                else:
                    print(f"{'Test Name':<23s} {'Total':>10s} {'Build':>10s} {'Probe':>10s} {'B/P':>7s} {'B Rows':>13s} {'P Rows':>13s} {'Out Rows':>13s} {'B Card%':>9s} {'P Card%':>9s}")
                    print("-" * 140)
            else:
                if has_build_max_key_count:
                    print(f"{'Test Name':<25s} {'Total':>10s} {'Build':>10s} {'Probe':>10s} {'B/P':>7s} {'B Rows':>15s} {'P Rows':>15s} {'B/P':>7s} {'B Card%':>9s} {'P Card%':>9s} {'B MaxKC':>10s}")
                    print("-" * 140)
                else:
                    print(f"{'Test Name':<25s} {'Total':>10s} {'Build':>10s} {'Probe':>10s} {'B/P':>7s} {'B Rows':>15s} {'P Rows':>15s} {'B/P':>7s} {'B Card%':>9s} {'P Card%':>9s}")
                    print("-" * 130)
            
            for idx, row in outlier_df_sorted.iterrows():
                test_name = row.get('TestName', 'unknown')[:21 if has_output_rows else 23]  # Truncate if too long
                total = row['TotalTimeMs']
                build = row['BuildTimeMs']
                probe = row['ProbeTimeMs']
                ratio = row['BuildProbeRatio'] if pd.notna(row['BuildProbeRatio']) else np.nan
                build_rows = row.get('BuildRows', np.nan)
                probe_rows = row.get('ProbeRows', np.nan)
                row_ratio = row.get('BuildProbeRowRatio', np.nan)
                build_card = row.get('BuildCardinalityPct', np.nan) * 100 if pd.notna(row.get('BuildCardinalityPct', np.nan)) else np.nan
                probe_card = row.get('ProbeCardinalityPct', np.nan) * 100 if pd.notna(row.get('ProbeCardinalityPct', np.nan)) else np.nan
                output_rows = row.get('OutputRows', np.nan) if has_output_rows else np.nan
                build_max_key_count = pd.to_numeric(row.get('BuildMaxKeyCount', np.nan), errors='coerce') if has_build_max_key_count else np.nan
                
                ratio_str = f"{ratio:.2f}" if pd.notna(ratio) else "N/A"
                build_rows_str = f"{build_rows:,.0f}" if pd.notna(build_rows) else "N/A"
                probe_rows_str = f"{probe_rows:,.0f}" if pd.notna(probe_rows) else "N/A"
                row_ratio_str = f"{row_ratio:.3f}" if pd.notna(row_ratio) else "N/A"
                build_card_str = f"{build_card:.2f}%" if pd.notna(build_card) else "N/A"
                probe_card_str = f"{probe_card:.2f}%" if pd.notna(probe_card) else "N/A"
                output_rows_str = f"{output_rows:,.0f}" if pd.notna(output_rows) else "N/A"
                build_max_key_count_str = f"{build_max_key_count:,.0f}" if pd.notna(build_max_key_count) else "N/A"
                
                if has_output_rows:
                    if has_build_max_key_count:
                        print(f"{test_name:<23s} {total:>10.1f} {build:>10.1f} {probe:>10.1f} {ratio_str:>7s} "
                              f"{build_rows_str:>13s} {probe_rows_str:>13s} {output_rows_str:>13s} "
                              f"{build_card_str:>9s} {probe_card_str:>9s} {build_max_key_count_str:>10s}")
                    else:
                        print(f"{test_name:<23s} {total:>10.1f} {build:>10.1f} {probe:>10.1f} {ratio_str:>7s} "
                              f"{build_rows_str:>13s} {probe_rows_str:>13s} {output_rows_str:>13s} "
                              f"{build_card_str:>9s} {probe_card_str:>9s}")
                else:
                    if has_build_max_key_count:
                        print(f"{test_name:<25s} {total:>10.1f} {build:>10.1f} {probe:>10.1f} {ratio_str:>7s} "
                              f"{build_rows_str:>15s} {probe_rows_str:>15s} {row_ratio_str:>7s} "
                              f"{build_card_str:>9s} {probe_card_str:>9s} {build_max_key_count_str:>10s}")
                    else:
                        print(f"{test_name:<25s} {total:>10.1f} {build:>10.1f} {probe:>10.1f} {ratio_str:>7s} "
                              f"{build_rows_str:>15s} {probe_rows_str:>15s} {row_ratio_str:>7s} "
                              f"{build_card_str:>9s} {probe_card_str:>9s}")
            
            print()
            
            # Analyze outliers by build vs probe time
            print(f"  Outlier Analysis by Phase:")
            outlier_build_mean = outlier_df['BuildTimeMs'].mean()
            outlier_probe_mean = outlier_df['ProbeTimeMs'].mean()
            outlier_total_mean = outlier_df['TotalTimeMs'].mean()
            
            normal_df = strategy_df[~total_outliers]
            normal_build_mean = normal_df['BuildTimeMs'].mean()
            normal_probe_mean = normal_df['ProbeTimeMs'].mean()
            normal_total_mean = normal_df['TotalTimeMs'].mean()
            
            print(f"    Outliers:")
            print(f"      Build time: {outlier_build_mean:.2f} ms ({outlier_build_mean/outlier_total_mean*100:.1f}% of total)")
            print(f"      Probe time: {outlier_probe_mean:.2f} ms ({outlier_probe_mean/outlier_total_mean*100:.1f}% of total)")
            print(f"      Total time: {outlier_total_mean:.2f} ms")
            print()
            print(f"    Normal cases:")
            print(f"      Build time: {normal_build_mean:.2f} ms ({normal_build_mean/normal_total_mean*100:.1f}% of total)")
            print(f"      Probe time: {normal_probe_mean:.2f} ms ({normal_probe_mean/normal_total_mean*100:.1f}% of total)")
            print(f"      Total time: {normal_total_mean:.2f} ms")
            print()
            print(f"    Comparison (outliers vs normal):")
            build_multiplier = outlier_build_mean / normal_build_mean if normal_build_mean > 0 else np.nan
            probe_multiplier = outlier_probe_mean / normal_probe_mean if normal_probe_mean > 0 else np.nan
            total_multiplier = outlier_total_mean / normal_total_mean if normal_total_mean > 0 else np.nan
            
            print(f"      Build time: {build_multiplier:.2f}x slower" if pd.notna(build_multiplier) else "      Build time: N/A")
            print(f"      Probe time: {probe_multiplier:.2f}x slower" if pd.notna(probe_multiplier) else "      Probe time: N/A")
            print(f"      Total time: {total_multiplier:.2f}x slower" if pd.notna(total_multiplier) else "      Total time: N/A")
            print()
            
            # Identify outliers where build time is unusually high
            build_outliers, build_lower, build_upper, build_q1, build_q3 = identify_outliers(strategy_df['BuildTimeMs'])
            build_outlier_count = build_outliers.sum()
            
            print(f"  Build Time Outliers: {build_outlier_count} / {len(strategy_df)} ({build_outlier_count/len(strategy_df)*100:.1f}%)")
            if build_outlier_count > 0:
                build_outlier_df = strategy_df[build_outliers].nlargest(10, 'BuildTimeMs')
                print(f"    Top 10 build time outliers:")
                has_output_rows = 'OutputRows' in strategy_df.columns
                has_build_max_key_count = 'BuildMaxKeyCount' in strategy_df.columns
                if has_output_rows:
                    if has_build_max_key_count:
                        print(f"    {'Test Name':<28s} {'Build':>10s} {'B Rows':>13s} {'B Card%':>10s} {'Out Rows':>13s} {'B MaxKC':>10s}")
                        print("    " + "-" * 100)
                    else:
                        print(f"    {'Test Name':<28s} {'Build':>10s} {'B Rows':>13s} {'B Card%':>10s} {'Out Rows':>13s}")
                        print("    " + "-" * 90)
                else:
                    if has_build_max_key_count:
                        print(f"    {'Test Name':<30s} {'Build':>10s} {'B Rows':>15s} {'B Card%':>10s} {'B DistKeys':>15s} {'B MaxKC':>10s}")
                        print("    " + "-" * 100)
                    else:
                        print(f"    {'Test Name':<30s} {'Build':>10s} {'B Rows':>15s} {'B Card%':>10s} {'B DistKeys':>15s}")
                        print("    " + "-" * 90)
                for idx, row in build_outlier_df.iterrows():
                    test_name = row.get('TestName', 'unknown')[:26 if has_output_rows else 28]
                    build_rows = row.get('BuildRows', np.nan)
                    build_card = row.get('BuildCardinalityPct', np.nan) * 100 if pd.notna(row.get('BuildCardinalityPct', np.nan)) else np.nan
                    build_distinct = row.get('BuildDistinctKeys', np.nan)
                    output_rows = row.get('OutputRows', np.nan) if has_output_rows else np.nan
                    build_max_key_count = pd.to_numeric(row.get('BuildMaxKeyCount', np.nan), errors='coerce') if has_build_max_key_count else np.nan
                    build_rows_str = f"{build_rows:,.0f}" if pd.notna(build_rows) else "N/A"
                    build_card_str = f"{build_card:.2f}%" if pd.notna(build_card) else "N/A"
                    build_distinct_str = f"{build_distinct:,.0f}" if pd.notna(build_distinct) else "N/A"
                    output_rows_str = f"{output_rows:,.0f}" if pd.notna(output_rows) else "N/A"
                    build_max_key_count_str = f"{build_max_key_count:,.0f}" if pd.notna(build_max_key_count) else "N/A"
                    if has_output_rows:
                        if has_build_max_key_count:
                            print(f"    {test_name:<28s} {row['BuildTimeMs']:>10.2f} {build_rows_str:>13s} {build_card_str:>10s} {output_rows_str:>13s} {build_max_key_count_str:>10s}")
                        else:
                            print(f"    {test_name:<28s} {row['BuildTimeMs']:>10.2f} {build_rows_str:>13s} {build_card_str:>10s} {output_rows_str:>13s}")
                    else:
                        if has_build_max_key_count:
                            print(f"    {test_name:<30s} {row['BuildTimeMs']:>10.2f} {build_rows_str:>15s} {build_card_str:>10s} {build_distinct_str:>15s} {build_max_key_count_str:>10s}")
                        else:
                            print(f"    {test_name:<30s} {row['BuildTimeMs']:>10.2f} {build_rows_str:>15s} {build_card_str:>10s} {build_distinct_str:>15s}")
            print()
            
            # Identify outliers where probe time is unusually high
            probe_outliers, probe_lower, probe_upper, probe_q1, probe_q3 = identify_outliers(strategy_df['ProbeTimeMs'])
            probe_outlier_count = probe_outliers.sum()
            
            print(f"  Probe Time Outliers: {probe_outlier_count} / {len(strategy_df)} ({probe_outlier_count/len(strategy_df)*100:.1f}%)")
            if probe_outlier_count > 0:
                probe_outlier_df = strategy_df[probe_outliers].nlargest(10, 'ProbeTimeMs')
                print(f"    Top 10 probe time outliers:")
                has_output_rows = 'OutputRows' in strategy_df.columns
                if has_output_rows:
                    print(f"    {'Test Name':<26s} {'Probe':>10s} {'P Rows':>13s} {'P Card%':>10s} {'B Rows':>13s} {'Out Rows':>13s}")
                    print("    " + "-" * 110)
                else:
                    print(f"    {'Test Name':<30s} {'Probe':>10s} {'P Rows':>15s} {'P Card%':>10s} {'B Rows':>15s} {'B Card%':>10s}")
                    print("    " + "-" * 110)
                for idx, row in probe_outlier_df.iterrows():
                    test_name = row.get('TestName', 'unknown')[:24 if has_output_rows else 28]
                    probe_rows = row.get('ProbeRows', np.nan)
                    probe_card = row.get('ProbeCardinalityPct', np.nan) * 100 if pd.notna(row.get('ProbeCardinalityPct', np.nan)) else np.nan
                    build_rows = row.get('BuildRows', np.nan)
                    build_card = row.get('BuildCardinalityPct', np.nan) * 100 if pd.notna(row.get('BuildCardinalityPct', np.nan)) else np.nan
                    output_rows = row.get('OutputRows', np.nan) if has_output_rows else np.nan
                    probe_rows_str = f"{probe_rows:,.0f}" if pd.notna(probe_rows) else "N/A"
                    probe_card_str = f"{probe_card:.2f}%" if pd.notna(probe_card) else "N/A"
                    build_rows_str = f"{build_rows:,.0f}" if pd.notna(build_rows) else "N/A"
                    build_card_str = f"{build_card:.2f}%" if pd.notna(build_card) else "N/A"
                    output_rows_str = f"{output_rows:,.0f}" if pd.notna(output_rows) else "N/A"
                    if has_output_rows:
                        print(f"    {test_name:<26s} {row['ProbeTimeMs']:>10.2f} {probe_rows_str:>13s} {probe_card_str:>10s} "
                              f"{build_rows_str:>13s} {output_rows_str:>13s}")
                    else:
                        print(f"    {test_name:<30s} {row['ProbeTimeMs']:>10.2f} {probe_rows_str:>15s} {probe_card_str:>10s} "
                              f"{build_rows_str:>15s} {build_card_str:>10s}")
                print()
                if has_output_rows:
                    print("    Note: Output rows shown to analyze correlation with probe time")
                else:
                    print("    Note: Build-side metrics shown to analyze their impact on probe time")
            print()
            
            # Analyze build/probe ratio outliers
            valid_ratios = strategy_df['BuildProbeRatio'].dropna()
            if len(valid_ratios) > 10:  # Need enough samples
                ratio_outliers, ratio_lower, ratio_upper, ratio_q1, ratio_q3 = identify_outliers(valid_ratios)
                ratio_outlier_count = ratio_outliers.sum()
                
                print(f"  Build/Probe Ratio Outliers: {ratio_outlier_count} / {len(valid_ratios)} ({ratio_outlier_count/len(valid_ratios)*100:.1f}%)")
                print(f"    Q1: {ratio_q1:.3f}, Q3: {ratio_q3:.3f}, IQR: {ratio_q3 - ratio_q1:.3f}")
                print(f"    Range: [{ratio_lower:.3f}, {ratio_upper:.3f}]")
                print()
                
                if ratio_outlier_count > 0:
                    ratio_outlier_df = strategy_df[strategy_df['BuildProbeRatio'].notna()][ratio_outliers].copy()
                    
                    # Separate into build-heavy and probe-heavy outliers
                    build_heavy = ratio_outlier_df[ratio_outlier_df['BuildProbeRatio'] > ratio_upper]
                    probe_heavy = ratio_outlier_df[ratio_outlier_df['BuildProbeRatio'] < ratio_lower]
                    
                    print(f"    Build-heavy outliers (ratio > {ratio_upper:.3f}): {len(build_heavy)} cases")
                    if len(build_heavy) > 0:
                        print(f"      Mean ratio: {build_heavy['BuildProbeRatio'].mean():.3f}")
                        print(f"      Mean build time: {build_heavy['BuildTimeMs'].mean():.2f} ms")
                        print(f"      Mean probe time: {build_heavy['ProbeTimeMs'].mean():.2f} ms")
                        print(f"      Top 5 build-heavy cases:")
                        for idx, row in build_heavy.nlargest(5, 'BuildProbeRatio').iterrows():
                            test_name = row.get('TestName', 'unknown')[:35]
                            print(f"        {test_name:<35s} Ratio: {row['BuildProbeRatio']:.3f}, "
                                  f"Build: {row['BuildTimeMs']:.2f} ms, Probe: {row['ProbeTimeMs']:.2f} ms")
                    print()
                    
                    print(f"    Probe-heavy outliers (ratio < {ratio_lower:.3f}): {len(probe_heavy)} cases")
                    if len(probe_heavy) > 0:
                        print(f"      Mean ratio: {probe_heavy['BuildProbeRatio'].mean():.3f}")
                        print(f"      Mean build time: {probe_heavy['BuildTimeMs'].mean():.2f} ms")
                        print(f"      Mean probe time: {probe_heavy['ProbeTimeMs'].mean():.2f} ms")
                        print(f"      Top 5 probe-heavy cases:")
                        for idx, row in probe_heavy.nsmallest(5, 'BuildProbeRatio').iterrows():
                            test_name = row.get('TestName', 'unknown')[:35]
                            print(f"        {test_name:<35s} Ratio: {row['BuildProbeRatio']:.3f}, "
                                  f"Build: {row['BuildTimeMs']:.2f} ms, Probe: {row['ProbeTimeMs']:.2f} ms")
                    print()
        
        print()
    
    # Cross-strategy comparison
    if len(hash_df) > 0 and len(sort_df) > 0:
        print("="*80)
        print("CROSS-STRATEGY COMPARISON")
        print("="*80)
        print()
        
        print("Average Timing Comparison:")
        print(f"{'Metric':<20s} {'Hash':>15s} {'Sort':>15s} {'Difference':>15s}")
        print("-" * 65)
        
        hash_total_mean = hash_df['TotalTimeMs'].mean()
        sort_total_mean = sort_df['TotalTimeMs'].mean()
        hash_build_mean = hash_df['BuildTimeMs'].mean()
        sort_build_mean = sort_df['BuildTimeMs'].mean()
        hash_probe_mean = hash_df['ProbeTimeMs'].mean()
        sort_probe_mean = sort_df['ProbeTimeMs'].mean()
        
        print(f"{'Total Time (ms)':<20s} {hash_total_mean:>15.2f} {sort_total_mean:>15.2f} "
              f"{hash_total_mean - sort_total_mean:>15.2f}")
        print(f"{'Build Time (ms)':<20s} {hash_build_mean:>15.2f} {sort_build_mean:>15.2f} "
              f"{hash_build_mean - sort_build_mean:>15.2f}")
        print(f"{'Probe Time (ms)':<20s} {hash_probe_mean:>15.2f} {sort_probe_mean:>15.2f} "
              f"{hash_probe_mean - sort_probe_mean:>15.2f}")
        print()
        
        # Build/probe ratio comparison
        hash_ratios = hash_df['BuildProbeRatio'].dropna()
        sort_ratios = sort_df['BuildProbeRatio'].dropna()
        
        if len(hash_ratios) > 0 and len(sort_ratios) > 0:
            print("Build/Probe Ratio Comparison:")
            print(f"  Hash mean ratio: {hash_ratios.mean():.3f}")
            print(f"  Sort mean ratio: {sort_ratios.mean():.3f}")
            print(f"  Difference: {hash_ratios.mean() - sort_ratios.mean():.3f}")
            print()
            
            print("Time Distribution Comparison:")
            print(f"  Hash - Build %: {hash_df['BuildTimePct'].mean():.1f}%, Probe %: {hash_df['ProbeTimePct'].mean():.1f}%")
            print(f"  Sort - Build %: {sort_df['BuildTimePct'].mean():.1f}%, Probe %: {sort_df['ProbeTimePct'].mean():.1f}%")
            print()
    
    print()

# ============================================================================
# Model Training and Evaluation
# ============================================================================

def compute_sample_weights(best_df, weight_strategy='exponential'):
    """
    Compute sample weights based on performance difference between strategies.
    
    Weight strategies:
    - 'none': No weighting (all samples equal weight = 1.0)
    - 'exponential': Exponentially increase weight with absolute time gap
    - 'exponential_pct': Exponentially increase weight with percentage gap
    - 'hybrid': Combine absolute time and percentage (geometric mean)
    - 'quadratic': Quadratic increase with absolute time gap
    - 'linear': Linear increase with absolute time gap  
    - 'threshold': High weight for time differences > 10ms, low weight otherwise
    
    IMPORTANT: We care about absolute time cost, not just percentages!
    - 100% difference on 1ms query = 1ms cost (maybe irrelevant)
    - 10% difference on 10s query = 1000ms cost (very important!)
    
    But sometimes percentage matters too (avoiding 2x slowdowns even on fast queries).
    The 'hybrid' strategy balances both concerns.
    """
    
    # Calculate ABSOLUTE time difference in milliseconds
    time_diff_ms = (best_df['AlternativeTime'] - best_df['BestTime']).values
    
    # Also get percentage difference (handle NaN from division by zero)
    time_diff_pct = best_df['TimeDiffPct'].values
    
    # Clean up any NaN or inf values (can happen with zero times or division issues)
    time_diff_ms = np.nan_to_num(time_diff_ms, nan=0.0, posinf=0.0, neginf=0.0)
    time_diff_pct = np.nan_to_num(time_diff_pct, nan=0.0, posinf=0.0, neginf=0.0)
    
    if weight_strategy == 'none':
        # No weighting - all samples treated equally
        weights = np.ones_like(time_diff_ms)
        
    elif weight_strategy == 'exponential':
        # Exponential weighting by ABSOLUTE TIME: weight = exp(time_diff_ms / scale)
        # AUTO-SCALE based on data to avoid numerical overflow
        median_diff = np.median(time_diff_ms)
        scale = max(50.0, median_diff)  # Use median or 50ms, whichever is larger
        
        # Prevent extreme weights (cap at reasonable range)
        weights = np.exp(np.minimum(time_diff_ms / scale, 10.0))  # Cap at e^10 ≈ 22000x
        
    elif weight_strategy == 'exponential_pct':
        # Exponential weighting by PERCENTAGE: weight = exp(time_diff_pct / scale)
        scale = 30.0  # percent
        # Cap to prevent overflow (max 3.3x difference means e^3 ≈ 20x weight)
        weights = np.exp(np.minimum(time_diff_pct / scale, 3.0))
        
    elif weight_strategy == 'hybrid':
        # HYBRID: Combine absolute and percentage weighting
        # This avoids both sub-ms over-weighting AND missing expensive mistakes
        median_diff = np.median(time_diff_ms)
        scale_ms = max(50.0, median_diff)  # Auto-scale based on data
        scale_pct = 30.0
        
        # Compute both weights with capping to prevent overflow
        weight_ms = np.exp(np.minimum(time_diff_ms / scale_ms, 10.0))
        weight_pct = np.exp(np.minimum(time_diff_pct / scale_pct, 3.0))
        
        # Geometric mean: sqrt(weight_ms * weight_pct)
        # This means BOTH absolute AND percentage must be high for high weight
        weights = np.sqrt(weight_ms * weight_pct)
        
    elif weight_strategy == 'quadratic':
        # Quadratic weighting: emphasize large time differences strongly
        normalized = time_diff_ms / 100.0
        weights = 1.0 + (normalized ** 2)
        
    elif weight_strategy == 'linear':
        # Linear weighting: proportional to time difference
        weights = 1.0 + (time_diff_ms / 100.0)
        
    elif weight_strategy == 'threshold':
        # Threshold weighting: high weight for significant time differences
        weights = np.where(time_diff_ms > 10.0, 5.0, 1.0)
        
    else:
        raise ValueError(f"Unknown weight_strategy: {weight_strategy}")
    
    # Final cleanup: Handle any NaN/inf from weight calculations
    weights = np.nan_to_num(weights, nan=1.0, posinf=1.0, neginf=1.0)
    
    # Ensure all weights are positive
    weights = np.maximum(weights, 0.001)  # Minimum weight to avoid zeros
    
    # Normalize weights to have mean=1 (preserves overall scale)
    weights = weights / weights.mean()
    
    return pd.Series(weights, index=best_df.index)

def assess_data_sufficiency(best_df):
    """Assess whether we have enough data for reliable model training."""
    print("="*80)
    print("DATA SUFFICIENCY ASSESSMENT")
    print("="*80)
    print()
    
    issues = []
    warnings = []
    
    # Check total sample size
    # INCREASED: Higher thresholds for better generalization
    total_samples = len(best_df)
    print(f"Total unique configurations: {total_samples}")
    if total_samples < 200:
        issues.append(f"Too few total samples ({total_samples} < 200)")
    elif total_samples < 400:
        warnings.append(f"Low total samples ({total_samples} < 400) - model may not generalize well")
    else:
        print(f"  ✓ Good sample size ({total_samples} >= 400)")
    print()
    
    # Check balance between strategies
    strategy_counts = best_df['BestStrategy'].value_counts()
    hash_count = strategy_counts.get('hash_object', 0)
    sort_count = strategy_counts.get('sort_object_post', 0)
    
    print(f"Strategy distribution (which strategy is faster):")
    print(f"  hash_object (class=0):       {hash_count:4d} ({hash_count/total_samples*100:.1f}%) - Hash-based join wins")
    print(f"  sort_object_post (class=1):  {sort_count:4d} ({sort_count/total_samples*100:.1f}%) - Sort-based join wins")
    print()
    
    if sort_count < MIN_SAMPLES_PER_STRATEGY:
        issues.append(f"Too few sort wins ({sort_count} < {MIN_SAMPLES_PER_STRATEGY}) - need more low cardinality tests!")
    elif sort_count < MIN_SAMPLES_PER_STRATEGY * 2:
        warnings.append(f"Low sort wins ({sort_count} < {MIN_SAMPLES_PER_STRATEGY * 2}) - consider more low cardinality tests")
    else:
        print(f"  ✓ Sufficient sort samples ({sort_count} >= {MIN_SAMPLES_PER_STRATEGY * 2})")
    
    if hash_count < MIN_SAMPLES_PER_STRATEGY:
        issues.append(f"Too few hash wins ({hash_count} < {MIN_SAMPLES_PER_STRATEGY})")
    else:
        print(f"  ✓ Sufficient hash samples ({hash_count} >= {MIN_SAMPLES_PER_STRATEGY})")
    print()
    
    # Check coverage of key regions (low cardinality where sort should win)
    features = create_build_probe_features(best_df)
    
    # Low cardinality bins
    print("Coverage by Build Cardinality (where sort typically wins):")
    cardinality_bins = [
        ("<0.5%", 0.0, 0.005),
        ("0.5-1%", 0.005, 0.01),
        ("1-2%", 0.01, 0.02),
        ("2-5%", 0.02, 0.05),
        ("5-10%", 0.05, 0.10),
        (">10%", 0.10, 1.0)
    ]
    
    for label, low, high in cardinality_bins:
        count = len(best_df[(features['BuildCardinalityPct'] >= low) & 
                            (features['BuildCardinalityPct'] < high)])
        sort_wins = len(best_df[(features['BuildCardinalityPct'] >= low) & 
                                 (features['BuildCardinalityPct'] < high) &
                                 (best_df['BestStrategy'] == 'sort_object_post')])
        
        status = "✓" if count >= 20 else ("~" if count >= 10 else "✗")
        print(f"  {label:8s}: {status} {count:4d} samples ({sort_wins:3d} sort wins)")
        
        if count < 10 and low < 0.05:  # Focus on very low cardinality
            issues.append(f"Too few samples in {label} cardinality range ({count} < 10)")
        elif count < 20 and low < 0.02:
            warnings.append(f"Low samples in {label} cardinality range ({count} < 20)")
    
    print()
    
    # Overall assessment
    print("="*80)
    print("OVERALL ASSESSMENT")
    print("="*80)
    print()
    
    if issues:
        print("CRITICAL ISSUES (must fix before model is reliable):")
        for issue in issues:
            print(f"  ✗ {issue}")
        print()
        return False, issues, warnings
    
    if warnings:
        print("WARNINGS (model may work but could be improved):")
        for warning in warnings:
            print(f"  ⚠ {warning}")
        print()
        return True, issues, warnings
    
    print("✓ Data looks sufficient for model training!")
    print()
    return True, issues, warnings

def train_and_evaluate_model(best_df, test_size=TEST_SIZE):
    """Train decision tree model and evaluate with train/test split."""
    print("="*80)
    print("MODEL TRAINING AND EVALUATION")
    print("="*80)
    print()
    
    # Create features
    X = create_build_probe_features(best_df)
    
    # Binary labels: 1 = sort wins, 0 = hash wins
    y = (best_df['BestStrategy'] == 'sort_object_post').astype(int)
    
    print(f"Feature matrix: {X.shape[0]} samples × {X.shape[1]} features")
    print(f"Label distribution: {y.sum()} sort wins, {(1-y).sum()} hash wins")
    print()
    
    # Create sample weights based on ABSOLUTE TIME DIFFERENCE
    # Cases with larger ABSOLUTE time differences get higher weight
    # This helps the model focus on avoiding truly expensive mistakes!
    sample_weights = compute_sample_weights(best_df, weight_strategy=WEIGHT_STRATEGY)
    
    # Calculate absolute time differences for analysis
    time_diff_ms = (best_df['AlternativeTime'] - best_df['BestTime']).values
    
    print(f"Sample weighting strategy: '{WEIGHT_STRATEGY}'")
    if WEIGHT_STRATEGY != 'none':
        print(f"  (This prioritizes cases with LARGE ABSOLUTE TIME DIFFERENCES)")
    print()
    
    print(f"Absolute time difference statistics (ms):")
    print(f"  Min time diff:     {time_diff_ms.min():.2f} ms")
    print(f"  Max time diff:     {time_diff_ms.max():.2f} ms")
    print(f"  Mean time diff:    {time_diff_ms.mean():.2f} ms")
    print(f"  Median time diff:  {np.median(time_diff_ms):.2f} ms")
    print()
    
    # Show time diff percentiles
    time_percentiles = [10, 25, 50, 75, 90, 95, 99]
    print("  Time difference percentiles:")
    for p in time_percentiles:
        val = np.percentile(time_diff_ms, p)
        print(f"    {p}th percentile: {val:>8.2f} ms")
    print()
    
    print(f"Sample weight statistics:")
    print(f"  Min weight:    {sample_weights.min():.3f}")
    print(f"  Max weight:    {sample_weights.max():.3f}")
    print(f"  Mean weight:   {sample_weights.mean():.3f}")
    print(f"  Median weight: {sample_weights.median():.3f}")
    print()
    
    # Show weight distribution
    weight_quantiles = [10, 25, 50, 75, 90, 95, 99]
    print("  Weight percentiles:")
    for q in weight_quantiles:
        val = np.percentile(sample_weights, q)
        print(f"    {q}th percentile: {val:>8.3f}")
    print()
    
    # Show correlation between weights and absolute time differences
    print(f"  Correlation with absolute time diff: {np.corrcoef(sample_weights, time_diff_ms)[0,1]:.3f}")
    print()
    
    # Show some examples to validate weighting makes sense
    print("  Example cases (showing how weights relate to time differences):")
    # Get a few representative examples
    sorted_indices = np.argsort(time_diff_ms)
    example_indices = [
        sorted_indices[len(sorted_indices)//10],  # 10th percentile
        sorted_indices[len(sorted_indices)//2],   # 50th percentile (median)
        sorted_indices[9*len(sorted_indices)//10] # 90th percentile
    ]
    for idx in example_indices:
        td = time_diff_ms[idx]
        w = sample_weights.iloc[idx]
        pct = best_df['TimeDiffPct'].iloc[idx]
        print(f"    Time diff: {td:>7.2f} ms ({pct:>5.1f}%) → Weight: {w:>6.3f}x")
    print()
    
    # Calculate appropriate test size based on data amount
    effective_test_size = min(test_size, 0.2)  # Cap at 20%
    if len(X) < 100:
        effective_test_size = 0.1  # Use smaller test set for small datasets
    
    print(f"Using {effective_test_size*100:.0f}% of data for testing")
    print()
    
    # Split train/test (include weights in split)
    X_train, X_test, y_train, y_test, weights_train, weights_test = train_test_split(
        X, y, sample_weights, test_size=effective_test_size, random_state=42, stratify=y if y.nunique() > 1 else None
    )
    
    print(f"Training samples: {len(X_train)}")
    print(f"Test samples: {len(X_test)}")
    print()
    
    # Handle class imbalance by adjusting weights
    # If we have severe imbalance (e.g., 90% hash wins, 10% sort wins),
    # boost the minority class so the model learns when to use sort
    class_counts = y_train.value_counts()
    if len(class_counts) == 2:
        hash_count = class_counts.get(0, 1)
        sort_count = class_counts.get(1, 1)
        imbalance_ratio = hash_count / sort_count
        
        if imbalance_ratio > 3.0:  # Significant imbalance
            print(f"  Detected class imbalance: {imbalance_ratio:.1f}:1 (hash:sort)")
            print(f"  Applying class balance correction...")
            
            # Apply inverse class frequency weighting
            # Minority class gets boosted proportionally
            class_weights = np.where(y_train == 0, 1.0, imbalance_ratio)
            
            # Combine with performance-based weights (element-wise product)
            combined_weights = weights_train * class_weights
            
            # Clean up any potential NaN/inf from combination
            combined_weights = np.nan_to_num(combined_weights, nan=1.0, posinf=1.0, neginf=1.0)
            combined_weights = np.maximum(combined_weights, 0.001)
            
            # Re-normalize
            combined_weights = combined_weights / combined_weights.mean()
            weights_train = combined_weights
            
            print(f"  Sort wins now weighted {imbalance_ratio:.1f}x higher to balance learning")
            print()
    
    # Final validation of weights before training
    if np.any(np.isnan(weights_train)) or np.any(np.isinf(weights_train)):
        print("  ⚠ WARNING: Found NaN/inf in training weights after all corrections!")
        print(f"  NaN count: {np.isnan(weights_train).sum()}")
        print(f"  Inf count: {np.isinf(weights_train).sum()}")
        print("  Replacing with uniform weights (all 1.0) as fallback...")
        weights_train = np.ones_like(weights_train)
    
    # Train decision tree WITH SAMPLE WEIGHTS
    print("Training Decision Tree (with sample weights to prioritize large performance gaps)...")
    dt = DecisionTreeClassifier(
        max_depth=6,
        min_samples_split=10,
        min_samples_leaf=5,
        random_state=42
    )
    # Key change: Pass sample_weight to focus on cases with large performance differences!
    dt.fit(X_train, y_train, sample_weight=weights_train)
    
    # Evaluate
    y_train_pred = dt.predict(X_train)
    y_test_pred = dt.predict(X_test)
    
    train_accuracy = accuracy_score(y_train, y_train_pred)
    test_accuracy = accuracy_score(y_test, y_test_pred)
    overfitting_gap = train_accuracy - test_accuracy
    
    print(f"Training Accuracy:   {train_accuracy:.1%}")
    print(f"Test Accuracy:       {test_accuracy:.1%}")
    print(f"Overfitting Gap:     {overfitting_gap:+.1%}")
    print()
    
    # Cross-validation (note: using standard cross-validation without weights for comparison)
    print("Running 5-fold cross-validation...")
    print("  (Note: This uses standard CV without weights for unbiased generalization estimate)")
    cv_scores = cross_val_score(dt, X, y, cv=min(5, len(X)//10), scoring='accuracy')
    print(f"Cross-validation scores: {cv_scores}")
    print(f"Mean CV accuracy: {cv_scores.mean():.1%} (+/- {cv_scores.std()*2:.1%})")
    print()
    
    # Weighted accuracy metrics (more relevant for our goal!)
    print("Weighted Accuracy Metrics (prioritizing large performance gaps):")
    weighted_train_acc = accuracy_score(y_train, y_train_pred, sample_weight=weights_train)
    weighted_test_acc = accuracy_score(y_test, y_test_pred, sample_weight=weights_test)
    print(f"  Weighted Training Accuracy:   {weighted_train_acc:.1%}")
    print(f"  Weighted Test Accuracy:       {weighted_test_acc:.1%}")
    print(f"  (These metrics give more weight to cases with large performance differences)")
    print()
    
    # Confusion matrix
    print("Confusion Matrix (Test Set):")
    print("  (Shows how well the model predicts hash vs sort wins)")
    print("-" * 80)
    cm = confusion_matrix(y_test, y_test_pred)
    
    header = "Actual \\ Predicted"
    print(f"{header:<20s} {'hash (class=0)':>20s} {'sort (class=1)':>20s}")
    print("-" * 62)
    print(f"{'hash_object':<20s} {cm[0][0]:>20d} {cm[0][1]:>20d}")
    print(f"{'sort_object_post':<20s} {cm[1][0]:>20d} {cm[1][1]:>20d}")
    print()
    
    # Classification report
    print("Classification Report (Test Set):")
    print(classification_report(y_test, y_test_pred, 
                                target_names=['hash_object', 'sort_object_post'],
                                zero_division=0))
    print()
    
    # Feature importance
    print("Top 10 Most Important Features:")
    feature_importance = pd.DataFrame({
        'feature': X.columns,
        'importance': dt.feature_importances_
    }).sort_values('importance', ascending=False)
    
    for idx, row in feature_importance.head(10).iterrows():
        if row['importance'] > 0.01:
            print(f"  {row['feature']:30s}: {row['importance']:.4f}")
    print()
    
    # Feature Distribution Analysis (to help interpret the tree)
    print("="*80)
    print("FEATURE DISTRIBUTIONS (Context for Understanding the Tree)")
    print("="*80)
    print()
    print("This shows the range of values in your data to help interpret tree splits.")
    print("For example, if a split is 'ProbeRows > 24.5M', you can see if that's")
    print("a high, medium, or low value compared to your typical data.")
    print()
    
    # Select key features that are likely to appear in the tree
    key_features = [
        'NumKeyColumns',
        'BuildRows', 'ProbeRows', 'TotalRows',
        'BuildDistinctKeys', 'ProbeDistinctKeys',
        'BuildCardinalityPct', 'ProbeCardinalityPct',
        'BuildProbeRatio', 'BuildProbeCardinalityRatio',
        'MaxCardinality', 'MinCardinality',
        'BuildAvgKeyBytes', 'ProbeAvgKeyBytes'
    ]
    
    for feature in key_features:
        if feature in X.columns:
            values = X[feature].values
            
            # Format based on feature type
            if 'Pct' in feature:
                # Percentage features (0-1 range)
                print(f"{feature:25s}: min={values.min()*100:>6.2f}%  "
                      f"p25={np.percentile(values, 25)*100:>6.2f}%  "
                      f"median={np.median(values)*100:>6.2f}%  "
                      f"p75={np.percentile(values, 75)*100:>6.2f}%  "
                      f"max={values.max()*100:>6.2f}%")
            elif 'Rows' in feature or 'Keys' in feature:
                # Count features (show with comma separators and scale)
                def format_count(x):
                    if x >= 1_000_000:
                        return f"{x/1_000_000:.1f}M"
                    elif x >= 1_000:
                        return f"{x/1_000:.1f}K"
                    else:
                        return f"{x:.0f}"
                
                print(f"{feature:25s}: min={format_count(values.min()):>7s}  "
                      f"p25={format_count(np.percentile(values, 25)):>7s}  "
                      f"median={format_count(np.median(values)):>7s}  "
                      f"p75={format_count(np.percentile(values, 75)):>7s}  "
                      f"max={format_count(values.max()):>7s}")
            elif 'Ratio' in feature:
                # Ratio features
                print(f"{feature:25s}: min={values.min():>6.3f}  "
                      f"p25={np.percentile(values, 25):>6.3f}  "
                      f"median={np.median(values):>6.3f}  "
                      f"p75={np.percentile(values, 75):>6.3f}  "
                      f"max={values.max():>6.3f}")
            else:
                # Generic numeric features
                print(f"{feature:25s}: min={values.min():>7.1f}  "
                      f"p25={np.percentile(values, 25):>7.1f}  "
                      f"median={np.median(values):>7.1f}  "
                      f"p75={np.percentile(values, 75):>7.1f}  "
                      f"max={values.max():>7.1f}")
    
    print()
    print("Interpretation Guide:")
    print("  • p25 (25th percentile): 25% of your data is below this value")
    print("  • median (50th percentile): Half your data is below, half above")
    print("  • p75 (75th percentile): 75% of your data is below this value")
    print("  • A split near p75 = high-end split (affects 25% of cases)")
    print("  • A split near p25 = low-end split (affects 75% of cases)")
    print("  • A split near median = middle split (affects 50% each way)")
    print()
    
    # Decision tree rules
    print("="*80)
    print("DECISION TREE RULES")
    print("="*80)
    print()
    print("=" * 80)
    print("LEGEND - How to Read the Tree:")
    print("=" * 80)
    print()
    print("CLASS VALUES (what the model predicts):")
    print("  • class = 0  →  hash_object wins (use hash-based join)")
    print("  • class = 1  →  sort_object_post wins (use sort-based join)")
    print()
    print("KEY TYPE SCORES (BuildKeyTypeScore / ProbeKeyTypeScore / MaxKeyTypeScore):")
    print("  Uses BIT FLAGS based on bit width:")
    print("  • 1 (0b0001)  →  32-bit only  (int, decimal(9,2))")
    print("  • 2 (0b0010)  →  64-bit only  (long, decimal(18,2))")
    print("  • 4 (0b0100)  →  128-bit only (decimal(38,2) high precision)")
    print("  • 8 (0b1000)  →  variable-width (string)")
    print()
    print("  Composite keys use bitwise OR (preserves all bit widths):")
    print("  • 3 (1|2)     →  32-bit + 64-bit")
    print("  • 9 (1|8)     →  32-bit + string")
    print("  • 10 (2|8)    →  64-bit + string")
    print("  • 11 (1|2|8)  →  32-bit + 64-bit + string")
    print("  • Higher scores = more bit widths or variable-width types")
    print()
    print("OTHER KEY FEATURES:")
    print("  • BuildRows:             Row count of build side (smaller table)")
    print("  • ProbeRows:             Row count of probe side (larger table)")
    print("  • BuildDistinctKeys:     Number of unique join keys in build table")
    print("  • ProbeDistinctKeys:     Number of unique join keys in probe table")
    print("  • BuildCardinalityPct:   (BuildDistinctKeys / BuildRows) × 100")
    print("                           How unique are the build keys?")
    print("                           100% = all unique, <10% = high duplication")
    print("                           Low values (<5%) often favor sort")
    print("  • ProbeCardinalityPct:   (ProbeDistinctKeys / ProbeRows) × 100")
    print("  • BuildProbeCardinalityRatio: BuildCardinalityPct / ProbeCardinalityPct")
    print("  • MaxCardinality:        max(BuildCardinalityPct, ProbeCardinalityPct)")
    print("  • MinCardinality:        min(BuildCardinalityPct, ProbeCardinalityPct)")
    print("  • NumKeyColumns:         Number of key columns (1, 2, or 3)")
    print("  • MixedKeys:             1 if multi-column key with DIFFERENT types (heterogeneous)")
    print("                           0 if single column OR all columns same type (homogeneous)")
    print("                           Examples: (int,int)=0, (int,string)=1")
    print()
    print("-" * 80)
    print()
    
    tree_rules = export_text(dt, feature_names=list(X.columns))
    
    # POST-PROCESS: Collapse redundant branches where all paths lead to same result
    def collapse_redundant_branches(tree_text):
        """
        Recursively determine the result class for each node.
        If a decision node's all possible paths lead to same class, collapse it.
        """
        
        def get_tree_depth(line):
            """Calculate tree depth by counting |--- occurrences before the actual condition."""
            # Count pipes that indicate tree structure (|   or |---)
            depth = 0
            i = 0
            while i < len(line):
                if line[i:i+4] == '|   ':
                    depth += 1
                    i += 4
                elif line[i:i+4] == '|---':
                    # This is the actual node marker, stop counting
                    break
                else:
                    i += 1
            return depth
        
        def get_all_leaf_classes(lines, start_idx):
            """Get all leaf classes that are descendants of the node at start_idx."""
            if start_idx >= len(lines):
                return []
            
            line = lines[start_idx]
            if not line.strip():
                return []
            
            # If this is a leaf, return its class
            if 'class: 0' in line:
                return [0]
            elif 'class: 1' in line:
                return [1]
            
            # This is a decision node - find all its children
            depth = get_tree_depth(line)
            classes = []
            
            j = start_idx + 1
            while j < len(lines):
                child_line = lines[j]
                if not child_line.strip():
                    j += 1
                    continue
                
                child_depth = get_tree_depth(child_line)
                
                # If back to same or shallower, done with this node's descendants
                if child_depth <= depth:
                    break
                
                # If this is a direct child (one level deeper)
                if child_depth == depth + 1 and '|---' in child_line:
                    # Recursively get classes from this child
                    child_classes = get_all_leaf_classes(lines, j)
                    classes.extend(child_classes)
                
                j += 1
            
            return classes
        
        max_iterations = 20
        for iteration in range(max_iterations):
            lines = tree_text.split('\n')
            changed = False
            
            # Work backwards (bottom-up)
            i = len(lines) - 1
            while i >= 0:
                line = lines[i]
                
                if not line.strip() or 'class:' in line:
                    i -= 1
                    continue
                
                # For each decision node, check if all descendants are same class
                if '|---' in line:
                    descendant_classes = get_all_leaf_classes(lines, i)
                    unique_classes = set(descendant_classes)
                    
                    # If all descendants lead to the same class, collapse!
                    if len(unique_classes) == 1 and len(descendant_classes) > 0:
                        the_class = list(unique_classes)[0]
                        depth = get_tree_depth(line)
                        
                        decision = line.strip().split('|---')[1].strip() if '|---' in line else '?'
                        
                        # Keep the parent decision node (lines[i] stays as-is)
                        # Delete all children and replace with a single class leaf
                        
                        # First, delete all existing children
                        j = i + 1
                        while j < len(lines):
                            child_line = lines[j]
                            if not child_line.strip():
                                j += 1
                                continue
                            
                            child_depth = get_tree_depth(child_line)
                            if child_depth <= depth:
                                break
                            
                            lines[j] = ''
                            j += 1
                        
                        # Insert a single class leaf as the only child
                        child_prefix = '|   ' * (depth + 1)
                        class_line = child_prefix + f'|--- class: {the_class}'
                        lines.insert(i + 1, class_line)
                        
                        changed = True
                
                i -= 1
            
            tree_text = '\n'.join([l for l in lines if l.strip()])
            
            if not changed:
                print(f"    Collapsed tree in {iteration + 1} iteration(s)")
                break
        
        return tree_text
    
    print("  Collapsing redundant branches...")
    newline = '\n'
    original_leaf_count = len([l for l in tree_rules.split(newline) if 'class:' in l])
    print(f"    Original tree has {original_leaf_count} leaf nodes")
    tree_rules = collapse_redundant_branches(tree_rules)
    simplified_leaf_count = len([l for l in tree_rules.split(newline) if 'class:' in l])
    print(f"    Simplified tree has {simplified_leaf_count} leaf nodes")
    
    # Add inline annotations for better readability
    # Annotate class values
    tree_rules = tree_rules.replace('class: 0', 'class: 0 → HASH wins (use hash_object)')
    tree_rules = tree_rules.replace('class: 1', 'class: 1 → SORT wins (use sort_object_post)')
    
    # Helper function to determine where a value falls in the distribution
    def get_percentile_context(feature_name, value, X_data):
        """Return a string describing where a split value falls in the distribution."""
        if feature_name not in X_data.columns:
            return ""
        
        values = X_data[feature_name].values
        percentile = (values < value).mean() * 100
        
        if percentile < 15:
            return f" [LOW: {percentile:.0f}th %ile]"
        elif percentile < 40:
            return f" [low-mid: {percentile:.0f}th %ile]"
        elif percentile < 60:
            return f" [MID: {percentile:.0f}th %ile]"
        elif percentile < 85:
            return f" [mid-high: {percentile:.0f}th %ile]"
        else:
            return f" [HIGH: {percentile:.0f}th %ile]"
    
    # Annotate key type scores in conditions
    import re  # For parsing tree rule patterns
    
    lines = tree_rules.split('\n')
    annotated_lines = []
    for line in lines:
        annotated_line = line
        
        # Annotate MaxKeyTypeScore, BuildKeyTypeScore, ProbeKeyTypeScore (bit width flags)
        if 'MaxKeyTypeScore' in line or 'BuildKeyTypeScore' in line or 'ProbeKeyTypeScore' in line:
            if '<= 1.50' in line or '> 1.50' in line:
                annotated_line += '  # 1=32-bit only'
            elif '<= 2.50' in line or '> 2.50' in line:
                annotated_line += '  # ≤2: 32-bit(1) or 64-bit(2) only'
            elif '<= 3.50' in line or '> 3.50' in line:
                annotated_line += '  # 3=32+64-bit, 4=128-bit'
            elif '<= 4.50' in line or '> 4.50' in line:
                annotated_line += '  # ≤4: fixed-width only (no strings)'
            elif '<= 7.50' in line or '> 7.50' in line:
                annotated_line += '  # checking for 128-bit or string combinations'
            elif '<= 8.50' in line or '> 8.50' in line:
                annotated_line += '  # ≤8: string only, >8: mixed with string'
            elif '<= 9.50' in line or '> 9.50' in line:
                annotated_line += '  # 9=32-bit+string, 10=64-bit+string'
            elif '<= 10.50' in line or '> 10.50' in line:
                annotated_line += '  # checking for 3+ bit widths'
            elif '11' in line:
                annotated_line += '  # 11+=32+64-bit+string (3+ bit widths)'
        
        # Annotate cardinality percentages
        if 'BuildCardinalityPct' in line:
            if '<= 0.00' in line or '> 0.00' in line:
                annotated_line += '  # 0% cardinality'
            elif '<= 0.01' in line or '> 0.01' in line:
                annotated_line += '  # 1% cardinality (very low - sort often wins)'
            elif '<= 0.02' in line or '> 0.02' in line:
                annotated_line += '  # 2% cardinality'
            elif '<= 0.05' in line or '> 0.05' in line:
                annotated_line += '  # 5% cardinality'
            elif '<= 0.10' in line or '> 0.10' in line:
                annotated_line += '  # 10% cardinality'
            elif '<= 0.50' in line or '> 0.50' in line:
                annotated_line += '  # 50% cardinality (high)'
        
        # Annotate mixed keys
        if 'MixedKeys' in line:
            if '<= 0.50' in line:
                annotated_line += '  # homogeneous (single column OR all same type like int,int)'
            elif '> 0.50' in line:
                annotated_line += '  # heterogeneous (multi-column with different types like int,string)'
        
        # Add percentile context for row counts and ratios
        # Extract feature name and threshold from line
        # Pattern: "feature_name <= threshold" or "feature_name > threshold"
        match = re.search(r'\|--- (\w+)\s*([<>]=?)\s*([\d.]+)', line)
        if match:
            feature_name = match.group(1)
            operator = match.group(2)
            threshold = float(match.group(3))
            
            # Add context for key features
            context_features = ['BuildProbeRatio', 'ProbeRows', 'BuildRows', 'TotalRows', 
                              'BuildDistinctKeys', 'ProbeDistinctKeys',
                              'BuildCardinalityPct', 'ProbeCardinalityPct',
                              'BuildProbeCardinalityRatio', 'MaxCardinality', 'MinCardinality',
                              'NumKeyColumns', 'BuildAvgKeyBytes', 'ProbeAvgKeyBytes']
            
            if feature_name in context_features:
                context = get_percentile_context(feature_name, threshold, X)
                
                # Format the threshold for display
                if 'Rows' in feature_name or 'Keys' in feature_name:
                    if threshold >= 1_000_000:
                        threshold_str = f" ({threshold/1_000_000:.1f}M)"
                    elif threshold >= 1_000:
                        threshold_str = f" ({threshold/1_000:.1f}K)"
                    else:
                        threshold_str = ""
                    annotated_line += threshold_str + context
                elif 'Pct' in feature_name:
                    annotated_line += f" ({threshold*100:.1f}%)" + context
                else:
                    annotated_line += context
        
        annotated_lines.append(annotated_line)
    
    tree_rules = '\n'.join(annotated_lines)
    print(tree_rules)
    print()
    
    # Timing Analysis - Show real-world impact
    print("=" * 80)
    print("TIMING ANALYSIS - Real-World Performance Impact")
    print("=" * 80)
    print()
    print("Compares different strategies to show the value of the model:")
    print("  1. Always HASH:    What if we always used hash_object?")
    print("  2. Always SORT:    What if we always used sort_object_post?")
    print("  3. Perfect Oracle: What if we always picked the fastest (best possible)?")
    print("  4. Model:          What does our trained model recommend?")
    print()
    
    # Get actual timings for both strategies from the original dataframe
    # Need to merge back to get both hash and sort times
    def compute_timing_comparison(X_subset, y_subset, y_pred_subset, indices):
        """Compute timing stats for a subset of data."""
        subset_df = best_df.loc[indices].copy()
        
        # Get hash and sort times (already have BestTime and AlternativeTime)
        hash_times = []
        sort_times = []
        oracle_times = []
        model_times = []
        
        # Reset index to align with y_pred_subset indices
        subset_df_reset = subset_df.reset_index(drop=True)
        
        for idx in range(len(subset_df_reset)):
            row = subset_df_reset.iloc[idx]
            if row['BestStrategy'] == 'hash_object':
                hash_time = row['BestTime']
                sort_time = row['AlternativeTime']
            else:
                sort_time = row['BestTime']
                hash_time = row['AlternativeTime']
            
            hash_times.append(hash_time)
            sort_times.append(sort_time)
            oracle_times.append(min(hash_time, sort_time))
            
            # Model prediction: 0=hash, 1=sort
            if y_pred_subset[idx] == 0:
                model_times.append(hash_time)
            else:
                model_times.append(sort_time)
        
        return {
            'always_hash': {'times': hash_times, 'total': sum(hash_times), 'avg': np.mean(hash_times)},
            'always_sort': {'times': sort_times, 'total': sum(sort_times), 'avg': np.mean(sort_times)},
            'oracle': {'times': oracle_times, 'total': sum(oracle_times), 'avg': np.mean(oracle_times)},
            'model': {'times': model_times, 'total': sum(model_times), 'avg': np.mean(model_times)}
        }
    
    # Compute for test set
    test_indices = X_test.index.tolist()
    test_timings = compute_timing_comparison(X_test, y_test, y_test_pred, test_indices)
    
    # Compute for all data
    y_all_pred = dt.predict(X)
    all_indices = X.index.tolist()
    all_timings = compute_timing_comparison(X, y, y_all_pred, all_indices)
    
    def print_timing_results(timings, n_samples, label):
        """Print timing results in a nice table."""
        print(f"\n{label} ({n_samples} joins):")
        print("-" * 80)
        
        always_hash = timings['always_hash']
        always_sort = timings['always_sort']
        oracle = timings['oracle']
        model = timings['model']
        
        # Calculate savings
        oracle_savings_vs_hash = ((always_hash['total'] - oracle['total']) / always_hash['total']) * 100
        oracle_savings_vs_sort = ((always_sort['total'] - oracle['total']) / always_sort['total']) * 100
        model_savings_vs_hash = ((always_hash['total'] - model['total']) / always_hash['total']) * 100
        model_savings_vs_sort = ((always_sort['total'] - model['total']) / always_sort['total']) * 100
        
        # Calculate model efficiency (how close to oracle)
        model_efficiency = (oracle['total'] / model['total']) * 100 if model['total'] > 0 else 0
        model_overhead = ((model['total'] - oracle['total']) / oracle['total']) * 100 if oracle['total'] > 0 else 0
        
        print(f"{'Strategy':<20s} {'Total Time (ms)':>18s} {'Avg Time (ms)':>15s} {'vs Always-Hash':>15s} {'vs Always-Sort':>15s}")
        print("-" * 85)
        print(f"{'1. Always HASH':<20s} {always_hash['total']:>18,.1f} {always_hash['avg']:>15,.1f} {'baseline':>15s} {oracle_savings_vs_sort:>14.1f}%")
        print(f"{'2. Always SORT':<20s} {always_sort['total']:>18,.1f} {always_sort['avg']:>15,.1f} {-oracle_savings_vs_hash:>14.1f}% {'baseline':>15s}")
        print(f"{'3. Perfect Oracle':<20s} {oracle['total']:>18,.1f} {oracle['avg']:>15,.1f} {oracle_savings_vs_hash:>14.1f}% {oracle_savings_vs_sort:>14.1f}%")
        print(f"{'4. Model':<20s} {model['total']:>18,.1f} {model['avg']:>15,.1f} {model_savings_vs_hash:>14.1f}% {model_savings_vs_sort:>14.1f}%")
        print()
        print(f"Model Efficiency: {model_efficiency:.1f}% of Oracle (overhead: {model_overhead:+.1f}%)")
        
        # Show which baseline is better
        if always_hash['total'] < always_sort['total']:
            best_baseline = 'HASH'
            best_baseline_time = always_hash['total']
            savings = ((best_baseline_time - model['total']) / best_baseline_time) * 100
        else:
            best_baseline = 'SORT'
            best_baseline_time = always_sort['total']
            savings = ((best_baseline_time - model['total']) / best_baseline_time) * 100
        
        print(f"Best naive strategy: Always {best_baseline}")
        print(f"Model saves {savings:.1f}% vs best naive strategy")
    
    print_timing_results(test_timings, len(X_test), "TEST SET")
    print_timing_results(all_timings, len(X), "ALL DATA")
    
    print()
    
    # HIGH-IMPACT CASE ANALYSIS
    # Focus on cases where ABSOLUTE TIME difference is large (>50ms)
    print("=" * 80)
    print("HIGH-IMPACT CASE ANALYSIS (by absolute time)")
    print("=" * 80)
    print()
    print("Focusing on cases where the ABSOLUTE TIME difference is LARGE (>50ms)")
    print("These are the pathologically bad cases we want to avoid!")
    print()
    
    # Identify high-impact cases in test set based on ABSOLUTE TIME
    test_df = best_df.loc[X_test.index]
    test_time_diff_ms = (test_df['AlternativeTime'] - test_df['BestTime']).values
    high_impact_mask = test_time_diff_ms > HIGH_IMPACT_THRESHOLD_MS
    high_impact_count = high_impact_mask.sum()
    
    if high_impact_count > 0:
        print(f"High-impact cases (>{HIGH_IMPACT_THRESHOLD_MS}ms) in test set: {high_impact_count} / {len(X_test)} ({high_impact_count/len(X_test)*100:.1f}%)")
        high_impact_time_diffs = test_time_diff_ms[high_impact_mask]
        print(f"  Mean time difference: {high_impact_time_diffs.mean():.2f} ms")
        print(f"  Max time difference:  {high_impact_time_diffs.max():.2f} ms")
        print()
        
        # Get predictions for high-impact cases
        high_impact_indices = test_df[high_impact_mask].index
        high_impact_X = X_test.loc[high_impact_indices]
        high_impact_y = y_test.loc[high_impact_indices]
        high_impact_pred = dt.predict(high_impact_X)
        
        # Calculate accuracy on high-impact cases
        high_impact_accuracy = accuracy_score(high_impact_y, high_impact_pred)
        print(f"Model accuracy on HIGH-IMPACT cases: {high_impact_accuracy:.1%}")
        print(f"Model accuracy on ALL cases:         {test_accuracy:.1%}")
        print()
        
        # Show confusion matrix for high-impact cases
        if high_impact_count > 1:
            from sklearn.metrics import confusion_matrix as cm
            hi_cm = cm(high_impact_y, high_impact_pred)
            print("High-Impact Cases Confusion Matrix:")
            print(f"  True Hash / Pred Hash: {hi_cm[0][0] if len(hi_cm) > 0 and len(hi_cm[0]) > 0 else 0}")
            true_hash_pred_sort = hi_cm[0][1] if len(hi_cm) > 0 and len(hi_cm[0]) > 1 else 0
            print(f"  True Hash / Pred Sort: {true_hash_pred_sort} ← BAD (chose sort when hash was much faster)")
            if len(hi_cm) > 1:
                true_sort_pred_hash = hi_cm[1][0] if len(hi_cm[1]) > 0 else 0
                print(f"  True Sort / Pred Hash: {true_sort_pred_hash} ← BAD (chose hash when sort was much faster)")
                print(f"  True Sort / Pred Sort: {hi_cm[1][1] if len(hi_cm[1]) > 1 else 0}")
            print()
        
        # Calculate cost of errors on high-impact cases (in ABSOLUTE TIME)
        high_impact_test_df = test_df.loc[high_impact_indices].reset_index(drop=True)
        high_impact_pred_reset = pd.Series(high_impact_pred)
        
        error_costs_ms = []
        error_costs_pct = []
        error_details = []  # Store details of each error
        
        for idx in range(len(high_impact_test_df)):
            row = high_impact_test_df.iloc[idx]
            pred = high_impact_pred_reset.iloc[idx]
            actual = high_impact_y.iloc[idx]
            
            if pred != actual:
                # Wrong prediction on a high-impact case!
                if row['BestStrategy'] == 'hash_object':
                    hash_time = row['BestTime']
                    sort_time = row['AlternativeTime']
                else:
                    sort_time = row['BestTime']
                    hash_time = row['AlternativeTime']
                
                if pred == 0:  # Predicted hash
                    chosen_time = hash_time
                    best_time = sort_time
                    pred_strategy = 'hash_object'
                else:  # Predicted sort
                    chosen_time = sort_time
                    best_time = hash_time
                    pred_strategy = 'sort_object_post'
                
                cost_ms = chosen_time - best_time
                cost_pct = (cost_ms / best_time) * 100
                error_costs_ms.append(cost_ms)
                error_costs_pct.append(cost_pct)
                
                # Store error details
                error_details.append({
                    'test_name': row.get('TestName', 'unknown'),
                    'best_strategy': row['BestStrategy'],
                    'predicted_strategy': pred_strategy,
                    'best_time': best_time,
                    'chosen_time': chosen_time,
                    'cost_ms': cost_ms,
                    'cost_pct': cost_pct,
                    'time_diff_ms': row['AlternativeTime'] - row['BestTime'],
                    'build_rows': row.get('LeftRows', 0) if row.get('LeftRows', 0) <= row.get('RightRows', 0) else row.get('RightRows', 0),
                    'probe_rows': row.get('RightRows', 0) if row.get('LeftRows', 0) <= row.get('RightRows', 0) else row.get('LeftRows', 0),
                    'build_card': row.get('LeftCardinalityPct', 0) if row.get('LeftRows', 0) <= row.get('RightRows', 0) else row.get('RightCardinalityPct', 0),
                    'build_key_type': row.get('LeftKeyType', 'unknown') if row.get('LeftRows', 0) <= row.get('RightRows', 0) else row.get('RightKeyType', 'unknown'),
                    'probe_key_type': row.get('RightKeyType', 'unknown') if row.get('LeftRows', 0) <= row.get('RightRows', 0) else row.get('LeftKeyType', 'unknown'),
                    'num_key_cols': row.get('NumKeyColumns', 0)
                })
        
        if error_costs_ms:
            print(f"Errors on high-impact cases: {len(error_costs_ms)} / {high_impact_count}")
            print(f"  Average cost of errors: {np.mean(error_costs_ms):.2f} ms ({np.mean(error_costs_pct):.1f}% slower)")
            print(f"  Max cost of error:      {np.max(error_costs_ms):.2f} ms ({np.max(error_costs_pct):.1f}% slower)")
            print(f"  Total cost of errors:   {np.sum(error_costs_ms):.2f} ms")
            print(f"  These are the bad cases we're trying to avoid!")
            print()
            
            # Print details of each error case
            print("Detailed Error Cases (High-Impact with Wrong Predictions):")
            print()
            # Sort by cost (most expensive errors first)
            error_details_sorted = sorted(error_details, key=lambda x: x['cost_ms'], reverse=True)
            
            print(f"{'Test Name':<30s} {'Best':<12s} {'Predicted':<12s} {'Cost (ms)':>12s} {'Cost %':>10s} {'B Rows':>12s} {'P Rows':>12s} {'B Card%':>10s} {'B KeyType':<20s} {'P KeyType':<20s}")
            print("-" * 160)
            
            for err in error_details_sorted:
                test_name = err['test_name'][:28] if len(err['test_name']) > 28 else err['test_name']
                build_key_type = err['build_key_type'][:18] if len(str(err['build_key_type'])) > 18 else str(err['build_key_type'])
                probe_key_type = err['probe_key_type'][:18] if len(str(err['probe_key_type'])) > 18 else str(err['probe_key_type'])
                print(f"{test_name:<30s} {err['best_strategy']:<12s} {err['predicted_strategy']:<12s} "
                      f"{err['cost_ms']:>12.2f} {err['cost_pct']:>9.1f}% "
                      f"{err['build_rows']:>12,.0f} {err['probe_rows']:>12,.0f} "
                      f"{err['build_card']*100:>9.2f}% {build_key_type:<20s} {probe_key_type:<20s}")
            print()
            
            # Group errors by key type combination to see patterns
            print("Error Patterns by Key Type Combination:")
            type_error_counts = {}
            for err in error_details:
                build_type = err['build_key_type']
                probe_type = err['probe_key_type']
                num_cols = err['num_key_cols']
                key = f"{build_type}|{probe_type}|{num_cols}cols"
                if key not in type_error_counts:
                    type_error_counts[key] = {
                        'count': 0,
                        'total_cost_ms': 0.0,
                        'avg_cost_ms': 0.0,
                        'max_cost_ms': 0.0
                    }
                type_error_counts[key]['count'] += 1
                type_error_counts[key]['total_cost_ms'] += err['cost_ms']
                type_error_counts[key]['max_cost_ms'] = max(type_error_counts[key]['max_cost_ms'], err['cost_ms'])
            
            for key, stats in type_error_counts.items():
                stats['avg_cost_ms'] = stats['total_cost_ms'] / stats['count']
            
            # Sort by total cost
            type_errors_sorted = sorted(type_error_counts.items(), key=lambda x: x[1]['total_cost_ms'], reverse=True)
            
            print(f"{'Key Type Combo':<50s} {'Errors':>8s} {'Total Cost (ms)':>18s} {'Avg Cost (ms)':>15s} {'Max Cost (ms)':>15s}")
            print("-" * 110)
            for key, stats in type_errors_sorted:
                key_display = key[:48] if len(key) > 48 else key
                print(f"{key_display:<50s} {stats['count']:>8d} {stats['total_cost_ms']:>18.2f} "
                      f"{stats['avg_cost_ms']:>15.2f} {stats['max_cost_ms']:>15.2f}")
            print()
        else:
            print(f"No errors on high-impact cases! Model is perfect on cases >{HIGH_IMPACT_THRESHOLD_MS}ms")
            print()
    else:
        print(f"No high-impact cases (>{HIGH_IMPACT_THRESHOLD_MS}ms difference) in test set")
        print()
    
    print()
    
    # Model quality assessment
    print("="*80)
    print("MODEL QUALITY ASSESSMENT")
    print("="*80)
    print()
    
    quality_issues = []
    quality_warnings = []
    
    if test_accuracy < MIN_TEST_ACCURACY:
        quality_issues.append(f"Low test accuracy ({test_accuracy:.1%} < {MIN_TEST_ACCURACY:.1%})")
    else:
        print(f"  ✓ Good test accuracy ({test_accuracy:.1%} >= {MIN_TEST_ACCURACY:.1%})")
    
    if overfitting_gap > MAX_OVERFITTING_GAP:
        quality_issues.append(f"High overfitting gap ({overfitting_gap:.1%} > {MAX_OVERFITTING_GAP:.1%})")
    else:
        print(f"  ✓ Acceptable overfitting ({overfitting_gap:.1%} <= {MAX_OVERFITTING_GAP:.1%})")
    
    if cv_scores.mean() < MIN_CROSS_VAL_SCORE:
        quality_issues.append(f"Low cross-validation score ({cv_scores.mean():.1%} < {MIN_CROSS_VAL_SCORE:.1%})")
    else:
        print(f"  ✓ Good cross-validation ({cv_scores.mean():.1%} >= {MIN_CROSS_VAL_SCORE:.1%})")
    
    print()
    
    return dt, X, y, train_accuracy, test_accuracy, cv_scores.mean(), quality_issues, quality_warnings

# ============================================================================
# Refinement Region Identification
# ============================================================================

def identify_refinement_regions(best_df, model, X):
    """
    Identify regions where we need more data to improve model.
    IMPROVED: Now uses model uncertainty and prediction errors to guide exploration.
    Focus on:
    1. Low cardinality regions (<5%) where sort typically wins
    2. Boundary regions where model is uncertain (NEW)
    3. Regions where model makes prediction errors (NEW)
    4. Undersampled key type / column count combinations (IMPROVED)
    """
    print("="*80)
    print("IDENTIFYING REFINEMENT REGIONS")
    print("="*80)
    print()
    
    features = create_build_probe_features(best_df)
    
    # Get model predictions and probabilities
    predictions = model.predict(X)
    probabilities = model.predict_proba(X)
    max_probs = probabilities.max(axis=1)  # Confidence of prediction
    
    # Find uncertain predictions (low confidence)
    uncertain_threshold = 0.7
    uncertain_mask = max_probs < uncertain_threshold
    
    # Find prediction errors (where model predicts wrong strategy)
    actual_labels = (best_df['BestStrategy'] == 'sort_object_post').astype(int).values
    prediction_errors = (predictions != actual_labels)
    
    print(f"Model confidence analysis:")
    print(f"  High confidence (>70%): {(~uncertain_mask).sum()} samples")
    print(f"  Low confidence (<70%):  {uncertain_mask.sum()} samples")
    print()
    
    print(f"Model error analysis:")
    print(f"  Correct predictions: {(~prediction_errors).sum()} samples")
    print(f"  Incorrect predictions: {prediction_errors.sum()} samples ({prediction_errors.sum()/len(predictions)*100:.1f}%)")
    print()
    
    # Analyze where sort wins (or should win)
    sort_wins = best_df['BestStrategy'] == 'sort_object_post'
    
    # NEW: Analyze uncertain regions by cardinality
    print("Model uncertainty by cardinality:")
    cardinality_bins = [
        ("<0.5%", 0.0, 0.005),
        ("0.5-1%", 0.005, 0.01),
        ("1-2%", 0.01, 0.02),
        ("2-5%", 0.02, 0.05),
        ("5-10%", 0.05, 0.10),
        (">10%", 0.10, 1.0)
    ]
    
    target_regions = []
    
    # Strategy 0: Target low max key count gaps (NEW)
    print("Max key count gap analysis:")
    if 'BuildMaxKeyCount' in best_df.columns:
        features_with_max = create_build_probe_features(best_df)
        
        # Key types are the same for left and right (join requirement), so just use LeftKeyType
        if 'BuildKeyType' not in best_df.columns:
            best_df['BuildKeyType'] = best_df['LeftKeyType']
        
        # Categorize by key type
        def categorize_key_type(key_type_str):
            """Categorize key type for analysis."""
            if pd.isna(key_type_str) or key_type_str == '':
                return 'unknown'
            key_type_str = str(key_type_str)
            # Check if it's a composite key (comma outside parentheses)
            if ',' in key_type_str:
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
                    return 'single_string' if 'string' in key_type_str.lower() else 'single_fixed'
            else:
                return 'single_string' if 'string' in key_type_str.lower() else 'single_fixed'
        
        best_df['KeyTypeCategory'] = best_df['BuildKeyType'].apply(categorize_key_type)
        
        # Analyze max key counts by category
        for category in ['single_string', 'multiple']:
            cat_df = best_df[best_df['KeyTypeCategory'] == category]
            if len(cat_df) == 0:
                continue
            
            # Check max key count distribution
            max_key_counts = cat_df['BuildMaxKeyCount'].dropna()
            if len(max_key_counts) == 0:
                continue
            
            median_max = max_key_counts.median()
            p75_max = max_key_counts.quantile(0.75)
            p90_max = max_key_counts.quantile(0.90)
            
            # Compare to fixed-width baseline
            fixed_df = best_df[best_df['KeyTypeCategory'] == 'single_fixed']
            if len(fixed_df) > 0:
                fixed_max = fixed_df['BuildMaxKeyCount'].dropna()
                if len(fixed_max) > 0:
                    fixed_median = fixed_max.median()
                    fixed_p90 = fixed_max.quantile(0.90)
                    
                    # If strings/multi-keys have significantly lower max counts, target them
                    if median_max < fixed_median * 0.5 or p90_max < fixed_p90 * 0.3:
                        print(f"  {category}: Median max={median_max:.0f}, P90={p90_max:.0f}")
                        print(f"    Fixed-width: Median={fixed_median:.0f}, P90={fixed_p90:.0f}")
                        print(f"    → Targeting low max key count gaps")
                        
                        # Get typical characteristics of low max key count cases
                        low_max_cases = cat_df[cat_df['BuildMaxKeyCount'] < p75_max]
                        if len(low_max_cases) > 0:
                            # Target regions with:
                            # 1. Higher memory targets (more rows)
                            # 2. Lower cardinality (fewer distinct keys = more duplicates)
                            # 3. Skewed distributions (zipf/gaussian create higher max counts)
                            
                            build_rows_min = int(low_max_cases['BuildRows'].quantile(0.25))
                            build_rows_max = int(low_max_cases['BuildRows'].quantile(0.75)) * 3  # 3x to get more rows
                            probe_rows_min = int(low_max_cases['ProbeRows'].quantile(0.25))
                            probe_rows_max = int(low_max_cases['ProbeRows'].quantile(0.75)) * 3
                            
                            # Target lower cardinality for more duplicates
                            card_min = 0.001
                            card_max = min(0.05, low_max_cases['BuildCardinalityPct'].quantile(0.75))
                            
                            # Determine key types to target
                            if category == 'single_string':
                                key_types = ['string']
                                num_cols = [1]
                            else:  # multiple
                                # Get typical multi-key column counts
                                num_cols_list = low_max_cases['NumKeyColumns'].unique().tolist()
                                num_cols = num_cols_list if len(num_cols_list) > 0 else [2, 3]
                                # Target mixed key types that include strings
                                key_types = ['string', 'int,string', 'string,long', 'string,int,long']
                            
                            target_regions.append({
                                'buildCardinalityMin': card_min,
                                'buildCardinalityMax': card_max,
                                'probeCardinalityMin': 0.0,
                                'probeCardinalityMax': 1.0,
                                'buildRowsMin': build_rows_min,
                                'buildRowsMax': build_rows_max,
                                'probeRowsMin': probe_rows_min,
                                'probeRowsMax': probe_rows_max,
                                'keyTypes': key_types,
                                'numKeyColumns': num_cols,
                                'importance': 0.9,
                                'reason': f"{category} low max key count: median={median_max:.0f}, need more rows/lower cardinality"
                            })
        
        print()
    
    # Strategy 1: Target cardinality bins with low samples or high uncertainty
    print("Cardinality-based refinement:")
    for label, low, high in cardinality_bins:
        in_range = (features['BuildCardinalityPct'] >= low) & (features['BuildCardinalityPct'] < high)
        count = in_range.sum()
        sort_count = (in_range & sort_wins).sum()
        sort_pct = (sort_count / count * 100) if count > 0 else 0
        
        # Calculate uncertainty and error rates in this bin
        # Ensure indices align: in_range is boolean Series, uncertain_mask and prediction_errors are arrays
        in_range_indices = np.where(in_range.values)[0] if isinstance(in_range, pd.Series) else np.where(in_range)[0]
        uncertain_in_range = uncertain_mask[in_range_indices].sum() if count > 0 else 0
        errors_in_range = prediction_errors[in_range_indices].sum() if count > 0 else 0
        uncertain_pct = (uncertain_in_range / count * 100) if count > 0 else 0
        error_pct = (errors_in_range / count * 100) if count > 0 else 0
        
        print(f"  {label:8s}: {count:4d} samples, {sort_count:3d} sort wins ({sort_pct:.0f}%), "
              f"{uncertain_in_range:3d} uncertain ({uncertain_pct:.0f}%), {errors_in_range:3d} errors ({error_pct:.0f}%)")
        
        # Determine if we need more samples in this region
        need_more = False
        importance = 0.0
        reason = ""
        
        if count < 30:  # INCREASED from 20
            # Very few samples - high priority
            need_more = True
            importance = 1.0
            reason = "too few samples"
        elif uncertain_pct > 40 and count < 50:
            # High uncertainty - model needs more data here
            need_more = True
            importance = 0.9
            reason = f"high uncertainty ({uncertain_pct:.0f}% uncertain)"
        elif error_pct > 30 and count < 50:
            # High error rate - model struggling here
            need_more = True
            importance = 0.85
            reason = f"high error rate ({error_pct:.0f}% errors)"
        elif count < 50 and sort_pct > 30:
            # Sort wins often but limited samples
            need_more = True
            importance = 0.7
            reason = "sort wins often, need more samples"
        elif sort_pct > 50 and count < 100:
            # Sort dominates but could use more samples
            need_more = True
            importance = 0.5
            reason = "sort dominates, increase confidence"
        
        if need_more:
            print(f"    → TARGET for refinement: {reason}")
            
            # Create target region
            # Get typical row counts in this range
            rows_in_range = features[in_range]
            if len(rows_in_range) > 0:
                build_rows_min = int(rows_in_range['BuildRows'].quantile(0.25))
                build_rows_max = int(rows_in_range['BuildRows'].quantile(0.75))
                probe_rows_min = int(rows_in_range['ProbeRows'].quantile(0.25))
                probe_rows_max = int(rows_in_range['ProbeRows'].quantile(0.75))
            else:
                # Default ranges if no samples
                build_rows_min = 10000
                build_rows_max = 1000000
                probe_rows_min = 10000
                probe_rows_max = 10000000
            
            target_regions.append({
                'buildCardinalityMin': low,
                'buildCardinalityMax': high,
                'probeCardinalityMin': 0.0,
                'probeCardinalityMax': 1.0,
                'buildRowsMin': build_rows_min,
                'buildRowsMax': build_rows_max,
                'probeRowsMin': probe_rows_min,
                'probeRowsMax': probe_rows_max,
                'keyTypes': ['int', 'long', 'string'],  # Will be refined below
                'numKeyColumns': [1, 2, 3],
                'importance': importance,
                'reason': f"{label} cardinality: {reason}"
            })
    
    print()
    
    # NEW: Analyze key type combinations that lead to uncertainty/errors
    print("Key type analysis (identifying problematic combinations):")
    
    # Extract key type information from dataframe
    key_type_combos = []
    for idx in range(len(best_df)):
        left_type = best_df.iloc[idx]['LeftKeyType']
        right_type = best_df.iloc[idx]['RightKeyType']
        num_cols = best_df.iloc[idx]['NumKeyColumns']
        
        # Normalize key type representation (handle mixed keys)
        def normalize_key_type(key_type_str):
            """Normalize key type to handle mixed keys.
            
            Handles commas inside parentheses (e.g., decimal(18,2)) correctly.
            Only splits on commas that are NOT inside parentheses.
            """
            if ',' not in key_type_str:
                return key_type_str
            
            # Check if this is a single type with commas inside parentheses (e.g., decimal(18,2))
            # If there's no comma outside parentheses, it's a single type
            paren_depth = 0
            has_comma_outside_parens = False
            for char in key_type_str:
                if char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
                elif char == ',' and paren_depth == 0:
                    has_comma_outside_parens = True
                    break
            
            # If no comma outside parentheses, it's a single type (like decimal(18,2))
            if not has_comma_outside_parens:
                return key_type_str
            
            # Mixed keys: split only on commas outside parentheses
            types = []
            current_type = []
            paren_depth = 0
            
            for char in key_type_str:
                if char == '(':
                    paren_depth += 1
                    current_type.append(char)
                elif char == ')':
                    paren_depth -= 1
                    current_type.append(char)
                elif char == ',' and paren_depth == 0:
                    # This comma separates types
                    types.append(''.join(current_type).strip())
                    current_type = []
                else:
                    current_type.append(char)
            
            # Add the last type
            if current_type:
                types.append(''.join(current_type).strip())
            
            # Extract unique types and sort
            types = sorted(set(types))
            return ','.join(types)
        
        left_norm = normalize_key_type(str(left_type))
        right_norm = normalize_key_type(str(right_type))
        
        # Create a canonical representation (smaller side is build)
        if best_df.iloc[idx]['LeftRows'] <= best_df.iloc[idx]['RightRows']:
            build_type = left_norm
            probe_type = right_norm
        else:
            build_type = right_norm
            probe_type = left_norm
        
        key_type_combos.append({
            'build_type': build_type,
            'probe_type': probe_type,
            'num_cols': num_cols,
            'is_uncertain': uncertain_mask[idx] if isinstance(uncertain_mask, np.ndarray) else uncertain_mask.iloc[idx],
            'has_error': prediction_errors[idx],
            'sort_wins': sort_wins.iloc[idx] if isinstance(sort_wins, pd.Series) else sort_wins[idx]
        })
    
    combo_df = pd.DataFrame(key_type_combos)
    
    # Analyze each key type combination
    type_issues = {}
    for combo_key, group in combo_df.groupby(['build_type', 'probe_type', 'num_cols']):
        build_type, probe_type, num_cols = combo_key
        count = len(group)
        uncertain_count = group['is_uncertain'].sum()
        error_count = group['has_error'].sum()
        sort_count = group['sort_wins'].sum()
        
        uncertain_pct = (uncertain_count / count * 100) if count > 0 else 0
        error_pct = (error_count / count * 100) if count > 0 else 0
        
        # Identify problematic combinations
        # Only flag if there are actual problems (uncertainty/errors) OR very low count with issues
        # Don't flag combinations that have 0% uncertainty and 0% errors just because count is low
        is_problematic = False
        if uncertain_pct > 40 or error_pct > 30:
            # High uncertainty or error rate - definitely problematic
            is_problematic = True
        elif count < 10 and (uncertain_pct > 20 or error_pct > 15):
            # Very low count AND some uncertainty/errors - problematic
            is_problematic = True
        elif count < 5:
            # Extremely low count (< 5) - flag even if no errors (might be missing coverage)
            is_problematic = True
        
        if is_problematic:
            type_key = f"{build_type}|{probe_type}|{num_cols}"
            type_issues[type_key] = {
                'build_type': build_type,
                'probe_type': probe_type,
                'num_cols': num_cols,
                'count': count,
                'uncertain_pct': uncertain_pct,
                'error_pct': error_pct,
                'sort_count': sort_count,
                'importance': max(0.6, min(1.0, (40 - count) / 20 + uncertain_pct / 50 + error_pct / 40))
            }
    
    if type_issues:
        print(f"  Found {len(type_issues)} problematic key type combinations:")
        for type_key, info in sorted(type_issues.items(), key=lambda x: x[1]['importance'], reverse=True)[:10]:
            print(f"    {info['build_type']:20s} | {info['probe_type']:20s} | {info['num_cols']} cols: "
                  f"{info['count']:3d} samples, {info['uncertain_pct']:.0f}% uncertain, {info['error_pct']:.0f}% errors")
            
            # Extract key types to target
            # Use the same logic as normalize_key_type to split correctly
            def split_types_safely(type_str):
                """Split type string on commas, but not inside parentheses."""
                if ',' not in type_str:
                    return [type_str]
                
                # Check if there's a comma outside parentheses
                paren_depth = 0
                has_comma_outside = False
                for char in type_str:
                    if char == '(':
                        paren_depth += 1
                    elif char == ')':
                        paren_depth -= 1
                    elif char == ',' and paren_depth == 0:
                        has_comma_outside = True
                        break
                
                if not has_comma_outside:
                    return [type_str]
                
                # Split on commas outside parentheses
                types = []
                current = []
                paren_depth = 0
                for char in type_str:
                    if char == '(':
                        paren_depth += 1
                        current.append(char)
                    elif char == ')':
                        paren_depth -= 1
                        current.append(char)
                    elif char == ',' and paren_depth == 0:
                        types.append(''.join(current).strip())
                        current = []
                    else:
                        current.append(char)
                if current:
                    types.append(''.join(current).strip())
                return types
            
            build_types = split_types_safely(info['build_type'])
            probe_types = split_types_safely(info['probe_type'])
            all_types = sorted(set(build_types + probe_types))
            
            # Map to allowed types in Scala benchmark
            # Preserve decimal types instead of mapping them to int/long
            # The Scala benchmark supports: int, long, decimal(9,2), decimal(18,2), decimal(38,2), string
            allowed_types = []
            supported_types = {
                'int': 'int',
                'long': 'long',
                'decimal(9,2)': 'decimal(9,2)',
                'decimal(18,2)': 'decimal(18,2)',
                'decimal(38,2)': 'decimal(38,2)',
                'string': 'string'
            }
            for t in all_types:
                t_stripped = t.strip()
                # Check if it's a supported type (exact match)
                if t_stripped in supported_types:
                    mapped = supported_types[t_stripped]
                    if mapped not in allowed_types:
                        allowed_types.append(mapped)
                else:
                    # Fallback: try to map unknown types
                    # For backward compatibility, map unknown types to int
                    if 'int' not in allowed_types:
                        allowed_types.append('int')
            
            if not allowed_types:
                allowed_types = ['int', 'long', 'string']
            
            # Find or create a cardinality region to attach this type constraint to
            # For now, create a separate region focused on this type combination
            # Use the cardinality range where this type combo appears most
            combo_mask = (
                (combo_df['build_type'] == info['build_type']) &
                (combo_df['probe_type'] == info['probe_type']) &
                (combo_df['num_cols'] == info['num_cols'])
            )
            if combo_mask.sum() > 0:
                combo_indices = combo_df[combo_mask].index
                combo_features = features.iloc[combo_indices]
                
                if len(combo_features) > 0:
                    card_min = combo_features['BuildCardinalityPct'].quantile(0.1)
                    card_max = combo_features['BuildCardinalityPct'].quantile(0.9)
                    build_rows_min = int(combo_features['BuildRows'].quantile(0.25))
                    build_rows_max = int(combo_features['BuildRows'].quantile(0.75))
                    probe_rows_min = int(combo_features['ProbeRows'].quantile(0.25))
                    probe_rows_max = int(combo_features['ProbeRows'].quantile(0.75))
                    
                    target_regions.append({
                        'buildCardinalityMin': max(0.0, card_min - 0.01),
                        'buildCardinalityMax': min(1.0, card_max + 0.01),
                        'probeCardinalityMin': 0.0,
                        'probeCardinalityMax': 1.0,
                        'buildRowsMin': build_rows_min,
                        'buildRowsMax': build_rows_max,
                        'probeRowsMin': probe_rows_min,
                        'probeRowsMax': probe_rows_max,
                        'keyTypes': allowed_types,
                        'numKeyColumns': [info['num_cols']],  # Focus on this column count
                        'importance': info['importance'],
                        'reason': f"Key type combo ({info['build_type']}|{info['probe_type']}|{info['num_cols']}cols): "
                                 f"{info['count']} samples, {info['uncertain_pct']:.0f}% uncertain, {info['error_pct']:.0f}% errors"
                    })
    else:
        print("  No problematic key type combinations identified")
    
    print()
    
    # Analyze key type coverage (original analysis)
    print("Coverage by Key Type:")
    for key_type in ['int', 'long', 'decimal(9,2)', 'decimal(18,2)', 'decimal(38,2)', 'string']:
        # Use left key type as representative
        # For decimal types, need exact match to avoid matching decimal(18,2) when searching for decimal(9,2)
        if key_type.startswith('decimal'):
            # Exact match for decimal types (they contain commas, so simple contains won't work)
            type_mask = best_df['LeftKeyType'] == key_type
        else:
            type_mask = best_df['LeftKeyType'].str.contains(key_type, na=False)
        count = type_mask.sum()
        sort_count = (type_mask & sort_wins).sum()
        type_mask_indices = np.where(type_mask.values)[0] if isinstance(type_mask, pd.Series) else np.where(type_mask)[0]
        uncertain_count = uncertain_mask[type_mask_indices].sum() if count > 0 else 0
        
        if count > 0:
            print(f"  {key_type:20s}: {count:4d} samples, {sort_count:3d} sort wins, {uncertain_count:3d} uncertain")
            if count < 50:  # INCREASED from 30
                print(f"    → Consider adding more samples for this key type")
    
    print()
    
    # NEW: Analyze uncertain regions by feature combinations
    print("High-uncertainty regions (model needs more data here):")
    uncertain_indices = np.where(uncertain_mask)[0]
    if len(uncertain_indices) > 0:
        uncertain_features = features.iloc[uncertain_indices]
        
        # Analyze cardinality distribution of uncertain cases
        uncertain_cards = uncertain_features['BuildCardinalityPct'].values
        if len(uncertain_cards) > 0:
            card_25 = np.percentile(uncertain_cards, 25)
            card_75 = np.percentile(uncertain_cards, 75)
            print(f"  Uncertain cases: {len(uncertain_indices)} samples")
            print(f"    Cardinality range: {card_25*100:.2f}% - {card_75*100:.2f}%")
            
            # If uncertain cases cluster in a specific range, add a targeted region
            if card_75 - card_25 < 0.05:  # Tight cluster
                target_regions.append({
                    'buildCardinalityMin': max(0.0, card_25 - 0.01),
                    'buildCardinalityMax': min(1.0, card_75 + 0.01),
                    'probeCardinalityMin': 0.0,
                    'probeCardinalityMax': 1.0,
                    'buildRowsMin': int(uncertain_features['BuildRows'].quantile(0.25)),
                    'buildRowsMax': int(uncertain_features['BuildRows'].quantile(0.75)),
                    'probeRowsMin': int(uncertain_features['ProbeRows'].quantile(0.25)),
                    'probeRowsMax': int(uncertain_features['ProbeRows'].quantile(0.75)),
                    'keyTypes': ['int', 'long', 'string'],
                    'numKeyColumns': [1, 2, 3],
                    'importance': 0.8,
                    'reason': f"High uncertainty cluster: {len(uncertain_indices)} uncertain cases in {card_25*100:.2f}%-{card_75*100:.2f}% cardinality"
                })
    
    print()
    
    # Summary
    print(f"Identified {len(target_regions)} target regions for refinement")
    if len(target_regions) > 0:
        print("  Regions prioritized by:")
        print("    1. Model uncertainty (where confidence < 70%)")
        print("    2. Prediction errors (where model predicts wrong strategy)")
        print("    3. Key type combinations with low samples or high errors")
        print("    4. Cardinality bins with insufficient coverage")
    print()
    
    return target_regions

# ============================================================================
# Refinement Config Generation
# ============================================================================

def generate_refinement_config(target_regions, output_path=REFINEMENT_CONFIG_PATH):
    """Generate JSON config for scala benchmark to target specific regions."""
    
    if not target_regions:
        print("No target regions identified - model appears well-trained!")
        return False
    
    # Calculate tests per region based on importance
    total_budget = 200  # Total tests to generate
    total_importance = sum(r['importance'] for r in target_regions)
    
    # Assign tests proportional to importance
    for region in target_regions:
        region['num_tests'] = max(10, int(total_budget * region['importance'] / total_importance))
    
    # Convert numpy types to native Python types for JSON serialization
    def convert_to_native(obj):
        """Recursively convert numpy types to native Python types."""
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {key: convert_to_native(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [convert_to_native(item) for item in obj]
        else:
            return obj
    
    # Convert target_regions to native types
    target_regions_native = [convert_to_native(region) for region in target_regions]
    
    # Write JSON config
    config = {
        'targetRegions': target_regions_native,
        'numTestsPerRegion': int(max(r['num_tests'] for r in target_regions))
    }
    
    with open(output_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    print("="*80)
    print("REFINEMENT CONFIG GENERATED")
    print("="*80)
    print()
    print(f"Written to: {output_path}")
    print()
    print(f"Config includes {len(target_regions)} target regions:")
    for region in target_regions:
        card_range = f"{region['buildCardinalityMin']*100:.1f}%-{region['buildCardinalityMax']*100:.1f}%"
        print(f"  • {card_range:12s} cardinality, {region['num_tests']:3d} tests, " +
              f"importance={region['importance']:.2f}")
        print(f"    Reason: {region['reason']}")
    print()
    print("Next step:")
    print("  Run the scala benchmark again:")
    print("    spark-shell --jars ... -i simple_hash_vs_sort_benchmark.scala")
    print("  It will automatically read this config and generate targeted tests.")
    print()
    
    return True

# ============================================================================
# Main Workflow
# ============================================================================

def main():
    # Determine TSV path
    if len(sys.argv) > 1:
        tsv_path = sys.argv[1]
    else:
        tsv_path = DEFAULT_TSV_PATH
    
    print("="*80)
    print("ITERATIVE MODEL TRAINER FOR HASH vs SORT JOIN STRATEGY")
    print("="*80)
    print()
    print(f"Loading data from: {tsv_path}")
    print()
    
    # Load and filter data
    df = load_and_filter_data(tsv_path)
    
    # Add build/probe features (including key type scores and key count stats)
    # This ensures all features are available for correlation analysis
    features = create_build_probe_features(df)
    # Merge features back into dataframe
    for col in features.columns:
        if col not in df.columns:
            df[col] = features[col]
        else:
            # Update existing columns (features may override raw columns)
            df[col] = features[col]
    
    # Analyze timing outliers (build vs probe time analysis)
    analyze_timing_outliers(df)
    
    # Find best strategy for each config
    best_df = find_best_strategy_per_config(df)
    
    # Assess data sufficiency
    sufficient, issues, warnings = assess_data_sufficiency(best_df)
    
    if not sufficient:
        print("="*80)
        print("INSUFFICIENT DATA FOR RELIABLE MODEL")
        print("="*80)
        print()
        print("The dataset has critical issues that must be addressed.")
        print("Generating refinement config to collect more data...")
        print()
        
        # Generate very targeted refinement to address critical issues
        target_regions = []
        
        # If too few sort wins, focus heavily on low cardinality
        strategy_counts = best_df['BestStrategy'].value_counts()
        sort_count = strategy_counts.get('sort_object_post', 0)
        
        if sort_count < MIN_SAMPLES_PER_STRATEGY:
            print(f"Critical: Only {sort_count} sort wins found!")
            print("Generating heavy low-cardinality sampling to find more sort wins...")
            print()
            
            # Very low cardinality regions
            target_regions.extend([
                {
                    'buildCardinalityMin': 0.001,
                    'buildCardinalityMax': 0.005,
                    'probeCardinalityMin': 0.0,
                    'probeCardinalityMax': 1.0,
                    'buildRowsMin': 50000,
                    'buildRowsMax': 5000000,
                    'probeRowsMin': 50000,
                    'probeRowsMax': 50000000,
                    'keyTypes': ['int', 'long', 'string'],
                    'numKeyColumns': [1, 2],
                    'importance': 1.0,
                    'reason': 'Critical: Need to find more sort wins (<0.5% cardinality)'
                },
                {
                    'buildCardinalityMin': 0.005,
                    'buildCardinalityMax': 0.01,
                    'probeCardinalityMin': 0.0,
                    'probeCardinalityMax': 1.0,
                    'buildRowsMin': 50000,
                    'buildRowsMax': 5000000,
                    'probeRowsMin': 50000,
                    'probeRowsMax': 50000000,
                    'keyTypes': ['int', 'long', 'string'],
                    'numKeyColumns': [1, 2],
                    'importance': 0.8,
                    'reason': 'Critical: Need to find more sort wins (0.5-1% cardinality)'
                }
            ])
        
        generate_refinement_config(target_regions)
        sys.exit(1)
    
    # Train and evaluate model
    model, X, y, train_acc, test_acc, cv_score, quality_issues, quality_warnings = \
        train_and_evaluate_model(best_df)
    
    # Identify refinement regions
    target_regions = identify_refinement_regions(best_df, model, X)
    
    # Overall recommendation
    print("="*80)
    print("FINAL RECOMMENDATION")
    print("="*80)
    print()
    
    if quality_issues:
        print("MODEL HAS QUALITY ISSUES:")
        for issue in quality_issues:
            print(f"  ✗ {issue}")
        print()
        print("Generating refinement config to improve model quality...")
        generate_refinement_config(target_regions)
    elif target_regions:
        print("MODEL IS ACCEPTABLE BUT CAN BE IMPROVED:")
        print(f"  • Test accuracy: {test_acc:.1%}")
        print(f"  • Cross-validation: {cv_score:.1%}")
        print()
        print("Refinement recommendations available to improve coverage.")
        generate_refinement_config(target_regions)
    else:
        print("MODEL IS WELL-TRAINED!")
        print(f"  ✓ Test accuracy: {test_acc:.1%}")
        print(f"  ✓ Cross-validation: {cv_score:.1%}")
        print(f"  ✓ Training samples: {len(X)}")
        print()
        print("The model appears to have good coverage and generalization.")
        print("You can proceed with using this model for production heuristics.")
        print()
        
        # Remove any existing refinement config
        if os.path.exists(REFINEMENT_CONFIG_PATH):
            os.remove(REFINEMENT_CONFIG_PATH)
            print(f"Removed refinement config: {REFINEMENT_CONFIG_PATH}")
            print()
    
    print("="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)
    print()

if __name__ == "__main__":
    main()

