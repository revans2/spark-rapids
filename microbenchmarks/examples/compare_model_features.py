#!/usr/bin/env python3
"""
Model Feature Comparison Script

Trains and compares three different models with varying feature complexity:
1. Model 1 (Simple): Basic features available without extra computation
   - Key types, avg key bytes, row counts, build distinct keys only
2. Model 2 (Build Stats): Simple features + build-side key count statistics  
   - All Model 1 features + build max key count + build stddev
3. Model 3 (Full Stats): All features including probe-side statistics
   - All Model 2 features + probe-side key count stats

Compares models on:
- Classification accuracy (train/test/cross-validation)
- Total runtime performance (vs baselines and oracle)
- Model efficiency and overhead

Usage:
    python compare_model_features.py [/path/to/benchmark_results.tsv]
    
    If no path is provided, defaults to:
        /data/tmp/simple_hash_vs_sort/benchmark_results.tsv

Requirements:
    pip install pandas scikit-learn numpy matplotlib seaborn
"""

import sys
import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.tree import DecisionTreeClassifier, export_text
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import warnings
warnings.filterwarnings('ignore')

# Default paths
DEFAULT_TSV_PATH = "/data/tmp/simple_hash_vs_sort/benchmark_results.tsv"

# Test set size
TEST_SIZE = 0.10  # 10% holdout for testing

# Decision tree parameters
DT_MAX_DEPTH = 10
DT_MIN_SAMPLES_SPLIT = 20
DT_MIN_SAMPLES_LEAF = 10

# ============================================================================
# Data Loading
# ============================================================================

def load_and_prepare_data(tsv_path):
    """Load TSV and prepare paired data for comparison."""
    print("="*80)
    print("LOADING AND PREPARING DATA")
    print("="*80)
    print()
    
    if not os.path.exists(tsv_path):
        print(f"ERROR: File not found: {tsv_path}")
        sys.exit(1)
    
    # Read TSV
    df = pd.read_csv(tsv_path, sep='\t')
    print(f"Loaded {len(df)} benchmark results")
    
    # Filter to successful runs only
    success_df = df[df['Status'] == 'SUCCESS'].copy()
    print(f"Successful runs: {len(success_df)} ({len(success_df)/len(df)*100:.1f}%)")
    
    # Filter to HashObject and SortObjectPost only
    target_strategies = ['hash_object', 'sort_object_post']
    filtered_df = success_df[success_df['JoinStrategy'].isin(target_strategies)].copy()
    print(f"HashObject/SortObjectPost runs: {len(filtered_df)} ({len(filtered_df)/len(success_df)*100:.1f}%)")
    
    if len(filtered_df) == 0:
        print("ERROR: No HashObject or SortObjectPost results found!")
        sys.exit(1)
    
    print()
    print("Strategy Distribution:")
    print(filtered_df['JoinStrategy'].value_counts().to_string())
    print()
    
    # Group by test configuration (same approach as iterative_model_trainer.py)
    # This is more robust than parsing test names
    print("Finding best strategy for each configuration...")
    config_cols = [
        'LeftRows', 'LeftDistinctKeys', 'LeftKeyType',
        'RightRows', 'RightDistinctKeys', 'RightKeyType',
        'NumKeyColumns'
    ]
    
    best_strategies = []
    
    for config, group in filtered_df.groupby(config_cols, dropna=False):
        # Get times for each strategy
        hash_rows = group[group['JoinStrategy'] == 'hash_object']
        sort_rows = group[group['JoinStrategy'] == 'sort_object_post']
        
        if len(hash_rows) == 0 or len(sort_rows) == 0:
            continue
        
        hash_time = hash_rows['MedianTimeMs'].min()
        sort_time = sort_rows['MedianTimeMs'].min()
        
        # Skip if either strategy is missing
        if pd.isna(hash_time) or pd.isna(sort_time):
            continue
        
        # Find which is faster
        if sort_time < hash_time:
            best_strategy = 'sort_object_post'
            best_time = sort_time
            alternative_time = hash_time
            best_row = sort_rows.iloc[0].copy()
        else:
            best_strategy = 'hash_object'
            best_time = hash_time
            alternative_time = sort_time
            best_row = hash_rows.iloc[0].copy()
        
        # Store the best run
        best_row['BestStrategy'] = best_strategy
        best_row['BestTime'] = best_time
        best_row['AlternativeTime'] = alternative_time
        best_row['TimeDiffMs'] = alternative_time - best_time
        best_row['TimeDiffPct'] = ((alternative_time - best_time) / alternative_time) * 100
        best_strategies.append(best_row)
    
    if len(best_strategies) == 0:
        print("ERROR: No complete test pairs found (need both hash_object and sort_object_post for each test)")
        sys.exit(1)
    
    best_df = pd.DataFrame(best_strategies)
    print(f"Found best strategies for {len(best_df)} unique configurations")
    print()
    
    print(f"Best strategy distribution:")
    print(best_df['BestStrategy'].value_counts().to_string())
    print()
    
    return best_df

# ============================================================================
# Feature Engineering - Three Different Feature Sets
# ============================================================================

def create_model1_features(df):
    """
    Model 1 (Simple): Basic features available without extra computation.
    
    Features:
    - Key types (as scores)
    - Average key bytes  
    - Row counts (left, right, build, probe)
    - Build distinct keys only
    - Derived features from above
    """
    features = pd.DataFrame()
    
    # Determine build and probe sides (smaller side is build)
    features['BuildRows'] = df.apply(
        lambda row: row['LeftRows'] if row['LeftRows'] <= row['RightRows'] else row['RightRows'],
        axis=1
    )
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
    
    features['ProbeRows'] = df.apply(
        lambda row: row['RightRows'] if row['LeftRows'] <= row['RightRows'] else row['LeftRows'],
        axis=1
    )
    # NOTE: ProbeDistinctKeys NOT included in Model 1 (too expensive to compute)
    
    # Key type scores
    key_type_flags = {
        'int': 1, 'decimal(9,2)': 1,
        'long': 2, 'decimal(18,2)': 2,
        'decimal(38,2)': 4,
        'string': 8
    }
    
    def compute_key_type_score(key_type_str):
        if ',' not in key_type_str:
            return key_type_flags.get(key_type_str, 1)
        
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
        
        if not has_comma_outside:
            return key_type_flags.get(key_type_str, 1)
        
        # Mixed keys: split and OR flags
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
    
    # Derived features
    features['TotalRows'] = features['BuildRows'] + features['ProbeRows']
    features['BuildProbeRatio'] = features['BuildRows'] / features['ProbeRows']
    features['BuildLowCardinality'] = (features['BuildCardinalityPct'] < 0.1).astype(int)
    features['BuildVeryLowCardinality'] = (features['BuildCardinalityPct'] < 0.05).astype(int)
    
    return features

def create_model2_features(df):
    """
    Model 2 (Build Stats): Model 1 + build-side key count statistics.
    
    Additional features beyond Model 1:
    - Build max key count
    - Build key count stddev
    """
    features = create_model1_features(df)
    
    # Add build-side key count statistics if available
    if 'BuildMaxKeyCount' in df.columns:
        # Map left/right to build/probe
        features['BuildMaxKeyCount'] = df.apply(
            lambda row: row.get('BuildMaxKeyCount', 0) if pd.notna(row.get('BuildMaxKeyCount')) else 0,
            axis=1
        )
    else:
        features['BuildMaxKeyCount'] = 0
    
    if 'BuildKeyCountStdDev' in df.columns:
        features['BuildKeyCountStdDev'] = df.apply(
            lambda row: row.get('BuildKeyCountStdDev', 0) if pd.notna(row.get('BuildKeyCountStdDev')) else 0,
            axis=1
        )
    else:
        features['BuildKeyCountStdDev'] = 0
    
    # Derived features from key count stats
    if 'BuildMaxKeyCount' in features.columns and 'BuildKeyCountStdDev' in features.columns:
        # Coefficient of variation (normalized stddev)
        features['BuildKeyCountCV'] = features['BuildKeyCountStdDev'] / features['BuildMaxKeyCount'].replace(0, 1)
        
        # High key count flags
        features['BuildHighMaxKeyCount'] = (features['BuildMaxKeyCount'] > 100).astype(int)
        features['BuildVeryHighMaxKeyCount'] = (features['BuildMaxKeyCount'] > 1000).astype(int)
    
    return features

def create_model3_features(df):
    """
    Model 3 (Full Stats): Model 2 + probe-side key count statistics.
    
    Additional features beyond Model 2:
    - Probe max key count
    - Probe key count stddev
    - Probe key count percentiles (p99, p95, p90, p75, p50)
    """
    features = create_model2_features(df)
    
    # Add probe-side key count statistics if available
    probe_stats_cols = [
        'ProbeMaxKeyCount', 'ProbeKeyCountStdDev', 'ProbeKeyCountP99', 
        'ProbeKeyCountP95', 'ProbeKeyCountP90', 'ProbeKeyCountP75', 'ProbeKeyCountP50'
    ]
    
    for col in probe_stats_cols:
        if col in df.columns:
            features[col] = df[col].fillna(0)
        else:
            features[col] = 0
    
    # Derived features from probe stats
    if 'ProbeMaxKeyCount' in features.columns and 'ProbeKeyCountStdDev' in features.columns:
        # Coefficient of variation
        features['ProbeKeyCountCV'] = features['ProbeKeyCountStdDev'] / features['ProbeMaxKeyCount'].replace(0, 1)
        
        # High key count flags
        features['ProbeHighMaxKeyCount'] = (features['ProbeMaxKeyCount'] > 100).astype(int)
        features['ProbeVeryHighMaxKeyCount'] = (features['ProbeMaxKeyCount'] > 1000).astype(int)
    
    # Build vs Probe key count comparisons
    if 'BuildMaxKeyCount' in features.columns and 'ProbeMaxKeyCount' in features.columns:
        features['BuildProbeMaxKeyCountRatio'] = features['BuildMaxKeyCount'] / features['ProbeMaxKeyCount'].replace(0, 1)
    
    return features

# ============================================================================
# Model Training and Evaluation
# ============================================================================

def train_and_evaluate_model(X, y, X_test, y_test, model_name, feature_names):
    """Train a decision tree model and return evaluation metrics."""
    
    # Train decision tree
    dt = DecisionTreeClassifier(
        max_depth=DT_MAX_DEPTH,
        min_samples_split=DT_MIN_SAMPLES_SPLIT,
        min_samples_leaf=DT_MIN_SAMPLES_LEAF,
        random_state=42
    )
    dt.fit(X, y)
    
    # Predictions
    y_train_pred = dt.predict(X)
    y_test_pred = dt.predict(X_test)
    
    # Accuracies
    train_accuracy = accuracy_score(y, y_train_pred)
    test_accuracy = accuracy_score(y_test, y_test_pred)
    
    # Cross-validation (5-fold)
    cv_scores = cross_val_score(dt, X, y, cv=5, scoring='accuracy')
    cv_mean = cv_scores.mean()
    cv_std = cv_scores.std()
    
    # Confusion matrix
    cm = confusion_matrix(y_test, y_test_pred)
    
    return {
        'model': dt,
        'model_name': model_name,
        'feature_names': feature_names,
        'train_accuracy': train_accuracy,
        'test_accuracy': test_accuracy,
        'cv_mean': cv_mean,
        'cv_std': cv_std,
        'confusion_matrix': cm,
        'y_train_pred': y_train_pred,
        'y_test_pred': y_test_pred
    }

class SimpleHeuristicModel:
    """
    Simple heuristic model that uses a single rule:
    If BuildMaxKeyCount <= threshold, use HASH (class 0)
    If BuildMaxKeyCount > threshold, use SORT (class 1)
    """
    def __init__(self, threshold, feature_name='BuildMaxKeyCount'):
        self.threshold = threshold
        self.feature_name = feature_name
    
    def predict(self, X):
        """Predict using the simple heuristic rule."""
        if self.feature_name not in X.columns:
            # Fallback: if feature not available, always predict HASH
            return np.zeros(len(X), dtype=int)
        
        predictions = np.where(X[self.feature_name] <= self.threshold, 0, 1)
        return predictions
    
    def predict_proba(self, X):
        """Return probability estimates (deterministic: 1.0 for predicted class)."""
        predictions = self.predict(X)
        proba = np.zeros((len(X), 2))
        proba[predictions == 0, 0] = 1.0
        proba[predictions == 1, 1] = 1.0
        return proba

def evaluate_heuristic_model(X, y, X_test, y_test, model_name, feature_names, threshold):
    """Evaluate a simple heuristic model."""
    
    # Create heuristic model
    heuristic = SimpleHeuristicModel(threshold=threshold, feature_name='BuildMaxKeyCount')
    
    # Predictions
    y_train_pred = heuristic.predict(X)
    y_test_pred = heuristic.predict(X_test)
    
    # Accuracies
    train_accuracy = accuracy_score(y, y_train_pred)
    test_accuracy = accuracy_score(y_test, y_test_pred)
    
    # For heuristic, cross-validation doesn't make sense (no training)
    # Just use test accuracy as "CV score"
    cv_mean = test_accuracy
    cv_std = 0.0
    
    # Confusion matrix
    cm = confusion_matrix(y_test, y_test_pred)
    
    return {
        'model': heuristic,
        'model_name': model_name,
        'feature_names': feature_names,
        'train_accuracy': train_accuracy,
        'test_accuracy': test_accuracy,
        'cv_mean': cv_mean,
        'cv_std': cv_std,
        'confusion_matrix': cm,
        'y_train_pred': y_train_pred,
        'y_test_pred': y_test_pred,
        'is_heuristic': True,
        'heuristic_rule': f"IF BuildMaxKeyCount <= {threshold:,.0f} THEN HASH ELSE SORT"
    }

def compute_runtime_metrics(best_df, X, y, X_test, y_test, y_all_pred, y_test_pred):
    """
    Compute runtime metrics for a model's predictions.
    
    Returns timing comparison for both test set and all data.
    """
    def compute_timings(indices, y_pred):
        """Compute timings for a subset of data."""
        subset_df = best_df.loc[indices].copy()
        subset_df_reset = subset_df.reset_index(drop=True)
        
        hash_times = []
        sort_times = []
        oracle_times = []
        model_times = []
        
        for idx in range(len(subset_df_reset)):
            row = subset_df_reset.iloc[idx]
            
            # Get hash and sort times
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
            if y_pred[idx] == 0:
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
    test_timings = compute_timings(test_indices, y_test_pred)
    
    # Compute for all data
    all_indices = X.index.tolist()
    all_timings = compute_timings(all_indices, y_all_pred)
    
    return test_timings, all_timings

# ============================================================================
# Results Printing
# ============================================================================

def print_classification_results(results):
    """Print classification accuracy metrics."""
    print(f"\nModel: {results['model_name']}")
    print("-" * 60)
    
    # Check if this is a heuristic model
    if results.get('is_heuristic', False):
        print(f"  Rule: {results['heuristic_rule']}")
        print(f"  Train Accuracy: {results['train_accuracy']:.3f}")
        print(f"  Test Accuracy:  {results['test_accuracy']:.3f}")
        print(f"  (No cross-validation for heuristics - they don't train)")
    else:
        print(f"  Features: {len(results['feature_names'])} features")
        print(f"  Train Accuracy: {results['train_accuracy']:.3f}")
        print(f"  Test Accuracy:  {results['test_accuracy']:.3f}")
        print(f"  CV Accuracy:    {results['cv_mean']:.3f} ± {results['cv_std']:.3f}")
        print(f"  Overfitting Gap: {results['train_accuracy'] - results['test_accuracy']:.3f}")
    
    print()
    print("  Confusion Matrix (Test Set):")
    cm = results['confusion_matrix']
    print(f"    True Hash/Pred Hash: {cm[0][0]}")
    print(f"    True Hash/Pred Sort: {cm[0][1]}")
    print(f"    True Sort/Pred Hash: {cm[1][0]}")
    print(f"    True Sort/Pred Sort: {cm[1][1]}")

def print_runtime_comparison(timings, n_samples, label):
    """Print runtime comparison table."""
    print(f"\n{label} ({n_samples} joins):")
    print("-" * 85)
    
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
    print(f"{'1. Always HASH':<20s} {always_hash['total']:>18,.1f} {always_hash['avg']:>15,.1f} {'baseline':>15s} {-oracle_savings_vs_sort:>14.1f}%")
    print(f"{'2. Always SORT':<20s} {always_sort['total']:>18,.1f} {always_sort['avg']:>15,.1f} {-oracle_savings_vs_hash:>14.1f}% {'baseline':>15s}")
    print(f"{'3. Perfect Oracle':<20s} {oracle['total']:>18,.1f} {oracle['avg']:>15,.1f} {oracle_savings_vs_hash:>14.1f}% {oracle_savings_vs_sort:>14.1f}%")
    print(f"{'4. Model':<20s} {model['total']:>18,.1f} {model['avg']:>15,.1f} {model_savings_vs_hash:>14.1f}% {model_savings_vs_sort:>14.1f}%")
    print()
    print(f"Model Efficiency: {model_efficiency:.1f}% of Oracle (overhead: {model_overhead:+.1f}%)")

def print_comparison_table(all_results):
    """Print comprehensive comparison table for all models."""
    print("\n" + "="*100)
    print("MODEL COMPARISON SUMMARY")
    print("="*100)
    print()
    
    # Classification metrics comparison
    print("CLASSIFICATION ACCURACY:")
    print("-" * 100)
    print(f"{'Model':<25s} {'Features':>10s} {'Train Acc':>11s} {'Test Acc':>10s} {'CV Acc':>15s} {'Overfit Gap':>12s}")
    print("-" * 100)
    
    for results in all_results:
        name = results['model_name']
        n_features = len(results['feature_names'])
        train_acc = results['train_accuracy']
        test_acc = results['test_accuracy']
        cv_mean = results['cv_mean']
        cv_std = results['cv_std']
        overfit_gap = train_acc - test_acc
        
        print(f"{name:<25s} {n_features:>10d} {train_acc:>10.3f} {test_acc:>10.3f} {cv_mean:>8.3f}±{cv_std:.3f} {overfit_gap:>11.3f}")
    
    print()
    
    # Runtime performance comparison (test set)
    print("RUNTIME PERFORMANCE (Test Set):")
    print("-" * 100)
    print(f"{'Model':<25s} {'Model Total (ms)':>18s} {'Model Avg (ms)':>15s} {'vs Hash':>10s} {'vs Sort':>10s} {'Efficiency':>12s} {'Overhead':>10s}")
    print("-" * 100)
    
    for results in all_results:
        name = results['model_name']
        timings = results['test_timings']
        
        always_hash = timings['always_hash']
        always_sort = timings['always_sort']
        oracle = timings['oracle']
        model = timings['model']
        
        model_savings_vs_hash = ((always_hash['total'] - model['total']) / always_hash['total']) * 100
        model_savings_vs_sort = ((always_sort['total'] - model['total']) / always_sort['total']) * 100
        model_efficiency = (oracle['total'] / model['total']) * 100 if model['total'] > 0 else 0
        model_overhead = ((model['total'] - oracle['total']) / oracle['total']) * 100 if oracle['total'] > 0 else 0
        
        print(f"{name:<25s} {model['total']:>18,.1f} {model['avg']:>15,.1f} {model_savings_vs_hash:>9.1f}% {model_savings_vs_sort:>9.1f}% {model_efficiency:>11.1f}% {model_overhead:>9.1f}%")
    
    # Add baseline rows for context
    example_timings = all_results[0]['test_timings']
    always_hash = example_timings['always_hash']
    always_sort = example_timings['always_sort']
    oracle = example_timings['oracle']
    
    print("-" * 100)
    print(f"{'Always HASH (baseline)':<25s} {always_hash['total']:>18,.1f} {always_hash['avg']:>15,.1f} {'baseline':>10s} {'-':>10s} {'-':>12s} {'-':>10s}")
    print(f"{'Always SORT (baseline)':<25s} {always_sort['total']:>18,.1f} {always_sort['avg']:>15,.1f} {'-':>10s} {'baseline':>10s} {'-':>12s} {'-':>10s}")
    print(f"{'Perfect Oracle':<25s} {oracle['total']:>18,.1f} {oracle['avg']:>15,.1f} {'-':>10s} {'-':>10s} {'100.0%':>12s} {'0.0%':>10s}")
    
    print()
    
    # Runtime performance comparison (all data)
    print("RUNTIME PERFORMANCE (All Data):")
    print("-" * 100)
    print(f"{'Model':<25s} {'Model Total (ms)':>18s} {'Model Avg (ms)':>15s} {'vs Hash':>10s} {'vs Sort':>10s} {'Efficiency':>12s} {'Overhead':>10s}")
    print("-" * 100)
    
    for results in all_results:
        name = results['model_name']
        timings = results['all_timings']
        
        always_hash = timings['always_hash']
        always_sort = timings['always_sort']
        oracle = timings['oracle']
        model = timings['model']
        
        model_savings_vs_hash = ((always_hash['total'] - model['total']) / always_hash['total']) * 100
        model_savings_vs_sort = ((always_sort['total'] - model['total']) / always_sort['total']) * 100
        model_efficiency = (oracle['total'] / model['total']) * 100 if model['total'] > 0 else 0
        model_overhead = ((model['total'] - oracle['total']) / oracle['total']) * 100 if oracle['total'] > 0 else 0
        
        print(f"{name:<25s} {model['total']:>18,.1f} {model['avg']:>15,.1f} {model_savings_vs_hash:>9.1f}% {model_savings_vs_sort:>9.1f}% {model_efficiency:>11.1f}% {model_overhead:>9.1f}%")
    
    print()

def print_feature_importance(results):
    """Print feature importance for a model."""
    model = results['model']
    feature_names = results['feature_names']
    
    # Get feature importances
    importances = model.feature_importances_
    
    # Sort by importance
    indices = np.argsort(importances)[::-1]
    
    print(f"\nTop 10 Most Important Features ({results['model_name']}):")
    print("-" * 60)
    for i in range(min(10, len(feature_names))):
        idx = indices[i]
        print(f"  {i+1:2d}. {feature_names[idx]:<40s}: {importances[idx]:.4f}")

def print_decision_tree_with_annotations(results, X, best_df):
    """
    Print decision tree with full annotations and explanations.
    Based on iterative_model_trainer.py approach.
    """
    from sklearn.tree import export_text
    import re
    
    model = results['model']
    feature_names = results['feature_names']
    model_name = results['model_name']
    
    print(f"\n{'='*80}")
    print(f"DECISION TREE: {model_name}")
    print(f"{'='*80}")
    print()
    
    # Print feature distributions for context
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
        'BuildDistinctKeys', 
        'BuildCardinalityPct', 
        'BuildProbeRatio',
        'BuildAvgKeyBytes',
        'BuildKeyTypeScore'
    ]
    
    # Add model-specific features
    if 'BuildMaxKeyCount' in feature_names:
        key_features.extend(['BuildMaxKeyCount', 'BuildKeyCountStdDev'])
    if 'ProbeMaxKeyCount' in feature_names:
        key_features.extend(['ProbeMaxKeyCount', 'ProbeKeyCountStdDev'])
    
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
            elif 'Rows' in feature or 'Keys' in feature or 'KeyCount' in feature:
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
    
    # Print legend
    print("="*80)
    print("LEGEND - How to Read the Tree:")
    print("="*80)
    print()
    print("CLASS VALUES (what the model predicts):")
    print("  • class = 0  →  hash_object wins (use hash-based join)")
    print("  • class = 1  →  sort_object_post wins (use sort-based join)")
    print()
    print("KEY TYPE SCORES (BuildKeyTypeScore / MaxKeyTypeScore):")
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
    print("  • BuildCardinalityPct:   (BuildDistinctKeys / BuildRows)")
    print("                           How unique are the build keys?")
    print("                           100% = all unique, <10% = high duplication")
    print("                           Low values (<5%) often favor sort")
    print("  • NumKeyColumns:         Number of key columns (1, 2, or 3)")
    print("  • MixedKeys:             1 if multi-column key with DIFFERENT types")
    print("                           0 if single column OR all same type")
    if 'BuildMaxKeyCount' in feature_names:
        print("  • BuildMaxKeyCount:      Maximum repetitions of any key in build side")
        print("  • BuildKeyCountStdDev:   Standard deviation of key repetitions")
    if 'ProbeMaxKeyCount' in feature_names:
        print("  • ProbeMaxKeyCount:      Maximum repetitions of any key in probe side")
        print("  • ProbeKeyCountStdDev:   Standard deviation of key repetitions")
    print()
    print("-" * 80)
    print()
    
    # Export tree rules
    tree_rules = export_text(model, feature_names=list(feature_names))
    
    # Collapse redundant branches
    def collapse_redundant_branches(tree_text):
        """Collapse branches where all paths lead to same result."""
        def get_tree_depth(line):
            depth = 0
            i = 0
            while i < len(line):
                if line[i:i+4] == '|   ':
                    depth += 1
                    i += 4
                elif line[i:i+4] == '|---':
                    break
                else:
                    i += 1
            return depth
        
        def get_all_leaf_classes(lines, start_idx):
            if start_idx >= len(lines):
                return []
            
            line = lines[start_idx]
            if not line.strip():
                return []
            
            if 'class: 0' in line:
                return [0]
            elif 'class: 1' in line:
                return [1]
            
            depth = get_tree_depth(line)
            classes = []
            
            j = start_idx + 1
            while j < len(lines):
                child_line = lines[j]
                if not child_line.strip():
                    j += 1
                    continue
                
                child_depth = get_tree_depth(child_line)
                if child_depth <= depth:
                    break
                
                if child_depth == depth + 1 and '|---' in child_line:
                    child_classes = get_all_leaf_classes(lines, j)
                    classes.extend(child_classes)
                
                j += 1
            
            return classes
        
        max_iterations = 20
        for iteration in range(max_iterations):
            lines = tree_text.split('\n')
            changed = False
            
            i = len(lines) - 1
            while i >= 0:
                line = lines[i]
                
                if not line.strip() or 'class:' in line:
                    i -= 1
                    continue
                
                if '|---' in line:
                    descendant_classes = get_all_leaf_classes(lines, i)
                    unique_classes = set(descendant_classes)
                    
                    if len(unique_classes) == 1 and len(descendant_classes) > 0:
                        the_class = list(unique_classes)[0]
                        depth = get_tree_depth(line)
                        
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
                        
                        child_prefix = '|   ' * (depth + 1)
                        class_line = child_prefix + f'|--- class: {the_class}'
                        lines.insert(i + 1, class_line)
                        
                        changed = True
                
                i -= 1
            
            tree_text = '\n'.join([l for l in lines if l.strip()])
            
            if not changed:
                break
        
        return tree_text
    
    print("  Collapsing redundant branches...")
    tree_rules = collapse_redundant_branches(tree_rules)
    
    # Add inline annotations
    tree_rules = tree_rules.replace('class: 0', 'class: 0 → HASH wins (use hash_object)')
    tree_rules = tree_rules.replace('class: 1', 'class: 1 → SORT wins (use sort_object_post)')
    
    # Helper function for percentile context
    def get_percentile_context(feature_name, value, X_data):
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
    
    # Annotate lines
    lines = tree_rules.split('\n')
    annotated_lines = []
    for line in lines:
        annotated_line = line
        
        # Annotate key type scores
        if 'KeyTypeScore' in line:
            if '<= 1.50' in line or '> 1.50' in line:
                annotated_line += '  # 1=32-bit only'
            elif '<= 2.50' in line or '> 2.50' in line:
                annotated_line += '  # ≤2: 32-bit(1) or 64-bit(2) only'
            elif '<= 4.50' in line or '> 4.50' in line:
                annotated_line += '  # ≤4: fixed-width only (no strings)'
            elif '<= 8.50' in line or '> 8.50' in line:
                annotated_line += '  # ≤8: string only, >8: mixed with string'
        
        # Annotate cardinality
        if 'BuildCardinalityPct' in line:
            if '<= 0.01' in line or '> 0.01' in line:
                annotated_line += '  # 1% cardinality (very low - sort often wins)'
            elif '<= 0.02' in line or '> 0.02' in line:
                annotated_line += '  # 2% cardinality'
            elif '<= 0.05' in line or '> 0.05' in line:
                annotated_line += '  # 5% cardinality'
            elif '<= 0.10' in line or '> 0.10' in line:
                annotated_line += '  # 10% cardinality'
        
        # Annotate mixed keys
        if 'MixedKeys' in line:
            if '<= 0.50' in line:
                annotated_line += '  # homogeneous (single column OR all same type)'
            elif '> 0.50' in line:
                annotated_line += '  # heterogeneous (different types like int,string)'
        
        # Add percentile context
        match = re.search(r'\|--- (\w+)\s*([<>]=?)\s*([\d.]+)', line)
        if match:
            feature_name = match.group(1)
            threshold = float(match.group(3))
            
            context_features = ['BuildProbeRatio', 'ProbeRows', 'BuildRows', 'TotalRows', 
                              'BuildDistinctKeys', 'BuildCardinalityPct',
                              'BuildMaxKeyCount', 'BuildKeyCountStdDev',
                              'ProbeMaxKeyCount', 'ProbeKeyCountStdDev',
                              'NumKeyColumns', 'BuildAvgKeyBytes']
            
            if feature_name in context_features:
                context = get_percentile_context(feature_name, threshold, X)
                
                if 'Rows' in feature_name or 'Keys' in feature_name or 'KeyCount' in feature_name:
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

# ============================================================================
# Main
# ============================================================================

def main():
    # Parse arguments
    tsv_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_TSV_PATH
    
    print("="*100)
    print("MODEL FEATURE COMPARISON")
    print("="*100)
    print()
    print("Comparing three models with different feature sets:")
    print("  1. Model 1 (Simple):      Basic features (key types, bytes, rows, build distinct keys)")
    print("  2. Model 2 (Build Stats): Model 1 + build max key count + build stddev")
    print("  3. Model 3 (Full Stats):  Model 2 + probe-side key count statistics")
    print()
    print(f"Data source: {tsv_path}")
    print()
    
    # Load data
    best_df = load_and_prepare_data(tsv_path)
    
    # Create target variable (0 = hash_object, 1 = sort_object_post)
    y = (best_df['BestStrategy'] == 'sort_object_post').astype(int)
    
    # Train/test split (use same split for all models for fair comparison)
    indices = np.arange(len(best_df))
    train_indices, test_indices = train_test_split(
        indices, test_size=TEST_SIZE, random_state=42, stratify=y
    )
    
    print(f"Train/test split: {len(train_indices)} train, {len(test_indices)} test")
    print()
    
    # ========================================================================
    # Train all three models
    # ========================================================================
    
    all_results = []
    
    # Model 1: Simple features
    print("="*80)
    print("TRAINING MODEL 1 (Simple Features)")
    print("="*80)
    X1 = create_model1_features(best_df)
    X1_train = X1.iloc[train_indices]
    X1_test = X1.iloc[test_indices]
    y_train = y.iloc[train_indices]
    y_test = y.iloc[test_indices]
    
    print(f"Features: {len(X1.columns)}")
    print(f"  {', '.join(X1.columns.tolist())}")
    print()
    
    results1 = train_and_evaluate_model(
        X1_train, y_train, X1_test, y_test,
        "Model 1 (Simple)", X1.columns.tolist()
    )
    
    # Compute runtime metrics
    y1_all_pred = results1['model'].predict(X1)
    test_timings1, all_timings1 = compute_runtime_metrics(
        best_df, X1, y, X1_test, y_test, y1_all_pred, results1['y_test_pred']
    )
    results1['test_timings'] = test_timings1
    results1['all_timings'] = all_timings1
    
    print_classification_results(results1)
    all_results.append(results1)
    
    # Model 2: Build stats
    print("\n" + "="*80)
    print("TRAINING MODEL 2 (Build Stats)")
    print("="*80)
    X2 = create_model2_features(best_df)
    X2_train = X2.iloc[train_indices]
    X2_test = X2.iloc[test_indices]
    
    print(f"Features: {len(X2.columns)}")
    print(f"  Additional features vs Model 1:")
    new_features2 = set(X2.columns) - set(X1.columns)
    print(f"    {', '.join(sorted(new_features2))}")
    print()
    
    results2 = train_and_evaluate_model(
        X2_train, y_train, X2_test, y_test,
        "Model 2 (Build Stats)", X2.columns.tolist()
    )
    
    # Compute runtime metrics
    y2_all_pred = results2['model'].predict(X2)
    test_timings2, all_timings2 = compute_runtime_metrics(
        best_df, X2, y, X2_test, y_test, y2_all_pred, results2['y_test_pred']
    )
    results2['test_timings'] = test_timings2
    results2['all_timings'] = all_timings2
    
    print_classification_results(results2)
    all_results.append(results2)
    
    # Model 3: Full stats
    print("\n" + "="*80)
    print("TRAINING MODEL 3 (Full Stats)")
    print("="*80)
    X3 = create_model3_features(best_df)
    X3_train = X3.iloc[train_indices]
    X3_test = X3.iloc[test_indices]
    
    print(f"Features: {len(X3.columns)}")
    print(f"  Additional features vs Model 2:")
    new_features3 = set(X3.columns) - set(X2.columns)
    print(f"    {', '.join(sorted(new_features3))}")
    print()
    
    results3 = train_and_evaluate_model(
        X3_train, y_train, X3_test, y_test,
        "Model 3 (Full Stats)", X3.columns.tolist()
    )
    
    # Compute runtime metrics
    y3_all_pred = results3['model'].predict(X3)
    test_timings3, all_timings3 = compute_runtime_metrics(
        best_df, X3, y, X3_test, y_test, y3_all_pred, results3['y_test_pred']
    )
    results3['test_timings'] = test_timings3
    results3['all_timings'] = all_timings3
    
    print_classification_results(results3)
    all_results.append(results3)
    
    # Heuristic: Simple rule based on BuildMaxKeyCount
    print("\n" + "="*80)
    print("EVALUATING SIMPLE HEURISTIC")
    print("="*80)
    print()
    print("Rule: IF BuildMaxKeyCount <= 171,609 THEN HASH ELSE SORT")
    print()
    print("This is a simple heuristic derived from observing the ML models.")
    print("It uses only a single feature and a single threshold.")
    print()
    
    # Only evaluate if BuildMaxKeyCount is available
    if 'BuildMaxKeyCount' in X2.columns:
        X_heuristic = X2[['BuildMaxKeyCount']]  # Only pass BuildMaxKeyCount
        X_heuristic_train = X2_train[['BuildMaxKeyCount']]
        X_heuristic_test = X2_test[['BuildMaxKeyCount']]
        
        results_heuristic = evaluate_heuristic_model(
            X_heuristic_train, y_train, X_heuristic_test, y_test,
            "Simple Heuristic", X_heuristic.columns.tolist(), threshold=171609.0
        )
        
        # Compute runtime metrics
        y_heuristic_all_pred = results_heuristic['model'].predict(X_heuristic)
        test_timings_heuristic, all_timings_heuristic = compute_runtime_metrics(
            best_df, X_heuristic, y, X_heuristic_test, y_test, 
            y_heuristic_all_pred, results_heuristic['y_test_pred']
        )
        results_heuristic['test_timings'] = test_timings_heuristic
        results_heuristic['all_timings'] = all_timings_heuristic
        
        print_classification_results(results_heuristic)
        all_results.append(results_heuristic)
    else:
        print("NOTE: BuildMaxKeyCount not available in dataset.")
        print("      Cannot evaluate this heuristic.")
        print()
    
    # ========================================================================
    # Print comprehensive comparison
    # ========================================================================
    
    print_comparison_table(all_results)
    
    # Print feature importances for each model (skip heuristics)
    print("="*100)
    print("FEATURE IMPORTANCE ANALYSIS")
    print("="*100)
    
    for results in all_results:
        if not results.get('is_heuristic', False):
            print_feature_importance(results)
    
    # Print detailed annotated decision trees for each model
    print("\n" + "="*100)
    print("DETAILED DECISION TREES WITH ANNOTATIONS")
    print("="*100)
    print()
    print("The following sections show the decision tree rules for each model.")
    print("Each tree is annotated with:")
    print("  • Feature value distributions (to understand where splits occur)")
    print("  • Percentile context (e.g., [LOW: 25th %ile] means this split affects 75% of cases)")
    print("  • Inline explanations (what feature values mean)")
    print("  • Simplified structure (redundant branches collapsed)")
    print()
    
    for results in all_results:
        # Skip heuristics (they don't have decision trees)
        if results.get('is_heuristic', False):
            continue
            
        # Get the feature matrix for this model to pass to the print function
        if results['model_name'] == 'Model 1 (Simple)':
            X_model = X1
        elif results['model_name'] == 'Model 2 (Build Stats)':
            X_model = X2
        else:  # Model 3
            X_model = X3
        
        print_decision_tree_with_annotations(results, X_model, best_df)
    
    # ========================================================================
    # Recommendations
    # ========================================================================
    
    print("\n" + "="*100)
    print("RECOMMENDATIONS")
    print("="*100)
    print()
    
    # Find best model by test accuracy
    best_by_accuracy = max(all_results, key=lambda r: r['test_accuracy'])
    
    # Find best model by runtime efficiency
    best_by_runtime = min(all_results, key=lambda r: r['all_timings']['model']['total'])
    
    # Find best model by model efficiency (closest to oracle)
    best_by_efficiency = max(all_results, key=lambda r: 
        (r['all_timings']['oracle']['total'] / r['all_timings']['model']['total']) * 100
    )
    
    print(f"Best Classification Accuracy: {best_by_accuracy['model_name']}")
    print(f"  Test Accuracy: {best_by_accuracy['test_accuracy']:.3f}")
    print()
    
    print(f"Best Runtime Performance: {best_by_runtime['model_name']}")
    print(f"  Total Time: {best_by_runtime['all_timings']['model']['total']:,.1f} ms")
    print()
    
    print(f"Best Model Efficiency: {best_by_efficiency['model_name']}")
    efficiency_pct = (best_by_efficiency['all_timings']['oracle']['total'] / 
                     best_by_efficiency['all_timings']['model']['total']) * 100
    print(f"  Efficiency: {efficiency_pct:.1f}% of Oracle")
    print()
    
    print("Summary:")
    print("  - Model 1 (Simple) uses only basic features that are cheap to compute")
    print("  - Model 2 (Build Stats) adds build-side key count stats (moderate cost)")
    print("  - Model 3 (Full Stats) adds probe-side stats (more expensive to compute)")
    print("  - Simple Heuristic uses a single rule: BuildMaxKeyCount <= 171,609 → HASH")
    print()
    print("  Consider the trade-off between:")
    print("    • Model accuracy/performance")
    print("    • Feature computation cost in production")
    print("    • Model simplicity and interpretability")
    print()
    print("  The simple heuristic is extremely cheap to compute (just one comparison)")
    print("  but may not capture the full complexity of join behavior across all cases.")
    print()


if __name__ == "__main__":
    main()

