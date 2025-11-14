#!/usr/bin/env python3
"""
Compare different weighting strategies to find the best one for your data.

Usage: python compare_strategies.py /path/to/benchmark_results.tsv
"""

import sys
import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score

# Import from the main script
sys.path.insert(0, os.path.dirname(__file__))
from iterative_model_trainer import (
    load_and_filter_data,
    find_best_strategy_per_config,
    create_build_probe_features,
    compute_sample_weights
)

def compare_weighting_strategies(tsv_path):
    """Compare all weighting strategies and show which performs best."""
    
    print("="*80)
    print("WEIGHTING STRATEGY COMPARISON")
    print("="*80)
    print()
    
    # Load data
    df = load_and_filter_data(tsv_path)
    best_df = find_best_strategy_per_config(df)
    
    # Create features
    X = create_build_probe_features(best_df)
    y = (best_df['BestStrategy'] == 'sort_object_post').astype(int)
    
    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.10, random_state=42, stratify=y if y.nunique() > 1 else None
    )
    
    # Get actual times for cost calculation
    test_df = best_df.loc[X_test.index]
    
    strategies = [
        'none',
        'linear', 
        'exponential_pct',
        'exponential',
        'hybrid',
        'quadratic',
        'threshold'
    ]
    
    results = []
    
    for strategy in strategies:
        print(f"Testing: {strategy}")
        
        # Compute weights
        if strategy == 'none':
            weights_train = np.ones(len(X_train))
            weights_test = np.ones(len(X_test))
        else:
            all_weights = compute_sample_weights(best_df, weight_strategy=strategy)
            weights_train = all_weights.loc[X_train.index].values
            weights_test = all_weights.loc[X_test.index].values
        
        # Train model
        dt = DecisionTreeClassifier(
            max_depth=6,
            min_samples_split=10,
            min_samples_leaf=5,
            random_state=42
        )
        dt.fit(X_train, y_train, sample_weight=weights_train)
        
        # Evaluate
        y_test_pred = dt.predict(X_test)
        
        # Standard accuracy
        test_acc = accuracy_score(y_test, y_test_pred)
        
        # Weighted accuracy
        weighted_acc = accuracy_score(y_test, y_test_pred, sample_weight=weights_test)
        
        # Cost analysis (total time wasted by wrong predictions)
        test_df_reset = test_df.reset_index(drop=True)
        total_cost_ms = 0
        error_count = 0
        
        for idx in range(len(test_df_reset)):
            row = test_df_reset.iloc[idx]
            pred = y_test_pred[idx]
            actual = y_test.iloc[idx]
            
            if pred != actual:
                error_count += 1
                if row['BestStrategy'] == 'hash_object':
                    hash_time = row['BestTime']
                    sort_time = row['AlternativeTime']
                else:
                    sort_time = row['BestTime']
                    hash_time = row['AlternativeTime']
                
                if pred == 0:  # Predicted hash
                    cost_ms = hash_time - sort_time
                else:  # Predicted sort
                    cost_ms = sort_time - hash_time
                
                total_cost_ms += cost_ms
        
        results.append({
            'strategy': strategy,
            'test_accuracy': test_acc,
            'weighted_accuracy': weighted_acc,
            'total_cost_ms': total_cost_ms,
            'avg_error_cost_ms': total_cost_ms / max(error_count, 1),
            'error_count': error_count
        })
    
    # Print results
    print()
    print("="*80)
    print("RESULTS")
    print("="*80)
    print()
    
    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('total_cost_ms')
    
    print(f"{'Strategy':<18s} {'Test Acc':>9s} {'Weighted':>9s} {'Total Cost':>12s} {'Avg Error':>11s} {'Errors':>7s}")
    print("-" * 80)
    
    for _, row in results_df.iterrows():
        marker = " ← BEST" if row['total_cost_ms'] == results_df['total_cost_ms'].min() else ""
        print(f"{row['strategy']:<18s} {row['test_accuracy']:>9.1%} {row['weighted_accuracy']:>9.1%} "
              f"{row['total_cost_ms']:>11.2f}ms {row['avg_error_cost_ms']:>10.2f}ms "
              f"{int(row['error_count']):>7d}{marker}")
    
    print()
    print("INTERPRETATION:")
    print("  - Test Acc: Standard accuracy on test set")
    print("  - Weighted: Accuracy weighted by importance (time difference)")
    print("  - Total Cost: Total milliseconds wasted by wrong predictions (LOWER IS BETTER)")
    print("  - Avg Error: Average cost per wrong prediction")
    print("  - Errors: Number of wrong predictions")
    print()
    
    best_strategy = results_df.iloc[0]['strategy']
    print(f"RECOMMENDED: WEIGHT_STRATEGY = '{best_strategy}'")
    print()
    
    # Compare to baselines
    print("Comparison to always-hash / always-sort:")
    
    # Calculate always-hash and always-sort costs
    always_hash_cost = 0
    always_sort_cost = 0
    
    for idx in range(len(test_df_reset)):
        row = test_df_reset.iloc[idx]
        
        if row['BestStrategy'] == 'hash_object':
            hash_time = row['BestTime']
            sort_time = row['AlternativeTime']
        else:
            sort_time = row['BestTime']
            hash_time = row['AlternativeTime']
        
        # Always hash cost: difference when hash is slower
        if hash_time > sort_time:
            always_hash_cost += (hash_time - sort_time)
        
        # Always sort cost: difference when sort is slower
        if sort_time > hash_time:
            always_sort_cost += (sort_time - hash_time)
    
    best_cost = results_df.iloc[0]['total_cost_ms']
    none_cost = results_df[results_df['strategy'] == 'none']['total_cost_ms'].values[0]
    
    print(f"  Always HASH cost:   {always_hash_cost:>11.2f}ms")
    print(f"  Always SORT cost:   {always_sort_cost:>11.2f}ms")
    print(f"  Model (none):       {none_cost:>11.2f}ms (saves {(min(always_hash_cost, always_sort_cost) - none_cost) / min(always_hash_cost, always_sort_cost) * 100:.1f}% vs best baseline)")
    print(f"  Model ({best_strategy}): {best_cost:>11.2f}ms (saves {(min(always_hash_cost, always_sort_cost) - best_cost) / min(always_hash_cost, always_sort_cost) * 100:.1f}% vs best baseline)")
    print()
    
    if best_cost >= none_cost:
        print("⚠ WARNING: Weighting is making things WORSE!")
        print("  → Use WEIGHT_STRATEGY = 'none' (no weighting)")
        print("  → Or your data may not have enough high-impact cases")
        print("  → Run diagnose_weighting.py to understand why")
    else:
        savings = (none_cost - best_cost) / none_cost * 100
        print(f"✓ Weighting helps! Saves {savings:.1f}% of error cost vs unweighted")
    print()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        tsv_path = sys.argv[1]
    else:
        tsv_path = "/data/tmp/simple_hash_vs_sort/benchmark_results.tsv"
    
    compare_weighting_strategies(tsv_path)

