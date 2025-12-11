#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Feature Importance Analysis for Random Forest Models
Author: Team 23
Purpose: Extract and visualize feature importance from trained RF models
"""

import os
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

# =============================================================================
# Configuration
# =============================================================================
PROJECT_ROOT = Path.cwd()
DATA_DIR = PROJECT_ROOT / "data"
CSV_DIR = DATA_DIR / "csv"
PLOTS_DIR = DATA_DIR / "plots"
MODELS_DIR = PROJECT_ROOT / "models"

for directory in (CSV_DIR, PLOTS_DIR):
    directory.mkdir(parents=True, exist_ok=True)

MODEL1_PATH = MODELS_DIR / "random_forest_model1_content_only"
MODEL2_PATH = MODELS_DIR / "random_forest_model2_early_comments"

# =============================================================================
# Spark Session (Local Mode - No S3 Needed!)
# =============================================================================

def create_spark_session():
    """Create local Spark session for loading models."""
    spark = (
        SparkSession.builder
        .appName("Feature_Importance_Analysis")
        .master("local[*]")  # Local mode - no cluster needed!
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark session created (local mode)")
    return spark


# =============================================================================
# Feature Name Mapping
# =============================================================================

def get_feature_names_model1():
    """
    Define feature names for Model 1.

    Model 1 features (in order they appear in the feature vector):
    - TF-IDF features (5000 dimensions from HashingTF)
    - Subreddit one-hot encoding (varies by number of unique subreddits)
    - Numeric features: title_len, selftext_len, post_hour, post_dow
    """
    feature_names = []

    # TF-IDF features (5000 hashed features)
    # Note: HashingTF doesn't preserve word mappings, so we label them generically
    for i in range(5000):
        feature_names.append(f"tfidf_{i}")

    # Subreddit features (one-hot encoded)
    # The exact number depends on unique subreddits in training data
    # We'll determine this dynamically from the model

    # Numeric features (always last 4 features for Model 1)
    numeric_features = [
        "title_length",
        "selftext_length",
        "posting_hour",
        "day_of_week"
    ]

    return feature_names, numeric_features


def get_feature_names_model2():
    """
    Define feature names for Model 2.

    Model 2 features (same as Model 1 + early comment features):
    - All Model 1 features
    - Early comment features: count, avg_score, max_score, avg_length, score_std
    """
    feature_names, numeric_features = get_feature_names_model1()

    # Add early comment features
    early_comment_features = [
        "early_comment_count",
        "early_comment_avg_score",
        "early_comment_max_score",
        "early_comment_avg_length",
        "early_comment_score_std"
    ]

    return feature_names, numeric_features + early_comment_features


# =============================================================================
# Feature Importance Extraction
# =============================================================================

def extract_feature_importance(model_path, model_name, feature_groups):
    """
    Load a trained model and extract feature importances.

    Args:
        model_path: Path to saved PipelineModel
        model_name: String identifier for the model
        feature_groups: Dict mapping feature group names to their index ranges

    Returns:
        DataFrame with feature importances
    """
    print(f"\n{'='*80}")
    print(f"Processing {model_name}")
    print(f"{'='*80}")

    # Load the pipeline model
    print(f"Loading model from {model_path}...")
    pipeline_model = PipelineModel.load(str(model_path))

    # The Random Forest model is the last stage of the pipeline
    rf_model = pipeline_model.stages[-1]

    # Extract feature importances (DenseVector)
    importances = rf_model.featureImportances
    n_features = len(importances)
    print(f"Total features: {n_features}")

    # Convert to dense array
    importance_array = importances.toArray()

    # Create feature importance dataframe
    importance_data = []

    # Group features by type for better interpretability
    for group_name, (start_idx, end_idx) in feature_groups.items():
        if end_idx > n_features:
            end_idx = n_features

        group_importances = importance_array[start_idx:end_idx]
        group_total = group_importances.sum()

        print(f"  {group_name}: {len(group_importances)} features, total importance = {group_total:.4f}")

        for i, imp in enumerate(group_importances):
            if group_name == "TF-IDF":
                feature_name = f"tfidf_term_{i}"
            elif group_name == "Subreddit":
                feature_name = f"subreddit_{i}"
            else:
                # For named features, we'll update this later
                feature_name = f"{group_name}_{i}"

            importance_data.append({
                'feature': feature_name,
                'feature_group': group_name,
                'importance': imp,
                'importance_pct': imp * 100
            })

    # Create DataFrame and sort
    df = pd.DataFrame(importance_data)
    df = df.sort_values('importance', ascending=False).reset_index(drop=True)

    # Print top features
    print(f"\nTop 10 most important features:")
    print(df.head(10)[['feature', 'feature_group', 'importance_pct']].to_string(index=False))

    # Print group-level summary
    print(f"\nImportance by Feature Group:")
    group_summary = df.groupby('feature_group')['importance'].sum().sort_values(ascending=False)
    for group, imp in group_summary.items():
        print(f"  {group:20s}: {imp:.4f} ({imp*100:.2f}%)")

    return df, group_summary


# =============================================================================
# Visualization
# =============================================================================

def plot_feature_importance(df, group_summary, model_name, top_n=20):
    """Create feature importance visualizations."""

    # Figure with 2 subplots
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Plot 1: Top N individual features
    ax1 = axes[0]
    top_features = df.head(top_n)
    colors = top_features['feature_group'].map({
        'TF-IDF': '#1f77b4',
        'Subreddit': '#ff7f0e',
        'Temporal': '#2ca02c',
        'Text_Length': '#d62728',
        'Early_Comments': '#9467bd'
    })

    ax1.barh(range(len(top_features)), top_features['importance_pct'], color=colors)
    ax1.set_yticks(range(len(top_features)))
    ax1.set_yticklabels(top_features['feature'].str[:30])  # Truncate long names
    ax1.set_xlabel('Importance (%)', fontweight='bold', fontsize=12)
    ax1.set_title(f'Top {top_n} Most Important Features - {model_name}',
                  fontweight='bold', fontsize=14)
    ax1.invert_yaxis()
    ax1.grid(axis='x', alpha=0.3)

    # Add value labels
    for i, (idx, row) in enumerate(top_features.iterrows()):
        ax1.text(row['importance_pct'] + 0.1, i, f"{row['importance_pct']:.2f}%",
                va='center', fontsize=9)

    # Plot 2: Feature group importance
    ax2 = axes[1]
    group_pct = (group_summary * 100).sort_values(ascending=True)
    colors_group = [
        '#1f77b4' if 'TF-IDF' in x else
        '#ff7f0e' if 'Subreddit' in x else
        '#2ca02c' if 'Temporal' in x else
        '#d62728' if 'Text' in x else
        '#9467bd'
        for x in group_pct.index
    ]

    ax2.barh(range(len(group_pct)), group_pct.values, color=colors_group)
    ax2.set_yticks(range(len(group_pct)))
    ax2.set_yticklabels(group_pct.index)
    ax2.set_xlabel('Total Importance (%)', fontweight='bold', fontsize=12)
    ax2.set_title(f'Feature Group Importance - {model_name}',
                  fontweight='bold', fontsize=14)
    ax2.grid(axis='x', alpha=0.3)

    # Add value labels
    for i, (group, pct) in enumerate(group_pct.items()):
        ax2.text(pct + 1, i, f"{pct:.1f}%", va='center', fontweight='bold', fontsize=10)

    plt.tight_layout()

    # Save
    filename = f"ml_feature_importance_{model_name.lower().replace(' ', '_')}.png"
    plt.savefig(PLOTS_DIR / filename, dpi=300, bbox_inches='tight')
    print(f"\n✅ Saved visualization: {PLOTS_DIR / filename}")
    plt.close()


# =============================================================================
# Main Analysis
# =============================================================================

def analyze_model1(spark):
    """Analyze Model 1 (Content-Only)."""

    # Define feature groups for Model 1
    # Based on the pipeline: TF-IDF (5000) + Subreddit (variable) + 4 numeric features
    # We need to load the model first to determine exact subreddit encoding size

    pipeline_model = PipelineModel.load(str(MODEL1_PATH))
    rf_model = pipeline_model.stages[-1]
    n_features = rf_model.numFeatures

    print(f"\nModel 1 has {n_features} total features")

    # Feature groups (approximate - exact subreddit count varies)
    # TF-IDF: 5000 features
    # Subreddit: remaining features minus 4 numeric
    # Numeric: last 4 features

    n_subreddit_features = n_features - 5000 - 4

    feature_groups = {
        'TF-IDF': (0, 5000),
        'Subreddit': (5000, 5000 + n_subreddit_features),
        'Text_Length': (5000 + n_subreddit_features, 5000 + n_subreddit_features + 2),
        'Temporal': (5000 + n_subreddit_features + 2, n_features)
    }

    # Extract importance
    df, group_summary = extract_feature_importance(
        MODEL1_PATH,
        "Model 1 (Content-Only)",
        feature_groups
    )

    # Add readable names for last 4 features
    numeric_names = ["title_length", "selftext_length", "posting_hour", "day_of_week"]
    start_idx = 5000 + n_subreddit_features
    for i, name in enumerate(numeric_names):
        mask = df['feature'] == f"Text_Length_{i}" if i < 2 else f"Temporal_{i-2}"
        df.loc[mask, 'feature'] = name

    # Save CSV
    csv_path = CSV_DIR / "ml_feature_importance_model1_content_only.csv"
    df.to_csv(csv_path, index=False)
    print(f"✅ Saved CSV: {csv_path}")

    # Plot
    plot_feature_importance(df, group_summary, "Model 1 (Content-Only)")

    return df, group_summary


def analyze_model2(spark):
    """Analyze Model 2 (Content + Early Comments)."""

    pipeline_model = PipelineModel.load(str(MODEL2_PATH))
    rf_model = pipeline_model.stages[-1]
    n_features = rf_model.numFeatures

    print(f"\nModel 2 has {n_features} total features")

    # Model 2 has same structure as Model 1 + 5 early comment features
    n_subreddit_features = n_features - 5000 - 4 - 5

    feature_groups = {
        'TF-IDF': (0, 5000),
        'Subreddit': (5000, 5000 + n_subreddit_features),
        'Text_Length': (5000 + n_subreddit_features, 5000 + n_subreddit_features + 2),
        'Temporal': (5000 + n_subreddit_features + 2, 5000 + n_subreddit_features + 4),
        'Early_Comments': (5000 + n_subreddit_features + 4, n_features)
    }

    # Extract importance
    df, group_summary = extract_feature_importance(
        MODEL2_PATH,
        "Model 2 (Content + Early Comments)",
        feature_groups
    )

    # Add readable names
    numeric_names = ["title_length", "selftext_length", "posting_hour", "day_of_week"]
    early_names = ["early_comment_count", "early_comment_avg_score",
                   "early_comment_max_score", "early_comment_avg_length",
                   "early_comment_score_std"]

    # Update text length and temporal features
    start_idx = 5000 + n_subreddit_features
    for i, name in enumerate(numeric_names):
        mask = df['feature'] == f"Text_Length_{i}" if i < 2 else f"Temporal_{i-2}"
        df.loc[mask, 'feature'] = name

    # Update early comment features
    for i, name in enumerate(early_names):
        mask = df['feature'] == f"Early_Comments_{i}"
        df.loc[mask, 'feature'] = name

    # Save CSV
    csv_path = CSV_DIR / "ml_feature_importance_model2_early_comments.csv"
    df.to_csv(csv_path, index=False)
    print(f"✅ Saved CSV: {csv_path}")

    # Plot
    plot_feature_importance(df, group_summary, "Model 2 (Content + Early Comments)")

    return df, group_summary


# =============================================================================
# Comparison Analysis
# =============================================================================

def compare_models(summary1, summary2):
    """Create side-by-side comparison of feature group importance."""

    # Combine summaries
    comparison = pd.DataFrame({
        'Model 1 (Content-Only)': summary1 * 100,
        'Model 2 (Content + Early Comments)': summary2 * 100
    }).fillna(0)

    print("\n" + "="*80)
    print("FEATURE GROUP IMPORTANCE COMPARISON")
    print("="*80)
    print(comparison.to_string())

    # Visualize
    fig, ax = plt.subplots(figsize=(10, 6))
    comparison.plot(kind='barh', ax=ax, color=['#1f77b4', '#ff7f0e'])
    ax.set_xlabel('Total Importance (%)', fontweight='bold', fontsize=12)
    ax.set_ylabel('Feature Group', fontweight='bold', fontsize=12)
    ax.set_title('Feature Group Importance Comparison Across Models',
                 fontweight='bold', fontsize=14)
    ax.legend(loc='best', frameon=True, shadow=True)
    ax.grid(axis='x', alpha=0.3)

    # Add value labels
    for container in ax.containers:
        ax.bar_label(container, fmt='%.1f%%', padding=3)

    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "ml_feature_importance_comparison.png",
                dpi=300, bbox_inches='tight')
    print(f"\n✅ Saved comparison plot: {PLOTS_DIR / 'ml_feature_importance_comparison.png'}")
    plt.close()

    # Save comparison CSV
    comparison.to_csv(CSV_DIR / "ml_feature_importance_comparison.csv")
    print(f"✅ Saved comparison CSV: {CSV_DIR / 'ml_feature_importance_comparison.csv'}")


# =============================================================================
# Main Execution
# =============================================================================

def main():
    """Main execution pipeline."""
    print("\n" + "="*80)
    print("FEATURE IMPORTANCE ANALYSIS")
    print("Random Forest Models - Engagement Prediction")
    print("="*80)

    spark = create_spark_session()

    try:
        # Analyze Model 1
        df1, summary1 = analyze_model1(spark)

        # Analyze Model 2
        df2, summary2 = analyze_model2(spark)

        # Compare models
        compare_models(summary1, summary2)

        print("\n" + "="*80)
        print("✅ FEATURE IMPORTANCE ANALYSIS COMPLETE")
        print("="*80)
        print("\nGenerated Files:")
        print(f"  CSVs: {CSV_DIR}")
        print(f"  - ml_feature_importance_model1_content_only.csv")
        print(f"  - ml_feature_importance_model2_early_comments.csv")
        print(f"  - ml_feature_importance_comparison.csv")
        print(f"\n  Plots: {PLOTS_DIR}")
        print(f"  - ml_feature_importance_model_1_(content-only).png")
        print(f"  - ml_feature_importance_model_2_(content_+_early_comments).png")
        print(f"  - ml_feature_importance_comparison.png")

        # Generate key insights for the write-up
        print("\n" + "="*80)
        print("KEY INSIGHTS FOR ML.QMD")
        print("="*80)

        print("\n1. Model 1 (Content-Only):")
        print(f"   Top feature group: {summary1.idxmax()} ({summary1.max()*100:.1f}%)")

        print("\n2. Model 2 (Content + Early Comments):")
        print(f"   Top feature group: {summary2.idxmax()} ({summary2.max()*100:.1f}%)")

        if 'Early_Comments' in summary2:
            early_importance = summary2['Early_Comments'] * 100
            print(f"\n3. Early comment features contribute {early_importance:.1f}% to Model 2")
            print(f"   This explains the R² improvement from 0.12 to 0.48!")

    finally:
        spark.stop()
        print("\n✅ Spark session stopped")


if __name__ == "__main__":
    main()
