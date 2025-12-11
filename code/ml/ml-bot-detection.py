#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Q8: Bot and Promotional Content Detection
Identify suspicious/promotional accounts vs. authentic users in audio product discussions

Author: Team 23 - jl3205
Business Question: Can we detect bots and promotional content from posting patterns?
"""
import logging
from pathlib import Path

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, avg as spark_avg, stddev, min as spark_min,
    max as spark_max, length, sum as spark_sum, datediff, hour, when, lit,
    from_unixtime, to_date
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# =============================================================================
# Configuration
# =============================================================================
PROJECT_ROOT = Path.cwd()
DATA_DIR = PROJECT_ROOT / "data"
CSV_DIR = DATA_DIR / "csv"
PLOTS_DIR = DATA_DIR / "plots"
MODELS_DIR = PROJECT_ROOT / "models"

for directory in (CSV_DIR, PLOTS_DIR, MODELS_DIR):
    directory.mkdir(parents=True, exist_ok=True)

SUBMISSIONS_PATH = str(DATA_DIR / "submissions")
COMMENTS_PATH = str(DATA_DIR / "comments")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# =============================================================================
# Spark Session
# =============================================================================

def create_spark_session(app_name: str = "Bot_Detection") -> SparkSession:
    """Initialize Spark session."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # Use local mode with all cores
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark Version: {spark.version}")
    return spark


# =============================================================================
# Feature Engineering
# =============================================================================

def engineer_user_behavioral_features(comments, submissions):
    """
    Engineer user-level behavioral features for bot detection.

    Suspicious patterns:
    - High posting frequency
    - Low subreddit diversity (focused on single brand)
    - Very short/generic comments
    - Consistent positive sentiment (promotional)
    - Low engagement (few upvotes)
    - Posting at odd hours
    """
    logger.info("Engineering user behavioral features...")

    # Audio-related subreddits
    audio_subs = [
        'headphones', 'audiophile', 'Earbuds', 'audio', 'AirPods',
        'airpods', 'SonyHeadphones', 'bose', 'JBL', 'bluetooth',
        'sony', 'technology', 'gadgets', 'samsung', 'GooglePixel',
        'Android', 'apple'
    ]

    comments_filtered = comments.filter(col("subreddit").isin(audio_subs))

    # User-level aggregations
    user_features = comments_filtered.groupBy("author").agg(
        # Posting volume
        count("*").alias("total_comments"),
        countDistinct("subreddit").alias("subreddit_diversity"),
        countDistinct(to_date(from_unixtime(col("created_utc")))).alias("active_days"),

        # Temporal patterns
        stddev(hour(from_unixtime(col("created_utc")))).alias("posting_hour_variance"),
        spark_avg(hour(from_unixtime(col("created_utc")))).alias("avg_posting_hour"),

        # Engagement patterns
        spark_avg("score").alias("avg_comment_score"),
        stddev("score").alias("score_variance"),
        spark_sum(when(col("score") > 1, 1).otherwise(0)).alias("upvoted_comments"),

        # Text patterns
        spark_avg(length("body")).alias("avg_comment_length"),
        stddev(length("body")).alias("text_length_variance"),
        spark_sum(when(length("body") < 20, 1).otherwise(0)).alias("very_short_comments"),

        # Activity span
        spark_min("created_utc").alias("first_comment"),
        spark_max("created_utc").alias("last_comment")
    )

    # Calculate derived features
    user_features = user_features.withColumn(
        "activity_span_days",
        datediff(to_date(from_unixtime(col("last_comment"))), to_date(from_unixtime(col("first_comment"))))
    )

    user_features = user_features.withColumn(
        "comments_per_day",
        when(col("activity_span_days") > 0,
             col("total_comments") / col("activity_span_days"))
        .otherwise(col("total_comments"))
    )

    user_features = user_features.withColumn(
        "upvote_rate",
        when(col("total_comments") > 0,
             col("upvoted_comments") / col("total_comments"))
        .otherwise(0)
    )

    user_features = user_features.withColumn(
        "short_comment_rate",
        when(col("total_comments") > 0,
             col("very_short_comments") / col("total_comments"))
        .otherwise(0)
    )

    # Fill nulls
    user_features = user_features.fillna(0, subset=[
        "posting_hour_variance", "score_variance", "text_length_variance"
    ])

    logger.info(f"Engineered features for {user_features.count():,} users")

    return user_features


def label_suspicious_users(user_features):
    """
    Label users as suspicious (1) or authentic (0) using rule-based heuristics.

    Suspicious indicators:
    - High volume + low diversity (brand spam)
    - Very short comments + low engagement
    - Consistent positive language (promotional)
    """
    logger.info("Labeling suspicious users...")

    labeled = user_features.withColumn(
        "is_suspicious",
        when(
            # High-volume, low-diversity accounts
            ((col("total_comments") > 100) & (col("subreddit_diversity") < 3)) |
            # Short generic comments with low engagement
            ((col("short_comment_rate") > 0.7) & (col("avg_comment_score") < 2)) |
            # Very high posting frequency
            (col("comments_per_day") > 20) |
            # Zero variance in posting time (bot-like)
            (col("posting_hour_variance") < 1)
            , 1
        ).otherwise(0)
    )

    # Count distribution
    class_dist = labeled.groupBy("is_suspicious").count().collect()
    logger.info("Label distribution:")
    for row in class_dist:
        label = "Suspicious" if row['is_suspicious'] == 1 else "Authentic"
        logger.info(f"  {label}: {row['count']:,}")

    return labeled


# =============================================================================
# Model Training
# =============================================================================

def train_bot_detector(df):
    """
    Train Random Forest classifier for bot detection.
    """
    logger.info("\n" + "="*80)
    logger.info("TRAINING BOT DETECTION MODEL")
    logger.info("="*80)

    # Select features for model
    feature_cols = [
        "total_comments", "subreddit_diversity", "active_days",
        "posting_hour_variance", "avg_posting_hour",
        "avg_comment_score", "score_variance", "upvoted_comments",
        "avg_comment_length", "text_length_variance",
        "activity_span_days", "comments_per_day",
        "upvote_rate", "short_comment_rate"
    ]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features")

    classifier = RandomForestClassifier(
        featuresCol="features",
        labelCol="is_suspicious",
        numTrees=100,
        maxDepth=8,
        seed=42
    )

    pipeline = Pipeline(stages=[assembler, scaler, classifier])

    # Train/test split
    train, test = df.randomSplit([0.8, 0.2], seed=42)
    logger.info(f"Training set: {train.count():,} users")
    logger.info(f"Test set: {test.count():,} users")

    # Train
    logger.info("Training Random Forest...")
    model = pipeline.fit(train)

    # Predict
    predictions = model.transform(test)

    # Evaluate
    metrics = evaluate_bot_detection(predictions)

    # Save model
    model_path = MODELS_DIR / "bot_detector_model"
    model.write().overwrite().save(str(model_path))
    logger.info(f"Model saved to {model_path}")

    # Feature importance
    rf_model = model.stages[-1]
    importances = rf_model.featureImportances.toArray()

    feature_importance_df = pd.DataFrame({
        'feature': feature_cols,
        'importance': importances
    }).sort_values('importance', ascending=False)

    logger.info("\nTop 10 Most Important Features:")
    print(feature_importance_df.head(10).to_string(index=False))

    # Save feature importance
    feature_importance_df.to_csv(
        CSV_DIR / "ml_bot_detection_feature_importance.csv",
        index=False
    )

    return model, predictions, metrics, feature_importance_df


def evaluate_bot_detection(predictions):
    """
    Evaluate bot detection classifier.
    """
    logger.info("\nEvaluating Bot Detection Model...")

    # Binary metrics
    binary_eval = BinaryClassificationEvaluator(
        labelCol="is_suspicious",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    auc_roc = binary_eval.evaluate(predictions)

    binary_eval.setMetricName("areaUnderPR")
    auc_pr = binary_eval.evaluate(predictions)

    # Multiclass metrics
    accuracy_eval = MulticlassClassificationEvaluator(
        labelCol="is_suspicious",
        predictionCol="prediction",
        metricName="accuracy"
    )
    accuracy = accuracy_eval.evaluate(predictions)

    f1_eval = MulticlassClassificationEvaluator(labelCol="is_suspicious", metricName="f1")
    f1 = f1_eval.evaluate(predictions)

    precision_eval = MulticlassClassificationEvaluator(labelCol="is_suspicious", metricName="weightedPrecision")
    precision = precision_eval.evaluate(predictions)

    recall_eval = MulticlassClassificationEvaluator(labelCol="is_suspicious", metricName="weightedRecall")
    recall = recall_eval.evaluate(predictions)

    metrics = {
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'f1': f1,
        'auc_roc': auc_roc,
        'auc_pr': auc_pr
    }

    logger.info(f"\nBot Detection Metrics:")
    logger.info(f"  Accuracy:  {accuracy:.4f}")
    logger.info(f"  Precision: {precision:.4f} (% of flagged users that are actually suspicious)")
    logger.info(f"  Recall:    {recall:.4f} (% of suspicious users correctly identified)")
    logger.info(f"  F1 Score:  {f1:.4f}")
    logger.info(f"  AUC-ROC:   {auc_roc:.4f}")
    logger.info(f"  AUC-PR:    {auc_pr:.4f}")

    return metrics


# =============================================================================
# Visualizations
# =============================================================================

def generate_visualizations(predictions, feature_importance_df):
    """
    Generate confusion matrix and feature importance plot.
    """
    logger.info("\nGenerating visualizations...")

    # Confusion matrix
    cm_data = predictions.groupBy("is_suspicious", "prediction").count().toPandas()
    cm = cm_data.pivot(index='is_suspicious', columns='prediction', values='count').fillna(0)

    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt='g', cmap='Reds', cbar_kws={'label': 'Count'})
    plt.title('Bot Detection Confusion Matrix', fontweight='bold', fontsize=14)
    plt.ylabel('Actual Label', fontweight='bold')
    plt.xlabel('Predicted Label', fontweight='bold')
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "ml_bot_detection_confusion_matrix.png", dpi=300, bbox_inches='tight')
    logger.info(f"Saved: {PLOTS_DIR / 'ml_bot_detection_confusion_matrix.png'}")
    plt.close()

    # Feature importance
    top_features = feature_importance_df.head(10)

    plt.figure(figsize=(10, 6))
    plt.barh(range(len(top_features)), top_features['importance'], color='steelblue')
    plt.yticks(range(len(top_features)), top_features['feature'])
    plt.xlabel('Feature Importance', fontweight='bold')
    plt.title('Top 10 Features for Bot Detection', fontweight='bold', fontsize=14)
    plt.gca().invert_yaxis()
    plt.grid(axis='x', alpha=0.3)

    for i, (idx, row) in enumerate(top_features.iterrows()):
        plt.text(row['importance'] + 0.005, i, f"{row['importance']:.3f}",
                va='center', fontsize=9)

    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "ml_bot_detection_feature_importance.png", dpi=300, bbox_inches='tight')
    logger.info(f"Saved: {PLOTS_DIR / 'ml_bot_detection_feature_importance.png'}")
    plt.close()


def save_results(metrics):
    """
    Save bot detection results.
    """
    df = pd.DataFrame([metrics])
    csv_path = CSV_DIR / "ml_bot_detection_metrics.csv"
    df.to_csv(csv_path, index=False)
    logger.info(f"\nSaved metrics to {csv_path}")

    print("\n" + "="*80)
    print("BOT DETECTION RESULTS")
    print("="*80)
    print(df.to_string(index=False))
    print("="*80)


# =============================================================================
# Main Execution
# =============================================================================

def main():
    """
    Main execution pipeline.
    """
    spark = create_spark_session()

    try:
        # Load data
        logger.info("Loading comments and submissions...")
        comments = spark.read.parquet(COMMENTS_PATH)
        submissions = spark.read.parquet(SUBMISSIONS_PATH)

        # Engineer features
        user_features = engineer_user_behavioral_features(comments, submissions)

        # Label data
        labeled_data = label_suspicious_users(user_features)

        # Train model
        model, predictions, metrics, feature_importance_df = train_bot_detector(labeled_data)

        # Visualizations
        generate_visualizations(predictions, feature_importance_df)

        # Save results
        save_results(metrics)

        logger.info("\n" + "="*80)
        logger.info("✅ BOT DETECTION COMPLETE")
        logger.info("="*80)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
