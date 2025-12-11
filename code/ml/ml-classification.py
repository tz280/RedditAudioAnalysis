#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
from pathlib import Path

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, length, log1p, concat_ws, hour, dayofweek, count, avg as spark_avg,
    stddev, when, expr, percent_rank, lit, from_unixtime, to_timestamp
)
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Tokenizer, StopWordsRemover, HashingTF, IDF,
    StringIndexer, OneHotEncoder, VectorAssembler
)
from pyspark.ml.classification import RandomForestClassifier
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

def create_spark_session(app_name: str = "Viral_Post_Classification") -> SparkSession:
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
# Data Loading
# =============================================================================

def load_and_prepare_data(spark):
    """
    Load submissions and comments, engineer features, define viral label.
    """
    logger.info("Loading submissions...")
    submissions = spark.read.parquet(SUBMISSIONS_PATH)

    logger.info("Loading comments...")
    comments = spark.read.parquet(COMMENTS_PATH)

    # Filter audio-related subreddits
    audio_subs = [
        'headphones', 'audiophile', 'Earbuds', 'audio', 'AirPods',
        'airpods', 'SonyHeadphones', 'bose', 'JBL', 'bluetooth',
        'sony', 'technology', 'gadgets', 'samsung', 'GooglePixel',
        'Android', 'apple', 'running', 'BuyItForLife', 'Costco'
    ]

    submissions = submissions.filter(col("subreddit").isin(audio_subs))

    # Basic features
    submissions = submissions.withColumn("log_score", log1p(col("score")))
    submissions = submissions.withColumn("title_len", length(col("title")))
    submissions = submissions.withColumn("selftext_len", length(col("selftext")))
    # Convert Unix timestamp to timestamp for time-based functions
    submissions = submissions.withColumn("timestamp", to_timestamp(from_unixtime(col("created_utc"))))
    submissions = submissions.withColumn("post_hour", hour(col("timestamp")))
    submissions = submissions.withColumn("post_dow", dayofweek(col("timestamp")))

    # Combine title + selftext for TF-IDF
    submissions = submissions.withColumn(
        "combined_text",
        concat_ws(" ", col("title"), col("selftext"))
    )

    # Calculate early comments (first hour)
    logger.info("Calculating early comment features...")
    # First, create a mapping of submission id to created_utc
    submission_times = submissions.select(
        col("id").alias("sub_id"),
        col("created_utc").alias("post_created_utc")
    )

    # Join comments with submission times
    comments_with_post_time = comments.join(
        submission_times,
        comments.link_id == submission_times.sub_id,
        "inner"
    )

    # Filter for early comments (within first hour)
    early_comments = comments_with_post_time.filter(
        (col("created_utc") - col("post_created_utc")) <= 3600
    ).groupBy("link_id").agg(
        count("*").alias("early_comment_count"),
        spark_avg("score").alias("early_comment_avg_score"),
        stddev("score").alias("early_comment_score_std")
    )

    # Join with submissions
    submissions = submissions.join(early_comments, submissions.id == early_comments.link_id, "left")
    submissions = submissions.fillna(0, subset=["early_comment_count", "early_comment_avg_score", "early_comment_score_std"])

    # Define VIRAL label: Top 10% by log_score
    logger.info("Defining viral label (top 10% by engagement)...")
    windowSpec = Window.orderBy(col("log_score").desc())
    submissions = submissions.withColumn("percentile_rank", percent_rank().over(windowSpec))
    submissions = submissions.withColumn(
        "is_viral",
        when(col("percentile_rank") <= 0.10, 1).otherwise(0)
    )

    # Count class distribution
    viral_counts = submissions.groupBy("is_viral").count().collect()
    logger.info("Class distribution:")
    for row in viral_counts:
        logger.info(f"  {'Viral' if row['is_viral'] == 1 else 'Non-viral'}: {row['count']:,}")

    return submissions


# =============================================================================
# Model Training
# =============================================================================

def build_classification_pipeline(num_features=5000):
    """
    Build ML pipeline for viral post classification.
    Two models:
      - Model 1: Content-only (TF-IDF, subreddit, text length, temporal)
      - Model 2: Content + early comments
    """
    # Text processing
    tokenizer = Tokenizer(inputCol="combined_text", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    hashing_tf = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=num_features)
    idf = IDF(inputCol="raw_features", outputCol="tfidf_features")

    # Subreddit encoding
    subreddit_indexer = StringIndexer(inputCol="subreddit", outputCol="subreddit_index", handleInvalid="keep")
    subreddit_encoder = OneHotEncoder(inputCol="subreddit_index", outputCol="subreddit_vec")

    return {
        'tokenizer': tokenizer,
        'remover': remover,
        'hashing_tf': hashing_tf,
        'idf': idf,
        'subreddit_indexer': subreddit_indexer,
        'subreddit_encoder': subreddit_encoder
    }


def train_model1_content_only(df, pipeline_stages):
    """
    Model 1: Content-only viral classification.
    Features: TF-IDF, subreddit, text length, temporal
    """
    logger.info("\n" + "="*80)
    logger.info("TRAINING MODEL 1: CONTENT-ONLY")
    logger.info("="*80)

    # Assemble features
    assembler = VectorAssembler(
        inputCols=["tfidf_features", "subreddit_vec", "title_len", "selftext_len", "post_hour", "post_dow"],
        outputCol="features"
    )

    # Classifier
    classifier = RandomForestClassifier(
        featuresCol="features",
        labelCol="is_viral",
        numTrees=100,
        maxDepth=10,
        seed=42
    )

    # Build pipeline
    pipeline = Pipeline(stages=[
        pipeline_stages['tokenizer'],
        pipeline_stages['remover'],
        pipeline_stages['hashing_tf'],
        pipeline_stages['idf'],
        pipeline_stages['subreddit_indexer'],
        pipeline_stages['subreddit_encoder'],
        assembler,
        classifier
    ])

    # Train/test split
    train, test = df.randomSplit([0.8, 0.2], seed=42)
    logger.info(f"Training set: {train.count():,} posts")
    logger.info(f"Test set: {test.count():,} posts")

    # Train
    logger.info("Training Random Forest Classifier...")
    model = pipeline.fit(train)

    # Predict
    predictions = model.transform(test)

    # Evaluate
    metrics = evaluate_classification(predictions, "Model 1 (Content-Only)")

    # Save model
    model_path = MODELS_DIR / "viral_classifier_model1_content_only"
    model.write().overwrite().save(str(model_path))
    logger.info(f"Model saved to {model_path}")

    return model, predictions, metrics


def train_model2_with_early_comments(df, pipeline_stages):
    """
    Model 2: Content + early comment features.
    """
    logger.info("\n" + "="*80)
    logger.info("TRAINING MODEL 2: CONTENT + EARLY COMMENTS")
    logger.info("="*80)

    # Assemble features (add early comment features)
    assembler = VectorAssembler(
        inputCols=[
            "tfidf_features", "subreddit_vec",
            "title_len", "selftext_len", "post_hour", "post_dow",
            "early_comment_count", "early_comment_avg_score", "early_comment_score_std"
        ],
        outputCol="features"
    )

    classifier = RandomForestClassifier(
        featuresCol="features",
        labelCol="is_viral",
        numTrees=100,
        maxDepth=10,
        seed=42
    )

    pipeline = Pipeline(stages=[
        pipeline_stages['tokenizer'],
        pipeline_stages['remover'],
        pipeline_stages['hashing_tf'],
        pipeline_stages['idf'],
        pipeline_stages['subreddit_indexer'],
        pipeline_stages['subreddit_encoder'],
        assembler,
        classifier
    ])

    train, test = df.randomSplit([0.8, 0.2], seed=42)

    logger.info("Training Random Forest Classifier...")
    model = pipeline.fit(train)
    predictions = model.transform(test)

    metrics = evaluate_classification(predictions, "Model 2 (Content + Early Comments)")

    model_path = MODELS_DIR / "viral_classifier_model2_early_comments"
    model.write().overwrite().save(str(model_path))
    logger.info(f"Model saved to {model_path}")

    return model, predictions, metrics


# =============================================================================
# Evaluation
# =============================================================================

def evaluate_classification(predictions, model_name):
    """
    Evaluate binary classification model.
    """
    logger.info(f"\nEvaluating {model_name}...")

    # Binary classification metrics
    binary_evaluator = BinaryClassificationEvaluator(
        labelCol="is_viral",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    auc_roc = binary_evaluator.evaluate(predictions)

    binary_evaluator.setMetricName("areaUnderPR")
    auc_pr = binary_evaluator.evaluate(predictions)

    # Multiclass metrics
    accuracy_eval = MulticlassClassificationEvaluator(
        labelCol="is_viral",
        predictionCol="prediction",
        metricName="accuracy"
    )
    accuracy = accuracy_eval.evaluate(predictions)

    f1_eval = MulticlassClassificationEvaluator(labelCol="is_viral", metricName="f1")
    f1 = f1_eval.evaluate(predictions)

    precision_eval = MulticlassClassificationEvaluator(labelCol="is_viral", metricName="weightedPrecision")
    precision = precision_eval.evaluate(predictions)

    recall_eval = MulticlassClassificationEvaluator(labelCol="is_viral", metricName="weightedRecall")
    recall = recall_eval.evaluate(predictions)

    metrics = {
        'model': model_name,
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'f1': f1,
        'auc_roc': auc_roc,
        'auc_pr': auc_pr
    }

    logger.info(f"\n{model_name} Metrics:")
    logger.info(f"  Accuracy:  {accuracy:.4f}")
    logger.info(f"  Precision: {precision:.4f}")
    logger.info(f"  Recall:    {recall:.4f}")
    logger.info(f"  F1 Score:  {f1:.4f}")
    logger.info(f"  AUC-ROC:   {auc_roc:.4f}")
    logger.info(f"  AUC-PR:    {auc_pr:.4f}")

    return metrics


def generate_visualizations(predictions, metrics, model_name):
    """
    Generate confusion matrix and ROC curve.
    """
    logger.info(f"\nGenerating visualizations for {model_name}...")

    # Confusion matrix
    cm_data = predictions.groupBy("is_viral", "prediction").count().toPandas()
    cm = cm_data.pivot(index='is_viral', columns='prediction', values='count').fillna(0)

    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt='g', cmap='Blues')
    plt.title(f'Confusion Matrix - {model_name}', fontweight='bold')
    plt.ylabel('Actual')
    plt.xlabel('Predicted')
    plt.tight_layout()

    filename = f"ml_classification_{model_name.lower().replace(' ', '_').replace('(', '').replace(')', '')}_confusion_matrix.png"
    plt.savefig(PLOTS_DIR / filename, dpi=300, bbox_inches='tight')
    logger.info(f"Saved: {PLOTS_DIR / filename}")
    plt.close()

    # Metrics comparison plot (if multiple models)
    return cm


def save_results(metrics_list):
    """
    Save classification results to CSV.
    """
    df = pd.DataFrame(metrics_list)
    csv_path = CSV_DIR / "ml_classification_viral_posts_metrics.csv"
    df.to_csv(csv_path, index=False)
    logger.info(f"\nSaved metrics to {csv_path}")

    print("\n" + "="*80)
    print("CLASSIFICATION RESULTS SUMMARY")
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
        df = load_and_prepare_data(spark)

        # Build pipeline stages
        pipeline_stages = build_classification_pipeline()

        # Train Model 1 (Content-Only)
        model1, pred1, metrics1 = train_model1_content_only(df, pipeline_stages)
        generate_visualizations(pred1, metrics1, "Model 1 Content-Only")

        # Train Model 2 (Content + Early Comments)
        model2, pred2, metrics2 = train_model2_with_early_comments(df, pipeline_stages)
        generate_visualizations(pred2, metrics2, "Model 2 Early Comments")

        # Save results
        save_results([metrics1, metrics2])

        logger.info("\n" + "="*80)
        logger.info("✅ VIRAL POST CLASSIFICATION COMPLETE")
        logger.info("="*80)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
