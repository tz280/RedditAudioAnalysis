#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wireless Earbuds ML- Regression
Author: Team 23
"""
import os
import re
import logging
from pathlib import Path

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lower, length, count, avg as spark_avg, log1p, concat_ws,
    stddev, when, from_unixtime, hour, dayofweek, unix_timestamp,
    lit, expr
)
from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Tokenizer, StopWordsRemover, HashingTF, IDF,
    StringIndexer, OneHotEncoder, VectorAssembler
)
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# =============================================================================
# Configuration
# =============================================================================
PROJECT_ROOT = Path.cwd()
DATA_DIR = PROJECT_ROOT / "data"
CSV_DIR = DATA_DIR / "csv"
PLOTS_DIR = DATA_DIR / "plots"
TXT_DIR = DATA_DIR / "txt"
MODELS_DIR = PROJECT_ROOT / "models"

for directory in (CSV_DIR, PLOTS_DIR, TXT_DIR, MODELS_DIR):
    directory.mkdir(parents=True, exist_ok=True)

SUBMISSIONS_PATH = "s3a://tz280-dsan6000-datasets/project/reddit/parquet/submissions/"
COMMENTS_PATH = "s3a://tz280-dsan6000-datasets/project/reddit/parquet/comments/"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# =============================================================================
# Spark Session
# =============================================================================

def create_spark_session(app_name: str = "Reddit_Engagement_Two_Model_Pipeline") -> SparkSession:
    """
    Initialize Spark session optimized for ~3 workers × 2 cores × 6.6 GiB.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://172.31.18.33:7077")

        # Cluster resource configuration
        .config("spark.executor.cores", "2")
        .config("spark.executor.instances", "3")
        .config("spark.executor.memory", "4500m")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.cores", "2")

        # Parallelism
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.default.parallelism", "6")

        # Memory tuning
        .config("spark.memory.fraction", "0.75")
        .config("spark.memory.storageFraction", "0.3")
        .config("spark.executor.memoryOverhead", "512m")

        # General optimizations
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "256m")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    logger.info("=" * 80)
    logger.info(f"Spark Version: {spark.version}")
    logger.info(f"Master URL: {spark.sparkContext.master}")
    logger.info("Model 1: Causal (content-only, no post-hoc features)")
    logger.info("Model 2: Content + early comments (first hour)")
    logger.info("=" * 80)

    return spark


def build_keyword_pattern(keywords):
    """
    Build a case-insensitive regex pattern from a list of literal keywords
    and optional dictionaries with custom regex under key 're'.
    """
    parts = []
    for keyword in keywords:
        if isinstance(keyword, dict) and "re" in keyword:
            parts.append(keyword["re"])
        else:
            parts.append(re.escape(keyword))
    return r"(?i)(" + "|".join(parts) + r")"


# =============================================================================
# Data Loading and Filtering
# =============================================================================

def load_and_filter_submissions(spark: SparkSession) -> DataFrame:
    """
    Load and filter Reddit submissions, focusing on earbud-related content.

    Returns a filtered submissions DataFrame with:
      - non-null title and score
      - non-negative score
      - earbud-related subreddits or posts filtered by keywords
    """
    try:
        logger.info("Loading submissions from S3...")
        submissions_df = spark.read.parquet(SUBMISSIONS_PATH)
        initial_count = submissions_df.count()
        logger.info(f"Initial load: {initial_count:,} submissions")
    except Exception as e:
        logger.error(f"Failed to load submissions data from S3: {e}")
        raise

    logger.info("Applying basic quality filters (non-null title & score, score >= 0)...")
    submissions_df = submissions_df.filter(
        (col("title").isNotNull()) &
        (col("score").isNotNull()) &
        (col("score") >= 0)
    )

    logger.info("Applying tiered earbud-focused filtering...")
    EARBUD_ONLY_SUBREDDITS = [
        "headphones", "Earbuds", "AirPods", "airpods",
        "sony", "bose", "Sennheiser", "JBL"
    ]

    MIXED_SUBREDDITS = [
        "Android", "apple", "samsung", "GooglePixel",
        "BuyItForLife", "reviews", "Amazon", "Costco",
        "audiophile", "bluetooth", "audio",
        "gadgets", "technology", "fitness", "running"
    ]

    EARBUD_KEYWORDS = [
        "earbud", "earbuds", "earphone", "earphones",
        "headphone", "headphones", "headset", "headsets",
        "in-ear", "in ear", "over-ear", "over ear", "on-ear", "on ear",
        "tws", "true wireless", "true-wireless",
        "airpods", "airpod pro", "airpod max",
        "galaxy buds", "buds pro", "buds live", "buds 2", "buds fe",
        "sony wf", "wf-1000xm4", "wf-1000xm5",
        "bose quietcomfort", "qc earbuds", "soundlink", "sport earbuds",
        "beats fit pro", "beats studio buds", "beats x", "beats flex",
        "jabra elite", "elite 3", "elite 4", "elite 7", "elite active",
        "anker soundcore", "liberty 4", "liberty air", "life p3", "life dot",
        "sennheiser momentum", "momentum true wireless", "cx plus", "cx true wireless",
        "1more", "one more", "nothing ear", "nothing ear 1", "nothing ear 2",
        "edifier", "soundpeats", "tozo", "taotronics",
        "marshall mode", "marshall minor",
        "cambridge audio", "melomania",
        "b&o beoplay", "bang & olufsen",
        "technics az60", "az80", "panasonic earbuds",
        "bluetooth earbuds", "wireless earbuds", "wireless earphones",
        "noise cancelling headphones", "noise canceling earbuds",
        "noise isolation", "active noise cancelling", "anc mode", "transparency mode",
        "charging case", "ear tips", "ear tip", "fit in ear", "ear comfort",
        "audio latency", "music listening", "soundstage", "microphone quality",
        "battery life", "eq settings", "touch controls",
        "multipoint", "connectivity issue", "pairing", "firmware update"
    ]

    earbud_pattern = build_keyword_pattern(EARBUD_KEYWORDS)

    submissions_filtered = (
        submissions_df
        .filter(
            (col("subreddit").isin(EARBUD_ONLY_SUBREDDITS)) |
            (
                col("subreddit").isin(MIXED_SUBREDDITS) &
                (
                    lower(col("title")).rlike(earbud_pattern) |
                    lower(col("selftext")).rlike(earbud_pattern)
                )
            )
        )
        .repartition(6)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    filtered_count = submissions_filtered.count()
    logger.info(
        f"Filtered dataset: {filtered_count:,} submissions "
        f"({filtered_count / initial_count * 100:.1f}% retained)"
    )

    submissions_df.unpersist()

    return submissions_filtered


def load_comments(spark: SparkSession) -> DataFrame:
    """
    Load comments parquet dataset.

    Expected schema (simplified):
      id, subreddit, author, body, score,
      created_utc, parent_id, link_id,
      controversiality, gilded
    """
    try:
        logger.info("Loading comments from S3...")
        comments_df = spark.read.parquet(COMMENTS_PATH)
        initial_count = comments_df.count()
        logger.info(f"Initial load: {initial_count:,} comments")
    except Exception as e:
        logger.error(f"Failed to load comments data from S3: {e}")
        raise

    # Basic quality filtering (optional)
    comments_df = comments_df.filter(
        (col("body").isNotNull()) &
        (col("score").isNotNull())
    )

    logger.info(f"Comments after basic filter: {comments_df.count():,}")
    return comments_df


# =============================================================================
# Feature Engineering Helpers
# =============================================================================

def add_content_time_features(submissions_df: DataFrame) -> DataFrame:
    """
    Add purely pre-publication features derived from content and timestamp:

      - concatenated text (title + selftext)
      - log-transformed target (log_score)
      - title length
      - selftext length
      - posting hour of day
      - day of week
    """
    logger.info("Adding content and time-based features (causal, pre-publication)...")

    df = submissions_df.withColumn(
        "text", concat_ws(" ", col("title"), col("selftext"))
    )

    df = df.withColumn("log_score", log1p(col("score")))

    df = df.withColumn("title_len", length(col("title")))
    df = df.withColumn("selftext_len", length(col("selftext")))

    df = df.withColumn("post_ts", from_unixtime(col("created_utc")))
    df = df.withColumn("post_hour", hour(col("post_ts")).cast("double"))
    df = df.withColumn("post_dow", dayofweek(col("post_ts")).cast("double"))

    return df


def compute_early_comment_features(
    spark: SparkSession,
    submissions_df: DataFrame,
    comments_df: DataFrame,
    time_window_seconds: int = 3600
) -> DataFrame:
    """
    Compute aggregated early comment features within a fixed time window
    (default: first hour after posting).

    Steps:
      1. Create a submissions view with 'link_id' in the same format as comments.
      2. Join comments to submissions using link_id.
      3. Filter comments to those whose created_utc lies within
         [submission_created_utc, submission_created_utc + time_window_seconds].
      4. Aggregate per submission:
         - early_comment_count
         - early_comment_avg_score
         - early_comment_max_score
         - early_comment_avg_length
         - early_comment_score_std
    """
    logger.info("Computing early comment features (first hour after post)...")

    # Create link_id in submissions matching the typical Reddit format "t3_<id>"
    subs_for_join = (
        submissions_df
        .select("id", "created_utc")
        .withColumn("link_id", concat_ws("", lit("t3_"), col("id")))
        .withColumnRenamed("id", "submission_id")
        .alias("s")
    )

    comments = comments_df.alias("c")

    # Join comments with submissions on link_id
    joined = (
        comments
        .join(
            subs_for_join,
            col("c.link_id") == col("s.link_id"),
            how="inner"
        )
        .filter(
            (col("c.created_utc") >= col("s.created_utc")) &
            (col("c.created_utc") <= col("s.created_utc") + time_window_seconds)
        )
    )

    logger.info("Filtering to comments in the first hour completed.")

    # Length of comment body
    joined = joined.withColumn("comment_len", length(col("c.body")))

    # Aggregations per submission
    early_agg = (
        joined
        .groupBy("s.submission_id")
        .agg(
            count("*").alias("early_comment_count"),
            spark_avg(col("c.score")).alias("early_comment_avg_score"),
            expr("max(c.score)").alias("early_comment_max_score"),
            spark_avg(col("comment_len")).alias("early_comment_avg_length"),
            stddev(col("c.score")).alias("early_comment_score_std")
        )
    )

    # Replace null stddev with 0 (e.g., single comment)
    early_agg = early_agg.fillna({"early_comment_score_std": 0.0})

    # Join back to submissions, keeping all submissions (left join)
    df_with_early = (
        submissions_df
        .join(
            early_agg,
            submissions_df.id == early_agg.submission_id,
            how="left"
        )
        .drop("submission_id")
    )

    # Fill missing early comment features with zero for posts with no early comments
    df_with_early = df_with_early.fillna({
        "early_comment_count": 0.0,
        "early_comment_avg_score": 0.0,
        "early_comment_max_score": 0.0,
        "early_comment_avg_length": 0.0,
        "early_comment_score_std": 0.0
    })

    logger.info("Early comment feature engineering complete.")
    return df_with_early


# =============================================================================
# Model 1: Content-Only (Causal) Model
# =============================================================================

def train_content_only_model(spark: SparkSession, submissions_df: DataFrame):
    """
    Train Model 1 using only features available at post time:

      - TF-IDF of concatenated text (title + selftext)
      - Subreddit (one-hot)
      - Title length
      - Body length
      - Posting hour
      - Day of week

    This model is strictly causal with respect to the posting moment.
    """
    logger.info("=" * 80)
    logger.info("TRAINING MODEL 1: CONTENT-ONLY CAUSAL MODEL")
    logger.info("=" * 80)

    df = add_content_time_features(submissions_df)

    total_samples = df.count()
    logger.info(f"Total samples for Model 1: {total_samples:,}")

    tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
    stop_words_remover = StopWordsRemover(inputCol="tokens", outputCol="filtered")
    hashing_tf = HashingTF(inputCol="filtered", outputCol="tf", numFeatures=5000)
    idf = IDF(inputCol="tf", outputCol="tfidf", minDocFreq=2)

    subreddit_indexer = StringIndexer(
        inputCol="subreddit",
        outputCol="subreddit_idx",
        handleInvalid="keep"
    )
    subreddit_encoder = OneHotEncoder(
        inputCols=["subreddit_idx"],
        outputCols=["subreddit_vec"]
    )

    feature_assembler = VectorAssembler(
        inputCols=[
            "tfidf",
            "subreddit_vec",
            "title_len",
            "selftext_len",
            "post_hour",
            "post_dow"
        ],
        outputCol="features",
        handleInvalid="skip"
    )

    rf_regressor = RandomForestRegressor(
        featuresCol="features",
        labelCol="log_score",
        numTrees=100,
        maxDepth=12,
        minInstancesPerNode=5,
        subsamplingRate=0.8,
        seed=42
    )

    pipeline = Pipeline(stages=[
        tokenizer,
        stop_words_remover,
        hashing_tf,
        idf,
        subreddit_indexer,
        subreddit_encoder,
        feature_assembler,
        rf_regressor
    ])

    logger.info("Splitting data into train/test (80/20) for Model 1...")
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    train_df.persist(StorageLevel.MEMORY_AND_DISK)
    test_df.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info(f"Training set size (Model 1): {train_df.count():,}")
    logger.info(f"Test set size (Model 1):     {test_df.count():,}")

    logger.info("Fitting Random Forest for Model 1 (content-only)...")
    model = pipeline.fit(train_df)
    logger.info("Model 1 training completed.")

    logger.info("Scoring test set for Model 1...")
    predictions = model.transform(test_df).persist(StorageLevel.MEMORY_AND_DISK)

    evaluator_r2 = RegressionEvaluator(
        labelCol="log_score", predictionCol="prediction", metricName="r2"
    )
    evaluator_rmse = RegressionEvaluator(
        labelCol="log_score", predictionCol="prediction", metricName="rmse"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol="log_score", predictionCol="prediction", metricName="mae"
    )

    r2_score = evaluator_r2.evaluate(predictions)
    rmse = evaluator_rmse.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)

    logger.info("Model 1 Performance:")
    logger.info(f"  R²   = {r2_score:.4f}")
    logger.info(f"  RMSE = {rmse:.4f}")
    logger.info(f"  MAE  = {mae:.4f}")

    # Subreddit-level diagnostics
    logger.info("Computing subreddit-level metrics for Model 1...")
    subreddit_metrics = (
        predictions.groupBy("subreddit")
        .agg(
            count("*").alias("n_samples"),
            spark_avg((col("prediction") - col("log_score")) ** 2).alias("MSE"),
            spark_avg(col("log_score")).alias("avg_true_log_score"),
            spark_avg(col("prediction")).alias("avg_pred_log_score")
        )
        .withColumn("RMSE", (col("MSE")) ** 0.5)
    )

    subreddit_metrics_pd = (
        subreddit_metrics.toPandas().sort_values("RMSE")
    )
    subreddit_metrics_pd.to_csv(
        CSV_DIR / "subreddit_performance_model1_content_only.csv",
        index=False
    )

    # Barplot of RMSE by subreddit for Model 1
    logger.info("Creating RMSE-by-subreddit plot for Model 1...")
    plt.figure(figsize=(12, 6))
    top_n = min(15, len(subreddit_metrics_pd))
    sns.barplot(
        data=subreddit_metrics_pd.head(top_n),
        x="RMSE",
        y="subreddit"
    )
    plt.title(
        f"Model 1 (Content-Only) - RMSE by Subreddit (Top {top_n})",
        fontsize=14,
        fontweight="bold"
    )
    plt.xlabel("RMSE (log-score space)")
    plt.ylabel("Subreddit")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "rmse_by_subreddit_model1_content_only.png",
                dpi=300, bbox_inches="tight")
    plt.close()

    # Save a sample of predictions
    predictions.select(
        "id", "subreddit", "title", "score", "log_score", "prediction"
    ).limit(5000).toPandas().to_csv(
        CSV_DIR / "predictions_model1_content_only.csv",
        index=False
    )

    # Save model
    model_path = str(MODELS_DIR / "random_forest_model1_content_only")
    logger.info(f"Saving Model 1 to {model_path}...")
    model.write().overwrite().save(model_path)

    # Cleanup
    train_df.unpersist()
    test_df.unpersist()
    predictions.unpersist()

    return r2_score, rmse, mae, subreddit_metrics_pd


# =============================================================================
# Model 2: Content + Early Comments Model
# =============================================================================

def train_early_comments_model(
    spark: SparkSession,
    submissions_df: DataFrame,
    comments_df: DataFrame
):
    """
    Train Model 2 using:
      - All content/time features from Model 1 (pre-publication)
      - Early comment aggregates from the first hour after posting:
          * early_comment_count
          * early_comment_avg_score
          * early_comment_max_score
          * early_comment_avg_length
          * early_comment_score_std

    This model is still realistic for "early detection" use cases but is no
    longer purely causal at posting time (it uses early engagement signals).
    """
    logger.info("=" * 80)
    logger.info("TRAINING MODEL 2: CONTENT + EARLY COMMENTS MODEL")
    logger.info("=" * 80)

    # First, compute early comment features and then add the same content features
    df_with_early = compute_early_comment_features(
        spark,
        submissions_df,
        comments_df,
        time_window_seconds=3600
    )
    df = add_content_time_features(df_with_early)

    total_samples = df.count()
    logger.info(f"Total samples for Model 2: {total_samples:,}")

    tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
    stop_words_remover = StopWordsRemover(inputCol="tokens", outputCol="filtered")
    hashing_tf = HashingTF(inputCol="filtered", outputCol="tf", numFeatures=5000)
    idf = IDF(inputCol="tf", outputCol="tfidf", minDocFreq=2)

    subreddit_indexer = StringIndexer(
        inputCol="subreddit",
        outputCol="subreddit_idx",
        handleInvalid="keep"
    )
    subreddit_encoder = OneHotEncoder(
        inputCols=["subreddit_idx"],
        outputCols=["subreddit_vec"]
    )

    feature_assembler = VectorAssembler(
        inputCols=[
            # Content + subreddit
            "tfidf",
            "subreddit_vec",
            "title_len",
            "selftext_len",
            "post_hour",
            "post_dow",
            # Early comment features
            "early_comment_count",
            "early_comment_avg_score",
            "early_comment_max_score",
            "early_comment_avg_length",
            "early_comment_score_std"
        ],
        outputCol="features",
        handleInvalid="skip"
    )

    rf_regressor = RandomForestRegressor(
        featuresCol="features",
        labelCol="log_score",
        numTrees=100,
        maxDepth=12,
        minInstancesPerNode=5,
        subsamplingRate=0.8,
        seed=42
    )

    pipeline = Pipeline(stages=[
        tokenizer,
        stop_words_remover,
        hashing_tf,
        idf,
        subreddit_indexer,
        subreddit_encoder,
        feature_assembler,
        rf_regressor
    ])

    logger.info("Splitting data into train/test (80/20) for Model 2...")
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    train_df.persist(StorageLevel.MEMORY_AND_DISK)
    test_df.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info(f"Training set size (Model 2): {train_df.count():,}")
    logger.info(f"Test set size (Model 2):     {test_df.count():,}")

    logger.info("Fitting Random Forest for Model 2 (content + early comments)...")
    model = pipeline.fit(train_df)
    logger.info("Model 2 training completed.")

    logger.info("Scoring test set for Model 2...")
    predictions = model.transform(test_df).persist(StorageLevel.MEMORY_AND_DISK)

    evaluator_r2 = RegressionEvaluator(
        labelCol="log_score", predictionCol="prediction", metricName="r2"
    )
    evaluator_rmse = RegressionEvaluator(
        labelCol="log_score", predictionCol="prediction", metricName="rmse"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol="log_score", predictionCol="prediction", metricName="mae"
    )

    r2_score = evaluator_r2.evaluate(predictions)
    rmse = evaluator_rmse.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)

    logger.info("Model 2 Performance:")
    logger.info(f"  R²   = {r2_score:.4f}")
    logger.info(f"  RMSE = {rmse:.4f}")
    logger.info(f"  MAE  = {mae:.4f}")

    # Subreddit-level diagnostics
    logger.info("Computing subreddit-level metrics for Model 2...")
    subreddit_metrics = (
        predictions.groupBy("subreddit")
        .agg(
            count("*").alias("n_samples"),
            spark_avg((col("prediction") - col("log_score")) ** 2).alias("MSE"),
            spark_avg(col("log_score")).alias("avg_true_log_score"),
            spark_avg(col("prediction")).alias("avg_pred_log_score")
        )
        .withColumn("RMSE", (col("MSE")) ** 0.5)
    )

    subreddit_metrics_pd = (
        subreddit_metrics.toPandas().sort_values("RMSE")
    )
    subreddit_metrics_pd.to_csv(
        CSV_DIR / "subreddit_performance_model2_early_comments.csv",
        index=False
    )

    # Barplot of RMSE by subreddit for Model 2
    logger.info("Creating RMSE-by-subreddit plot for Model 2...")
    plt.figure(figsize=(12, 6))
    top_n = min(15, len(subreddit_metrics_pd))
    sns.barplot(
        data=subreddit_metrics_pd.head(top_n),
        x="RMSE",
        y="subreddit"
    )
    plt.title(
        f"Model 2 (Content + Early Comments) - RMSE by Subreddit (Top {top_n})",
        fontsize=14,
        fontweight="bold"
    )
    plt.xlabel("RMSE (log-score space)")
    plt.ylabel("Subreddit")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "rmse_by_subreddit_model2_early_comments.png",
                dpi=300, bbox_inches="tight")
    plt.close()

    # Save a sample of predictions
    predictions.select(
        "id", "subreddit", "title", "score", "log_score", "prediction"
    ).limit(5000).toPandas().to_csv(
        CSV_DIR / "predictions_model2_early_comments.csv",
        index=False
    )

    # Save model
    model_path = str(MODELS_DIR / "random_forest_model2_early_comments")
    logger.info(f"Saving Model 2 to {model_path}...")
    model.write().overwrite().save(model_path)

    # Cleanup
    train_df.unpersist()
    test_df.unpersist()
    predictions.unpersist()

    return r2_score, rmse, mae, subreddit_metrics_pd


# =============================================================================
# Comparative Report
# =============================================================================

def generate_comparative_report(
    model1_metrics,
    model2_metrics
):
    """
    Generate a concise comparative report between:
      - Model 1 (content-only, causal)
      - Model 2 (content + early comments within 1 hour)
    """
    m1_r2, m1_rmse, m1_mae, _ = model1_metrics
    m2_r2, m2_rmse, m2_mae, _ = model2_metrics

    path = TXT_DIR / "MODEL_COMPARISON_REPORT.txt"
    logger.info(f"Writing comparative report to {path}...")

    with open(path, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("REDDIT EARPHONE/AUDIO ENGAGEMENT PREDICTION\n")
        f.write("MODEL COMPARISON: CONTENT-ONLY vs. CONTENT + EARLY COMMENTS\n")
        f.write("=" * 80 + "\n\n")

        f.write("TASK\n")
        f.write("-" * 80 + "\n")
        f.write("Regression: predict log(1 + score) for Reddit submissions.\n\n")

        f.write("MODEL DEFINITIONS\n")
        f.write("-" * 80 + "\n")
        f.write("Model 1 (Content-Only, Causal):\n")
        f.write("  - Inputs available at posting time only:\n")
        f.write("    • Title + selftext (TF-IDF)\n")
        f.write("    • Subreddit (one-hot encoded)\n")
        f.write("    • Title length, selftext length\n")
        f.write("    • Posting hour of day, day of week\n\n")
        f.write("Model 2 (Content + Early Comments):\n")
        f.write("  - All features from Model 1, plus aggregated early engagement\n")
        f.write("    using comments posted in the first hour after submission:\n")
        f.write("    • Early comment count\n")
        f.write("    • Average early comment score\n")
        f.write("    • Maximum early comment score\n")
        f.write("    • Average early comment length\n")
        f.write("    • Standard deviation of early comment score\n\n")

        f.write("GLOBAL PERFORMANCE\n")
        f.write("-" * 80 + "\n")
        f.write("Model 1 (Content-Only):\n")
        f.write(f"  R²   = {m1_r2:.4f}\n")
        f.write(f"  RMSE = {m1_rmse:.4f}\n")
        f.write(f"  MAE  = {m1_mae:.4f}\n\n")
        f.write("Model 2 (Content + Early Comments):\n")
        f.write(f"  R²   = {m2_r2:.4f}\n")
        f.write(f"  RMSE = {m2_rmse:.4f}\n")
        f.write(f"  MAE  = {m2_mae:.4f}\n\n")

        f.write("PERFORMANCE DIFFERENCE (MODEL 2 – MODEL 1)\n")
        f.write("-" * 80 + "\n")
        f.write(f"  ΔR²   = {m2_r2 - m1_r2:+.4f}\n")
        f.write(f"  ΔRMSE = {m2_rmse - m1_rmse:+.4f}\n")
        f.write(f"  ΔMAE  = {m2_mae - m1_mae:+.4f}\n\n")

        f.write("INTERPRETATION\n")
        f.write("-" * 80 + "\n")
        f.write(
            "Model 1 captures the portion of engagement that can be explained purely\n"
            "from content and posting time. Its R² reflects the intrinsic difficulty\n"
            "of predicting social media engagement based only on what is known at\n"
            "the moment of posting.\n\n"
        )
        f.write(
            "Model 2 shows how much additional variance becomes predictable once we\n"
            "observe early engagement within the first hour. A substantial increase\n"
            "in R² (for example, from ≈0.14 to ≈0.35–0.45) indicates that early\n"
            "comments provide a strong signal for final outcomes.\n\n"
        )
        f.write(
            "This gap between Model 1 and Model 2 quantifies the contribution of\n"
            "early engagement dynamics (\"snowball effects\") beyond pure content\n"
            "quality.\n\n"
        )

        f.write("PRACTICAL USE CASES\n")
        f.write("-" * 80 + "\n")
        f.write("Model 1 (Content-Only):\n")
        f.write("  • Use before posting to compare alternative titles/selftexts.\n")
        f.write("  • Run counterfactuals for content strategy (A/B testing).\n\n")
        f.write("Model 2 (Content + Early Comments):\n")
        f.write("  • Use after 1 hour to prioritize potentially viral posts.\n")
        f.write("  • Support moderation or recommendation decisions.\n\n")

        f.write("=" * 80 + "\n")
        f.write("END OF REPORT\n")
        f.write("=" * 80 + "\n")

    logger.info("Comparative report written.")


# =============================================================================
# Main
# =============================================================================

def main():
    """Main execution pipeline for both models."""
    spark = None
    try:
        spark = create_spark_session()

        logger.info("=" * 80)
        logger.info("STARTING TWO-MODEL ENGAGEMENT PREDICTION PIPELINE")
        logger.info("=" * 80)

        # Load submissions (filtered to earbud-related posts)
        submissions_df = load_and_filter_submissions(spark)

        # ---------- Model 1: Content-Only ----------
        model1_metrics = train_content_only_model(spark, submissions_df)

        # ---------- Model 2: Content + Early Comments ----------
        comments_df = load_comments(spark)
        model2_metrics = train_early_comments_model(spark, submissions_df, comments_df)

        # ---------- Comparative Report ----------
        generate_comparative_report(model1_metrics, model2_metrics)

        logger.info("=" * 80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(
            f"Model 1 R² = {model1_metrics[0]:.4f} "
            f"(content-only, causal at post time)"
        )
        logger.info(
            f"Model 2 R² = {model2_metrics[0]:.4f} "
            f"(content + early comments, early-engagement model)"
        )
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        raise
    finally:
        if spark is not None:
            logger.info("Stopping Spark session...")
            spark.stop()


if __name__ == "__main__":
    main()
