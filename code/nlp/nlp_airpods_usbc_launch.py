#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import argparse
from pathlib import Path
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, length, when, sum as _sum, count, avg,
    from_unixtime, to_date, month, year, weekofyear,
    lit, concat_ws, regexp_extract, size, split, collect_list,
    pandas_udf, PandasUDFType
)
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window
from pyspark import StorageLevel

# VADER for sentiment analysis
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# For topic modeling
from pyspark.ml.feature import (
    Tokenizer, StopWordsRemover, CountVectorizer, IDF
)
from pyspark.ml.clustering import LDA
from pyspark.ml import Pipeline

# =============================================================================
# CONFIGURATION
# =============================================================================

# S3 paths - will be set via command-line argument
COMMENTS_PATH = None
SUBMISSIONS_PATH = None

# Product launch date: September 22, 2023
LAUNCH_DATE = "2023-09-22"
PRE_LAUNCH_START = "2023-06-01"  # 3 months before
POST_LAUNCH_END = "2024-07-31"   # End of dataset

# Keywords to track
TRACKED_KEYWORDS = [
    "usb-c", "usb c", "lightning", "upgrade", "worth it",
    "lossless", "vision pro", "charging port", "usb type c"
]

# AirPods-related patterns
AIRPODS_PATTERNS = [
    r'\bairpods?\s+pro\b',
    r'\bapp\s+2\b',
    r'\bairpods?\s+pro\s+2\b',
    r'\bairpods?\s+pro\s+second\s+gen\b',
    r'\bairpods?\s+pro\s+gen\s+2\b',
    r'\bairpods?\s+pro\s+\(2nd\s+gen\)',
]

MIN_TEXT_LENGTH = 20

# Output directories
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
CSV_DIR = DATA_DIR / "csv"
PLOTS_DIR = DATA_DIR / "plots"

for d in (CSV_DIR, PLOTS_DIR):
    d.mkdir(parents=True, exist_ok=True)

# Plot styling
sns.set_style("whitegrid")
sns.set_palette("husl")
plt.rcParams["figure.figsize"] = (14, 8)
plt.rcParams["font.size"] = 10

# =============================================================================
# SPARK SESSION
# =============================================================================

def create_spark_session():
    """Create Spark session with S3 configuration"""
    spark = SparkSession.builder \
        .appName("AirPods_USB-C_Launch_Analysis") \
        .master("local[4]") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
        .config("spark.driver.memory", "6g") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()

    # Override Hadoop configuration
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.connection.maximum", "100")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.fast.upload", "true")

    spark.sparkContext.setLogLevel("WARN")
    return spark

# =============================================================================
# DATA LOADING AND FILTERING
# =============================================================================

def load_and_filter_airpods_data(spark):
    """
    Load Reddit data and filter for AirPods Pro discussions
    """
    print("=" * 80)
    print("LOADING DATA")
    print("=" * 80)

    # Load comments
    print(f"Loading comments from: {COMMENTS_PATH}")
    comments_df = spark.read.parquet(COMMENTS_PATH)

    # Load submissions
    print(f"Loading submissions from: {SUBMISSIONS_PATH}")
    submissions_df = spark.read.parquet(SUBMISSIONS_PATH)

    # Process comments
    comments_df = comments_df.select(
        col("subreddit"),
        col("body").alias("text"),
        col("score"),
        col("created_utc"),
        lit("comment").alias("type")
    )

    # Process submissions (combine title and selftext)
    submissions_df = submissions_df.withColumn(
        "text",
        when(
            col("selftext").isNotNull() & (col("selftext") != ""),
            concat_ws(" ", col("title"), col("selftext"))
        ).otherwise(col("title"))
    )

    submissions_df = submissions_df.select(
        col("subreddit"),
        col("text"),
        col("score"),
        col("created_utc"),
        lit("submission").alias("type")
    )

    # Union comments and submissions
    df = comments_df.union(submissions_df)

    print(f"Total rows before filtering: {df.count():,}")

    # Filter for audio-related subreddits
    audio_subreddits = [
        'headphones', 'audiophile', 'airpods', 'apple', 'iphone',
        'audio', 'bose', 'sonyheadphones', 'sony', 'earbuds'
    ]
    df = df.filter(lower(col("subreddit")).isin(audio_subreddits))

    # Filter for AirPods Pro mentions using regex
    airpods_pattern = "|".join(AIRPODS_PATTERNS)
    df = df.filter(col("text").rlike(f"(?i)({airpods_pattern})"))

    # Filter by text length
    df = df.filter(length(col("text")) >= MIN_TEXT_LENGTH)

    # Add date columns
    df = df.withColumn("date", to_date(from_unixtime(col("created_utc"))))
    df = df.withColumn("year", year(col("date")))
    df = df.withColumn("month", month(col("date")))
    df = df.withColumn("year_month", concat_ws("-", col("year"), col("month")))

    # Filter date range
    df = df.filter(
        (col("date") >= lit(PRE_LAUNCH_START)) &
        (col("date") <= lit(POST_LAUNCH_END))
    )

    # Add period label (pre vs post launch)
    df = df.withColumn(
        "period",
        when(col("date") < lit(LAUNCH_DATE), "Pre-Launch")
        .otherwise("Post-Launch")
    )

    print(f"Rows mentioning AirPods Pro: {df.count():,}")

    # Cache for reuse
    df = df.persist(StorageLevel.MEMORY_AND_DISK)

    return df

# =============================================================================
# SENTIMENT ANALYSIS WITH VADER
# =============================================================================

def add_vader_sentiment(df):
    """
    Add VADER sentiment scores to dataframe
    """
    print("\n" + "=" * 80)
    print("SENTIMENT ANALYSIS WITH VADER")
    print("=" * 80)

    # Define UDF for VADER sentiment
    def vader_sentiment(text: pd.Series) -> pd.Series:
        def score_text(t):
            if pd.isna(t) or not t or len(str(t).strip()) == 0:
                return 0.0
            try:
                analyzer = SentimentIntensityAnalyzer()
                scores = analyzer.polarity_scores(str(t))
                return float(scores['compound'])
            except:
                return 0.0
        return text.apply(score_text)

    vader_udf = pandas_udf(vader_sentiment, DoubleType())

    # Add sentiment score
    df = df.withColumn("sentiment_score", vader_udf(col("text")))

    # Add sentiment category
    df = df.withColumn(
        "sentiment_category",
        when(col("sentiment_score") >= 0.05, "Positive")
        .when(col("sentiment_score") <= -0.05, "Negative")
        .otherwise("Neutral")
    )

    print("Sentiment analysis complete!")

    return df

# =============================================================================
# KEYWORD TRACKING
# =============================================================================

def add_keyword_tracking(df):
    """
    Track presence and frequency of key terms
    """
    print("\n" + "=" * 80)
    print("KEYWORD TRACKING")
    print("=" * 80)

    # Add binary flags for each keyword
    for keyword in TRACKED_KEYWORDS:
        # Create safe column name
        col_name = f"mentions_{keyword.replace(' ', '_').replace('-', '_')}"
        pattern = keyword.replace('-', '[-\\s]?').replace(' ', '[-\\s]?')
        df = df.withColumn(
            col_name,
            when(lower(col("text")).rlike(f"(?i)\\b{pattern}\\b"), 1).otherwise(0)
        )

    print(f"Added tracking for {len(TRACKED_KEYWORDS)} keywords")

    return df

# =============================================================================
# ANALYSIS 1: DISCUSSION VOLUME OVER TIME
# =============================================================================

def analyze_discussion_volume(df):
    """
    Analyze how discussion volume changed over time
    """
    print("\n" + "=" * 80)
    print("ANALYSIS 1: DISCUSSION VOLUME")
    print("=" * 80)

    # Overall volume by period
    period_volume = df.groupBy("period").agg(
        count("*").alias("total_posts"),
        avg("score").alias("avg_score")
    ).toPandas()

    print("\nVolume by Period:")
    print(period_volume)
    period_volume.to_csv(CSV_DIR / "airpods_volume_by_period.csv", index=False)

    # Weekly volume over time
    weekly_volume = df.groupBy("date").agg(
        count("*").alias("post_count"),
        avg("score").alias("avg_score")
    ).orderBy("date").toPandas()

    weekly_volume.to_csv(CSV_DIR / "airpods_weekly_volume.csv", index=False)

    # Monthly volume by type
    monthly_volume = df.groupBy("year_month", "period", "type").agg(
        count("*").alias("count")
    ).orderBy("year_month").toPandas()

    monthly_volume.to_csv(CSV_DIR / "airpods_monthly_volume_by_type.csv", index=False)

    # Visualization
    plot_volume_over_time(weekly_volume, period_volume)

    return period_volume, weekly_volume

def plot_volume_over_time(weekly_df, period_df):
    """Create volume visualization"""
    fig, axes = plt.subplots(2, 1, figsize=(14, 10))

    # Plot 1: Daily volume with launch date marker
    ax1 = axes[0]
    weekly_df['date'] = pd.to_datetime(weekly_df['date'])
    ax1.plot(weekly_df['date'], weekly_df['post_count'],
             linewidth=2, color='steelblue', alpha=0.7)
    ax1.axvline(pd.Timestamp(LAUNCH_DATE), color='red',
                linestyle='--', linewidth=2, label='USB-C Launch (Sept 22, 2023)')
    ax1.fill_between(weekly_df['date'], weekly_df['post_count'],
                     alpha=0.3, color='steelblue')
    ax1.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Daily Post Count', fontsize=12, fontweight='bold')
    ax1.set_title('AirPods Pro Discussion Volume Over Time',
                  fontsize=14, fontweight='bold')
    ax1.legend(fontsize=10)
    ax1.grid(True, alpha=0.3)

    # Plot 2: Period comparison
    ax2 = axes[1]
    periods = period_df['period'].tolist()
    counts = period_df['total_posts'].tolist()
    colors = ['skyblue', 'coral']
    bars = ax2.bar(periods, counts, color=colors, alpha=0.8, edgecolor='black')
    ax2.set_ylabel('Total Posts', fontsize=12, fontweight='bold')
    ax2.set_title('Discussion Volume: Pre-Launch vs Post-Launch',
                  fontsize=14, fontweight='bold')

    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}',
                ha='center', va='bottom', fontweight='bold', fontsize=11)

    ax2.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "airpods_discussion_volume.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved: {PLOTS_DIR / 'airpods_discussion_volume.png'}")

# =============================================================================
# ANALYSIS 2: SENTIMENT COMPARISON
# =============================================================================

def analyze_sentiment_changes(df):
    """
    Compare sentiment before and after launch
    """
    print("\n" + "=" * 80)
    print("ANALYSIS 2: SENTIMENT CHANGES")
    print("=" * 80)

    # Overall sentiment by period
    sentiment_by_period = df.groupBy("period").agg(
        avg("sentiment_score").alias("avg_sentiment"),
        count("*").alias("total_posts")
    ).toPandas()

    print("\nAverage Sentiment by Period:")
    print(sentiment_by_period)
    sentiment_by_period.to_csv(CSV_DIR / "airpods_sentiment_by_period.csv", index=False)

    # Sentiment distribution
    sentiment_dist = df.groupBy("period", "sentiment_category").agg(
        count("*").alias("count")
    ).toPandas()

    sentiment_dist.to_csv(CSV_DIR / "airpods_sentiment_distribution.csv", index=False)

    # Monthly sentiment trend
    monthly_sentiment = df.groupBy("year_month", "period").agg(
        avg("sentiment_score").alias("avg_sentiment"),
        count("*").alias("count")
    ).orderBy("year_month").toPandas()

    monthly_sentiment.to_csv(CSV_DIR / "airpods_monthly_sentiment.csv", index=False)

    # Visualization
    plot_sentiment_analysis(sentiment_by_period, sentiment_dist, monthly_sentiment)

    return sentiment_by_period, sentiment_dist

def plot_sentiment_analysis(period_sent, dist, monthly):
    """Create sentiment visualizations"""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))

    # Plot 1: Average sentiment by period
    ax1 = axes[0, 0]
    colors = ['skyblue' if x >= 0 else 'lightcoral'
              for x in period_sent['avg_sentiment']]
    bars = ax1.bar(period_sent['period'], period_sent['avg_sentiment'],
                   color=colors, alpha=0.8, edgecolor='black')
    ax1.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax1.set_ylabel('Average Sentiment Score', fontsize=11, fontweight='bold')
    ax1.set_title('Average Sentiment: Pre vs Post Launch',
                  fontsize=12, fontweight='bold')
    ax1.grid(True, alpha=0.3, axis='y')

    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.3f}',
                ha='center', va='bottom' if height > 0 else 'top',
                fontweight='bold')

    # Plot 2: Sentiment distribution
    ax2 = axes[0, 1]
    pivot_dist = dist.pivot(index='period', columns='sentiment_category', values='count')
    pivot_dist = pivot_dist.div(pivot_dist.sum(axis=1), axis=0) * 100
    pivot_dist[['Negative', 'Neutral', 'Positive']].plot(
        kind='bar', ax=ax2, color=['#e74c3c', '#95a5a6', '#2ecc71'],
        alpha=0.8, edgecolor='black'
    )
    ax2.set_ylabel('Percentage (%)', fontsize=11, fontweight='bold')
    ax2.set_title('Sentiment Distribution by Period', fontsize=12, fontweight='bold')
    ax2.legend(title='Sentiment', fontsize=9)
    ax2.set_xticklabels(ax2.get_xticklabels(), rotation=0)
    ax2.grid(True, alpha=0.3, axis='y')

    # Plot 3: Monthly sentiment trend
    ax3 = axes[1, 0]
    for period in monthly['period'].unique():
        period_data = monthly[monthly['period'] == period]
        ax3.plot(range(len(period_data)), period_data['avg_sentiment'],
                marker='o', label=period, linewidth=2, markersize=6)

    ax3.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
    ax3.set_xlabel('Time (Monthly)', fontsize=11, fontweight='bold')
    ax3.set_ylabel('Average Sentiment', fontsize=11, fontweight='bold')
    ax3.set_title('Sentiment Trend Over Time', fontsize=12, fontweight='bold')
    ax3.legend(fontsize=9)
    ax3.grid(True, alpha=0.3)

    # Plot 4: Post count vs sentiment
    ax4 = axes[1, 1]
    ax4_twin = ax4.twinx()

    x = range(len(period_sent))
    width = 0.35
    ax4.bar([i - width/2 for i in x], period_sent['total_posts'],
            width, label='Post Count', color='steelblue', alpha=0.7)
    ax4_twin.bar([i + width/2 for i in x], period_sent['avg_sentiment'],
                 width, label='Avg Sentiment', color='coral', alpha=0.7)

    ax4.set_xlabel('Period', fontsize=11, fontweight='bold')
    ax4.set_ylabel('Post Count', fontsize=11, fontweight='bold', color='steelblue')
    ax4_twin.set_ylabel('Avg Sentiment', fontsize=11, fontweight='bold', color='coral')
    ax4.set_title('Volume vs Sentiment by Period', fontsize=12, fontweight='bold')
    ax4.set_xticks(x)
    ax4.set_xticklabels(period_sent['period'])
    ax4.tick_params(axis='y', labelcolor='steelblue')
    ax4_twin.tick_params(axis='y', labelcolor='coral')
    ax4.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "airpods_sentiment_analysis.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved: {PLOTS_DIR / 'airpods_sentiment_analysis.png'}")

# =============================================================================
# ANALYSIS 3: KEYWORD EVOLUTION
# =============================================================================

def analyze_keyword_evolution(df):
    """
    Track how keyword mentions evolved pre vs post launch
    """
    print("\n" + "=" * 80)
    print("ANALYSIS 3: KEYWORD EVOLUTION")
    print("=" * 80)

    # Aggregate keyword mentions by period
    keyword_cols = [f"mentions_{kw.replace(' ', '_').replace('-', '_')}"
                    for kw in TRACKED_KEYWORDS]

    # Calculate mention rates by period
    keyword_data = []
    for keyword, col_name in zip(TRACKED_KEYWORDS, keyword_cols):
        period_mentions = df.groupBy("period").agg(
            _sum(col(col_name)).alias("mentions"),
            count("*").alias("total")
        ).toPandas()

        for _, row in period_mentions.iterrows():
            keyword_data.append({
                'keyword': keyword,
                'period': row['period'],
                'mentions': row['mentions'],
                'total_posts': row['total'],
                'mention_rate': (row['mentions'] / row['total']) * 100
            })

    keyword_df = pd.DataFrame(keyword_data)
    keyword_df.to_csv(CSV_DIR / "airpods_keyword_evolution.csv", index=False)

    print("\nKeyword Mention Rates:")
    print(keyword_df.pivot(index='keyword', columns='period', values='mention_rate'))

    # Calculate percentage change
    keyword_change = keyword_df.pivot(index='keyword', columns='period',
                                      values='mention_rate')
    keyword_change['change_pct'] = (
        (keyword_change['Post-Launch'] - keyword_change['Pre-Launch']) /
        keyword_change['Pre-Launch'] * 100
    )
    keyword_change = keyword_change.sort_values('change_pct', ascending=False)
    keyword_change.to_csv(CSV_DIR / "airpods_keyword_change.csv")

    print("\nKeyword Change (%):")
    print(keyword_change)

    # Visualization
    plot_keyword_evolution(keyword_df, keyword_change)

    return keyword_df, keyword_change

def plot_keyword_evolution(keyword_df, keyword_change):
    """Create keyword evolution visualizations"""
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Plot 1: Mention rates comparison
    ax1 = axes[0]
    pivot = keyword_df.pivot(index='keyword', columns='period', values='mention_rate')
    pivot.plot(kind='barh', ax=ax1, color=['skyblue', 'coral'],
               alpha=0.8, edgecolor='black')
    ax1.set_xlabel('Mention Rate (%)', fontsize=11, fontweight='bold')
    ax1.set_ylabel('Keyword', fontsize=11, fontweight='bold')
    ax1.set_title('Keyword Mention Rates: Pre vs Post Launch',
                  fontsize=12, fontweight='bold')
    ax1.legend(title='Period', fontsize=9)
    ax1.grid(True, alpha=0.3, axis='x')

    # Plot 2: Percentage change
    ax2 = axes[1]
    colors = ['green' if x > 0 else 'red' for x in keyword_change['change_pct']]
    bars = ax2.barh(range(len(keyword_change)), keyword_change['change_pct'],
                    color=colors, alpha=0.7, edgecolor='black')
    ax2.set_yticks(range(len(keyword_change)))
    ax2.set_yticklabels(keyword_change.index)
    ax2.set_xlabel('Change in Mention Rate (%)', fontsize=11, fontweight='bold')
    ax2.set_title('Keyword Mention Rate Change (Post vs Pre)',
                  fontsize=12, fontweight='bold')
    ax2.axvline(x=0, color='black', linestyle='-', linewidth=1)
    ax2.grid(True, alpha=0.3, axis='x')

    # Add value labels
    for i, (bar, val) in enumerate(zip(bars, keyword_change['change_pct'])):
        ax2.text(val, i, f' {val:+.1f}%',
                va='center', fontweight='bold', fontsize=9)

    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "airpods_keyword_evolution.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved: {PLOTS_DIR / 'airpods_keyword_evolution.png'}")

# =============================================================================
# ANALYSIS 4: TOPIC MODELING
# =============================================================================

def perform_topic_modeling(df, spark, num_topics=5):
    """
    Perform LDA topic modeling for pre and post launch periods
    """
    print("\n" + "=" * 80)
    print("ANALYSIS 4: TOPIC MODELING")
    print("=" * 80)

    topics_summary = []

    for period in ["Pre-Launch", "Post-Launch"]:
        print(f"\nAnalyzing topics for: {period}")

        period_df = df.filter(col("period") == period)

        # Text preprocessing pipeline
        tokenizer = Tokenizer(inputCol="text", outputCol="words")

        # Custom stop words (add domain-specific ones)
        custom_stops = [
            'airpods', 'pro', 'apple', 'https', 'www', 'com', 'http',
            'deleted', 'removed', 'edit', 'update', 'im', 'ive', 'dont'
        ]
        remover = StopWordsRemover(
            inputCol="words",
            outputCol="filtered",
            stopWords=StopWordsRemover.loadDefaultStopWords("english") + custom_stops
        )

        cv = CountVectorizer(
            inputCol="filtered",
            outputCol="features",
            vocabSize=1000,
            minDF=5
        )

        # LDA model
        lda = LDA(
            k=num_topics,
            maxIter=20,
            featuresCol="features",
            seed=42
        )

        pipeline = Pipeline(stages=[tokenizer, remover, cv, lda])

        # Fit model
        model = pipeline.fit(period_df)

        # Extract vocabulary and topics
        cv_model = model.stages[2]
        lda_model = model.stages[3]

        vocab = cv_model.vocabulary
        topics = lda_model.describeTopics(maxTermsPerTopic=10)

        # Convert topics to readable format
        topics_pd = topics.toPandas()

        for idx, row in topics_pd.iterrows():
            topic_num = idx
            term_indices = row['termIndices']
            term_weights = row['termWeights']

            terms = [vocab[i] for i in term_indices]

            topic_info = {
                'period': period,
                'topic_num': topic_num,
                'top_terms': ', '.join(terms[:10]),
                'weights': ', '.join([f"{w:.3f}" for w in term_weights[:10]])
            }
            topics_summary.append(topic_info)

            print(f"  Topic {topic_num}: {', '.join(terms[:7])}")

    # Save topics
    topics_df = pd.DataFrame(topics_summary)
    topics_df.to_csv(CSV_DIR / "airpods_topics.csv", index=False)

    print(f"\n✓ Saved: {CSV_DIR / 'airpods_topics.csv'}")

    return topics_df

# =============================================================================
# ARGUMENT PARSING
# =============================================================================

def parse_args():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description='Analyze AirPods Pro USB-C launch impact on Reddit discussions'
    )
    parser.add_argument(
        '--bucket',
        type=str,
        required=True,
        help='S3 bucket name (e.g., tz280-dsan6000-datasets or project-zp134)'
    )
    parser.add_argument(
        '--comments-path',
        type=str,
        default=None,
        help='Custom path to comments (overrides bucket default)'
    )
    parser.add_argument(
        '--submissions-path',
        type=str,
        default=None,
        help='Custom path to submissions (overrides bucket default)'
    )
    return parser.parse_args()

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function"""
    global COMMENTS_PATH, SUBMISSIONS_PATH

    # Parse arguments
    args = parse_args()

    # Set S3 paths based on bucket structure
    if args.comments_path:
        COMMENTS_PATH = args.comments_path
    elif args.bucket == "project-zp134":
        COMMENTS_PATH = f"s3a://{args.bucket}/comments/"
    else:
        COMMENTS_PATH = f"s3a://{args.bucket}/project/reddit/parquet/comments/"

    if args.submissions_path:
        SUBMISSIONS_PATH = args.submissions_path
    elif args.bucket == "project-zp134":
        SUBMISSIONS_PATH = f"s3a://{args.bucket}/submissions/"
    else:
        SUBMISSIONS_PATH = f"s3a://{args.bucket}/project/reddit/parquet/submissions/"

    print("\n" + "=" * 80)
    print("AIRPODS PRO USB-C LAUNCH IMPACT ANALYSIS")
    print("=" * 80)
    print(f"S3 Bucket: {args.bucket}")
    print(f"Comments Path: {COMMENTS_PATH}")
    print(f"Submissions Path: {SUBMISSIONS_PATH}")
    print(f"Launch Date: {LAUNCH_DATE}")
    print(f"Pre-Launch Period: {PRE_LAUNCH_START} to {LAUNCH_DATE}")
    print(f"Post-Launch Period: {LAUNCH_DATE} to {POST_LAUNCH_END}")
    print("=" * 80)

    # Create Spark session
    spark = create_spark_session()

    try:
        # Load and filter data
        df = load_and_filter_airpods_data(spark)

        # Add sentiment analysis
        df = add_vader_sentiment(df)

        # Add keyword tracking
        df = add_keyword_tracking(df)

        # Cache the enriched dataframe
        df = df.persist(StorageLevel.MEMORY_AND_DISK)

        # Analyze discussion volume
        period_volume, weekly_volume = analyze_discussion_volume(df)

        # Analyze sentiment changes
        sentiment_by_period, sentiment_dist = analyze_sentiment_changes(df)

        # Analyze keyword evolution
        keyword_df, keyword_change = analyze_keyword_evolution(df)

        # Topic modeling
        topics_df = perform_topic_modeling(df, spark, num_topics=5)

        # Generate summary report
        print("\n" + "=" * 80)
        print("ANALYSIS COMPLETE!")
        print("=" * 80)
        print("\nGenerated Files:")
        print(f"  CSV Files: {len(list(CSV_DIR.glob('airpods_*.csv')))} files in {CSV_DIR}")
        print(f"  Plots: {len(list(PLOTS_DIR.glob('airpods_*.png')))} files in {PLOTS_DIR}")

        # Print key findings
        print("\n" + "=" * 80)
        print("KEY FINDINGS SUMMARY")
        print("=" * 80)

        print("\n1. DISCUSSION VOLUME:")
        pre_count = period_volume[period_volume['period'] == 'Pre-Launch']['total_posts'].values[0]
        post_count = period_volume[period_volume['period'] == 'Post-Launch']['total_posts'].values[0]
        volume_change = ((post_count - pre_count) / pre_count) * 100
        print(f"   Pre-Launch:  {pre_count:,} posts")
        print(f"   Post-Launch: {post_count:,} posts")
        print(f"   Change: {volume_change:+.1f}%")

        print("\n2. SENTIMENT:")
        pre_sent = sentiment_by_period[sentiment_by_period['period'] == 'Pre-Launch']['avg_sentiment'].values[0]
        post_sent = sentiment_by_period[sentiment_by_period['period'] == 'Post-Launch']['avg_sentiment'].values[0]
        sent_change = post_sent - pre_sent
        print(f"   Pre-Launch Sentiment:  {pre_sent:.3f}")
        print(f"   Post-Launch Sentiment: {post_sent:.3f}")
        print(f"   Change: {sent_change:+.3f}")

        print("\n3. TOP KEYWORD CHANGES:")
        for i, (keyword, change) in enumerate(keyword_change['change_pct'].head(3).items(), 1):
            print(f"   {i}. '{keyword}': {change:+.1f}%")

    finally:
        spark.stop()
        print("\n✓ Spark session closed")

if __name__ == "__main__":
    main()
