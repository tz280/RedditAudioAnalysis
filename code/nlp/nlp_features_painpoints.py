#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wireless Earbuds Features & Pain Points Analysis (VADER Sentiment Version)

Author: Team 23 
"""

import os
import re
import argparse
import builtins
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, length, when, sum as _sum
from pyspark import StorageLevel
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


# =============================================================================
# CONFIGURATION
# =============================================================================

# S3 sources 
COMMENTS_PATH = "s3a://tz280-dsan6000-datasets/project/reddit/parquet/comments/"
SUBMISSIONS_PATH = "s3a://tz280-dsan6000-datasets/project/reddit/parquet/submissions/"

MIN_COMMENT_LENGTH = 8

SENTIMENT_SAMPLE_FRACTION = 0.08  
PAINPOINT_SAMPLE_FRACTION = 0.05   

# Plot aesthetics
sns.set_style("whitegrid")
plt.rcParams["figure.figsize"] = (12, 8)

_THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = _THIS_FILE.parent

DATA_DIR = PROJECT_ROOT / "data"
CSV_DIR = DATA_DIR / "csv"
PLOTS_DIR = DATA_DIR / "plots"
TXT_DIR = DATA_DIR / "txt"

for d in (CSV_DIR, PLOTS_DIR, TXT_DIR):
    d.mkdir(parents=True, exist_ok=True)

# =============================================================================
# PATTERN BUILDER
# =============================================================================

def build_pattern(keywords):
    """
    Build a case-insensitive regex pattern from a list of keywords.
    - Plain strings are escaped for literal matches.
    - Dict items like {'re': '...'} are injected as raw regex (NOT escaped).
    Returns a string suitable for Spark rlike().
    """
    parts = []
    for kw in keywords:
        if isinstance(kw, dict) and "re" in kw:
            parts.append(kw["re"])
        else:
            parts.append(re.escape(kw))
    return r"(?i)(" + "|".join(parts) + r")"

# =============================================================================
# FEATURES
# =============================================================================

AUDIO_FEATURES = {
    # === ANC & Noise Control ===
    'anc': [
        'anc', 'active noise cancelling', 'active noise canceling',
        'noise cancelling', 'noise canceling', 'noise cancellation', 'noise cancelation',
        'noise-cancelling', 'noise-canceling', 'nc mode', 'anc mode',
        'adaptive anc', 'hybrid anc', 'feedforward anc', 'feedback anc',
        'transparency vs anc'
    ],

    # === Battery & Charging ===
    'battery': [
        'battery', 'battery life', 'battery drain', 'battery issue',
        'battery problem', 'charge', 'charging', 'charging time',
        'quick charge', 'fast charging', 'wireless charging', 'charging speed',
        'battery performance', 'low battery', 'charge indicator'
    ],

    # === Sound Quality ===
    'sound_quality': [
        'sound quality', 'audio quality', 'sound signature',
        'bass', 'treble', 'mids', 'highs', 'lows',
        'soundstage', 'imaging', 'clarity', 'detail', 'resolution',
        'tuning', 'eq settings', 'equalizer', 'flat sound', 'balanced sound',
        'too much bass', 'harsh treble', 'muddy mids', 'crisp sound',
        'audio performance', 'music quality', 'instrument separation'
    ],

    # === Comfort & Fit ===
    'comfort': [
        'comfort', 'comfortable', 'fit', 'fit in ear',
        'ear tips', 'ear tip', 'ear seal', 'fitment',
        'ergonomic', 'hurt', 'pain', 'pressure', 'ear fatigue',
        'fall out', 'tight fit', 'loose fit', 'comfort level'
    ],

    # === Microphone Quality ===
    'mic_quality': [
        'mic', 'microphone', 'call quality', 'call clarity',
        'voice pickup', 'mic performance', 'mic quality',
        'background noise', 'wind noise', 'talk quality',
        'zoom calls', 'meeting audio', 'voice clarity', 'speech intelligibility'
    ],

    # === Connectivity ===
    'connectivity': [
        'bluetooth', 'connection', 'connectivity', 'pairing',
        'disconnect', 'reconnect', 'latency', 'lag',
        'dropout', 'stutter', 'signal loss', 'interference',
        'connection issue', 'pairing issue', 'connection stability',
        'audio lag', 'video delay', 'bluetooth 5', 'bluetooth 5.3'
    ],

    # === Durability / Build Quality ===
    'durability': [
        'durability', 'build quality', 'sturdy', 'solid build',
        'cheap build', 'flimsy', 'warranty', 'replace', 'repair',
        'broke', 'broken', 'cracked', 'fail', 'failed',
        'dead', 'died', 'stop working', 'stopped working',
        'loose hinge', 'charging pin issue', 'charging contact',
        'scratch', 'damaged', 'wear and tear', 'defect'
    ],

    # === Water / Sweat Resistance ===
    'water_resistance': [
        'waterproof', 'water resistant', 'ipx', 'ipx4', 'ipx5', 'ipx7',
        'sweat', 'rain', 'shower', 'pool', 'moisture', 'splash proof',
        'sweat resistance', 'gym sweat', 'workout sweat'
    ],

    # === Multipoint / Multi-device ===
    'multipoint': [
        'multipoint', 'multi-point', 'multiple devices',
        'device switching', 'dual connection',
        'connect two devices', 'connect laptop and phone',
        'auto switch devices'
    ],

    # === Transparency / Ambient Mode ===
    'transparency': [
        'transparency', 'transparency mode', 'ambient mode',
        'ambient sound', 'hear through', 'hear-through',
        'aware mode', 'talk through', 'passthrough',
        'environment sound', 'outside noise'
    ],

    # === Touch Controls ===
    'touch_controls': [
        'touch controls', 'touch sensor', 'touch area', 'touchpad',
        'touch surface', 'tap control', 'tap gesture',
        'double tap', 'triple tap', 'long press',
        'volume control', 'touch responsiveness',
        'touch sensitivity', 'mis-tap', 'touch delay', 'touch accuracy'
    ],

    # === App / Software ===
    'app': [
        'headphones app', 'earbuds app', 'companion app', 'mobile app',
        'eq app', 'equalizer app', 'control app',
        'app settings', 'app control', 'app features',
        'app bug', 'app issue', 'app crash',
        'firmware update', 'software update', 'app firmware',
        'driver update', 'app preset', 'app equalizer',
        'sony headphones app', 'bose music app', 'soundcore app',
        'jabra sound+ app', 'sennheiser smart control', 'galaxy wearable', 'nothing x app'
    ],

    # === Charging Case ===
    'case': [
        'charging case', 'case battery', 'case hinge', 'case lid',
        'case magnet', 'case design', 'case quality', 'case durability',
        'case feels cheap', 'case light', 'case heavy',
        'case charging port', 'usb-c port', 'type-c port',
        'charging case cover', 'case latch', 'case crack'
    ],

    # === Price & Value ===
    'price': [
        'price', 'cost', 'expensive', 'cheap', 'value', 'worth',
        'worth the money', 'good value', 'value for money',
        'price to performance', 'price/performance',
        'budget earbuds', 'midrange price', 'premium price',
        'too expensive', 'affordable', 'deal', 'discount',
        'on sale', 'best deal', 'worth every penny', 'under $',
        'less than $', 'around $', 'for $', 'price point'
    ]
}

# =============================================================================
# SPARK SESSION
# =============================================================================

def create_spark(local: bool = False):
    """
    Create an optimized SparkSession for large text data processing.
    - Cluster mode (default): uses cluster settings.
    - Local mode: for debugging.
    """
    import os
    from pyspark.sql import SparkSession

    mode_env = os.environ.get("SPARK_MODE", "").lower()
    local = local or (mode_env == "local")

    print("Creating Spark session... (mode: {})".format("LOCAL" if local else "CLUSTER"))

    builder = (
        SparkSession.builder
        .appName("Earbuds Features Analysis - Optimized VADER")
        # ---------- Memory & Execution ----------
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "6g"))
        .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "4g"))
        .config("spark.executor.memoryOverhead", os.environ.get("SPARK_EXECUTOR_OVERHEAD", "1024"))
        .config("spark.executor.cores", os.environ.get("SPARK_EXECUTOR_CORES", "2"))
        .config("spark.memory.fraction", "0.7")            # execution + storage split
        .config("spark.memory.storageFraction", "0.3")     # avoid OOM during cache
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "1g")
        .config("spark.task.maxFailures", "8")             # retry failed tasks more
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")

        # ---------- Shuffle / Parallelism ----------
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "96"))
        .config("spark.default.parallelism", os.environ.get("SPARK_DEFAULT_PARALLELISM", "96"))
        .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB per partition

        # ---------- Serialization ----------
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")

        # ---------- Columnar Cache ----------
        .config("spark.sql.inMemoryColumnarStorage.compressed", "false")
        .config("spark.sql.inMemoryColumnarStorage.batchSize", "1000")

        # ---------- S3A ----------
        .config(
            "spark.jars.packages",
            os.environ.get(
                "SPARK_JARS_PACKAGES",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.691"
            )
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    )

    if local:
        builder = builder.master("local[*]")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("Spark UI:", spark.sparkContext.uiWebUrl)
    print("Spark master:", spark.sparkContext.master)
    print("Driver memory:", spark.conf.get("spark.driver.memory"))
    print("Executor memory:", spark.conf.get("spark.executor.memory"))
    print("Shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
    return spark


# =============================================================================
# DATA LOADING & CLEANING (Tiered Subreddit-Based Filtering) 
# =============================================================================


def load_and_clean_data(spark, comments_path, submissions_path):
    """
    Load and clean Reddit comments and submissions data for earbud analysis.
    This version is optimized for large datasets and memory safety.
    """

    print("\n==================== 1. Loading Data ====================")
    comments_df = spark.read.parquet(comments_path)
    submissions_df = spark.read.parquet(submissions_path)

    total_comments_raw = comments_df.count()
    total_submissions_raw = submissions_df.count()
    print(f"Loaded {total_comments_raw:,} comments and {total_submissions_raw:,} submissions.")

    # --------------------------------------------------------
    # 1. Basic comment cleaning and caching
    # --------------------------------------------------------
    print("\n==================== 2. Cleaning Comments ====================")
    comments_clean = (
        comments_df.filter(
            (col("body").isNotNull()) &
            (col("body") != "[deleted]") &
            (col("body") != "[removed]") &
            (col("author") != "AutoModerator") &
            (length(col("body")) > MIN_COMMENT_LENGTH)
        )
        .withColumn("body_lower", lower(col("body")))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    comments_clean = comments_clean.repartition(48)
    _ = comments_clean.limit(1000).count()
    print("comments_clean cached structure initialized.")

    total_comments = comments_clean.count()
    print(f"After removing deleted/short comments: {total_comments:,}")

    # --------------------------------------------------------
    # 2. Subreddit tiers and keyword filtering
    # --------------------------------------------------------
    print("\n==================== 3. Subreddit and Keyword Filtering ====================")

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

    earbud_pattern = build_pattern(EARBUD_KEYWORDS)

    comments_clean = (
        comments_clean.filter(
            (col("subreddit").isin(EARBUD_ONLY_SUBREDDITS)) |
            ((col("subreddit").isin(MIXED_SUBREDDITS)) & col("body_lower").rlike(earbud_pattern))
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    comments_clean = comments_clean.repartition(48)
    _ = comments_clean.limit(1000).count()
    print("comments_clean (earbud-filtered) cached structure initialized.")

    retained_comments = comments_clean.count()
    print(f"Retained {retained_comments:,} comments after subreddit-based earbud filtering.")

    # --------------------------------------------------------
    # 3. Basic submission cleaning
    # --------------------------------------------------------
    print("\n==================== 4. Cleaning Submissions ====================")
    submissions_clean = (
        submissions_df.filter(
            (col("title").isNotNull()) & (length(col("title")) > 5)
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    _ = submissions_clean.limit(1000).count()
    print("submissions_clean cached structure initialized.")

    retained_submissions = submissions_clean.count()
    print(f"Submissions retained: {retained_submissions:,}")

    print("\n==================== Data Cleaning Completed ====================")
    return comments_clean, submissions_clean

# =============================================================================
# FEATURE EXTRACTION 
# =============================================================================

def extract_feature_mentions(spark, comments_df, feature_dict):
    print("\nExtracting feature mentions (single scan, optimized)...")
    exprs = [
        _sum(when(col("body_lower").rlike(build_pattern(kws)), 1).otherwise(0)).alias(feature)
        for feature, kws in feature_dict.items()
    ]
    row = comments_df.select(*exprs).collect()[0]
    result = row.asDict()
    feature_counts = {k: {"count": int(v)} for k, v in result.items()}
    for k, v in feature_counts.items():
        print(f"{k:<15}: {v['count']:,}")
    return feature_counts

# =============================================================================
# VADER SENTIMENT ANALYSIS
# =============================================================================

def analyze_feature_sentiment_vader(spark, comments_df, feature_dict):
    print("\nPerforming VADER sentiment analysis (driver-side sampled)...")
    analyzer = SentimentIntensityAnalyzer()

    sample_pd = comments_df.sample(fraction=SENTIMENT_SAMPLE_FRACTION, seed=42).select("body").toPandas()
    print(f"Sampled {len(sample_pd):,} comments for sentiment analysis "
          f"({SENTIMENT_SAMPLE_FRACTION*100:.1f}%).")

    sentiment_results = {}
    for feature, keywords in feature_dict.items():
        pattern = re.compile(build_pattern(keywords), re.I)
        subset = sample_pd[sample_pd["body"].str.contains(pattern, na=False)]
        total = subset.shape[0]
        if total == 0:
            continue

        sentiments = [analyzer.polarity_scores(txt)["compound"] for txt in subset["body"]]
        pos = sum(s >= 0.05 for s in sentiments)
        neg = sum(s <= -0.05 for s in sentiments)
        neu = sum(-0.05 < s < 0.05 for s in sentiments)
        avg = sum(sentiments) / len(sentiments)

        sentiment_results[feature] = {
            "total": total,
            "positive": pos,
            "negative": neg,
            "neutral": neu,
            "positive_pct": pos / len(sentiments) * 100,
            "negative_pct": neg / len(sentiments) * 100,
            "neutral_pct": neu / len(sentiments) * 100,
            "avg_sentiment": avg,
            "analyzed_sample": len(sentiments),
        }
        print(f"{feature:<15}: total={total}, avg={avg:+.3f}")
    return sentiment_results

# =============================================================================
# PAIN POINTS 
# =============================================================================

def identify_pain_points_vader(spark, comments_df, feature_dict):
    print("\nIdentifying major pain points (sampled estimation)...")
    analyzer = SentimentIntensityAnalyzer()
    sample_pd = comments_df.sample(fraction=PAINPOINT_SAMPLE_FRACTION, seed=123).select("body").toPandas()
    pain_by_feature = {}

    for feature, keywords in feature_dict.items():
        pattern = re.compile(build_pattern(keywords), re.I)
        subset = sample_pd[sample_pd["body"].str.contains(pattern, na=False)]
        total = subset.shape[0]
        if total == 0:
            continue

        neg_comments = [
            analyzer.polarity_scores(t)["compound"]
            for t in subset["body"]
            if analyzer.polarity_scores(t)["compound"] <= -0.1
        ]
        if neg_comments:
            ratio = len(neg_comments) / len(subset)
            estimated = int((len(subset) / PAINPOINT_SAMPLE_FRACTION) * ratio)
            pain_by_feature[feature] = {"count": estimated}
    return pain_by_feature

# =============================================================================
# BRAND COMPARISON 
# =============================================================================

def compare_brands(spark, comments_df, feature_dict):
    """
    Compare brand mentions in an earbud context.
    Optimized for memory safety and reduced cache pressure.
    """

    print("\n==================== Brand Comparison ====================")
    brands = ["airpods", "sony", "bose", "jabra", "samsung", "anker"]

    # Define earbud context keywords (full list, unchanged)
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
        "charging case", "ear tips", "fit in ear", "ear comfort",
        "audio latency", "microphone quality", "battery life", "eq settings",
        "touch controls", "multipoint", "connectivity issue", "pairing", "firmware update"
    ]

    earbud_pattern = build_pattern(EARBUD_KEYWORDS)

    # Cache the main DataFrame once (serialized)
    comments_lower = comments_df.persist(StorageLevel.MEMORY_AND_DISK)
    _ = comments_lower.limit(1000).count()
    print("comments_df cached structure initialized.")

    brand_feature_data = {}

    for brand in brands:
        brand_pattern = r"(?i)\b" + re.escape(brand) + r"\b"
        combined_pattern = f"({brand_pattern}).*({earbud_pattern})|({earbud_pattern}).*({brand_pattern})"

        # Filter comments mentioning both brand and earbud context
        brand_comments = (
            comments_lower
            .filter(col("body_lower").rlike(combined_pattern))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        # Trigger cache materialization safely
        _ = brand_comments.limit(500).count()
        count = brand_comments.count()

        if count < 10:
            brand_comments.unpersist()
            continue

        print(f"{brand.upper():<10}: {count:,} mentions (earbud-related)")

        # Feature occurrence per brand
        brand_features = {}
        for feature_name, keywords in feature_dict.items():
            pattern = build_pattern(keywords)
            feature_count = brand_comments.filter(col("body_lower").rlike(pattern)).count()
            if feature_count > 0:
                brand_features[feature_name] = feature_count

        brand_feature_data[brand] = brand_features

        # Explicitly unpersist to free executor memory before next brand iteration
        brand_comments.unpersist()
        spark.catalog.clearCache()

    # Unpersist the main cached DataFrame when done
    comments_lower.unpersist()
    spark.catalog.clearCache()

    print("==================== Brand Comparison Completed ====================")
    return brand_feature_data

# =============================================================================
# VISUALIZATION
# =============================================================================

def visualize_results(feature_counts, pain_by_feature):
    print("\nCreating visualizations...")

    # Feature mentions
    features = list(feature_counts.keys())
    counts = [feature_counts[f]["count"] for f in features]
    if counts and sum(counts) > 0:
        plt.barh(features, counts)
        plt.xlabel("Number of Mentions")
        plt.title("Most Discussed Earbud Features")
        plt.gca().invert_yaxis()
        for i, v in enumerate(counts):
            plt.text(v + (max(counts) * 0.01 if counts else 0.5), i, f"{v:,}", va="center")
        plt.tight_layout()
        out1 = PLOTS_DIR / "1_feature_mentions.png"
        plt.savefig(out1.as_posix(), dpi=300)
        plt.close()

    # Pain points
    if pain_by_feature:
        pain_features = list(pain_by_feature.keys())[:10]
        pain_counts = [pain_by_feature[f]["count"] for f in pain_features]
        if pain_counts and sum(pain_counts) > 0:
            plt.barh(pain_features, pain_counts)
            plt.xlabel("Number of Negative Mentions")
            plt.title("Top Pain Points (VADER)")
            plt.gca().invert_yaxis()
            plt.tight_layout()
            out2 = PLOTS_DIR / "3_pain_points_vader.png"
            plt.savefig(out2.as_posix(), dpi=300)
            plt.close()

# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description="Wireless Earbuds Analysis (VADER) — Optimized Full")
    parser.add_argument("--local", action="store_true",
                        help="Force local Spark session (debugging).")

    parser.add_argument("--sample-fraction", type=float, default=1.0,
                        help="(Kept for CLI compatibility; ignored — always run full data.)")
    return parser.parse_args()

def main():
    print("=" * 80)
    print("WIRELESS EARBUDS FEATURES & PAIN POINTS ANALYSIS (VADER) — OPTIMIZED FULL")
    print("=" * 80)

    args = parse_args()
    if args.sample_fraction != 1.0:
        print(f"[Info] Ignoring --sample-fraction={args.sample_fraction}; running FULL data by design.")
    spark = create_spark(local=args.local)
    try:
        comments_df, submissions_df = load_and_clean_data(
            spark, COMMENTS_PATH, SUBMISSIONS_PATH
        )

        feature_counts = extract_feature_mentions(spark, comments_df, AUDIO_FEATURES)
        sentiment_results = analyze_feature_sentiment_vader(spark, comments_df, AUDIO_FEATURES)
        pain_by_feature = identify_pain_points_vader(spark, comments_df, AUDIO_FEATURES)
        brand_feature_data = compare_brands(spark, comments_df, AUDIO_FEATURES)
        visualize_results(feature_counts, pain_by_feature)

        # ------------------------------------------------------------------
        # SAVE CSV RESULTS → data/csv/
        # ------------------------------------------------------------------
        (pd.DataFrame([
            {"feature": f, "mentions": v["count"]}
            for f, v in feature_counts.items()
        ])
         .to_csv((CSV_DIR / "nlp_feature_mentions.csv").as_posix(), index=False))

        (pd.DataFrame([
            {
                "feature": f,
                "total_mentions": v["total"],
                "positive": v["positive"],
                "negative": v["negative"],
                "neutral": v["neutral"],
                "avg_sentiment": v["avg_sentiment"],
            }
            for f, v in sentiment_results.items()
        ])
         .to_csv((CSV_DIR / "nlp_feature_sentiment_vader.csv").as_posix(), index=False))

        (pd.DataFrame([
            {"feature": f, "estimated_negative_mentions": v["count"]}
            for f, v in pain_by_feature.items()
        ])
         .to_csv((CSV_DIR / "nlp_pain_points_vader.csv").as_posix(), index=False))

        # Save brand mentions
        brand_summary = []
        for brand, features in brand_feature_data.items():
            total_mentions = sum(features.values())
            top_feats = sorted(features.items(), key=lambda x: x[1], reverse=True)[:3]
            top_feats_str = ", ".join([f"{f} ({c})" for f, c in top_feats])
            brand_summary.append({
                "brand": brand,
                "total_mentions": total_mentions,
                "top_features": top_feats_str
            })

        pd.DataFrame(brand_summary).to_csv((CSV_DIR / "nlp_brand_mentions.csv").as_posix(), index=False)

        # ------------------------------------------------------------------
        # GENERATE TEXT REPORT → data/txt/
        # ------------------------------------------------------------------
        report_path = TXT_DIR / "ANALYSIS_REPORT_VADER.txt"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write("WIRELESS EARBUDS ANALYSIS REPORT (VADER)\n")
            f.write("=" * 80 + "\n\n")

            # Section 1: Most Discussed Features
            f.write("1. MOST DISCUSSED FEATURES\n")
            sorted_features = sorted(
                feature_counts.items(), key=lambda x: x[1]["count"], reverse=True
            )
            for i, (feat, data) in enumerate(sorted_features[:10], 1):
                f.write(f"{i:2d}. {feat:<18}: {data['count']:>8,} mentions\n")
            f.write("\n")

            # Section 2: Top Negative Features (lowest avg sentiment)
            f.write("2. TOP NEGATIVE FEATURES\n")
            sorted_sentiment = sorted(
                sentiment_results.items(),
                key=lambda x: x[1]["avg_sentiment"]
            )
            for i, (feat, data) in enumerate(sorted_sentiment[:5], 1):
                f.write(f"{i:2d}. {feat:<15}: {data['avg_sentiment']:+.3f}\n")
            f.write("\n")

            # Section 3: Top Pain Points (estimated negative mentions)
            f.write("3. TOP PAIN POINTS\n")
            sorted_pain = sorted(
                pain_by_feature.items(),
                key=lambda x: x[1]["count"],
                reverse=True
            )
            for i, (feat, data) in enumerate(sorted_pain[:10], 1):
                f.write(f"{i:2d}. {feat:<15}: ~{data['count']:,} negative\n")
            f.write("\n")

            # Section 4: Brand Mentions
            f.write("4. BRAND MENTIONS\n")
            if brand_summary:
                sorted_brands = sorted(brand_summary, key=lambda x: x["total_mentions"], reverse=True)
                for i, b in enumerate(sorted_brands, 1):
                    f.write(f"{i:2d}. {b['brand'].upper():<10}: {b['total_mentions']:>6,} mentions\n")
                    f.write(f"    Top features: {b['top_features']}\n")
            else:
                f.write("   (No brand mentions detected)\n")

        print("\nAll results saved under:", DATA_DIR.as_posix())
        print(f"   ├─ CSVs:   {CSV_DIR.as_posix()}")
        print(f"   ├─ Plots:  {PLOTS_DIR.as_posix()}")
        print(f"   └─ Reports:{TXT_DIR.as_posix()}")

    except Exception as e:
        import traceback
        print(f"ERROR: {e}")
        traceback.print_exc()
        raise
    finally:
        spark.stop()
        print("Spark session stopped.")

# =============================================================================

if __name__ == "__main__":
    main()
