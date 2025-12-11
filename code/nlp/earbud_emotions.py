#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wireless Earbuds Features & Pain Points Analysis (Emotion-Level Version)

Author: Team 23
Notes:
- Keeps the original cleaning, subreddit/keyword filtering, feature counting, brand comparison,
  plotting, CSV/Report outputs.
- Replaces VADER-only sentiment with emotion-level analysis tied to feature keywords (battery, ANC, etc.).
- Uses NRC-style emotions (anger, anticipation, disgust, fear, joy, sadness, surprise, trust) + pos/neg.
- Fallback micro-lexicon included; prefer full NRC lexicon CSV/TSV if available:
    data/resources/NRC-Emotion-Lexicon-Wordlevel-v0.92.csv (or .txt)
"""

import os
import re
import argparse
from pathlib import Path
from collections import Counter, defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    length,
    when,
    sum as _sum,
    from_unixtime,
    to_timestamp,
    month,
    year,
)
from pyspark import StorageLevel
from pyspark import SparkConf

# =============================================================================
# CONFIGURATION
# =============================================================================

# S3 sources (ALL comments/submissions; Spark will read partitions yyyy=...)
COMMENTS_PATH = "s3a://shh73-dsan6000-datasets/reddit/parquet/comments/"
SUBMISSIONS_PATH = "s3a://shh73-dsan6000-datasets/reddit/parquet/submissions/"


MIN_COMMENT_LENGTH = 8

# Sampling for CPU-side NLP passes
# Set to 1.0 to use ALL filtered comments by default (no sampling).
EMOTION_SAMPLE_FRACTION = 1.0  # for per-feature emotion analysis
PAINPOINT_SAMPLE_FRACTION = 1.0  # for negative-lean detection estimation

# Plot aesthetics
sns.set_style("whitegrid")
plt.rcParams["figure.figsize"] = (12, 8)

_THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = _THIS_FILE.parent
DATA_DIR = PROJECT_ROOT / "data"
CSV_DIR = DATA_DIR / "csv"
PLOTS_DIR = DATA_DIR / "plots"
TXT_DIR = DATA_DIR / "txt"
RESOURCES_DIR = DATA_DIR / "resources"

for d in (CSV_DIR, PLOTS_DIR, TXT_DIR, RESOURCES_DIR):
    d.mkdir(parents=True, exist_ok=True)

# Preferred NRC lexicon path (drop the file here if you have it)
NRC_LEXICON_PATH = RESOURCES_DIR / "NRC-Emotion-Lexicon-Wordlevel-v0.92.csv"


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
# FEATURES (used for keyword→feature mapping)
# =============================================================================

AUDIO_FEATURES = {
    "anc": [
        "anc",
        "active noise cancelling",
        "active noise canceling",
        "noise cancelling",
        "noise canceling",
        "noise cancellation",
        "noise cancelation",
        "noise-cancelling",
        "noise-canceling",
        "nc mode",
        "anc mode",
        "adaptive anc",
        "hybrid anc",
        "feedforward anc",
        "feedback anc",
        "transparency vs anc",
    ],
    "battery": [
        "battery",
        "battery life",
        "battery drain",
        "battery issue",
        "battery problem",
        "charge",
        "charging",
        "charging time",
        "quick charge",
        "fast charging",
        "wireless charging",
        "charging speed",
        "battery performance",
        "low battery",
        "charge indicator",
    ],
    "sound_quality": [
        "sound quality",
        "audio quality",
        "sound signature",
        "bass",
        "treble",
        "mids",
        "highs",
        "lows",
        "soundstage",
        "imaging",
        "clarity",
        "detail",
        "resolution",
        "tuning",
        "eq settings",
        "equalizer",
        "flat sound",
        "balanced sound",
        "too much bass",
        "harsh treble",
        "muddy mids",
        "crisp sound",
        "audio performance",
        "music quality",
        "instrument separation",
    ],
    "comfort": [
        "comfort",
        "comfortable",
        "fit",
        "fit in ear",
        "ear tips",
        "ear tip",
        "ear seal",
        "fitment",
        "ergonomic",
        "hurt",
        "pain",
        "pressure",
        "ear fatigue",
        "fall out",
        "tight fit",
        "loose fit",
        "comfort level",
    ],
    "mic_quality": [
        "mic",
        "microphone",
        "call quality",
        "call clarity",
        "voice pickup",
        "mic performance",
        "mic quality",
        "background noise",
        "wind noise",
        "talk quality",
        "zoom calls",
        "meeting audio",
        "voice clarity",
        "speech intelligibility",
    ],
    "connectivity": [
        "bluetooth",
        "connection",
        "connectivity",
        "pairing",
        "disconnect",
        "reconnect",
        "latency",
        "lag",
        "dropout",
        "stutter",
        "signal loss",
        "interference",
        "connection issue",
        "pairing issue",
        "connection stability",
        "audio lag",
        "video delay",
        "bluetooth 5",
        "bluetooth 5.3",
    ],
    "durability": [
        "durability",
        "build quality",
        "sturdy",
        "solid build",
        "cheap build",
        "flimsy",
        "warranty",
        "replace",
        "repair",
        "broke",
        "broken",
        "cracked",
        "fail",
        "failed",
        "dead",
        "died",
        "stop working",
        "stopped working",
        "loose hinge",
        "charging pin issue",
        "charging contact",
        "scratch",
        "damaged",
        "wear and tear",
        "defect",
    ],
    "water_resistance": [
        "waterproof",
        "water resistant",
        "ipx",
        "ipx4",
        "ipx5",
        "ipx7",
        "sweat",
        "rain",
        "shower",
        "pool",
        "moisture",
        "splash proof",
        "sweat resistance",
        "gym sweat",
        "workout sweat",
    ],
    "multipoint": [
        "multipoint",
        "multi-point",
        "multiple devices",
        "device switching",
        "dual connection",
        "connect two devices",
        "connect laptop and phone",
        "auto switch devices",
    ],
    "transparency": [
        "transparency",
        "transparency mode",
        "ambient mode",
        "ambient sound",
        "hear through",
        "hear-through",
        "aware mode",
        "talk through",
        "passthrough",
        "environment sound",
        "outside noise",
    ],
    "touch_controls": [
        "touch controls",
        "touch sensor",
        "touch area",
        "touchpad",
        "touch surface",
        "tap control",
        "tap gesture",
        "double tap",
        "triple tap",
        "long press",
        "volume control",
        "touch responsiveness",
        "touch sensitivity",
        "mis-tap",
        "touch delay",
        "touch accuracy",
    ],
    "app": [
        "headphones app",
        "earbuds app",
        "companion app",
        "mobile app",
        "eq app",
        "equalizer app",
        "control app",
        "app settings",
        "app control",
        "app features",
        "app bug",
        "app issue",
        "app crash",
        "firmware update",
        "software update",
        "app firmware",
        "driver update",
        "app preset",
        "app equalizer",
        "sony headphones app",
        "bose music app",
        "soundcore app",
        "jabra sound+ app",
        "sennheiser smart control",
        "galaxy wearable",
        "nothing x app",
    ],
    "case": [
        "charging case",
        "case battery",
        "case hinge",
        "case lid",
        "case magnet",
        "case design",
        "case quality",
        "case durability",
        "case feels cheap",
        "case light",
        "case heavy",
        "case charging port",
        "usb-c port",
        "type-c port",
        "charging case cover",
        "case latch",
        "case crack",
    ],
    "price": [
        "price",
        "cost",
        "expensive",
        "cheap",
        "value",
        "worth",
        "worth the money",
        "good value",
        "value for money",
        "price to performance",
        "price/performance",
        "budget earbuds",
        "midrange price",
        "premium price",
        "too expensive",
        "affordable",
        "deal",
        "discount",
        "on sale",
        "best deal",
        "worth every penny",
        "under $",
        "less than $",
        "around $",
        "for $",
        "price point",
    ],
}

# >>> NEW: Friendly names for features (for brand impressions)
FEATURE_FRIENDLY_NAMES = {
    "anc": "noise cancelling",
    "battery": "battery life",
    "sound_quality": "sound quality",
    "comfort": "comfort / fit",
    "mic_quality": "mic / call quality",
    "connectivity": "connectivity",
    "durability": "durability",
    "water_resistance": "water resistance",
    "multipoint": "multipoint",
    "transparency": "transparency / ambient",
    "touch_controls": "touch controls",
    "app": "companion app",
    "case": "charging case",
    "price": "price / value",
}

# =============================================================================
# EMOTION LEXICON / SCORING
# =============================================================================

EMOTION_LABELS = [
    "anger",
    "anticipation",
    "disgust",
    "fear",
    "joy",
    "sadness",
    "surprise",
    "trust",
    "positive",
    "negative",
]

# Negative-leaning emotions for pain-point detection
NEG_EMOTIONS = ["anger", "disgust", "fear", "sadness", "negative"]

# Tiny fallback lexicon (so pipeline runs even if full NRC is missing).
FALLBACK_LEXICON = {
    "amazing": {"joy": 1, "positive": 1},
    "great": {"joy": 1, "positive": 1, "trust": 1},
    "love": {"joy": 1, "positive": 1, "anticipation": 1},
    "trust": {"trust": 1, "positive": 1},
    "reliable": {"trust": 1, "positive": 1},
    "excited": {"anticipation": 1, "joy": 1, "positive": 1},
    "angry": {"anger": 1, "negative": 1},
    "annoying": {"anger": 1, "negative": 1},
    "disappointed": {"sadness": 1, "negative": 1},
    "terrible": {"disgust": 1, "negative": 1},
    "awful": {"disgust": 1, "negative": 1},
    "scared": {"fear": 1, "negative": 1},
    "bug": {"anger": 1, "negative": 1},
    "drain": {"anger": 1, "sadness": 1, "negative": 1},
    "lag": {"anger": 1, "negative": 1},
    "disconnect": {"anger": 1, "negative": 1},
    "dead": {"sadness": 1, "negative": 1},
    "broken": {"sadness": 1, "negative": 1},
    "crack": {"fear": 1, "negative": 1},
    "refund": {"anger": 1, "negative": 1},
}


def load_nrc_lexicon(path: Path):
    """
    Loads NRC Emotion Lexicon if present.
    Accepts TSV/CSV with columns like: word, emotion, association (1/0).

    Returns dict[word] -> dict[emotion] -> weight
    """
    if not path.exists():
        print(f"[WARN] NRC lexicon not found at {path}. Using fallback micro-lexicon.")
        return FALLBACK_LEXICON

    # Try TSV first; fallback to CSV.
    try:
        df = pd.read_csv(path, sep="\t", header=None, names=["word", "emotion", "association"])
    except Exception:
        df = pd.read_csv(path, header=None, names=["word", "emotion", "association"])

    df = df[df["association"] == 1]
    lex = defaultdict(dict)
    for _, row in df.iterrows():
        lex[str(row["word"]).lower()][str(row["emotion"]).lower()] = 1

    print(f"[OK] Loaded NRC lexicon: {len(lex):,} words with emotion tags.")
    return lex


def tokenize(text):
    return re.findall(r"[a-zA-Z']+", text.lower())


def score_emotions(text, lexicon):
    """
    Sum emotion counts from lexicon. Returns dict of emotion -> count.
    """
    counts = Counter()
    for tok in tokenize(text):
        if tok in lexicon:
            counts.update(lexicon[tok])
    return counts


# =============================================================================
# SPARK SESSION
# =============================================================================

def create_spark(local: bool = False):
    print("Creating Spark session...")
    conf = SparkConf().setAppName("Earbuds_Features_Emotions")

    # ---------------- AWS S3 SETTINGS ----------------
    conf.set(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    )
    conf.set("spark.hadoop.fs.s3a.connection.timeout", "200000")
    conf.set("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
    conf.set("spark.hadoop.fs.s3a.attempts.maximum", "10")
    conf.set("spark.hadoop.fs.s3a.retry.limit", "5")
    conf.set("spark.hadoop.fs.s3a.retry.interval", "500")
    conf.set("spark.hadoop.fs.s3a.threads.max", "20")
    conf.set("spark.hadoop.fs.s3a.threads.core", "15")
    conf.set("spark.hadoop.fs.s3a.max.total.tasks", "20")
    conf.set("spark.hadoop.fs.s3a.socket.send.buffer", "8192")
    conf.set("spark.hadoop.fs.s3a.socket.recv.buffer", "8192")
    conf.set("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
    conf.set("spark.hadoop.fs.s3a.multipart.purge", "true")
    conf.set("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
    conf.set("spark.hadoop.fs.s3a.block.size", "134217728")

    # ---------------- MEMORY & EXECUTION SETTINGS ----------------
    conf.set("spark.driver.memory", "6g")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.executor.memoryOverhead", "1024")
    conf.set("spark.executor.cores", "2")
    conf.set("spark.sql.shuffle.partitions", "96")
    conf.set("spark.default.parallelism", "96")

    if local:
        conf.setMaster("local[*]")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("✅ Spark session initialized.")
    print(" UI:", spark.sparkContext.uiWebUrl)
    print(" Master:", spark.sparkContext.master)
    return spark


# =============================================================================
# CHECK S3 PATH
# =============================================================================

def check_s3_path_exists(spark, path):
    try:
        print(f"[INFO] Checking path: {path}")
        _ = spark.read.parquet(path).limit(1).count()
        print(f"[OK] Verified S3 path exists: {path}")
        return True
    except Exception as e:
        print(f"[ERROR] Cannot access S3 path {path}: {e}")
        return False


# =============================================================================
# DATA LOADING & CLEANING (+ ingest sampling)
# =============================================================================

def load_and_clean_data(spark, comments_path, submissions_path, sample_fraction=1.0):
    print("\n==================== 1. Loading Data ====================")

    if not check_s3_path_exists(spark, comments_path):
        raise RuntimeError("Invalid COMMENTS_PATH — please verify your bucket and path")
    if not check_s3_path_exists(spark, submissions_path):
        raise RuntimeError("Invalid SUBMISSIONS_PATH — please verify your bucket and path")

    comments_df = spark.read.parquet(comments_path)
    submissions_df = spark.read.parquet(submissions_path)

    # Ingest-level sampling: keep only a fraction of rows right after read
    if 0 < sample_fraction < 1.0:
        print(f"[INFO] Ingest sampling {sample_fraction*100:.1f}% of rows...")
        comments_df = comments_df.sample(withReplacement=False, fraction=sample_fraction, seed=42)
        submissions_df = submissions_df.sample(
            withReplacement=False, fraction=sample_fraction, seed=42
        )
    else:
        print("[INFO] Using FULL dataset for ingest (no sampling).")

    total_comments_raw = comments_df.count()
    total_submissions_raw = submissions_df.count()
    print(
        f"Loaded {total_comments_raw:,} comments and {total_submissions_raw:,} submissions "
        f"after ingest sampling."
    )

    print("\n==================== 2. Cleaning Comments ====================")

    comments_clean = (
        comments_df.filter(
            (col("body").isNotNull())
            & (col("body") != "[deleted]")
            & (col("body") != "[removed]")
            & (col("author") != "AutoModerator")
            & (length(col("body")) > MIN_COMMENT_LENGTH)
        )
        .withColumn("body_lower", lower(col("body")))
        .withColumn("created_ts", to_timestamp(from_unixtime(col("created_utc").cast("bigint"))))
        .withColumn("yyyymm", (year(col("created_ts")) * 100 + month(col("created_ts"))))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    comments_clean = comments_clean.repartition(48)
    _ = comments_clean.limit(1000).count()
    print("comments_clean cached structure initialized.")

    total_comments = comments_clean.count()
    print(f"After removing deleted/short comments: {total_comments:,}")

    print("\n==================== 3. Subreddit and Keyword Filtering ====================")

    EARBUD_ONLY_SUBREDDITS = [
        "headphones",
        "Earbuds",
        "AirPods",
        "airpods",
        "sony",
        "bose",
        "Sennheiser",
        "JBL",
    ]
    MIXED_SUBREDDITS = [
        "Android",
        "apple",
        "samsung",
        "GooglePixel",
        "BuyItForLife",
        "reviews",
        "Amazon",
        "Costco",
        "audiophile",
        "bluetooth",
        "audio",
        "gadgets",
        "technology",
        "fitness",
        "running",
    ]
    EARBUD_KEYWORDS = [
        "earbud",
        "earbuds",
        "earphone",
        "earphones",
        "headphone",
        "headphones",
        "headset",
        "headsets",
        "in-ear",
        "in ear",
        "over-ear",
        "over ear",
        "on-ear",
        "on ear",
        "tws",
        "true wireless",
        "true-wireless",
        "airpods",
        "airpod pro",
        "airpod max",
        "galaxy buds",
        "buds pro",
        "buds live",
        "buds 2",
        "buds fe",
        "sony wf",
        "wf-1000xm4",
        "wf-1000xm5",
        "bose quietcomfort",
        "qc earbuds",
        "soundlink",
        "sport earbuds",
        "beats fit pro",
        "beats studio buds",
        "beats x",
        "beats flex",
        "jabra elite",
        "elite 3",
        "elite 4",
        "elite 7",
        "elite active",
        "anker soundcore",
        "liberty 4",
        "liberty air",
        "life p3",
        "life dot",
        "sennheiser momentum",
        "momentum true wireless",
        "cx plus",
        "cx true wireless",
        "1more",
        "one more",
        "nothing ear",
        "nothing ear 1",
        "nothing ear 2",
        "edifier",
        "soundpeats",
        "tozo",
        "taotronics",
        "marshall mode",
        "marshall minor",
        "cambridge audio",
        "melomania",
        "b&o beoplay",
        "bang & olufsen",
        "technics az60",
        "az80",
        "panasonic earbuds",
        "bluetooth earbuds",
        "wireless earbuds",
        "wireless earphones",
        "noise cancelling headphones",
        "noise canceling earbuds",
        "noise isolation",
        "active noise cancelling",
        "anc mode",
        "transparency mode",
        "charging case",
        "ear tips",
        "fit in ear",
        "ear comfort",
        "audio latency",
        "music listening",
        "soundstage",
        "microphone quality",
        "battery life",
        "eq settings",
        "touch controls",
        "multipoint",
        "connectivity issue",
        "pairing",
        "firmware update",
    ]

    earbud_pattern = build_pattern(EARBUD_KEYWORDS)

    comments_clean = (
        comments_clean.filter(
            (col("subreddit").isin(EARBUD_ONLY_SUBREDDITS))
            | (
                (col("subreddit").isin(MIXED_SUBREDDITS))
                & col("body_lower").rlike(earbud_pattern)
            )
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    comments_clean = comments_clean.repartition(48)
    _ = comments_clean.limit(1000).count()
    print("comments_clean (earbud-filtered) cached structure initialized.")

    retained_comments = comments_clean.count()
    print(f"Retained {retained_comments:,} comments after subreddit-based earbud filtering.")

    print("\n==================== 4. Cleaning Submissions ====================")

    submissions_clean = (
        submissions_df.filter(
            (col("title").isNotNull())
            & (length(col("title")) > 5)
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
# FEATURE MENTIONS
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
# EMOTION-LEVEL ANALYSIS (per feature + optional monthly timeline)
# =============================================================================

def analyze_feature_emotions(spark, comments_df, feature_dict, lexicon):
    """
    For each feature:
    - sample comments mentioning the feature (or all, if fraction=1.0)
    - compute emotion distribution using NRC-style lexicon
    - return dict: feature -> {emotion -> count, *_pct}

    Also returns per-month emotion tallies for trend plots.
    """
    print("\nPerforming emotion-level analysis (driver-side)...")

    frac = EMOTION_SAMPLE_FRACTION
    if 0 < frac < 1.0:
        print(f"[INFO] Emotion analysis sampling {frac*100:.1f}% of rows...")
        source_df = comments_df.sample(fraction=frac, seed=42)
    else:
        print("[INFO] Emotion analysis using FULL filtered dataset (no sampling).")
        source_df = comments_df

    sample_pd = source_df.select("body", "body_lower", "yyyymm").toPandas()
    print(f"Emotion analysis rows: {len(sample_pd):,}")

    results = {}
    monthly = {}  # feature -> yyyymm -> emotion -> count

    for feature, keywords in feature_dict.items():
        pattern = re.compile(build_pattern(keywords), re.I)
        subset = sample_pd[sample_pd["body_lower"].str.contains(pattern, na=False)]
        total = subset.shape[0]
        if total == 0:
            continue

        emo_counter = Counter()
        month_map = defaultdict(Counter)

        for _, row in subset.iterrows():
            counts = score_emotions(row["body"], lexicon)
            if counts:
                emo_counter.update(counts)
                month_map[int(row["yyyymm"])].update(counts)

        # Normalize / format output
        total_tokens = sum(emo_counter.values())
        feature_result = {
            "total_comments": int(total),
            "total_emotion_hits": int(total_tokens),
        }
        for lab in EMOTION_LABELS:
            c = emo_counter.get(lab, 0)
            feature_result[lab] = int(c)
            feature_result[f"{lab}_pct"] = (c / total_tokens * 100.0) if total_tokens > 0 else 0.0

        results[feature] = feature_result

        # Monthly
        monthly[feature] = {}
        for yyyymm, cnts in month_map.items():
            total_m = sum(cnts.values())
            month_row = {
                "yyyymm": int(yyyymm),
                "total_emotion_hits": int(total_m),
            }
            for lab in EMOTION_LABELS:
                c = cnts.get(lab, 0)
                month_row[lab] = int(c)
                month_row[f"{lab}_pct"] = (c / total_m * 100.0) if total_m > 0 else 0.0
            monthly[feature][yyyymm] = month_row

        print(f"{feature:<15}: comments={total:,}, emotion_hits={total_tokens:,}")

    return results, monthly


# =============================================================================
# PAIN POINTS VIA EMOTIONS
# =============================================================================

def identify_pain_points_emotions(spark, comments_df, feature_dict, lexicon):
    """
    Estimate negative-leaning mentions per feature using emotion lexicon.

    Returns:
        dict[feature] -> {"count": estimated_negative_hits, "total_comments": n}
    """
    print("\nEstimating pain points via negative emotions...")

    frac = PAINPOINT_SAMPLE_FRACTION
    if 0 < frac < 1.0:
        print(f"[INFO] Pain-point analysis sampling {frac*100:.1f}% of rows...")
        source_df = comments_df.sample(fraction=frac, seed=43)
    else:
        print("[INFO] Pain-point analysis using FULL filtered dataset (no sampling).")
        source_df = comments_df

    sample_pd = source_df.select("body", "body_lower").toPandas()
    print(f"Pain-point analysis rows: {len(sample_pd):,}")

    results = {}

    for feature, keywords in feature_dict.items():
        pattern = re.compile(build_pattern(keywords), re.I)
        subset = sample_pd[sample_pd["body_lower"].str.contains(pattern, na=False)]
        total_comments = subset.shape[0]
        if total_comments == 0:
            continue

        neg_sum = 0
        for _, row in subset.iterrows():
            emo_counts = score_emotions(row["body"], lexicon)
            if emo_counts:
                neg_sum += sum(emo_counts.get(e, 0) for e in NEG_EMOTIONS)

        results[feature] = {
            "total_comments": int(total_comments),
            "count": int(neg_sum),
        }

        print(f"{feature:<15}: comments={total_comments:,}, est_neg_hits={neg_sum:,}")

    return results


# =============================================================================
# BRAND COMPARISON
# =============================================================================

def compare_brands(spark, comments_df, feature_dict):
    print("\n==================== Brand Comparison ====================")

    brands = ["airpods", "sony", "bose", "jabra", "samsung", "anker"]

    EARBUD_KEYWORDS = [
        "earbud",
        "earbuds",
        "earphone",
        "earphones",
        "headphone",
        "headphones",
        "headset",
        "headsets",
        "in-ear",
        "in ear",
        "over-ear",
        "over ear",
        "on-ear",
        "on ear",
        "tws",
        "true wireless",
        "true-wireless",
        "airpods",
        "airpod pro",
        "airpod max",
        "galaxy buds",
        "buds pro",
        "buds live",
        "buds 2",
        "buds fe",
        "sony wf",
        "wf-1000xm4",
        "wf-1000xm5",
        "bose quietcomfort",
        "qc earbuds",
        "soundlink",
        "sport earbuds",
        "beats fit pro",
        "beats studio buds",
        "beats x",
        "beats flex",
        "jabra elite",
        "elite 3",
        "elite 4",
        "elite 7",
        "elite active",
        "anker soundcore",
        "liberty 4",
        "liberty air",
        "life p3",
        "life dot",
        "sennheiser momentum",
        "momentum true wireless",
        "cx plus",
        "cx true wireless",
        "1more",
        "one more",
        "nothing ear",
        "nothing ear 1",
        "nothing ear 2",
        "edifier",
        "soundpeats",
        "tozo",
        "taotronics",
        "marshall mode",
        "marshall minor",
        "cambridge audio",
        "melomania",
        "b&o beoplay",
        "bang & olufsen",
        "technics az60",
        "az80",
        "panasonic earbuds",
        "bluetooth earbuds",
        "wireless earbuds",
        "wireless earphones",
        "noise cancelling headphones",
        "noise canceling earbuds",
        "noise isolation",
        "active noise cancelling",
        "anc mode",
        "transparency mode",
        "charging case",
        "ear tips",
        "fit in ear",
        "ear comfort",
        "audio latency",
        "microphone quality",
        "battery life",
        "eq settings",
        "touch controls",
        "multipoint",
        "connectivity issue",
        "pairing",
        "firmware update",
    ]

    earbud_pattern = build_pattern(EARBUD_KEYWORDS)

    comments_lower = comments_df.persist(StorageLevel.MEMORY_AND_DISK)
    _ = comments_lower.limit(1000).count()
    print("comments_df cached structure initialized.")

    brand_feature_data = {}

    for brand in brands:
        brand_pattern = r"(?i)\b" + re.escape(brand) + r"\b"
        combined_pattern = f"({brand_pattern}).*({earbud_pattern})|({earbud_pattern}).*({brand_pattern})"

        brand_comments = (
            comments_lower
            .filter(col("body_lower").rlike(combined_pattern))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        _ = brand_comments.limit(500).count()
        count = brand_comments.count()
        if count < 10:
            brand_comments.unpersist()
            continue

        print(f"{brand.upper():<10}: {count:,} mentions (earbud-related)")

        brand_features = {}
        for feature_name, keywords in feature_dict.items():
            pattern = build_pattern(keywords)
            feature_count = brand_comments.filter(col("body_lower").rlike(pattern)).count()
            if feature_count > 0:
                brand_features[feature_name] = feature_count

        brand_feature_data[brand] = brand_features
        brand_comments.unpersist()

    spark.catalog.clearCache()
    comments_lower.unpersist()
    spark.catalog.clearCache()

    print("==================== Brand Comparison Completed ====================")
    return brand_feature_data


# =============================================================================
# VISUALIZATION
# =============================================================================

def visualize_results(feature_counts, pain_by_feature, feature_emotions):
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
            plt.xlabel("Estimated Negative Mentions")
            plt.title("Top Pain Points (Emotion-based)")
            plt.gca().invert_yaxis()
            plt.tight_layout()
            out2 = PLOTS_DIR / "3_pain_points_emotions.png"
            plt.savefig(out2.as_posix(), dpi=300)
            plt.close()

    # Emotion heatmap across features
    if feature_emotions:
        heat_df = pd.DataFrame.from_records(
            [
                {"feature": feat, **{lab: vals.get(lab + "_pct", 0.0) for lab in EMOTION_LABELS}}
                for feat, vals in feature_emotions.items()
            ]
        )
        if not heat_df.empty:
            heat_df = heat_df.set_index("feature")[EMOTION_LABELS]
            plt.figure(figsize=(14, max(6, 0.5 * len(heat_df))))
            sns.heatmap(heat_df, annot=False, linewidths=0.3)
            plt.title("Emotion Distribution by Feature (% of emotion hits)")
            plt.tight_layout()
            out3 = PLOTS_DIR / "2_emotion_by_feature_heatmap.png"
            plt.savefig(out3.as_posix(), dpi=300)
            plt.close()


def plot_monthly_comment_trend(comments_df):
    """Simple monthly trend of earbud-related comments over time."""
    print("\nCreating monthly trend plot...")
    monthly_counts = (
        comments_df
        .groupBy("yyyymm")
        .count()
        .orderBy("yyyymm")
        .toPandas()
    )
    if monthly_counts.empty:
        print("No monthly data to plot.")
        return

    # Make x-axis labels nicer
    monthly_counts["yyyymm_str"] = monthly_counts["yyyymm"].astype(str)

    plt.figure(figsize=(12, 6))
    plt.plot(monthly_counts["yyyymm_str"], monthly_counts["count"], marker="o", linewidth=2)
    plt.title("Monthly Trend of Earbud-Related Comments")
    plt.xlabel("Month (YYYYMM)")
    plt.ylabel("Number of Comments")
    plt.xticks(rotation=45, ha="right")
    plt.grid(True)
    plt.tight_layout()

    outpath = PLOTS_DIR / "4_monthly_comment_trend.png"
    plt.savefig(outpath.as_posix(), dpi=300)
    plt.close()
    print(f"Saved monthly trend plot to: {outpath}")


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Wireless Earbuds Analysis (Emotion-Level) — Optimized Full"
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Force local Spark session (debugging).",
    )
    # Default 1.0 = no ingest sampling (full data)
    parser.add_argument(
        "--ingest-sample",
        type=float,
        default=1.0,
        help="Fraction of rows to keep immediately after read (e.g., 0.02 for 2%).",
    )
    parser.add_argument(
        "--nrc",
        type=str,
        default=str(NRC_LEXICON_PATH),
        help="Path to NRC Emotion Lexicon (TSV/CSV): word,emotion,association.",
    )
    parser.add_argument(
        "--emotion-sample",
        type=float,
        default=EMOTION_SAMPLE_FRACTION,
        help="Sample fraction for emotion analysis (0< f ≤1, default=1 for full data).",
    )
    parser.add_argument(
        "--pain-sample",
        type=float,
        default=PAINPOINT_SAMPLE_FRACTION,
        help="Sample fraction for pain estimation (0< f ≤1, default=1 for full data).",
    )
    return parser.parse_args()


def main():
    print("=" * 80)
    print("WIRELESS EARBUDS FEATURES & PAIN POINTS ANALYSIS (EMOTION-LEVEL) — OPTIMIZED FULL")
    print("=" * 80)

    args = parse_args()

    # Allow quick tuning from CLI
    global EMOTION_SAMPLE_FRACTION, PAINPOINT_SAMPLE_FRACTION
    if 0 < args.emotion_sample <= 1:
        EMOTION_SAMPLE_FRACTION = args.emotion_sample
    if 0 < args.pain_sample <= 1:
        PAINPOINT_SAMPLE_FRACTION = args.pain_sample

    # Safety clamp for ingest sample
    ingest_sample = args.ingest_sample
    if not (0 < ingest_sample <= 1.0):
        print(f"[WARN] Invalid --ingest-sample={ingest_sample}, defaulting to 1.0 (no ingest sampling).")
        ingest_sample = 1.0

    spark = create_spark(local=args.local)

    try:
        comments_df, submissions_df = load_and_clean_data(
            spark,
            COMMENTS_PATH,
            SUBMISSIONS_PATH,
            sample_fraction=ingest_sample,
        )

        # Load lexicon (NRC preferred, fallback otherwise)
        lexicon = load_nrc_lexicon(Path(args.nrc))

        # 1) Feature mentions
        feature_counts = extract_feature_mentions(spark, comments_df, AUDIO_FEATURES)

        # 2) Emotion-level analysis by feature (+ monthly)
        feature_emotions, monthly_emotions = analyze_feature_emotions(
            spark,
            comments_df,
            AUDIO_FEATURES,
            lexicon,
        )

        # 3) Pain points via negative-lean emotions
        pain_by_feature = identify_pain_points_emotions(
            spark,
            comments_df,
            AUDIO_FEATURES,
            lexicon,
        )

        # 4) Brand → feature co-mentions
        brand_feature_data = compare_brands(spark, comments_df, AUDIO_FEATURES)

        # 5) Visuals
        visualize_results(feature_counts, pain_by_feature, feature_emotions)
        plot_monthly_comment_trend(comments_df)

        # ------------------------------------------------------------------
        # SAVE CSV RESULTS → data/csv/
        # ------------------------------------------------------------------

        # Feature mentions
        pd.DataFrame(
            [
                {"feature": f, "mentions": v["count"]}
                for f, v in feature_counts.items()
            ]
        ).to_csv((CSV_DIR / "nlp_feature_mentions.csv").as_posix(), index=False)

        # Emotions per feature
        pd.DataFrame(
            [
                {"feature": feat, **vals}
                for feat, vals in feature_emotions.items()
            ]
        ).to_csv((CSV_DIR / "nlp_feature_emotions.csv").as_posix(), index=False)

        # Monthly emotion timelines (one row per feature-month)
        monthly_rows = []
        for feat, months in monthly_emotions.items():
            for yyyymm, vals in months.items():
                row = {"feature": feat, **vals}
                monthly_rows.append(row)

        if monthly_rows:
            pd.DataFrame(monthly_rows).to_csv(
                (CSV_DIR / "nlp_feature_emotions_monthly.csv").as_posix(),
                index=False,
            )

        # Pain points (emotion based)
        pd.DataFrame(
            [
                {"feature": f, "estimated_negative_mentions": v["count"]}
                for f, v in pain_by_feature.items()
            ]
        ).to_csv((CSV_DIR / "nlp_pain_points_emotions.csv").as_posix(), index=False)

        # Brand mentions summary
        brand_summary = []
        for brand, features in brand_feature_data.items():
            total_mentions = sum(features.values())
            top_feats = sorted(features.items(), key=lambda x: x[1], reverse=True)[:3]
            top_feats_str = ", ".join([f"{f} ({c})" for f, c in top_feats])
            brand_summary.append(
                {
                    "brand": brand,
                    "total_mentions": total_mentions,
                    "top_features": top_feats_str,
                }
            )

        pd.DataFrame(brand_summary).to_csv(
            (CSV_DIR / "nlp_brand_mentions.csv").as_posix(),
            index=False,
        )

        # ------------------------------------------------------------------
        # TEXT REPORT → data/txt/
        # ------------------------------------------------------------------

        report_path = TXT_DIR / "ANALYSIS_REPORT_EMOTIONS.txt"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write("WIRELESS EARBUDS ANALYSIS REPORT (EMOTION-LEVEL)\n")
            f.write("=" * 80 + "\n\n")

            # 1) Most Discussed Features
            f.write("1. MOST DISCUSSED FEATURES\n")
            sorted_features = sorted(
                feature_counts.items(),
                key=lambda x: x[1]["count"],
                reverse=True,
            )
            for i, (feat, data) in enumerate(sorted_features[:10], 1):
                f.write(f"{i:2d}. {feat:<18}: {data['count']:>8,} mentions\n")
            f.write("\n")

            # 2) Emotion Landscape by Feature (top 5 by negative-lean)
            f.write("2. MOST NEGATIVE-LEAN FEATURES (by emotion hits)\n")
            neg_score = lambda v: sum(v.get(e, 0) for e in NEG_EMOTIONS)
            sorted_neg = sorted(
                feature_emotions.items(),
                key=lambda x: neg_score(x[1]),
                reverse=True,
            )
            for i, (feat, vals) in enumerate(sorted_neg[:5], 1):
                f.write(f"{i:2d}. {feat:<18}: ")
                f.write(
                    ", ".join(
                        [
                            f"{e}={vals.get(e + '_pct', 0):.1f}%"
                            for e in ["anger", "disgust", "fear", "sadness", "negative"]
                        ]
                    )
                )
                f.write("\n")
            f.write("\n")

            # 3) Top Pain Points (Emotion-based)
            f.write("3. TOP PAIN POINTS (Emotion-based)\n")
            sorted_pain = sorted(
                pain_by_feature.items(),
                key=lambda x: x[1]["count"],
                reverse=True,
            )
            for i, (feat, data) in enumerate(sorted_pain[:10], 1):
                f.write(f"{i:2d}. {feat:<15}: ~{data['count']:,} negative\n")
            f.write("\n")

            # 4) Brand Mentions
            f.write("4. BRAND MENTIONS\n")
            if brand_summary:
                sorted_brands = sorted(
                    brand_summary,
                    key=lambda x: x["total_mentions"],
                    reverse=True,
                )
                for i, b in enumerate(sorted_brands, 1):
                    f.write(f"{i:2d}. {b['brand'].upper():<10}: {b['total_mentions']:>6,} mentions\n")
                    f.write(f"    Top features: {b['top_features']}\n")
            else:
                f.write(" (No brand mentions detected)\n")

            # 5) Brand Impressions (short, data-driven)
            f.write("\n5. BRAND IMPRESSIONS (based on feature + emotion patterns)\n")
            if brand_summary:
                for b in sorted_brands:
                    brand = b["brand"]
                    feats_counts = brand_feature_data.get(brand, {})
                    if not feats_counts:
                        continue

                    # top 2 features for this brand
                    top_feats = sorted(
                        feats_counts.items(),
                        key=lambda x: x[1],
                        reverse=True,
                    )[:2]
                    phrases = []
                    for feat, cnt in top_feats:
                        pretty = FEATURE_FRIENDLY_NAMES.get(
                            feat,
                            feat.replace("_", " "),
                        )
                        emo = feature_emotions.get(feat, {})
                        pos = emo.get("positive_pct", 0.0)
                        neg = emo.get("negative_pct", 0.0)
                        if pos == 0 and neg == 0:
                            tone = ""
                        elif pos >= neg * 1.2:
                            tone = "strength"
                        elif neg >= pos * 1.2:
                            tone = "issue"
                        else:
                            tone = "mixed"

                        if tone:
                            phrases.append(f"{pretty} ({tone})")
                        else:
                            phrases.append(pretty)

                    if phrases:
                        f.write(f" - {brand.title()}: {', '.join(phrases)}\n")
            else:
                f.write(" (No brand impressions — no brand mentions found.)\n")

        print("\nAll results saved under:", DATA_DIR.as_posix())
        print(f" ├─ CSVs:  {CSV_DIR.as_posix()}")
        print(f" ├─ Plots: {PLOTS_DIR.as_posix()}")
        print(f" └─ Reports: {TXT_DIR.as_posix()}")

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
