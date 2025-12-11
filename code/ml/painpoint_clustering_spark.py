#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Painpoint Clustering Prep (Spark)
Author: Team 23

Steps:
1. Load comments + submissions from S3
2. Clean + filter audio-related text
3. Extract painpoint flags (battery, anc, comfort, etc.)
4. (Optimization) Filter to rows with ANY painpoint
5. Compute sentiment (VADER) only on painpoint rows (Pandas UDF)
6. Keep only NEGATIVE rows:
      - Negative comments
      - Negative submissions with actual painpoints
7. Union comments + submissions (fixed schema)
8. Export unified CSV for sklearn clustering
"""

from pathlib import Path

import pandas as pd

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, length, when, regexp_replace, lit, concat_ws
)
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import FloatType

# ================================================================
# PATHS / CONFIG
# ================================================================

PROJECT_ROOT = Path("/home/ubuntu/fall-2025-project-team23")
DATA_DIR = PROJECT_ROOT / "data"
CSV_DIR = DATA_DIR / "csv"
TXT_DIR = DATA_DIR / "txt"
CSV_DIR.mkdir(parents=True, exist_ok=True)
TXT_DIR.mkdir(parents=True, exist_ok=True)

COMMENTS_PATH = "s3a://shh73-dsan6000-datasets/reddit/parquet/comments/"
SUBMISSIONS_PATH = "s3a://shh73-dsan6000-datasets/reddit/parquet/submissions/"

# Toggle this while testing so you don't pull full data
TEST_MODE = False          # 🔁 set to False for full run
TEST_FRACTION = 0.001     # 0.1% sample for quick tests

# Painpoint columns we care about for clustering
PAINPOINTS = {
    "battery": r"battery|charge|charging",
    "anc": r"anc|noise.?cancell",
    "comfort": r"comfort|fit|ear.?tip|ear.?seal",
    "price": r"price|expensive|cheap|budget|\$",
    "durability": r"break|crack|dead|die|stop working|fault",
    "connectivity": r"disconnect|connection|pair|bluetooth issue",
    "sound": r"sound|bass|treble|audio|quality|mids|highs",
}

EXPORT_COLS = [
    "subreddit", "text", "source", "sentiment",
    "battery", "anc", "comfort", "price", "durability",
    "connectivity", "sound",
]

# ================================================================
# SPARK SETUP
# ================================================================

def create_spark(local: bool = False) -> SparkSession:
    print("Creating Spark session...")
    conf = SparkConf().setAppName("Earbuds_Painpoints_Prep")

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
    conf.set("spark.sql.shuffle.partitions", "32")

    if local:
        conf.setMaster("local[*]")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("✅ Spark session initialized.")
    print(" UI:", spark.sparkContext.uiWebUrl)
    print(" Master:", spark.sparkContext.master)
    return spark

# ================================================================
# LOAD COMMENTS
# ================================================================

def load_comments(spark: SparkSession):
    df = spark.read.parquet(COMMENTS_PATH)
    df = (
        df.filter(
            (col("body").isNotNull()) &
            (col("body") != "[deleted]") &
            (col("author") != "AutoModerator") &
            (length(col("body")) > 10)
        )
        .withColumn("text", lower(col("body")))
        .withColumn("source", lit("comment"))
        .drop("body")  # ensure no stray `body` column
    )
    return df

# ================================================================
# LOAD SUBMISSIONS
# ================================================================

def load_submissions(spark: SparkSession):
    df = spark.read.parquet(SUBMISSIONS_PATH)

    df = (
        df.filter(col("title").isNotNull())
          .withColumn(
              "text",
              lower(
                  regexp_replace(
                      concat_ws(" ", col("title"), col("selftext")),
                      r"\s+",
                      " "
                  )
              )
          )
          .withColumn("source", lit("submission"))
    )
    return df

# ================================================================
# AUDIO FILTER
# ================================================================

AUDIO_KW = [
    "earbud", "earbuds", "headphone", "airpod", "bluetooth", "anc",
    "noise cancelling", "xm4", "xm5", "qc", "senn", "bose", "sony", "jbl",
]
AUDIO_PATTERN = "(?i)(" + "|".join(AUDIO_KW) + ")"

def filter_audio(df):
    return df.filter(col("text").rlike(AUDIO_PATTERN))

# ================================================================
# PAINPOINT FLAGS
# ================================================================

def add_painpoints(df):
    """Add binary painpoint columns (0/1) based on regex."""
    for colname, regex in PAINPOINTS.items():
        df = df.withColumn(colname, when(col("text").rlike(regex), 1).otherwise(0))
    return df

def filter_with_any_painpoint(df):
    """Keep only rows where ANY painpoint == 1."""
    pain_cols = list(PAINPOINTS.keys())
    condition = None
    for c in pain_cols:
        flag = (col(c) == 1)
        condition = flag if condition is None else (condition | flag)
    return df.filter(condition)

# ================================================================
# SENTIMENT (VADER via Pandas UDF)
# ================================================================

def add_sentiment(df):
    """
    Add VADER sentiment using a Pandas UDF (batch / vectorized).
    """
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

    @pandas_udf(FloatType())
    def senti_batch(texts: pd.Series) -> pd.Series:
        analyzer = SentimentIntensityAnalyzer()
        texts = texts.fillna("")
        return texts.apply(lambda x: float(analyzer.polarity_scores(x)["compound"]))

    return df.withColumn("sentiment", senti_batch(col("text")))

# ================================================================
# EXPORT
# ================================================================

def export(df):
    """
    Export final combined negative painpoint rows to CSV for sklearn.
    NOTE: .toPandas() assumes this dataset is moderate in size.
    """
    pdf = df.select(*EXPORT_COLS).toPandas()
    outpath = CSV_DIR / "painpoint_vectors_combined.csv"
    pdf.to_csv(outpath, index=False)
    print("Saved combined painpoint vectors →", outpath)
    return outpath

# ================================================================
# MAIN
# ================================================================

def main():
    spark = create_spark(local=False)
    try:
        # ---------------- LOAD ----------------
        comments = load_comments(spark)
        submissions = load_submissions(spark)

        if TEST_MODE:
            print(f"TEST_MODE is ON → sampling {TEST_FRACTION * 100:.3f}% of each source")
            comments = comments.sample(False, TEST_FRACTION, seed=42)
            submissions = submissions.sample(False, TEST_FRACTION, seed=42)

        # ---------------- AUDIO FILTER ----------------
        comments = filter_audio(comments)
        submissions = filter_audio(submissions)

        # ---------------- PAINPOINTS FIRST (OPTIMIZATION) ----------------
        comments = add_painpoints(comments)
        submissions = add_painpoints(submissions)

        comments = filter_with_any_painpoint(comments)
        submissions = filter_with_any_painpoint(submissions)

        # ---------------- SENTIMENT (on reduced set) ----------------
        comments = add_sentiment(comments)
        submissions = add_sentiment(submissions)

        # ---------------- NEGATIVE FILTER ----------------
        comments_neg = comments.filter(col("sentiment") < -0.2)
        submissions_neg = submissions.filter(col("sentiment") < -0.2)

        print("Neg comments (with painpoints):", comments_neg.count())
        print("Neg submissions (with painpoints):", submissions_neg.count())

        # ---------------- FORCE COMMON SCHEMA ----------------
        comments_neg = comments_neg.select(*EXPORT_COLS)
        submissions_neg = submissions_neg.select(*EXPORT_COLS)

        # ---------------- COMBINE & EXPORT ----------------
        combined = comments_neg.unionByName(submissions_neg)

        export(combined)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
