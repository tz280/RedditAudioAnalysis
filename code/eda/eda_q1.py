#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F
from pyspark import SparkConf
import os, glob, shutil, sys, subprocess

# =========================
# CONFIG
# =========================
DATA_PATH   = "s3a://shh73-spark-reddit/reddit/comments/"
OUTPUT_BASE = "/home/ubuntu/data/csv"   # ABSOLUTE path so scp is trivial

AUDIO_SUBS = [
    "headphones", "audiophile", "airpods", "bose", "sony",
    "earbuds", "audio", "technology"
]

NOTECH_REMOVE = {"technology"}  # "notech" variants exclude this

# =========================
# HELPERS
# =========================
def to_timestamp_smart(df, utc_col="created_utc", out_col="created_ts"):
    epoch_s = F.when(F.col(utc_col) > F.lit(10**12),
                     F.col(utc_col) / F.lit(1000)).otherwise(F.col(utc_col))
    return df.withColumn(out_col, F.to_timestamp(F.from_unixtime(epoch_s.cast("bigint"))))

def build_spark():
    conf = SparkConf().setAppName("EDA_Temporal_All")
    conf.set("spark.jars.packages",
             "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider",
             "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
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
    return SparkSession.builder.config(conf=conf).getOrCreate()

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def write_single_csv(df, out_dir, flat_name):
    """
    Write df as CSV (coalesce(1)) to out_dir, then also copy the single part file
    to OUTPUT_BASE/flat_name so you can scp a normal file.
    Prints the exact paths for you.
    """
    df_count = df.count()
    print(f"[INFO] Writing {flat_name} rows = {df_count}")
    if df_count == 0:
        print(f"[WARN] {flat_name} has 0 rows — skipping write.")
        return None, None

    # Write folder
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_dir)
    # Find part file
    part_files = glob.glob(os.path.join(out_dir, "part-*.csv"))
    if not part_files:
        # Some Spark distros create a subdir; try glob recursively
        part_files = glob.glob(os.path.join(out_dir, "**", "part-*.csv"), recursive=True)
    if not part_files:
        print(f"[ERROR] Could not locate part-*.csv under {out_dir}")
        return out_dir, None

    part_path = part_files[0]
    flat_path = os.path.join(OUTPUT_BASE, flat_name)
    try:
        shutil.copy2(part_path, flat_path)
    except Exception as e:
        print(f"[ERROR] copying part to flat file: {e}")

    print(f"✓ Folder: {out_dir}")
    print(f"✓ Part:   {part_path}")
    print(f"✓ Flat:   {flat_path}")
    return out_dir, flat_path

def ls_path(p):
    print(f"\n[ls] {p}")
    try:
        subprocess.run(["/bin/ls", "-lh", p], check=False)
    except Exception as e:
        print(f"(ls error) {e}")

# =========================
# MAIN
# =========================
def main():
    spark = build_spark()
    ensure_dir(OUTPUT_BASE)

    # -------- Load once --------
    df = spark.read.parquet(DATA_PATH)
    df = to_timestamp_smart(df, "created_utc", "created_ts")
    df = (df.select("score", "subreddit", "created_ts")
            .dropna(subset=["created_ts"])
            .withColumn("subreddit", F.lower(F.col("subreddit")))
            .cache())

    print("[INFO] Distinct subreddits in dataset (sample):")
    print([r[0] for r in df.select("subreddit").distinct().limit(20).collect()])

    # -------- Build filtered views --------
    subs_including = [s.lower() for s in AUDIO_SUBS]
    df_incl = df.filter(F.col("subreddit").isin(subs_including))

    subs_excluding = [s for s in subs_including if s not in NOTECH_REMOVE]
    df_notech = df.filter(F.col("subreddit").isin(subs_excluding))

    print("[INFO] Counts after filters:")
    print("  with tech :", df_incl.count())
    print("  no tech   :", df_notech.count())

    # -------- Time buckets --------
    def add_buckets(x):
        return (x.withColumn("hour", F.hour("created_ts"))
                 .withColumn("dow",  F.dayofweek("created_ts"))  # 1=Sun..7=Sat
                 .withColumn("mon",  F.month("created_ts"))
                 .withColumn("yr",   F.year("created_ts")))

    df_incl_b = add_buckets(df_incl)
    df_notech_b = add_buckets(df_notech)

    # -------- Aggregations (by subreddit) --------
    hourly_sub_incl = (df_incl_b.groupBy("subreddit","hour")
                       .agg(F.count(F.lit(1)).alias("post_count"), F.avg("score").alias("avg_score"))
                       .orderBy("subreddit","hour"))

    ymdow_sub_incl  = (df_incl_b.groupBy("subreddit","yr","mon","dow")
                       .agg(F.count(F.lit(1)).alias("post_count"), F.avg("score").alias("avg_score"))
                       .orderBy("subreddit","yr","mon","dow"))

    hourly_sub_notech = (df_notech_b.groupBy("subreddit","hour")
                         .agg(F.count(F.lit(1)).alias("post_count"), F.avg("score").alias("avg_score"))
                         .orderBy("subreddit","hour"))

    ymdow_sub_notech  = (df_notech_b.groupBy("subreddit","yr","mon","dow")
                         .agg(F.count(F.lit(1)).alias("post_count"), F.avg("score").alias("avg_score"))
                         .orderBy("subreddit","yr","mon","dow"))

    # (Optional) Overall (no subreddit grouping)
    hourly_overall = (df_incl_b.groupBy("hour")
                      .agg(F.count(F.lit(1)).alias("post_count"), F.avg("score").alias("avg_score"))
                      .orderBy("hour"))

    ymdow_overall  = (df_incl_b.groupBy("yr","mon","dow")
                      .agg(F.count(F.lit(1)).alias("post_count"), F.avg("score").alias("avg_score"))
                      .orderBy("yr","mon","dow"))

    # -------- Writes (folders + flat CSV files) --------
    paths = []

    paths.append(write_single_csv(hourly_sub_incl,
        f"{OUTPUT_BASE}/eda_temporal_by_hour_subreddit",
        "eda_temporal_by_hour_subreddit.csv"))

    paths.append(write_single_csv(ymdow_sub_incl,
        f"{OUTPUT_BASE}/eda_temporal_by_y_m_dow_subreddit",
        "eda_temporal_by_y_m_dow_subreddit.csv"))

    paths.append(write_single_csv(hourly_sub_notech,
        f"{OUTPUT_BASE}/eda_temporal_by_hour_subredditnotech",
        "eda_temporal_by_hour_subredditnotech.csv"))

    paths.append(write_single_csv(ymdow_sub_notech,
        f"{OUTPUT_BASE}/eda_temporal_by_y_m_dow_subredditnotech",
        "eda_temporal_by_y_m_dow_subredditnotech.csv"))

    # Optional overall outputs (comment out if you don’t want them)
    paths.append(write_single_csv(hourly_overall,
        f"{OUTPUT_BASE}/eda_temporal_by_hour_overall",
        "eda_temporal_by_hour_overall.csv"))

    paths.append(write_single_csv(ymdow_overall,
        f"{OUTPUT_BASE}/eda_temporal_by_y_m_dow_overall",
        "eda_temporal_by_y_m_dow_overall.csv"))

    # -------- Show where everything is --------
    ls_path(OUTPUT_BASE)
    for p in [os.path.join(OUTPUT_BASE, d) for d in os.listdir(OUTPUT_BASE)]:
        if os.path.isdir(p):
            ls_path(p)
    # also show flat CSVs:
    print("\n[INFO] Flat CSVs under OUTPUT_BASE:")
    for f in sorted(glob.glob(os.path.join(OUTPUT_BASE, "*.csv"))):
        print("  -", f)

    spark.stop()

if __name__ == "__main__":
    sys.exit(main())
