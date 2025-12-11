from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, from_unixtime, to_timestamp,
    hour, dayofweek, month, year, count, avg
)

DATA_PATH = "s3a://shh73-spark-reddit/reddit/comments/yyyy=2024/mm=01/"
AUDIO_SUBS = ["headphones","audiophile","airpods","bose","sony","Earbuds","audio","technology"]

def to_timestamp_smart(df, utc_col="created_utc", out_col="created_ts"):
    epoch_s = when(col(utc_col) > lit(10**12), col(utc_col) / lit(1000)).otherwise(col(utc_col))
    return df.withColumn(out_col, to_timestamp(from_unixtime(epoch_s.cast("bigint"))))

spark = (
    SparkSession.builder
      .appName("EDA_Temporal_BySubreddit")
      .config("spark.jars.packages",
              "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
      .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider",
              "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
      .getOrCreate()
)

df = spark.read.parquet(DATA_PATH)
df = to_timestamp_smart(df, "created_utc", "created_ts")

df = (df.select("score","subreddit","created_ts")
        .dropna(subset=["created_ts"])
        .filter(col("subreddit").isin(AUDIO_SUBS))
        .cache())

df = (df.withColumn("hour", hour("created_ts"))
        .withColumn("dow", dayofweek("created_ts"))
        .withColumn("mon", month("created_ts"))
        .withColumn("yr", year("created_ts")))

hourly_sub = (df.groupBy("subreddit","hour")
                .agg(count("*").alias("post_count"), avg("score").alias("avg_score"))
                .orderBy("subreddit","hour"))
hourly_sub.coalesce(1).write.mode("overwrite").option("header",True)\
    .csv("data/csv/eda_temporal_by_hour_subreddit")

ymdow_sub = (df.groupBy("subreddit","yr","mon","dow")
               .agg(count("*").alias("post_count"), avg("score").alias("avg_score"))
               .orderBy("subreddit","yr","mon","dow"))
ymdow_sub.coalesce(1).write.mode("overwrite").option("header",True)\
    .csv("data/csv/eda_temporal_by_y_m_dow_subreddit")

print("Saved:")
print(" - data/csv/eda_temporal_by_hour_subreddit/")
print(" - data/csv/eda_temporal_by_y_m_dow_subreddit/")
spark.stop()
