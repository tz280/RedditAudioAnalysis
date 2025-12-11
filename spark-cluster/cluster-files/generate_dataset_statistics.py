#!/usr/bin/env python3
"""
Generate Dataset Statistics for Filtered Reddit Data.

This script generates three CSV files documenting the filtered dataset:
1. dataset_summary.csv - Overall statistics
2. subreddit_statistics.csv - Per-subreddit breakdown  
3. temporal_distribution.csv - Data distribution over time
"""

import logging
import sys
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, avg, min, max, 
    from_unixtime, to_date, date_format,
    countDistinct, sum as spark_sum,
    lit, coalesce, concat
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)
logger = logging.getLogger(__name__)


def create_spark_session(net_id: str) -> SparkSession:
    """Create Spark session for statistics generation."""
    spark = (
        SparkSession.builder
        .appName(f"Generate_Dataset_Statistics_{net_id}")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .getOrCreate()
    )
    logger.info("Spark session created successfully")
    return spark


def read_filtered_data(spark: SparkSession, net_id: str) -> Tuple[DataFrame, DataFrame]:
    """Read filtered comments and submissions data."""
    logger.info("Reading filtered data from S3...")
    
    comments_path = f"s3a://{net_id}-dsan6000-datasets/project/reddit/parquet/comments/"
    submissions_path = f"s3a://{net_id}-dsan6000-datasets/project/reddit/parquet/submissions/"
    
    print(f"\nReading comments from: {comments_path}")
    comments = spark.read.parquet(comments_path)
    
    print(f"Reading submissions from: {submissions_path}")
    submissions = spark.read.parquet(submissions_path)
    
    # Add date columns
    comments = comments.withColumn("date", to_date(from_unixtime(col("created_utc"))))
    submissions = submissions.withColumn("date", to_date(from_unixtime(col("created_utc"))))
    
    logger.info(f"Loaded comments and submissions with date columns")
    return comments, submissions


def generate_dataset_summary(
    spark: SparkSession,
    comments: DataFrame, 
    submissions: DataFrame,
    net_id: str
) -> None:
    """
    Generate dataset_summary.csv
    Columns: data_type, total_rows, size_gb, date_range_start, date_range_end
    """
    logger.info("Generating dataset_summary.csv...")
    print("\n" + "="*80)
    print("Generating Dataset Summary")
    print("="*80)
    
    # Calculate comments statistics
    comments_count = comments.count()
    comments_min_date = comments.agg(min("date")).collect()[0][0]
    comments_max_date = comments.agg(max("date")).collect()[0][0]
    
    print(f"Comments: {comments_count:,} rows")
    print(f"Date range: {comments_min_date} to {comments_max_date}")
    
    # Calculate submissions statistics
    submissions_count = submissions.count()
    submissions_min_date = submissions.agg(min("date")).collect()[0][0]
    submissions_max_date = submissions.agg(max("date")).collect()[0][0]
    
    print(f"Submissions: {submissions_count:,} rows")
    print(f"Date range: {submissions_min_date} to {submissions_max_date}")
    
    # Create summary dataframe
    summary_data = [
        ("comments", comments_count, "TBD", str(comments_min_date), str(comments_max_date)),
        ("submissions", submissions_count, "TBD", str(submissions_min_date), str(submissions_max_date))
    ]
    
    summary_df = spark.createDataFrame(
        summary_data,
        ["data_type", "total_rows", "size_gb", "date_range_start", "date_range_end"]
    )
    
    # Save to S3
    output_path = f"s3a://{net_id}-dsan6000-datasets/project/data/csv/dataset_summary/"
    summary_df.coalesce(1).write.csv(
        output_path,
        header=True,
        mode="overwrite"
    )
    
    print(f"✅ Saved to: {output_path}")
    logger.info("dataset_summary.csv generated successfully")


def generate_subreddit_statistics(
    spark: SparkSession,
    comments: DataFrame,
    submissions: DataFrame,
    net_id: str
) -> None:
    """
    Generate subreddit_statistics.csv
    Columns: subreddit, num_comments, num_submissions, total_rows, avg_score, date_range
    """
    logger.info("Generating subreddit_statistics.csv...")
    print("\n" + "="*80)
    print("Generating Subreddit Statistics")
    print("="*80)
    
    # Comments by subreddit
    comments_stats = comments.groupBy("subreddit").agg(
        count("*").alias("num_comments"),
        avg("score").alias("avg_comment_score"),
        min("date").alias("min_date"),
        max("date").alias("max_date")
    )
    
    # Submissions by subreddit
    submissions_stats = submissions.groupBy("subreddit").agg(
        count("*").alias("num_submissions"),
        avg("score").alias("avg_submission_score")
    )
    
    # Join statistics
    subreddit_stats = comments_stats.join(
        submissions_stats, 
        "subreddit", 
        "full_outer"
    ).fillna(0, subset=["num_comments", "num_submissions"])
    
    # Calculate combined statistics
    subreddit_stats = subreddit_stats.withColumn(
        "total_rows",
        col("num_comments") + col("num_submissions")
    ).withColumn(
        "avg_score",
        (coalesce(col("avg_comment_score"), lit(0)) + 
         coalesce(col("avg_submission_score"), lit(0))) / 2
    ).withColumn(
        "date_range",
        concat(
            date_format(col("min_date"), "yyyy-MM-dd"),
            lit(" to "),
            date_format(col("max_date"), "yyyy-MM-dd")
        )
    )
    
    # Select final columns
    final_stats = subreddit_stats.select(
        "subreddit",
        "num_comments",
        "num_submissions", 
        "total_rows",
        "avg_score",
        "date_range"
    ).orderBy(col("total_rows").desc())
    
    # Show preview
    print("\nTop 10 subreddits by total rows:")
    final_stats.show(10, truncate=False)
    
    # Save to S3
    output_path = f"s3a://{net_id}-dsan6000-datasets/project/data/csv/subreddit_statistics/"
    final_stats.coalesce(1).write.csv(
        output_path,
        header=True,
        mode="overwrite"
    )
    
    print(f"✅ Saved to: {output_path}")
    logger.info("subreddit_statistics.csv generated successfully")


def generate_temporal_distribution(
    spark: SparkSession,
    comments: DataFrame,
    submissions: DataFrame,
    net_id: str
) -> None:
    """
    Generate temporal_distribution.csv
    Columns: year_month, num_comments, num_submissions, total_rows
    """
    logger.info("Generating temporal_distribution.csv...")
    print("\n" + "="*80)
    print("Generating Temporal Distribution")
    print("="*80)
    
    # Extract year-month from comments
    comments_temporal = comments.withColumn(
        "year_month", 
        date_format(col("date"), "yyyy-MM")
    ).groupBy("year_month").agg(
        count("*").alias("num_comments")
    )
    
    # Extract year-month from submissions
    submissions_temporal = submissions.withColumn(
        "year_month",
        date_format(col("date"), "yyyy-MM")
    ).groupBy("year_month").agg(
        count("*").alias("num_submissions")
    )
    
    # Join temporal statistics
    temporal_dist = comments_temporal.join(
        submissions_temporal,
        "year_month",
        "full_outer"
    ).fillna(0, subset=["num_comments", "num_submissions"])
    
    # Calculate total rows
    temporal_dist = temporal_dist.withColumn(
        "total_rows",
        col("num_comments") + col("num_submissions")
    ).select(
        "year_month",
        "num_comments",
        "num_submissions",
        "total_rows"
    ).orderBy("year_month")
    
    # Show preview
    print("\nTemporal distribution:")
    temporal_dist.show(20, truncate=False)
    
    # Save to S3
    output_path = f"s3a://{net_id}-dsan6000-datasets/project/data/csv/temporal_distribution/"
    temporal_dist.coalesce(1).write.csv(
        output_path,
        header=True,
        mode="overwrite"
    )
    
    print(f"✅ Saved to: {output_path}")
    logger.info("temporal_distribution.csv generated successfully")


def main() -> int:
    """Main function to generate all statistics."""
    print("="*80)
    print("DATASET STATISTICS GENERATION")
    print("="*80)
    
    if len(sys.argv) < 2:
        print("❌ Error: NET_ID not provided")
        print("Usage: python generate_dataset_statistics.py <NET_ID>")
        print("\nExample:")
        print("  spark-submit generate_dataset_statistics.py tz280")
        return 1
    
    net_id = sys.argv[1]
    logger.info(f"Using NET_ID: {net_id}")
    print(f"\nNET_ID: {net_id}")
    
    try:
        # Create Spark session
        spark = create_spark_session(net_id)
        
        # Read filtered data
        comments, submissions = read_filtered_data(spark, net_id)
        
        # Generate all three CSV files
        generate_dataset_summary(spark, comments, submissions, net_id)
        generate_subreddit_statistics(spark, comments, submissions, net_id)
        generate_temporal_distribution(spark, comments, submissions, net_id)
        
        print("\n" + "="*80)
        print("✅ ALL STATISTICS GENERATED SUCCESSFULLY!")
        print("="*80)
        print(f"\nGenerated files saved to:")
        print(f"  - s3://{net_id}-dsan6000-datasets/project/data/csv/dataset_summary/")
        print(f"  - s3://{net_id}-dsan6000-datasets/project/data/csv/subreddit_statistics/")
        print(f"  - s3://{net_id}-dsan6000-datasets/project/data/csv/temporal_distribution/")
        print(f"\nNext step: Download these CSV files to your local data/csv/ folder")
        print("="*80)
        
        spark.stop()
        logger.info("Statistics generation completed successfully")
        return 0
        
    except Exception as e:
        logger.exception(f"Error during statistics generation: {str(e)}")
        print(f"\n❌ Error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())