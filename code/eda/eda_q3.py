#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, length, hour, dayofweek, month, year,
    when, avg, min as spark_min, max as spark_max,
    sum as spark_sum, stddev, percentile_approx,
    to_date, from_unixtime, concat, lit, floor
)
from pyspark.sql.types import IntegerType
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from scipy.stats import pearsonr, spearmanr

# ============================================================================
# CONFIGURATION
# ============================================================================

SUBMISSIONS_PATH = 's3a://jl3205-dsan6000-datasets/project/reddit/parquet/submissions/'
COMMENTS_PATH = 's3a://jl3205-dsan6000-datasets/project/reddit/parquet/comments/'

# Output paths
PLOTS_DIR = 'data/plots/'
CSV_DIR = 'data/csv/'

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
COLOR_PRIMARY = '#2E86AB'
COLOR_SECONDARY = '#A23B72'
COLOR_ACCENT = '#F18F01'

# ============================================================================
# SPARK SESSION SETUP
# ============================================================================

print("="*80)
print("EDA QUESTION 3: POST TIMING & STRUCTURE VS DISCUSSION DEPTH")
print("="*80)

spark = SparkSession.builder \
    .appName("EDA_Q3_Post_Timing_Structure") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.attempts.maximum", "10") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
    .getOrCreate()

# ============================================================================
# DATA LOADING
# ============================================================================

print("\n[1/6] Loading data from S3...")

submissions = spark.read.parquet(SUBMISSIONS_PATH)
comments = spark.read.parquet(COMMENTS_PATH)

# Convert dates
submissions = submissions.withColumn('date', to_date(col('date')))
comments = comments.withColumn('date', to_date(col('date')))

# Clean and prepare submissions data
submissions = submissions \
    .withColumn('score', col('score').cast('int')) \
    .withColumn('score', when(col('score').isNull(), 0).otherwise(col('score'))) \
    .withColumn('num_comments', col('num_comments').cast('int')) \
    .withColumn('num_comments', when(col('num_comments').isNull(), 0).otherwise(col('num_comments')))

# Cache for multiple operations
submissions.cache()
comments.cache()

print(f"Loaded {submissions.count():,} submissions")
print(f"Loaded {comments.count():,} comments")

# ============================================================================
# FEATURE ENGINEERING
# ============================================================================

print("\n[2/6] Engineering features...")

# Add temporal features to submissions
submissions_with_features = submissions \
    .withColumn('title_length', length(col('title'))) \
    .withColumn('hour_posted', hour(from_unixtime(col('created_utc')))) \
    .withColumn('day_of_week', dayofweek(from_unixtime(col('created_utc')))) \
    .withColumn('month', month(from_unixtime(col('created_utc')))) \
    .withColumn('year', year(from_unixtime(col('created_utc')))) \
    .withColumn('has_selftext', when(length(col('selftext')) > 0, 1).otherwise(0)) \
    .withColumn('is_nsfw', when(col('over_18') == True, 1).otherwise(0))

# Create link_id for joining (comments have link_id like "t3_xxxxx", submissions have id)
submissions_with_features = submissions_with_features \
    .withColumn('link_id', concat(lit('t3_'), col('id')))

# ============================================================================
# JOIN SUBMISSIONS WITH COMMENT COUNTS
# ============================================================================

print("\n[3/6] Joining submissions with actual comment counts...")

# Count comments per submission
actual_comment_counts = comments.groupBy('link_id').agg(
    count('*').alias('actual_comment_count')
)

# Join with submissions
posts_with_comments = submissions_with_features.join(
    actual_comment_counts,
    on='link_id',
    how='left'
).fillna(0, subset=['actual_comment_count'])

# Cache the joined dataset
posts_with_comments.cache()

print(f"Joined dataset size: {posts_with_comments.count():,} posts")

# ============================================================================
# CORRELATION ANALYSIS
# ============================================================================

print("\n[4/6] Computing correlations...")

# Select relevant features for correlation analysis
correlation_features = [
    'score', 'title_length', 'hour_posted', 'day_of_week',
    'has_selftext', 'is_nsfw', 'actual_comment_count'
]

# Convert to Pandas for correlation analysis
correlation_data = posts_with_comments.select(correlation_features).toPandas()

# Compute correlation matrix
correlation_matrix = correlation_data.corr()
print("\nCorrelation with actual_comment_count:")
print(correlation_matrix['actual_comment_count'].sort_values(ascending=False))

# Save correlation matrix
correlation_matrix.to_csv(f'{CSV_DIR}eda_q3_correlation_matrix.csv')

# ============================================================================
# TEMPORAL ANALYSIS
# ============================================================================

print("\n[5/6] Analyzing temporal patterns...")

# Aggregate by hour of day
hourly_engagement = posts_with_comments.groupBy('hour_posted').agg(
    count('*').alias('num_posts'),
    avg('actual_comment_count').alias('avg_comments'),
    avg('score').alias('avg_score'),
    percentile_approx('actual_comment_count', 0.5).alias('median_comments')
).orderBy('hour_posted')

hourly_df = hourly_engagement.toPandas()
hourly_df.to_csv(f'{CSV_DIR}eda_q3_hourly_engagement.csv', index=False)

# Aggregate by day of week (1=Sunday, 7=Saturday)
dow_engagement = posts_with_comments.groupBy('day_of_week').agg(
    count('*').alias('num_posts'),
    avg('actual_comment_count').alias('avg_comments'),
    avg('score').alias('avg_score'),
    percentile_approx('actual_comment_count', 0.5).alias('median_comments')
).orderBy('day_of_week')

dow_df = dow_engagement.toPandas()
dow_df['day_name'] = dow_df['day_of_week'].map({
    1: 'Sunday', 2: 'Monday', 3: 'Tuesday', 4: 'Wednesday',
    5: 'Thursday', 6: 'Friday', 7: 'Saturday'
})
dow_df.to_csv(f'{CSV_DIR}eda_q3_day_of_week_engagement.csv', index=False)

# ============================================================================
# SCORE BINS ANALYSIS
# ============================================================================

# Create score bins
posts_with_bins = posts_with_comments.withColumn(
    'score_bin',
    when(col('score') <= 1, '0-1')
    .when(col('score') <= 5, '2-5')
    .when(col('score') <= 10, '6-10')
    .when(col('score') <= 25, '11-25')
    .when(col('score') <= 50, '26-50')
    .when(col('score') <= 100, '51-100')
    .otherwise('100+')
)

score_bin_engagement = posts_with_bins.groupBy('score_bin').agg(
    count('*').alias('num_posts'),
    avg('actual_comment_count').alias('avg_comments'),
    percentile_approx('actual_comment_count', 0.5).alias('median_comments'),
    spark_max('actual_comment_count').alias('max_comments')
)

score_bin_df = score_bin_engagement.toPandas()
# Order bins properly
bin_order = ['0-1', '2-5', '6-10', '11-25', '26-50', '51-100', '100+']
score_bin_df['score_bin'] = pd.Categorical(score_bin_df['score_bin'], categories=bin_order, ordered=True)
score_bin_df = score_bin_df.sort_values('score_bin')
score_bin_df.to_csv(f'{CSV_DIR}eda_q3_score_bins_engagement.csv', index=False)

# ============================================================================
# TITLE LENGTH ANALYSIS
# ============================================================================

# Create title length bins
posts_with_title_bins = posts_with_comments.withColumn(
    'title_length_bin',
    when(col('title_length') <= 30, '0-30')
    .when(col('title_length') <= 60, '31-60')
    .when(col('title_length') <= 90, '61-90')
    .when(col('title_length') <= 120, '91-120')
    .otherwise('120+')
)

title_length_engagement = posts_with_title_bins.groupBy('title_length_bin').agg(
    count('*').alias('num_posts'),
    avg('actual_comment_count').alias('avg_comments'),
    avg('score').alias('avg_score')
)

title_length_df = title_length_engagement.toPandas()
title_order = ['0-30', '31-60', '61-90', '91-120', '120+']
title_length_df['title_length_bin'] = pd.Categorical(
    title_length_df['title_length_bin'],
    categories=title_order,
    ordered=True
)
title_length_df = title_length_df.sort_values('title_length_bin')
title_length_df.to_csv(f'{CSV_DIR}eda_q3_title_length_engagement.csv', index=False)

# ============================================================================
# CONTENT TYPE ANALYSIS
# ============================================================================

# Selftext vs no selftext
content_type_engagement = posts_with_comments.groupBy('has_selftext').agg(
    count('*').alias('num_posts'),
    avg('actual_comment_count').alias('avg_comments'),
    avg('score').alias('avg_score'),
    percentile_approx('actual_comment_count', 0.5).alias('median_comments')
)

content_type_df = content_type_engagement.toPandas()
content_type_df['content_type'] = content_type_df['has_selftext'].map({
    0: 'Link/Media Only',
    1: 'Has Text Content'
})
content_type_df.to_csv(f'{CSV_DIR}eda_q3_content_type_engagement.csv', index=False)

# ============================================================================
# VISUALIZATIONS
# ============================================================================

print("\n[6/6] Creating visualizations...")

# VIZ 1: Hourly posting patterns and engagement
fig1, axes = plt.subplots(2, 1, figsize=(14, 10))

# Posts and comments by hour
ax1 = axes[0]
ax1_twin = ax1.twinx()
ax1.bar(hourly_df['hour_posted'], hourly_df['num_posts'],
        alpha=0.6, color=COLOR_PRIMARY, label='Number of Posts')
ax1_twin.plot(hourly_df['hour_posted'], hourly_df['avg_comments'],
              color=COLOR_ACCENT, marker='o', linewidth=2, markersize=6,
              label='Avg Comments per Post')
ax1.set_xlabel('Hour of Day (UTC)', fontsize=12, fontweight='bold')
ax1.set_ylabel('Number of Posts', fontsize=11, color=COLOR_PRIMARY)
ax1_twin.set_ylabel('Avg Comments per Post', fontsize=11, color=COLOR_ACCENT)
ax1.set_title('Posting Activity and Discussion Depth by Hour of Day',
              fontsize=13, fontweight='bold')
ax1.tick_params(axis='y', labelcolor=COLOR_PRIMARY)
ax1_twin.tick_params(axis='y', labelcolor=COLOR_ACCENT)
ax1.set_xticks(range(0, 24))
ax1.grid(alpha=0.3)
ax1.legend(loc='upper left')
ax1_twin.legend(loc='upper right')

# Median comments by hour (more robust to outliers)
ax2 = axes[1]
ax2.plot(hourly_df['hour_posted'], hourly_df['median_comments'],
         color=COLOR_SECONDARY, marker='s', linewidth=2.5, markersize=7)
ax2.fill_between(hourly_df['hour_posted'], hourly_df['median_comments'],
                  alpha=0.3, color=COLOR_SECONDARY)
ax2.set_xlabel('Hour of Day (UTC)', fontsize=12, fontweight='bold')
ax2.set_ylabel('Median Comments per Post', fontsize=11)
ax2.set_title('Median Discussion Depth by Hour (Robust to Outliers)',
              fontsize=13, fontweight='bold')
ax2.set_xticks(range(0, 24))
ax2.grid(alpha=0.3)

plt.tight_layout()
plt.savefig(f'{PLOTS_DIR}eda_q3_hourly_patterns.png', dpi=300, bbox_inches='tight')
plt.close()
print("Saved: eda_q3_hourly_patterns.png")

# VIZ 2: Day of week patterns
fig2, ax = plt.subplots(figsize=(12, 6))
x_pos = np.arange(len(dow_df))
bars = ax.bar(x_pos, dow_df['avg_comments'], color=COLOR_PRIMARY, alpha=0.7,
              edgecolor='black', linewidth=1.5)
ax.set_xlabel('Day of Week', fontsize=12, fontweight='bold')
ax.set_ylabel('Average Comments per Post', fontsize=11)
ax.set_title('Discussion Depth by Day of Week', fontsize=13, fontweight='bold')
ax.set_xticks(x_pos)
ax.set_xticklabels(dow_df['day_name'], rotation=45, ha='right')
ax.grid(axis='y', alpha=0.3)

# Add value labels on bars
for i, (bar, val) in enumerate(zip(bars, dow_df['avg_comments'])):
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{val:.2f}', ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig(f'{PLOTS_DIR}eda_q3_day_of_week.png', dpi=300, bbox_inches='tight')
plt.close()
print("Saved: eda_q3_day_of_week.png")

# VIZ 3: Score bins vs comment count
fig3, axes = plt.subplots(1, 2, figsize=(16, 6))

# Average comments by score bin
ax1 = axes[0]
bars1 = ax1.bar(range(len(score_bin_df)), score_bin_df['avg_comments'],
                color=COLOR_ACCENT, alpha=0.7, edgecolor='black')
ax1.set_xlabel('Post Score Bin', fontsize=12, fontweight='bold')
ax1.set_ylabel('Average Comments', fontsize=11)
ax1.set_title('Discussion Depth by Post Score Range', fontsize=13, fontweight='bold')
ax1.set_xticks(range(len(score_bin_df)))
ax1.set_xticklabels(score_bin_df['score_bin'], rotation=45, ha='right')
ax1.grid(axis='y', alpha=0.3)

# Median comments by score bin (log scale)
ax2 = axes[1]
ax2.bar(range(len(score_bin_df)), score_bin_df['median_comments'],
        color=COLOR_SECONDARY, alpha=0.7, edgecolor='black')
ax2.set_xlabel('Post Score Bin', fontsize=12, fontweight='bold')
ax2.set_ylabel('Median Comments (log scale)', fontsize=11)
ax2.set_title('Median Discussion Depth by Post Score (Robust Metric)',
              fontsize=13, fontweight='bold')
ax2.set_xticks(range(len(score_bin_df)))
ax2.set_xticklabels(score_bin_df['score_bin'], rotation=45, ha='right')
ax2.set_yscale('log')
ax2.grid(axis='y', alpha=0.3, which='both')

plt.tight_layout()
plt.savefig(f'{PLOTS_DIR}eda_q3_score_bins.png', dpi=300, bbox_inches='tight')
plt.close()
print("Saved: eda_q3_score_bins.png")

# VIZ 4: Title length impact
fig4, ax = plt.subplots(figsize=(10, 6))
bars = ax.bar(range(len(title_length_df)), title_length_df['avg_comments'],
              color=COLOR_PRIMARY, alpha=0.7, edgecolor='black', linewidth=1.5)
ax.set_xlabel('Title Length (characters)', fontsize=12, fontweight='bold')
ax.set_ylabel('Average Comments per Post', fontsize=11)
ax.set_title('Impact of Title Length on Discussion Depth', fontsize=13, fontweight='bold')
ax.set_xticks(range(len(title_length_df)))
ax.set_xticklabels(title_length_df['title_length_bin'], rotation=45, ha='right')
ax.grid(axis='y', alpha=0.3)

# Add value labels
for bar, val in zip(bars, title_length_df['avg_comments']):
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{val:.2f}', ha='center', va='bottom', fontweight='bold', fontsize=9)

plt.tight_layout()
plt.savefig(f'{PLOTS_DIR}eda_q3_title_length.png', dpi=300, bbox_inches='tight')
plt.close()
print("Saved: eda_q3_title_length.png")

# VIZ 5: Content type comparison
fig5, axes = plt.subplots(1, 2, figsize=(14, 6))

# Bar chart
ax1 = axes[0]
bars = ax1.bar(range(len(content_type_df)), content_type_df['avg_comments'],
               color=[COLOR_PRIMARY, COLOR_ACCENT], alpha=0.7, edgecolor='black', linewidth=1.5)
ax1.set_xlabel('Content Type', fontsize=12, fontweight='bold')
ax1.set_ylabel('Average Comments', fontsize=11)
ax1.set_title('Discussion Depth: Link/Media vs Text Posts', fontsize=13, fontweight='bold')
ax1.set_xticks(range(len(content_type_df)))
ax1.set_xticklabels(content_type_df['content_type'], rotation=20, ha='right')
ax1.grid(axis='y', alpha=0.3)

for bar, val in zip(bars, content_type_df['avg_comments']):
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height,
             f'{val:.2f}', ha='center', va='bottom', fontweight='bold')

# Pie chart for post distribution
ax2 = axes[1]
ax2.pie(content_type_df['num_posts'], labels=content_type_df['content_type'],
        autopct='%1.1f%%', colors=[COLOR_PRIMARY, COLOR_ACCENT],
        startangle=90, textprops={'fontsize': 11, 'fontweight': 'bold'})
ax2.set_title('Distribution of Post Types', fontsize=13, fontweight='bold')

plt.tight_layout()
plt.savefig(f'{PLOTS_DIR}eda_q3_content_type.png', dpi=300, bbox_inches='tight')
plt.close()
print("Saved: eda_q3_content_type.png")

# VIZ 6: Correlation heatmap
fig6, ax = plt.subplots(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, fmt='.3f', cmap='coolwarm',
            center=0, square=True, linewidths=1, cbar_kws={"shrink": 0.8},
            ax=ax)
ax.set_title('Correlation Matrix: Post Features vs Discussion Depth',
             fontsize=13, fontweight='bold', pad=20)
plt.tight_layout()
plt.savefig(f'{PLOTS_DIR}eda_q3_correlation_heatmap.png', dpi=300, bbox_inches='tight')
plt.close()
print("Saved: eda_q3_correlation_heatmap.png")

# ============================================================================
# SUMMARY STATISTICS
# ============================================================================

print("\n" + "="*80)
print("SUMMARY STATISTICS")
print("="*80)

# Overall statistics
overall_stats = posts_with_comments.select(
    avg('actual_comment_count').alias('avg_comments'),
    percentile_approx('actual_comment_count', 0.5).alias('median_comments'),
    spark_max('actual_comment_count').alias('max_comments'),
    stddev('actual_comment_count').alias('std_comments')
).collect()[0]

print(f"\nOverall Comment Statistics:")
print(f"  Average: {overall_stats['avg_comments']:.2f}")
print(f"  Median: {overall_stats['median_comments']:.2f}")
print(f"  Max: {overall_stats['max_comments']}")
print(f"  Std Dev: {overall_stats['std_comments']:.2f}")

print(f"\nPeak Engagement Hour: {hourly_df.loc[hourly_df['avg_comments'].idxmax(), 'hour_posted']:.0f}:00 UTC")
print(f"Best Day for Engagement: {dow_df.loc[dow_df['avg_comments'].idxmax(), 'day_name']}")

print(f"\nCorrelation with Comment Count:")
print(f"  Score: {correlation_matrix.loc['score', 'actual_comment_count']:.3f}")
print(f"  Title Length: {correlation_matrix.loc['title_length', 'actual_comment_count']:.3f}")

# Save summary
summary_data = {
    'Metric': [
        'Average Comments per Post',
        'Median Comments per Post',
        'Max Comments on Single Post',
        'Std Dev of Comments',
        'Peak Engagement Hour (UTC)',
        'Best Day for Engagement',
        'Correlation: Score vs Comments',
        'Correlation: Title Length vs Comments'
    ],
    'Value': [
        f"{overall_stats['avg_comments']:.2f}",
        f"{overall_stats['median_comments']:.2f}",
        f"{overall_stats['max_comments']}",
        f"{overall_stats['std_comments']:.2f}",
        f"{hourly_df.loc[hourly_df['avg_comments'].idxmax(), 'hour_posted']:.0f}:00",
        dow_df.loc[dow_df['avg_comments'].idxmax(), 'day_name'],
        f"{correlation_matrix.loc['score', 'actual_comment_count']:.3f}",
        f"{correlation_matrix.loc['title_length', 'actual_comment_count']:.3f}"
    ]
}

summary_df = pd.DataFrame(summary_data)
summary_df.to_csv(f'{CSV_DIR}eda_q3_summary.csv', index=False)

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)
print(f"\nGenerated CSV files in: {CSV_DIR}")
print(f"Generated visualizations in: {PLOTS_DIR}")
print("\nNext steps:")
print("  1. Review visualizations and insights")
print("  2. Update website-source/eda.qmd with findings")
print("  3. Interpret results in business context")
print("="*80)

# Stop Spark
spark.stop()
