from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, mean, median, stddev, sum as spark_sum, count, 
    countDistinct, datediff, min as spark_min, max as spark_max, lit, when
)
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
from datetime import datetime

spark = SparkSession.builder \
    .appName("Reddit EDA analysis: question 2") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

SUBMISSIONS_PATH = 's3a://project-zp134/submissions/'
COMMENTS_PATH = 's3a://project-zp134/comments/'
PLOTS_DIR = '../data/plots/'
CSV_DIR = '../data/csv/'

BRAND_SUBREDDITS = ['sennheiser', 'sony', 'bose', 'beats', 'JBL', 'AKG',
                   'airpods', 'bang & olufsen']
GENERAL_AUDIO_SUBREDDITS = ['headphones', 'audiophile', 'audio', 'BudgetAudiophile',
                           'HeadphoneAdvice', 'Earbuds']

COLOR_BRAND = 'lightcoral'        
COLOR_GENERAL = 'mediumturquoise' 
COLOR_ACCENT = 'mediumorchid'     
COLOR_LINE = 'red'

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

print("Loading data from S3...")

submissions = spark.read.parquet(SUBMISSIONS_PATH)
comments = spark.read.parquet(COMMENTS_PATH)

submissions = submissions.withColumn('date', to_date(col('date')))
comments = comments.withColumn('date', to_date(col('date')))

submissions = submissions \
    .withColumn('score', col('score').cast('int')) \
    .withColumn('score', when(col('score').isNull(), 0).otherwise(col('score'))) \
    .withColumn('num_comments', col('num_comments').cast('int')) \
    .withColumn('num_comments', when(col('num_comments').isNull(), 0).otherwise(col('num_comments')))

comments = comments \
    .withColumn('score', col('score').cast('int')) \
    .withColumn('score', when(col('score').isNull(), 0).otherwise(col('score')))

all_audio_subreddits = BRAND_SUBREDDITS + GENERAL_AUDIO_SUBREDDITS
audio_submissions = submissions.filter(col('subreddit').isin(all_audio_subreddits))
audio_comments = comments.filter(col('subreddit').isin(all_audio_subreddits))
audio_submissions.cache()
audio_comments.cache()

print(f"Total audio submissions: {audio_submissions.count()}")
print(f"Total audio comments: {audio_comments.count()}")
print("Computing engagement metrics...")

engagement_metrics_spark = audio_submissions.groupBy('subreddit').agg(
    mean('score').alias('score_mean'),
    median('score').alias('score_median'),
    stddev('score').alias('score_std'),
    spark_sum('score').alias('score_sum'),
    mean('num_comments').alias('num_comments_mean'),
    median('num_comments').alias('num_comments_median'),
    stddev('num_comments').alias('num_comments_std'),
    spark_sum('num_comments').alias('num_comments_sum'),
    count('id').alias('total_posts'),
    countDistinct('author').alias('unique_authors'),
    spark_min('date').alias('min_date'),
    spark_max('date').alias('max_date')
)

engagement_metrics_spark = engagement_metrics_spark.withColumn(
    'days', 
    datediff(col('max_date'), col('min_date')) + 1
).withColumn(
    'posts_per_day',
    col('total_posts') / col('days')
)

def get_community_type(subreddit):
    return 'Brand' if subreddit in BRAND_SUBREDDITS else 'General Audio'

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

type_udf = udf(get_community_type, StringType())
engagement_metrics_spark = engagement_metrics_spark.withColumn('type', type_udf(col('subreddit')))
engagement_metrics_spark = engagement_metrics_spark.withColumn(
    'total_activity',
    col('total_posts') + col('num_comments_sum')
)
engagement_metrics = engagement_metrics_spark.toPandas()
engagement_metrics.set_index('subreddit', inplace=True)

numeric_cols = engagement_metrics.select_dtypes(include=[np.number]).columns
engagement_metrics[numeric_cols] = engagement_metrics[numeric_cols].round(2)

engagement_metrics_by_mean = engagement_metrics.sort_values('score_mean', ascending=False)
engagement_metrics_by_median = engagement_metrics.sort_values('score_median', ascending=False)

print("Top subreddits by mean score:")
print(engagement_metrics_by_mean[['score_mean', 'score_median', 'num_comments_mean', 
                                   'num_comments_median', 'posts_per_day']].head(10))

print("Analyzing user overlap...")

sub_users = audio_submissions.select('author', 'subreddit').withColumn('activity', lit('post'))
comm_users = audio_comments.select('author', 'subreddit').withColumn('activity', lit('comment'))

all_user_activity = sub_users.union(comm_users).filter(col('author') != '[deleted]')

user_participation = all_user_activity.groupBy('author') \
    .agg(countDistinct('subreddit').alias('num_subreddits'))

multi_sub_users = user_participation.filter(col('num_subreddits') > 1)

print(f"Total unique users: {user_participation.count()}")
print(f"Multi-community users: {multi_sub_users.count()}")

user_subreddit_pairs = all_user_activity.select('author', 'subreddit').distinct()

overlap_spark = user_subreddit_pairs.alias('a').join(
    user_subreddit_pairs.alias('b'),
    col('a.author') == col('b.author')
).filter(
    col('a.subreddit') < col('b.subreddit')  
).groupBy(
    col('a.subreddit').alias('sub1'),
    col('b.subreddit').alias('sub2')
).agg(
    count('*').alias('shared_users')
).orderBy(col('shared_users').desc())

overlap_df = overlap_spark.toPandas()

overlap_matrix = pd.pivot_table(
    overlap_df, 
    values='shared_users', 
    index='sub1', 
    columns='sub2', 
    fill_value=0
)

overlap_matrix_full = overlap_matrix.add(overlap_matrix.T, fill_value=0)
for sub in engagement_metrics.index:
    if sub not in overlap_matrix_full.index:
        overlap_matrix_full.loc[sub, sub] = 0
    else:
        overlap_matrix_full.loc[sub, sub] = engagement_metrics.loc[sub, 'unique_authors']

# Distribution statistics

submissions_stats = audio_submissions.select(
    mean('score').alias('mean_score'),
    median('score').alias('median_score'),
    stddev('score').alias('std_score'),
    spark_min('score').alias('min_score'),
    spark_max('score').alias('max_score'),
    mean('num_comments').alias('mean_comments'),
    median('num_comments').alias('median_comments'),
    stddev('num_comments').alias('std_comments'),
    spark_min('num_comments').alias('min_comments'),
    spark_max('num_comments').alias('max_comments')
).collect()[0]

#print("\nSubmissions Score Statistics:")
#print(f"  Mean: {submissions_stats['mean_score']:.2f}")
#print(f"  Median: {submissions_stats['median_score']:.2f}")
#print(f"  Std Dev: {submissions_stats['std_score']:.2f}")
#print(f"  Min: {submissions_stats['min_score']}")
#print(f"  Max: {submissions_stats['max_score']}")

#print("\nSubmissions Comments Statistics:")
#print(f"  Mean: {submissions_stats['mean_comments']:.2f}")
#print(f"  Median: {submissions_stats['median_comments']:.2f}")
#print(f"  Std Dev: {submissions_stats['std_comments']:.2f}")
#print(f"  Min: {submissions_stats['min_comments']}")
#print(f"  Max: {submissions_stats['max_comments']}")



# VIZ 1: Mean-based engagement bar chart
fig1, axes = plt.subplots(2, 2, figsize=(16, 10))

# Top by mean score
top_by_score = engagement_metrics_by_mean.head(11)
colors_score = [COLOR_BRAND if t == 'Brand' else COLOR_GENERAL for t in top_by_score['type']]
axes[0, 0].barh(range(len(top_by_score)), top_by_score['score_mean'], color=colors_score)
axes[0, 0].set_yticks(range(len(top_by_score)))
axes[0, 0].set_yticklabels(top_by_score.index)
axes[0, 0].set_xlabel('Mean Score', fontsize=11)
axes[0, 0].set_title('Top Subreddits by Mean Score', fontsize=12, fontweight='bold')
axes[0, 0].invert_yaxis()
axes[0, 0].grid(axis='x', alpha=0.3)

# Top by mean comments
top_by_comments = engagement_metrics.sort_values('num_comments_mean', ascending=False).head(11)
colors_comments = [COLOR_BRAND if t == 'Brand' else COLOR_GENERAL for t in top_by_comments['type']]
axes[0, 1].barh(range(len(top_by_comments)), top_by_comments['num_comments_mean'], color=colors_comments)
axes[0, 1].set_yticks(range(len(top_by_comments)))
axes[0, 1].set_yticklabels(top_by_comments.index)
axes[0, 1].set_xlabel('Mean Number of Comments', fontsize=11)
axes[0, 1].set_title('Top Subreddits by Mean Comments', fontsize=12, fontweight='bold')
axes[0, 1].invert_yaxis()
axes[0, 1].grid(axis='x', alpha=0.3)

# Top by posting frequency
top_by_frequency = engagement_metrics.sort_values('posts_per_day', ascending=False).head(11)
colors_freq = [COLOR_BRAND if t == 'Brand' else COLOR_GENERAL for t in top_by_frequency['type']]
axes[1, 0].barh(range(len(top_by_frequency)), top_by_frequency['posts_per_day'], color=colors_freq)
axes[1, 0].set_yticks(range(len(top_by_frequency)))
axes[1, 0].set_yticklabels(top_by_frequency.index)
axes[1, 0].set_xlabel('Posts Per Day', fontsize=11)
axes[1, 0].set_title('Top Subreddits by Posting Frequency', fontsize=12, fontweight='bold')
axes[1, 0].invert_yaxis()
axes[1, 0].grid(axis='x', alpha=0.3)

# Top by total activity
top_by_activity = engagement_metrics.sort_values('total_activity', ascending=False).head(11)
colors_activity = [COLOR_BRAND if t == 'Brand' else COLOR_GENERAL for t in top_by_activity['type']]
axes[1, 1].barh(range(len(top_by_activity)), top_by_activity['total_activity'], color=colors_activity)
axes[1, 1].set_yticks(range(len(top_by_activity)))
axes[1, 1].set_yticklabels(top_by_activity.index)
axes[1, 1].set_xlabel('Total Activity (Posts + Comments)', fontsize=11)
axes[1, 1].set_title('Top Subreddits by Total Activity', fontsize=12, fontweight='bold')
axes[1, 1].invert_yaxis()
axes[1, 1].grid(axis='x', alpha=0.3)

from matplotlib.patches import Patch
legend_elements = [Patch(facecolor=COLOR_BRAND, label='Brand'),
                  Patch(facecolor=COLOR_GENERAL, label='General Audio')]
fig1.legend(handles=legend_elements, loc='upper right', bbox_to_anchor=(0.98, 0.98))

plt.tight_layout()
plt.savefig('engagement_mean_analysis.png', dpi=300, bbox_inches='tight')
plt.close()


# VIZ 2: Median analysis
fig2 = plt.figure(figsize=(18, 12))
gs = fig2.add_gridspec(3, 3, hspace=0.35, wspace=0.3)

# Median score bar chart
ax1 = fig2.add_subplot(gs[0, 0])
top_by_median = engagement_metrics_by_median.head(11)
colors = [COLOR_BRAND if t == 'Brand' else COLOR_GENERAL for t in top_by_median['type']]
ax1.barh(range(len(top_by_median)), top_by_median['score_median'], color=colors, alpha=0.8, edgecolor='black')
ax1.set_yticks(range(len(top_by_median)))
ax1.set_yticklabels(top_by_median.index)
ax1.set_xlabel('Median Score', fontsize=11, fontweight='bold')
ax1.set_title('Subreddits by Median Score', fontsize=12, fontweight='bold')
ax1.invert_yaxis()
ax1.grid(axis='x', alpha=0.3)

# Distribution of median scores
ax2 = fig2.add_subplot(gs[0, 1])
median_scores = engagement_metrics['score_median'].values
ax2.hist(median_scores, bins=20, color=COLOR_ACCENT, alpha=0.7, edgecolor='black')
ax2.axvline(median_scores.mean(), color=COLOR_LINE, linestyle='--', linewidth=2,
           label=f'Mean: {median_scores.mean():.2f}')
ax2.set_xlabel('Median Score Value', fontsize=11)
ax2.set_ylabel('Number of Subreddits', fontsize=11)
ax2.set_title('Distribution of Median Scores', fontsize=12, fontweight='bold')
ax2.legend()
ax2.grid(alpha=0.3)

plt.suptitle('Median Distribution Analysis', fontsize=16, fontweight='bold', y=0.997)
plt.savefig('median_distribution_analysis.png', dpi=300, bbox_inches='tight')
plt.close()


# VIZ 3: Brand vs general audio comparison
fig3, axes = plt.subplots(1, 2, figsize=(14, 6))

type_comparison = engagement_metrics.groupby('type').agg({
   'score_mean': 'mean',
   'num_comments_mean': 'mean',
   'posts_per_day': 'mean'
}).round(2)

metrics = ['Mean\nScore', 'Mean\nComments', 'Posts\nPer Day']
brand_values = [type_comparison.loc['Brand', 'score_mean'],
               type_comparison.loc['Brand', 'num_comments_mean'],
               type_comparison.loc['Brand', 'posts_per_day']]
general_values = [type_comparison.loc['General Audio', 'score_mean'] if 'General Audio' in type_comparison.index else 0,
                 type_comparison.loc['General Audio', 'num_comments_mean'] if 'General Audio' in type_comparison.index else 0,
                 type_comparison.loc['General Audio', 'posts_per_day'] if 'General Audio' in type_comparison.index else 0]

x = np.arange(len(metrics))
width = 0.35

bars1 = axes[0].bar(x - width/2, brand_values, width, label='Brand', color=COLOR_BRAND)
bars2 = axes[0].bar(x + width/2, general_values, width, label='General Audio', color=COLOR_GENERAL)
axes[0].set_ylabel('Value', fontsize=11)
axes[0].set_title('Brand vs General Audio: Engagement', fontsize=12, fontweight='bold')
axes[0].set_xticks(x)
axes[0].set_xticklabels(metrics, fontsize=9)
axes[0].legend()
axes[0].grid(axis='y', alpha=0.3)

for bars in [bars1, bars2]:
   for bar in bars:
       height = bar.get_height()
       axes[0].text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:.1f}', ha='center', va='bottom', fontsize=9)

type_counts = engagement_metrics['type'].value_counts()
axes[1].pie(type_counts.values, labels=type_counts.index, autopct='%1.1f%%',
           colors=[COLOR_BRAND, COLOR_GENERAL], startangle=90)
axes[1].set_title('Community Distribution', fontsize=12, fontweight='bold')

plt.tight_layout()
plt.savefig('brand_vs_general_comparison.png', dpi=300, bbox_inches='tight')
plt.close()


# VIZ 4: User overlap heatmap
if len(overlap_matrix_full) > 0:
   fig4, ax = plt.subplots(figsize=(12, 10))
  
   top_subs = engagement_metrics_by_mean.head(11).index.tolist()
   # Filter for top subreddits that exist in overlap matrix
   top_subs_in_matrix = [s for s in top_subs if s in overlap_matrix_full.index]
   overlap_subset = overlap_matrix_full.loc[top_subs_in_matrix, top_subs_in_matrix]
  
   sns.heatmap(overlap_subset, annot=True, fmt='g', cmap='YlOrRd',
               ax=ax, cbar_kws={'label': 'Shared Users'})
   ax.set_title('User Overlap Between Communities', fontsize=14, fontweight='bold')
   ax.set_xlabel('Subreddit', fontsize=11)
   ax.set_ylabel('Subreddit', fontsize=11)
   plt.xticks(rotation=45, ha='right')
   plt.yticks(rotation=0)
  
   plt.tight_layout()
   plt.savefig('user_overlap_heatmap.png', dpi=300, bbox_inches='tight')
   plt.close()




# Engagement metrics
engagement_metrics.to_csv('engagement_metrics.csv')


# Overlap pairs
overlap_df.to_csv('user_overlap_pairs.csv', index=False)


# Summary statistics
summary_stats = {
   'Metric': [
       'Total Subreddits',
       'Total Posts',
       'Total Comments',
       'Unique Users',
       'Multi-Community Users',
       'Overall Mean Score',
       'Overall Median Score',
       'Overall Mean Comments',
       'Overall Median Comments',
       'Top Community (Mean Score)',
       'Top Community (Median Comments)'
   ],
   'Value': [
       len(engagement_metrics),
       int(engagement_metrics['total_posts'].sum()),
       int(engagement_metrics['num_comments_sum'].sum()),
       user_participation.count(),
       multi_sub_users.count(),
       submissions_stats['mean_score'],
       submissions_stats['median_score'],
       submissions_stats['mean_comments'],
       submissions_stats['median_comments'],
       f"{engagement_metrics_by_mean.index[0]} ({engagement_metrics_by_mean.iloc[0]['score_mean']:.2f})",
       f"{engagement_metrics['num_comments_median'].idxmax()} ({engagement_metrics['num_comments_median'].max():.1f})"
   ]
}

summary_df = pd.DataFrame(summary_stats)
summary_df.to_csv('summary_statistics.csv', index=False)

# Stop Spark session
spark.stop()
