from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType
from pyspark.sql.window import Window
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re


# S3 bucket paths
INPUT_COMMENTS_PATH = 's3a://project-zp134/comments/'
INPUT_SUBMISSIONS_PATH = 's3a://project-zp134/submissions/'
MAX_BRANDS_TO_VISUALIZE = 7

def create_spark_session():
    """Create and configure Spark session with S3 access"""
    spark = SparkSession.builder \
        .appName("Audio_Brand_Sentiment_Analysis") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def detect_brand_udf():
    """UDF for brand detection"""
    def detect_brand(text):
        if text is None or text == '':
            return None
        text_lower = str(text).lower()

        brand_patterns = {
            'Apple': [
                r'\bapple\b', r'\bairpods\b', r'\bairpod\b', r'\bbeats\b',
                r'\bhomepod\b', r'\bpro\s?max\b'
            ],
            'Samsung': [
                r'\bsamsung\b', r'\bgalaxy\s?buds\b', r'\bbuds\s?pro\b',
                r'\bbuds\s?live\b', r'\bbuds\s?\d+\b', r'\bbuds\s?2\b'
            ],
            'Sony': [
                r'\bsony\b', r'\bwh-1000xm\d\b', r'\bwf-1000xm\d\b',
                r'\bxm\d\b', r'\blinkbuds\b', r'\bwh-ch\d+\b'
            ],
            'Bose': [
                r'\bbose\b', r'\bquietcomfort\b', r'\bqc\s?\d+\b',
                r'\bqc\b', r'\bsoundlink\b', r'\bsport\s?earbuds\b'
            ],
            'JBL': [
                r'\bjbl\b', r'\btune\b', r'\bflip\b', r'\bcharge\s?\d+\b',
                r'\bgo\s?\d+\b', r'\bxtreme\b', r'\bpartybox\b'
            ],
            'Sennheiser': [
                r'\bsennheiser\b', r'\bhd\s?\d+\b', r'\bmomentum\b',
                r'\bie\s?\d+\b', r'\bcx\s?\d+\b'
            ],
            'Beats': [
                r'\bbeats\b', r'\bbeats\s?by\s?dre\b', r'\bpowerbeats\b',
                r'\bstudio\s?buds\b'
            ],
        }

        brand_counts = {}
        for brand, patterns in brand_patterns.items():
            count = sum(len(re.findall(pat, text_lower)) for pat in patterns)
            if count > 0:
                brand_counts[brand] = count

        if not brand_counts:
            return None 
        max_count = max(brand_counts.values())
        top = [b for b, c in brand_counts.items() if c == max_count]
        return top[0] if len(top) == 1 else None
    
    return F.udf(detect_brand, StringType())


def preprocess_text_udf():
    """UDF for text preprocessing"""
    def preprocess_text(text):
        if text is None or text == '':
            return ""
        text = str(text).lower()
        text = re.sub(r'http\S+|www\S+', '', text)
        text = re.sub(r'[^a-z\s]', ' ', text)
        text = ' '.join(text.split())
        return text
    
    return F.udf(preprocess_text, StringType())


def analyze_sentiment_udf():
    """UDF for sentiment analysis"""
    def analyze_sentiment(text):
        if text is None or text == '':
            return 'neutral'
        positive_words = ['love', 'great', 'excellent', 'amazing', 'perfect',
                          'fantastic', 'awesome', 'wonderful', 'best', 'happy',
                          'impressed', 'recommend', 'satisfied', 'pleased', 'quality']
        negative_words = ['hate', 'terrible', 'awful', 'worst', 'bad', 'poor',
                          'disappointed', 'frustrating', 'annoying', 'regret',
                          'waste', 'broken', 'useless', 'defective', 'fail']
        t = text.lower()
        pos = sum(1 for w in positive_words if w in t)
        neg = sum(1 for w in negative_words if w in t)
        if pos > neg:
            return 'positive'
        if neg > pos:
            return 'negative'
        return 'neutral'
    
    return F.udf(analyze_sentiment, StringType())


def identify_journey_stage_udf():
    """UDF for customer journey stage identification"""
    def identify_journey_stage(text):
        if text is None or text == '':
            return 'general discussion'
        research = ['recommend', 'suggestion', 'should i', 'advice',
                    'which', 'vs', 'versus', 'compare', 'comparison',
                    'considering', 'thinking about', 'looking for']
        purchase = ['bought', 'purchased', 'ordered', 'got', 'received',
                    'unboxed', 'arrived', 'new']
        usage = ['using', 'owned', 'have had', 'after months',
                 'after years', 'still', 'been using']
        problem = ['problem', 'issue', 'broken', 'stopped working',
                   'not working', 'help', 'fix', 'repair']
        t = text.lower()
        if any(k in t for k in problem):
            return 'troubleshooting'
        if any(k in t for k in research):
            return 'research'
        if any(k in t for k in purchase):
            return 'post-purchase'
        if any(k in t for k in usage):
            return 'long-term use'
        return 'general discussion'
    
    return F.udf(identify_journey_stage, StringType())


def load_and_preprocess_data_dual(spark, comments_path, submissions_path):
    """Load and preprocess data from S3 using PySpark"""
    
    cdf = spark.read.parquet(comments_path)
    
    sdf = spark.read.parquet(submissions_path)
    
    cdf = cdf.withColumn('score', F.col('score').cast(DoubleType()))
    cdf = cdf.withColumn('source', F.lit('comment'))
    cdf = cdf.select('subreddit', F.col('body'), 'score', 'source')
    
    sdf = sdf.withColumn('score', F.col('score').cast(DoubleType()))
    
    sdf = sdf.withColumn('body', 
        F.when(F.col('title').isNotNull() & F.col('selftext').isNotNull(),
               F.concat_ws(' ', F.col('title'), F.col('selftext')))
        .when(F.col('title').isNotNull(), F.col('title'))
        .when(F.col('selftext').isNotNull(), F.col('selftext'))
        .otherwise(F.lit('')))
    
    sdf = sdf.withColumn('source', F.lit('submission'))
    sdf = sdf.select('subreddit', 'body', 'score', 'source')
    
    df = cdf.union(sdf)
    
    audio_subreddits = [
        'headphones', 'audiophile', 'airpods', 'audio', 'bose',
        'sonyheadphones', 'sony', 'jbl', 'earbuds', 'samsung',
        'bluetooth_speakers', 'budgetaudiophile'
    ]
    
    df = df.filter(F.lower(F.col('subreddit')).isin(audio_subreddits))
    
    detect_brand_func = detect_brand_udf()
    df = df.withColumn('brand', detect_brand_func(F.col('body')))
    
    df = df.filter(F.col('brand').isNotNull())
    
    return df


def process_sentiment_and_journey(df):
    """Apply text preprocessing, sentiment analysis, and journey stage identification"""
    
    # Apply preprocessing
    preprocess_func = preprocess_text_udf()
    df = df.withColumn('processed_text', preprocess_func(F.col('body')))
    
    # Filter out short texts
    df = df.filter(F.length(F.col('processed_text')) >= 10)
    
    # Apply sentiment analysis
    sentiment_func = analyze_sentiment_udf()
    df = df.withColumn('sentiment', sentiment_func(F.col('processed_text')))
    
    # Apply journey stage identification
    journey_func = identify_journey_stage_udf()
    df = df.withColumn('journey_stage', journey_func(F.col('processed_text')))
    
    return df


def show_sentiment_distribution(pdf, brands):
    """Display sentiment distribution by brand"""
    sentiment = pdf.groupby(['brand', 'sentiment']).size().unstack(fill_value=0)
    for col in ['negative', 'neutral', 'positive']:
        if col not in sentiment.columns:
            sentiment[col] = 0
    sentiment = sentiment[['negative', 'neutral', 'positive']]
    pct = sentiment.div(sentiment.sum(axis=1), axis=0) * 100

    plt.figure(figsize=(10, 6))
    ax = plt.gca()
    pct.loc[brands].plot(kind='bar', ax=ax, color=['red', 'gray', 'green'])
    plt.title('Sentiment Distribution by Brand')
    plt.xlabel('Brand')
    plt.ylabel('Percentage (%)')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('sentiment_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()


def show_volume_engagement(pdf, brands):
    
    eng = (pdf.groupby('brand').agg({'body': 'count', 'score': 'mean'}).reindex(brands))
    eng = eng.rename(columns={'body': 'Count', 'score': 'Avg Score'})
    x = np.arange(len(eng))
    width = 0.4

    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.bar(x - width/2, eng['Count'], width, label='Volume (Count)', color='tab:blue')
    ax1.set_xlabel('Brand')
    ax1.set_ylabel('Count')
    ax1.set_xticks(x)
    ax1.set_xticklabels(eng.index, rotation=45, ha='right')

    ax2 = ax1.twinx()
    ax2.bar(x + width/2, eng['Avg Score'], width, label='Engagement (Avg Score)', color='tab:orange')
    ax2.set_ylabel('Average Score')

    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc='upper left')

    plt.title('Volume & Engagement by Brand')
    fig.tight_layout()
    plt.savefig('volume_engagement.png', dpi=300, bbox_inches='tight')
    plt.close()



def show_posneg_ratio(pdf, brands, ascending=True):
    
    sentiment = pdf.groupby(['brand', 'sentiment']).size().unstack(fill_value=0)
    for col in ['negative', 'neutral', 'positive']:
        if col not in sentiment.columns:
            sentiment[col] = 0
    ratio = (sentiment['positive'] / (sentiment['negative'] + 1)).reindex(brands)
    ratio = ratio.fillna(0).sort_values(ascending=ascending)

    plt.figure(figsize=(8, 6))
    ax = plt.gca()
    ratio.plot(kind='barh', ax=ax, color='orange')
    plt.axvline(1, linestyle='--', linewidth=1, color='black')
    plt.title('Positive/Negative Sentiment Ratio (Ordered)')
    plt.xlabel('Ratio (higher = more positive)')
    plt.ylabel('Brand')
    plt.tight_layout()
    plt.savefig('posneg_ratio.png', dpi=300, bbox_inches='tight')
    plt.close()


def show_concern_heatmap(pdf, brands):
    """Display concern mentions heatmap"""
    concern_types = ['problem', 'issue', 'broken', 'defect', 'fail', 'return', 'warranty', 'disappointed']
    mat = []
    for b in brands:
        t = ' '.join(pdf[pdf['brand'] == b]['processed_text'])
        mat.append([t.count(c) for c in concern_types])
    dfm = pd.DataFrame(mat, index=brands, columns=concern_types)

    plt.figure(figsize=(10, 6))
    sns.heatmap(dfm, annot=True, fmt='d', cmap='Oranges')
    plt.title('Concern Mentions by Brand')
    plt.xlabel('Concern')
    plt.ylabel('Brand')
    plt.tight_layout()
    plt.savefig('concern_heatmap.png', dpi=300, bbox_inches='tight')
    plt.close()


def show_feature_heatmap(pdf, brands):
   
    feature_types = ['sound', 'bass', 'anc', 'noise cancelling', 'battery', 'comfort', 'app']
    mat = []
    for b in brands:
        t = ' '.join(pdf[pdf['brand'] == b]['processed_text'])
        mat.append([t.count(f) for f in feature_types])
    dfm = pd.DataFrame(mat, index=brands, columns=feature_types)

    plt.figure(figsize=(10, 6))
    sns.heatmap(dfm, annot=True, fmt='d', cmap='GnBu')
    plt.title('Feature Discussions by Brand')
    plt.xlabel('Feature')
    plt.ylabel('Brand')
    plt.tight_layout()
    plt.savefig('feature_heatmap.png', dpi=300, bbox_inches='tight')
    plt.close()


def show_problem_vs_feature(pdf, brands):
    """Display problem vs feature discussion comparison"""
    problem_kw = ['problem', 'issue', 'broken', 'defect', 'fail']
    feature_kw = ['sound', 'bass', 'anc', 'battery', 'comfort']
    rows = []
    for b in brands:
        t = ' '.join(pdf[pdf['brand'] == b]['processed_text'])
        rows.append({'Brand': b,
                     'Problems': sum(t.count(k) for k in problem_kw),
                     'Features': sum(t.count(k) for k in feature_kw)})
    dfm = pd.DataFrame(rows).set_index('Brand').loc[brands]

    plt.figure(figsize=(10, 6))
    ax = plt.gca()
    dfm.plot(kind='bar', ax=ax, color=['red', 'green'])
    plt.title('Problem vs Feature Discussion')
    plt.xlabel('Brand')
    plt.ylabel('Mentions')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('problem_vs_feature.png', dpi=300, bbox_inches='tight')
    plt.close()

def show_avg_length(pdf, brands):
    """Display average text length by brand"""
    avg_len = pdf.groupby('brand')['body'].apply(lambda x: x.str.len().mean()).reindex(brands)

    plt.figure(figsize=(8, 6))
    avg_len.plot(kind='barh', color='tab:brown')
    plt.title('Average Text Length by Brand')
    plt.xlabel('Average Characters')
    plt.ylabel('Brand')
    plt.tight_layout()
    plt.savefig('avg_text_length.png', dpi=300, bbox_inches='tight')
    plt.close()


def save_csv_outputs(audio_pdf, brands):

    audio_pdf.to_csv('full_processed_data.csv', index=False)
    sentiment_dist = audio_pdf.groupby(['brand', 'sentiment']).size().unstack(fill_value=0)
    for col in ['negative', 'neutral', 'positive']:
        if col not in sentiment_dist.columns:
            sentiment_dist[col] = 0
    sentiment_dist = sentiment_dist[['negative', 'neutral', 'positive']]
    sentiment_pct = sentiment_dist.div(sentiment_dist.sum(axis=1), axis=0) * 100
    sentiment_pct.to_csv('sentiment_distribution.csv')
    
    engagement = audio_pdf.groupby('brand').agg({
        'body': 'count',
        'score': ['mean', 'median', 'std', 'min', 'max']
    })
    engagement.columns = ['_'.join(col).strip() for col in engagement.columns.values]
    engagement.rename(columns={'body_count': 'total_posts'}, inplace=True)
    engagement.to_csv('volume_engagement_metrics.csv')
    
    
    concern_types = ['problem', 'issue', 'broken', 'defect', 'fail', 'return', 'warranty', 'disappointed']
    concern_data = []
    for b in brands:
        brand_texts = ' '.join(audio_pdf[audio_pdf['brand'] == b]['processed_text'])
        row = {'brand': b}
        for concern in concern_types:
            row[concern] = brand_texts.count(concern)
        concern_data.append(row)
    concern_df = pd.DataFrame(concern_data)
    concern_df.to_csv('concern_mentions.csv', index=False)
    
    feature_types = ['sound', 'bass', 'anc', 'noise cancelling', 'battery', 'comfort', 'app']
    feature_data = []
    for b in brands:
        brand_texts = ' '.join(audio_pdf[audio_pdf['brand'] == b]['processed_text'])
        row = {'brand': b}
        for feature in feature_types:
            row[feature] = brand_texts.count(feature)
        feature_data.append(row)
    feature_df = pd.DataFrame(feature_data)
    feature_df.to_csv('feature_mentions.csv', index=False)
    
    sentiment = audio_pdf.groupby(['brand', 'sentiment']).size().unstack(fill_value=0)
    for col in ['negative', 'neutral', 'positive']:
        if col not in sentiment.columns:
            sentiment[col] = 0
    ratio_df = pd.DataFrame({
        'brand': sentiment.index,
        'positive_count': sentiment['positive'].values,
        'negative_count': sentiment['negative'].values,
        'neutral_count': sentiment['neutral'].values,
        'pos_neg_ratio': (sentiment['positive'] / (sentiment['negative'] + 1)).values
    })
    ratio_df.to_csv('sentiment_ratios.csv', index=False)
    
    
def main():
    
    spark = create_spark_session()
    
    try:
        audio_df = load_and_preprocess_data_dual(spark, INPUT_COMMENTS_PATH, INPUT_SUBMISSIONS_PATH)
        
        if audio_df.count() == 0:
            return
        
        # Process sentiment and journey stages
        audio_df = process_sentiment_and_journey(audio_df)
        
        # Cache the dataframe for multiple operations
        audio_df.cache()
        

        audio_pdf = audio_df.toPandas()
        
        # Get top brands
        brands = sorted(audio_pdf['brand'].unique())[:MAX_BRANDS_TO_VISUALIZE]
        
        # Generate all visualizations
        show_sentiment_distribution(audio_pdf, brands)
        show_volume_engagement(audio_pdf, brands)
        show_posneg_ratio(audio_pdf, brands, ascending=True)
        show_concern_heatmap(audio_pdf, brands)
        show_feature_heatmap(audio_pdf, brands)
        show_problem_vs_feature(audio_pdf, brands)
        show_avg_length(audio_pdf, brands)
        
       
        
        save_csv_outputs(audio_pdf, brands)
        
            
    finally:
      
        spark.stop()


if __name__ == "__main__":
    main()