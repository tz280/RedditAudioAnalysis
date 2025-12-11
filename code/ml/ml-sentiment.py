from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_replace, trim, length, concat, lit, 
    when, udf, pandas_udf, broadcast
)
from pyspark.sql.types import StringType
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression as SparkLogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, IndexToString
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import classification_report, confusion_matrix, precision_recall_fscore_support
import re

def create_spark_session():
    return SparkSession.builder \
        .appName("RedditSentimentAnalysis-MemoryOptimized") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "512m") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .getOrCreate()

def preprocess_text_udf():
    def preprocess(text):
        if text is None or text == '':
            return ""
        text = str(text).lower()
        text = re.sub(r'http\S+|www\S+', '', text)
        text = re.sub(r'[^a-z\s]', ' ', text)
        text = ' '.join(text.split())
        return text
    return udf(preprocess, StringType())

def label_sentiment_udf():
    def label_sentiment(text):
        if text is None or text == '':
            return 'neutral'
        
        text_lower = str(text).lower()
        
        positive_words = ['love', 'great', 'excellent', 'amazing', 'perfect',
                          'fantastic', 'awesome', 'wonderful', 'best', 'good',
                          'recommend', 'satisfied', 'happy', 'quality', 'comfortable']
        
        negative_words = ['hate', 'terrible', 'awful', 'worst', 'bad', 'poor',
                          'disappointed', 'disappointing', 'frustrating', 'annoying',
                          'waste', 'broken', 'useless', 'defective', 'fail', 'sucks','despise','horrible','waste of money',
                          'disgusting']
        
        pos_count = sum(1 for word in positive_words if word in text_lower)
        neg_count = sum(1 for word in negative_words if word in text_lower)
        
        if pos_count > neg_count:
            return 'positive'
        elif neg_count > pos_count:
            return 'negative'
        else:
            return 'neutral'
    return udf(label_sentiment, StringType())

def load_data(spark, sample_fraction=None):
    
    comments = spark.read.parquet('s3a://project-zp134/comments/') \
        .select('body') \
        .filter(col('body').isNotNull())
    
    submissions = spark.read.parquet('s3a://project-zp134/submissions/') \
        .select('title', 'selftext') \
        .filter(col('title').isNotNull())
    
    if sample_fraction and sample_fraction < 1.0:
        print(f"\n⚡ Applying {sample_fraction*100:.0f}% sampling for memory efficiency")
        comments = comments.sample(fraction=sample_fraction, seed=42)
        submissions = submissions.sample(fraction=sample_fraction, seed=42)
    
    # Process comments
    comments = comments.select(
        col('body').alias('text'),
        lit('comment').alias('source')
    )

    # Process submissions
    submissions = submissions.select(
        concat(
            col('title'), 
            lit(' '), 
            col('selftext')
        ).alias('text'),
        lit('submission').alias('source')
    )
    
    # Union and filter
    df = comments.union(submissions)
    df = df.filter(trim(col('text')) != '')
    
    # Repartition for better parallelism
    df = df.repartition(200)
    
    
    return df

def prepare_features(df):
   
    preprocess_udf = preprocess_text_udf()
    sentiment_udf = label_sentiment_udf()
    
    
    df = df.withColumn('processed_text', preprocess_udf(col('text')))
    df = df.filter(length(col('processed_text')) >= 20)
    df = df.withColumn('sentiment', sentiment_udf(col('text')))
    df = df.select('text', 'source', 'processed_text', 'sentiment')
    
    total = df.count()
    sentiment_counts = df.groupBy('sentiment').count().collect()
    
    for row in sentiment_counts:
        sentiment = row['sentiment']
        count = row['count']
        print(f"  {sentiment.capitalize():8s}: {count:6,} ({count/total*100:5.1f}%)")
    
    return df

def train_model_spark(df):

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    train_df = train_df.select('processed_text', 'sentiment')
    train_df.cache()
    train_count = train_df.count() 
    test_df = test_df.select('text', 'processed_text', 'sentiment')
    test_count = test_df.count()
    
    total = train_count + test_count
    
    print(f"\nDATASET SPLIT:")
    print("-"*60)
    print(f"Training set: {train_count:,} samples ({train_count/total*100:.1f}%)")
    print(f"Test set:     {test_count:,} samples ({test_count/total*100:.1f}%)")
    
    print("\nTraining distribution:")
    for row in train_df.groupBy('sentiment').count().collect():
        print(f"  {row['sentiment'].capitalize():8s}: {row['count']:6,} ({row['count']/train_count*100:5.1f}%)")
    
    print("\nTest distribution:")
    for row in test_df.groupBy('sentiment').count().collect():
        print(f"  {row['sentiment'].capitalize():8s}: {row['count']:6,} ({row['count']/test_count*100:5.1f}%)")
    print("="*60 + "\n")
    
    tokenizer = Tokenizer(inputCol="processed_text", outputCol="words")
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=1000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    
    label_indexer = StringIndexer(inputCol="sentiment", outputCol="label")
    lr = SparkLogisticRegression(
        maxIter=100, 
        regParam=0.0, 
        elasticNetParam=0.0,
        standardization=True,
        fitIntercept=True
    )
    label_converter = IndexToString(
        inputCol="prediction", 
        outputCol="predicted_sentiment", 
        labels=label_indexer.fit(train_df).labels
    )
    
    pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, label_indexer, lr, label_converter])
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)
    
    predictions = predictions.select('text', 'sentiment', 'predicted_sentiment')
    
    
    return model, predictions, test_df, train_df

def save_performance_metrics(results, classes, total_count, train_count, test_count):
    metrics_data = []
    for sentiment in classes:
        metrics_data.append({
            'Class': sentiment.capitalize(),
            'Precision': results['precision'][sentiment],
            'Recall': results['recall'][sentiment],
            'F1-Score': results['f1_scores'][sentiment],
            'Support': results['support'][sentiment]
        })
    
    metrics_df = pd.DataFrame(metrics_data)
    
    overall_data = {
        'Metric': ['Overall Accuracy', 'Macro Avg Precision', 'Macro Avg Recall', 'Macro Avg F1-Score',
                   'Weighted Avg Precision', 'Weighted Avg Recall', 'Weighted Avg F1-Score'],
        'Value': [
            results['accuracy'],
            np.mean([results['precision'][c] for c in classes]),
            np.mean([results['recall'][c] for c in classes]),
            np.mean([results['f1_scores'][c] for c in classes]),
            results['weighted_precision'],
            results['weighted_recall'],
            results['weighted_f1']
        ]
    }
    overall_df = pd.DataFrame(overall_data)
    
    dataset_info = {
        'Dataset_Metric': ['Total Samples', 'Training Samples', 'Test Samples', 
                           'Training Percentage', 'Test Percentage'],
        'Value': [
            total_count,
            train_count,
            test_count,
            f"{train_count/total_count*100:.1f}%",
            f"{test_count/total_count*100:.1f}%"
        ]
    }
    dataset_df = pd.DataFrame(dataset_info)
    
    with open('logistic_regression_model_performance.csv', 'w') as f:
        f.write("LOGISTIC REGRESSION MODEL PERFORMANCE REPORT\n")
        f.write("=" * 60 + "\n\n")
        
        f.write("DATASET INFORMATION\n")
        f.write("-" * 60 + "\n")
        dataset_df.to_csv(f, index=False)
        f.write("\n")
        
        f.write("PER-CLASS PERFORMANCE METRICS\n")
        f.write("-" * 60 + "\n")
        metrics_df.to_csv(f, index=False)
        f.write("\n")
        
        f.write("OVERALL PERFORMANCE METRICS\n")
        f.write("-" * 60 + "\n")
        overall_df.to_csv(f, index=False)
        f.write("\n")
    
    return metrics_df, overall_df, dataset_df

def evaluate_and_visualize(predictions, test_df, train_df, total_count, train_count, test_count):
    results_pd = predictions.select('sentiment', 'predicted_sentiment').toPandas()
    
    y_test = results_pd['sentiment']
    y_pred = results_pd['predicted_sentiment']
    
    classes = sorted(y_test.unique())
    
    accuracy = (y_test == y_pred).mean()
    
    print(classification_report(y_test, y_pred, zero_division=0))
    
    cm = confusion_matrix(y_test, y_pred, labels=classes)
    print("\nConfusion Matrix:")
    print(f"{'':12s}", "  ".join(f"{c:>8s}" for c in classes))
    for i, true_class in enumerate(classes):
        print(f"Actual {true_class:8s}", "  ".join(f"{cm[i,j]:8d}" for j in range(len(classes))))
    
    plot_confusion_matrix(y_test, y_pred, classes)
    
    precision, recall, f1, support = precision_recall_fscore_support(
        y_test, y_pred, labels=classes, zero_division=0
    )
    
    weighted_precision, weighted_recall, weighted_f1, _ = precision_recall_fscore_support(
        y_test, y_pred, average='weighted', zero_division=0
    )
    
    results = {
        'accuracy': accuracy,
        'precision': dict(zip(classes, precision)),
        'recall': dict(zip(classes, recall)),
        'f1_scores': dict(zip(classes, f1)),
        'support': dict(zip(classes, support)),
        'weighted_precision': weighted_precision,
        'weighted_recall': weighted_recall,
        'weighted_f1': weighted_f1
    }
    
    plot_performance_metrics(results, classes)
    
    y_train = train_df.select('sentiment').limit(10000).toPandas()['sentiment']
    plot_class_balance(y_train, y_test)
    
    
    save_performance_metrics(results, classes, total_count, train_count, test_count)
    
    
    return results, classes

def plot_confusion_matrix(y_test, y_pred, classes):
    fig, ax = plt.subplots(figsize=(8, 6))
    cm = confusion_matrix(y_test, y_pred, labels=classes)
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', 
                xticklabels=classes, yticklabels=classes, ax=ax,
                cbar_kws={'label': 'Count'})
    ax.set_title('Logistic Regression - Confusion Matrix', 
                fontweight='bold', fontsize=14)
    ax.set_ylabel('True Label', fontweight='bold', fontsize=12)
    ax.set_xlabel('Predicted Label', fontweight='bold', fontsize=12)
    plt.tight_layout()
    plt.savefig('logistic_regression_confusion_matrix.png', 
                dpi=300, bbox_inches='tight')
    plt.close()

def plot_performance_metrics(results, classes):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    ax1 = axes[0]
    metrics_data = {
        'Precision': [results['precision'][c] for c in classes],
        'Recall': [results['recall'][c] for c in classes],
        'F1-Score': [results['f1_scores'][c] for c in classes]
    }
    
    x = np.arange(len(classes))
    width = 0.25
    
    for i, (metric, values) in enumerate(metrics_data.items()):
        offset = (i - 1) * width
        bars = ax1.bar(x + offset, values, width, label=metric, alpha=0.8)
        for bar, val in zip(bars, values):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                    f'{val:.2f}', ha='center', va='bottom', fontsize=9)
    
    ax1.set_ylabel('Score', fontweight='bold', fontsize=12)
    ax1.set_title('Performance Metrics by Sentiment Class', fontweight='bold', fontsize=14)
    ax1.set_xticks(x)
    ax1.set_xticklabels([c.capitalize() for c in classes])
    ax1.legend()
    ax1.set_ylim(0, 1.1)
    ax1.grid(axis='y', alpha=0.3)
    
    ax2 = axes[1]
    ax2.bar(['Logistic Regression'], [results['accuracy']], 
           color='forestgreen', edgecolor='black', linewidth=2, alpha=0.8)
    ax2.set_ylabel('Accuracy', fontweight='bold', fontsize=12)
    ax2.set_title('Overall Model Accuracy', fontweight='bold', fontsize=14)
    ax2.set_ylim(0, 1.0)
    ax2.grid(axis='y', alpha=0.3)
    ax2.text(0, results['accuracy'] + 0.02, f"{results['accuracy']:.1%}",
            ha='center', va='bottom', fontweight='bold', fontsize=14)
    
    plt.tight_layout()
    plt.savefig('logistic_regression_performance.png', 
                dpi=300, bbox_inches='tight')
    plt.close()

def plot_class_balance(y_train, y_test):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    colors_map = {'positive': 'green', 'neutral': 'gray', 'negative': 'red'}
    
    ax1 = axes[0]
    train_counts = y_train.value_counts()
    colors = [colors_map.get(s, 'blue') for s in train_counts.index]
    bars = ax1.bar(train_counts.index, train_counts.values, 
                   color=colors, edgecolor='black', linewidth=2, alpha=0.7)
    ax1.set_ylabel('Count', fontweight='bold', fontsize=12)
    ax1.set_title('Training Set Distribution (Sample)', fontweight='bold', fontsize=14)
    ax1.grid(axis='y', alpha=0.3)
    
    for bar, count in zip(bars, train_counts.values):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + max(train_counts.values)*0.01,
                f'{count:,}\n({count/len(y_train)*100:.1f}%)', 
                ha='center', va='bottom', fontweight='bold', fontsize=10)
    
    ax2 = axes[1]
    test_counts = y_test.value_counts()
    colors = [colors_map.get(s, 'blue') for s in test_counts.index]
    bars = ax2.bar(test_counts.index, test_counts.values, 
                   color=colors, edgecolor='black', linewidth=2, alpha=0.7)
    ax2.set_ylabel('Count', fontweight='bold', fontsize=12)
    ax2.set_title('Test Set Distribution', fontweight='bold', fontsize=14)
    ax2.grid(axis='y', alpha=0.3)
    
    for bar, count in zip(bars, test_counts.values):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + max(test_counts.values)*0.01,
                f'{count:,}\n({count/len(y_test)*100:.1f}%)', 
                ha='center', va='bottom', fontweight='bold', fontsize=10)
    
    plt.tight_layout()
    plt.savefig('logistic_regression_class.png', 
                dpi=300, bbox_inches='tight')
    plt.close()

def main():
    import time
    start_time = time.time()
    
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")  
    
    df = load_data(spark, sample_fraction=None)
    
    df = prepare_features(df)
    
    total_count = df.count()
    
    model, predictions, test_df, train_df = train_model_spark(df)
    
    train_count = train_df.count()
    test_count = test_df.count()
    
    results, classes = evaluate_and_visualize(
        predictions, test_df, train_df, 
        total_count, train_count, test_count
    )
    
    print("FINAL RESULTS SUMMARY")
    print(f"{'Class':<10} {'Precision':<12} {'Recall':<12} {'F1-Score':<12} {'Support':<10}")
    print("-" * 60)
    for sentiment in classes:
        print(f"{sentiment.capitalize():<10} "
              f"{results['precision'][sentiment]:<12.3f} "
              f"{results['recall'][sentiment]:<12.3f} "
              f"{results['f1_scores'][sentiment]:<12.3f} "
              f"{int(results['support'][sentiment]):<10}")
    
    print(f"Overall Accuracy: {results['accuracy']:.1%}")
    
    elapsed_time = time.time() - start_time
    print(f"Total execution time: {elapsed_time/60:.2f} minutes")
    
    train_df.unpersist()
    
    spark.stop()

if __name__ == "__main__":
    main()