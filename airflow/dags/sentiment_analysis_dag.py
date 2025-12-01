"""
Sentiment Analysis DAG - Complex Pipeline

Processes game reviews from Snowflake, applies NLP sentiment analysis,
and generates sentiment summaries and trending insights.

Pipeline:
1. Extract reviews from GAME_REVIEWS table
2. Clean and preprocess text
3. Apply VADER sentiment analysis
4. Calculate game-level sentiment scores
5. Identify trending positive/negative games
6. Store results in GAME_SENTIMENT_ANALYSIS table

Schedule: Daily at 2 AM UTC
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import snowflake.connector
import logging

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'steam_analytics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Snowflake connection settings
SNOWFLAKE_CONFIG = {
    'account': os.environ.get('SNOWFLAKE_ACCOUNT', 'NMB12256'),
    'user': os.environ.get('SNOWFLAKE_USER', 'dog'),
    'database': os.environ.get('SNOWFLAKE_DATABASE', 'STEAM_ANALYTICS'),
    'schema': os.environ.get('SNOWFLAKE_SCHEMA', 'RAW'),
    'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE', 'DOG_WH'),
    'role': os.environ.get('SNOWFLAKE_ROLE', 'TRAINING_ROLE'),
    'private_key_file': os.environ.get('SNOWFLAKE_PRIVATE_KEY_FILE', '/opt/airflow/config/rsa_key.p8'),
}

DATABASE = SNOWFLAKE_CONFIG['database']
SCHEMA = SNOWFLAKE_CONFIG['schema']

# Batch size for processing
BATCH_SIZE = 1000


def get_snowflake_connection():
    """Get Snowflake connection using key-pair authentication"""
    return snowflake.connector.connect(
        account=SNOWFLAKE_CONFIG['account'],
        user=SNOWFLAKE_CONFIG['user'],
        private_key_file=SNOWFLAKE_CONFIG['private_key_file'],
        database=SNOWFLAKE_CONFIG['database'],
        schema=SNOWFLAKE_CONFIG['schema'],
        warehouse=SNOWFLAKE_CONFIG['warehouse'],
        role=SNOWFLAKE_CONFIG['role']
    )


def create_sentiment_tables(**context):
    """
    Create tables for storing sentiment analysis results.
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f"USE DATABASE {DATABASE}")
        cursor.execute(f"USE SCHEMA {SCHEMA}")

        # Table for individual review sentiments
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS REVIEW_SENTIMENTS (
            review_id VARCHAR(255) PRIMARY KEY,
            appid INTEGER,
            game_name VARCHAR(1000),
            sentiment_compound FLOAT,
            sentiment_positive FLOAT,
            sentiment_negative FLOAT,
            sentiment_neutral FLOAT,
            sentiment_label VARCHAR(20),
            review_length INTEGER,
            processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """)
        logger.info("Created REVIEW_SENTIMENTS table")

        # Table for game-level sentiment aggregations
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS GAME_SENTIMENT_ANALYSIS (
            appid INTEGER,
            game_name VARCHAR(1000),
            analysis_date DATE,
            total_reviews_analyzed INTEGER,
            avg_sentiment_compound FLOAT,
            avg_sentiment_positive FLOAT,
            avg_sentiment_negative FLOAT,
            positive_review_pct FLOAT,
            negative_review_pct FLOAT,
            neutral_review_pct FLOAT,
            sentiment_trend VARCHAR(20),
            sentiment_score FLOAT,
            top_positive_keywords VARCHAR(2000),
            top_negative_keywords VARCHAR(2000),
            processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (appid, analysis_date)
        )
        """)
        logger.info("Created GAME_SENTIMENT_ANALYSIS table")

        # Table for trending sentiment
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS SENTIMENT_TRENDS (
            trend_date DATE,
            category VARCHAR(50),
            appid INTEGER,
            game_name VARCHAR(1000),
            sentiment_score FLOAT,
            review_count INTEGER,
            rank_in_category INTEGER,
            processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (trend_date, category, appid)
        )
        """)
        logger.info("Created SENTIMENT_TRENDS table")

        conn.commit()

    finally:
        cursor.close()
        conn.close()

    return "Tables created successfully"


def extract_reviews(**context):
    """
    Extract reviews that haven't been sentiment-analyzed yet.
    Processes in batches to handle large volumes.
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f"USE DATABASE {DATABASE}")
        cursor.execute(f"USE SCHEMA {SCHEMA}")

        # Get reviews not yet analyzed (LEFT JOIN with REVIEW_SENTIMENTS)
        query = """
        SELECT
            gr.review_id,
            gr.appid,
            gr.game_name,
            gr.review_text,
            gr.voted_up,
            gr.playtime_at_review
        FROM GAME_REVIEWS gr
        LEFT JOIN REVIEW_SENTIMENTS rs ON gr.review_id = rs.review_id
        WHERE rs.review_id IS NULL
        AND gr.review_text IS NOT NULL
        AND LENGTH(gr.review_text) > 10
        LIMIT 5000
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        reviews = []
        for row in rows:
            reviews.append({
                'review_id': row[0],
                'appid': row[1],
                'game_name': row[2],
                'review_text': row[3],
                'voted_up': row[4],
                'playtime_at_review': row[5]
            })

        logger.info(f"Extracted {len(reviews)} reviews for sentiment analysis")
        context['ti'].xcom_push(key='reviews', value=reviews)

        return len(reviews)

    finally:
        cursor.close()
        conn.close()


def preprocess_text(text: str) -> str:
    """
    Clean and preprocess review text for sentiment analysis.
    """
    import re

    if not text:
        return ""

    # Convert to lowercase
    text = text.lower()

    # Remove URLs
    text = re.sub(r'http\S+|www\S+', '', text)

    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s.,!?\'"-]', ' ', text)

    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    # Truncate very long reviews
    if len(text) > 5000:
        text = text[:5000]

    return text


def analyze_sentiment_batch(**context):
    """
    Apply VADER sentiment analysis to extracted reviews.
    VADER is good for social media/informal text like game reviews.
    """
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

    ti = context['ti']
    reviews = ti.xcom_pull(key='reviews', task_ids='extract_reviews')

    if not reviews:
        logger.info("No reviews to analyze")
        return 0

    analyzer = SentimentIntensityAnalyzer()
    analyzed_reviews = []

    for review in reviews:
        try:
            # Preprocess text
            clean_text = preprocess_text(review['review_text'])

            if not clean_text:
                continue

            # Get VADER sentiment scores
            scores = analyzer.polarity_scores(clean_text)

            # Determine sentiment label
            compound = scores['compound']
            if compound >= 0.05:
                label = 'POSITIVE'
            elif compound <= -0.05:
                label = 'NEGATIVE'
            else:
                label = 'NEUTRAL'

            analyzed_reviews.append({
                'review_id': review['review_id'],
                'appid': review['appid'],
                'game_name': review['game_name'],
                'sentiment_compound': compound,
                'sentiment_positive': scores['pos'],
                'sentiment_negative': scores['neg'],
                'sentiment_neutral': scores['neu'],
                'sentiment_label': label,
                'review_length': len(clean_text)
            })

        except Exception as e:
            logger.warning(f"Error analyzing review {review['review_id']}: {e}")
            continue

    logger.info(f"Analyzed sentiment for {len(analyzed_reviews)} reviews")
    context['ti'].xcom_push(key='analyzed_reviews', value=analyzed_reviews)

    return len(analyzed_reviews)


def store_review_sentiments(**context):
    """
    Store individual review sentiment scores in Snowflake.
    """
    ti = context['ti']
    analyzed_reviews = ti.xcom_pull(key='analyzed_reviews', task_ids='analyze_sentiment')

    if not analyzed_reviews:
        logger.info("No analyzed reviews to store")
        return 0

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f"USE DATABASE {DATABASE}")
        cursor.execute(f"USE SCHEMA {SCHEMA}")

        insert_sql = """
        INSERT INTO REVIEW_SENTIMENTS
        (review_id, appid, game_name, sentiment_compound, sentiment_positive,
         sentiment_negative, sentiment_neutral, sentiment_label, review_length)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Batch insert
        batch_data = [
            (
                r['review_id'],
                r['appid'],
                r['game_name'],
                r['sentiment_compound'],
                r['sentiment_positive'],
                r['sentiment_negative'],
                r['sentiment_neutral'],
                r['sentiment_label'],
                r['review_length']
            )
            for r in analyzed_reviews
        ]

        cursor.executemany(insert_sql, batch_data)
        conn.commit()

        logger.info(f"Stored {len(batch_data)} review sentiments")
        return len(batch_data)

    finally:
        cursor.close()
        conn.close()


def aggregate_game_sentiments(**context):
    """
    Aggregate sentiment scores at the game level.
    Calculates averages, percentages, and trends.
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f"USE DATABASE {DATABASE}")
        cursor.execute(f"USE SCHEMA {SCHEMA}")

        # Calculate game-level aggregations
        aggregate_sql = """
        MERGE INTO GAME_SENTIMENT_ANALYSIS target
        USING (
            SELECT
                appid,
                game_name,
                CURRENT_DATE() as analysis_date,
                COUNT(*) as total_reviews_analyzed,
                AVG(sentiment_compound) as avg_sentiment_compound,
                AVG(sentiment_positive) as avg_sentiment_positive,
                AVG(sentiment_negative) as avg_sentiment_negative,
                ROUND(SUM(CASE WHEN sentiment_label = 'POSITIVE' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as positive_review_pct,
                ROUND(SUM(CASE WHEN sentiment_label = 'NEGATIVE' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as negative_review_pct,
                ROUND(SUM(CASE WHEN sentiment_label = 'NEUTRAL' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as neutral_review_pct,
                -- Sentiment score: weighted combination
                ROUND(AVG(sentiment_compound) * 50 + 50, 2) as sentiment_score
            FROM REVIEW_SENTIMENTS
            GROUP BY appid, game_name
            HAVING COUNT(*) >= 5  -- Minimum reviews for meaningful analysis
        ) source
        ON target.appid = source.appid AND target.analysis_date = source.analysis_date
        WHEN MATCHED THEN UPDATE SET
            game_name = source.game_name,
            total_reviews_analyzed = source.total_reviews_analyzed,
            avg_sentiment_compound = source.avg_sentiment_compound,
            avg_sentiment_positive = source.avg_sentiment_positive,
            avg_sentiment_negative = source.avg_sentiment_negative,
            positive_review_pct = source.positive_review_pct,
            negative_review_pct = source.negative_review_pct,
            neutral_review_pct = source.neutral_review_pct,
            sentiment_score = source.sentiment_score,
            processed_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            appid, game_name, analysis_date, total_reviews_analyzed,
            avg_sentiment_compound, avg_sentiment_positive, avg_sentiment_negative,
            positive_review_pct, negative_review_pct, neutral_review_pct,
            sentiment_score
        ) VALUES (
            source.appid, source.game_name, source.analysis_date, source.total_reviews_analyzed,
            source.avg_sentiment_compound, source.avg_sentiment_positive, source.avg_sentiment_negative,
            source.positive_review_pct, source.negative_review_pct, source.neutral_review_pct,
            source.sentiment_score
        )
        """

        cursor.execute(aggregate_sql)
        rows_affected = cursor.rowcount
        conn.commit()

        logger.info(f"Aggregated sentiment for {rows_affected} games")
        return rows_affected

    finally:
        cursor.close()
        conn.close()


def calculate_sentiment_trends(**context):
    """
    Calculate trending games by sentiment.
    Identifies top positive, top negative, and most controversial games.
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f"USE DATABASE {DATABASE}")
        cursor.execute(f"USE SCHEMA {SCHEMA}")

        # Clear today's trends first
        cursor.execute("""
        DELETE FROM SENTIMENT_TRENDS WHERE trend_date = CURRENT_DATE()
        """)

        # Top positive sentiment games
        cursor.execute("""
        INSERT INTO SENTIMENT_TRENDS (trend_date, category, appid, game_name, sentiment_score, review_count, rank_in_category)
        SELECT
            CURRENT_DATE(),
            'TOP_POSITIVE',
            appid,
            game_name,
            sentiment_score,
            total_reviews_analyzed,
            ROW_NUMBER() OVER (ORDER BY sentiment_score DESC)
        FROM GAME_SENTIMENT_ANALYSIS
        WHERE analysis_date = CURRENT_DATE()
        AND total_reviews_analyzed >= 10
        ORDER BY sentiment_score DESC
        LIMIT 20
        """)

        # Top negative sentiment games
        cursor.execute("""
        INSERT INTO SENTIMENT_TRENDS (trend_date, category, appid, game_name, sentiment_score, review_count, rank_in_category)
        SELECT
            CURRENT_DATE(),
            'TOP_NEGATIVE',
            appid,
            game_name,
            sentiment_score,
            total_reviews_analyzed,
            ROW_NUMBER() OVER (ORDER BY sentiment_score ASC)
        FROM GAME_SENTIMENT_ANALYSIS
        WHERE analysis_date = CURRENT_DATE()
        AND total_reviews_analyzed >= 10
        ORDER BY sentiment_score ASC
        LIMIT 20
        """)

        # Most controversial (high volume, mixed sentiment ~50%)
        cursor.execute("""
        INSERT INTO SENTIMENT_TRENDS (trend_date, category, appid, game_name, sentiment_score, review_count, rank_in_category)
        SELECT
            CURRENT_DATE(),
            'CONTROVERSIAL',
            appid,
            game_name,
            sentiment_score,
            total_reviews_analyzed,
            ROW_NUMBER() OVER (ORDER BY ABS(positive_review_pct - 50) ASC, total_reviews_analyzed DESC)
        FROM GAME_SENTIMENT_ANALYSIS
        WHERE analysis_date = CURRENT_DATE()
        AND total_reviews_analyzed >= 20
        ORDER BY ABS(positive_review_pct - 50) ASC, total_reviews_analyzed DESC
        LIMIT 20
        """)

        # Most reviewed (high engagement)
        cursor.execute("""
        INSERT INTO SENTIMENT_TRENDS (trend_date, category, appid, game_name, sentiment_score, review_count, rank_in_category)
        SELECT
            CURRENT_DATE(),
            'MOST_REVIEWED',
            appid,
            game_name,
            sentiment_score,
            total_reviews_analyzed,
            ROW_NUMBER() OVER (ORDER BY total_reviews_analyzed DESC)
        FROM GAME_SENTIMENT_ANALYSIS
        WHERE analysis_date = CURRENT_DATE()
        ORDER BY total_reviews_analyzed DESC
        LIMIT 20
        """)

        conn.commit()
        logger.info("Calculated sentiment trends")

        # Log summary
        cursor.execute("""
        SELECT category, COUNT(*) as count
        FROM SENTIMENT_TRENDS
        WHERE trend_date = CURRENT_DATE()
        GROUP BY category
        """)

        for row in cursor.fetchall():
            logger.info(f"  {row[0]}: {row[1]} games")

    finally:
        cursor.close()
        conn.close()


def generate_sentiment_report(**context):
    """
    Generate a summary report of sentiment analysis.
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f"USE DATABASE {DATABASE}")
        cursor.execute(f"USE SCHEMA {SCHEMA}")

        report = {
            'analysis_date': str(datetime.utcnow().date()),
            'summary': {},
            'top_positive': [],
            'top_negative': [],
            'controversial': []
        }

        # Overall summary
        cursor.execute("""
        SELECT
            COUNT(DISTINCT appid) as games_analyzed,
            SUM(total_reviews_analyzed) as total_reviews,
            AVG(sentiment_score) as avg_sentiment_score,
            AVG(positive_review_pct) as avg_positive_pct
        FROM GAME_SENTIMENT_ANALYSIS
        WHERE analysis_date = CURRENT_DATE()
        """)
        row = cursor.fetchone()
        report['summary'] = {
            'games_analyzed': row[0] or 0,
            'total_reviews': row[1] or 0,
            'avg_sentiment_score': round(row[2] or 0, 2),
            'avg_positive_pct': round(row[3] or 0, 2)
        }

        # Top positive games
        cursor.execute("""
        SELECT game_name, sentiment_score, review_count
        FROM SENTIMENT_TRENDS
        WHERE trend_date = CURRENT_DATE() AND category = 'TOP_POSITIVE'
        ORDER BY rank_in_category
        LIMIT 5
        """)
        for row in cursor.fetchall():
            report['top_positive'].append({
                'game': row[0],
                'score': round(row[1], 2),
                'reviews': row[2]
            })

        # Top negative games
        cursor.execute("""
        SELECT game_name, sentiment_score, review_count
        FROM SENTIMENT_TRENDS
        WHERE trend_date = CURRENT_DATE() AND category = 'TOP_NEGATIVE'
        ORDER BY rank_in_category
        LIMIT 5
        """)
        for row in cursor.fetchall():
            report['top_negative'].append({
                'game': row[0],
                'score': round(row[1], 2),
                'reviews': row[2]
            })

        logger.info("=== SENTIMENT ANALYSIS REPORT ===")
        logger.info(f"Date: {report['analysis_date']}")
        logger.info(f"Games Analyzed: {report['summary']['games_analyzed']}")
        logger.info(f"Total Reviews: {report['summary']['total_reviews']}")
        logger.info(f"Avg Sentiment Score: {report['summary']['avg_sentiment_score']}/100")
        logger.info(f"Avg Positive %: {report['summary']['avg_positive_pct']}%")

        if report['top_positive']:
            logger.info("\nTop Positive Games:")
            for g in report['top_positive']:
                logger.info(f"  - {g['game']}: {g['score']}/100 ({g['reviews']} reviews)")

        if report['top_negative']:
            logger.info("\nTop Negative Games:")
            for g in report['top_negative']:
                logger.info(f"  - {g['game']}: {g['score']}/100 ({g['reviews']} reviews)")

        context['ti'].xcom_push(key='sentiment_report', value=report)
        return report

    finally:
        cursor.close()
        conn.close()


# Define the DAG
with DAG(
    dag_id='steam_sentiment_analysis',
    default_args=default_args,
    description='Daily sentiment analysis of Steam game reviews',
    schedule='0 2 * * *',  # Daily at 2 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['steam', 'sentiment', 'nlp', 'analytics'],
) as dag:

    start = EmptyOperator(task_id='start')

    setup_tables = PythonOperator(
        task_id='create_sentiment_tables',
        python_callable=create_sentiment_tables,
    )

    extract = PythonOperator(
        task_id='extract_reviews',
        python_callable=extract_reviews,
    )

    analyze = PythonOperator(
        task_id='analyze_sentiment',
        python_callable=analyze_sentiment_batch,
    )

    store_reviews = PythonOperator(
        task_id='store_review_sentiments',
        python_callable=store_review_sentiments,
    )

    aggregate = PythonOperator(
        task_id='aggregate_game_sentiments',
        python_callable=aggregate_game_sentiments,
    )

    trends = PythonOperator(
        task_id='calculate_trends',
        python_callable=calculate_sentiment_trends,
    )

    report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_sentiment_report,
    )

    end = EmptyOperator(task_id='end')

    # Define task dependencies
    # Linear pipeline with clear data flow
    start >> setup_tables >> extract >> analyze >> store_reviews >> aggregate >> trends >> report >> end
