import textwrap
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow.sdk import DAG

NUMBER_OF_RECENT_REVIEWS = 5

# Shorter than 1 minute is not recommended as Airflow is not fast enough
MINUTES_BETWEEN_RUNS = 1

# Change the numbers below with today's date
DATE_TODAY = {"day": 1, "month": 12, "year": 2025}

RETRIES = 1
MINUTES_BETWEEN_RETRIES = 5

# Add the conn_id for testing
CONNECTION_ID = ""

DATABASE = "STEAM_ANALYTICS"
RAW_DATA_SCHEMA = "RAW"
RAW_DATA_TABLE_NAMES = {
    "GAMES": f"{DATABASE}.{RAW_DATA_SCHEMA}.GAMES",
    "GAME_REVIEWS": f"{DATABASE}.{RAW_DATA_SCHEMA}.GAME_REVIEWS"
}
PUBLIC_DATA_SCHEMA = "PUBLIC"
RECENT_REVIEWS_FACT_TABLE_NAME = "RECENT_REVIEWS"
RECENT_REVIEWS_FACT_FULL_TABLE_NAME = f"{DATABASE}.{PUBLIC_DATA_SCHEMA}.{RECENT_REVIEWS_FACT_TABLE_NAME}"

with DAG(
    "FindRecentReviews",
    default_args={
        "depends_on_past": False,
        "retries": RETRIES,
        "retry_delay": timedelta(minutes=MINUTES_BETWEEN_RETRIES),
        "conn_id": CONNECTION_ID
    },
    description=f"Finds the {NUMBER_OF_RECENT_REVIEWS} latest reviews for each game and logs them into both a fact table.",
    schedule=timedelta(minutes=MINUTES_BETWEEN_RUNS),
    start_date=datetime(DATE_TODAY["year"], DATE_TODAY["month"], DATE_TODAY["day"]),
    catchup=False,
    tags=["reviews"],
) as dag:
    insert_recent_reviews_fact_table = SQLExecuteQueryOperator(
        task_id="insert_recent_reviews_fact_table",
        conn_id=CONNECTION_ID,
        sql=f"""
            CREATE OR REPLACE TABLE {RECENT_REVIEWS_FACT_FULL_TABLE_NAME} AS
            SELECT
                appid,
                game_name,
                steamid,
                playtime_forever,
                playtime_at_review,
                voted_up,
                votes_up,
                votes_funny,
                weighted_vote_score,
                review_text,
                timestamp_updated,
                ROW_NUMBER() OVER (PARTITION BY appid ORDER BY timestamp_updated DESC) AS position
            FROM {RAW_DATA_TABLE_NAMES["GAME_REVIEWS"]}
            QUALIFY position <= {NUMBER_OF_RECENT_REVIEWS}
            ORDER BY appid ASC, timestamp_updated DESC;
        """
    )
    insert_recent_reviews_fact_table
