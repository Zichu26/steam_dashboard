import textwrap
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow.sdk import DAG

NUMBER_OF_POPULAR_GAMES = 100

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
    "GAME_PLAYER_COUNTS": f"{DATABASE}.{RAW_DATA_SCHEMA}.GAME_PLAYER_COUNTS"
}
PUBLIC_DATA_SCHEMA = "PUBLIC"
POPULAR_GAMES_FACT_TABLE_NAME = "POPULAR_GAMES"
POPULAR_GAMES_FACT_FULL_TABLE_NAME = f"{DATABASE}.{PUBLIC_DATA_SCHEMA}.{POPULAR_GAMES_FACT_TABLE_NAME}"
POPULAR_GAMES_HISTORICAL_TABLE_NAME = "POPULAR_GAMES_HISTORICAL"
POPULAR_GAMES_HISTORICAL_FULL_TABLE_NAME = f"{DATABASE}.{PUBLIC_DATA_SCHEMA}.{POPULAR_GAMES_HISTORICAL_TABLE_NAME}"

with DAG(
    "FindPopularGames",
    default_args={
        "depends_on_past": False,
        "retries": RETRIES,
        "retry_delay": timedelta(minutes=MINUTES_BETWEEN_RETRIES),
        "conn_id": CONNECTION_ID
    },
    description=f"Finds the {NUMBER_OF_POPULAR_GAMES} games wth most users currently and logs them into both a fact and historical table.",
    schedule=timedelta(minutes=MINUTES_BETWEEN_RUNS),
    start_date=datetime(DATE_TODAY["year"], DATE_TODAY["month"], DATE_TODAY["day"]),
    catchup=False,
    tags=["games"],
) as dag:
    create_historical_table = SQLExecuteQueryOperator(
        task_id="create_historical_table",
        conn_id=CONNECTION_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {POPULAR_GAMES_HISTORICAL_FULL_TABLE_NAME} (
                fetched_at DATETIME,
                appid INTEGER,
                game_name VARCHAR,
                live_player_count INTEGER,
                ranking INTEGER
            );
        """
    )
    insert_popular_games_historical_table = SQLExecuteQueryOperator(
        task_id="insert_popular_games_historical_table",
        conn_id=CONNECTION_ID,
        sql=f"""
            INSERT INTO {POPULAR_GAMES_HISTORICAL_FULL_TABLE_NAME}
            WITH CURRENT_GAMES AS (
                SELECT * FROM {RAW_DATA_TABLE_NAMES["GAME_PLAYER_COUNTS"]}
                WHERE fetched_at = (SELECT MAX(fetched_at) FROM {RAW_DATA_TABLE_NAMES["GAME_PLAYER_COUNTS"]})
            ),
            POPULAR_GAMES AS (
                SELECT
                    fetched_at,
                    appid,
                    game_name,
                    live_player_count,
                    ROW_NUMBER() OVER (ORDER BY live_player_count DESC) AS ranking
                FROM CURRENT_GAMES
            )
            SELECT * FROM POPULAR_GAMES WHERE ROW_NUMBER <= {NUMBER_OF_POPULAR_GAMES};
        """
    )
    insert_popular_games_fact_table = SQLExecuteQueryOperator(
        task_id="insert_popular_games_fact_table",
        conn_id=CONNECTION_ID,
        sql=f"""
            CREATE OR REPLACE TABLE {POPULAR_GAMES_FACT_FULL_TABLE_NAME} AS
            SELECT * FROM {POPULAR_GAMES_HISTORICAL_FULL_TABLE_NAME}
            WHERE fetched_at = (SELECT MAX(fetched_at) FROM {POPULAR_GAMES_HISTORICAL_FULL_TABLE_NAME})
            ORDER BY ranking;
        """
    )
    create_historical_table >> insert_popular_games_historical_table
    insert_popular_games_historical_table >> insert_popular_games_fact_table
