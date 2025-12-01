"""
Game Details Batch Producer

Fetches detailed game information (descriptions, genres, etc.) from Steam Store API
and loads to Snowflake. This is a BATCH job for the recommendation engine.

Target: Top 5000 games with full details
"""

import argparse
import json
from datetime import datetime, timezone
from typing import List, Dict, Any

from src.ingestion.steam_store_client import SteamStoreClient
from src.ingestion.steamspy_client import SteamSpyClient
from streaming.snowflake_manager import SnowflakeManager
from streaming.snowflake_settings import streaming_settings
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


class GameDetailsBatchProducer:
    """Batch producer for game details"""

    def __init__(self):
        self.store_client = SteamStoreClient()
        self.steamspy_client = SteamSpyClient()
        self.snowflake = SnowflakeManager()
        self.settings = streaming_settings

    def get_top_game_appids(self, num_games: int = 5000) -> List[int]:
        """Get top game app IDs from SteamSpy"""
        appids = set()

        # Get top 100 lists
        for fetch_func in [
            self.steamspy_client.get_top_100_in_2weeks,
            self.steamspy_client.get_top_100_forever,
            self.steamspy_client.get_top_100_owned
        ]:
            try:
                data = fetch_func()
                appids.update(int(k) for k in data.keys())
            except Exception as e:
                logger.warning(f"Failed to fetch top list: {e}")

        # Get more from 'all' endpoint if needed
        page = 0
        while len(appids) < num_games and page < 10:
            try:
                all_games = self.steamspy_client.get_all_games(page=page)
                appids.update(int(k) for k in all_games.keys())
                page += 1
                logger.info(f"Fetched page {page}, total appids: {len(appids)}")
            except Exception as e:
                logger.error(f"Failed to fetch page {page}: {e}")
                break

        result = list(appids)[:num_games]
        logger.info(f"Got {len(result)} app IDs to process")
        return result

    def fetch_and_load_details(self, appids: List[int]) -> int:
        """
        Fetch game details and load to Snowflake.

        Args:
            appids: List of game app IDs

        Returns:
            Total games loaded
        """
        logger.info("=" * 60)
        logger.info(f"Fetching details for {len(appids)} games")
        logger.info("=" * 60)

        conn = self.snowflake.connect()
        cursor = conn.cursor()
        total_loaded = 0

        try:
            cursor.execute(f"USE DATABASE {self.settings.snowflake.snowflake_database}")
            cursor.execute(f"USE SCHEMA {self.settings.snowflake.snowflake_schema}")

            insert_sql = f"""
            MERGE INTO {self.settings.snowflake.table_game_details} AS target
            USING (SELECT
                %s AS appid, %s AS name, %s AS type, %s AS required_age,
                %s AS is_free, %s AS detailed_description, %s AS short_description,
                %s AS about_the_game, %s AS supported_languages, %s AS header_image,
                %s AS website, PARSE_JSON(%s) AS developers, PARSE_JSON(%s) AS publishers,
                PARSE_JSON(%s) AS categories, PARSE_JSON(%s) AS genres,
                %s AS release_date, %s AS coming_soon, %s AS metacritic_score,
                %s AS metacritic_url, %s AS recommendations_total, %s AS achievements_total,
                PARSE_JSON(%s) AS price_overview, %s AS fetched_at, %s AS source
            ) AS source
            ON target.appid = source.appid
            WHEN MATCHED THEN UPDATE SET
                name = source.name,
                detailed_description = source.detailed_description,
                short_description = source.short_description,
                about_the_game = source.about_the_game,
                metacritic_score = source.metacritic_score,
                recommendations_total = source.recommendations_total,
                price_overview = source.price_overview,
                fetched_at = source.fetched_at,
                updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                appid, name, type, required_age, is_free, detailed_description,
                short_description, about_the_game, supported_languages, header_image,
                website, developers, publishers, categories, genres, release_date,
                coming_soon, metacritic_score, metacritic_url, recommendations_total,
                achievements_total, price_overview, fetched_at, source
            ) VALUES (
                source.appid, source.name, source.type, source.required_age,
                source.is_free, source.detailed_description, source.short_description,
                source.about_the_game, source.supported_languages, source.header_image,
                source.website, source.developers, source.publishers, source.categories,
                source.genres, source.release_date, source.coming_soon,
                source.metacritic_score, source.metacritic_url, source.recommendations_total,
                source.achievements_total, source.price_overview, source.fetched_at,
                source.source
            )
            """

            for i, appid in enumerate(appids):
                try:
                    details = self.store_client.get_app_details(appid)

                    if not details:
                        continue

                    fetched_at = datetime.now(timezone.utc)

                    record = (
                        details['appid'],
                        details['name'],
                        details['type'],
                        details['required_age'],
                        details['is_free'],
                        details['detailed_description'],
                        details['short_description'],
                        details['about_the_game'],
                        details['supported_languages'],
                        details['header_image'],
                        details['website'],
                        json.dumps(details['developers']),
                        json.dumps(details['publishers']),
                        json.dumps(details['categories']),
                        json.dumps(details['genres']),
                        details['release_date'],
                        details['coming_soon'],
                        details['metacritic_score'],
                        details['metacritic_url'],
                        details['recommendations_total'],
                        details['achievements_total'],
                        json.dumps(details['price_overview']) if details['price_overview'] else None,
                        fetched_at,
                        'steam_store_api'
                    )

                    cursor.execute(insert_sql, record)
                    total_loaded += 1

                    if (i + 1) % 100 == 0:
                        conn.commit()
                        logger.info(f"[{i+1}/{len(appids)}] Loaded {total_loaded} games")

                except Exception as e:
                    logger.warning(f"Failed to process appid {appid}: {e}")
                    continue

            conn.commit()

        finally:
            cursor.close()

        logger.info("=" * 60)
        logger.info(f"Total games loaded: {total_loaded}")
        logger.info("=" * 60)
        return total_loaded

    def run(self, num_games: int = 5000):
        """Main entry point for batch job"""
        logger.info("Starting Game Details Batch Producer")

        # Get top game IDs
        appids = self.get_top_game_appids(num_games)

        # Fetch and load details
        total = self.fetch_and_load_details(appids)

        logger.info(f"Batch job complete. Loaded {total} games.")
        return total

    def close(self):
        """Clean up resources"""
        self.snowflake.close()


def main():
    parser = argparse.ArgumentParser(description='Batch fetch game details')
    parser.add_argument('--num-games', '-n', type=int, default=5000,
                        help='Number of top games to fetch details for')

    args = parser.parse_args()

    producer = GameDetailsBatchProducer()

    try:
        producer.run(num_games=args.num_games)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
