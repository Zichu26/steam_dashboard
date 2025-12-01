"""
Reviews Batch Producer

Fetches game reviews for top games and loads directly to Snowflake.
This is a BATCH job designed to run periodically (e.g., daily via Airflow).

Target: 100 most recent reviews from top 2000 games
"""

import argparse
import hashlib
from datetime import datetime, timezone
from typing import List, Dict, Any

from src.ingestion.steam_reviews_client import SteamReviewsClient
from src.ingestion.steamspy_client import SteamSpyClient
from streaming.snowflake_manager import SnowflakeManager
from streaming.snowflake_settings import streaming_settings
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


class ReviewsBatchProducer:
    """Batch producer for game reviews"""

    def __init__(self):
        self.reviews_client = SteamReviewsClient()
        self.steamspy_client = SteamSpyClient()
        self.snowflake = SnowflakeManager()
        self.settings = streaming_settings

    def get_top_game_appids(self, num_games: int = 2000) -> List[int]:
        """
        Get top game app IDs from SteamSpy.

        Combines top 100 lists and 'all' endpoint to get diverse top games.
        """
        appids = set()

        # Get top 100 by recent players
        try:
            top_2weeks = self.steamspy_client.get_top_100_in_2weeks()
            appids.update(int(k) for k in top_2weeks.keys())
            logger.info(f"Added {len(top_2weeks)} games from top 2 weeks")
        except Exception as e:
            logger.error(f"Failed to fetch top 2 weeks: {e}")

        # Get top 100 by all-time players
        try:
            top_forever = self.steamspy_client.get_top_100_forever()
            appids.update(int(k) for k in top_forever.keys())
            logger.info(f"Added {len(top_forever)} games from top forever")
        except Exception as e:
            logger.error(f"Failed to fetch top forever: {e}")

        # Get top 100 by owners
        try:
            top_owned = self.steamspy_client.get_top_100_owned()
            appids.update(int(k) for k in top_owned.keys())
            logger.info(f"Added {len(top_owned)} games from top owned")
        except Exception as e:
            logger.error(f"Failed to fetch top owned: {e}")

        # If we need more, fetch from 'all' endpoint (paginated)
        if len(appids) < num_games:
            pages_needed = (num_games - len(appids)) // 1000 + 1
            for page in range(min(pages_needed, 5)):  # Max 5 pages
                try:
                    all_games = self.steamspy_client.get_all_games(page=page)
                    appids.update(int(k) for k in all_games.keys())
                    logger.info(f"Added games from all page {page}, total: {len(appids)}")
                    if len(appids) >= num_games:
                        break
                except Exception as e:
                    logger.error(f"Failed to fetch all games page {page}: {e}")
                    break

        result = list(appids)[:num_games]
        logger.info(f"Total unique games to process: {len(result)}")
        return result

    def _generate_review_id(self, appid: int, recommendationid: str) -> str:
        """Generate unique review ID"""
        return hashlib.md5(f"{appid}_{recommendationid}".encode()).hexdigest()

    def fetch_and_load_reviews(
        self,
        appids: List[int],
        reviews_per_game: int = 100
    ) -> int:
        """
        Fetch reviews for games and load to Snowflake.

        Args:
            appids: List of game app IDs
            reviews_per_game: Number of reviews per game

        Returns:
            Total reviews loaded
        """
        logger.info("=" * 60)
        logger.info(f"Fetching reviews for {len(appids)} games ({reviews_per_game} reviews each)")
        logger.info("=" * 60)

        # Get game names from SteamSpy (for denormalization)
        game_names = {}
        try:
            for appid in appids[:100]:  # Cache names for top games
                details = self.steamspy_client.get_app_details(appid)
                if details and details.get('name'):
                    game_names[appid] = details['name']
        except Exception as e:
            logger.warning(f"Failed to fetch some game names: {e}")

        conn = self.snowflake.connect()
        cursor = conn.cursor()
        total_loaded = 0

        try:
            cursor.execute(f"USE DATABASE {self.settings.snowflake.snowflake_database}")
            cursor.execute(f"USE SCHEMA {self.settings.snowflake.snowflake_schema}")

            insert_sql = f"""
            MERGE INTO {self.settings.snowflake.table_game_reviews} AS target
            USING (SELECT
                %s AS review_id, %s AS appid, %s AS game_name, %s AS recommendationid,
                %s AS steamid, %s AS playtime_forever, %s AS playtime_at_review,
                %s AS voted_up, %s AS votes_up, %s AS votes_funny, %s AS weighted_vote_score,
                %s AS review_text, %s AS timestamp_created, %s AS timestamp_updated,
                %s AS language, %s AS comment_count, %s AS steam_purchase,
                %s AS received_for_free, %s AS fetched_at, %s AS source
            ) AS source
            ON target.review_id = source.review_id
            WHEN MATCHED THEN UPDATE SET
                votes_up = source.votes_up,
                votes_funny = source.votes_funny,
                weighted_vote_score = source.weighted_vote_score,
                timestamp_updated = source.timestamp_updated,
                comment_count = source.comment_count,
                fetched_at = source.fetched_at,
                updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                review_id, appid, game_name, recommendationid, steamid,
                playtime_forever, playtime_at_review, voted_up, votes_up,
                votes_funny, weighted_vote_score, review_text, timestamp_created,
                timestamp_updated, language, comment_count, steam_purchase,
                received_for_free, fetched_at, source
            ) VALUES (
                source.review_id, source.appid, source.game_name, source.recommendationid,
                source.steamid, source.playtime_forever, source.playtime_at_review,
                source.voted_up, source.votes_up, source.votes_funny,
                source.weighted_vote_score, source.review_text, source.timestamp_created,
                source.timestamp_updated, source.language, source.comment_count,
                source.steam_purchase, source.received_for_free, source.fetched_at,
                source.source
            )
            """

            for i, appid in enumerate(appids):
                try:
                    reviews = self.reviews_client.get_reviews(
                        appid,
                        num_reviews=reviews_per_game,
                        language='english',
                        filter_type='recent'
                    )

                    game_name = game_names.get(appid, f"Game_{appid}")
                    fetched_at = datetime.now(timezone.utc)

                    for review in reviews:
                        review_id = self._generate_review_id(appid, review['recommendationid'])

                        record = (
                            review_id,
                            appid,
                            game_name,
                            review['recommendationid'],
                            review['steamid'],
                            review['playtime_forever'],
                            review['playtime_at_review'],
                            review['voted_up'],
                            review['votes_up'],
                            review['votes_funny'],
                            review['weighted_vote_score'],
                            review['review_text'],
                            datetime.fromtimestamp(review['timestamp_created']) if review['timestamp_created'] else None,
                            datetime.fromtimestamp(review['timestamp_updated']) if review['timestamp_updated'] else None,
                            review['language'],
                            review['comment_count'],
                            review['steam_purchase'],
                            review['received_for_free'],
                            fetched_at,
                            'steam_reviews_api'
                        )

                        cursor.execute(insert_sql, record)
                        total_loaded += 1

                    conn.commit()
                    logger.info(f"[{i+1}/{len(appids)}] Loaded {len(reviews)} reviews for appid {appid}")

                except Exception as e:
                    logger.warning(f"Failed to process appid {appid}: {e}")
                    continue

        finally:
            cursor.close()

        logger.info("=" * 60)
        logger.info(f"Total reviews loaded: {total_loaded}")
        logger.info("=" * 60)
        return total_loaded

    def run(self, num_games: int = 2000, reviews_per_game: int = 100):
        """Main entry point for batch job"""
        logger.info("Starting Reviews Batch Producer")

        # Get top game IDs
        appids = self.get_top_game_appids(num_games)

        # Fetch and load reviews
        total = self.fetch_and_load_reviews(appids, reviews_per_game)

        logger.info(f"Batch job complete. Loaded {total} reviews.")
        return total

    def close(self):
        """Clean up resources"""
        self.snowflake.close()


def main():
    parser = argparse.ArgumentParser(description='Batch fetch game reviews')
    parser.add_argument('--num-games', '-n', type=int, default=2000,
                        help='Number of top games to fetch reviews for')
    parser.add_argument('--reviews-per-game', '-r', type=int, default=100,
                        help='Number of reviews per game')

    args = parser.parse_args()

    producer = ReviewsBatchProducer()

    try:
        producer.run(
            num_games=args.num_games,
            reviews_per_game=args.reviews_per_game
        )
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
