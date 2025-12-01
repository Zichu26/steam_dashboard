"""
Real-Time Player Count Producer

Continuously fetches live player counts for top games and writes directly to Snowflake.
This is the MOST REAL-TIME component - updates every few minutes.

Target: Top 1000 games, refreshed every 5 minutes
"""

import argparse
import hashlib
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Set

from src.ingestion.player_count_client import PlayerCountClient
from src.ingestion.steamspy_client import SteamSpyClient
from streaming.snowflake_manager import SnowflakeManager
from streaming.snowflake_settings import streaming_settings
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


class PlayerCountProducer:
    """Real-time producer for live player counts"""

    def __init__(self):
        self.player_count_client = PlayerCountClient()
        self.steamspy_client = SteamSpyClient()
        self.snowflake = SnowflakeManager()
        self.settings = streaming_settings
        self.game_names: Dict[int, str] = {}
        self.top_appids: List[int] = []

    def _generate_record_id(self, appid: int, timestamp: datetime) -> str:
        """Generate unique record ID for time series data"""
        ts_str = timestamp.strftime('%Y%m%d%H%M')
        return hashlib.md5(f"{appid}_{ts_str}".encode()).hexdigest()

    def load_top_games(self, num_games: int = 1000) -> List[int]:
        """
        Load top game app IDs and their names from SteamSpy.
        Caches results to avoid repeated API calls.
        """
        if self.top_appids and len(self.top_appids) >= num_games:
            return self.top_appids[:num_games]

        logger.info(f"Loading top {num_games} games from SteamSpy...")
        appids = set()

        # Get top 100 lists (fast, no rate limit issues)
        try:
            top_2weeks = self.steamspy_client.get_top_100_in_2weeks()
            for appid_str, data in top_2weeks.items():
                appid = int(appid_str)
                appids.add(appid)
                self.game_names[appid] = data.get('name', f'Game_{appid}')
        except Exception as e:
            logger.warning(f"Failed to fetch top 2 weeks: {e}")

        try:
            top_forever = self.steamspy_client.get_top_100_forever()
            for appid_str, data in top_forever.items():
                appid = int(appid_str)
                appids.add(appid)
                self.game_names[appid] = data.get('name', f'Game_{appid}')
        except Exception as e:
            logger.warning(f"Failed to fetch top forever: {e}")

        try:
            top_owned = self.steamspy_client.get_top_100_owned()
            for appid_str, data in top_owned.items():
                appid = int(appid_str)
                appids.add(appid)
                self.game_names[appid] = data.get('name', f'Game_{appid}')
        except Exception as e:
            logger.warning(f"Failed to fetch top owned: {e}")

        # If we need more, fetch from 'all' endpoint
        if len(appids) < num_games:
            pages_needed = (num_games - len(appids)) // 1000 + 1
            for page in range(min(pages_needed, 3)):
                try:
                    all_games = self.steamspy_client.get_all_games(page=page)
                    for appid_str, data in all_games.items():
                        appid = int(appid_str)
                        appids.add(appid)
                        self.game_names[appid] = data.get('name', f'Game_{appid}')
                    if len(appids) >= num_games:
                        break
                except Exception as e:
                    logger.warning(f"Failed to fetch all games page {page}: {e}")
                    break

        self.top_appids = list(appids)[:num_games]
        logger.info(f"Loaded {len(self.top_appids)} top games")
        return self.top_appids

    def fetch_and_store_counts(self, appids: List[int]) -> int:
        """
        Fetch live player counts and store in Snowflake.

        Args:
            appids: List of app IDs to fetch

        Returns:
            Number of records stored
        """
        logger.info(f"Fetching live player counts for {len(appids)} games...")

        conn = self.snowflake.connect()
        cursor = conn.cursor()
        stored = 0
        fetched_at = datetime.now(timezone.utc)

        try:
            cursor.execute(f"USE DATABASE {self.settings.snowflake.snowflake_database}")
            cursor.execute(f"USE SCHEMA {self.settings.snowflake.snowflake_schema}")

            insert_sql = f"""
            INSERT INTO {self.settings.snowflake.table_player_counts}
            (record_id, appid, game_name, live_player_count, fetched_at, source)
            VALUES (%s, %s, %s, %s, %s, %s)
            """

            for appid in appids:
                try:
                    count = self.player_count_client.get_current_players(appid)

                    if count is not None:
                        record_id = self._generate_record_id(appid, fetched_at)
                        game_name = self.game_names.get(appid, f'Game_{appid}')

                        cursor.execute(insert_sql, (
                            record_id,
                            appid,
                            game_name,
                            count,
                            fetched_at,
                            'steam_api'
                        ))
                        stored += 1

                except Exception as e:
                    logger.debug(f"Failed to fetch/store count for appid {appid}: {e}")

            conn.commit()
            logger.info(f"Stored {stored} player count records")

        finally:
            cursor.close()

        return stored

    def run_continuous(self, num_games: int = 1000, interval_minutes: int = 5):
        """
        Run continuous player count collection.

        Args:
            num_games: Number of top games to track
            interval_minutes: Minutes between refresh cycles
        """
        logger.info("=" * 60)
        logger.info(f"REAL-TIME PLAYER COUNT PRODUCER")
        logger.info(f"Tracking {num_games} games, refreshing every {interval_minutes} min")
        logger.info("=" * 60)

        # Load top games once
        appids = self.load_top_games(num_games)

        cycle = 0
        total_stored = 0

        while True:
            try:
                cycle += 1
                start_time = time.time()

                logger.info(f"\n{'='*60}")
                logger.info(f"Cycle #{cycle} - Fetching live player counts")
                logger.info(f"{'='*60}")

                stored = self.fetch_and_store_counts(appids)
                total_stored += stored

                elapsed = time.time() - start_time
                logger.info(f"Cycle #{cycle} complete: {stored} records in {elapsed:.1f}s")
                logger.info(f"Total records stored: {total_stored:,}")

                # Sleep until next cycle
                sleep_time = max(0, interval_minutes * 60 - elapsed)
                logger.info(f"Next refresh in {sleep_time:.0f} seconds...")
                time.sleep(sleep_time)

            except KeyboardInterrupt:
                logger.info("Interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error in cycle #{cycle}: {e}")
                logger.info("Retrying in 60 seconds...")
                time.sleep(60)

    def run_once(self, num_games: int = 1000) -> int:
        """Run a single collection cycle"""
        appids = self.load_top_games(num_games)
        return self.fetch_and_store_counts(appids)

    def close(self):
        """Clean up resources"""
        self.snowflake.close()
        logger.info("PlayerCountProducer closed")


def main():
    parser = argparse.ArgumentParser(
        description='Real-time player count producer'
    )
    parser.add_argument(
        '--num-games', '-n',
        type=int,
        default=1000,
        help='Number of top games to track (default: 1000)'
    )
    parser.add_argument(
        '--interval', '-i',
        type=int,
        default=5,
        help='Minutes between refresh cycles (default: 5)'
    )
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run once instead of continuously'
    )

    args = parser.parse_args()

    producer = PlayerCountProducer()

    try:
        if args.once:
            count = producer.run_once(num_games=args.num_games)
            logger.info(f"Single run complete: {count} records stored")
        else:
            producer.run_continuous(
                num_games=args.num_games,
                interval_minutes=args.interval
            )
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
