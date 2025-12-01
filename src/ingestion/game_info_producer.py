"""
Game Info Producer - Fetches game metadata from SteamSpy and publishes to Kafka

This module fetches detailed game information (player counts, owners, price, etc.)
from the SteamSpy API and publishes it to the steam.game_info Kafka topic.

Supports:
- Fetching top 100 games from various rankings
- Fetching ALL games (100,000+) via paginated API
- Continuous updates at configurable intervals
"""

import time
import argparse
from datetime import datetime, timezone
from typing import Set

from src.ingestion.steamspy_client import SteamSpyClient
from src.ingestion.kafka_producer import SteamKafkaProducer
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


class GameInfoProducer:
    """Fetches game info from SteamSpy and publishes to Kafka"""

    def __init__(self):
        self.steamspy_client = SteamSpyClient()
        self.kafka_producer = SteamKafkaProducer()
        self.processed_appids: Set[int] = set()
        self.total_published = 0

    def _enrich_game_data(self, game_data: dict) -> dict:
        """Add metadata to game data before publishing"""
        game_data['fetched_at'] = datetime.now(timezone.utc).isoformat()
        game_data['source'] = 'steamspy'
        return game_data

    def fetch_and_publish_top_games(self) -> int:
        """
        Fetch top 100 games from multiple rankings and publish to Kafka

        Returns:
            Number of games published
        """
        logger.info("=" * 60)
        logger.info("Starting Game Info Producer - Fetching Top Games")
        logger.info("=" * 60)

        all_appids = set()
        count = 0

        # Fetch top 100 by players in last 2 weeks
        try:
            top_2weeks = self.steamspy_client.get_top_100_in_2weeks()
            all_appids.update(int(appid) for appid in top_2weeks.keys())
            logger.info(f"Found {len(top_2weeks)} games from top 2 weeks")
        except Exception as e:
            logger.error(f"Failed to fetch top 2 weeks: {e}")

        # Fetch top 100 by all-time players
        try:
            top_forever = self.steamspy_client.get_top_100_forever()
            all_appids.update(int(appid) for appid in top_forever.keys())
            logger.info(f"Found {len(top_forever)} games from top forever")
        except Exception as e:
            logger.error(f"Failed to fetch top forever: {e}")

        # Fetch top 100 by owners
        try:
            top_owned = self.steamspy_client.get_top_100_owned()
            all_appids.update(int(appid) for appid in top_owned.keys())
            logger.info(f"Found {len(top_owned)} games from top owned")
        except Exception as e:
            logger.error(f"Failed to fetch top owned: {e}")

        logger.info(f"Total unique games to fetch: {len(all_appids)}")

        # Fetch detailed info for each game
        for appid in all_appids:
            if appid in self.processed_appids:
                continue

            try:
                game_details = self.steamspy_client.get_app_details(appid)

                if game_details and game_details.get('name'):
                    enriched_data = self._enrich_game_data(game_details)
                    self.kafka_producer.send_game_info_data(
                        enriched_data,
                        key=str(appid)
                    )
                    self.processed_appids.add(appid)
                    count += 1
                    logger.debug(f"Published game: {game_details.get('name')} (appid: {appid})")

            except Exception as e:
                logger.warning(f"Failed to fetch/publish appid {appid}: {e}")

        self.kafka_producer.flush()
        logger.info(f"Published {count} games to Kafka")
        return count

    def fetch_and_publish_by_appids(self, appids: list[int]) -> int:
        """
        Fetch and publish specific games by their app IDs

        Args:
            appids: List of Steam app IDs to fetch

        Returns:
            Number of games published
        """
        logger.info(f"Fetching {len(appids)} specific games")
        count = 0

        for appid in appids:
            try:
                game_details = self.steamspy_client.get_app_details(appid)

                if game_details and game_details.get('name'):
                    enriched_data = self._enrich_game_data(game_details)
                    self.kafka_producer.send_game_info_data(
                        enriched_data,
                        key=str(appid)
                    )
                    count += 1
                    logger.debug(f"Published game: {game_details.get('name')}")

            except Exception as e:
                logger.warning(f"Failed to fetch/publish appid {appid}: {e}")

        self.kafka_producer.flush()
        logger.info(f"Published {count} games to Kafka")
        return count

    def fetch_and_publish_all_games(self, max_games: int = 100000) -> int:
        """
        Fetch ALL games from SteamSpy using paginated 'all' endpoint.

        The 'all' endpoint returns 1000 games per page, sorted by owners.
        Rate limit: 1 request per 60 seconds for this endpoint.

        Args:
            max_games: Maximum number of games to fetch (default 100,000)

        Returns:
            Number of games published
        """
        logger.info("=" * 60)
        logger.info(f"Starting FULL Game Scrape - Target: {max_games:,} games")
        logger.info("Note: Rate limit is 1 page/minute (1000 games/page)")
        logger.info("=" * 60)

        pages_needed = (max_games + 999) // 1000  # Round up
        count = 0
        page = 0

        while count < max_games and page < pages_needed:
            try:
                logger.info(f"Fetching page {page} ({count:,}/{max_games:,} games)...")

                # This has 60-second rate limit built into steamspy_client
                games_page = self.steamspy_client.get_all_games(page=page)

                if not games_page:
                    logger.info(f"No more games at page {page}, stopping")
                    break

                page_count = 0
                for appid_str, game_data in games_page.items():
                    if count >= max_games:
                        break

                    appid = int(appid_str)
                    if appid in self.processed_appids:
                        continue

                    try:
                        # Enrich with metadata
                        game_data['appid'] = appid
                        enriched_data = self._enrich_game_data(game_data)

                        self.kafka_producer.send_game_info_data(
                            enriched_data,
                            key=str(appid)
                        )
                        self.processed_appids.add(appid)
                        count += 1
                        page_count += 1

                    except Exception as e:
                        logger.warning(f"Failed to publish appid {appid}: {e}")

                self.kafka_producer.flush()
                logger.info(f"Page {page}: Published {page_count} games (Total: {count:,})")
                page += 1

            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                logger.info("Waiting 60 seconds before retry...")
                time.sleep(60)

        self.total_published += count
        logger.info("=" * 60)
        logger.info(f"Completed: Published {count:,} games to Kafka")
        logger.info("=" * 60)
        return count

    def run_continuous(self, interval_hours: int = 24, max_games: int = 100000):
        """
        Run continuous data collection - fetches all games then refreshes periodically.

        Args:
            interval_hours: Hours between collection runs (default 24)
            max_games: Maximum games to fetch per run (default 100,000)
        """
        logger.info("=" * 60)
        logger.info(f"CONTINUOUS MODE - {max_games:,} games every {interval_hours} hours")
        logger.info("=" * 60)

        run_count = 0
        while True:
            try:
                run_count += 1
                logger.info(f"\n{'='*60}")
                logger.info(f"Starting run #{run_count}")
                logger.info(f"{'='*60}")

                self.processed_appids.clear()  # Reset for fresh fetch
                count = self.fetch_and_publish_all_games(max_games=max_games)

                logger.info(f"Run #{run_count} complete - published {count:,} games")
                logger.info(f"Total published across all runs: {self.total_published:,}")
                logger.info(f"Next refresh in {interval_hours} hours...")
                time.sleep(interval_hours * 3600)

            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                break
            except Exception as e:
                logger.error(f"Error in continuous run: {e}")
                logger.info("Retrying in 5 minutes...")
                time.sleep(300)

    def close(self):
        """Clean up resources"""
        self.kafka_producer.close()
        logger.info("GameInfoProducer closed")


def main():
    """Main entry point with CLI arguments"""
    parser = argparse.ArgumentParser(
        description='Fetch Steam game info from SteamSpy and publish to Kafka'
    )
    parser.add_argument(
        '--mode', '-m',
        choices=['top', 'all', 'continuous'],
        default='all',
        help='Mode: top (top 100), all (100k games once), continuous (100k + refresh)'
    )
    parser.add_argument(
        '--max-games', '-n',
        type=int,
        default=100000,
        help='Maximum games to fetch (default: 100000)'
    )
    parser.add_argument(
        '--interval', '-i',
        type=int,
        default=24,
        help='Hours between refreshes in continuous mode (default: 24)'
    )

    args = parser.parse_args()

    producer = GameInfoProducer()

    try:
        if args.mode == 'top':
            logger.info("Mode: Fetching top 100 games only")
            producer.fetch_and_publish_top_games()

        elif args.mode == 'all':
            logger.info(f"Mode: Fetching {args.max_games:,} games (single run)")
            producer.fetch_and_publish_all_games(max_games=args.max_games)

        elif args.mode == 'continuous':
            logger.info(f"Mode: Continuous updates every {args.interval} hours")
            producer.run_continuous(
                interval_hours=args.interval,
                max_games=args.max_games
            )

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
