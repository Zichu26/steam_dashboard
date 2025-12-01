import requests
from ratelimit import limits, sleep_and_retry
from typing import Optional, List
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


class SteamSpyClient:
    """Client for interacting with SteamSpy API"""

    BASE_URL = "https://steamspy.com/api.php"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SteamDashboard/1.0'
        })

    @sleep_and_retry
    @limits(calls=1, period=1)  # 1 request per second for most endpoints
    def _make_request(self, params: dict) -> dict:
        """Make a rate-limited API request"""
        try:
            response = self.session.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"SteamSpy API request failed: {e}")
            raise

    def get_app_details(self, appid: int) -> dict:
        """
        Get detailed information for a specific game

        Returns:
            dict with: appid, name, developer, publisher, owners, players_forever,
                      players_2weeks, average_forever, average_2weeks, median_forever,
                      median_2weeks, ccu, price, initialprice, discount, languages,
                      genre, tags, positive, negative
        """
        params = {
            'request': 'appdetails',
            'appid': appid
        }
        logger.debug(f"Fetching details for appid: {appid}")
        return self._make_request(params)

    def get_top_100_in_2weeks(self) -> dict:
        """
        Get top 100 games by players in the last 2 weeks

        Returns:
            dict with appid as keys, game data as values
        """
        params = {'request': 'top100in2weeks'}
        logger.info("Fetching top 100 games by players in last 2 weeks")
        return self._make_request(params)

    def get_top_100_forever(self) -> dict:
        """
        Get top 100 games by all-time player count

        Returns:
            dict with appid as keys, game data as values
        """
        params = {'request': 'top100forever'}
        logger.info("Fetching top 100 games by all-time players")
        return self._make_request(params)

    def get_top_100_owned(self) -> dict:
        """
        Get top 100 games by owner count

        Returns:
            dict with appid as keys, game data as values
        """
        params = {'request': 'top100owned'}
        logger.info("Fetching top 100 games by owners")
        return self._make_request(params)

    @sleep_and_retry
    @limits(calls=1, period=60)  # 1 request per 60 seconds for 'all' endpoint
    def get_all_games(self, page: int = 0) -> dict:
        """
        Get all games sorted by owners (1000 per page)

        Args:
            page: Page number (starts at 0)

        Returns:
            dict with appid as keys, game data as values
        """
        params = {
            'request': 'all',
            'page': page
        }
        logger.info(f"Fetching all games page {page}")
        response = self.session.get(self.BASE_URL, params=params, timeout=60)
        response.raise_for_status()
        return response.json()

    def get_games_by_genre(self, genre: str) -> dict:
        """
        Get games by genre

        Args:
            genre: Genre name (e.g., 'Action', 'RPG', 'Strategy')
        """
        params = {
            'request': 'genre',
            'genre': genre
        }
        logger.info(f"Fetching games for genre: {genre}")
        return self._make_request(params)

    def get_games_by_tag(self, tag: str) -> dict:
        """
        Get games by tag

        Args:
            tag: Tag name (e.g., 'Multiplayer', 'Indie', 'Free to Play')
        """
        params = {
            'request': 'tag',
            'tag': tag
        }
        logger.info(f"Fetching games for tag: {tag}")
        return self._make_request(params)

    def get_detailed_game_info(self, appids: List[int]) -> List[dict]:
        """
        Get detailed info for multiple games

        Args:
            appids: List of app IDs to fetch

        Returns:
            List of game detail dictionaries
        """
        results = []
        for appid in appids:
            try:
                details = self.get_app_details(appid)
                if details and 'name' in details:
                    results.append(details)
                    logger.debug(f"Fetched details for {details.get('name', appid)}")
            except Exception as e:
                logger.warning(f"Failed to fetch details for appid {appid}: {e}")
        return results
