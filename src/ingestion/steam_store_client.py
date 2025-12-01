"""
Steam Store API Client

Fetches detailed game information from the Steam Store API.
Endpoint: https://store.steampowered.com/api/appdetails?appids=<appid>

Rate limit: 200 requests per 5 minutes (~1 request per 1.5 seconds)
"""

import requests
from ratelimit import limits, sleep_and_retry
from typing import Optional, Dict, Any, List
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


class SteamStoreClient:
    """Client for fetching game details from Steam Store API"""

    BASE_URL = "https://store.steampowered.com/api/appdetails"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SteamDashboard/1.0'
        })

    @sleep_and_retry
    @limits(calls=1, period=1.5)  # 1 request per 1.5 seconds (conservative)
    def _make_request(self, appid: int, params: dict = None) -> dict:
        """Make a rate-limited API request"""
        params = params or {}
        params['appids'] = appid

        try:
            response = self.session.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Steam Store API request failed for appid {appid}: {e}")
            raise

    def get_app_details(self, appid: int, language: str = 'english') -> Optional[Dict[str, Any]]:
        """
        Get detailed information for a game.

        Args:
            appid: Steam app ID
            language: Language for descriptions (default 'english')

        Returns:
            dict with game details or None if not found
        """
        params = {'l': language}

        try:
            data = self._make_request(appid, params)

            # Response format: {"<appid>": {"success": true, "data": {...}}}
            app_data = data.get(str(appid), {})

            if not app_data.get('success'):
                logger.debug(f"No data found for appid {appid}")
                return None

            game_data = app_data.get('data', {})

            # Extract and normalize the data
            return {
                'appid': appid,
                'name': game_data.get('name'),
                'type': game_data.get('type'),  # 'game', 'dlc', 'demo', etc.
                'required_age': game_data.get('required_age', 0),
                'is_free': game_data.get('is_free', False),
                'detailed_description': game_data.get('detailed_description', '')[:50000],
                'short_description': game_data.get('short_description', '')[:5000],
                'about_the_game': game_data.get('about_the_game', '')[:50000],
                'supported_languages': game_data.get('supported_languages', ''),
                'header_image': game_data.get('header_image'),
                'website': game_data.get('website'),
                'developers': game_data.get('developers', []),
                'publishers': game_data.get('publishers', []),
                'categories': [c.get('description') for c in game_data.get('categories', [])],
                'genres': [g.get('description') for g in game_data.get('genres', [])],
                'release_date': game_data.get('release_date', {}).get('date'),
                'coming_soon': game_data.get('release_date', {}).get('coming_soon', False),
                'metacritic_score': game_data.get('metacritic', {}).get('score'),
                'metacritic_url': game_data.get('metacritic', {}).get('url'),
                'recommendations_total': game_data.get('recommendations', {}).get('total'),
                'achievements_total': game_data.get('achievements', {}).get('total'),
                'price_overview': game_data.get('price_overview')
            }

        except Exception as e:
            logger.warning(f"Failed to get details for appid {appid}: {e}")
            return None

    def get_multiple_app_details(self, appids: List[int]) -> List[Dict[str, Any]]:
        """
        Get details for multiple games.

        Args:
            appids: List of app IDs

        Returns:
            List of game detail dictionaries
        """
        results = []

        for i, appid in enumerate(appids):
            try:
                details = self.get_app_details(appid)
                if details:
                    results.append(details)
                    logger.debug(f"[{i+1}/{len(appids)}] Fetched: {details.get('name', appid)}")
            except Exception as e:
                logger.warning(f"Failed to fetch appid {appid}: {e}")

        logger.info(f"Fetched details for {len(results)}/{len(appids)} games")
        return results
