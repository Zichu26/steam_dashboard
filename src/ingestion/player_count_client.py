"""
Steam Player Count API Client

Fetches real-time current player counts for games.
Endpoint: https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/

This is the OFFICIAL Steam API for live player counts.
Rate limit: ~100 requests per minute (be conservative)
"""

import requests
from ratelimit import limits, sleep_and_retry
from typing import Optional, List, Dict, Any
from src.utils.settings import settings
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


class PlayerCountClient:
    """Client for fetching real-time player counts from Steam API"""

    BASE_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"

    def __init__(self):
        self.api_key = settings.steam.api_key
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SteamDashboard/1.0'
        })

    @sleep_and_retry
    @limits(calls=50, period=60)  # 50 requests per minute (conservative)
    def get_current_players(self, appid: int) -> Optional[int]:
        """
        Get current number of players for a game.

        Args:
            appid: Steam app ID

        Returns:
            Current player count or None if failed
        """
        params = {
            'appid': appid,
            'key': self.api_key
        }

        try:
            response = self.session.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get('response', {}).get('result') == 1:
                return data['response'].get('player_count', 0)
            else:
                logger.debug(f"No player count for appid {appid}")
                return None

        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to get player count for appid {appid}: {e}")
            return None

    def get_player_counts_batch(self, appids: List[int]) -> List[Dict[str, Any]]:
        """
        Get player counts for multiple games.

        Args:
            appids: List of app IDs

        Returns:
            List of dicts with appid and player_count
        """
        results = []

        for appid in appids:
            count = self.get_current_players(appid)
            if count is not None:
                results.append({
                    'appid': appid,
                    'player_count': count
                })

        logger.info(f"Fetched player counts for {len(results)}/{len(appids)} games")
        return results
