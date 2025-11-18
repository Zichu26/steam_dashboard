import requests
from ratelimit import limits, sleep_and_retry
from typing import Optional
from src.utils.settings import settings

from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

class SteamAPIClient:
    """Client for interacting with Steam Web API"""
    
    def __init__(self, api_key: Optional[str] = None):
        print("fuck")
        self.api_key = api_key or settings.steam.api_key
        print(self.api_key)
        self.base_url = settings.steam.api_base_url
        self.session = requests.Session()  # Reuse TCP connections
        
        # Add user agent for politeness
        self.session.headers.update({
            'User-Agent': 'SteamDashboard/1.0'
        })

    @sleep_and_retry  # If rate limit hit, sleep and retry
    @limits(calls=100, period=60)  # Max 100 calls per 60 seconds
    def _make_request(self, endpoint: str, params: dict[str, any]) -> dict:
        """
        Make a rate-limited API request
        """
        # Add API key to every request
        params['key'] = self.api_key
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()  
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise

    def get_player_summaries(self, steam_ids: list[str]) -> dict:
        """
        Get player summaries for given Steam IDs
        
        Args:
            steam_ids: List of Steam IDs (max 100 per request)
        
        Returns:
            Player summaries data
        """
        if len(steam_ids) > 100:
            logger.warning(f"Too many Steam IDs ({len(steam_ids)}), truncating to 100")
            steam_ids = steam_ids[:100]
        
        endpoint = "/ISteamUser/GetPlayerSummaries/v2/"
        params = {
            'steamids': ','.join(steam_ids)  # Join IDs with commas
        }
        
        logger.debug(f"Fetching player summaries for {len(steam_ids)} users")
        return self._make_request(endpoint, params)

    def get_owned_games(self, steam_id: str, include_appinfo: bool = True) -> dict:
        """
        Get games owned by a player
        """
        endpoint = "/IPlayerService/GetOwnedGames/v1/"
        params = {
            'steamid': steam_id,
            'include_appinfo': 1 if include_appinfo else 0,
            'include_played_free_games': 1  # Include free games too
        }
        
        return self._make_request(endpoint, params)

    def get_recently_played_games(self, steam_id: str) -> dict:
        """
        Get recently played games for a player
        """
        endpoint = "/IPlayerService/GetRecentlyPlayedGames/v1/"
        params = {
            'steamid': steam_id,
            'count': 10  
        }
        
        return self._make_request(endpoint, params)