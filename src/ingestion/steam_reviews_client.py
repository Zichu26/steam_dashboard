"""
Steam Reviews API Client

Fetches game reviews from the Steam Store API.
Endpoint: https://store.steampowered.com/appreviews/<appid>?json=1

Rate limit: ~10 requests per second (be conservative)
"""

import requests
from ratelimit import limits, sleep_and_retry
from typing import Optional, List, Dict, Any
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


class SteamReviewsClient:
    """Client for fetching Steam game reviews"""

    BASE_URL = "https://store.steampowered.com/appreviews"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SteamDashboard/1.0'
        })

    @sleep_and_retry
    @limits(calls=5, period=1)  # 5 requests per second (conservative)
    def _make_request(self, appid: int, params: dict) -> dict:
        """Make a rate-limited API request"""
        url = f"{self.BASE_URL}/{appid}"
        params['json'] = 1

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Steam Reviews API request failed for appid {appid}: {e}")
            raise

    def get_review_summary(self, appid: int) -> Optional[Dict[str, Any]]:
        """
        Get review summary (scores, totals) for a game.

        Returns:
            dict with: review_score, review_score_desc, total_positive,
                      total_negative, total_reviews
        """
        params = {
            'filter': 'all',
            'language': 'all',
            'num_per_page': 0,  # Just get summary, no reviews
            'purchase_type': 'all'
        }

        try:
            data = self._make_request(appid, params)
            if data.get('success') and 'query_summary' in data:
                summary = data['query_summary']
                return {
                    'appid': appid,
                    'review_score': summary.get('review_score', 0),
                    'review_score_desc': summary.get('review_score_desc', ''),
                    'total_positive': summary.get('total_positive', 0),
                    'total_negative': summary.get('total_negative', 0),
                    'total_reviews': summary.get('total_reviews', 0)
                }
        except Exception as e:
            logger.warning(f"Failed to get review summary for appid {appid}: {e}")

        return None

    def get_reviews(
        self,
        appid: int,
        num_reviews: int = 100,
        language: str = 'english',
        filter_type: str = 'recent'
    ) -> List[Dict[str, Any]]:
        """
        Get individual reviews for a game.

        Args:
            appid: Steam app ID
            num_reviews: Max number of reviews to fetch (default 100)
            language: Language filter (default 'english')
            filter_type: 'recent', 'updated', or 'all'

        Returns:
            List of review dictionaries
        """
        reviews = []
        cursor = '*'
        per_page = min(100, num_reviews)

        while len(reviews) < num_reviews:
            params = {
                'filter': filter_type,
                'language': language,
                'cursor': cursor,
                'num_per_page': per_page,
                'purchase_type': 'all',
                'review_type': 'all'
            }

            try:
                data = self._make_request(appid, params)

                if not data.get('success'):
                    break

                batch = data.get('reviews', [])
                if not batch:
                    break

                for review in batch:
                    reviews.append({
                        'appid': appid,
                        'recommendationid': review.get('recommendationid'),
                        'steamid': review['author'].get('steamid'),
                        'playtime_forever': review['author'].get('playtime_forever', 0),
                        'playtime_at_review': review['author'].get('playtime_at_review', 0),
                        'voted_up': review.get('voted_up', False),
                        'votes_up': review.get('votes_up', 0),
                        'votes_funny': review.get('votes_funny', 0),
                        'weighted_vote_score': review.get('weighted_vote_score', 0),
                        'review_text': review.get('review', '')[:5000],  # Truncate long reviews
                        'timestamp_created': review.get('timestamp_created'),
                        'timestamp_updated': review.get('timestamp_updated'),
                        'language': review.get('language', language),
                        'comment_count': review.get('comment_count', 0),
                        'steam_purchase': review.get('steam_purchase', False),
                        'received_for_free': review.get('received_for_free', False)
                    })

                    if len(reviews) >= num_reviews:
                        break

                # Get next cursor for pagination
                cursor = data.get('cursor')
                if not cursor or cursor == '*':
                    break

            except Exception as e:
                logger.warning(f"Failed to fetch reviews for appid {appid}: {e}")
                break

        logger.debug(f"Fetched {len(reviews)} reviews for appid {appid}")
        return reviews

    def get_reviews_for_games(
        self,
        appids: List[int],
        reviews_per_game: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get reviews for multiple games.

        Args:
            appids: List of app IDs
            reviews_per_game: Reviews to fetch per game

        Returns:
            List of all reviews
        """
        all_reviews = []

        for i, appid in enumerate(appids):
            try:
                reviews = self.get_reviews(appid, num_reviews=reviews_per_game)
                all_reviews.extend(reviews)
                logger.info(f"[{i+1}/{len(appids)}] Fetched {len(reviews)} reviews for appid {appid}")
            except Exception as e:
                logger.warning(f"Failed to fetch reviews for appid {appid}: {e}")

        return all_reviews
