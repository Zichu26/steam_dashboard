import time
import signal
from datetime import datetime

class SteamDataProducer:
    """Main service for producing Steam data to Kafka"""
    
    def __init__(self):
        # Initialize clients
        self.steam_client = SteamAPIClient()
        self.kafka_producer = SteamKafkaProducer()
        self.running = False
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)   # Ctrl+C
        signal.signal(signal.SIGTERM, self._signal_handler)  # Kill command
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown gracefully"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def fetch_and_publish_player_data(self, steam_ids: List[str]) -> int:
        """
        Fetch player data from Steam and publish to Kafka
        
        Returns:
            Number of players processed
        """
        try:
            # 1. Fetch from Steam API
            response = self.steam_client.get_player_summaries(steam_ids)
            
            # 2. Validate response structure
            if 'response' not in response or 'players' not in response['response']:
                logger.warning("No player data in response")
                return 0
            
            players = response['response']['players']
            
            # 3. Process each player
            for player in players:
                # Add metadata
                player['fetched_at'] = datetime.utcnow().isoformat()
                player['source'] = 'steam_api'
                
                # Use steam_id as message key
                key = player.get('steamid')
                
                # 4. Send to Kafka
                self.kafka_producer.send_player_data(player, key=key)
                logger.debug(f"Published player data for {key}")
            
            logger.info(f"Successfully published {len(players)} player records")
            return len(players)
            
        except Exception as e:
            logger.error(f"Error fetching/publishing player data: {e}")
            return 0


    def fetch_and_publish_game_data(self, steam_id: str) -> int:
        """
        Fetch owned games for a player
        """
        try:
            response = self.steam_client.get_owned_games(steam_id)
            
            if 'response' not in response or 'games' not in response['response']:
                logger.warning(f"No game data for user {steam_id}")
                return 0
            
            games = response['response']['games']
            
            for game in games:
                # Add metadata
                game['steamid'] = steam_id  # Link to player
                game['fetched_at'] = datetime.utcnow().isoformat()
                game['source'] = 'steam_api'
                
                # Use app_id as key
                key = str(game.get('appid'))
                
                self.kafka_producer.send_game_data(game, key=key)
            
            logger.info(f"Published {len(games)} game records for user {steam_id}")
            return len(games)
            
        except Exception as e:
            logger.error(f"Error fetching/publishing game data: {e}")
            return 0

    def fetch_and_publish_recent_games(self, steam_id: str) -> int:
            """
            Fetch recently played games and publish to Kafka.
            
            Args:
                steam_id: Steam ID of the player
                
            Returns:
                Number of games processed
            """
            try:
                response = self.steam_client.get_recently_played_games(steam_id)
                
                if 'response' not in response or 'games' not in response['response']:
                    logger.warning(f"No recent games for user {steam_id}")
                    return 0
                
                games = response['response']['games']
                
                for game in games:
                    # Add metadata
                    game['steamid'] = steam_id
                    game['fetched_at'] = datetime.utcnow().isoformat()
                    game['source'] = 'steam_api'
                    game['data_type'] = 'recently_played'
                    
                    # Use combination of steamid and appid as key
                    key = f"{steam_id}_{game.get('appid')}"
                    
                    self.kafka_producer.send_stats_data(game, key=key)
                    logger.debug(f"Published recent game data: {key}")
                
                logger.info(f"Published {len(games)} recent games for user {steam_id}")
                return len(games)
                
            except Exception as e:
                logger.error(f"Error fetching/publishing recent games: {e}")
                return 0

    def run_continuous(self, steam_ids: List[str], interval: int = 300):
            """
            Run continuous data collection.
            
            Args:
                steam_ids: List of Steam IDs to monitor
                interval: Time between fetches in seconds (default: 5 minutes)
            """
            self.running = True
            logger.info(f"Starting continuous data collection for {len(steam_ids)} users")
            logger.info(f"Fetch interval: {interval} seconds")
            
            iteration = 0
            
            while self.running:
                try:
                    iteration += 1
                    logger.info(f"--- Iteration {iteration} started ---")
                    start_time = time.time()
                    
                    # Fetch player summaries
                    player_count = self.fetch_and_publish_player_data(steam_ids)
                    
                    # Fetch game data for each player
                    total_games = 0
                    total_recent = 0
                    
                    for steam_id in steam_ids:
                        if not self.running:
                            break
                        
                        # Fetch owned games (less frequently to respect rate limits)
                        if iteration % 10 == 0:  # Every 10 iterations
                            games = self.fetch_and_publish_game_data(steam_id)
                            total_games += games
                        
                        # Fetch recently played games
                        recent = self.fetch_and_publish_recent_games(steam_id)
                        total_recent += recent
                        
                        # Small delay between users to avoid rate limiting
                        time.sleep(1)
                    
                    # Flush producer
                    self.kafka_producer.flush()
                    
                    elapsed = time.time() - start_time
                    logger.info(
                        f"--- Iteration {iteration} completed in {elapsed:.2f}s ---\n"
                        f"Players: {player_count}, Recent games: {total_recent}, "
                        f"Owned games: {total_games}"
                    )
                    
                    # Wait for next iteration
                    if self.running:
                        sleep_time = max(0, interval - elapsed)
                        logger.info(f"Sleeping for {sleep_time:.2f} seconds...")
                        time.sleep(sleep_time)
                    
                except Exception as e:
                    logger.error(f"Error in main loop: {e}", exc_info=True)
                    time.sleep(60)  # Wait before retrying
            
            logger.info("Continuous collection stopped")
    
    def close(self):
        """Close all connections."""
        logger.info("Closing connections...")
        self.kafka_producer.close()
        self.steam_client.close()
        logger.info("All connections closed")


def main():
    """Main entry point."""
    example_steam_ids = [
        '76561197960435530',  # Gabe
    ]
    
    logger.info("=" * 60)
    logger.info("Steam to Kafka Data Pipeline Starting")
    logger.info("=" * 60)
    
    producer = SteamDataProducer()
    
    try:
        # Run continuous collection
        # Adjust interval based on your needs and API rate limits
        producer.run_continuous(example_steam_ids, interval=300)  # 5 minutes
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        producer.close()
        logger.info("Application shutdown complete")


if __name__ == "__main__":
    main()