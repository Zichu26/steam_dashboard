from kafka import KafkaProducer
import json
from typing import Optional
from src.utils.settings import settings
from kafka.errors import KafkaError

from src.utils.logger import setup_logging, get_logger
setup_logging()
logger = get_logger(__name__)


class SteamKafkaProducer:
    """Kafka producer for Steam data pipeline"""
    
    def __init__(self, bootstrap_servers: Optional[str] = None):
        servers = bootstrap_servers or settings.kafka.bootstrap_servers
        
        self.producer = KafkaProducer(
            # Kafka server addresses
            bootstrap_servers=servers.split(','),
            
            # How to serialize the value (convert Python dict → JSON bytes)
            # {'steamid': '123'} → b'{"steamid":"123"}'
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            
            # How to serialize the key (convert string → bytes)
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            
            # Reliability settings
            acks=settings.kafka.acks,  # 'all' = wait for all replicas
            retries=settings.kafka.retries,  # Retry 3 times on failure
            
            # Performance settings
            compression_type=settings.kafka.compression_type,  # 'snappy'
            linger_ms=10,  # Wait 10ms to batch messages
            batch_size=16384,  # 16KB batches
        )
        
        # Store topic names
        self.topics = {
            'players': settings.kafka.topic_players,
            'games': settings.kafka.topic_games,
            'stats': settings.kafka.topic_stats,
            'game_info': settings.kafka.topic_game_info
        }

    def send_player_data(self, player_data: dict[str, any], key: Optional[str] = None):
        """
        Send player data to Kafka
        
        Args:
            player_data: Dictionary with player info
            key: steam_id
        """
        topic = self.topics['players']
        self._send_message(topic, player_data, key)

    def send_game_data(self, game_data: dict[str, any], key: Optional[str] = None) -> None:
        """
        Send game data to Kafka.
        
        Args:
            game_data: Game data dictionary
            key: Optional message key (app_id recommended)
        """
        topic = self.topics['games']
        self._send_message(topic, game_data, key)

    def send_stats_data(self, stats_data: dict[str, any], key: Optional[str] = None) -> None:
        """
        Send stats data to Kafka.

        Args:
            stats_data: Stats data dictionary
            key: Optional message key (combination of steam_id and app_id)
        """
        topic = self.topics['stats']
        self._send_message(topic, stats_data, key)

    def send_game_info_data(self, game_info: dict[str, any], key: Optional[str] = None) -> None:
        """
        Send game info data to Kafka.

        Args:
            game_info: Game info dictionary from SteamSpy
            key: Optional message key (appid recommended)
        """
        topic = self.topics['game_info']
        self._send_message(topic, game_info, key)

    def _send_message(self, topic: str, data: dict[str, any], key: Optional[str] = None):
        """
        Internal method to send any message
        """
        try:
            # Send message asynchronously
            future = self.producer.send(topic, value=data, key=key)
            
            # Wait for confirmation (synchronous)
            record_metadata = future.get(timeout=10)
            
            logger.debug(
                f"Message sent to {topic} "
                f"[partition: {record_metadata.partition}, "
                f"offset: {record_metadata.offset}]"
            )
            
        except KafkaError as e:
            logger.error(f"Failed to send message to {topic}: {e}")
            raise

    def flush(self):
        """Flush any pending messages"""
        self.producer.flush()
        # Ensures all buffered messages are sent

    def close(self):
        """Close the Kafka producer"""
        self.producer.flush()  # Send remaining messages
        self.producer.close()  # Close connection