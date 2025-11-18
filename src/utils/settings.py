from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class SteamSettings(BaseSettings):
    api_key: str = Field(..., env='STEAM_API_KEY')
    api_base_url: str = "https://api.steampowered.com"
    rate_limit: int = Field(100, env='STEAM_API_RATE_LIMIT')

    model_config = SettingsConfigDict(env_file='.env', extra='ignore', case_sensitive=True)


class KafkaSettings(BaseSettings):
    bootstrap_servers: str = Field('localhost:9092', env='KAFKA_BOOTSTRAP_SERVERS')
    topic_players: str = Field('steam.players', env='KAFKA_TOPIC_PLAYERS')
    topic_games: str = Field('steam.games', env='KAFKA_TOPIC_GAMES')
    topic_stats: str = Field('steam.stats', env='KAFKA_TOPIC_STATS')

    acks: str = 'all'
    retries: int = 3
    compression_type: str = 'snappy'

    model_config = SettingsConfigDict(env_file='.env', extra='ignore', case_sensitive=True)


class Settings(BaseSettings):
    steam: SteamSettings = SteamSettings()
    kafka: KafkaSettings = KafkaSettings()

    model_config = SettingsConfigDict(env_file='.env',extra='ignore', case_sensitive=True)


settings = Settings()
