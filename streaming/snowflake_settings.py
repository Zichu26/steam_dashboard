from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class SnowflakeSettings(BaseSettings):
    """Snowflake connection configuration"""
    
    # Account details
    snowflake_account: str = Field(..., env='snowflake_account')
    snowflake_user: str = Field(..., env='snowflake_user')
    private_key_file: str = Field(..., env='private_key_file')
    
    # Database details
    snowflake_database: str = Field('STEAM_ANALYTICS', env='SNOWFLAKE_DATABASE')
    snowflake_schema: str = Field('RAW', env='SNOWFLAKE_SCHEMA')
    snowflake_warehouse: str = Field('COMPUTE_WH', env='SNOWFLAKE_WAREHOUSE')
    snowflake_role: str = Field('ACCOUNTADMIN', env='SNOWFLAKE_ROLE')
    
    # Table names
    table_players: str = Field('PLAYERS', env='SNOWFLAKE_TABLE_PLAYERS')
    table_games: str = Field('GAMES', env='SNOWFLAKE_TABLE_GAMES')
    table_stats: str = Field('PLAYER_STATS', env='SNOWFLAKE_TABLE_STATS')
    table_game_info: str = Field('GAME_INFO', env='SNOWFLAKE_TABLE_GAME_INFO')
    table_game_reviews: str = Field('GAME_REVIEWS', env='SNOWFLAKE_TABLE_GAME_REVIEWS')
    table_game_details: str = Field('GAME_DETAILS', env='SNOWFLAKE_TABLE_GAME_DETAILS')
    table_player_counts: str = Field('GAME_PLAYER_COUNTS', env='SNOWFLAKE_TABLE_PLAYER_COUNTS')
    
    model_config = SettingsConfigDict(env_file='.env', extra='ignore', case_sensitive=True)


class SparkSettings(BaseSettings):
    """Spark Streaming configuration"""
    
    app_name: str = Field('SteamKafkaToSnowflake', env='SPARK_APP_NAME')
    master: str = Field('local[*]', env='SPARK_MASTER')
    
    # Kafka settings
    kafka_bootstrap_servers: str = Field('localhost:9092', env='KAFKA_BOOTSTRAP_SERVERS')
    starting_offsets: str = Field('latest', env='SPARK_STARTING_OFFSETS')  # 'latest' or 'earliest'
    
    # Streaming settings
    trigger_interval: str = Field('30 seconds', env='SPARK_TRIGGER_INTERVAL')
    checkpoint_location: str = Field('/tmp/spark-checkpoints', env='SPARK_CHECKPOINT_LOCATION')
    
    # Processing settings
    max_offsets_per_trigger: int = Field(1000, env='SPARK_MAX_OFFSETS_PER_TRIGGER')
    
    model_config = SettingsConfigDict(env_file='.env', extra='ignore', case_sensitive=True)


class StreamingSettings(BaseSettings):
    """Combined settings for streaming application"""
    
    snowflake: SnowflakeSettings = SnowflakeSettings()
    spark: SparkSettings = SparkSettings()
    
    model_config = SettingsConfigDict(env_file='.env', extra='ignore', case_sensitive=True)


streaming_settings = StreamingSettings()