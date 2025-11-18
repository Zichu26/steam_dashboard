import snowflake.connector
from snowflake.connector import DictCursor
from streaming.snowflake_settings import streaming_settings
from typing import Optional, List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class SnowflakeManager:
    """Manages Snowflake connections and operations"""
    
    def __init__(self):
        self.settings = streaming_settings.snowflake
        self.connection: Optional[snowflake.connector.SnowflakeConnection] = None
    
    def connect(self) -> snowflake.connector.SnowflakeConnection:
        """Establish connection to Snowflake"""
        if self.connection is None or self.connection.is_closed():
            logger.info("Connecting to Snowflake...")
            self.connection = snowflake.connector.connect(
                account=self.settings.snowflake_account,
                user=self.settings.snowflake_user,
                private_key_file=self.settings.private_key_file,
                database=self.settings.snowflake_database,
                schema=self.settings.snowflake_schema,
                warehouse=self.settings.snowflake_warehouse,
                role=self.settings.snowflake_role
            )
            logger.info("Connected to Snowflake successfully")
        return self.connection
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results"""
        conn = self.connect()
        cursor = conn.cursor(DictCursor)
        try:
            cursor.execute(query, params)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
    
    def execute_many(self, query: str, data: List[tuple]) -> None:
        """Execute batch insert"""
        conn = self.connect()
        cursor = conn.cursor()
        try:
            cursor.executemany(query, data)
            conn.commit()
        finally:
            cursor.close()
    
    def create_database_and_schema(self):
        """Create database and schema if they don't exist"""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # Create database
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.settings.snowflake_database}")
            logger.info(f"Database {self.settings.snowflake_database} created/verified")
            
            # Use database
            cursor.execute(f"USE DATABASE {self.settings.snowflake_database}")
            
            # Create schema
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.settings.snowflake_schema}")
            logger.info(f"Schema {self.settings.snowflake_schema} created/verified")
            
        finally:
            cursor.close()
    
    def create_tables(self):
        """Create all required tables for Steam data"""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # Use the correct database and schema
            cursor.execute(f"USE DATABASE {self.settings.snowflake_database}")
            cursor.execute(f"USE SCHEMA {self.settings.snowflake_schema}")
            
            # Create PLAYERS table
            players_ddl = f"""
            CREATE TABLE IF NOT EXISTS {self.settings.table_players} (
                steamid VARCHAR(255) PRIMARY KEY,
                personaname VARCHAR(500),
                profileurl VARCHAR(500),
                avatar VARCHAR(500),
                avatarmedium VARCHAR(500),
                avatarfull VARCHAR(500),
                personastate INTEGER,
                communityvisibilitystate INTEGER,
                profilestate INTEGER,
                lastlogoff TIMESTAMP_NTZ,
                commentpermission INTEGER,
                realname VARCHAR(500),
                primaryclanid VARCHAR(255),
                timecreated TIMESTAMP_NTZ,
                gameid VARCHAR(255),
                gameserverip VARCHAR(100),
                gameextrainfo VARCHAR(500),
                loccountrycode VARCHAR(10),
                locstatecode VARCHAR(10),
                loccityid INTEGER,
                fetched_at TIMESTAMP_NTZ,
                source VARCHAR(100),
                inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            cursor.execute(players_ddl)
            logger.info(f"Table {self.settings.table_players} created/verified")
            
            # Create GAMES table
            games_ddl = f"""
            CREATE TABLE IF NOT EXISTS {self.settings.table_games} (
                game_id VARCHAR(255) PRIMARY KEY,
                steamid VARCHAR(255),
                appid INTEGER,
                name VARCHAR(1000),
                playtime_forever INTEGER,
                img_icon_url VARCHAR(500),
                img_logo_url VARCHAR(500),
                playtime_windows_forever INTEGER,
                playtime_mac_forever INTEGER,
                playtime_linux_forever INTEGER,
                playtime_deck_forever INTEGER,
                rtime_last_played TIMESTAMP_NTZ,
                playtime_disconnected INTEGER,
                fetched_at TIMESTAMP_NTZ,
                source VARCHAR(100),
                inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                FOREIGN KEY (steamid) REFERENCES {self.settings.table_players}(steamid)
            )
            """
            cursor.execute(games_ddl)
            logger.info(f"Table {self.settings.table_games} created/verified")
            
            # Create PLAYER_STATS table (for recently played games)
            stats_ddl = f"""
            CREATE TABLE IF NOT EXISTS {self.settings.table_stats} (
                stat_id VARCHAR(255) PRIMARY KEY,
                steamid VARCHAR(255),
                appid INTEGER,
                name VARCHAR(1000),
                playtime_2weeks INTEGER,
                playtime_forever INTEGER,
                img_icon_url VARCHAR(500),
                img_logo_url VARCHAR(500),
                playtime_windows_forever INTEGER,
                playtime_mac_forever INTEGER,
                playtime_linux_forever INTEGER,
                playtime_deck_forever INTEGER,
                rtime_last_played TIMESTAMP_NTZ,
                data_type VARCHAR(100),
                fetched_at TIMESTAMP_NTZ,
                source VARCHAR(100),
                inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                FOREIGN KEY (steamid) REFERENCES {self.settings.table_players}(steamid)
            )
            """
            cursor.execute(stats_ddl)
            logger.info(f"Table {self.settings.table_stats} created/verified")
            
        finally:
            cursor.close()
    
    def setup_schema(self):
        """Complete schema setup"""
        logger.info("Setting up Snowflake schema...")
        self.create_database_and_schema()
        self.create_tables()
        logger.info("Schema setup complete!")
    
    def get_snowflake_options(self) -> Dict[str, str]:
        """Get Snowflake connection options for Spark"""
        return {
            "sfURL": f"{self.settings.snowflake_account}.snowflakecomputing.com",
            "sfUser": self.settings.snowflake_user,
            "pem_private_key": """MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCJVc6BHlozP5Ab
bjcN0lEziscaK6UyHc1onbrZuG4f1K7WLflptDVRH4DfIB8OxUeoJ3U+UY4JEHS6
C2onbbnYynbMkdO8neyIxBz9T9ufCnw/rMK4mnBXQtGLmRq4sICKJOj8DYyufEAP
dImCUSfT3F3eX8OJ6nMRKs8cZfQvSzPkhzRJW8oog490KGkzK0qzWvUHlvfj3AGI
DrlzoaL7+fCHf3LZpYtP0RwNfBXFcteLBpzmhow+F5u40y5SvMMPLN0GnVKY6JVv
4rFfAe3u0pwLByTq/qm3/H3u7iQ3FWtXyUTWrHRBeGqkHrbrzr9ZkecECeZ8xomT
Vj9KkKzDAgMBAAECggEAL22jwHGkAvjboq2Ac9SDWIJ/2ijihxlFu/Bk/BW2amW/
75W3AetLUnXHwsbMEwTMh2UBF4GAxN6bATk58t3xBc5+Eo32CribXGwl+tFebmFn
oXHfWXN0gp4/0a8RTGEgMxUmJQ3k3MlqMiwz5P5z+7Wp8UcvN2zxSid3mT4Yg1A6
2ENndrjjGpxHICbBquUY50Ei7rhBUpu6hfEORR/AMP8y9pX/omk+EKGMyzGKRrni
QQSiU6vnqVoT+AbCombHAQ2kSuhl51sH4++K8dzeemGiUahEY9bGPSWm54GnOik2
v7DCTiriHCwpYPW7wS5PqvI0AYsOYiH6YO8j5qOh4QKBgQC+XdBq86dT19J3OHnW
ryqOGm7FoYqXzXXEgRs6loplm2SSoQlcwkKyTZoGeVhVoq4kx9cTc1xqiYsPAU85
PSDoJbmPKFHhUTcwaESh7g94TBTWHxraajZebyuP6U1LEzO8Sp7xT8KHJVB8Ne0E
Sgte02D2MsugMO4eLTSbvXP68wKBgQC4r1QlpEoKKnfGH1DC+b2qTbonpBYrjMGY
XdXHhd8/RODslwOJw3L+iipazexg1pwxsdYEtnklXfprmOlDA8oZ0z1hkNRolbGb
VXcvoKLTqRjF2bSvnxgDNlANduxofEtFsPnB2sJs44fL5ZdF5qhBU2UymsIDgM9x
tiCt5zha8QKBgF9lj+4oz+96NiQ8jwIAoei4Yk5zl4pfEec5c7W2fwBQEORSAukT
CG1l6gvYf2Xasj4IT0WWB/fzmVYZp+PdOEJdtVGCORYapveBKPOk4ue76K0sxT4y
/6/vMftaRLuckc6H7oKrH6auEllMhMwjdvM8Jlj6N4S9QHaq3LQ3Con1AoGBAJNh
97kCXsO2GFdXbk/qlV1vIfu4iqi+b1B0scHEJ+CJyyWDbiNmgkJ3aE2Gv/iXy5Ys
4SpppEwZr1pmcOA2CfaKCRXvSlMcAtx/W9n6S3a2Hzrk/biZMcR+d2wHV6oaZsd6
8BWvyjCzcw9YbVwvMl7PepbjwaiiBuor+fBN8T9BAoGBALN5b8psuQLpqliTr7rG
nAmsQ8jqrOAKatNswaJzHeb9CTwD0dXdoC09nitkGZbZv443egcmXVDpZyqc2MN4
7YRG5Ij1mxbOTYevJeyuCTGOZf9tNSfi+c5NhYStEKQThJFVYMUBNLHt9prciAJv
wDhRRpJaOKfnySljgnPyYery""",
            "sfDatabase": self.settings.snowflake_database,
            "sfSchema": self.settings.snowflake_schema,
            "sfWarehouse": self.settings.snowflake_warehouse,
            "sfRole": self.settings.snowflake_role
        }
    
    def close(self):
        """Close connection"""
        if self.connection and not self.connection.is_closed():
            self.connection.close()
            logger.info("Snowflake connection closed")


# Convenience function for setup
def setup_snowflake_schema():
    """Setup Snowflake schema - run this once"""
    manager = SnowflakeManager()
    try:
        manager.setup_schema()
    finally:
        manager.close()


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run setup
    setup_snowflake_schema()