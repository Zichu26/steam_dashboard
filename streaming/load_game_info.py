"""
Direct Kafka to Snowflake loader for game info data.

This script consumes messages from the steam.game_info Kafka topic
and loads them directly into Snowflake without requiring Spark.
"""

import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from streaming.snowflake_manager import SnowflakeManager
from streaming.snowflake_settings import streaming_settings
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GameInfoLoader:
    """Loads game info from Kafka to Snowflake"""

    def __init__(self):
        self.settings = streaming_settings
        self.snowflake = SnowflakeManager()
        self.consumer = self._create_consumer()

    def _create_consumer(self, timeout_ms: int = 10000) -> KafkaConsumer:
        """Create Kafka consumer"""
        logger.info("Creating Kafka consumer...")
        return KafkaConsumer(
            'steam.game_info',
            bootstrap_servers=self.settings.spark.kafka_bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='game_info_loader',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=timeout_ms
        )

    def _prepare_record(self, data: dict) -> tuple:
        """Prepare a record for Snowflake insertion"""
        # Convert tags dict to JSON string
        tags_json = json.dumps(data.get('tags', {})) if data.get('tags') else None

        # Parse fetched_at timestamp
        fetched_at = None
        if data.get('fetched_at'):
            try:
                fetched_at = datetime.fromisoformat(data['fetched_at'].replace('+00:00', ''))
            except:
                fetched_at = datetime.utcnow()

        return (
            data.get('appid'),
            data.get('name'),
            data.get('developer'),
            data.get('publisher'),
            data.get('owners'),
            data.get('owners_variance'),
            data.get('players_forever'),
            data.get('players_2weeks'),
            data.get('average_forever'),
            data.get('average_2weeks'),
            data.get('median_forever'),
            data.get('median_2weeks'),
            data.get('ccu'),
            int(data.get('price', 0)) if data.get('price') else None,
            int(data.get('initialprice', 0)) if data.get('initialprice') else None,
            int(data.get('discount', 0)) if data.get('discount') else None,
            data.get('languages'),
            data.get('genre'),
            tags_json,
            data.get('positive'),
            data.get('negative'),
            fetched_at,
            data.get('source', 'steamspy')
        )

    def load_all(self) -> int:
        """Load all messages from Kafka to Snowflake"""
        logger.info("=" * 60)
        logger.info("Starting Game Info Loader")
        logger.info("=" * 60)

        records = []

        # Consume all messages
        logger.info("Consuming messages from Kafka...")
        for message in self.consumer:
            try:
                record = self._prepare_record(message.value)
                records.append(record)
                logger.debug(f"Prepared: {message.value.get('name', 'Unknown')}")
            except Exception as e:
                logger.warning(f"Failed to prepare record: {e}")

        logger.info(f"Consumed {len(records)} messages from Kafka")

        if not records:
            logger.info("No records to load")
            return 0

        # Insert into Snowflake using MERGE (upsert)
        logger.info("Loading records into Snowflake...")
        conn = self.snowflake.connect()
        cursor = conn.cursor()

        try:
            # Use database and schema
            cursor.execute(f"USE DATABASE {self.settings.snowflake.snowflake_database}")
            cursor.execute(f"USE SCHEMA {self.settings.snowflake.snowflake_schema}")

            # Insert with ON CONFLICT handling (merge/upsert)
            insert_sql = f"""
            MERGE INTO {self.settings.snowflake.table_game_info} AS target
            USING (SELECT
                %s AS appid, %s AS name, %s AS developer, %s AS publisher,
                %s AS owners, %s AS owners_variance, %s AS players_forever,
                %s AS players_2weeks, %s AS average_forever, %s AS average_2weeks,
                %s AS median_forever, %s AS median_2weeks, %s AS ccu,
                %s AS price, %s AS initialprice, %s AS discount,
                %s AS languages, %s AS genre, PARSE_JSON(%s) AS tags,
                %s AS positive, %s AS negative, %s AS fetched_at, %s AS source
            ) AS source
            ON target.appid = source.appid
            WHEN MATCHED THEN UPDATE SET
                name = source.name,
                developer = source.developer,
                publisher = source.publisher,
                owners = source.owners,
                owners_variance = source.owners_variance,
                players_forever = source.players_forever,
                players_2weeks = source.players_2weeks,
                average_forever = source.average_forever,
                average_2weeks = source.average_2weeks,
                median_forever = source.median_forever,
                median_2weeks = source.median_2weeks,
                ccu = source.ccu,
                price = source.price,
                initialprice = source.initialprice,
                discount = source.discount,
                languages = source.languages,
                genre = source.genre,
                tags = source.tags,
                positive = source.positive,
                negative = source.negative,
                fetched_at = source.fetched_at,
                source = source.source,
                updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                appid, name, developer, publisher, owners, owners_variance,
                players_forever, players_2weeks, average_forever, average_2weeks,
                median_forever, median_2weeks, ccu, price, initialprice, discount,
                languages, genre, tags, positive, negative, fetched_at, source
            ) VALUES (
                source.appid, source.name, source.developer, source.publisher,
                source.owners, source.owners_variance, source.players_forever,
                source.players_2weeks, source.average_forever, source.average_2weeks,
                source.median_forever, source.median_2weeks, source.ccu,
                source.price, source.initialprice, source.discount,
                source.languages, source.genre, source.tags,
                source.positive, source.negative, source.fetched_at, source.source
            )
            """

            success_count = 0
            for record in records:
                try:
                    cursor.execute(insert_sql, record)
                    success_count += 1
                except Exception as e:
                    logger.warning(f"Failed to insert appid {record[0]}: {e}")

            conn.commit()
            logger.info(f"Successfully loaded {success_count} records into Snowflake")
            return success_count

        finally:
            cursor.close()

    def close(self):
        """Clean up resources"""
        self.consumer.close()
        self.snowflake.close()
        logger.info("Loader closed")


    def run_continuous(self, batch_interval: int = 60):
        """
        Run continuously, loading batches from Kafka to Snowflake.

        Args:
            batch_interval: Seconds between batch loads (default 60)
        """
        logger.info("=" * 60)
        logger.info(f"CONTINUOUS MODE - Loading batches every {batch_interval}s")
        logger.info("=" * 60)

        total_loaded = 0
        batch_num = 0

        while True:
            try:
                batch_num += 1
                count = self.load_all()
                total_loaded += count

                if count > 0:
                    logger.info(f"Batch #{batch_num}: Loaded {count} records (Total: {total_loaded:,})")
                else:
                    logger.debug(f"Batch #{batch_num}: No new records")

                # Recreate consumer for next batch
                self.consumer.close()
                self.consumer = self._create_consumer(timeout_ms=30000)

                time.sleep(batch_interval)

            except KeyboardInterrupt:
                logger.info("Interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error in batch #{batch_num}: {e}")
                logger.info("Retrying in 30 seconds...")
                time.sleep(30)


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Load game info from Kafka to Snowflake')
    parser.add_argument('--continuous', '-c', action='store_true',
                        help='Run in continuous mode')
    parser.add_argument('--interval', '-i', type=int, default=60,
                        help='Seconds between batches in continuous mode (default: 60)')

    args = parser.parse_args()

    loader = GameInfoLoader()

    try:
        if args.continuous:
            loader.run_continuous(batch_interval=args.interval)
        else:
            count = loader.load_all()
            logger.info(f"Completed - loaded {count} games into GAME_INFO table")
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        loader.close()


if __name__ == "__main__":
    main()
