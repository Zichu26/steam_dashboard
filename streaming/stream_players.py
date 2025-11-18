from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, TimestampType
)
from streaming.snowflake_settings import streaming_settings
from streaming.snowflake_manager import SnowflakeManager
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PlayersStreamProcessor:
    """Spark Streaming processor for players topic"""
    
    def __init__(self):
        self.settings = streaming_settings
        self.spark = self._create_spark_session()
        self.snowflake_manager = SnowflakeManager()
        
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with necessary configurations"""
        logger.info("Creating Spark session...")
        
        spark = SparkSession.builder \
            .appName(f"{self.settings.spark.app_name}_Players") \
            .master(self.settings.spark.master) \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                   "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,"
                   "net.snowflake:snowflake-jdbc:3.13.30") \
            .config("spark.sql.streaming.checkpointLocation", 
                   f"{self.settings.spark.checkpoint_location}/players") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        return spark
    
    def _get_player_schema(self) -> StructType:
        """Define schema for player data from Kafka"""
        return StructType([
            StructField("steamid", StringType(), True),
            StructField("personaname", StringType(), True),
            StructField("profileurl", StringType(), True),
            StructField("avatar", StringType(), True),
            StructField("avatarmedium", StringType(), True),
            StructField("avatarfull", StringType(), True),
            StructField("personastate", IntegerType(), True),
            StructField("communityvisibilitystate", IntegerType(), True),
            StructField("profilestate", IntegerType(), True),
            StructField("lastlogoff", LongType(), True),
            StructField("commentpermission", IntegerType(), True),
            StructField("realname", StringType(), True),
            StructField("primaryclanid", StringType(), True),
            StructField("timecreated", LongType(), True),
            StructField("gameid", StringType(), True),
            StructField("gameserverip", StringType(), True),
            StructField("gameextrainfo", StringType(), True),
            StructField("loccountrycode", StringType(), True),
            StructField("locstatecode", StringType(), True),
            StructField("loccityid", IntegerType(), True),
            StructField("fetched_at", StringType(), True),
            StructField("source", StringType(), True),
        ])
    
    def read_from_kafka(self):
        """Read streaming data from Kafka"""
        logger.info(f"Reading from Kafka topic: {self.settings.spark.kafka_bootstrap_servers}")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.settings.spark.kafka_bootstrap_servers) \
            .option("subscribe", "steam.players") \
            .option("startingOffsets", self.settings.spark.starting_offsets) \
            .option("maxOffsetsPerTrigger", self.settings.spark.max_offsets_per_trigger) \
            .option("failOnDataLoss", "false") \
            .load()
        
        return df
    
    def transform_data(self, df):
        """Transform Kafka data to Snowflake format"""
        logger.info("Transforming player data...")
        
        # Parse JSON from Kafka value
        player_schema = self._get_player_schema()
        
        df_parsed = df.select(
            from_json(col("value").cast("string"), player_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        )
        
        # Flatten the structure
        df_flat = df_parsed.select("data.*", "kafka_timestamp")
        
        # Convert Unix timestamps to proper timestamps
        df_transformed = df_flat \
            .withColumn("lastlogoff", 
                       to_timestamp(col("lastlogoff"))) \
            .withColumn("timecreated", 
                       to_timestamp(col("timecreated"))) \
            .withColumn("fetched_at", 
                       to_timestamp(col("fetched_at"))) \
            .withColumn("updated_at", 
                       current_timestamp())
        
        return df_transformed
    
    def write_to_snowflake(self, df, epoch_id):
        """Write batch to Snowflake"""
        logger.info(f"Writing batch {epoch_id} to Snowflake...")
        
        try:
            # Get Snowflake options
            sf_options = self.snowflake_manager.get_snowflake_options()
            
            # Write to Snowflake
            df.write \
                .format("snowflake") \
                .options(**sf_options) \
                .option("dbtable", self.settings.snowflake.table_players) \
                .option("truncate_table", "off") \
                .option("usestagingtable", "on") \
                .mode("append") \
                .save()
            
            count = df.count()
            logger.info(f"Successfully wrote {count} records to Snowflake (batch {epoch_id})")
            
        except Exception as e:
            logger.error(f"Error writing to Snowflake: {e}", exc_info=True)
            raise
    
    def start_streaming(self):
        """Start the streaming pipeline"""
        logger.info("="*60)
        logger.info("Starting Players Stream Processor")
        logger.info("="*60)
        
        try:
            # Read from Kafka
            kafka_df = self.read_from_kafka()
            
            # Transform data
            transformed_df = self.transform_data(kafka_df)
            
            # Write to Snowflake
            query = transformed_df \
                .writeStream \
                .outputMode("append") \
                .foreachBatch(self.write_to_snowflake) \
                .trigger(processingTime=self.settings.spark.trigger_interval) \
                .option("checkpointLocation", 
                       f"{self.settings.spark.checkpoint_location}/players") \
                .start()
            
            logger.info(f"Streaming query started. Checkpoint: {self.settings.spark.checkpoint_location}/players")
            logger.info(f"Trigger interval: {self.settings.spark.trigger_interval}")
            logger.info("Waiting for data...")
            
            # Wait for termination
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Streaming error: {e}", exc_info=True)
            raise
        finally:
            self.stop()
    
    def stop(self):
        """Stop Spark session"""
        logger.info("Stopping Players Stream Processor...")
        if self.spark:
            self.spark.stop()
        logger.info("Stopped successfully")


def main():
    """Main entry point"""
    processor = PlayersStreamProcessor()
    
    try:
        processor.start_streaming()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        processor.stop()


if __name__ == "__main__":
    main()