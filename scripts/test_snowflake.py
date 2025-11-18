import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

# --- Key Pair Authentication variables ---
PRIVATE_KEY_PATH = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')

try:
    # Key Pair Authentication uses private_key_file and private_key_file_pwd
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        # Set the path to the encrypted private key file
        private_key_file=PRIVATE_KEY_PATH, 
    )
    print("✓ Connected to Snowflake using Key Pair Authentication!")
    
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION()")
    print(f"✓ Version: {cursor.fetchone()[0]}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"✗ Failed: {e}")