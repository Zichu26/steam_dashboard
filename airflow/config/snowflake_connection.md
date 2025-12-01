# Snowflake Connection Setup for Airflow

After starting Airflow, configure the Snowflake connection:

## Option 1: Airflow UI

1. Go to http://localhost:8080
2. Login with airflow/airflow
3. Navigate to Admin > Connections
4. Add a new connection:
   - Connection Id: `snowflake_default`
   - Connection Type: `Snowflake`
   - Account: `your_account` (e.g., `abc12345.us-east-1`)
   - Login: `your_username`
   - Password: (leave blank if using key auth)
   - Schema: `RAW`
   - Extra:
     ```json
     {
       "database": "STEAM_ANALYTICS",
       "warehouse": "COMPUTE_WH",
       "role": "ACCOUNTADMIN",
       "private_key_file": "/path/to/rsa_key.p8"
     }
     ```

## Option 2: Airflow CLI

```bash
docker-compose exec airflow-webserver airflow connections add 'snowflake_default' \
    --conn-type 'snowflake' \
    --conn-login 'your_username' \
    --conn-host 'your_account' \
    --conn-schema 'RAW' \
    --conn-extra '{"database": "STEAM_ANALYTICS", "warehouse": "COMPUTE_WH", "role": "ACCOUNTADMIN"}'
```

## Option 3: Environment Variable

Set in docker-compose.yaml or .env:

```
AIRFLOW_CONN_SNOWFLAKE_DEFAULT='snowflake://user@account/STEAM_ANALYTICS/RAW?warehouse=COMPUTE_WH&role=ACCOUNTADMIN'
```
