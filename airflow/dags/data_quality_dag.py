"""
Data Quality DAG - Simple Pipeline

Runs daily to check data freshness, row counts, and null percentages
across all Steam Analytics tables. Alerts if any checks fail.

Schedule: Daily at 6 AM UTC
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import snowflake.connector
import logging

logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'steam_analytics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Tables to monitor with their expected update frequency (hours)
TABLES_CONFIG = {
    'GAME_INFO': {
        'expected_freshness_hours': 48,
        'min_row_count': 1000,
        'critical_columns': ['appid', 'name'],
    },
    'GAME_PLAYER_COUNTS': {
        'expected_freshness_hours': 1,
        'min_row_count': 100,
        'critical_columns': ['appid', 'live_player_count', 'fetched_at'],
    },
    'GAME_REVIEWS': {
        'expected_freshness_hours': 168,  # 1 week
        'min_row_count': 100,
        'critical_columns': ['appid', 'review_text', 'voted_up'],
    },
    'GAME_DETAILS': {
        'expected_freshness_hours': 168,  # 1 week
        'min_row_count': 100,
        'critical_columns': ['appid', 'name', 'short_description'],
    },
    'PLAYERS': {
        'expected_freshness_hours': 168,
        'min_row_count': 10,
        'critical_columns': ['steamid', 'personaname'],
    },
    'PLAYER_STATS': {
        'expected_freshness_hours': 168,
        'min_row_count': 10,
        'critical_columns': ['steamid', 'appid'],
    },
}

# Snowflake connection settings
SNOWFLAKE_CONFIG = {
    'account': os.environ.get('SNOWFLAKE_ACCOUNT', 'NMB12256'),
    'user': os.environ.get('SNOWFLAKE_USER', 'dog'),
    'database': os.environ.get('SNOWFLAKE_DATABASE', 'STEAM_ANALYTICS'),
    'schema': os.environ.get('SNOWFLAKE_SCHEMA', 'RAW'),
    'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE', 'DOG_WH'),
    'role': os.environ.get('SNOWFLAKE_ROLE', 'TRAINING_ROLE'),
    'private_key_file': os.environ.get('SNOWFLAKE_PRIVATE_KEY_FILE', '/opt/airflow/config/rsa_key.p8'),
}

DATABASE = SNOWFLAKE_CONFIG['database']
SCHEMA = SNOWFLAKE_CONFIG['schema']


def get_snowflake_connection():
    """Get Snowflake connection using key-pair authentication"""
    return snowflake.connector.connect(
        account=SNOWFLAKE_CONFIG['account'],
        user=SNOWFLAKE_CONFIG['user'],
        private_key_file=SNOWFLAKE_CONFIG['private_key_file'],
        database=SNOWFLAKE_CONFIG['database'],
        schema=SNOWFLAKE_CONFIG['schema'],
        warehouse=SNOWFLAKE_CONFIG['warehouse'],
        role=SNOWFLAKE_CONFIG['role']
    )


def check_table_freshness(**context):
    """
    Check if tables have been updated within expected timeframe.
    Returns dict of table -> freshness status
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    results = {}

    try:
        cursor.execute(f"USE DATABASE {DATABASE}")
        cursor.execute(f"USE SCHEMA {SCHEMA}")

        for table_name, config in TABLES_CONFIG.items():
            try:
                # Check the most recent record timestamp
                query = f"""
                SELECT MAX(COALESCE(fetched_at, inserted_at, updated_at)) as last_update
                FROM {table_name}
                """
                cursor.execute(query)
                row = cursor.fetchone()

                if row and row[0]:
                    last_update = row[0]
                    hours_since_update = (datetime.utcnow() - last_update).total_seconds() / 3600

                    is_fresh = hours_since_update <= config['expected_freshness_hours']
                    results[table_name] = {
                        'status': 'PASS' if is_fresh else 'FAIL',
                        'last_update': str(last_update),
                        'hours_since_update': round(hours_since_update, 2),
                        'threshold_hours': config['expected_freshness_hours']
                    }
                else:
                    results[table_name] = {
                        'status': 'FAIL',
                        'last_update': None,
                        'hours_since_update': None,
                        'threshold_hours': config['expected_freshness_hours'],
                        'error': 'No data found'
                    }

            except Exception as e:
                results[table_name] = {
                    'status': 'ERROR',
                    'error': str(e)
                }

    finally:
        cursor.close()
        conn.close()

    # Push results to XCom
    context['ti'].xcom_push(key='freshness_results', value=results)

    # Log summary
    failed = [t for t, r in results.items() if r['status'] != 'PASS']
    logger.info(f"Freshness check complete. {len(results) - len(failed)}/{len(results)} tables passed.")
    if failed:
        logger.warning(f"Tables with stale data: {failed}")

    return results


def check_row_counts(**context):
    """
    Check if tables have minimum expected row counts.
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    results = {}

    try:
        cursor.execute(f"USE DATABASE {DATABASE}")
        cursor.execute(f"USE SCHEMA {SCHEMA}")

        for table_name, config in TABLES_CONFIG.items():
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]

                is_valid = row_count >= config['min_row_count']
                results[table_name] = {
                    'status': 'PASS' if is_valid else 'FAIL',
                    'row_count': row_count,
                    'min_expected': config['min_row_count']
                }

            except Exception as e:
                results[table_name] = {
                    'status': 'ERROR',
                    'error': str(e)
                }

    finally:
        cursor.close()
        conn.close()

    context['ti'].xcom_push(key='row_count_results', value=results)

    failed = [t for t, r in results.items() if r['status'] != 'PASS']
    logger.info(f"Row count check complete. {len(results) - len(failed)}/{len(results)} tables passed.")

    return results


def check_null_percentages(**context):
    """
    Check null percentages in critical columns.
    Fails if any critical column has >10% nulls.
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    results = {}

    try:
        cursor.execute(f"USE DATABASE {DATABASE}")
        cursor.execute(f"USE SCHEMA {SCHEMA}")

        for table_name, config in TABLES_CONFIG.items():
            table_results = {}

            for column in config['critical_columns']:
                try:
                    query = f"""
                    SELECT
                        COUNT(*) as total,
                        SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) as null_count
                    FROM {table_name}
                    """
                    cursor.execute(query)
                    row = cursor.fetchone()

                    total, null_count = row[0], row[1]
                    null_pct = (null_count / total * 100) if total > 0 else 0

                    table_results[column] = {
                        'status': 'PASS' if null_pct <= 10 else 'FAIL',
                        'null_percentage': round(null_pct, 2),
                        'null_count': null_count,
                        'total_rows': total
                    }

                except Exception as e:
                    table_results[column] = {
                        'status': 'ERROR',
                        'error': str(e)
                    }

            results[table_name] = table_results

    finally:
        cursor.close()
        conn.close()

    context['ti'].xcom_push(key='null_check_results', value=results)

    # Count failures
    failed_cols = []
    for table, cols in results.items():
        for col, res in cols.items():
            if res['status'] != 'PASS':
                failed_cols.append(f"{table}.{col}")

    logger.info(f"Null check complete. {len(failed_cols)} columns with high null rates.")

    return results


def generate_quality_report(**context):
    """
    Generate a consolidated data quality report.
    """
    ti = context['ti']

    freshness = ti.xcom_pull(key='freshness_results', task_ids='check_freshness')
    row_counts = ti.xcom_pull(key='row_count_results', task_ids='check_row_counts')
    null_checks = ti.xcom_pull(key='null_check_results', task_ids='check_null_percentages')

    report = {
        'run_date': str(datetime.utcnow()),
        'summary': {
            'freshness_passed': sum(1 for r in freshness.values() if r['status'] == 'PASS'),
            'freshness_total': len(freshness),
            'row_count_passed': sum(1 for r in row_counts.values() if r['status'] == 'PASS'),
            'row_count_total': len(row_counts),
        },
        'details': {
            'freshness': freshness,
            'row_counts': row_counts,
            'null_checks': null_checks
        }
    }

    # Determine overall status
    all_passed = (
        report['summary']['freshness_passed'] == report['summary']['freshness_total'] and
        report['summary']['row_count_passed'] == report['summary']['row_count_total']
    )

    report['overall_status'] = 'HEALTHY' if all_passed else 'DEGRADED'

    logger.info(f"=== DATA QUALITY REPORT ===")
    logger.info(f"Overall Status: {report['overall_status']}")
    logger.info(f"Freshness: {report['summary']['freshness_passed']}/{report['summary']['freshness_total']} passed")
    logger.info(f"Row Counts: {report['summary']['row_count_passed']}/{report['summary']['row_count_total']} passed")

    # Store report in Snowflake (optional - create a report table)
    context['ti'].xcom_push(key='quality_report', value=report)

    return report


def decide_alert(**context):
    """
    Decide whether to send an alert based on quality report.
    """
    ti = context['ti']
    report = ti.xcom_pull(key='quality_report', task_ids='generate_report')

    if report['overall_status'] == 'HEALTHY':
        return 'all_checks_passed'
    else:
        return 'send_alert'


def send_alert(**context):
    """
    Send alert for data quality issues.
    In production, this would send to Slack, email, PagerDuty, etc.
    """
    ti = context['ti']
    report = ti.xcom_pull(key='quality_report', task_ids='generate_report')

    logger.warning("=== DATA QUALITY ALERT ===")
    logger.warning(f"Status: {report['overall_status']}")

    # Log specific failures
    for table, result in report['details']['freshness'].items():
        if result['status'] != 'PASS':
            logger.warning(f"STALE DATA: {table} - last updated {result.get('hours_since_update', 'N/A')} hours ago")

    for table, result in report['details']['row_counts'].items():
        if result['status'] != 'PASS':
            logger.warning(f"LOW ROW COUNT: {table} - {result.get('row_count', 0)} rows (min: {result.get('min_expected', 0)})")

    # In production: send to Slack/email
    # slack_webhook.send(message)

    return "Alert sent"


# Define the DAG
with DAG(
    dag_id='steam_data_quality',
    default_args=default_args,
    description='Daily data quality checks for Steam Analytics tables',
    schedule='0 6 * * *',  # Daily at 6 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['steam', 'data-quality', 'monitoring'],
) as dag:

    start = EmptyOperator(task_id='start')

    check_freshness = PythonOperator(
        task_id='check_freshness',
        python_callable=check_table_freshness,
    )

    check_rows = PythonOperator(
        task_id='check_row_counts',
        python_callable=check_row_counts,
    )

    check_nulls = PythonOperator(
        task_id='check_null_percentages',
        python_callable=check_null_percentages,
    )

    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_quality_report,
    )

    decide = BranchPythonOperator(
        task_id='decide_alert',
        python_callable=decide_alert,
    )

    all_passed = EmptyOperator(task_id='all_checks_passed')

    alert = PythonOperator(
        task_id='send_alert',
        python_callable=send_alert,
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed_min_one_success'
    )

    # Define task dependencies
    start >> [check_freshness, check_rows, check_nulls]
    [check_freshness, check_rows, check_nulls] >> generate_report
    generate_report >> decide
    decide >> [all_passed, alert]
    [all_passed, alert] >> end
