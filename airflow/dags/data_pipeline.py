from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import sys

# Add scripts directory to path to import modules
scripts_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
sys.path.append(scripts_dir)

# Import modules from scripts - note: ETL_transfer is not used as per your request
from scripts import crawl, convertTextToJson, LoadDataIntoDataLake, ETL
from scripts.kafka_listeners import send_kafka_message

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'car_data_pipeline',
    default_args=default_args,
    description='Data pipeline for car data with crawling, processing, and ETL to PostgreSQL',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Define Python function tasks
def run_crawl():
    """Run the crawler to gather car data."""
    return crawl.main()

def run_convert_to_json():
    """Convert crawled text files to structured JSON."""
    return convertTextToJson.main()

def run_load_to_datalake():
    """Load JSON data to HDFS data lake."""
    return LoadDataIntoDataLake.main()

def run_etl():
    """Run ETL process to load data from HDFS to PostgreSQL warehouse."""
    return ETL.main()

def trigger_kafka_etl():
    """Send Kafka message to notify ETL process."""
    return send_kafka_message(
        bootstrap_servers='localhost:9092',
        topic='new-data-topic',
        key=f'new-data-{datetime.now().strftime("%Y%m%d")}',
        value={
            "status": "new_data_available",
            "timestamp": datetime.now().isoformat(),
            "source": "data_pipeline",
            "message": "New data available in HDFS data lake"
        }
    )

def trigger_kafka_app():
    """Send Kafka message to notify applications of new data in warehouse."""
    return send_kafka_message(
        bootstrap_servers='localhost:9092',
        topic='new-warehouse-data',
        key=f'warehouse-update-{datetime.now().strftime("%Y%m%d")}',
        value={
            "status": "warehouse_updated",
            "timestamp": datetime.now().isoformat(),
            "source": "etl_process",
            "message": "PostgreSQL data warehouse has been updated"
        }
    )

# Define tasks
crawl_task = PythonOperator(
    task_id='crawl_data',
    python_callable=run_crawl,
    dag=dag,
)

convert_to_json_task = PythonOperator(
    task_id='convert_text_to_json',
    python_callable=run_convert_to_json,
    dag=dag,
)

load_to_datalake_task = PythonOperator(
    task_id='load_to_hdfs_datalake',
    python_callable=run_load_to_datalake,
    dag=dag,
)

trigger_etl_kafka_task = PythonOperator(
    task_id='trigger_etl_kafka',
    python_callable=trigger_kafka_etl,
    dag=dag,
)

etl_task = PythonOperator(
    task_id='run_etl_to_postgres',
    python_callable=run_etl,
    dag=dag,
)

trigger_app_kafka_task = PythonOperator(
    task_id='trigger_app_kafka',
    python_callable=trigger_kafka_app,
    dag=dag,
)

# Define task dependencies - skip ETL_transfer as requested
crawl_task >> convert_to_json_task >> load_to_datalake_task >> trigger_etl_kafka_task >> etl_task >> trigger_app_kafka_task