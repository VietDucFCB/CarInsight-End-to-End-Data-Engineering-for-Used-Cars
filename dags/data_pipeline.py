from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import sys

# Thêm thư mục scripts vào sys.path để có thể import các module từ scripts
scripts_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
sys.path.append(scripts_dir)

# Import các module từ scripts
from scripts import crawl, convertTextToJson, ETL, ETL_transfer, LoadDataIntoDataLake
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
    'data_pipeline',
    default_args=default_args,
    description='Data pipeline with crawling, processing, and ETL',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Define Python function tasks instead of bash commands
def run_crawl():
    crawl.main()

def run_convert_to_json():
    convertTextToJson.main()

def run_load_to_datalake():
    LoadDataIntoDataLake.main()

def run_etl():
    ETL.main()

def run_etl_transfer():
    ETL_transfer.main()

def trigger_kafka_etl():
    send_kafka_message(
        bootstrap_servers='localhost:9092',
        topic='new-data-topic',
        key=f'new-data-{datetime.now().strftime("%Y%m%d")}',
        value={"status": "new_data_available", "timestamp": datetime.now().isoformat()}
    )

def trigger_kafka_app():
    send_kafka_message(
        bootstrap_servers='localhost:9092',
        topic='new-warehouse-data',
        key=f'warehouse-update-{datetime.now().strftime("%Y%m%d")}',
        value={"status": "warehouse_updated", "timestamp": datetime.now().isoformat()}
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
    task_id='load_to_datalake',
    python_callable=run_load_to_datalake,
    dag=dag,
)

trigger_etl_kafka_task = PythonOperator(
    task_id='trigger_etl_kafka',
    python_callable=trigger_kafka_etl,
    dag=dag,
)

etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl,
    dag=dag,
)

etl_transfer_task = PythonOperator(
    task_id='run_etl_transfer',
    python_callable=run_etl_transfer,
    dag=dag,
)

trigger_app_kafka_task = PythonOperator(
    task_id='trigger_app_kafka',
    python_callable=trigger_kafka_app,
    dag=dag,
)

# Define task dependencies
crawl_task >> convert_to_json_task >> load_to_datalake_task >> trigger_etl_kafka_task >> etl_task >> etl_transfer_task >> trigger_app_kafka_task