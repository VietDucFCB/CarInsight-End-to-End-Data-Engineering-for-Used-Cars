from datetime import datetime, timedelta
from airflow import DAG
import time
from airflow.operators.python import PythonOperator
import os
import sys
import json
import logging

# Fix import paths by adding the correct locations to system path
dag_folder = os.path.dirname(os.path.abspath(__file__))
project_folder = os.path.dirname(dag_folder)
scripts_folder = os.path.join(project_folder, 'scripts')

# Add scripts folder to path to make imports work
if scripts_folder not in sys.path:
    sys.path.append(scripts_folder)
    print(f"Added {scripts_folder} to Python path")
if project_folder not in sys.path:
    sys.path.append(project_folder)
    print(f"Added {project_folder} to Python path")

# Import the email utilities from our custom script
from sendMail import EmailSender, generate_pipeline_report

# Define default arguments with email settings
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 13),
    'email': ['kkagiuma1@gmail.com'],
    'email_on_failure': True,
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

# Global status tracking for pipeline report
pipeline_status = {
    'start_time': datetime.now(),
    'tasks': {},
    'overall_status': 'running'
}


# Helper function to update task status
def update_task_status(task_id, status, **kwargs):
    """Update a task's status in the pipeline status tracking."""
    if task_id not in pipeline_status['tasks']:
        pipeline_status['tasks'][task_id] = {
            'status': status,
            'start_time': kwargs.get('start_time', datetime.now())
        }
    else:
        pipeline_status['tasks'][task_id]['status'] = status

    if status in ['success', 'failed']:
        pipeline_status['tasks'][task_id]['end_time'] = kwargs.get('end_time', datetime.now())

        # Calculate execution time
        if 'start_time' in pipeline_status['tasks'][task_id]:
            start = pipeline_status['tasks'][task_id]['start_time']
            end = pipeline_status['tasks'][task_id]['end_time']
            execution_time = (end - start).total_seconds()
            pipeline_status['tasks'][task_id]['execution_time'] = f"{execution_time:.2f}s"

    # Add error if provided
    if 'error' in kwargs:
        pipeline_status['tasks'][task_id]['error'] = kwargs['error']

    # Add any additional data
    for key, value in kwargs.items():
        if key not in ['start_time', 'end_time', 'error']:
            pipeline_status['tasks'][task_id][key] = value

    # Update overall status if any task fails
    if status == 'failed':
        pipeline_status['overall_status'] = 'failed'


# Task success/failure callback functions
def task_success_callback(context):
    """Callback for successful tasks to update the pipeline status."""
    task_id = context['task'].task_id
    update_task_status(
        task_id,
        'success',
        end_time=datetime.now()
    )


def task_failure_callback(context):
    """Callback for failed tasks to update the pipeline status."""
    task_id = context['task'].task_id
    error = str(context.get('exception', 'Unknown error'))
    update_task_status(
        task_id,
        'failed',
        end_time=datetime.now(),
        error=error
    )


# Define Python function tasks
def run_crawl(**kwargs):
    """Run the crawler to gather car data."""
    task_id = kwargs['task_instance'].task_id
    update_task_status(task_id, 'running', start_time=datetime.now())

    logging.info("Running crawler to gather car data")

    try:
        # Get the scripts folder path
        scripts_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scripts')
        crawler_path = os.path.join(scripts_folder, 'crawl.py')

        logging.info(f"Checking if crawler exists at: {crawler_path}")
        if not os.path.exists(crawler_path):
            raise FileNotFoundError(f"Crawler script not found at {crawler_path}")

        logging.info("Running crawler directly")
        start_time = datetime.now()

        # Solution: Add __file__ to the globals dictionary
        crawler_globals = {'__file__': crawler_path}
        with open(crawler_path) as f:
            exec(f.read(), crawler_globals)
        result = crawler_globals.get('main')()

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()

        logging.info(f"Crawler completed in {execution_time:.2f} seconds")

        # Update status
        update_task_status(
            task_id,
            'success',
            start_time=start_time,
            end_time=end_time,
            execution_time=f"{execution_time:.2f}s"
        )

        return {
            "status": "success",
            "message": "Crawling completed",
            "execution_time": f"{execution_time:.2f}s",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user": "VietDucFCB"
        }
    except FileNotFoundError as e:
        logging.error(f"Crawler file not found: {str(e)}")
        update_task_status(task_id, 'failed', error=str(e))
        raise
    except Exception as e:
        logging.error(f"Crawling failed: {str(e)}")
        update_task_status(task_id, 'failed', error=str(e))
        raise


def run_convert_to_json(**kwargs):
    """Convert crawled text files to structured JSON."""
    task_id = kwargs['task_instance'].task_id
    update_task_status(task_id, 'running', start_time=datetime.now())

    try:
        # Get the scripts folder path
        scripts_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scripts')
        converter_path = os.path.join(scripts_folder, 'convertTextToJson.py')

        logging.info(f"Checking if converter exists at: {converter_path}")
        if not os.path.exists(converter_path):
            raise FileNotFoundError(f"Converter script not found at {converter_path}")

        logging.info("Running converter directly")
        start_time = datetime.now()

        # Execute the converter script with a prepared environment
        converter_globals = {
            '__file__': converter_path,  # Add missing __file__ variable
            'os': os,
            'json': json,
            'logging': logging,
            'datetime': datetime
        }

        with open(converter_path) as f:
            exec(f.read(), converter_globals)
        result = converter_globals.get('main')()

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()

        logging.info(f"Text conversion completed in {execution_time:.2f} seconds")

        # Update status
        update_task_status(
            task_id,
            'success',
            start_time=start_time,
            end_time=end_time,
            execution_time=f"{execution_time:.2f}s"
        )

        return {
            "status": "success",
            "message": "Text conversion completed",
            "execution_time": f"{execution_time:.2f}s",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user": "VietDucFCB"  # Fixed username
        }
    except FileNotFoundError as e:
        update_task_status(task_id, 'failed', error=str(e))
        logging.error(f"Converter file not found: {str(e)}")
        raise
    except Exception as e:
        update_task_status(task_id, 'failed', error=str(e))
        logging.error(f"Text conversion failed: {str(e)}")
        raise


def run_load_to_datalake(**kwargs):
    """Load JSON data to HDFS data lake."""
    task_id = kwargs['task_instance'].task_id
    update_task_status(task_id, 'running', start_time=datetime.now())

    try:
        # Get the scripts folder path
        scripts_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scripts')
        loader_path = os.path.join(scripts_folder, 'LoadDataIntoDataLake.py')

        logging.info(f"Checking if loader exists at: {loader_path}")
        if not os.path.exists(loader_path):
            raise FileNotFoundError(f"Loader script not found at {loader_path}")

        logging.info("Running loader directly")
        start_time = datetime.now()

        # Import basic modules that should always be available
        loader_globals = {
            'os': os,
            'json': json,
            'logging': logging,
            'datetime': datetime,
            'shutil': __import__('shutil'),
            'tempfile': __import__('tempfile'),
            'subprocess': __import__('subprocess'),
            'time': __import__('time')
        }

        # Try to import hdfs, but don't fail if it's not available
        try:
            hdfs = __import__('hdfs')
            loader_globals['hdfs'] = hdfs
            loader_globals['InsecureClient'] = hdfs.InsecureClient
            logging.info("Successfully imported HDFS module")
        except ImportError:
            logging.warning("HDFS module not available, will use fallback storage")

        # Execute the loader script
        with open(loader_path) as f:
            exec(f.read(), loader_globals)
        result = loader_globals.get('main')()

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()

        logging.info(f"Data lake loading completed in {execution_time:.2f} seconds")

        # Update status
        update_task_status(
            task_id,
            'success',
            start_time=start_time,
            end_time=end_time,
            execution_time=f"{execution_time:.2f}s"
        )

        return {
            "status": "success",
            "message": "Data loaded to datalake",
            "execution_time": f"{execution_time:.2f}s",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user": "VietDucFCB"
        }
    except Exception as e:
        update_task_status(task_id, 'failed', error=str(e))
        logging.error(f"Data lake loading failed: {str(e)}")
        raise


def run_etl(**kwargs):
    """Run ETL process by submitting it to Spark master."""
    task_id = kwargs['task_instance'].task_id
    update_task_status(task_id, 'running', start_time=datetime.now())

    start_time = datetime.now()

    try:
        # Get the scripts folder path
        scripts_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scripts')
        etl_path = os.path.join(scripts_folder, 'ETL.py')

        logging.info(f"Checking if ETL script exists at: {etl_path}")
        if not os.path.exists(etl_path):
            raise FileNotFoundError(f"ETL script not found at {etl_path}")

        # Create logs directory in Spark master container
        logging.info("Creating logs directory in Spark master container")
        os.system("docker exec spark-master mkdir -p /tmp/etl_logs")
        os.system("docker exec spark-master chmod 777 /tmp/etl_logs")

        # Create a modified version of the ETL script with updated logs path
        with open(etl_path, 'r') as f:
            etl_content = f.read()

        # Replace the logs directory path
        modified_etl_content = etl_content.replace(
            'logs_dir = os.path.join(os.path.dirname(__file__), "..", "logs")',
            'logs_dir = "/tmp/etl_logs"'
        )

        # Write the modified ETL script to a temporary file
        temp_etl_path = os.path.join('/tmp', 'ETL_modified.py')
        with open(temp_etl_path, 'w') as f:
            f.write(modified_etl_content)

        # Copy modified ETL script to Spark master
        copy_cmd = f"docker cp {temp_etl_path} spark-master:/tmp/ETL.py"
        logging.info(f"Copying modified ETL script to Spark master: {copy_cmd}")
        os.system(copy_cmd)

        # Install required Python packages in Spark master
        logging.info("Installing required Python packages in Spark master")
        install_cmd = "docker exec spark-master pip install py4j psycopg2-binary"
        os.system(install_cmd)

        # Create processed_markers directory in Spark master if it doesn't exist
        os.system("docker exec spark-master mkdir -p /tmp/processed_markers")
        os.system("docker exec spark-master chmod 777 /tmp/processed_markers")

        # Set environment variables for the ETL script
        env_vars = " ".join([
            f"HDFS_URL=http://namenode:9870",
            f"HDFS_USER=hadoop",
            f"HDFS_BASE_DIR=/user/hadoop/datalake",
            f"PG_HOST=postgres",
            f"PG_PORT=5432",
            f"PG_DATABASE=car_warehouse",
            f"PG_USER=postgres",
            f"PG_PASSWORD=postgres",
            f"CURRENT_DATE_UTC={datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}",
            f"CURRENT_USER=VietDucFCB"
        ])

        # Run the ETL script on Spark master
        cmd = f"docker exec spark-master bash -c '{env_vars} PROCESSED_MARKER_DIR=/tmp/processed_markers python /tmp/ETL.py'"
        logging.info(f"Running ETL on Spark master: {cmd}")

        import subprocess
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        # Copy logs back to Airflow
        os.system("docker cp spark-master:/tmp/etl_logs/. /opt/airflow/logs/etl/")

        # Copy processed markers back to Airflow
        os.system("docker cp spark-master:/tmp/processed_markers/. /opt/airflow/processed_markers/")

        # Check if ETL was successful
        if process.returncode != 0:
            logging.error(f"ETL process failed with code {process.returncode}")
            logging.error(f"Stdout: {stdout.decode('utf-8')}")
            logging.error(f"Stderr: {stderr.decode('utf-8')}")
            raise Exception(f"ETL process failed with code {process.returncode}")

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()

        logging.info(f"ETL process completed in {execution_time:.2f} seconds")
        logging.info(f"ETL output: {stdout.decode('utf-8')}")

        # Update status
        update_task_status(
            task_id,
            'success',
            start_time=start_time,
            end_time=end_time,
            execution_time=f"{execution_time:.2f}s"
        )

        return {
            "status": "success",
            "message": "ETL process completed",
            "execution_time": f"{execution_time:.2f}s",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user": "VietDucFCB"
        }
    except Exception as e:
        update_task_status(task_id, 'failed', error=str(e))
        logging.error(f"ETL process failed: {str(e)}")
        raise


def trigger_kafka_etl(**kwargs):
    """Send Kafka message to notify ETL process."""
    task_id = kwargs['task_instance'].task_id
    update_task_status(task_id, 'running', start_time=datetime.now())

    start_time = datetime.now()

    try:
        message = {
            "status": "new_data_available",
            "timestamp": datetime.now().isoformat(),
            "source": "data_pipeline",
            "message": "New data available in HDFS data lake",
            "user": "VietDucFCB"
        }

        logging.info(f"Would send Kafka message for ETL trigger: {json.dumps(message)}")

        # For now, we'll just simulate the sending
        # In production, you would implement actual Kafka message sending
        time.sleep(1)  # Simulate processing time

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()

        # Update status
        update_task_status(
            task_id,
            'success',
            start_time=start_time,
            end_time=end_time,
            execution_time=f"{execution_time:.2f}s"
        )

        return {
            "status": "success",
            "message": "Kafka ETL notification sent",
            "execution_time": f"{execution_time:.2f}s",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user": "VietDucFCB"
        }
    except Exception as e:
        update_task_status(task_id, 'failed', error=str(e))
        logging.error(f"Failed to send Kafka ETL notification: {str(e)}")
        raise


def trigger_kafka_app(**kwargs):
    """Send Kafka message to notify applications of new data in warehouse."""
    task_id = kwargs['task_instance'].task_id
    update_task_status(task_id, 'running', start_time=datetime.now())

    start_time = datetime.now()

    try:
        message = {
            "status": "warehouse_updated",
            "timestamp": datetime.now().isoformat(),
            "source": "etl_process",
            "message": "PostgreSQL data warehouse has been updated",
            "user": "VietDucFCB"
        }

        logging.info(f"Would send Kafka message for application notification: {json.dumps(message)}")

        # For now, we'll just simulate the sending
        # In production, you would implement actual Kafka message sending
        time.sleep(1)  # Simulate processing time

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()

        # Update status
        update_task_status(
            task_id,
            'success',
            start_time=start_time,
            end_time=end_time,
            execution_time=f"{execution_time:.2f}s"
        )

        # Set overall pipeline status to success if all tasks so far have succeeded
        all_successful = all(
            details.get('status') == 'success'
            for task_id, details in pipeline_status['tasks'].items()
            if task_id != 'send_pipeline_report_email'
        )
        if all_successful:
            pipeline_status['overall_status'] = 'success'

        return {
            "status": "success",
            "message": "Kafka application notification sent",
            "execution_time": f"{execution_time:.2f}s",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user": "VietDucFCB"
        }
    except Exception as e:
        update_task_status(task_id, 'failed', error=str(e))
        pipeline_status['overall_status'] = 'failed'
        logging.error(f"Failed to send Kafka application notification: {str(e)}")
        raise


def send_pipeline_report_email(**kwargs):
    """Generate and send email report about pipeline execution."""
    task_id = kwargs['task_instance'].task_id
    update_task_status(task_id, 'running', start_time=datetime.now())

    start_time = datetime.now()

    try:
        # Gather information about all tasks from the current DAG run
        dag_run = kwargs['dag_run']
        task_instances = dag_run.get_task_instances()

        # Update pipeline status with information from Airflow task instances if missing
        for ti in task_instances:
            ti_id = ti.task_id
            if ti_id != task_id and ti_id not in pipeline_status['tasks']:
                ti_state = ti.state

                # Convert Airflow state to our status format
                status = 'success' if ti_state == 'success' else (
                    'failed' if ti_state == 'failed' else 'running')

                # Add to pipeline status
                update_task_status(
                    ti_id,
                    status,
                    start_time=ti.start_date,
                    end_time=ti.end_date,
                    execution_time=f"{(ti.end_date - ti.start_date).total_seconds():.2f}s" if ti.end_date and ti.start_date else "N/A"
                )

        # Finalize overall status based on task statuses
        if all(details.get('status') == 'success' for task_id, details in pipeline_status['tasks'].items()):
            pipeline_status['overall_status'] = 'success'

        # Generate the email content using our custom function
        html_content = generate_pipeline_report(pipeline_status)

        # Create sender without config - it will use environment variables
        sender = EmailSender()

        # Get recipients from DAG default_args
        recipients = kwargs['dag_run'].dag.default_args.get('email', ['kkagiuma1@gmail.com'])

        subject = f"CarInsight Data Pipeline Report - {datetime.now().strftime('%Y-%m-%d')}"

        # Send the email
        success = sender.send_email(
            to_emails=recipients,
            subject=subject,
            html_content=html_content
        )

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()

        if success:
            logging.info(f"Email sent successfully in {execution_time:.2f} seconds")
            update_task_status(
                task_id,
                'success',
                start_time=start_time,
                end_time=end_time,
                execution_time=f"{execution_time:.2f}s"
            )
        else:
            logging.warning("Email delivery failed")
            update_task_status(
                task_id,
                'failed',
                start_time=start_time,
                end_time=end_time,
                execution_time=f"{execution_time:.2f}s",
                error="Email delivery failed"
            )

        return {
            "status": "success" if success else "failed",
            "message": "Email report sent" if success else "Email delivery failed",
            "execution_time": f"{execution_time:.2f}s",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user": "VietDucFCB"
        }

    except Exception as e:
        update_task_status(task_id, 'failed', error=str(e))
        logging.error(f"Email sending failed: {str(e)}")
        raise


# Define tasks with callbacks for status tracking
crawl_task = PythonOperator(
    task_id='crawl_data',
    python_callable=run_crawl,
    provide_context=True,
    on_success_callback=task_success_callback,
    on_failure_callback=task_failure_callback,
    dag=dag,
)

convert_to_json_task = PythonOperator(
    task_id='convert_text_to_json',
    python_callable=run_convert_to_json,
    provide_context=True,
    on_success_callback=task_success_callback,
    on_failure_callback=task_failure_callback,
    dag=dag,
)

load_to_datalake_task = PythonOperator(
    task_id='load_to_hdfs_datalake',
    python_callable=run_load_to_datalake,
    provide_context=True,
    on_success_callback=task_success_callback,
    on_failure_callback=task_failure_callback,
    dag=dag,
)

trigger_etl_kafka_task = PythonOperator(
    task_id='trigger_etl_kafka',
    python_callable=trigger_kafka_etl,
    provide_context=True,
    on_success_callback=task_success_callback,
    on_failure_callback=task_failure_callback,
    dag=dag,
)

etl_task = PythonOperator(
    task_id='run_etl_to_postgres',
    python_callable=run_etl,
    provide_context=True,
    on_success_callback=task_success_callback,
    on_failure_callback=task_failure_callback,
    dag=dag,
)

trigger_app_kafka_task = PythonOperator(
    task_id='trigger_app_kafka',
    python_callable=trigger_kafka_app,
    provide_context=True,
    on_success_callback=task_success_callback,
    on_failure_callback=task_failure_callback,
    dag=dag,
)

send_email_task = PythonOperator(
    task_id='send_pipeline_report_email',
    python_callable=send_pipeline_report_email,
    provide_context=True,
    on_success_callback=task_success_callback,
    on_failure_callback=task_failure_callback,
    dag=dag,
)

# Define task dependencies
crawl_task >> convert_to_json_task >> load_to_datalake_task >> trigger_etl_kafka_task >> etl_task >> trigger_app_kafka_task >> send_email_task