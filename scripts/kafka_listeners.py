#!/usr/bin/env python
import os
import json
import logging
import time
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import threading
import subprocess

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# Kafka topics
TOPIC_NEW_DATA = 'new-data-topic'
TOPIC_WAREHOUSE_UPDATE = 'new-warehouse-data'


def send_kafka_message(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=None, key=None, value=None):
    """
    Send a message to a Kafka topic

    Args:
        bootstrap_servers (str): Kafka bootstrap servers address
        topic (str): Topic to send the message to
        key (str): Message key
        value (dict): Message value
    """
    if topic is None or value is None:
        logger.error("Topic and value must be provided")
        return False

    try:
        # Create Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode() if isinstance(k, str) else str(k).encode(),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Send message
        future = producer.send(topic, key=key, value=value)
        result = future.get(timeout=60)

        logger.info(f"Message sent to topic {topic}: {value}")
        producer.flush()
        producer.close()

        return True

    except KafkaError as e:
        logger.error(f"Error sending message to Kafka: {e}")
        return False


def trigger_new_data_event(timestamp=None):
    """
    Trigger an event when new data is available in data lake
    """
    if timestamp is None:
        timestamp = datetime.now().isoformat()

    message_key = f"new-data-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    message_value = {
        "status": "new_data_available",
        "timestamp": timestamp,
        "source": "data_pipeline"
    }

    return send_kafka_message(topic=TOPIC_NEW_DATA, key=message_key, value=message_value)


def trigger_warehouse_update_event(timestamp=None, data_info=None):
    """
    Trigger an event when data warehouse is updated
    """
    if timestamp is None:
        timestamp = datetime.now().isoformat()

    message_key = f"warehouse-update-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    message_value = {
        "status": "warehouse_updated",
        "timestamp": timestamp,
        "data_info": data_info or {},
        "source": "etl_transfer"
    }

    return send_kafka_message(topic=TOPIC_WAREHOUSE_UPDATE, key=message_key, value=message_value)


def consume_messages(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=None, group_id=None, callback=None):
    """
    Consume messages from a Kafka topic

    Args:
        bootstrap_servers (str): Kafka bootstrap servers address
        topic (str): Topic to consume messages from
        group_id (str): Consumer group ID
        callback (function): Callback function to process messages
    """
    if topic is None or group_id is None or callback is None:
        logger.error("Topic, group_id, and callback must be provided")
        return

    try:
        # Create Kafka consumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        logger.info(f"Started Kafka consumer for topic {topic}, group {group_id}")

        # Consume messages
        for message in consumer:
            try:
                logger.info(f"Received message from topic {message.topic}: {message.value}")
                callback(message)
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except KafkaError as e:
        logger.error(f"Error consuming messages from Kafka: {e}")


def handle_new_data_message(message):
    """
    Handle messages from new-data-topic
    """
    try:
        value = message.value
        timestamp = value.get('timestamp')

        logger.info(f"Handling new data event from {timestamp}")

        # Run the ETL script
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ETL.py')

        if os.path.exists(script_path):
            logger.info(f"Running ETL script: {script_path}")
            subprocess.run(['python', script_path])
        else:
            logger.error(f"ETL script not found: {script_path}")

    except Exception as e:
        logger.error(f"Error handling new data message: {e}")


def handle_warehouse_update_message(message):
    """
    Handle messages from new-warehouse-data topic
    """
    try:
        value = message.value
        timestamp = value.get('timestamp')

        logger.info(f"Handling warehouse update event from {timestamp}")

        # Trigger app refresh or notify app about new data
        app_script = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'app', 'app.py')

        if os.path.exists(app_script):
            logger.info(f"Notifying app about new data: {app_script}")
            # In a real implementation, you might use a REST API call or another mechanism
            # to notify the app rather than running the script directly
            subprocess.run(['python', app_script, '--refresh'])
        else:
            logger.error(f"App script not found: {app_script}")

    except Exception as e:
        logger.error(f"Error handling warehouse update message: {e}")


def start_listeners():
    """
    Start Kafka listeners in separate threads
    """
    # Start new data listener
    new_data_thread = threading.Thread(
        target=consume_messages,
        args=(KAFKA_BOOTSTRAP_SERVERS, TOPIC_NEW_DATA, 'etl-consumers', handle_new_data_message),
        daemon=True
    )
    new_data_thread.start()

    # Start warehouse update listener
    warehouse_thread = threading.Thread(
        target=consume_messages,
        args=(KAFKA_BOOTSTRAP_SERVERS, TOPIC_WAREHOUSE_UPDATE, 'app-consumers', handle_warehouse_update_message),
        daemon=True
    )
    warehouse_thread.start()

    return new_data_thread, warehouse_thread


def run_listener_service():
    """
    Run as a standalone listener service
    """
    logger.info("Starting Kafka listeners service")

    threads = start_listeners()

    try:
        # Keep the service running
        while True:
            time.sleep(1)

            # Check if threads are alive
            if not all(thread.is_alive() for thread in threads):
                logger.error("One or more listener threads died, restarting...")
                threads = start_listeners()

    except KeyboardInterrupt:
        logger.info("Kafka listeners service stopping...")


if __name__ == "__main__":
    run_listener_service()