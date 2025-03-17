import os
import json
import logging
from datetime import datetime
import requests
import shutil
import tempfile
import base64
import socket
import time

# Configure logging
logs_dir = "/opt/airflow/logs"
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(logs_dir, "load_to_datalake.log")),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'json_data_dir': '/opt/airflow/data_json',
    'hdfs_url': 'http://namenode:9870/webhdfs/v1',  # WebHDFS REST API endpoint
    'hdfs_user': 'hadoop',
    'hdfs_base_dir': '/user/hadoop/datalake',
    'archive_dir': '/opt/airflow/archive',
    'user': 'VietDucFCB',
    'current_time': '2025-03-17 07:28:51'  # Current time from user input
}


class WebHDFSClient:
    """Simple WebHDFS client using requests library."""

    def __init__(self, base_url, user):
        """Initialize WebHDFS client."""
        self.base_url = base_url.rstrip('/')
        self.user = user
        self.session = requests.Session()

    def _build_url(self, path, operation, **params):
        """Build WebHDFS URL."""
        path = path.lstrip('/')
        params['op'] = operation
        params['user.name'] = self.user

        # Convert params to query string
        query = '&'.join([f"{k}={v}" for k, v in params.items()])

        return f"{self.base_url}/{path}?{query}"

    def check_connection(self):
        """Check if WebHDFS is accessible."""
        try:
            url = self._build_url('/', 'LISTSTATUS')
            response = self.session.get(url, timeout=5)
            if response.status_code == 200:
                logger.info("WebHDFS connection successful")
                return True
            else:
                logger.error(f"WebHDFS connection failed with status {response.status_code}: {response.text}")
                return False
        except Exception as e:
            logger.error(f"WebHDFS connection failed: {str(e)}")
            return False

    def check_exists(self, path):
        """Check if path exists in HDFS."""
        try:
            url = self._build_url(path, 'GETFILESTATUS')
            response = self.session.get(url)
            return response.status_code == 200
        except:
            return False

    def mkdir(self, path):
        """Create directory in HDFS."""
        try:
            url = self._build_url(path, 'MKDIRS')
            response = self.session.put(url)

            if response.status_code == 200:
                logger.info(f"Created directory: {path}")
                return True
            else:
                logger.error(f"Failed to create directory {path}: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error creating directory {path}: {str(e)}")
            return False

    def upload_file(self, local_path, hdfs_path):
        """Upload a file to HDFS."""
        try:
            # First create a file with CREATE operation
            create_url = self._build_url(hdfs_path, 'CREATE', overwrite='true')

            # Get the redirect URL from the Location header
            response = self.session.put(create_url, allow_redirects=False)

            if response.status_code != 307:  # 307 means redirect to datanode
                logger.error(f"Failed to initiate upload: {response.status_code} - {response.text}")
                return False

            # Get the redirect URL
            redirect_url = response.headers['Location']

            # Now upload the file content to the redirect URL
            with open(local_path, 'rb') as f:
                upload_response = self.session.put(
                    redirect_url,
                    data=f,
                    headers={'Content-Type': 'application/octet-stream'}
                )

            if upload_response.status_code == 201:  # 201 Created
                logger.info(f"Uploaded {local_path} to {hdfs_path}")
                return True
            else:
                logger.error(f"Failed to upload file: {upload_response.status_code} - {upload_response.text}")
                return False
        except Exception as e:
            logger.error(f"Error uploading file {local_path}: {str(e)}")
            return False


def create_fallback_storage(message="No reason provided"):
    """Create fallback storage when HDFS is not accessible."""
    fallback_dir = os.path.join(CONFIG['archive_dir'], "hdfs_fallback", datetime.now().strftime("%Y-%m-%d"))

    # Create directory structure
    os.makedirs(fallback_dir, exist_ok=True)

    # Create a fallback info file
    with open(os.path.join(fallback_dir, "fallback_info.txt"), "w") as f:
        f.write(f"Fallback created: {datetime.now()}\n")
        f.write(f"Reason: {message}\n")
        f.write(f"User: {CONFIG['user']}\n")

    logger.info(f"Created fallback directory: {fallback_dir}")
    return fallback_dir


def add_metadata_to_json(json_data):
    """Add additional metadata to JSON data."""
    json_data['datalake_metadata'] = {
        'load_timestamp': CONFIG['current_time'],
        'data_source': 'truecar_crawler',
        'data_version': '1.0',
        'processing_pipeline': 'airflow_kafka_pipeline',
        'storage_system': 'hdfs',
        'loader_user': CONFIG['user']
    }
    return json_data


def load_to_hdfs(hdfs_client, json_data_dir, hdfs_base_dir):
    """Load JSON files to HDFS."""
    loaded_files = []

    # Create a date-specific directory in HDFS
    current_date = datetime.now().strftime("%Y-%m-%d")
    hdfs_date_dir = f"{hdfs_base_dir}/{current_date}"

    # Create the base directory if it doesn't exist
    if not hdfs_client.check_exists(hdfs_base_dir):
        logger.info(f"Creating HDFS base directory: {hdfs_base_dir}")
        hdfs_client.mkdir(hdfs_base_dir)

    # Create the date directory if it doesn't exist
    if not hdfs_client.check_exists(hdfs_date_dir):
        logger.info(f"Creating HDFS date directory: {hdfs_date_dir}")
        hdfs_client.mkdir(hdfs_date_dir)

    # Create a temporary directory for enhanced JSON files
    with tempfile.TemporaryDirectory() as temp_dir:
        # Process each directory (page)
        for page_dir in [d for d in os.listdir(json_data_dir) if os.path.isdir(os.path.join(json_data_dir, d))]:
            page_path = os.path.join(json_data_dir, page_dir)
            hdfs_page_dir = f"{hdfs_date_dir}/{page_dir}"

            # Create page directory in HDFS
            if not hdfs_client.check_exists(hdfs_page_dir):
                logger.info(f"Creating HDFS page directory: {hdfs_page_dir}")
                hdfs_client.mkdir(hdfs_page_dir)

            # Get all JSON files
            json_files = [f for f in os.listdir(page_path) if
                          f.endswith('.json') and f != 'conversion_summary.json']

            for json_file in json_files:
                try:
                    # Get file path
                    json_file_path = os.path.join(page_path, json_file)

                    # Read JSON data
                    with open(json_file_path, 'r', encoding='utf-8') as file:
                        data = json.load(file)

                    # Add metadata
                    enhanced_data = add_metadata_to_json(data)

                    # Save with timestamp prefix for uniqueness
                    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                    new_filename = f"{timestamp}_{json_file}"
                    temp_file_path = os.path.join(temp_dir, new_filename)

                    with open(temp_file_path, 'w', encoding='utf-8') as file:
                        json.dump(enhanced_data, file, indent=2)

                    # Upload to HDFS
                    hdfs_file_path = f"{hdfs_page_dir}/{new_filename}"
                    if hdfs_client.upload_file(temp_file_path, hdfs_file_path):
                        loaded_files.append(hdfs_file_path)

                except Exception as e:
                    logger.error(f"Error processing file {json_file}: {str(e)}")

        # Create a manifest file
        manifest = {
            'load_timestamp': CONFIG['current_time'],
            'loaded_files': loaded_files,
            'loaded_files_count': len(loaded_files),
            'source_directory': json_data_dir,
            'hdfs_directory': hdfs_date_dir,
            'user': CONFIG['user']
        }

        manifest_path = os.path.join(temp_dir, 'manifest.json')
        with open(manifest_path, 'w', encoding='utf-8') as file:
            json.dump(manifest, file, indent=2)

        # Upload manifest to HDFS
        hdfs_manifest_path = f"{hdfs_date_dir}/manifest.json"
        hdfs_client.upload_file(manifest_path, hdfs_manifest_path)
        logger.info(f"Uploaded manifest to {hdfs_manifest_path}")

    return loaded_files


def load_to_fallback(json_data_dir, fallback_dir):
    """Load JSON files to fallback location."""
    loaded_files = []

    # Process each directory (page)
    for page_dir in [d for d in os.listdir(json_data_dir) if os.path.isdir(os.path.join(json_data_dir, d))]:
        page_path = os.path.join(json_data_dir, page_dir)
        fallback_page_dir = os.path.join(fallback_dir, page_dir)

        # Create page directory in fallback
        os.makedirs(fallback_page_dir, exist_ok=True)

        # Get all JSON files
        json_files = [f for f in os.listdir(page_path) if
                      f.endswith('.json') and f != 'conversion_summary.json']

        for json_file in json_files:
            try:
                # Get file path
                json_file_path = os.path.join(page_path, json_file)

                # Read JSON data
                with open(json_file_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)

                # Add metadata
                enhanced_data = add_metadata_to_json(data)
                enhanced_data['datalake_metadata']['storage_system'] = 'local_fallback'

                # Save with timestamp prefix for uniqueness
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                new_filename = f"{timestamp}_{json_file}"
                fallback_file_path = os.path.join(fallback_page_dir, new_filename)

                with open(fallback_file_path, 'w', encoding='utf-8') as file:
                    json.dump(enhanced_data, file, indent=2)

                logger.info(f"FALLBACK: Copied {json_file_path} to {fallback_file_path}")
                loaded_files.append(fallback_file_path)

            except Exception as e:
                logger.error(f"Error processing file {json_file}: {str(e)}")

    # Create a manifest file
    manifest = {
        'load_timestamp': CONFIG['current_time'],
        'loaded_files': loaded_files,
        'loaded_files_count': len(loaded_files),
        'source_directory': json_data_dir,
        'storage_type': 'local_fallback',
        'fallback_directory': fallback_dir,
        'user': CONFIG['user']
    }

    manifest_path = os.path.join(fallback_dir, 'manifest.json')
    with open(manifest_path, 'w', encoding='utf-8') as file:
        json.dump(manifest, file, indent=2)

    logger.info(f"FALLBACK: Created manifest at {manifest_path}")

    return loaded_files


def archive_processed_data(json_data_dir):
    """Archive the processed data."""
    try:
        # Create archive directory with timestamp
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        archive_path = os.path.join(CONFIG['archive_dir'], f"json_data_{timestamp}")

        # Copy data to archive
        shutil.copytree(json_data_dir, archive_path)
        logger.info(f"Archived data to {archive_path}")

        return True
    except Exception as e:
        logger.error(f"Error archiving data: {str(e)}")
        return False


def main():
    """Main function to load data."""
    logger.info("Starting data lake loading process using WebHDFS API")

    try:
        # Check if namenode is reachable
        try:
            hostname = CONFIG['hdfs_url'].split('//')[1].split('/')[0].split(':')[0]
            port = int(CONFIG['hdfs_url'].split(':')[-1].split('/')[0])

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            result = sock.connect_ex((hostname, port))
            sock.close()

            if result != 0:
                logger.error(f"Cannot connect to namenode at {hostname}:{port}")
                raise Exception(f"Connection to namenode {hostname}:{port} failed")

            logger.info(f"Namenode at {hostname}:{port} is reachable")
        except Exception as e:
            logger.error(f"Error checking namenode connection: {str(e)}")
            raise

        # Create WebHDFS client
        hdfs_client = WebHDFSClient(CONFIG['hdfs_url'], CONFIG['hdfs_user'])

        # Check HDFS connection
        if not hdfs_client.check_connection():
            logger.error("Failed to connect to WebHDFS API")
            raise Exception("WebHDFS connection failed")

        # Load data to HDFS
        loaded_files = load_to_hdfs(hdfs_client, CONFIG['json_data_dir'], CONFIG['hdfs_base_dir'])

        # Archive original data
        archive_processed_data(CONFIG['json_data_dir'])

        logger.info(f"HDFS data lake loading complete. Loaded {len(loaded_files)} files.")
        return True

    except Exception as e:
        logger.error(f"Error in HDFS data lake loading process: {str(e)}")

        # Create fallback storage
        fallback_dir = create_fallback_storage(str(e))

        # Load to fallback storage
        loaded_files = load_to_fallback(CONFIG['json_data_dir'], fallback_dir)

        # Archive original data
        archive_processed_data(CONFIG['json_data_dir'])

        logger.info(f"Data lake loading complete. Loaded {len(loaded_files)} files to local fallback storage.")
        return True


if __name__ == "__main__":
    main()