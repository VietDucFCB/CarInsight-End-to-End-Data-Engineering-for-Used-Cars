import os
import json
import logging
from datetime import datetime

# Configure logging - using absolute paths instead of __file__
logs_dir = "/opt/airflow/logs"
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(logs_dir, "convert_to_json.log")),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Configuration - use absolute paths
CONFIG = {
    'input_dir': '/opt/airflow/data_crawled',  # Fixed path
    'output_dir': '/opt/airflow/data_json',  # Fixed path
    'timestamp_file': 'last_crawl_timestamp.txt'
}


def initialize_output_directory(output_dir):
    """Create output directory if it doesn't exist."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created output directory: {output_dir}")
    return output_dir


def parse_text_file(file_path):
    """Parse the text file and extract structured data."""
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    lines = content.split('\n')
    data = {}

    # Extract key fields
    for line in lines:
        if not line.strip():
            continue

        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip()

            if key == 'Features':
                # Split the features by semicolon
                value = [feature.strip() for feature in value.split(';') if feature.strip()]

            data[key] = value

    # Extract overview info
    overview_keys = [
        'Exterior', 'Interior', 'Mileage', 'Fuel Type', 'MPG',
        'Transmission', 'Drivetrain', 'Engine', 'Location',
        'Listed Since', 'VIN', 'Stock Number'
    ]

    overview_info = {}
    for key in overview_keys:
        if key in data:
            overview_info[key] = data.pop(key)

    # Structure final data
    structured_data = {
        'title': data.get('Title', 'No Title'),
        'price_info': {
            'cash_price': data.get('Cash Price', 'N/A'),
            'finance_price': data.get('Finance Price', 'N/A'),
            'finance_details': data.get('Finance Details', 'N/A')
        },
        'overview': overview_info,
        'features': data.get('Features', []),
        'crawl_time': data.get('Crawl Time', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        'conversion_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    return structured_data


def convert_text_files_to_json(input_dir, output_dir):
    """Convert all text files in the input directory to JSON format."""
    logger.info(f"Starting conversion from {input_dir} to {output_dir}")

    # Get all page directories
    page_dirs = [d for d in os.listdir(input_dir) if os.path.isdir(os.path.join(input_dir, d))]
    logger.info(f"Found {len(page_dirs)} page directories: {page_dirs}")

    total_converted = 0

    for page_dir in page_dirs:
        page_path = os.path.join(input_dir, page_dir)
        page_output_path = os.path.join(output_dir, page_dir)

        # Create page output directory
        if not os.path.exists(page_output_path):
            os.makedirs(page_output_path)
            logger.info(f"Created directory {page_output_path}")

        # Get all text files (excluding metadata files)
        text_files = [f for f in os.listdir(page_path) if f.endswith('.txt') and not f.endswith('_meta.txt')]
        logger.info(f"Found {len(text_files)} text files in {page_dir}")

        for text_file in text_files:
            try:
                # Get file path
                text_file_path = os.path.join(page_path, text_file)
                logger.info(f"Processing {text_file_path}")

                # Get corresponding metadata file if exists
                meta_json_path = os.path.join(page_path, text_file.replace('.txt', '_meta.json'))

                # Parse text file
                data = parse_text_file(text_file_path)

                # Add metadata if available
                if os.path.exists(meta_json_path):
                    with open(meta_json_path, 'r', encoding='utf-8') as meta_file:
                        metadata = json.load(meta_file)
                        data['metadata'] = metadata
                        logger.info(f"Added metadata from {meta_json_path}")

                # Save as JSON
                json_file_path = os.path.join(page_output_path, text_file.replace('.txt', '.json'))
                with open(json_file_path, 'w', encoding='utf-8') as json_file:
                    json.dump(data, json_file, indent=2)

                logger.info(f"Converted {text_file_path} to {json_file_path}")
                total_converted += 1

            except Exception as e:
                logger.error(f"Error converting {text_file}: {str(e)}")

    # Create a summary file
    summary = {
        'conversion_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'converted_files_count': total_converted
    }

    with open(os.path.join(output_dir, 'conversion_summary.json'), 'w', encoding='utf-8') as summary_file:
        json.dump(summary, summary_file, indent=2)

    logger.info(f"Created conversion summary: {total_converted} files converted")

    return total_converted


def main():
    """Main function to convert text files to JSON."""
    logger.info("Starting text to JSON conversion process")

    # Initialize output directory
    initialize_output_directory(CONFIG['output_dir'])

    # Convert text files to JSON
    total = convert_text_files_to_json(CONFIG['input_dir'], CONFIG['output_dir'])

    logger.info(f"Text to JSON conversion complete - {total} files processed")
    return True


if __name__ == "__main__":
    main()