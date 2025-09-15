"""
csv_producer_mk.py

Stream numeric data to a Kafka topic.

It is common to transfer csv data as JSON so 
each field is clearly labeled. 
"""

# Standard Library
import os
import sys
import time
import pathlib
import csv
import json
from datetime import datetime

# Add project root to sys.path
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

# External Packages
from dotenv import load_dotenv

# Internal Imports
from utils.utils_logger import logger
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)

# Load environment variables
load_dotenv()

def get_kafka_topic() -> str:
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_message_interval() -> int:
    interval = int(os.getenv("SMOKER_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval

# Use your custom data file
DATA_FILE = PROJECT_ROOT / "data" / "mk_temps.csv"
logger.info(f"Data file: {DATA_FILE}")

def generate_messages(file_path: pathlib.Path):
    """
    Read each row from the CSV file and yield a structured message.
    """
    while True:
        try:
            logger.info(f"Opening data file in read mode: {file_path}")
            with open(file_path, "r") as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    if "temperature" not in row:
                        logger.warning(f"Missing temperature in row: {row}")
                        continue
                    message = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "temperature": float(row["temperature"])
                    }
                    logger.debug(f"Generated message: {message}")
                    yield message
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            sys.exit(3)

def main():
    logger.info("START producer.")
    verify_services()

    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Kafka producer could not be created. Exiting...")
        sys.exit(3)

    try:
        create_kafka_topic(topic)
    except Exception as e:
        logger.error(f"Error managing topic '{topic}': {e}")
        sys.exit(1)

    try:
        for message in generate_messages(DATA_FILE):
            producer.send(topic, value=message)
            logger.info(f"Sent: {message}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error during production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")
        logger.info("END producer.")

if __name__ == "__main__":
    main()
