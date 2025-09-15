"""
csv_consumer_mk.py

Consume JSON messages from a Kafka topic and process them.

Example Kafka message format:
{"timestamp": "2025-01-11T18:15:00Z", "temperature": 225.0}
"""

#####################################
# Import Modules
#####################################

import os
import json
from collections import deque
from dotenv import load_dotenv

from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("MK_TOPIC", "unknown_topic")
    logger.info(f"[MK] Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id = os.getenv("MK_CONSUMER_GROUP_ID", "mk_default_group")
    logger.info(f"[MK] Kafka consumer group id: {group_id}")
    return group_id

def get_stall_threshold() -> float:
    """Fetch message interval from environment or use default."""
    threshold = float(os.getenv("MK_STALL_THRESHOLD_F", 0.2))
    logger.info(f"[MK] Max stall temperature range: {threshold} F")
    return threshold

def get_rolling_window_size() -> int:
    """Fetch rolling window size from environment or use default."""
    window_size = int(os.getenv("MK_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"[MK] Rolling window size: {window_size}")
    return window_size

#####################################
# Stall Detection
#####################################

def detect_stall(rolling_window_deque: deque) -> bool:
    """Detect temperature stall using rolling window."""
    WINDOW_SIZE = get_rolling_window_size()
    if len(rolling_window_deque) < WINDOW_SIZE:
        logger.debug(f"[MK] Rolling window not full: {len(rolling_window_deque)}/{WINDOW_SIZE}")
        return False

    temp_range = max(rolling_window_deque) - min(rolling_window_deque)
    is_stalled = temp_range <= get_stall_threshold()
    logger.debug(f"[MK] Temp range: {temp_range}°F. Stalled: {is_stalled}")
    return is_stalled

#####################################
# Message Processing
#####################################

def process_message(message: str, rolling_window: deque, window_size: int) -> None:
    """Process a Kafka JSON message and detect temperature stalls."""
    try:
        logger.debug(f"[MK] Raw message: {message}")
        data = json.loads(message)
        temperature = data.get("temperature")
        timestamp = data.get("timestamp")

        logger.info(f"[MK] Processed message: {data}")

        if temperature is None or timestamp is None:
            logger.error(f"[MK] Invalid message: {message}")
            return

        rolling_window.append(temperature)

        # High temperature warning
        if temperature > 300:
            logger.warning(f"[MK] HIGH TEMP ALERT at {timestamp}: {temperature}°F")

        # Stall detection
        if detect_stall(rolling_window):
            logger.info(
                f"[MK] STALL DETECTED at {timestamp}: Temp stable at {temperature}°F over last {window_size} readings."
            )

    except json.JSONDecodeError as e:
        logger.error(f"[MK] JSON decode error: {e}")
    except Exception as e:
        logger.error(f"[MK] Unexpected error: {e}")

#####################################
# Main Function
#####################################

def main() -> None:
    """Start MK Kafka consumer and process messages."""
    logger.info("[MK] START consumer.")
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"[MK] Listening on topic '{topic}' with group ID '{group_id}'")

    rolling_window = deque(maxlen=window_size)
    consumer = create_kafka_consumer(topic, group_id)

    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"[MK] Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, window_size)
    except KeyboardInterrupt:
        logger.warning("[MK] Consumer interrupted.")
    except Exception as e:
        logger.error(f"[MK] Consumer error: {e}")
    finally:
        consumer.close()
        logger.info(f"[MK] Kafka consumer for topic '{topic}' closed.")

#####################################
# Run Main if Executed Directly
#####################################

if __name__ == "__main__":
    main()
