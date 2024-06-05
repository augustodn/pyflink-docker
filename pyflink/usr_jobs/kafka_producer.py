import json
import random
import time
from datetime import datetime, timezone
from typing import Any
import uuid

from kafka import KafkaProducer

SLEEP_TIME = 0.5

def generate_sensor_data() -> dict[str, Any]:
    """Generates random sensor data. It also adds a timestamp for traceability."""
    sensor_id = random.randint(1, 34)
    temperature = round(random.uniform(15, 35), 2)
    pressure = round(random.uniform(950, 1050), 2)
    vibration = round(random.uniform(0, 10), 2)
    sensor_data = {
        "message_id": uuid.uuid4().hex,
        "sensor_id": sensor_id,
        "message": {
            "temperature": temperature,
            "pressure": pressure,
            "vibration": vibration,
        },
        # utc timestamp
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return sensor_data


def main() -> None:
    """
    Controls the flow of the producer. It first subscribes to the topic and then
    generates sensor data and sends it to the topic.
    """
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    while True:
        sensor_data = generate_sensor_data()
        producer.send("sensors", value=sensor_data)
        print(f"Produced: {sensor_data}")
        time.sleep(SLEEP_TIME)


if __name__ == "__main__":
    main()
