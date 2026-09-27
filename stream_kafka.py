import json
import os
import random
import time
from datetime import datetime, timezone


TOPIC = "tsdb_poc"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVER")
KAFKA_API_KEY = os.environ.get("KAFKA_API_KEY")
KAFKA_API_SECRET = os.environ.get("KAFKA_API_SECRET")
KAFKA_SECURITY_PROTOCOL = "SASL_SSL"
KAFKA_SASL_MECHANISM = "PLAIN"
SENSOR_COUNT = 100
BATCH_SIZE = 1000
INTERVAL_SECONDS = 0.5


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def generate_row(sensor_id):
    return {
        "timestamp": datetime.now(timezone.utc),
        "id": f"sensor-{sensor_id}",
        "value": round(random.uniform(15.0, 35.0), 3),
        "tags": {
            "type": "temperature",
            "location": f"room-{sensor_id % 10}",
            "environment": "test",
        },
    }


def serialize_row(row):
    return json.dumps(row, default=_json_default).encode("utf-8")


def create_producer():
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError(
            "Kafka support is missing. Install it with: pip install kafka-python"
        ) from exc

    producer_kwargs = {
        "bootstrap_servers": BOOTSTRAP_SERVERS.split(","),
        "value_serializer": lambda value: serialize_row(value),
        "key_serializer": lambda key: key.encode("utf-8") if isinstance(key, str) else key,
        "acks": "all",
        "retries": 3,
        "linger_ms": 10,
    }

    if KAFKA_API_KEY and KAFKA_API_SECRET:
        producer_kwargs.update(
            {
                "security_protocol": KAFKA_SECURITY_PROTOCOL,
                "sasl_mechanism": KAFKA_SASL_MECHANISM,
                "sasl_plain_username": KAFKA_API_KEY,
                "sasl_plain_password": KAFKA_API_SECRET,
            }
        )

    return KafkaProducer(**producer_kwargs)


def main():
    producer = create_producer()

    print("Connected to Kafka")
    print(f"Streaming {SENSOR_COUNT} sensors to topic '{TOPIC}'...")

    while True:
        rows = []

        for _ in range(BATCH_SIZE):
            sensor_id = random.randint(1, SENSOR_COUNT)
            rows.append(generate_row(sensor_id))

        for row in rows:
            producer.send(TOPIC, key=row["id"], value=row)

        producer.flush()

        print(
            f"Published {len(rows):,} rows "
            f"({SENSOR_COUNT} sensors) to Kafka topic '{TOPIC}'"
        )

        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
