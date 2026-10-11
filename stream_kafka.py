import json
import os
import random
import time
import math
from datetime import datetime, timezone, timedelta


TOPIC = "tsdb_poc_forecast"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVER")
KAFKA_API_KEY = os.environ.get("KAFKA_API_KEY")
KAFKA_API_SECRET = os.environ.get("KAFKA_API_SECRET")
KAFKA_SECURITY_PROTOCOL = "SASL_SSL"
KAFKA_SASL_MECHANISM = "PLAIN"
SENSOR_COUNT = 100
FORECAST_LENGTH = 100
BATCH_SIZE = 2000
INTERVAL_SECONDS = 0.1
TIMEBAND_VALUES = [
    "TB_A",
    "TB_B",
    "TB_C",
    "TB_D",
    "TB_E",
    "TB_F",
    "TB_G",
    "TB_H",
    "TB_I",
    "TB_J",
    "TB_K",
    "TB_L",
    "TB_M",
    "TB_N",
    "TB_O",
    "TB_P",
    "TB_Q",
    "TB_R",
    "TB_S",
    "TB_T",
    "TB_U",
    "TB_V",
    "TB_W",
    "TB_X",
    "TB_Y",
    "TB_Z",
]

ROOM_COUNT = 10

# UTC date range: October 10 through October 20, 2026, inclusive.
START_DATE = datetime(2026, 10, 10, 0, 0, 0, tzinfo=timezone.utc)
END_DATE_EXCLUSIVE = datetime(2026, 10, 21, 0, 0, 0, tzinfo=timezone.utc)

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

def generate_row_forecast(sensor_id,length):
    now = datetime.now(timezone.utc)
    
    # Auf die nächste Viertelstunde (900 Sekunden) aufrunden
    future_ts = math.ceil(now.timestamp() / 900) * 900
    future_timestamp = datetime.fromtimestamp(future_ts, tz=timezone.utc)
    
    return {
        "timestamp": future_timestamp.isoformat(),
        "as_of": now.isoformat(),
        "id": f"sensor-{sensor_id}",
        "value": [round(random.uniform(15.0, 35.0), 3) for _ in range(length)],
        "tags": {
            "type": "temperature",
            "location": f"room-{sensor_id % 10}",
            "environment": "test",
        },
    }

def iso_utc(value):
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")

def generate_characteristics(room_id,valid_from,valid_until):
    return {
        "characteristic_id": room_id,
        "valid_from": iso_utc(valid_from),
        "valid_until": iso_utc(valid_until),
        "value": random.choice(TIMEBAND_VALUES),
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
    """
    producer = create_producer()

    print("Connected to Kafka")
    print(f"Streaming {SENSOR_COUNT} sensors to topic '{TOPIC}'...")

    room_ids = [f"room-{i}" for i in range(ROOM_COUNT)]
    total_messages = 0
    valid_from = START_DATE

    try:
        while valid_from < END_DATE_EXCLUSIVE:
            valid_until = valid_from + timedelta(hours=1)

            for room_id in room_ids:
                row = generate_characteristics(
                    room_id,
                    valid_from,
                    valid_until,
                )

                producer.send(TOPIC, key=row["characteristic_id"], value=row,)

                total_messages += 1
                print(json.dumps(row))

            # Ensure this hourly batch has been delivered before continuing.
            producer.flush()

            print(
                f"Published window {iso_utc(valid_from)} - "
                f"{iso_utc(valid_until)}"
            )

            valid_from = valid_until

    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        producer.flush()
        producer.close()

    print(f"Finished. Published {total_messages} messages.")

    """
    producer = create_producer()

    print("Connected to Kafka")
    print(f"Streaming {SENSOR_COUNT} sensors to topic '{TOPIC}'...")

    while True:
        rows = []

        for _ in range(BATCH_SIZE):
            sensor_id = random.randint(1, SENSOR_COUNT)
            rows.append(generate_row_forecast(sensor_id, FORECAST_LENGTH))

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
