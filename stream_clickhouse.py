import os
import random
import time
from datetime import datetime, timezone

import clickhouse_connect

DATABASE = "tsdb_poc"
TABLE = "sensor_data"

SENSOR_COUNT = 100
BATCH_SIZE = 1000
INTERVAL_SECONDS = 0.5


def create_client():
    return clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_SERVER', ''),
        user='default',
        password=os.environ.get('CLICKHOUSE_SECRET', ''),
        secure=True,
    )


def create_table(client):
    client.command(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{TABLE} (
            timestamp DateTime64(3, 'UTC'),
            id String,
            value Float64,
            tags Map(String, String)
        )
        ENGINE = MergeTree()
        ORDER BY (timestamp, id)
        """
    )


def generate_row(sensor_id):
    return [
        datetime.now(timezone.utc),
        f"sensor-{sensor_id}",
        random.uniform(15.0, 35.0),
        {
            "type": "temperature",
            "location": f"room-{sensor_id % 10}",
            "environment": "test",
        },
    ]


def main():
    client = create_client()

    create_table(client)

    print("Connected to ClickHouse")
    print(f"Streaming {SENSOR_COUNT} sensors...")

    while True:
        rows = []

        for _ in range(BATCH_SIZE):
            sensor_id = random.randint(1, SENSOR_COUNT)
            rows.append(generate_row(sensor_id))

        client.insert(
            f"{DATABASE}.{TABLE}",
            rows,
            column_names=[
                "timestamp",
                "id",
                "value",
                "tags",
            ],
        )

        print(
            f"Inserted {len(rows):,} rows "
            f"({SENSOR_COUNT} sensors)"
        )

        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
