import json

from stream_kafka import generate_row, serialize_row


def test_generate_row_returns_expected_fields():
    row = generate_row(7)

    assert set(row.keys()) == {"timestamp", "id", "value", "tags"}
    assert row["id"] == "sensor-7"
    assert 15.0 <= row["value"] <= 35.0
    assert row["tags"]["type"] == "temperature"


def test_serialize_row_is_json_serializable():
    row = generate_row(3)
    payload = serialize_row(row)

    assert isinstance(payload, bytes)
    parsed = json.loads(payload.decode("utf-8"))
    assert parsed["id"] == "sensor-3"
    assert parsed["tags"]["location"].startswith("room-")
