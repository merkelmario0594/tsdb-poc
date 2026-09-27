import os

api_key = os.environ.get("CLICKHOUSE_SECRET")
if api_key is None:
    raise RuntimeError("Secret MY_SECRET is not set")

print(api_key)