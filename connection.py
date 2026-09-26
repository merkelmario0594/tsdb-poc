import os

import clickhouse_connect

if __name__ == '__main__':
    client = clickhouse_connect.get_client(
        host='z287e7mm2l.eu-central-1.aws.clickhouse.cloud',
        user='default',
        password=os.environ.get('CLICKHOUSE_SECRET', ''),
        secure=True
    )
    print("Result:", client.query("SELECT 1").result_set[0][0])