import clickhouse_connect

if __name__ == '__main__':
    client = clickhouse_connect.get_client(
        host='v7xdgma2dw.asia-southeast1.gcp.clickhouse.cloud',
        user='default',
        password='UoU_InFIzwWX3',
        secure=True
    )
    print("Result:", client.query("SELECT 1").result_set[0][0])