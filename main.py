import base64
import requests
from clickhouse_connect import get_client
from keys.keys import API_KEY, PASSWORD


def main():
    # ---------- Cliniko Setup ----------
    auth_string = f"{API_KEY}:".encode("utf-8")
    auth_base64 = base64.b64encode(auth_string).decode("utf-8")

    headers = {
        "Authorization": f"Basic {auth_base64}",
        "User-Agent": "MyAwesomeApp (support@myawesomeapp.com)",  # adjust to your app
        "Accept": "application/json",
    }

    # ---------- Fetch Data from Cliniko ----------
    response = requests.get("https://api.au4.cliniko.com/v1/appointments", headers=headers)
    if response.status_code != 200:
        print("Error:", response.status_code, response.text)
        raise SystemExit("Failed to fetch data from Cliniko")

    data = response.json()
    print(data)
    # Often Cliniko returns {"users": [...]}; verify your exact JSON structure
    users_list = data.get("users", [])

    # ---------- ClickHouse Setup ----------
    client = get_client(
        host='v7xdgma2dw.asia-southeast1.gcp.clickhouse.cloud',
        username='default',
        password=PASSWORD,
        secure=True
    )

    # Create table if it doesn’t exist (adjust column names/types to match your data)
    client.command("""
        CREATE TABLE IF NOT EXISTS cliniko_users (
            user_id      UInt64,
            first_name   String,
            last_name    String,
            email        String
        ) 
        ENGINE = MergeTree
        ORDER BY user_id
    """)

    # ---------- Prepare Data for Insert ----------
    rows_to_insert = []
    for user in users_list:
        # Match the actual JSON keys from Cliniko
        user_id = user.get("id", 0)
        first_name = user.get("first_name", "")
        last_name = user.get("last_name", "")
        email = user.get("email", "")
        rows_to_insert.append((user_id, first_name, last_name, email))

    # ---------- Insert Data into ClickHouse ----------
    if rows_to_insert:
        # Use client.insert() to cleanly insert your rows
        client.insert(
            table='cliniko_users',
            data=rows_to_insert,
            column_names=['user_id', 'first_name', 'last_name', 'email']
        )
        print(f"Inserted {len(rows_to_insert)} rows into ClickHouse.")
    else:
        print("No user data found to insert.")


if __name__ == "__main__":
    main()
