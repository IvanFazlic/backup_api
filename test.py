import base64
import requests
import datetime
from clickhouse_connect import get_client
from keys.keys import API_KEY, PASSWORD

def main():
    # ---------- Cliniko Setup ----------
    auth_string = f"{API_KEY}:".encode("utf-8")  # API key + colon, no password needed for Cliniko's Basic Auth
    auth_base64 = base64.b64encode(auth_string).decode("utf-8")
    headers = {
        "Authorization": f"Basic {auth_base64}",
        "User-Agent": "MyAwesomeApp (support@myawesomeapp.com)",  # adjust as needed
        "Accept": "application/json",
    }

    # ---------- Fetch Appointment Data from Cliniko ----------
    url = "https://api.au4.cliniko.com/v1/patients/1386591894061983172"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("Error:", response.status_code, response.text)
        raise SystemExit("Failed to fetch data from Cliniko")

    data = response.json()
    print(data)
if __name__ == "__main__":
    main()