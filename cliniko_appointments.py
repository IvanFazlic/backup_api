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
    url = "https://api.au4.cliniko.com/v1/appointments"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("Error:", response.status_code, response.text)
        raise SystemExit("Failed to fetch data from Cliniko")

    data = response.json()
    print(data)
    # for d in data['appointments']:
    #     print(d , end="\n----------------")
    # The relevant list of appointments
    # appointments_list = data.get("appointments", [])

    # # ---------- ClickHouse Setup ----------
    # client = get_client(
    #     host='v7xdgma2dw.asia-southeast1.gcp.clickhouse.cloud',
    #     username='default',
    #     password=PASSWORD,
    #     secure=True
    # )

    # # Create the appointments table if it doesn't exist
    # client.command(
    #     """
    #     CREATE TABLE IF NOT EXISTS cliniko_appointments (
    #         appointment_id  UInt64,
    #         starts_at       DateTime64(3, 'UTC'),
    #         ends_at         DateTime64(3, 'UTC'),
    #         patient_name    String,
    #         invoice_status  Nullable(Int64),
    #         did_not_arrive  UInt8,
    #         patient_arrived UInt8,
    #         no_charge       UInt8,
    #         intro_offer     UInt8,
    #         notes           String
    #     )
    #     ENGINE = MergeTree
    #     ORDER BY appointment_id
    #     """
    # )

    # # ---------- Prepare Data for Insert ----------
    # # Define a helper function to parse ISO datetime strings
    # def parse_datetime(dt_string):
    #     if not dt_string:
    #         return datetime.datetime.now()
    #     try:
    #         # Replace 'Z' with '+00:00' for ISO format compatibility
    #         if dt_string.endswith('Z'):
    #             dt_string = dt_string[:-1] + '+00:00'
    #         return datetime.datetime.fromisoformat(dt_string)
    #     except ValueError:
    #         print(f"Warning: Could not parse datetime: {dt_string}")
    #         return datetime.datetime.now()

    # rows = []
    # for appointment in appointments_list:
    #     appointment_id = appointment.get("id")
    #     if isinstance(appointment_id, str):
    #         try:
    #             appointment_id = int(appointment_id)
    #         except ValueError:
    #             appointment_id = 0
        
    #     # Parse date strings into datetime objects
    #     starts_at = parse_datetime(appointment.get("starts_at", ""))
    #     ends_at = parse_datetime(appointment.get("ends_at", ""))
        
    #     # Ensure patient_name is never None, use empty string instead
    #     patient_name = appointment.get("patient_name", "") or ""
    #     invoice_status = appointment.get("invoice_status")
    #     if invoice_status is not None:
    #         try:
    #             invoice_status = int(invoice_status)
    #         except (ValueError, TypeError):
    #             invoice_status = None
        
    #     # Convert booleans to UInt8
    #     did_not_arrive = 1 if appointment.get("did_not_arrive", False) else 0
    #     patient_arrived = 1 if appointment.get("patient_arrived", False) else 0
        
    #     # Process notes
    #     notes = appointment.get("notes", "") or ""
    #     notes_lower = notes.lower()
        
    #     # Flag special conditions
    #     no_charge = 1 if "no charge" in notes_lower or "opening special" in notes_lower else 0
    #     intro_offer = 1 if "intro" in notes_lower else 0
        
    #     # Add the row - ensure all required fields have proper non-None values
    #     row = {
    #         'appointment_id': appointment_id,
    #         'starts_at': starts_at,
    #         'ends_at': ends_at,
    #         'patient_name': patient_name or "",  # Ensure string fields are never None
    #         'invoice_status': invoice_status,  # This can be None because it's defined as Nullable in the schema
    #         'did_not_arrive': did_not_arrive,
    #         'patient_arrived': patient_arrived,
    #         'no_charge': no_charge,
    #         'intro_offer': intro_offer,
    #         'notes': notes or ""  # Ensure string fields are never None
    #     }
    #     rows.append(row)

    # # ---------- Insert Data into ClickHouse ----------
    # if rows:
    #     try:
    #         # Convert the rows to a list of tuples in the exact order of columns
    #         formatted_rows = []
    #         for row in rows:
    #             formatted_row = (
    #                 row['appointment_id'],
    #                 row['starts_at'],
    #                 row['ends_at'],
    #                 row['patient_name'],
    #                 row['invoice_status'],
    #                 row['did_not_arrive'],
    #                 row['patient_arrived'],
    #                 row['no_charge'],
    #                 row['intro_offer'],
    #                 row['notes']
    #             )
    #             formatted_rows.append(formatted_row)
                
    #         # Debug what we're inserting
    #         if formatted_rows:
    #             print("Sample row being inserted:")
    #             for i, col_name in enumerate(['appointment_id', 'starts_at', 'ends_at', 'patient_name', 
    #                                           'invoice_status', 'did_not_arrive', 'patient_arrived', 
    #                                           'no_charge', 'intro_offer', 'notes']):
    #                 print(f"  {col_name}: {formatted_rows[0][i]} ({type(formatted_rows[0][i]).__name__})")
                
    #         # Try inserting with explicitly specified column names and order
    #         client.insert(
    #             table='cliniko_appointments',
    #             data=formatted_rows,
    #             column_names=[
    #                 'appointment_id', 'starts_at', 'ends_at', 'patient_name', 
    #                 'invoice_status', 'did_not_arrive', 'patient_arrived', 
    #                 'no_charge', 'intro_offer', 'notes'
    #             ]
    #         )
    #         print(f"Inserted {len(formatted_rows)} appointment rows into ClickHouse.")
    #     except Exception as e:
    #         print(f"Error inserting data: {e}")
    #         # Print detailed error information
    #         import traceback
    #         traceback.print_exc()
    # else:
    #     print("No appointment data found to insert.")

if __name__ == "__main__":
    main()