import base64
import requests
import datetime
from clickhouse_connect import get_client
from keys.keys import API_KEY, PASSWORD, HOST_CLICKHOUSE, CLIENT_NAME, CLIENT_INSTANCE

# Set your desired batch size for ClickHouse inserts
BATCH_SIZE = 500

def parse_datetime(dt_string):
    if not dt_string:
        return datetime.datetime.now()
    try:
        # Replace 'Z' with '+00:00' for ISO format compatibility
        if dt_string.endswith('Z'):
            dt_string = dt_string[:-1] + '+00:00'
        return datetime.datetime.fromisoformat(dt_string)
    except ValueError:
        print(f"Warning: Could not parse datetime: {dt_string}")
        return datetime.datetime.now()

def transform_appointment(appointment):
    # Process appointment ID
    appointment_id = appointment.get("id")
    if isinstance(appointment_id, str):
        try:
            appointment_id = int(appointment_id)
        except ValueError:
            appointment_id = 0

    # Parse date strings into datetime objects
    starts_at = parse_datetime(appointment.get("starts_at", ""))
    ends_at = parse_datetime(appointment.get("ends_at", ""))
    
    # Ensure patient_name is never None
    patient_name = appointment.get("patient_name", "") or ""
    
    # Extract role, default to empty string if not provided
    role = appointment.get("role", "") or ""
    
    invoice_status = appointment.get("invoice_status")
    if invoice_status is not None:
        try:
            invoice_status = int(invoice_status)
        except (ValueError, TypeError):
            invoice_status = None
    
    # Convert booleans to UInt8
    did_not_arrive = 1 if appointment.get("did_not_arrive", False) else 0
    patient_arrived = 1 if appointment.get("patient_arrived", False) else 0
    
    # Process notes and flag special conditions
    notes = appointment.get("notes", "") or ""
    notes_lower = notes.lower()
    no_charge = 1 if "no charge" in notes_lower or "opening special" in notes_lower else 0
    intro_offer = 1 if "intro" in notes_lower else 0

    return (
        appointment_id,
        starts_at,
        ends_at,
        patient_name,
        role,
        invoice_status,
        did_not_arrive,
        patient_arrived,
        no_charge,
        intro_offer,
        notes
    )

def main():
    # ---------- Cliniko Setup ----------
    auth_string = f"{API_KEY}:".encode("utf-8")
    auth_base64 = base64.b64encode(auth_string).decode("utf-8")
    headers = {
        "Authorization": f"Basic {auth_base64}",
        "User-Agent": "MyAwesomeApp (support@myawesomeapp.com)",
        "Accept": "application/json",
    }
    session = requests.Session()
    session.headers.update(headers)

    # ---------- ClickHouse Setup ----------
    client = get_client(
        host=HOST_CLICKHOUSE,
        username='default',
        password=PASSWORD,
        secure=True
    )
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_appointments (
            appointment_id  UInt64,
            starts_at       DateTime64(3, 'UTC'),
            ends_at         DateTime64(3, 'UTC'),
            {CLIENT_INSTANCE} String
            patient_name    String,
            role            String,
            invoice_status  Nullable(Int64),
            did_not_arrive  UInt8,
            patient_arrived UInt8,
            no_charge       UInt8,
            intro_offer     UInt8,
            notes           String
        )
        ENGINE = MergeTree
        ORDER BY appointment_id
        """
    )

    # ---------- Fetch and Process Data with Pagination and Batching ----------
    base_url = "https://api.au4.cliniko.com/v1/appointments"
    next_url = base_url
    batch = []

    while next_url:
        response = session.get(next_url)
        if response.status_code != 200:
            print("Error:", response.status_code, response.text)
            raise SystemExit("Failed to fetch data from Cliniko")
        data = response.json()

        # Process appointments from the current page
        appointments = data.get("appointments", [])
        for appointment in appointments:
            row = transform_appointment(appointment)
            batch.append(row)
            # Insert the batch when the batch size limit is reached
            if len(batch) >= BATCH_SIZE:
                client.insert(
                    table=f'{CLIENT_NAME}_cliniko_appointments',
                    data=batch,
                    column_names=[
                        'appointment_id', 'starts_at', 'ends_at',{CLIENT_INSTANCE}, 'patient_name', 'role',
                        'invoice_status', 'did_not_arrive', 'patient_arrived', 
                        'no_charge', 'intro_offer', 'notes'
                    ]
                )
                print(f"Inserted batch of {len(batch)} rows into ClickHouse.")
                batch = []

        # Get next page URL
        next_url = data.get("links", {}).get("next")
        if next_url:
            print(f"Fetching next page: {next_url}")
        else:
            print("No more pages found.")

    # Insert any remaining rows in the final batch
    if batch:
        client.insert(
            table=f'{CLIENT_NAME}_cliniko_appointments',
            data=batch,
            column_names=[
                'appointment_id', 'starts_at', 'ends_at',{CLIENT_INSTANCE}, 'patient_name', 'role',
                'invoice_status', 'did_not_arrive', 'patient_arrived', 
                'no_charge', 'intro_offer', 'notes'
            ]
        )
        print(f"Inserted final batch of {len(batch)} rows into ClickHouse.")

if __name__ == "__main__":
    main()
