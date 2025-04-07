import base64
import requests
import datetime
import json
from clickhouse_connect import get_client
from keys.keys import API_KEY, PASSWORD, HOST_CLICKHOUSE, CLIENT_NAME, CLIENT_INSTANCE

# Set your desired batch size for ClickHouse inserts
BATCH_SIZE = 500

def parse_datetime(dt_string):
    if not dt_string:
        return None
    try:
        if dt_string.endswith('Z'):
            dt_string = dt_string[:-1] + '+00:00'
        return datetime.datetime.fromisoformat(dt_string)
    except ValueError:
        print(f"Warning: Could not parse datetime: {dt_string}")
        return None

def bool_to_uint8(val):
    return 1 if val else 0

def safe_int(val):
    try:
        return int(val)
    except (ValueError, TypeError):
        return None

def safe_json(val):
    if val is None:
        return None
    try:
        return json.dumps(val)
    except Exception as e:
        print(f"Error converting to JSON: {e}")
        return None

def get_patient_data(patient_obj, session):
    """
    Given a patient object from the appointment and a session,
    fetch full patient details from the API.
    """
    if not patient_obj or not isinstance(patient_obj, dict):
        return None
    link = patient_obj.get("links", {}).get("self")
    if link:
        response = session.get(link)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to get patient details from {link}: {response.status_code}")
    return None

def transform_appointment(appointment, session):
    # Process datetime fields
    appointment_end = parse_datetime(appointment.get("appointment_end"))
    appointment_start = parse_datetime(appointment.get("appointment_start"))
    archived_at = parse_datetime(appointment.get("archived_at"))
    booking_ip_address = appointment.get("booking_ip_address") or None
    cancellation_note = appointment.get("cancellation_note") or None
    cancellation_reason = appointment.get("cancellation_reason") or None
    cancellation_time = parse_datetime(appointment.get("cancellation_time"))
    cancelled_at = parse_datetime(appointment.get("cancelled_at"))
    conflicts = safe_json(appointment.get("conflicts"))
    created_at = parse_datetime(appointment.get("created_at"))
    deleted_at = parse_datetime(appointment.get("deleted_at"))
    did_not_arrive = bool_to_uint8(appointment.get("did_not_arrive", False))
    email_reminder_sent = bool_to_uint8(appointment.get("email_reminder_sent", False))
    ends_at = parse_datetime(appointment.get("ends_at"))
    appointment_id = safe_int(appointment.get("id")) or 0
    invoice_status = safe_int(appointment.get("invoice_status"))
    notes = appointment.get("notes") or ""
    online_booking_policy_accepted = appointment.get("online_booking_policy_accepted")
    if online_booking_policy_accepted is not None:
        online_booking_policy_accepted = bool_to_uint8(online_booking_policy_accepted)
    patient_arrived = bool_to_uint8(appointment.get("patient_arrived", False))
    patient_name = appointment.get("patient_name") or ""
    repeat_rule = safe_json(appointment.get("repeat_rule"))
    repeats = safe_json(appointment.get("repeats"))
    sms_reminder_sent = bool_to_uint8(appointment.get("sms_reminder_sent", False))
    starts_at = parse_datetime(appointment.get("starts_at"))
    treatment_note_status = safe_int(appointment.get("treatment_note_status")) or 0
    updated_at = parse_datetime(appointment.get("updated_at"))
    appointment_type = safe_json(appointment.get("appointment_type"))
    business = safe_json(appointment.get("business"))
    practitioner = safe_json(appointment.get("practitioner"))
    
    # Instead of storing only the patient link, fetch the full patient details
    raw_patient = appointment.get("patient")
    patient_data = get_patient_data(raw_patient, session)
    # Store the full patient data as a JSON string (or fallback to the original link JSON)
    patient = safe_json(patient_data) if patient_data else safe_json(raw_patient)
    
    attendees = safe_json(appointment.get("attendees"))
    invoices = safe_json(appointment.get("invoices"))
    links = safe_json(appointment.get("links"))

    return (
        appointment_end,
        appointment_start,
        archived_at,
        booking_ip_address,
        cancellation_note,
        cancellation_reason,
        cancellation_time,
        cancelled_at,
        conflicts,
        created_at,
        deleted_at,
        did_not_arrive,
        email_reminder_sent,
        ends_at,
        appointment_id,
        invoice_status,
        notes,
        online_booking_policy_accepted,
        patient_arrived,
        patient_name,
        repeat_rule,
        repeats,
        sms_reminder_sent,
        starts_at,
        treatment_note_status,
        updated_at,
        appointment_type,
        business,
        practitioner,
        patient,
        attendees,
        invoices,
        links
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
        CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_{CLIENT_INSTANCE}_cliniko_appointments (
            appointment_end                     DateTime64(3, 'UTC'),
            appointment_start                   DateTime64(3, 'UTC'),
            archived_at                         Nullable(DateTime64(3, 'UTC')),
            booking_ip_address                  Nullable(String),
            cancellation_note                   Nullable(String),
            cancellation_reason                 Nullable(String),
            cancellation_time                   Nullable(DateTime64(3, 'UTC')),
            cancelled_at                        Nullable(DateTime64(3, 'UTC')),
            conflicts                           Nullable(String),
            created_at                          DateTime64(3, 'UTC'),
            deleted_at                          Nullable(DateTime64(3, 'UTC')),
            did_not_arrive                      UInt8,
            email_reminder_sent                 UInt8,
            ends_at                             DateTime64(3, 'UTC'),
            id                                  UInt64,
            invoice_status                      Nullable(Int64),
            notes                               String,
            online_booking_policy_accepted      Nullable(UInt8),
            patient_arrived                     UInt8,
            patient_name                        String,
            repeat_rule                         Nullable(String),
            repeats                             Nullable(String),
            sms_reminder_sent                   UInt8,
            starts_at                           DateTime64(3, 'UTC'),
            treatment_note_status               Int64,
            updated_at                          DateTime64(3, 'UTC'),
            appointment_type                    Nullable(String),
            business                            Nullable(String),
            practitioner                        Nullable(String),
            patient                             Nullable(String),
            attendees                           Nullable(String),
            invoices                            Nullable(String),
            links                               Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY id
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

        appointments = data.get("appointments", [])
        for appointment in appointments:
            row = transform_appointment(appointment, session)
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                client.insert(
                    table=f'{CLIENT_NAME}_{CLIENT_INSTANCE}_cliniko_appointments',
                    data=batch,
                    column_names=[
                        'appointment_end', 'appointment_start', 'archived_at', 'booking_ip_address',
                        'cancellation_note', 'cancellation_reason', 'cancellation_time', 'cancelled_at',
                        'conflicts', 'created_at', 'deleted_at', 'did_not_arrive', 'email_reminder_sent',
                        'ends_at', 'id', 'invoice_status', 'notes', 'online_booking_policy_accepted',
                        'patient_arrived', 'patient_name', 'repeat_rule', 'repeats', 'sms_reminder_sent',
                        'starts_at', 'treatment_note_status', 'updated_at', 'appointment_type', 'business',
                        'practitioner', 'patient', 'attendees', 'invoices', 'links'
                    ]
                )
                print(f"Inserted batch of {len(batch)} rows into ClickHouse.")
                batch = []

        next_url = data.get("links", {}).get("next")
        if next_url:
            print(f"Fetching next page: {next_url}")
        else:
            print("No more pages found.")

    if batch:
        client.insert(
            table=f'{CLIENT_NAME}_{CLIENT_INSTANCE}_cliniko_appointments',
            data=batch,
            column_names=[
                'appointment_end', 'appointment_start', 'archived_at', 'booking_ip_address',
                'cancellation_note', 'cancellation_reason', 'cancellation_time', 'cancelled_at',
                'conflicts', 'created_at', 'deleted_at', 'did_not_arrive', 'email_reminder_sent',
                'ends_at', 'id', 'invoice_status', 'notes', 'online_booking_policy_accepted',
                'patient_arrived', 'patient_name', 'repeat_rule', 'repeats', 'sms_reminder_sent',
                'starts_at', 'treatment_note_status', 'updated_at', 'appointment_type', 'business',
                'practitioner', 'patient', 'attendees', 'invoices', 'links'
            ]
        )
        print(f"Inserted final batch of {len(batch)} rows into ClickHouse.")

if __name__ == "__main__":
    main()
