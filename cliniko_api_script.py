import base64
import requests
import datetime
from clickhouse_connect import get_client
from keys.keys import API_KEY, PASSWORD, HOST_CLICKHOUSE, CLIENT_NAME, CLIENT_INSTANCE

BATCH_SIZE = 800  # For batch inserts

# Helper conversion functions
def safe_str(val):
    return str(val) if val is not None else ""

def safe_int(val):
    try:
        return int(val) if val is not None else 0
    except (ValueError, TypeError):
        return 0

def safe_float(val):
    try:
        return float(val) if val is not None else 0.0
    except (ValueError, TypeError):
        return 0.0

def safe_array(val):
    return val if val is not None else []

def parse_datetime(dt_string):
    """
    Parse a datetime string like 2019-08-24T14:15:22Z into a Python datetime (UTC).
    Returns None if the string is invalid or empty.
    """
    if not dt_string:
        return None
    try:
        # Replace 'Z' with '+00:00' for ISO format compatibility
        if dt_string.endswith('Z'):
            dt_string = dt_string[:-1] + '+00:00'
        return datetime.datetime.fromisoformat(dt_string)
    except ValueError:
        print(f"Warning: Could not parse datetime: {dt_string}")
        return None

def bool_to_uint8(value):
    """
    Convert a boolean True/False to 1/0.
    If value is None or not strictly True, return 0.
    """
    return 1 if value is True else 0

def extract_last_segment(url: str) -> str:
    """
    Given a URL, split it by '/' (removing any trailing slash) and return the last segment.
    For example:
      url = "https://api.au1.cliniko.com/v1/patients/1"
      returns "1"
    """
    if not url:
        return ""
    segments = url.rstrip("/").split("/")
    return segments[-1]

# Transformation functions for existing endpoints

def transform_appointment_type(item):
    """
    Transforms appointment type data from Cliniko API.
    """
    return (
        safe_str(item.get("id")),
        safe_str(CLIENT_INSTANCE),
        bool_to_uint8(item.get("add_deposit_to_account_credit")),
        safe_array(item.get("appointment_confirmation_template_ids")),
        safe_array(item.get("appointment_follow_up_template_ids")),
        safe_array(item.get("appointment_reminder_template_ids")),
        parse_datetime(item.get("archived_at")),
        safe_str(item.get("category")),
        safe_str(item.get("color")),
        parse_datetime(item.get("created_at")),
        safe_str(item.get("deposit_price")),
        safe_str(item.get("description")),
        safe_int(item.get("duration_in_minutes")),
        safe_int(item.get("max_attendees")),
        safe_str(item.get("name")),
        safe_int(item.get("online_bookings_lead_time_hours")),
        bool_to_uint8(item.get("online_payments_enabled")),
        safe_str(item.get("online_payments_mode")),
        bool_to_uint8(item.get("show_in_online_bookings")),
        bool_to_uint8(item.get("telehealth_enabled")),
        parse_datetime(item.get("updated_at"))
    )

def transform_booking(item):
    """
    Transforms booking data from Cliniko API.
    """
    repeat_rule = item.get("repeat_rule", {})
    return (
        safe_str(item.get("id")),
        safe_str(CLIENT_INSTANCE),
        parse_datetime(item.get("archived_at")),
        parse_datetime(item.get("created_at")),
        parse_datetime(item.get("deleted_at")),
        parse_datetime(item.get("ends_at")),
        parse_datetime(item.get("starts_at")),
        safe_str(item.get("notes")),
        safe_array(item.get("patient_ids")),
        safe_int(item.get("max_attendees")),
        safe_str(item.get("telehealth_url")),
        parse_datetime(item.get("updated_at")),
        safe_int(repeat_rule.get("number_of_repeats")),
        safe_str(repeat_rule.get("repeat_type")),
        safe_int(repeat_rule.get("repeating_interval"))
    )

def transform_availability_block(item):
    """
    Transforms availability block data from Cliniko API.
    """
    repeat_rule = item.get("repeat_rule", {})
    return (
        safe_str(item.get("id")),
        safe_str(CLIENT_INSTANCE),
        parse_datetime(item.get("created_at")),
        parse_datetime(item.get("ends_at")),
        parse_datetime(item.get("starts_at")),
        parse_datetime(item.get("updated_at")),
        safe_int(repeat_rule.get("number_of_repeats")),
        safe_str(repeat_rule.get("repeat_type")),
        safe_int(repeat_rule.get("repeating_interval"))
    )

def transform_unavailable_block(item):
    """
    Transforms unavailable block data from Cliniko API.
    """
    repeat_rule = item.get("repeat_rule", {})
    return (
        safe_str(item.get("id")),
        safe_str(CLIENT_INSTANCE),
        parse_datetime(item.get("archived_at")),
        parse_datetime(item.get("created_at")),
        parse_datetime(item.get("deleted_at")),
        parse_datetime(item.get("ends_at")),
        safe_str(item.get("notes")),
        parse_datetime(item.get("starts_at")),
        parse_datetime(item.get("updated_at")),
        safe_int(repeat_rule.get("number_of_repeats")),
        safe_str(repeat_rule.get("repeat_type")),
        safe_int(repeat_rule.get("repeating_interval"))
    )

def transform_practitioner(item):
    """
    Transforms practitioner data from Cliniko API.
    """
    return (
        safe_str(item.get("id")),
        safe_str(CLIENT_INSTANCE),
        bool_to_uint8(item.get("active")),
        safe_str(item.get("description")),
        safe_str(item.get("designation")),
        safe_str(item.get("display_name")),
        safe_str(item.get("first_name")),
        safe_str(item.get("label")),
        safe_str(item.get("last_name")),
        bool_to_uint8(item.get("show_in_online_bookings")),
        safe_str(item.get("title")),
        parse_datetime(item.get("created_at")),
        parse_datetime(item.get("updated_at"))
    )

def transform_practitioner_reference_number(item):
    """
    Transforms practitioner reference number data from Cliniko API.
    """
    return (
        safe_str(item.get("id")),
        safe_str(CLIENT_INSTANCE),
        parse_datetime(item.get("created_at")),
        safe_str(item.get("name")),
        safe_str(item.get("reference_number")),
        parse_datetime(item.get("updated_at"))
    )

def transform_invoice(item):
    """
    Transforms invoice data from Cliniko API.
    """
    def as_float(s):
        return safe_float(s)
    return (
        safe_str(item.get("id")),
        safe_str(CLIENT_INSTANCE),
        parse_datetime(item.get("archived_at")),
        parse_datetime(item.get("closed_at")),
        parse_datetime(item.get("created_at")),
        parse_datetime(item.get("deleted_at")),
        as_float(item.get("discounted_amount")),
        as_float(item.get("net_amount")),
        safe_str(item.get("issue_date")),
        safe_int(item.get("number")),
        safe_str(item.get("online_payment_url")),
        safe_str(item.get("notes")),
        safe_int(item.get("status")),
        safe_str(item.get("status_description")),
        as_float(item.get("tax_amount")),
        as_float(item.get("total_amount")),
        parse_datetime(item.get("updated_at"))
    )

def transform_invoice_item(item):
    """
    Transforms invoice item data from Cliniko API.
    """
    def as_float(s):
        return safe_float(s)
    return (
        safe_str(item.get("id")),
        safe_str(CLIENT_INSTANCE),
        parse_datetime(item.get("archived_at")),
        parse_datetime(item.get("created_at")),
        parse_datetime(item.get("deleted_at")),
        safe_str(item.get("code")),
        safe_str(item.get("concession_type_name")),
        as_float(item.get("discounted_amount")),
        safe_str(item.get("name")),
        as_float(item.get("tax_amount")),
        safe_str(item.get("tax_name")),
        as_float(item.get("tax_rate")),
        safe_float(item.get("total_including_tax")),
        as_float(item.get("unit_price")),
        parse_datetime(item.get("updated_at"))
    )

def transform_patient(patient):
    """
    Transforms patient data from Cliniko API.
    """
    return (
        safe_str(patient.get("id")),
        safe_str(CLIENT_INSTANCE),
        bool_to_uint8(patient.get("accepted_email_marketing")),
        bool_to_uint8(patient.get("accepted_privacy_policy")),
        bool_to_uint8(patient.get("accepted_sms_marketing")),
        safe_str(patient.get("address_1")),
        safe_str(patient.get("address_2")),
        safe_str(patient.get("address_3")),
        safe_str(patient.get("appointment_notes")),
        parse_datetime(patient.get("archived_at")),
        safe_str(patient.get("city")),
        parse_datetime(patient.get("created_at")),
        safe_str(patient.get("date_of_birth")),
        safe_str(patient.get("email")),
        safe_str(patient.get("first_name")),
        safe_str(patient.get("last_name")),
        safe_str(patient.get("notes")),
        parse_datetime(patient.get("updated_at"))
    )

def transform_communication(item):
    """
    Transforms communication data from Cliniko API.
    Maps sender and recipient to from_address and to_address.
    """
    from_address = safe_str(item.get("from"))
    to_address = safe_str(item.get("to"))
    comm_type = safe_str(item.get("type"))
    comm_type_code = safe_int(item.get("type_code"))
    return (
        safe_str(item.get("id")),
        safe_str(CLIENT_INSTANCE),
        parse_datetime(item.get("archived_at")),
        safe_str(item.get("category")),
        safe_int(item.get("category_code")),
        bool_to_uint8(item.get("confidential")),
        safe_str(item.get("content")),
        parse_datetime(item.get("created_at")),
        safe_int(item.get("direction_code")),
        safe_str(item.get("direction_description")),
        from_address,
        to_address,
        comm_type,
        comm_type_code,
        parse_datetime(item.get("updated_at"))
    )

def transform_business(item):
    """
    Transforms business data from Cliniko API.
    """
    return (
        safe_str(item.get("id")),
        safe_str(CLIENT_INSTANCE),
        safe_str(item.get("additional_information")),
        safe_str(item.get("additional_invoice_information")),
        safe_str(item.get("address_1")),
        safe_str(item.get("address_2")),
        safe_str(item.get("business_name")),
        safe_str(item.get("business_registration_name")),
        safe_str(item.get("business_registration_value")),
        safe_str(item.get("city")),
        safe_str(item.get("country")),
        parse_datetime(item.get("created_at")),
        parse_datetime(item.get("deleted_at")),
        safe_str(item.get("display_name")),
        safe_str(item.get("email_reply_to")),
        safe_str(item.get("label")),
        safe_str(item.get("post_code")),
        bool_to_uint8(item.get("show_in_online_bookings")),
        safe_str(item.get("state")),
        safe_str(item.get("time_zone")),
        safe_str(item.get("time_zone_identifier")),
        parse_datetime(item.get("updated_at")),
        safe_str(item.get("website_address"))
    )

# --- New Transformation Functions for Individual and Group Appointments ---

def transform_individual_appointment(item):
    """
    Transforms individual appointment data from Cliniko API.
    Extracts IDs from nested URL links for:
      - appointment_type_id from appointment_type->links->self
      - business_id from business->links->self
      - patient_id from patient->links->self
      - practitioner_id from practitioner->links->self
      - repeated_from_id from repeated_from->links->self

    Expected fields (with updated types):
      - appointment_type_id: int64
      - archived_at: date-time
      - business_id: int64
      - cancelled_at: date-time
      - cancellation_reason: int64
      - cancellation_reason_description: string
      - created_at: date-time
      - deleted_at: date-time
      - did_not_arrive: boolean
      - ends_at: date-time
      - id: int64
      - patient_id: int64
      - practitioner_id: int64
      - repeated_from_id: int64
      - starts_at: date-time
      - updated_at: date-time
    """
    appointment_type_url = safe_str(item.get("appointment_type", {}).get("links", {}).get("self", ""))
    business_url = safe_str(item.get("business", {}).get("links", {}).get("self", ""))
    patient_url = safe_str(item.get("patient", {}).get("links", {}).get("self", ""))
    practitioner_url = safe_str(item.get("practitioner", {}).get("links", {}).get("self", ""))
    repeated_from_url = safe_str(item.get("repeated_from", {}).get("links", {}).get("self", ""))
    
    appointment_type_id = safe_int(extract_last_segment(appointment_type_url))
    business_id = safe_int(extract_last_segment(business_url))
    patient_id = safe_int(extract_last_segment(patient_url))
    practitioner_id = safe_int(extract_last_segment(practitioner_url))
    repeated_from_id = safe_int(extract_last_segment(repeated_from_url))
    
    return (
        appointment_type_id,
        parse_datetime(item.get("archived_at")),
        business_id,
        parse_datetime(item.get("cancelled_at")),
        safe_int(item.get("cancellation_reason")),
        safe_str(item.get("cancellation_reason_description")),
        parse_datetime(item.get("created_at")),
        parse_datetime(item.get("deleted_at")),
        bool_to_uint8(item.get("did_not_arrive")),
        parse_datetime(item.get("ends_at")),
        safe_int(item.get("id")),
        patient_id,
        practitioner_id,
        repeated_from_id,
        parse_datetime(item.get("starts_at")),
        parse_datetime(item.get("updated_at"))
    )

def transform_group_appointment(item):
    """
    Transforms group appointment data from Cliniko API.
    Expected fields (with updated types):
      - id: int64
      - archived_at: date-time
      - created_at: date-time
      - updated_at: date-time
      - starts_at: date-time
      - ends_at: date-time
      - notes: string
      - telehealth_url: string
      - max_attendees: int64
    """
    return (
        safe_int(item.get("id")),
        safe_str(CLIENT_INSTANCE),
        parse_datetime(item.get("archived_at")),
        parse_datetime(item.get("created_at")),
        parse_datetime(item.get("updated_at")),
        parse_datetime(item.get("starts_at")),
        parse_datetime(item.get("ends_at")),
        safe_str(item.get("notes")),
        safe_str(item.get("telehealth_url")),
        safe_int(item.get("max_attendees"))
    )

# --- Generic Fetcher Function ---

def fetch_and_insert_data(session, client, base_url, transform_fn, table, columns):
    """
    Generic fetcher that:
    - Uses Cliniko pagination via `links.next`
    - Collects data in batches
    - Inserts into ClickHouse (which uses ReplacingMergeTree to replace duplicates)
    """
    next_url = base_url
    batch = []
    while next_url:
        response = session.get(next_url)
        if response.status_code != 200:
            print("Error:", response.status_code, response.text)
            raise SystemExit("Failed to fetch data from Cliniko")
        data = response.json()
        # Determine the key that contains the array of items.
        top_keys = [k for k in data.keys() if k not in ("links", "total_entries")]
        if not top_keys:
            print(f"No data found in response for {table}.")
            break
        array_key = top_keys[0]
        items = data.get(array_key, [])
        if isinstance(items, dict):
            items = [items]
        for item in items:
            row = transform_fn(item)
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                client.insert(table=table, data=batch, column_names=columns)
                print(f"Inserted batch of {len(batch)} rows into {table}.")
                batch = []
        next_url = data.get("links", {}).get("next")
        if next_url:
            print(f"Fetching next page: {next_url}")
        else:
            print(f"No more pages found for {table}.")
    if batch:
        client.insert(table=table, data=batch, column_names=columns)
        print(f"Inserted final batch of {len(batch)} rows into {table}.")

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

    # ---------- Create Tables in ClickHouse ----------
    # Appointment Types
    client.command(f"""
    CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_appointment_types (
        id                                        String,
        client_instance                           String,
        add_deposit_to_account_credit             UInt8,
        appointment_confirmation_template_ids     Array(String),
        appointment_follow_up_template_ids        Array(String),
        appointment_reminder_template_ids         Array(String),
        archived_at                               Nullable(DateTime64(3, 'UTC')),
        category                                  String,
        color                                     String,
        created_at                                Nullable(DateTime64(3, 'UTC')),
        deposit_price                             String,
        description                               String,
        duration_in_minutes                       UInt32,
        max_attendees                             UInt32,
        name                                      String,
        online_bookings_lead_time_hours           UInt32,
        online_payments_enabled                   UInt8,
        online_payments_mode                      String,
        show_in_online_bookings                   UInt8,
        telehealth_enabled                        UInt8,
        updated_at                                Nullable(DateTime64(3, 'UTC'))
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY id
    """)
    # Bookings
    client.command(f"""
    CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_bookings (
        id                    String,
        client_instance       String,
        archived_at           Nullable(DateTime64(3, 'UTC')),
        created_at            Nullable(DateTime64(3, 'UTC')),
        deleted_at            Nullable(DateTime64(3, 'UTC')),
        ends_at               Nullable(DateTime64(3, 'UTC')),
        starts_at             Nullable(DateTime64(3, 'UTC')),
        notes                 String,
        patient_ids           Array(String),
        max_attendees         UInt32,
        telehealth_url        String,
        updated_at            Nullable(DateTime64(3, 'UTC')),
        repeat_number         UInt32,
        repeat_type           String,
        repeat_interval       UInt32
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY id
    """)
    # Availability Blocks
    client.command(f"""
    CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_availability_blocks (
        id                  String,
        client_instance     String,
        created_at          Nullable(DateTime64(3, 'UTC')),
        ends_at             Nullable(DateTime64(3, 'UTC')),
        starts_at           Nullable(DateTime64(3, 'UTC')),
        updated_at          Nullable(DateTime64(3, 'UTC')),
        repeat_number       UInt32,
        repeat_type         String,
        repeat_interval     UInt32
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY id
    """)
    # Unavailable Blocks
    client.command(f"""
    CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_unavailable_blocks (
        id                String,
        client_instance   String,
        archived_at       Nullable(DateTime64(3, 'UTC')),
        created_at        Nullable(DateTime64(3, 'UTC')),
        deleted_at        Nullable(DateTime64(3, 'UTC')),
        ends_at           Nullable(DateTime64(3, 'UTC')),
        notes             String,
        starts_at         Nullable(DateTime64(3, 'UTC')),
        updated_at        Nullable(DateTime64(3, 'UTC')),
        repeat_number     UInt32,
        repeat_type       String,
        repeat_interval   UInt32
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY id
    """)
    # Practitioners
    client.command(f"""
    CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_practitioners (
        id                      String,
        client_instance         String,
        active                  UInt8,
        description             String,
        designation             String,
        display_name            String,
        first_name              String,
        label                   String,
        last_name               String,
        show_in_online_bookings UInt8,
        title                   String,
        created_at              Nullable(DateTime64(3, 'UTC')),
        updated_at              Nullable(DateTime64(3, 'UTC'))
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY id
    """)
    # Practitioner Reference Numbers
    client.command(f"""
    CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_practitioner_reference_numbers (
        id                String,
        client_instance   String,
        created_at        Nullable(DateTime64(3, 'UTC')),
        name              String,
        reference_number  String,
        updated_at        Nullable(DateTime64(3, 'UTC'))
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY id
    """)
    # Invoices
    client.command(f"""
    CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_invoices (
        id                   String,
        client_instance      String,
        archived_at          Nullable(DateTime64(3, 'UTC')),
        closed_at            Nullable(DateTime64(3, 'UTC')),
        created_at           Nullable(DateTime64(3, 'UTC')),
        deleted_at           Nullable(DateTime64(3, 'UTC')),
        discounted_amount    Float64,
        net_amount           Float64,
        issue_date           String,
        number               Int32,
        online_payment_url   String,
        notes                String,
        status               Int32,
        status_description   String,
        tax_amount           Float64,
        total_amount         Float64,
        updated_at           Nullable(DateTime64(3, 'UTC'))
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY id
    """)
    # Invoice Items
    client.command(f"""
    CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_invoice_items (
        id                     String,
        client_instance        String,
        archived_at            Nullable(DateTime64(3, 'UTC')),
        created_at             Nullable(DateTime64(3, 'UTC')),
        deleted_at             Nullable(DateTime64(3, 'UTC')),
        code                   String,
        concession_type_name   String,
        discounted_amount      Float64,
        name                   String,
        tax_amount             Float64,
        tax_name               String,
        tax_rate               Float64,
        total_including_tax    Float64,
        unit_price             Float64,
        updated_at             Nullable(DateTime64(3, 'UTC'))
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY id
    """)
    # Patients
    client.command(f"""
    CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_patients (
        id                     String,
        client_instance        String,
        accepted_email_marketing  UInt8,
        accepted_privacy_policy   UInt8,
        accepted_sms_marketing    UInt8,
        address_1                 String,
        address_2                 String,
        address_3                 String,
        appointment_notes         String,
        archived_at               Nullable(DateTime64(3, 'UTC')),
        city                      String,
        created_at                Nullable(DateTime64(3, 'UTC')),
        date_of_birth             String,
        email                     String,
        first_name                String,
        last_name                 String,
        notes                     String,
        updated_at                Nullable(DateTime64(3, 'UTC'))
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY id
    """)
    # Communications
    client.command(f"""
    CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_communications (
        id                       String,
        client_instance          String,
        archived_at              Nullable(DateTime64(3, 'UTC')),
        category                 String,
        category_code            UInt32,
        confidential             UInt8,
        content                  String,
        created_at               Nullable(DateTime64(3, 'UTC')),
        direction_code           UInt32,
        direction_description    String,
        from_address             String,
        to_address               String,
        comm_type                String,
        comm_type_code           UInt32,
        updated_at               Nullable(DateTime64(3, 'UTC'))
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY id
    """)
    # Businesses
    client.command(f"""
    CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_businesses (
        id                              String,
        client_instance                 String,
        additional_information          String,
        additional_invoice_information  String,
        address_1                       String,
        address_2                       String,
        business_name                   String,
        business_registration_name      String,
        business_registration_value     String,
        city                            String,
        country                         String,
        created_at                      Nullable(DateTime64(3, 'UTC')),
        deleted_at                      Nullable(DateTime64(3, 'UTC')),
        display_name                    String,
        email_reply_to                  String,
        label                           String,
        post_code                       String,
        show_in_online_bookings         UInt8,
        state                           String,
        time_zone                       String,
        time_zone_identifier            String,
        updated_at                      Nullable(DateTime64(3, 'UTC')),
        website_address                 String
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY id
    """)
    # --- New Tables for Individual and Group Appointments ---
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_appointments (
            appointment_type_id                  Int64,
            archived_at                          Nullable(DateTime64(3, 'UTC')),
            business_id                          Int64,
            cancelled_at                         Nullable(DateTime64(3, 'UTC')),
            cancellation_reason                  Int64,
            cancellation_reason_description      String,
            created_at                           Nullable(DateTime64(3, 'UTC')),
            deleted_at                           Nullable(DateTime64(3, 'UTC')),
            did_not_arrive                       UInt8,
            ends_at                              Nullable(DateTime64(3, 'UTC')),
            id                                   Int64,
            patient_id                           Int64,
            practitioner_id                      Int64,
            repeated_from_id                     Int64,
            starts_at                            Nullable(DateTime64(3, 'UTC')),
            updated_at                           Nullable(DateTime64(3, 'UTC'))
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY id
        """)
    client.command(f"""
    CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_group_appointments (
        id                    String,
        client_instance       String,
        archived_at           Nullable(DateTime64(3, 'UTC')),
        created_at            Nullable(DateTime64(3, 'UTC')),
        updated_at            Nullable(DateTime64(3, 'UTC')),
        starts_at             Nullable(DateTime64(3, 'UTC')),
        ends_at               Nullable(DateTime64(3, 'UTC')),
        notes                 String,
        telehealth_url        String,
        max_attendees         UInt32
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY id
    """)
    
    # ---------- Fetch and Insert Calls ----------
    # Appointment Types
    appointment_types_url = "https://api.au4.cliniko.com/v1/appointment_types"
    appointment_type_cols = [
        "id",
        "client_instance",
        "add_deposit_to_account_credit",
        "appointment_confirmation_template_ids",
        "appointment_follow_up_template_ids",
        "appointment_reminder_template_ids",
        "archived_at",
        "category",
        "color",
        "created_at",
        "deposit_price",
        "description",
        "duration_in_minutes",
        "max_attendees",
        "name",
        "online_bookings_lead_time_hours",
        "online_payments_enabled",
        "online_payments_mode",
        "show_in_online_bookings",
        "telehealth_enabled",
        "updated_at"
    ]
    fetch_and_insert_data(
        session,
        client,
        appointment_types_url,
        transform_appointment_type,
        f"{CLIENT_NAME}_cliniko_appointment_types",
        appointment_type_cols
    )
    # Bookings
    bookings_url = "https://api.au4.cliniko.com/v1/bookings"
    booking_cols = [
        "id",
        "client_instance",
        "archived_at",
        "created_at",
        "deleted_at",
        "ends_at",
        "starts_at",
        "notes",
        "patient_ids",
        "max_attendees",
        "telehealth_url",
        "updated_at",
        "repeat_number",
        "repeat_type",
        "repeat_interval"
    ]
    fetch_and_insert_data(
        session,
        client,
        bookings_url,
        transform_booking,
        f"{CLIENT_NAME}_cliniko_bookings",
        booking_cols
    )
    # Availability Blocks
    availability_blocks_url = "https://api.au4.cliniko.com/v1/availability_blocks"
    availability_block_cols = [
        "id",
        "client_instance",
        "created_at",
        "ends_at",
        "starts_at",
        "updated_at",
        "repeat_number",
        "repeat_type",
        "repeat_interval"
    ]
    fetch_and_insert_data(
        session,
        client,
        availability_blocks_url,
        transform_availability_block,
        f"{CLIENT_NAME}_cliniko_availability_blocks",
        availability_block_cols
    )
    # Unavailable Blocks
    unavailable_blocks_url = "https://api.au4.cliniko.com/v1/unavailable_blocks"
    unavailable_block_cols = [
        "id",
        "client_instance",
        "archived_at",
        "created_at",
        "deleted_at",
        "ends_at",
        "notes",
        "starts_at",
        "updated_at",
        "repeat_number",
        "repeat_type",
        "repeat_interval"
    ]
    fetch_and_insert_data(
        session,
        client,
        unavailable_blocks_url,
        transform_unavailable_block,
        f"{CLIENT_NAME}_cliniko_unavailable_blocks",
        unavailable_block_cols
    )
    # Practitioners
    practitioners_url = "https://api.au4.cliniko.com/v1/practitioners"
    practitioner_cols = [
        "id",
        "client_instance",
        "active",
        "description",
        "designation",
        "display_name",
        "first_name",
        "label",
        "last_name",
        "show_in_online_bookings",
        "title",
        "created_at",
        "updated_at"
    ]
    fetch_and_insert_data(
        session,
        client,
        practitioners_url,
        transform_practitioner,
        f"{CLIENT_NAME}_cliniko_practitioners",
        practitioner_cols
    )
    # Practitioner Reference Numbers
    practitioner_ref_url = "https://api.au4.cliniko.com/v1/practitioner_reference_numbers"
    practitioner_ref_cols = [
        "id",
        "client_instance",
        "created_at",
        "name",
        "reference_number",
        "updated_at"
    ]
    fetch_and_insert_data(
        session,
        client,
        practitioner_ref_url,
        transform_practitioner_reference_number,
        f"{CLIENT_NAME}_cliniko_practitioner_reference_numbers",
        practitioner_ref_cols
    )
    # Invoices
    invoices_url = "https://api.au4.cliniko.com/v1/invoices"
    invoice_cols = [
        "id",
        "client_instance",
        "archived_at",
        "closed_at",
        "created_at",
        "deleted_at",
        "discounted_amount",
        "net_amount",
        "issue_date",
        "number",
        "online_payment_url",
        "notes",
        "status",
        "status_description",
        "tax_amount",
        "total_amount",
        "updated_at"
    ]
    fetch_and_insert_data(
        session,
        client,
        invoices_url,
        transform_invoice,
        f"{CLIENT_NAME}_cliniko_invoices",
        invoice_cols
    )
    # Invoice Items
    invoice_items_url = "https://api.au4.cliniko.com/v1/invoice_items"
    invoice_item_cols = [
        "id",
        "client_instance",
        "archived_at",
        "created_at",
        "deleted_at",
        "code",
        "concession_type_name",
        "discounted_amount",
        "name",
        "tax_amount",
        "tax_name",
        "tax_rate",
        "total_including_tax",
        "unit_price",
        "updated_at"
    ]
    fetch_and_insert_data(
        session,
        client,
        invoice_items_url,
        transform_invoice_item,
        f"{CLIENT_NAME}_cliniko_invoice_items",
        invoice_item_cols
    )
    # Patients
    patients_url = "https://api.au4.cliniko.com/v1/patients"
    patient_cols = [
        "id",
        "client_instance",
        "accepted_email_marketing",
        "accepted_privacy_policy",
        "accepted_sms_marketing",
        "address_1",
        "address_2",
        "address_3",
        "appointment_notes",
        "archived_at",
        "city",
        "created_at",
        "date_of_birth",
        "email",
        "first_name",
        "last_name",
        "notes",
        "updated_at"
    ]
    fetch_and_insert_data(
        session,
        client,
        patients_url,
        transform_patient,
        f"{CLIENT_NAME}_cliniko_patients",
        patient_cols
    )
    # Communications
    communications_url = "https://api.au4.cliniko.com/v1/communications"
    communication_cols = [
        "id",
        "client_instance",
        "archived_at",
        "category",
        "category_code",
        "confidential",
        "content",
        "created_at",
        "direction_code",
        "direction_description",
        "from_address",
        "to_address",
        "comm_type",
        "comm_type_code",
        "updated_at"
    ]
    fetch_and_insert_data(
        session,
        client,
        communications_url,
        transform_communication,
        f"{CLIENT_NAME}_cliniko_communications",
        communication_cols
    )
    # Businesses
    businesses_url = "https://api.au4.cliniko.com/v1/businesses"
    business_cols = [
        "id",
        "client_instance",
        "additional_information",
        "additional_invoice_information",
        "address_1",
        "address_2",
        "business_name",
        "business_registration_name",
        "business_registration_value",
        "city",
        "country",
        "created_at",
        "deleted_at",
        "display_name",
        "email_reply_to",
        "label",
        "post_code",
        "show_in_online_bookings",
        "state",
        "time_zone",
        "time_zone_identifier",
        "updated_at",
        "website_address"
    ]
    fetch_and_insert_data(
        session,
        client,
        businesses_url,
        transform_business,
        f"{CLIENT_NAME}_cliniko_businesses",
        business_cols
    )
    # Appointments (previously Individual Appointments)
    # Note the change in URL from /individual_appointments to /appointments.
    appointments_url = "https://api.au4.cliniko.com/v1/appointments"
    individual_appointment_cols = [
        "appointment_type_id",
        "archived_at",
        "business_id",
        "cancelled_at",
        "cancellation_reason",
        "cancellation_reason_description",
        "created_at",
        "deleted_at",
        "did_not_arrive",
        "ends_at",
        "id",
        "patient_id",
        "practitioner_id",
        "repeated_from_id",
        "starts_at",
        "updated_at"
    ]
    fetch_and_insert_data(
        session,
        client,
        appointments_url,
        transform_individual_appointment,
        f"{CLIENT_NAME}_cliniko_appointments",
        individual_appointment_cols
    )
    # Group Appointments
    group_appointments_url = "https://api.au4.cliniko.com/v1/group_appointments"
    group_appointment_cols = [
        "id",
        "client_instance",
        "archived_at",
        "created_at",
        "updated_at",
        "starts_at",
        "ends_at",
        "notes",
        "telehealth_url",
        "max_attendees"
    ]
    # Uncomment the following lines if you need to fetch group appointments:
    # fetch_and_insert_data(
    #     session,
    #     client,
    #     group_appointments_url,
    #     transform_group_appointment,
    #     f"{CLIENT_NAME}_cliniko_group_appointments",
    #     group_appointment_cols
    # )

if __name__ == "__main__":
    main()
