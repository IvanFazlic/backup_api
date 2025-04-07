import base64
import requests
import datetime
from clickhouse_connect import get_client
from keys.keys import API_KEY, PASSWORD, HOST_CLICKHOUSE, CLIENT_NAME, CLIENT_INSTANCE

# Set your desired batch size for ClickHouse inserts
BATCH_SIZE = 500

def parse_datetime(dt_string):
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

# ---------------------- Transformation Functions ---------------------- #

def transform_appointment_type(item):
    appointment_type_id = item.get("id")
    try:
        appointment_type_id = int(appointment_type_id)
    except (ValueError, TypeError):
        appointment_type_id = 0
    name = item.get("name", "") or ""
    description = item.get("description", "") or ""
    # Optional: duration (in minutes) if provided
    duration = item.get("duration")
    try:
        duration = int(duration) if duration is not None else None
    except (ValueError, TypeError):
        duration = None
    created_at = parse_datetime(item.get("created_at", ""))
    updated_at = parse_datetime(item.get("updated_at", ""))
    return (
        appointment_type_id,
        str(CLIENT_INSTANCE),
        name,
        description,
        duration,
        created_at,
        updated_at
    )

def transform_booking(item):
    booking_id = item.get("id")
    try:
        booking_id = int(booking_id)
    except (ValueError, TypeError):
        booking_id = 0
    starts_at = parse_datetime(item.get("starts_at", ""))
    ends_at = parse_datetime(item.get("ends_at", ""))
    # Depending on your API, these may be null or missing
    patient_id = item.get("patient_id")
    try:
        patient_id = int(patient_id) if patient_id is not None else None
    except (ValueError, TypeError):
        patient_id = None
    appointment_type_id = item.get("appointment_type_id")
    try:
        appointment_type_id = int(appointment_type_id) if appointment_type_id is not None else None
    except (ValueError, TypeError):
        appointment_type_id = None
    status = item.get("status", "") or ""
    return (
        booking_id,
        str(CLIENT_INSTANCE),
        starts_at,
        ends_at,
        patient_id,
        appointment_type_id,
        status
    )

def transform_availability_block(item):
    block_id = item.get("id")
    try:
        block_id = int(block_id)
    except (ValueError, TypeError):
        block_id = 0
    practitioner_id = item.get("practitioner_id")
    try:
        practitioner_id = int(practitioner_id) if practitioner_id is not None else None
    except (ValueError, TypeError):
        practitioner_id = None
    starts_at = parse_datetime(item.get("starts_at", ""))
    ends_at = parse_datetime(item.get("ends_at", ""))
    return (
        block_id,
        str(CLIENT_INSTANCE),
        practitioner_id,
        starts_at,
        ends_at
    )

def transform_unavailable_block(item):
    # Similar to availability blocks
    block_id = item.get("id")
    try:
        block_id = int(block_id)
    except (ValueError, TypeError):
        block_id = 0
    practitioner_id = item.get("practitioner_id")
    try:
        practitioner_id = int(practitioner_id) if practitioner_id is not None else None
    except (ValueError, TypeError):
        practitioner_id = None
    starts_at = parse_datetime(item.get("starts_at", ""))
    ends_at = parse_datetime(item.get("ends_at", ""))
    return (
        block_id,
        str(CLIENT_INSTANCE),
        practitioner_id,
        starts_at,
        ends_at
    )

def transform_practitioner(item):
    practitioner_id = item.get("id")
    try:
        practitioner_id = int(practitioner_id)
    except (ValueError, TypeError):
        practitioner_id = 0
    first_name = item.get("first_name", "") or ""
    last_name = item.get("last_name", "") or ""
    email = item.get("email", "") or ""
    phone = item.get("phone", "") or ""
    created_at = parse_datetime(item.get("created_at", ""))
    updated_at = parse_datetime(item.get("updated_at", ""))
    return (
        practitioner_id,
        str(CLIENT_INSTANCE),
        first_name,
        last_name,
        email,
        phone,
        created_at,
        updated_at
    )

def transform_practitioner_reference_number(item):
    ref_id = item.get("id")
    try:
        ref_id = int(ref_id)
    except (ValueError, TypeError):
        ref_id = 0
    practitioner_id = item.get("practitioner_id")
    try:
        practitioner_id = int(practitioner_id) if practitioner_id is not None else None
    except (ValueError, TypeError):
        practitioner_id = None
    reference_number = item.get("reference_number", "") or ""
    return (
        ref_id,
        str(CLIENT_INSTANCE),
        practitioner_id,
        reference_number
    )

def transform_invoice(item):
    invoice_id = item.get("id")
    try:
        invoice_id = int(invoice_id)
    except (ValueError, TypeError):
        invoice_id = 0
    issued_date = parse_datetime(item.get("issued_date", ""))
    try:
        amount = float(item.get("amount", 0))
    except (ValueError, TypeError):
        amount = 0.0
    status = str(item.get("status", ""))
    paid = 1 if status.lower() == "paid" else 0
    appointment_id = item.get("appointment_id")
    try:
        appointment_id = int(appointment_id) if appointment_id is not None else None
    except (ValueError, TypeError):
        appointment_id = None
    return (
        invoice_id,
        str(CLIENT_INSTANCE),
        issued_date,
        amount,
        status,
        paid,
        appointment_id
    )

def transform_invoice_item(item):
    invoice_item_id = item.get("id")
    try:
        invoice_item_id = int(invoice_item_id)
    except (ValueError, TypeError):
        invoice_item_id = 0
    invoice_id = item.get("invoice_id")
    try:
        invoice_id = int(invoice_id) if invoice_id is not None else None
    except (ValueError, TypeError):
        invoice_id = None
    description = item.get("description", "") or ""
    try:
        amount = float(item.get("amount", 0))
    except (ValueError, TypeError):
        amount = 0.0
    try:
        quantity = int(item.get("quantity", 1))
    except (ValueError, TypeError):
        quantity = 1
    return (
        invoice_item_id,
        str(CLIENT_INSTANCE),
        invoice_id,
        description,
        amount,
        quantity
    )

def transform_patient(patient):
    patient_id = patient.get("id")
    try:
        patient_id = int(patient_id)
    except (ValueError, TypeError):
        patient_id = 0
    first_name = patient.get("first_name", "") or ""
    last_name = patient.get("last_name", "") or ""
    email = patient.get("email", "") or ""
    created_at = parse_datetime(patient.get("created_at", ""))
    updated_at = parse_datetime(patient.get("updated_at", ""))
    return (
        patient_id,
        str(CLIENT_INSTANCE),
        first_name,
        last_name,
        email,
        created_at,
        updated_at
    )

def transform_communication(item):
    communication_id = item.get("id")
    try:
        communication_id = int(communication_id)
    except (ValueError, TypeError):
        communication_id = 0
    patient_id = item.get("patient_id")
    try:
        patient_id = int(patient_id) if patient_id is not None else None
    except (ValueError, TypeError):
        patient_id = None
    message = item.get("message", "") or ""
    sent_at = parse_datetime(item.get("sent_at", ""))
    return (
        communication_id,
        str(CLIENT_INSTANCE),
        patient_id,
        message,
        sent_at
    )

def transform_payment(item):
    payment_id = item.get("id")
    try:
        payment_id = int(payment_id)
    except (ValueError, TypeError):
        payment_id = 0
    try:
        amount = float(item.get("amount", 0))
    except (ValueError, TypeError):
        amount = 0.0
    paid_at = parse_datetime(item.get("paid_at", ""))
    invoice_id = item.get("invoice_id")
    try:
        invoice_id = int(invoice_id) if invoice_id is not None else None
    except (ValueError, TypeError):
        invoice_id = None
    payment_method = item.get("payment_method", "") or ""
    return (
        payment_id,
        str(CLIENT_INSTANCE),
        amount,
        paid_at,
        invoice_id,
        payment_method
    )

def transform_business(item):
    business_id = item.get("id")
    try:
        business_id = int(business_id)
    except (ValueError, TypeError):
        business_id = 0
    name = item.get("name", "") or ""
    location = item.get("location", "") or ""
    created_at = parse_datetime(item.get("created_at", ""))
    updated_at = parse_datetime(item.get("updated_at", ""))
    return (
        business_id,
        str(CLIENT_INSTANCE),
        name,
        location,
        created_at,
        updated_at
    )

# ---------------------- Fetch and Insert Function ---------------------- #

def fetch_and_insert_data(session, client, base_url, transform_fn, table, columns):
    next_url = base_url
    batch = []
    while next_url:
        response = session.get(next_url)
        if response.status_code != 200:
            print("Error:", response.status_code, response.text)
            raise SystemExit("Failed to fetch data from Cliniko")
        data = response.json()

        # Determine which key holds the list (e.g. "appointments", "patients", etc.)
        key = list(data.keys())[0]
        items = data.get(key, [])
        for item in items:
            row = transform_fn(item)
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                client.insert(
                    table=table,
                    data=batch,
                    column_names=columns
                )
                print(f"Inserted batch of {len(batch)} rows into {table}.")
                batch = []
        next_url = data.get("links", {}).get("next")
        if next_url:
            print(f"Fetching next page: {next_url}")
        else:
            print("No more pages found for", table)
    if batch:
        client.insert(
            table=table,
            data=batch,
            column_names=columns
        )
        print(f"Inserted final batch of {len(batch)} rows into {table}.")

# ---------------------- Main Function ---------------------- #

def main():
    # ---------- Cliniko API Setup ----------
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
            appointment_type_id UInt64,
            client_instance String,
            name              String,
            description       String,
            duration          Nullable(Int64),
            created_at        Nullable(DateTime64(3, 'UTC')),
            updated_at        Nullable(DateTime64(3, 'UTC'))
        )
        ENGINE = MergeTree
        ORDER BY appointment_type_id
    """)

    # Bookings
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_bookings (
            booking_id          UInt64,
            client_instance     String,
            starts_at           Nullable(DateTime64(3, 'UTC')),
            ends_at             Nullable(DateTime64(3, 'UTC')),
            patient_id          Nullable(UInt64),
            appointment_type_id Nullable(UInt64),
            status              String
        )
        ENGINE = MergeTree
        ORDER BY booking_id
    """)

    # Availability Blocks
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_availability_blocks (
            block_id         UInt64,
            client_instance  String,
            practitioner_id  Nullable(UInt64),
            starts_at        Nullable(DateTime64(3, 'UTC')),
            ends_at          Nullable(DateTime64(3, 'UTC'))
        )
        ENGINE = MergeTree
        ORDER BY block_id
    """)

    # Unavailable Blocks
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_unavailable_blocks (
            block_id         UInt64,
            client_instance  String,
            practitioner_id  Nullable(UInt64),
            starts_at        Nullable(DateTime64(3, 'UTC')),
            ends_at          Nullable(DateTime64(3, 'UTC'))
        )
        ENGINE = MergeTree
        ORDER BY block_id
    """)

    # Practitioners
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_practitioners (
            practitioner_id  UInt64,
            client_instance  String,
            first_name       String,
            last_name        String,
            email            String,
            phone            String,
            created_at       Nullable(DateTime64(3, 'UTC')),
            updated_at       Nullable(DateTime64(3, 'UTC'))
        )
        ENGINE = MergeTree
        ORDER BY practitioner_id
    """)

    # Practitioner Reference Numbers
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_practitioner_reference_numbers (
            id               UInt64,
            client_instance  String,
            practitioner_id  Nullable(UInt64),
            reference_number String
        )
        ENGINE = MergeTree
        ORDER BY id
    """)

    # Invoices
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_invoices (
            invoice_id      UInt64,
            client_instance String,
            issued_date     Nullable(DateTime64(3, 'UTC')),
            amount          Float64,
            status          String,
            paid            UInt8,
            appointment_id  Nullable(UInt64)
        )
        ENGINE = MergeTree
        ORDER BY invoice_id
    """)

    # Invoice Items
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_invoice_items (
            invoice_item_id UInt64,
            client_instance String,
            invoice_id      Nullable(UInt64),
            description     String,
            amount          Float64,
            quantity        UInt32
        )
        ENGINE = MergeTree
        ORDER BY invoice_item_id
    """)

    # Patients
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_patients (
            patient_id      UInt64,
            client_instance String,
            first_name      String,
            last_name       String,
            email           String,
            created_at      Nullable(DateTime64(3, 'UTC')),
            updated_at      Nullable(DateTime64(3, 'UTC'))
        )
        ENGINE = MergeTree
        ORDER BY patient_id
    """)

    # Communications
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_communications (
            communication_id UInt64,
            client_instance  String,
            patient_id       Nullable(UInt64),
            message          String,
            sent_at          Nullable(DateTime64(3, 'UTC'))
        )
        ENGINE = MergeTree
        ORDER BY communication_id
    """)

    # Payments
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_payments (
            payment_id      UInt64,
            client_instance String,
            amount          Float64,
            paid_at         Nullable(DateTime64(3, 'UTC')),
            invoice_id      Nullable(UInt64),
            payment_method  String
        )
        ENGINE = MergeTree
        ORDER BY payment_id
    """)

    # Businesses
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CLIENT_NAME}_cliniko_businesses (
            business_id     UInt64,
            client_instance String,
            name            String,
            location        String,
            created_at      Nullable(DateTime64(3, 'UTC')),
            updated_at      Nullable(DateTime64(3, 'UTC'))
        )
        ENGINE = MergeTree
        ORDER BY business_id
    """)

    # ---------- Fetch and Insert Raw Data ----------

    # Appointment Types
    appointment_types_url = "https://api.au4.cliniko.com/v1/appointment_types"
    appointment_type_columns = [
        'appointment_type_id', 'client_instance', 'name', 'description', 'duration', 'created_at', 'updated_at'
    ]
    #fetch_and_insert_data(session, client, appointment_types_url, transform_appointment_type,
    #                      f"{CLIENT_NAME}_cliniko_appointment_types", appointment_type_columns)

    # Bookings
    bookings_url = "https://api.au4.cliniko.com/v1/bookings"
    booking_columns = [
        'booking_id', 'client_instance', 'starts_at', 'ends_at', 'patient_id', 'appointment_type_id', 'status'
    ]
    #fetch_and_insert_data(session, client, bookings_url, transform_booking,
    #                      f"{CLIENT_NAME}_cliniko_bookings", booking_columns)

    # Availability Blocks
    availability_blocks_url = "https://api.au4.cliniko.com/v1/availability_blocks"
    availability_block_columns = [
        'block_id', 'client_instance', 'practitioner_id', 'starts_at', 'ends_at'
    ]
    #fetch_and_insert_data(session, client, availability_blocks_url, transform_availability_block,
    #                      f"{CLIENT_NAME}_cliniko_availability_blocks", availability_block_columns)

    # Unavailable Blocks
    unavailable_blocks_url = "https://api.au4.cliniko.com/v1/unavailable_blocks"
    unavailable_block_columns = [
        'block_id', 'client_instance', 'practitioner_id', 'starts_at', 'ends_at'
    ]
    #fetch_and_insert_data(session, client, unavailable_blocks_url, transform_unavailable_block,
    #                      f"{CLIENT_NAME}_cliniko_unavailable_blocks", unavailable_block_columns)

    # Practitioners
    practitioners_url = "https://api.au4.cliniko.com/v1/practitioners"
    practitioner_columns = [
        'practitioner_id', 'client_instance', 'first_name', 'last_name', 'email', 'phone', 'created_at', 'updated_at'
    ]
    #fetch_and_insert_data(session, client, practitioners_url, transform_practitioner,
    #                      f"{CLIENT_NAME}_cliniko_practitioners", practitioner_columns)

    # Practitioner Reference Numbers
    practitioner_ref_url = "https://api.au4.cliniko.com/v1/practitioner_reference_numbers"
    practitioner_ref_columns = [
        'id', 'client_instance', 'practitioner_id', 'reference_number'
    ]
    #fetch_and_insert_data(session, client, practitioner_ref_url, transform_practitioner_reference_number,
    #                      f"{CLIENT_NAME}_cliniko_practitioner_reference_numbers", practitioner_ref_columns)

    # Invoices
    invoices_url = "https://api.au4.cliniko.com/v1/invoices"
    invoice_columns = [
        'invoice_id', 'client_instance', 'issued_date', 'amount', 'status', 'paid', 'appointment_id'
    ]
    #fetch_and_insert_data(session, client, invoices_url, transform_invoice,
    #                      f"{CLIENT_NAME}_cliniko_invoices", invoice_columns)

    # Invoice Items
    invoice_items_url = "https://api.au4.cliniko.com/v1/invoice_items"
    invoice_item_columns = [
        'invoice_item_id', 'client_instance', 'invoice_id', 'description', 'amount', 'quantity'
    ]
    #fetch_and_insert_data(session, client, invoice_items_url, transform_invoice_item,
    #                      f"{CLIENT_NAME}_cliniko_invoice_items", invoice_item_columns)

    # Patients
    patients_url = "https://api.au4.cliniko.com/v1/patients"
    patient_columns = [
        'patient_id', 'client_instance', 'first_name', 'last_name', 'email', 'created_at', 'updated_at'
    ]
    #fetch_and_insert_data(session, client, patients_url, transform_patient,
    #                      f"{CLIENT_NAME}_cliniko_patients", patient_columns)



    # Payments
    payments_url = "https://api.au4.cliniko.com/v1/payments"
    payment_columns = [
        'payment_id', 'client_instance', 'amount', 'paid_at', 'invoice_id', 'payment_method'
    ]
    fetch_and_insert_data(session, client, payments_url, transform_payment,
                          f"{CLIENT_NAME}_cliniko_payments", payment_columns)

    # Businesses
    businesses_url = "https://api.au4.cliniko.com/v1/businesses"
    business_columns = [
        'business_id', 'client_instance', 'name', 'location', 'created_at', 'updated_at'
    ]
    fetch_and_insert_data(session, client, businesses_url, transform_business,
                          f"{CLIENT_NAME}_cliniko_businesses", business_columns)

if __name__ == "__main__":
    main()
