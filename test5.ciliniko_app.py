import base64
import requests
import datetime
from clickhouse_connect import get_client
from keys.keys import API_KEY, PASSWORD, HOST_CLICKHOUSE, CLIENT_NAME, CLIENT_INSTANCE

BATCH_SIZE = 500  # For batch inserts

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