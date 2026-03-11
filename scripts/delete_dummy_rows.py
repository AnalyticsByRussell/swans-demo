#!/usr/bin/env python3
"""
delete_dummy_raw_rows.py

Deletes the 2 demo rows in raw.ca_crashes_json_only after they have been loaded to staging.
Does not touch any system objects or other data.
"""

import os
import json
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# --- Load .env ---
load_dotenv()

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", 5432))
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DB = os.getenv("PG_DB")

# --- Connect ---
conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DB,
    user=PG_USER,
    password=PG_PASSWORD
)
conn.autocommit = True
cur = conn.cursor()

# --- Delete dummy rows by matching unique JSON content ---
dummy_rows = [
    {"County Code": 1, "County Name": "Alameda", "City Name": "Unincorporated"},
    {"County Code": 2, "County Name": "Los Angeles", "City Name": "Los Angeles"}
]

for row in dummy_rows:
    cur.execute(
        """
        DELETE FROM raw.ca_crashes_json_only
        WHERE raw_json @> %s
        """,
        [json.dumps(row)]
    )
    print(f"[INFO] Deleted row matching: {row}")

cur.close()
conn.close()
print("[SUCCESS] Dummy rows removed from raw.ca_crashes_json_only")