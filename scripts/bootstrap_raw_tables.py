#!/usr/bin/env python3
"""
bootstrap_raw_tables.py

Creates all schemas and raw tables in swans_demo db, and inserts 2 demo helper rows of JSON data.
Reads connection details from .env.
"""

import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import json

# --- Load .env ---
load_dotenv()

# --- Read connection details ---
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", 5432))
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DB = os.getenv("PG_DB")

# --- Validate required variables ---
missing = [k for k, v in {"PG_HOST": PG_HOST, "PG_USER": PG_USER, "PG_PASSWORD": PG_PASSWORD, "PG_DB": PG_DB}.items() if not v]
if missing:
    print(f"[ERROR] Missing required environment variables: {', '.join(missing)}")
    sys.exit(1)

# --- Connect to target database ---
try:
    conn = psycopg2.connect(
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )
    conn.autocommit = True
    cur = conn.cursor()
except Exception as e:
    print(f"[ERROR] Could not connect to database '{PG_DB}': {e}")
    sys.exit(1)

# --- Schemas to create ---
schemas = ["fct", "stg", "raw", "marts"]  # add more later as needed

for schema in schemas:
    try:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        print(f"[INFO] Schema '{schema}' ready")
    except Exception as e:
        print(f"[ERROR] Could not create schema '{schema}': {e}")
        cur.close()
        conn.close()
        sys.exit(1)

# --- Create raw.ca_crashes_helpers table ---
try:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.ca_crashes_helpers (
            raw_row_id SERIAL PRIMARY KEY,
            raw_json JSONB NOT NULL
        )
    """)
    print("[INFO] Table 'raw.ca_crashes_helpers' ready")
except Exception as e:
    print(f"[ERROR] Could not create helper table: {e}")
    cur.close()
    conn.close()
    sys.exit(1)

# --- Insert sample helper rows if table is empty ---
cur.execute("SELECT COUNT(*) FROM raw.ca_crashes_helpers")
count = cur.fetchone()[0]

if count == 0:
    sample_data = [
        {
            "County Code": 1,
            "County Name": "Alameda",
            "City Name": "Unincorporated",
            "PrimaryRoad": "I-680 NORTHBOUND",
            "Collision Type Description": "SIDE SWIPE",
            "NumberKilled": 0,
            "NumberInjured": 0,
            "Crash Date Time": "2025-01-17T20:06:00"
        },
        {
            "County Code": 2,
            "County Name": "Los Angeles",
            "City Name": "Los Angeles",
            "PrimaryRoad": "I-405 SOUTHBOUND",
            "Collision Type Description": "REAR END",
            "NumberKilled": 1,
            "NumberInjured": 2,
            "Crash Date Time": "2025-02-01T15:30:00"
        }
    ]

    for row in sample_data:
        cur.execute(
            "INSERT INTO raw.ca_crashes_helpers (raw_json) VALUES (%s)",
            [json.dumps(row)]
        )
    print(f"[INFO] Inserted {len(sample_data)} helper rows into 'raw.ca_crashes_helpers'")
else:
    print(f"[INFO] Table already has {count} helper rows, skipping insert")

cur.close()
conn.close()
print("[SUCCESS] All schemas and raw tables are ready for dbt")