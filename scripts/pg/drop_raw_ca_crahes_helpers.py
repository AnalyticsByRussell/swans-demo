#!/usr/bin/env python3
"""
drop_raw_ca_crashes_helpers.py

Drops the raw.ca_crashes_helpers table.
"""

import os
import sys
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

# --- Connect to DB ---
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

# --- Drop the table ---
try:
    cur.execute(sql.SQL("DROP TABLE IF EXISTS raw.ca_crashes_helpers;"))
    print("[INFO] Table 'raw.ca_crashes_helpers' dropped (if it existed).")
except Exception as e:
    print(f"[ERROR] Could not drop table: {e}")
finally:
    cur.close()
    conn.close()