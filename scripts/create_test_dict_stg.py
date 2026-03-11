#!/usr/bin/env python3
"""
create_test_dict_stg.py

Creates or resets the staging test rules table (stg.stg_test_rules) and loads all rules.
Includes:
- test_id: serial PK
- column_name: column in stg table
- rule: textual description of the test
- data_type: type of the column
- is_active: boolean flag, defaults to TRUE
"""

import os
import sys
from datetime import datetime
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", 5432))
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD") or ""
PG_DB = os.getenv("PG_DB")

TABLE_NAME = "stg_test_rules"
SCHEMA = "stg"

# --- Connect ---
try:
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()
except Exception as e:
    print(f"[ERROR] Could not connect to DB: {e}")
    sys.exit(1)

# --- Drop existing table ---
try:
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
        sql.Identifier(SCHEMA),
        sql.Identifier(TABLE_NAME)
    ))
    print(f"[INFO] Dropped table {SCHEMA}.{TABLE_NAME} if existed")
except Exception as e:
    print(f"[ERROR] Could not drop table: {e}")
    cur.close()
    conn.close()
    sys.exit(1)

# --- Create table ---
try:
    cur.execute(sql.SQL("""
        CREATE TABLE {}.{} (
            test_id SERIAL PRIMARY KEY,
            column_name TEXT NOT NULL,
            rule TEXT NOT NULL,
            data_type TEXT,
            is_active BOOLEAN DEFAULT TRUE
        )
    """).format(sql.Identifier(SCHEMA), sql.Identifier(TABLE_NAME)))
    print(f"[INFO] Table '{SCHEMA}.{TABLE_NAME}' created")
except Exception as e:
    print(f"[ERROR] Could not create table: {e}")
    cur.close()
    conn.close()
    sys.exit(1)

# --- Define rules ---
rules = [
    # null checks first (must run first)
    ("crash_id", "Must not be null", "integer"),
    ("county_code", "Must not be null", "text"),
    ("county_name", "Must not be null", "text"),
    ("city_name", "Must not be null", "text"),
    ("primary_road", "Must not be null", "text"),
    ("collision_type", "Must not be null", "text"),
    ("number_killed", "Must not be null", "integer"),
    ("number_injured", "Must not be null", "integer"),
    ("crash_datetime", "Must not be null", "timestamp"),

    # numeric sanity checks
    ("number_killed", "Must be >= 0", "integer"),
    ("number_injured", "Must be >= 0", "integer"),
]

# --- Insert rules ---
try:
    for col, rule_text, dtype in rules:
        cur.execute(sql.SQL("""
            INSERT INTO {}.{} (column_name, rule, data_type)
            VALUES (%s, %s, %s)
        """).format(sql.Identifier(SCHEMA), sql.Identifier(TABLE_NAME)),
                    [col, rule_text, dtype])
    print(f"[INFO] Inserted {len(rules)} rules into {SCHEMA}.{TABLE_NAME}")
except Exception as e:
    print(f"[ERROR] Could not insert rules: {e}")
    cur.close()
    conn.close()
    sys.exit(1)

cur.close()
conn.close()
print("[SUCCESS] Test rules table ready")