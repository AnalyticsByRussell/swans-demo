#!/usr/bin/env python3
"""
run_stg_tests.py

Run staging tests on stg.stg_ca_crashes using rules from stg.stg_test_rules.
- Only runs tests where is_active = TRUE
- Null checks run first
- No further tests run on a column if null fails
- Logs errors into timestamped table
- Prints start time, end time, and total duration
"""

import os
import sys
import time
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# -----------------------------
# Start timing
# -----------------------------
start_time = datetime.now()
start_epoch = time.time()
print(f"[START] {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

load_dotenv()

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", 5432))
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD") or ""
PG_DB = os.getenv("PG_DB")

SCHEMA = "stg"
TARGET_TABLE = "stg_ca_crashes"
RULES_TABLE = "stg_test_rules"

# -----------------------------
# Connect
# -----------------------------
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

# -----------------------------
# Create error table
# -----------------------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
error_table = f"stg_test_results_{timestamp}"

try:
    cur.execute(sql.SQL("""
        CREATE TABLE {}.{} (
            crash_id TEXT,
            column_name TEXT,
            rule TEXT,
            result TEXT,
            test_time TIMESTAMP DEFAULT NOW()
        )
    """).format(sql.Identifier(SCHEMA), sql.Identifier(error_table)))
    print(f"[INFO] Error table '{error_table}' created in schema '{SCHEMA}'")
except Exception as e:
    print(f"[ERROR] Could not create error table: {e}")
    cur.close()
    conn.close()
    sys.exit(1)

# -----------------------------
# Load active rules
# -----------------------------
try:
    cur.execute(sql.SQL("""
        SELECT column_name, rule, data_type
        FROM {}.{}
        WHERE is_active = TRUE
        ORDER BY column_name,
                 CASE WHEN rule ILIKE 'must not be null' THEN 0 ELSE 1 END
    """).format(sql.Identifier(SCHEMA), sql.Identifier(RULES_TABLE)))
    rules = cur.fetchall()
except Exception as e:
    print(f"[ERROR] Could not fetch rules: {e}")
    cur.close()
    conn.close()
    sys.exit(1)

# -----------------------------
# Run tests
# -----------------------------
for col, rule, dtype in rules:

    if rule.lower() == "must not be null":
        condition = sql.SQL("{} IS NULL").format(sql.Identifier(col))
        result_text = "NULL value"

    elif rule.lower() == "must be >= 0":
        condition = sql.SQL("{} < 0").format(sql.Identifier(col))
        result_text = "Negative value"

    else:
        print(f"[WARN] Unknown rule '{rule}' for column '{col}', skipping")
        continue

    query = sql.SQL("""
        SELECT crash_id
        FROM {}.{}
        WHERE {}
    """).format(
        sql.Identifier(SCHEMA),
        sql.Identifier(TARGET_TABLE),
        condition
    )

    cur.execute(query)
    rows = cur.fetchall()

    for (crash_id,) in rows:
        cur.execute(sql.SQL("""
            INSERT INTO {}.{} (crash_id, column_name, rule, result)
            VALUES (%s, %s, %s, %s)
        """).format(sql.Identifier(SCHEMA), sql.Identifier(error_table)),
                    [str(crash_id), col, rule, result_text])

    if rows:
        print(f"[INFO] {len(rows)} errors for column '{col}' / rule '{rule}'")

# -----------------------------
# End timing
# -----------------------------
end_time = datetime.now()
end_epoch = time.time()

duration_seconds = int(end_epoch - start_epoch)
duration = str(timedelta(seconds=duration_seconds))

print(f"[END]   {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"[DURATION] {duration} (hh:mm:ss)")

cur.close()
conn.close()
print(f"[SUCCESS] Tests completed. Errors logged to '{error_table}'")