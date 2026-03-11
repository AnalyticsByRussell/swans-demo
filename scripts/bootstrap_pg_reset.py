#!/usr/bin/env python3
"""
bootstrap_pg_reset.py

Standalone script to drop and recreate the target Postgres database ONLY.
Reads connection details from .env file.
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

# --- Load .env ---
load_dotenv()

# --- Read connection details from .env ---
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

# --- Step 1: Connect to default 'postgres' DB ---
try:
    conn = psycopg2.connect(
        dbname="postgres",
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )
    conn.autocommit = True
    cur = conn.cursor()
except Exception as e:
    print(f"[ERROR] Could not connect to Postgres: {e}")
    sys.exit(1)

# --- Step 2: Drop database if exists ---
try:
    cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{PG_DB}'")
    if cur.fetchone():
        cur.execute(f"DROP DATABASE {PG_DB}")
        print(f"[INFO] Dropped existing database '{PG_DB}'")
    else:
        print(f"[INFO] Database '{PG_DB}' does not exist, skipping drop")
except Exception as e:
    print(f"[ERROR] Could not drop database: {e}")
    cur.close()
    conn.close()
    sys.exit(1)

# --- Step 3: Create fresh database ---
try:
    cur.execute(f"CREATE DATABASE {PG_DB}")
    print(f"[INFO] Created new database '{PG_DB}'")
except Exception as e:
    print(f"[ERROR] Could not create database: {e}")
    cur.close()
    conn.close()
    sys.exit(1)

cur.close()
conn.close()

print(f"[SUCCESS] Database '{PG_DB}' is ready for use.")
print("Next: create schemas and run dbt.")