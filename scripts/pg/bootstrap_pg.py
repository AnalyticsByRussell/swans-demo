#!/usr/bin/env python3
"""
bootstrap_pg.py

Drop and recreate the 'swans' database and layered schemas for dbt.
Reads Postgres connection info from environment variables.
"""

import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# --- Load environment variables ---
load_dotenv()

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", 5432))
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DB = os.getenv("PG_DB", "swans")  # Target DB name

# --- Validate required variables ---
missing = [k for k, v in {"PG_USER": PG_USER, "PG_PASSWORD": PG_PASSWORD}.items() if not v]
if missing:
    print(f"[ERROR] Missing required environment variables: {', '.join(missing)}")
    sys.exit(1)


# --- Function to drop & create the database ---
def recreate_database(dbname=None):
    dbname = dbname or PG_DB
    try:
        conn = psycopg2.connect(dbname="postgres", user=PG_USER, password=PG_PASSWORD,
                                host=PG_HOST, port=PG_PORT)
        conn.autocommit = True
        cur = conn.cursor()

        # Drop database if exists
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(dbname)))
        print(f"[INFO] Database '{dbname}' dropped (if existed).")

        # Create database
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        print(f"[INFO] Database '{dbname}' created.")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERROR] Failed to drop/create database '{dbname}': {e}")
        sys.exit(1)


# --- Function to create schemas ---
def create_schemas(dbname=PG_DB):
    """Create layered schemas: stg, fct, marts, raw."""
    try:
        conn = psycopg2.connect(dbname=dbname, user=PG_USER, password=PG_PASSWORD,
                                host=PG_HOST, port=PG_PORT)
        conn.autocommit = True
        cur = conn.cursor()

        schemas = ["stg", "fct", "marts", "raw"]
        for s in schemas:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(s)))
            print(f"[INFO] Schema '{s}' created or already exists.")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERROR] Failed to create schemas in '{dbname}': {e}")
        sys.exit(1)


# --- Main ---
if __name__ == "__main__":
    recreate_database()  # <- drops & creates fresh DB
    create_schemas()
    print(f"[SUCCESS] Database '{PG_DB}' dropped, recreated, and schemas ready for dbt.")