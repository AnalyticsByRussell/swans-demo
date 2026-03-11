"""
===========================================================
Postgres Dev DB Schema Cleanup Script
===========================================================

Purpose:
--------
- resets dev without touching system schemas or raw, to avoid re-fetching the data.
-

Behavior:
---------
1. Loads PostgreSQL connection credentials from a `.env` file:
   - PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

2. Connects to the target database using psycopg2.

3. Identifies all schemas in the database that:
   - Do NOT start with 'pg_' (Postgres system schemas)
   - Are NOT 'information_schema'
   - Are NOT listed in PRESERVE_SCHEMAS (default ['raw'])

4. Loops through the resulting list of schemas and drops each one:
   - Uses CASCADE to remove all tables, views, sequences, and other
     objects contained within the schema
   - Uses safe SQL composition to prevent SQL injection

5. Prints each schema being dropped and a final summary of preserved schemas.

Safety Notes:
-------------
- System schemas (pg_catalog, pg_toast, etc.) are never touched.
- Preserved schemas in PRESERVE_SCHEMAS are never dropped.
- All objects within dropped schemas are permanently deleted.
- The script is intended for **development/testing environments only**.

Usage:
------
- Make sure the `.env` file contains valid connection parameters.
- Run the script in a Python environment with psycopg2 installed.
- Example:
      python pg_reset_schemas.py

===========================================================
"""
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql

load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD") or ""

PRESERVE_SCHEMAS = ["raw"]  # schemas to keep

def main():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        # Get all user schemas except system and preserved
        placeholders = sql.SQL(', ').join(sql.Literal(s) for s in PRESERVE_SCHEMAS)
        query = sql.SQL("""
            SELECT nspname
            FROM pg_namespace
            WHERE nspname NOT LIKE 'pg_%'
              AND nspname <> 'information_schema'
              AND nspname NOT IN ({preserve})
        """).format(preserve=placeholders)

        cur.execute(query)
        schemas = [row[0] for row in cur.fetchall()]

        for schema in schemas:
            print(f"Dropping schema: {schema}")
            cur.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))

    print("Done. Preserved schemas:", ", ".join(PRESERVE_SCHEMAS))
    conn.close()

if __name__ == "__main__":
    main()