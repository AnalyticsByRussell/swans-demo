import os
import requests
import psycopg2
from dotenv import load_dotenv
from pathlib import Path
import json
from datetime import datetime  # <-- new import

# ---- Load .env ----
dotenv_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=dotenv_path)

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# ---- Start timer ----
start_time = datetime.now()
print(f"[INFO] Fetch job started at {start_time}")

# ---- Postgres connection ----
conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DB,
    user=PG_USER,
    password=PG_PASSWORD
)
cur = conn.cursor()

# ---- Create table ----
cur.execute("""
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.ca_crashes_json_only (
    row_id SERIAL PRIMARY KEY,
    raw_json JSONB NOT NULL
);
""")
conn.commit()

# ---- CKAN API setup ----
BASE_URL = "https://data.ca.gov/api/3/action/datastore_search"
RESOURCE_ID = "9f4fc839-122d-4595-a146-43bc4ed16f46"
LIMIT = 1000
offset = 0
row_counter = 0

while True:
    params = {"resource_id": RESOURCE_ID, "limit": LIMIT, "offset": offset}
    r = requests.get(BASE_URL, params=params)
    r.raise_for_status()
    records = r.json()["result"]["records"]

    if not records:
        break

    for record in records:
        try:
            cur.execute(
                "INSERT INTO raw.ca_crashes_json_only (raw_json) VALUES (%s)",
                [json.dumps(record)]
            )
            row_counter += 1
        except Exception as e:
            print(f"[ERROR] Failed to insert record at offset {offset}: {e}")

    conn.commit()
    offset += LIMIT
    print(f"[INFO] Processed {offset} rows from CKAN, inserted {row_counter} so far...")

cur.close()
conn.close()

# ---- End timer ----
end_time = datetime.now()
duration = end_time - start_time
print(f"[INFO] Fetch job completed at {end_time}")
print(f"[INFO] Total duration: {str(duration).split('.')[0]}")  # hh:mm:ss
print(f"[INFO] Total rows inserted: {row_counter}")