# scripts/fetch_ca_crashes.py

import os
import requests
import psycopg2
from dotenv import load_dotenv
from pathlib import Path
import json

# ---- Load .env ----
dotenv_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=dotenv_path)

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# ---- Target counties ----
TARGET_COUNTIES = {
    "037": "LOS ANGELES",
    "059": "ORANGE",
    "073": "SAN DIEGO",
    "065": "RIVERSIDE",
    "071": "SAN BERNARDINO"
}

# ---- Postgres connection ----
conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DB,
    user=PG_USER,
    password=PG_PASSWORD
)
cur = conn.cursor()

# ---- Create table if not exists ----
cur.execute("""
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.ca_crashes (
    crash_id TEXT PRIMARY KEY,
    crash_date DATE,
    county TEXT,
    number_of_vehicles INT,
    number_of_injuries INT,
    number_of_fatalities INT,
    commercial_vehicle BOOLEAN,
    dui BOOLEAN,
    raw_json JSONB
);
""")
conn.commit()

# ---- CKAN API setup ----
BASE_URL = "https://data.ca.gov/api/3/action/datastore_search"
RESOURCE_ID = "9f4fc839-122d-4595-a146-43bc4ed16f46"
LIMIT = 1000
offset = 0

while True:
    params = {
        "resource_id": RESOURCE_ID,
        "limit": LIMIT,
        "offset": offset
    }
    r = requests.get(BASE_URL, params=params)
    r.raise_for_status()
    records = r.json()["result"]["records"]

    if not records:
        break

    # Filter and map County_Code to full names
    filtered = []
    for rec in records:
        code = rec.get("County_Code")
        if code in TARGET_COUNTIES:
            rec["county"] = TARGET_COUNTIES[code]
            filtered.append(rec)

    if not filtered:
        offset += LIMIT
        continue

    for record in filtered:
        crash_id = record.get("Collision_ID") or record.get("_id")
        crash_date = record.get("Collision_Date")
        num_vehicles = int(record.get("Number_of_Vehicles_Involved") or 0)
        num_injuries = int(record.get("Number_of_Persons_Injured") or 0)
        num_fatalities = int(record.get("Number_of_Persons_Killed") or 0)
        commercial = str(record.get("Commercial_Vehicle_Involved", "")).lower() == "yes"
        dui = str(record.get("DUI_Involved", "")).lower() == "yes"

        try:
            cur.execute("""
            INSERT INTO raw.ca_crashes (
                crash_id, crash_date, county, number_of_vehicles,
                number_of_injuries, number_of_fatalities,
                commercial_vehicle, dui, raw_json
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (crash_id) DO NOTHING;
            """, (
                crash_id, crash_date, record["county"], num_vehicles,
                num_injuries, num_fatalities, commercial, dui,
                json.dumps(record)
            ))
        except Exception as e:
            print(f"Failed to insert {crash_id}: {e}")

    conn.commit()
    offset += LIMIT
    print(f"Processed {offset} rows from CKAN...")

cur.close()
conn.close()
print("Data fetch complete.")