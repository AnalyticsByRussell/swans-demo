import requests, json, psycopg2
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")

conn = psycopg2.connect("dbname=swans_demo user=your_user password=your_pass host=localhost")
cur = conn.cursor()

# Example JSON ingestion
data = requests.get("https://data.ca.gov/resource/your_dataset.json?$limit=50000").json()

for row in data:
    cur.execute("INSERT INTO raw.ca_crashes (raw_json) VALUES (%s)", [json.dumps(row)])

conn.commit()
cur.close()
conn.close()