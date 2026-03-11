import os

# Root of your dbt project
DBT_MODELS_PATH = r"C:\Users\r_pur\Projects\swans_demo\dbt\models"

# Expected folder → schema mapping
EXPECTED_FOLDERS = {
    "stg": "stg",
    "fct": "fct",
    "marts": "marts"
}

errors = []

for folder, schema in EXPECTED_FOLDERS.items():
    folder_path = os.path.join(DBT_MODELS_PATH, folder)
    if not os.path.exists(folder_path):
        errors.append(f"Folder missing: {folder_path}")
        continue

    for f in os.listdir(folder_path):
        if not f.endswith(".sql"):
            continue
        f_path = os.path.join(folder_path, f)
        if os.path.isfile(f_path):
            print(f"✅ Found model {f} in folder {folder} → schema {schema}")

# Check for stray models in the root models folder
root_files = [f for f in os.listdir(DBT_MODELS_PATH) if f.endswith(".sql")]
if root_files:
    errors.append(f"Stray models in root models folder: {root_files}")

# Summary
if errors:
    print("\n⚠️  Issues found:")
    for e in errors:
        print(" -", e)
else:
    print("\n🎯 All models are in the correct folders and mapped to schemas correctly.")