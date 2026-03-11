import os
import shutil
from pathlib import Path
from datetime import datetime

# -----------------------------
# CONFIGURATION
# -----------------------------
DBT_ROOT = Path("dbt")
RAW_SCHEMA = "raw"
DBT_PROJECT_NAME = "swans_demo"
PROFILE_NAME = "swans_demo"

# STG/FCT/MARTS models
STG_MODELS = {
    "stg_ca_crashes.sql": f"""
-- STG: CA crashes staging
with raw_data as (
    select *
    from {RAW_SCHEMA}.ca_crashes_json_only
)
select
    value::json->>'collision_date' as collision_date,
    value::json->>'county' as county,
    value::json->>'severity' as severity,
    value::json->>'location' as location
from raw_data
""".strip()
}

FCT_MODELS = {
    "fct_ca_crashes.sql": f"""
-- FCT: CA crashes fact table
select
    collision_date::date as crash_date,
    county,
    count(*) as total_crashes,
    sum(case when severity='Fatal' then 1 else 0 end) as fatal_crashes
from {{ ref('stg_ca_crashes') }}
group by 1,2
""".strip()
}

MARTS_MODELS = {
    "marts_ca_summary.sql": f"""
-- MARTS: CA crash summary
select
    crash_date,
    county,
    total_crashes,
    fatal_crashes
from {{ ref('fct_ca_crashes') }}
order by crash_date desc
""".strip()
}

# Macros
MACROS = {
    "utils.sql": """
-- Utility macros
{% macro count_distinct(column) %}
    count(distinct {{ column }})
{% endmacro %}
""".strip()
}

# Seeds
SEEDS = {
    "counties.csv": "county\nLos Angeles\nSan Diego\nOrange\n"
}

# -----------------------------
# FUNCTIONS
# -----------------------------
def prompt_continue():
    answer = input(
        f"WARNING: This will overwrite the '{DBT_ROOT}' folder if it exists. Continue? [y/N]: "
    )
    return answer.lower() == "y"

def backup_old_dbt():
    if DBT_ROOT.exists():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_folder = DBT_ROOT.parent / f"{DBT_ROOT.name}_old_{ts}"
        shutil.move(str(DBT_ROOT), str(backup_folder))
        print(f"Old dbt folder backed up to: {backup_folder}")

def make_dirs():
    paths = [
        DBT_ROOT / "models" / "stg",
        DBT_ROOT / "models" / "fct",
        DBT_ROOT / "models" / "marts",
        DBT_ROOT / "macros",
        DBT_ROOT / "seeds",
        DBT_ROOT / "tests",
    ]
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)

def write_files():
    # STG
    for fname, content in STG_MODELS.items():
        path = DBT_ROOT / "models" / "stg" / fname
        path.write_text(content)
    # FCT
    for fname, content in FCT_MODELS.items():
        path = DBT_ROOT / "models" / "fct" / fname
        path.write_text(content)
    # MARTS
    for fname, content in MARTS_MODELS.items():
        path = DBT_ROOT / "models" / "marts" / fname
        path.write_text(content)
    # Macros
    for fname, content in MACROS.items():
        path = DBT_ROOT / "macros" / fname
        path.write_text(content)
    # Seeds
    for fname, content in SEEDS.items():
        path = DBT_ROOT / "seeds" / fname
        path.write_text(content)
    # dbt_project.yml
    (DBT_ROOT / "dbt_project.yml").write_text(f"""
name: '{DBT_PROJECT_NAME}'
version: '1.0'
config-version: 2

profile: '{PROFILE_NAME}'

source-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
macro-paths: ["macros"]
data-paths: ["seeds"]
target-path: "target"
clean-targets:
  - "target"
  - "dbt_modules"

models:
  {DBT_PROJECT_NAME}:
    stg:
      +materialized: table
    fct:
      +materialized: table
    marts:
      +materialized: table
""".strip())
    # profiles.yml
    profiles_path = Path.home() / ".dbt" / "profiles.yml"
    profiles_path.parent.mkdir(exist_ok=True)
    profiles_path.write_text(f"""
{PROFILE_NAME}:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{{{ env_var('PG_HOST') }}}}"
      user: "{{{{ env_var('PG_USER') }}}}"
      password: "{{{{ env_var('PG_PASSWORD') }}}}"
      dbname: "{{{{ env_var('PG_DB') }}}}"
      schema: {DBT_PROJECT_NAME}
      port: 5432
""".strip())

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print("=== DBT Bootstrap Script ===")
    if not prompt_continue():
        print("Aborted by user.")
        exit(0)

    backup_old_dbt()
    make_dirs()
    write_files()
    print(f"✅ DBT project created successfully in {DBT_ROOT}")
    print("Set environment variables PG_HOST, PG_USER, PG_PASSWORD, PG_DB, then run:")
    print("  dbt deps")
    print("  dbt seed")
    print("  dbt run")
    print("  dbt test")