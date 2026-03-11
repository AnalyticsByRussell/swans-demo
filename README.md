# California Crash Analytics Pipeline

**Author:** Russell Purdy
**Stack:** Python | PostgreSQL | dbt | Tableau | GitHub  
**Dataset:** [California Crash Reporting System (CCRS)](https://data.ca.gov/dataset/ccrs)  

---

## Project Overview

This project demonstrates end-to-end ownership of a data pipeline, from raw extraction through transformation, modeling, and visualization.  
It was built to simulate a consulting engagement: taking fragmented operational data and turning it into a single source of truth for decision-making.

**Goal:** Provide a structured analytics pipeline that enables insights into traffic collision patterns across California counties, highlighting areas for intervention and policy planning.

---

## Architecture

```text
California Open Data Portal
        ↓
Python Extraction
        ↓
PostgreSQL Warehouse
    ├── raw
    ├── staging (stg)
    ├── fact (fct)
    └── marts
        ↓
dbt Transformations
        ↓
Analytics-ready Tables
        ↓
Tableau Dashboard
