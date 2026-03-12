# California Crash Analytics Pipeline

**Author:** Russell Purdy
**Stack:** Python | PostgreSQL | dbt | Tableau | GitHub  
**Dataset:** [California Crash Reporting System (CCRS)](https://data.ca.gov/dataset/ccrs)  

---

## Project Overview

This project demonstrates end-to-end ownership of a data pipeline, from raw extraction through transformation, modeling, and visualization.  
It simulates a consulting engagement: taking fragmented operational data and turning it into a single source of truth for decision-making.

**Goal:** Provide a structured analytics pipeline that enables insights into injury-producing traffic collisions across California counties, highlighting patterns by location and time to support decision-making for policy, public safety, or consulting interventions.

**Project Video:**  
Watch the demo here: [Swans Demo Video](https://1drv.ms/v/c/fc395d0c2c142e6a/IQBTAQN6LB_FQb6BZJ6tQCeuAff7apRBG3HiPc0qw36B43Q?e=4HuHbO)  
*The video is viewable without sign-in and showcases the full pipeline, including Tableau visualizations and data modeling.*
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
