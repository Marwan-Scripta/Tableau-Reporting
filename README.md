# Tableau Data Source Joiner

This Python script joins two Tableau metadata CSV exports — one from the REST API (project and datasource info) and one from the GraphQL Metadata API (schema, table, and database info). The result is a merged file with combined details for easier analysis.

You must first run the following scripts in order for the script that joins the two csvs to work:

1- datasource_projects.py
2- datasource_tables.py
3- snowflake_users_stats.py
4- join_csvs.py

---

## 📁 Files

- `join_csvs.py` — Main script that joins the two CSVs
- `datasource_projects.csv` — CSV from REST API (must include `Datasource Name`, `Datasource ID`, `Project Name`)
- `datasource_tables.csv` — CSV from GraphQL Metadata API (must include `Datasource Name`, `Datasource ID`, `Table Schema`, `Table Name`, `Database Name`, `Database Connection Type`)
- `snowflake_users_stats.csv` — CSV from REST API (must include `Datasource Name`, `Datasource ID`, `Snowflake User`, `Authentication Method`, `Database Name`, `Database Connection Type`)
- `joined_datasource_output.csv` — Output file generated by the script

---

## ✅ Output Format

The script produces a CSV with the following columns:

- `Project Name`
- `Datasource Name`
- `Database Name`
- `Table Schema`
- `Table Name`
- `Database Connection Type`

---

## 🚀 How to Use

### 1. Install Dependencies

Make sure Python 3 is installed, then run:

```bash
pip install -r requirements.txt
