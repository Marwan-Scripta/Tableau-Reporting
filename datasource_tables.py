import requests
import xml.etree.ElementTree as ET
import csv
import config  # import your config values from config.py

# Step 1: Authenticate
credentials = config.get_tableau_credentials()

# Assign credentials to variables
TABLEAU_SERVER = credentials["TABLEAU_SERVER"]
SITE_ID = credentials["SITE_ID"]
PAT_NAME = credentials["PAT_NAME"]
PAT_SECRET = credentials["PAT_SECRET"]
API_VERSION = 3.24  # Adjust as needed


auth_url = f"{config.TABLEAU_SERVER}api/{config.API_VERSION}/auth/signin"
auth_payload = {
    "credentials": {
        "personalAccessTokenName": config.PAT_NAME,
        "personalAccessTokenSecret": config.PAT_SECRET,
        "site": {"contentUrl": config.SITE_ID}
    }
}
auth_response = requests.post(auth_url, json=auth_payload)
auth_response.raise_for_status()

root = ET.fromstring(auth_response.text)
ns = {'t': 'http://tableau.com/api'}
token = root.find('.//t:credentials', ns).attrib['token']
site_id = root.find('.//t:site', ns).attrib['id']

print("✅ Logged in successfully")

# Step 2: Query Metadata API via GraphQL
graphql_url = f"{config.TABLEAU_SERVER}api/metadata/graphql"
headers = {
    "X-Tableau-Auth": token,
    "Content-Type": "application/json"
}

query = """
{
  datasources {
    name
    id
    upstreamTables {
      schema
      name
      database {
        name
        connectionType
      }
    }
  }
}
"""

graphql_response = requests.post(graphql_url, headers=headers, json={"query": query})
graphql_response.raise_for_status()
response_json = graphql_response.json()

datasources = response_json.get("data", {}).get("datasources", [])

# Step 3: Write to CSV
with open(config.OUTPUT_CSV_TABLES, mode='w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=[
        "Datasource Name",
        "Datasource ID",
        "Table Schema",
        "Table Name",
        "Database Name",
        "Database Connection Type"
    ])
    writer.writeheader()

    for ds in datasources:
        ds_name = ds.get("name", "")
        ds_id = ds.get("id", "")
        for table in ds.get("upstreamTables", []):
            db = table.get("database") or {}
            writer.writerow({
                "Datasource Name": ds_name,
                "Datasource ID": ds_id,
                "Table Schema": table.get("schema", ""),
                "Table Name": table.get("name", ""),
                "Database Name": db.get("name", ""),
                "Database Connection Type": db.get("connectionType", "")
            })

print(f"✅ Raw upstream data exported to: {config.OUTPUT_CSV_TABLES}")

# Step 4: Sign out
signout_url = f"{config.TABLEAU_SERVER}api/{config.API_VERSION}/auth/signout"
signout_response = requests.post(signout_url, headers={"X-Tableau-Auth": token})
if signout_response.status_code == 204:
    print("👋 Signed out of Tableau Cloud.")
