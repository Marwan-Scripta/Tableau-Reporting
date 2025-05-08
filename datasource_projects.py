import requests
import xml.etree.ElementTree as ET
import csv
import config  # Import configuration values

# import your config values from config.py
# Step 1: Authenticate

# Fetch Tableau credentials
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

# Step 2: Query Metadata API
url = f"{config.TABLEAU_SERVER}api/{config.API_VERSION}/sites/{site_id}/datasources"
headers = {
    'X-Tableau-Auth': token
}

datasource_list = []

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    namespaces = {'t': 'http://tableau.com/api'}
    root = ET.fromstring(response.content)
    datasources = root.findall('.//t:datasource', namespaces)

    for ds in datasources:
        project = ds.find('.//t:project', namespaces)
        project_name = project.attrib['name'] if project is not None else 'Unknown'
        datasource_id = ds.attrib.get('id', 'N/A')
        datasource_name = ds.attrib.get('name', 'N/A')

        datasource_list.append([datasource_id, datasource_name, project_name])

except ET.ParseError as parse_err:
    print(f"Failed to parse XML response: {parse_err}")
    raise Exception(parse_err)
except Exception as err:
    print(f"Other error occurred: {err}")
    raise Exception(err)

# Step 3: Write results to CSV
with open(config.OUTPUT_CSV_PROJECTS, mode='w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Datasource ID", "Datasource Name", "Project Name"])
    writer.writerows(datasource_list)

print(f"✅ Data written to {config.OUTPUT_CSV_PROJECTS}")

# Step 4: Sign out
signout_url = f"{config.TABLEAU_SERVER}/api/{config.API_VERSION}/auth/signout"
signout_response = requests.post(signout_url, headers={"X-Tableau-Auth": token})
if signout_response.status_code == 204:
    print("👋 Signed out of Tableau Cloud.")
