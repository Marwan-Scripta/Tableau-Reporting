import boto3
import json

# Output CSV paths
OUTPUT_CSV_PROJECTS = "datasource_projects.csv"
OUTPUT_CSV_TABLES = "datasource_tables.csv"
OUTPUT_CSV_SNOWFLAKE_ACCESS = "snowflake_tableau_user_access.csv"


def get_tableau_credentials():
    secret_name = "prod/tableau"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    client = boto3.client(service_name="secretsmanager", region_name=region_name)

    try:
        # Retrieve the secret value
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response["SecretString"]
        credentials = json.loads(secret)
        return credentials
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise

# Fetch Tableau credentials
credentials = get_tableau_credentials()

# Assign credentials to variables
TABLEAU_SERVER = credentials["TABLEAU_SERVER"]
SITE_ID = credentials["SITE_ID"]
PAT_NAME = credentials["PAT_NAME"]
PAT_SECRET = credentials["PAT_SECRET"]
API_VERSION = 3.24  # Adjust as needed



def get_snowflake_credentials():
    secret_name = "secrets/prod/snowflakeETL"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    client = boto3.client(service_name="secretsmanager", region_name=region_name)

    try:
        # Retrieve the secret value
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response["SecretString"]
        credentials = json.loads(secret)
        return credentials
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise

# Fetch Tableau credentials
credentials = get_snowflake_credentials()

# Assign credentials to variables
SNOWFLAKE_USER = credentials["userid"]
SNOWFLAKE_PASSWORD = credentials["password"]
SNOWFLAKE_ACCOUNT = credentials["host"]
if ".snowflakecomputing.com" in SNOWFLAKE_ACCOUNT:
    SNOWFLAKE_ACCOUNT = SNOWFLAKE_ACCOUNT.split(".snowflakecomputing.com")[0]
else:
    SNOWFLAKE_ACCOUNT

SNOWFLAKE_WAREHOUSE = credentials["warehouse"]
SNOWFLAKE_ROLE = credentials["role"]
SNOWFLAKE_DATABASE = "RAW_LAYER"
SNOWFLAKE_SCHEMA = "TABLEAU"


