import snowflake.connector
import pandas as pd
import config  # import from config.py

# Connect to Snowflake using values from config

# Fetch Tableau credentials
credentials = config.get_snowflake_credentials()

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


conn = snowflake.connector.connect(
    user=config.SNOWFLAKE_USER,
    password=config.SNOWFLAKE_PASSWORD,
    account=config.SNOWFLAKE_ACCOUNT,
    warehouse=config.SNOWFLAKE_WAREHOUSE,
    role=config.SNOWFLAKE_ROLE,
    database=config.SNOWFLAKE_DATABASE,
    schema=config.SNOWFLAKE_SCHEMA
)

# SQL query - final version (merging user access and Tableau datasource info)
query = """
SELECT DISTINCT 
  DATASOURCE_NAME as "Datasource Name",
  snowflake.USER_NAME, 
  snowflake.auth_method AS AUTHENTICATION_METHOD  
FROM RAW_LAYER.TABLEAU.TABLEAU_SNOWFLAKE_DATASOURCES tableau
LEFT JOIN
(
    WITH user_auth AS (
      SELECT
        USER_NAME,
        FIRST_AUTHENTICATION_FACTOR AS auth_method
      FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
      WHERE USER_NAME ILIKE 'Scripta%' 
        AND USER_NAME NOT ILIKE '%TEST%'
        AND EVENT_TIMESTAMP >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
    )
    SELECT DISTINCT
      qh.USER_NAME,
      qh.DATABASE_NAME,
      ua.auth_method
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
    JOIN user_auth ua
      ON qh.USER_NAME = ua.USER_NAME
    WHERE qh.query_text ILIKE '%tableau%'
      AND qh.USER_NAME ILIKE 'Scripta%'
      AND qh.USER_NAME NOT ILIKE '%TEST%'
      AND qh.DATABASE_NAME NOT ILIKE '%TEST%'
) snowflake
ON tableau.database_name = snowflake.DATABASE_NAME
WHERE DATABASE_CONNECTION_TYPE <> 'hyper' 
  AND tableau.database_name NOT ILIKE '%TEST%'
"""

# Execute and export result
try:
    df = pd.read_sql(query, conn)
    df.to_csv(config.OUTPUT_CSV_SNOWFLAKE_ACCESS, index=False)
    print(f"✅ Data exported to: {config.OUTPUT_CSV_SNOWFLAKE_ACCESS}")
except Exception as e:
    print(f"❌ Error: {e}")
finally:
    conn.close()
