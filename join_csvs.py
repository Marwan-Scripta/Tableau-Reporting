import pandas as pd

# 🔧 Input CSVs
CSV_1 = "datasource_projects.csv"  # From REST API (includes Project Name)
CSV_2 = "datasource_tables.csv"    # From GraphQL Metadata API
CSV_3 = "snowflake_tableau_user_access.csv"  # From Snowflake query

# 🔧 Output CSV
OUTPUT_CSV = "Datasource_breakdown_by_snowflake_user.csv"

# 🔄 Read all CSVs
df1 = pd.read_csv(CSV_1)
df2 = pd.read_csv(CSV_2)
df3 = pd.read_csv(CSV_3)

# 🔗 Perform joins
joined_df = pd.merge(df1, df2, on=["Datasource Name"], how="inner")
final_df = pd.merge(joined_df, df3, on=["Datasource Name"], how="inner")

# 🧼 Select and reorder columns
final_df = final_df[[
    "Project Name",
    "Datasource Name",
    "Database Name",
    "Table Schema",
    "Table Name",
    "Database Connection Type",
    "USER_NAME",
    "AUTHENTICATION_METHOD"
]]

# ✅ Drop duplicate rows (ensuring uniqueness)
final_df = final_df.drop_duplicates()

# 💾 Save the result
final_df.to_csv(OUTPUT_CSV, index=False)
print(f"✅ Filtered and joined CSV saved as: {OUTPUT_CSV}")
