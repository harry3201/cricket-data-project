# Databricks notebook source
# DBTITLE 1,import libraries
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# DBTITLE 1,Create Catalog ,schema and volumes
spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.cricket_api_data_project")
base_path='/Volumes/workspace/default/cricket_api_data_project'

# COMMAND ----------

# DBTITLE 1,call cricket API
API_KEY='effebc16-e060-4e5b-b5c5-014b583f1e3d'
api_url=f"https://api.cricapi.com/v1/currentMatches?apikey={API_KEY}&offset=0"
response = requests.get(api_url)
response.raise_for_status()
api_data=response.json()
print(json.dumps(api_data,indent=2)[:2000])


# COMMAND ----------

# DBTITLE 1,Saving RAW API response in volumes
raw_file_path=f'{base_path}/current_matches_raw.json'
with open(raw_file_path,'w') as file:
  json.dump(api_data,file)
print("Raw API data is saved in :  ",raw_file_path)


# COMMAND ----------

# DBTITLE 1,CREATE Bronze layer DATAFRAME & TABLE
bronze_data=[{
"source_api":api_url,
"raw_data":json.dumps(api_data),
"ingestion_time":None
}]


bronze_schema=StructType([
StructField("source_api",StringType(),True),
StructField("raw_data",StringType(),True),
StructField("ingestion_time",TimestampType(),True)
])
bronze_df=(spark.createDataFrame(bronze_data,schema=bronze_schema)
    .withColumn("ingestion_time",current_timestamp()))
display(bronze_df)

# COMMAND ----------

# DBTITLE 1,Save the bronze Table
bronze_df.write\
    .format("delta")\
    .mode("append")\
    .saveAsTable("workspace.default.cricket_bronze_current_matches")

print("BRONZE TABLE IS SUCCESSFULLY CREATED")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.default.cricket_bronze_current_matches
# MAGIC

# COMMAND ----------

!cd /Workspace/Users/h.incworks@gmail.com/cricket-api-data-project && DB_GIT_CREDENTIAL_NAME="GitHub 2026-09-08 09:47:26" git push -u origin main