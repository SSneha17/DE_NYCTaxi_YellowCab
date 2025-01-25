# Databricks notebook source
# MAGIC %md
# MAGIC # Read and Write Data and Create DELTA tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Access

# COMMAND ----------

app_id = "1fe9314f-4b7c-42b0-9389-887358a50105"
dir_id = "4f8ee04c-0ba4-4e10-94ba-e090d891eb69"
secret = (service_credentials)  = "jes8Q~khX~ET0~Xjh~ydkCrxzxElq2lXrqKlKcH8"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.nyctaxistoragedev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxistoragedev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxistoragedev.dfs.core.windows.net", "1fe9314f-4b7c-42b0-9389-887358a50105")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxistoragedev.dfs.core.windows.net","jes8Q~khX~ET0~Xjh~ydkCrxzxElq2lXrqKlKcH8")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxistoragedev.dfs.core.windows.net", "https://login.microsoftonline.com/4f8ee04c-0ba4-4e10-94ba-e090d891eb69/oauth2/token")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Database Creation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE gold
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA ZONE

# COMMAND ----------

# MAGIC %md
# MAGIC Storing variables

# COMMAND ----------

silver = 'abfss://silver@nyctaxistoragedev.dfs.core.windows.net'
gold = 'abfss://gold@nyctaxistoragedev.dfs.core.windows.net'

# COMMAND ----------

data_zone = spark.read.parquet(f'{silver}/trip_zone')
my_schema = data_zone.schema

# COMMAND ----------

df_zone = spark.read.format('parquet')\
  .schema(my_schema)\
    .option('header',True)\
      .load(f'{silver}/trip_zone')
            

# COMMAND ----------

df_zone.display()

# COMMAND ----------

df_zone.write.format('delta').mode('append')\
    .option('path',f'{gold}/trip_zone')\
        .saveAsTable('gold.trip_zone')


# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC Trip Type

# COMMAND ----------

trip_type = spark.read.parquet(f'{silver}/trip_type')
type_schema = trip_type.schema

# COMMAND ----------

df_type = spark.read.format('parquet')\
  .schema(type_schema)\
    .option('header',True)\
      .load(f'{silver}/trip_type')

# COMMAND ----------

df_type.write.format('delta').mode('append')\
    .option('path',f'{gold}/trip_type')\
        .saveAsTable('gold.trip_type')

# COMMAND ----------

df_type.display()

# COMMAND ----------

trips = spark.read.parquet(f'{silver}/trip_data_2024')
trip_schema = trips.schema

df_trips = spark.read.format('parquet')\
  .schema(trip_schema)\
    .option('header',True)\
      .load(f'{silver}/trip_data_2024')


# COMMAND ----------

df_trips.write.format('delta').mode('append')\
    .option('path',f'{gold}/trip_data_2024')\
        .saveAsTable('gold.trips_2024')

# COMMAND ----------

spark.table('gold.trips_2024').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select * from gold.trip_zone
# MAGIC where LocationID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE gold.trip_zone 
# MAGIC SET Borough = 'EMR' 
# MAGIC WHERE LocationID = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Versioning

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from gold.trips_2024 LIMIT 5
