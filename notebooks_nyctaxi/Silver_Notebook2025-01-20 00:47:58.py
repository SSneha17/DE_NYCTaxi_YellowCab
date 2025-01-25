# Databricks notebook source
# MAGIC %md
# MAGIC Data Access
# MAGIC

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

# MAGIC %md
# MAGIC ad the container name before @ folled by the storage name

# COMMAND ----------

spark.conf.set("fs.azure.account.key.nyctaxistoragedev.dfs.core.windows.net", "jes8Q~khX~ET0~Xjh~ydkCrxzxElq2lXrqKlKcH8")
dbutils.fs.ls("abfss://bronze@nyctaxistoragedev.dfs.core.windows.net")


# COMMAND ----------

# MAGIC %md
# MAGIC ## # Data Reading
# MAGIC ### ## # Importing Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading CSV data

# COMMAND ----------

df_trip_type = spark.read.format("csv")\
    .option("inferSchema", "true")\
        .option("header", "true")\
            .load("abfss://bronze@nyctaxistoragedev.dfs.core.windows.net/trip_type")

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_zone = spark.read.format("csv")\
    .option("inferSchema", "true")\
        .option("header", "true")\
            .load("abfss://bronze@nyctaxistoragedev.dfs.core.windows.net/trip_zone")
            

# COMMAND ----------

df_trip_zone.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Trip Data
# MAGIC
# MAGIC [To define scheam, 3 options- InferSchema from source, StructType and StructFields, OR DDL ]
# MAGIC
# MAGIC Ex.:  schema ='''
# MAGIC col1 INT, 
# MAGIC col2 FLOAt
# MAGIC '''

# COMMAND ----------

# MAGIC %md
# MAGIC To get the schema from Parquet file

# COMMAND ----------

data = spark.read.parquet("abfss://bronze@nyctaxistoragedev.dfs.core.windows.net/trips_2024_yellowcab/trip-data")
my_schema = data.schema

# COMMAND ----------

df_trip = spark.read.format("parquet")\
    .schema(my_schema)\
        .option("header",True)\
            .option("recursiveFileLookup", True)\
            .load("abfss://bronze@nyctaxistoragedev.dfs.core.windows.net/trips_2024_yellowcab/")
            

# COMMAND ----------

df_trip.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Transformations

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_type = df_trip_type.withColumnRenamed("description", "trip_description")

# COMMAND ----------

# MAGIC %md
# MAGIC 4 types of modes: Append, Overwrite, Error, Ignore

# COMMAND ----------

df_trip_type.write.format('parquet')\
    .mode('append')\
        .option("path", "abfss://silver@nyctaxistoragedev.dfs.core.windows.net/trip_type")\
            .save()

# COMMAND ----------

df_trip_zone.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Few records have two Zones combined, separated by '/'. Let's split the Zones

# COMMAND ----------

df_trip_zone = df_trip_zone.withColumn('Zone1', split(col('Zone'), '/')[0])
df_trip_zone = df_trip_zone.withColumn('Zone2', split(col('Zone'), '/')[1])
df_trip_zone.show()


# COMMAND ----------

df_trip_zone.write.format('parquet')\
    .mode('append')\
        .option("path", "abfss://silver@nyctaxistoragedev.dfs.core.windows.net/trip_zone")\
            .save()

# COMMAND ----------

df_trip.show()

# COMMAND ----------

df_trip = df_trip.withColumn("trip_date", to_date("tpep_pickup_datetime"))\
    .withColumn('trip_year',year('tpep_pickup_datetime'))\
        .withColumn('trip_month',month('tpep_pickup_datetime'))

# COMMAND ----------

df_trip.show()

# COMMAND ----------

df_trip = df_trip.select('VendorID','PULocationID','DOLocationID','trip_distance','fare_amount','total_amount')
df_trip.show()

# COMMAND ----------

df_trip.write.format('parquet')\
    .mode('append')\
        .option("path", "abfss://silver@nyctaxistoragedev.dfs.core.windows.net/trip_data_2024")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC Trip Data Analysis

# COMMAND ----------

display(df_trip)
