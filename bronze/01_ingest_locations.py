# Databricks notebook source
# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../config/table_names

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
v_file_date = dbutils.widgets.get('p_file_date')
v_file_date

# COMMAND ----------

dbutils.widgets.text('p_source_file', '')
v_source_file = dbutils.widgets.get('p_source_file')
v_source_file

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the file

# COMMAND ----------

locations_schema = StructType(fields=[
    StructField('Loc.ID', IntegerType(), True),
    StructField('CityName ', StringType(), False),
    StructField('Region###', StringType(), False),
    StructField('cntry ', StringType(), False)
])

# COMMAND ----------

df_locations = spark.read.format('json')\
                        .schema(locations_schema)\
                        .option('multiline', True)\
                        .load(f'{bronze_folder_path}/{v_file_date}/locations.json')

# COMMAND ----------

df_locations = add_source_file_date(df_locations, v_source_file)

# COMMAND ----------

df_locations = add_ingestion_date(df_locations)

# COMMAND ----------

merge_condition = "trg.`Loc.ID` = src.`Loc.ID`"
merge_delta_data_managed(df_locations, 'bronze', bronze_tables['raw_locations'], merge_condition, 'ingestion_date')