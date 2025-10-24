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

descriptions_schema = StructType(fields=[
    StructField('JobId ', IntegerType(), False),
    StructField('desc_text ', StringType(), True)
])

# COMMAND ----------

df_descriptions = spark.read.format('csv')\
                            .option('header', True)\
                            .schema(descriptions_schema)\
                            .load(f'{bronze_folder_path}/{v_file_date}/descriptions')

# COMMAND ----------

df_descriptions = add_source_file_date(df_descriptions, v_source_file)

# COMMAND ----------

df_descriptions = add_ingestion_date(df_descriptions)

# COMMAND ----------

merge_condition = "trg.JobId = src.JobId"
merge_delta_data_managed(df_descriptions, 'bronze', bronze_tables['raw_descriptions'], merge_condition, 'ingestion_date')