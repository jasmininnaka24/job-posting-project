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

salaries_schema = StructType(fields=[
  StructField('SalID', IntegerType(), False),
  StructField('Salary', StringType(), True),
  StructField('CurrencyType?', StringType(), True),
  StructField('rangeInfo', StringType(), True)
])

# COMMAND ----------

df_salaries = spark.read.format('csv')\
                        .option('header', True)\
                        .schema(salaries_schema)\
                        .load(f'{bronze_folder_path}/{v_file_date}/salaries.csv')

# COMMAND ----------

df_salaries = add_source_file_date(df_salaries, v_source_file)

# COMMAND ----------

df_salaries = add_ingestion_date(df_salaries)

# COMMAND ----------

merge_condition = "trg.SalID = src.SalID"
merge_delta_data_managed(df_salaries, 'bronze', bronze_tables['raw_salaries'], merge_condition, 'ingestion_date')