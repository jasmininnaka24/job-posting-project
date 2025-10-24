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
from pyspark.sql import functions, Window
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the file

# COMMAND ----------

companies_schema = StructType(fields=[
    StructField('Comp.ID', IntegerType(), False),
    StructField('Indus_Try', StringType(), True),
    StructField('Size__', StringType(), True),
    StructField('cmpnyName ', StringType(), True),
    StructField('founded_YR', StringType(), True)
])

# COMMAND ----------

df_companies = spark.read.format('json')\
                        .schema(companies_schema)\
                        .option('multiline', True)\
                        .load(f'{bronze_folder_path}/{v_file_date}/companies.json')

# COMMAND ----------

df_companies = add_source_file_date(df_companies, v_source_file)

# COMMAND ----------

df_companies = add_ingestion_date(df_companies)

# COMMAND ----------

merge_condition = "trg.`Comp.ID` = src.`Comp.ID`"
merge_delta_data_managed(df_companies, 'bronze', bronze_tables['raw_companies'], merge_condition, 'ingestion_date')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM job_posting_catalog.bronze.raw_companies;

# COMMAND ----------

