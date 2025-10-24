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

skills_schema = StructType(fields=[
    StructField('job_id', IntegerType(), True),
    StructField('skills', StringType(), False)
])

# COMMAND ----------

df_skills = spark.read.format('json')\
                    .schema(skills_schema)\
                    .option('multiline', True)\
                    .load(f'{bronze_folder_path}/{v_file_date}/skills')

# COMMAND ----------

df_skills = add_source_file_date(df_skills, v_source_file)

# COMMAND ----------

df_skills = add_ingestion_date(df_skills)

# COMMAND ----------

merge_condition = "trg.job_id = src.job_id"
merge_delta_data_managed(df_skills, 'bronze', bronze_tables['raw_skills'], merge_condition, 'ingestion_date')