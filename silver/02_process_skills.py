# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %run ../config/table_names

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the data

# COMMAND ----------

df_skills = spark.sql(f"SELECT * FROM job_posting_catalog.bronze.{bronze_tables['raw_skills']}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Clean up skills column

# COMMAND ----------

df_skills = df_skills.withColumn('skills', trim(regexp_replace(col('skills'), r"n/a|\?\?\?|none|None|N/A", "")))\
            .withColumn('skills', regexp_replace(col('skills'), r";|\||/|\s+", ","))\
            .withColumn('skills', split(col('skills'), ','))\
            .withColumn('skill', explode(col('skills')))\
            .withColumnRenamed('skills', 'skills_array')

# COMMAND ----------

cols = [c for c in df_skills.columns if c not in ['source_file', 'ingestion_date']]
df_skills = df_skills.select(*cols, 'source_file', 'ingestion_date')

# COMMAND ----------

df_skills = add_processed_date(df_skills)

# COMMAND ----------

merge_condition = "trg.job_id = src.job_id"
merge_delta_data_external(df_skills, 'silver', silver_tables['processed_skills'], merge_condition, silver_folder_path, 'processed_date')