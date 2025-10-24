# Databricks notebook source
# MAGIC %run ../includes/configuration
# MAGIC

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

df_descriptions = spark.sql(f"SELECT * FROM job_posting_catalog.bronze.{bronze_tables['raw_descriptions']}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename the columns

# COMMAND ----------

df_descriptions = df_descriptions.withColumnRenamed('JobId ', 'job_id')\
                                .withColumnRenamed('desc_text ', 'description_text')

# COMMAND ----------

html_patterns = [r'<[^>]*>', r'[*!#]*']
text_patterns = ['🎉', 'apply', '&128512;']

df_descriptions = df_descriptions.withColumn('description_text', regexp_replace(col('description_text'), '|'.join(html_patterns), ''))\
               .withColumn('description_text', regexp_replace(col('description_text'), '|'.join(text_patterns), ''))

# COMMAND ----------

cols = [c for c in df_descriptions.columns if c not in ['source_file', 'ingestion_date']]
df_descriptions = df_descriptions.select(*cols, 'source_file', 'ingestion_date')

# COMMAND ----------

df_descriptions = add_processed_date(df_descriptions)

# COMMAND ----------

merge_condition = "trg.job_id = src.job_id"
merge_delta_data_external(df_descriptions, 'silver', silver_tables['processed_descriptions'], merge_condition, silver_folder_path, 'processed_date')