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

job_schema = StructType(fields=[
                                StructField('Job.ID', IntegerType(), False),  
                                StructField('job_Title', StringType(), True),    
                                StructField('Comp_ID', IntegerType(), True),      
                                StructField('Loc.ID', IntegerType(), True),  
                                StructField('SalID', IntegerType(), True),      
                                StructField('Post.Date', StringType(), True),    
                            ])

# COMMAND ----------

df_jobs = spark.read.format('csv')\
                    .option('header', True)\
                    .schema(job_schema)\
                    .load(f'{bronze_folder_path}/{v_file_date}/jobs.csv')

# COMMAND ----------

df_jobs = add_source_file_date(df_jobs, v_source_file)

# COMMAND ----------

df_jobs = add_ingestion_date(df_jobs)

# COMMAND ----------

merge_condition = "trg.JobId = src.JobId AND trg.Comp_ID = src.Comp_ID AND trg.`Loc.ID` = src.`Loc.ID` AND trg.SalID = src.SalID"
merge_delta_data_managed(df_jobs, 'bronze', bronze_tables['raw_jobs'], merge_condition, 'ingestion_date')