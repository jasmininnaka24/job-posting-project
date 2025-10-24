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
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the Data

# COMMAND ----------

df_jobs = spark.sql(f"SELECT * FROM job_posting_catalog.bronze.{bronze_tables['raw_jobs']}")

# COMMAND ----------

columns_to_rename = ['job_id', 'job_title', 'company_id', 'location_id', 'salary_id', 'post_date', 'source_file', 'ingestion_date']

df_jobs = df_jobs.toDF(*columns_to_rename)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import DateType
from datetime import datetime

invalid_formats = ['unknown', 'n/a', 'null', '??']

# ---------- OPTION 1: Spark-native parsing ----------
try:
    formats = ("MM-dd-yyyy", "MM/dd/yyyy", "yyyy-MM-dd", "yyyy/MM/dd", "MM-dd-yy", "MMMM dd, yyyy", "MMM dd yyyy")

    split_date = split(col('post_date'), '-')

    df_jobs = df_jobs.withColumn("post_date_cleaned", when(
        (col('post_date').contains('-')) &
        (size(split_date) == 3) &
        (length(split_date.getItem(0)) <= 2) &
        (length(split_date.getItem(1)) <= 2) &
        (length(split_date.getItem(2)) == 2),
        concat(
            split_date.getItem(0), lit('-'),
            split_date.getItem(1), lit('-'),
            lit('20'), split_date.getItem(2)
        )
    ).otherwise(col('post_date')))

    df_jobs = df_jobs.withColumn(
        'post_date_cleaned',
        when(
            (lower(col('post_date_cleaned')).isin(invalid_formats)) |
            (col('post_date_cleaned').isNull()),
            lit(None)
        ).otherwise(col('post_date_cleaned'))
    )

    df_jobs = df_jobs.withColumn(
        'post_date',
        coalesce(*[to_date(col('post_date_cleaned'), fmt) for fmt in formats])
    ).drop('post_date_cleaned')

    print("✅ Used Option 1 (Spark-native parsing)")

# ---------- OPTION 2: Fallback to Python UDF ----------
except Exception as e:
    print("⚠️ Spark-native parse failed, switching to Option 2 (Python UDF)")
    print("Error:", e)

    formats = [
        "%m-%d-%Y", "%m/%d/%Y", "%Y-%m-%d",
        "%Y/%m/%d", "%m-%d-%y", "%B %d, %Y", "%b %d %Y"
    ]

    def safe_parse_date(x):
        if x is None:
            return None
        for f in formats:
            try:
                return datetime.strptime(x, f).date()
            except Exception:
                continue
        return None

    parse_date_udf = udf(safe_parse_date, DateType())

    df_jobs = df_jobs.withColumn(
        'post_date',
        when(
            (lower(col('post_date')).isin(invalid_formats)) |
            (col('post_date').isNull()),
            None
        ).otherwise(col('post_date'))
    )

    df_jobs = df_jobs.withColumn("post_date", parse_date_udf(col("post_date")))

# COMMAND ----------

df_jobs = df_jobs.withColumn("post_date", col("post_date").cast("string"))

# COMMAND ----------

df_jobs = df_jobs.withColumn('job_title', trim(initcap(col('job_title'))))

# COMMAND ----------

import os, json

json_path = os.path.join('..', 'config', 'job_title_corrections.json')
with open(json_path) as f:
    correction_dict = json.load(f)

# COMMAND ----------

correction_dict

# COMMAND ----------

for wrong, correct in correction_dict.items():
    df_jobs = df_jobs.withColumn('job_title', when(col('job_title') == wrong, correct).otherwise(col('job_title')))

# COMMAND ----------

cols = [c for c in df_jobs.columns if c not in ['source_file', 'ingestion_date']]
df_jobs = df_jobs.select(*cols, 'source_file', 'ingestion_date')

# COMMAND ----------

df_jobs = add_processed_date(df_jobs)

# COMMAND ----------

merge_condition = "trg.job_id = src.job_id AND trg.company_id = src.company_id AND trg.location_id = src.location_id AND trg.salary_id = src.salary_id"
merge_delta_data_external(df_jobs, 'silver', silver_tables['processed_jobs'], merge_condition, silver_folder_path, 'processed_date')