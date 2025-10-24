# Databricks notebook source
# MAGIC %run ../includes/configuration

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
# MAGIC # Read the data

# COMMAND ----------

df_salaries = spark.sql(f"SELECT * FROM job_posting_catalog.bronze.{bronze_tables['raw_salaries']}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename the columns

# COMMAND ----------

df_salaries = df_salaries.withColumnRenamed('SalID', 'salary_id')\
                            .withColumnRenamed('Salary', 'salary')\
                            .withColumnRenamed('CurrencyType?', 'currency_type')\
                            .withColumnRenamed('rangeInfo', 'range_info')

# COMMAND ----------

df_salaries = df_salaries.withColumn('currency_type', when(
    (lower(col('salary')).contains('php')) | 
    (col('salary').contains('₱')),
    lit('PHP'))\
.when(
    (lower(col("salary")).contains('usd')) |
    (col('salary').contains('$')),
    lit('USD')
).otherwise(None))

# COMMAND ----------

invalid_values = ['??', 'na', 'n/a']

df_salaries = df_salaries.withColumn('range_info', when(
    col('salary').rlike(r"/month|\s*month|/\s*month|per\s*mo|per\s*month"), 'Monthly'
).when(col('range_info').isNotNull(), col("range_info")).otherwise(None))

df_salaries = df_salaries.withColumn('range_info', initcap(col('range_info')))

df_salaries = df_salaries.withColumn('range_info', when(
    (col("range_info").isin(invalid_values)) | (col('range_info').isNull()), lit(None)
).otherwise(col('range_info')))

# COMMAND ----------

invalid_format = ['na', 'unknown', 'n/a', "??"]

df_salaries = df_salaries.withColumn('salary', trim(regexp_replace(lower(col('salary')), r'php|₱|usd|\$|per|mo|month|/|nth', "")))\
                            .withColumn('salary', when(col('salary').isin(invalid_format), lit(None)).otherwise(col('salary')))\
                            .withColumn('max_salary', when(
                                col('salary').contains("-"),
                                split(col('salary'), "-")[1]
                            ).otherwise(None))\
                            .withColumn('salary', when(
                                col("salary").contains('-'),
                                split(col('salary'), '-')[0]
                            ).otherwise(col('salary')))

# COMMAND ----------

# MAGIC %md
# MAGIC # Cast Salary and Max Salary to double

# COMMAND ----------

df_salaries = df_salaries.withColumn('salary', col('salary').cast('double'))\
                        .withColumn('max_salary', col('max_salary').cast('double'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Drop Duplicates

# COMMAND ----------

df_salaries = df_salaries.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC # Drop Rows that have null values

# COMMAND ----------

df_salaries = df_salaries.dropna(how='all', subset=['salary', 'max_salary'])

# COMMAND ----------

# MAGIC %md
# MAGIC # Add currency inferred columns for currency that have nulls

# COMMAND ----------

df_salaries = df_salaries.withColumn('currency_inferred', when(
    col('currency_type').isNull(), True
).otherwise(False))

# COMMAND ----------

cols = [c for c in df_salaries.columns if c not in ['source_file', 'ingestion_date']]
df_salaries = df_salaries.select(*cols, 'source_file', 'ingestion_date')

# COMMAND ----------

df_salaries = add_processed_date(df_salaries)

# COMMAND ----------

merge_condition = "trg.salary_id = src.salary_id"
merge_delta_data_external(df_salaries, 'silver', silver_tables['processed_salaries'], merge_condition, silver_folder_path, 'processed_date')