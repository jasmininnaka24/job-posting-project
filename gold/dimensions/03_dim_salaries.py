# Databricks notebook source
# MAGIC %run ../../config/table_names

# COMMAND ----------

# MAGIC %run ../../includes/configuration

# COMMAND ----------

# MAGIC %run ../../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create incremental flag

# COMMAND ----------

dbutils.widgets.text('p_incremental_flag', '0')
v_incremental_flag = dbutils.widgets.get('p_incremental_flag')
v_incremental_flag

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_src = spark.sql(f'''SELECT * FROM job_posting_catalog.silver.{silver_tables['processed_salaries']}''')

# COMMAND ----------

# MAGIC %md
# MAGIC ## df_sink - initial and incremental

# COMMAND ----------

if spark.catalog.tableExists(f"job_posting_catalog.gold.{gold_tables['dim_salaries']}"):
    df_sink = spark.sql(f'''SELECT * FROM job_posting_catalog.gold.{gold_tables['dim_salaries']}''')
else:
    df_sink = spark.sql(f'''
                        SELECT 1 AS dim_salary_key, *
                        FROM job_posting_catalog.silver.{silver_tables['processed_salaries']}
                        WHERE 1=0
                        ''')

# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## df_filter

# COMMAND ----------

df_filter = df_src.join(df_sink,
                        df_src['salary_id'] == df_sink['salary_id'],
                        "left"
                        ).select(
                            df_sink['dim_salary_key'],
                            df_src['*']
                        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## df_filter_old

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_salary_key').isNotNull())
display(df_filter_old)

# COMMAND ----------

# MAGIC %md
# MAGIC ## df_filter_new

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_salary_key').isNull())
display(df_filter_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Surrogate Key

# COMMAND ----------

if v_incremental_flag == '0':
    max_value = 1
else:
    max_value = spark.sql(f'''SELECT MAX(dim_salary_key) FROM job_posting_catalog.gold.{gold_tables['dim_salaries']}''')
    max_value = max_value.collect[0][0] + 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## create surrogate key and max surrogate key

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_salary_key', max_value+monotonically_increasing_id())
df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # create df_final_filter = df_filter_new + df_filter_old

# COMMAND ----------

df_filter_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

df_filter_final = add_last_updated_date(df_filter_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 1 - Upsert

# COMMAND ----------

merge_condition = 'trg.salary_id = src.salary_id'
table_name = gold_tables['dim_salaries']
merge_delta_data_external(df_filter_final, 'gold', table_name, merge_condition, gold_folder_path, 'last_updated_at')