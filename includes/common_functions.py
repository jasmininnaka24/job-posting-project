# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

def add_source_file_date(input, value):
    output_df = input.withColumn('source_file', lit(value))
    return output_df

# COMMAND ----------

def add_ingestion_date(input):
    output_df = input.withColumn('ingestion_date', current_date())
    return output_df

# COMMAND ----------

def add_processed_date(input):
    output_df = input.withColumn('processed_date', current_date())
    return output_df

# COMMAND ----------

def add_last_updated_date(input):
    output_df = input.withColumn('last_updated_at', current_date())
    return output_df

# COMMAND ----------

def merge_delta_data_external(input_df, schema_name, table_name, merge_condition, gold_folder_path, partition_column):
    from delta.tables import DeltaTable

    if spark.catalog.tableExists(f"job_posting_catalog.{schema_name}.{table_name}"):
        deltaTable = DeltaTable.forPath(spark, f'{gold_folder_path}/{table_name}')

        deltaTable.alias('trg').merge(input_df.alias('src'), merge_condition)\
                                                    .whenMatchedUpdateAll()\
                                                    .whenNotMatchedInsertAll()\
                                                    .execute()

    else:
        input_df.write.format('delta')\
                            .mode('overwrite')\
                            .partitionBy(partition_column)\
                            .option('path', f'{gold_folder_path}/{table_name}')\
                            .saveAsTable(f"job_posting_catalog.{schema_name}.{table_name}")

# COMMAND ----------

def merge_delta_data_managed(input_df, schema_name, table_name, merge_condition, partition_column):
    from delta.tables import DeltaTable

    full_name = f"job_posting_catalog.{schema_name}.{table_name}"

    if spark.catalog.tableExists(full_name):
        deltaTable = DeltaTable.forName(spark, full_name)

        deltaTable.alias('trg').merge(input_df.alias('src'), merge_condition)\
                                            .whenMatchedUpdateAll()\
                                            .whenNotMatchedInsertAll()\
                                            .execute()

    else:
        input_df.write.format('delta')\
                            .mode('overwrite')\
                            .partitionBy(partition_column)\
                            .option("delta.columnMapping.mode", "name")\
                            .saveAsTable(f"job_posting_catalog.{schema_name}.{table_name}")






from delta.tables import DeltaTable
from pyspark.sql import functions as F


# Add the SCD tracking columns
now = F.current_timestamp()
input_df = (
    input_df
    .withColumn("effective_start_date", now)
    .withColumn("effective_end_date", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
)

if spark.catalog.tableExists(job_posting_catalog.{schema_name}.{table_name}):
    deltaTable = DeltaTable.forPath(spark, "abfss://gold@sgjobpostingproj.dfs.core.windows.net/{table_name}")

    # 👇 Expire the old records when a match happens
    deltaTable.alias('tgt').merge(
        input_df.alias('src'),
        merge_condition + " AND tgt.is_current = true"
    ).whenMatchedUpdate(set={
        "effective_end_date": now,
        "is_current": "false"
    }).whenNotMatchedInsertAll().execute()

    # 👇 Now add the new (changed) records as new rows
    input_df.write.format('delta') \
        .mode('append') \
        .option('path', "{gold_folder_path}/{table_name}") \
        .saveAsTable(job_posting_catalog.{schema_name}.{table_name})

else:
    # first load
    input_df.write.format('delta') \
        .mode('overwrite') \
        .partitionBy(partition_column) \
        .option('path', "{gold_folder_path}/{table_name}") \
        .saveAsTable(job_posting_catalog.{schema_name}.{table_name})






# For incremental load that's also the same with Slowly Changing Dimension Type 1

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Your incremental data (new/changed records only)
# input_df = ...

if spark.catalog.tableExists(f"job_posting_catalog.{schema_name}.{table_name}"):
    # Table exists - do incremental merge
    deltaTable = DeltaTable.forPath(spark, f"{gold_folder_path}/{table_name}")

    deltaTable.alias('target').merge(
        input_df.alias('source'),
        "target.id = source.id"  # Your merge key(s)
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

else:
    # First load - create table
    input_df.write.format('delta') \
        .mode('overwrite') \
        .option('path', f"{gold_folder_path}/{table_name}") \
        .saveAsTable(f"job_posting_catalog.{schema_name}.{table_name}")

print(f"✅ Incremental load completed for {table_name}")