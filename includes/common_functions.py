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