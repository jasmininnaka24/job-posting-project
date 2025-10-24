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

df_locations = spark.sql(f"SELECT * FROM job_posting_catalog.bronze.{bronze_tables['raw_locations']}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename the columns

# COMMAND ----------

columns = ['location_id', 'city_name', 'region', 'country', 'source_file', 'ingestion_date']

df_locations = df_locations.toDF(*columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # Change Country and Region column for ph values

# COMMAND ----------

df_locations = df_locations.withColumn('country', when(
                lower(col('city_name')).like('ph%') | lower(col('city_name')).like('%ph'),
                lit('Philippines')
            ).otherwise(col('country')))\
            .withColumn('region', when(
                lower(col('country')).like('ph%'),
                lit('APAC')
            ).otherwise(col('region')))

# COMMAND ----------

# MAGIC %md
# MAGIC # Clean up country column

# COMMAND ----------

invalid_country_values = ['###', '']

df_locations = df_locations.withColumn('country', when(
                    lower(col('country')).like('ph%'),
                    lit('Philippines')
                ).when(
                    lower(col('country')).rlike('worldwide'),
                    lit('Worldwide')
                ).when(
                    col('country').isin(invalid_country_values) | col('country').isNull(),
                    lit(None)
                ).otherwise(col("country")))

# COMMAND ----------

# MAGIC %md
# MAGIC # Clean up region column

# COMMAND ----------

invalid_region_values = ['na', '?']

df_locations = df_locations.withColumn('region', when(
                    lower(col('country')).rlike('uk'),
                    lit('EMEA')
                ).when(
                    lower(col('country')).like('us%'),
                    lit('AMER')
                ).when(
                    lower(col('region')).isin(invalid_region_values) | col('region').isNull(),
                    lit(None)
                ).otherwise(col('region')))\
            .withColumn('region', regexp_replace(col('region'), lower(col("region")).rlike('global'), lit('Global')))

# COMMAND ----------

# MAGIC %md
# MAGIC # Clean up city name column

# COMMAND ----------

df_locations = df_locations.withColumn('city_name', when(
                            lower(col('city_name')).rlike('mn') | lower(col("city_name")).rlike('manila') | lower(col("city_name")).rlike('mnl'),
                            lit('Manila')
                        ).when(
                            lower(col('city_name')).rlike('cebu'),
                            lit('Cebu')
                        ).otherwise(col('city_name')))\
            .withColumn('city_name', initcap(col('city_name')))\
            .withColumn('city_name', when(
                col('city_name').isin('') | col('city_name').isNull(),
                lit(None)
            ).otherwise(col('city_name')))

# COMMAND ----------

cols = [c for c in df_locations.columns if c not in ['source_file', 'ingestion_date']]
df_locations = df_locations.select(*cols, 'source_file', 'ingestion_date')

# COMMAND ----------

df_locations = add_processed_date(df_locations)

# COMMAND ----------

merge_condition = "trg.location_id = src.company_id"
merge_delta_data_external(df_locations, 'silver', silver_tables['processed_locations'], merge_condition, silver_folder_path, 'processed_date')