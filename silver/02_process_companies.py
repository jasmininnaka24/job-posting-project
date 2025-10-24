# Databricks notebook source
# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../config/table_names

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the data

# COMMAND ----------

df_companies = spark.sql(f"SELECT * FROM job_posting_catalog.bronze.{bronze_tables['raw_companies']}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename the columns

# COMMAND ----------

formatted_column_names = ['company_id', 'industry', 'company_size', 'company_name', 'founded_year', 'source_file', 'ingestion_date']

df_companies = df_companies.toDF(*formatted_column_names)

# COMMAND ----------

# MAGIC %md
# MAGIC # Clean up Size column

# COMMAND ----------

invalid_company_size_values = ['unknown', '??', ""]

# trim
# list invalid values
# remove invalid values, make them null
# make a company_min_size column, take the first value of the size
# make a company_max_size column, take the last value of the size
# cast company_min_size and company_max_size to integer

df_companies = df_companies.withColumn('company_size', when(
                                trim(col('company_size')).isin(invalid_company_size_values),
                                lit(None)
                            ).otherwise(col('company_size')))\
                            .withColumn('company_min_size', (split(col('company_size'), '-')[0]).cast('integer'))\
                            .withColumn('company_max_size', (split(col('company_size'), '-')[1]).cast('integer'))


# COMMAND ----------

# MAGIC %md
# MAGIC # Clean up founded year column

# COMMAND ----------

invalid_founded_year_values = ['year unknown', 'N/A', '###']

# trim
# list all the invalid values
# remove invalid values and replace them with none
# cast the year founded into integer
df_companies = df_companies.withColumn('founded_year', when(trim(col('founded_year')).isin(invalid_founded_year_values), lit(None)).otherwise(col('founded_year')))\
            .withColumn('founded_year', col('founded_year').cast('integer'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Clean up company name column

# COMMAND ----------

invalid_company_names_values = ['???', 'n/a']
openai_invalid_formats = ['open-ai', 'openai', 'open ai']

# trim
# make title case
# list invalid values
# remove invalid values
# check "..." from the values, replace with ""
# fix OpenAI names
df_companies = df_companies.withColumn('company_name', initcap(trim(col('company_name'))))\
            .withColumn('company_name', when(
                (lower(col('company_name')).isin(invalid_company_names_values)) | (col('company_name').isNull()), lit(None)
            ).otherwise(col("company_name")))\
            .withColumn('company_name', regexp_replace(col("company_name"), r"^,|^\.\.\.|\.\.\.$|,$|^,{2,}|,{2,}$", ""))\
            .withColumn('company_name', when(
                lower(col("company_name")).isin(openai_invalid_formats),
                lit('OpenAI')
            ).otherwise(col('company_name')))

# COMMAND ----------

invalid_industry_values = ['n/a', '###', '?']

df_companies = df_companies.withColumn('industry', when(
                lower(col('industry')).isin(invalid_industry_values) | col('industry').isNull(),
                lit(None)
            ).otherwise(col('industry')))\
            .withColumn('industry', regexp_replace(col('industry'), r'Tech', lit('Technology')))

# COMMAND ----------

cols = [c for c in df_companies.columns if c not in ['source_file', 'ingestion_date']]
df_companies = df_companies.select(*cols, 'source_file', 'ingestion_date')

# COMMAND ----------

df_companies = add_processed_date(df_companies)

# COMMAND ----------

merge_condition = "trg.company_id = src.company_id"
merge_delta_data_external(df_companies, 'silver', silver_tables['processed_companies'], merge_condition, silver_folder_path, 'processed_date')

# COMMAND ----------

