# Databricks notebook source
# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS job_posting_catalog.bronze CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS job_posting_catalog.bronze
# MAGIC MANAGED LOCATION 'abfss://bronze@sgjobpostingproj.dfs.core.windows.net'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS job_posting_catalog.silver CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS job_posting_catalog.silver
# MAGIC MANAGED LOCATION 'abfss://silver@sgjobpostingproj.dfs.core.windows.net'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS job_posting_catalog.gold CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS job_posting_catalog.gold
# MAGIC MANAGED LOCATION 'abfss://gold@sgjobpostingproj.dfs.core.windows.net'

# COMMAND ----------

