-- Databricks notebook source
-- Count how many jobs each company has
SELECT
  c.company_name,
  COUNT(j.job_id) AS total_jobs
FROM job_posting_catalog.gold.dim_jobs AS j
LEFT JOIN job_posting_catalog.gold.dim_companies AS c
  ON j.company_id = c.company_id
GROUP BY c.company_name
ORDER BY total_jobs DESC;


-- COMMAND ----------

