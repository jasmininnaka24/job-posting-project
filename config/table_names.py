# Databricks notebook source
bronze_tables = {
    "raw_companies": "raw_companies",
    "raw_descriptions": "raw_descriptions",
    "raw_jobs": "raw_jobs",
    "raw_locations": "raw_locations",
    "raw_salaries": "raw_salaries",
    "raw_skills": "raw_skills"
}

silver_tables = {
    "processed_companies": "processed_companies",
    "processed_descriptions": "processed_descriptions",
    "processed_jobs": "processed_jobs",
    "processed_locations": "processed_locations",
    "processed_salaries": "processed_salaries",
    "processed_skills": "processed_skills"
}

gold_tables = {
    "dim_companies": "dim_companies",
    "dim_descriptions": "dim_descriptions",
    "dim_jobs": "dim_jobs",
    "dim_locations": "dim_locations",
    "dim_salaries": "dim_salaries",
    "dim_skills": "dim_skills"
}