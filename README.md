# 🧠 Job Posting Data Pipeline Project

## 📘 Overview
The **JobPostingProj** is a data engineering project designed to automate the ingestion, transformation, and preparation of job posting data for analytics.  

It follows a **Medallion Architecture** — **Bronze**, **Silver**, and **Gold** — combined with a **Star Schema** data model to ensure data quality, scalability, and maintainability across all stages of the pipeline.

This architecture supports both **incremental data loading** and **dimensional modeling** — optimizing data for analysis and reporting in the **Gold** layer.

---

## 🗂️ Project Structure

| Folder / File | Description |
|----------------|-------------|
| **bronze/** | Raw data ingestion layer (unprocessed job posting data). |
| **silver/** | Cleaned, standardized, and deduplicated job data ready for analysis. |
| **gold/** | Curated data models using a **Star Schema** (Fact and Dimension tables). |
| **config/** | Configuration files for environment variables, paths, and schema mappings. |
| **includes/** | Utility functions and reusable helper modules. |
| **create_schema.py** | Script to create and initialize database schemas and tables. |
| **prepare_for_incremental_load.py** | Script to handle incremental job posting loads efficiently. |

---

## ⚙️ Features

- 🧩 **Schema Management:** Automatically creates and validates database schemas.  
- 🔁 **Incremental Loading:** Efficiently processes new or updated job postings without reloading entire datasets.  
- 🧱 **Layered Data Processing:** Implements the Medallion (Bronze → Silver → Gold) architecture for progressive data refinement.  
- ⭐ **Star Schema Modeling:** The Gold layer uses a **Fact and Dimension** design for analytics and BI reporting.  
- ⚙️ **Configurable Setup:** Manage settings through the `config/` folder for flexible environment and schema adjustments.  
- 🧰 **Modular Design:** Reusable functions and transformations are centralized in the `includes/` folder for maintainability.  

---

## 🚀 Getting Started

### 1️⃣ Clone the Repository
```bash
git clone https://github.com/jasmininnaka24/job-posting-project.git
cd JobPostingProj
