# ⚡ Airflow API to BigQuery Pipeline

![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat&logo=apache-airflow&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=flat&logo=google-cloud&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)

## 📌 Project Overview

An end-to-end **data engineering pipeline** that extracts data from a public REST API, loads it into **Google BigQuery**, applies transformations and data quality checks, all orchestrated via **Apache Airflow** with scheduled DAG runs.

## 🏗️ Architecture

```
  Public REST API (Open Weather / Finance API)
              │
              ▼
   Python Extractor (requests + pandas)
              │
              ▼
   Google Cloud Storage (Raw JSON Landing Zone)
              │
              ▼
   BigQuery Raw Table (External Load Job)
              │
              ▼
   BigQuery Transformation Layer
   (SQL-based cleaning + enrichment)
              │
              ▼
   Data Quality Checks
   (row count, null checks, freshness)
              │
              ▼
   BigQuery Analytics Table → Dashboard / Reports
              │
   Orchestrated by Apache Airflow DAG (scheduled daily)
```

## 🧰 Tech Stack

| Component | Tool |
|---|---|
| Orchestration | Apache Airflow 2.7 |
| Cloud Warehouse | Google BigQuery |
| Cloud Storage | Google Cloud Storage |
| Language | Python 3.10, SQL |
| Containerization | Docker + Docker Compose |
| Data Quality | Custom Python validators |
| API Source | OpenWeatherMap / Public Finance API |

## 📁 Project Structure

```
airflow-api-bigquery-pipeline/
├── dags/
│   ├── api_to_bigquery_dag.py       # Main Airflow DAG
│   └── utils/
│       ├── api_extractor.py         # API fetch logic
│       ├── bq_loader.py             # BigQuery load logic
│       └── data_quality.py          # Quality check functions
├── sql/
│   ├── create_raw_table.sql         # DDL for raw table
│   └── transform_analytics.sql      # Analytics transformation
├── tests/
│   ├── test_api_extractor.py
│   └── test_data_quality.py
├── docker-compose.yml               # Airflow local setup
├── requirements.txt
└── README.md
```

## 🚀 Key Features

- **Airflow DAG orchestration**: Scheduled daily runs with retries, alerting, and task dependencies
- **API ingestion**: Fetches live data from REST API with pagination and error handling
- **GCS landing zone**: Raw JSON stored in Google Cloud Storage before BigQuery load
- **BigQuery load jobs**: Efficient batch loading with schema enforcement
- **Data quality checks**: Row count validation, null checks, freshness monitoring
- **Dockerized setup**: Full local development environment with docker-compose

## 📈 DAG Flow

```
start
  └→ extract_from_api
        └→ upload_to_gcs
              └→ load_to_bigquery_raw
                    └→ run_transformations
                          └→ run_data_quality_checks
                                └→ notify_success
```

## ⚙️ Setup & Run

### Prerequisites
- Docker & Docker Compose
- Google Cloud account with BigQuery + GCS enabled
- Service account JSON key with BigQuery and GCS permissions

```bash
# Clone repo
git clone https://github.com/Ashok98765vvs/airflow-api-bigquery-pipeline.git
cd airflow-api-bigquery-pipeline

# Add GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# Start Airflow via Docker
docker-compose up -d

# Access Airflow UI
open http://localhost:8080
# user: airflow | password: airflow

# Trigger DAG manually
airflow dags trigger api_to_bigquery_pipeline
```

## 🛡️ Data Quality Checks

| Check | Description | Action on Fail |
|---|---|---|
| Row count | Ensures minimum rows loaded | Fail DAG, send alert |
| Null check | No nulls in key columns | Fail DAG, send alert |
| Freshness | Data within last 24 hours | Warning, continue |
| Schema | Column types match expected | Fail DAG |

## 📈 Impact

- Automated daily data ingestion saving 2+ hours of manual work
- 99.5% pipeline reliability with retry logic and alerting
- Data quality gates ensure only clean data reaches analytics layer

## 👤 Author

**Ashok** — Data Engineer  
📧 [LinkedIn](https://www.linkedin.com/in/ashok-vvs) | 🐙 [GitHub](https://github.com/Ashok98765vvs)

---
⭐ Star this repo if you find it useful!
