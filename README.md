<<<<<<< Updated upstream
**Kafka** and a **fraud watchlist of JSON files** (via Auto Loader), processes them through a medallion architecture (bronze → silver → gold), detects suspicious activity, and sends **real-time email alerts** — all surfaced on a fraud KPIs dashboard.
=======
# FinGuard — Real-Time Fraud Detection Pipeline

FinGuard is an end-to-end, streaming fraud detection platform built on Databricks. It ingests live card **transactions from Kafka** and a **fraud watchlist of JSON files** (via Auto Loader), processes them through a medallion architecture (bronze → silver → gold), detects suspicious activity, and sends **real-time email alerts** — all surfaced on a fraud KPIs dashboard.
>>>>>>> Stashed changes

## Architecture

```
 Kafka (transactions)  ─┐
                        ├─► Bronze ─► Silver ─► Gold ─► Alert notifiers (email)
 Auto Loader (watchlist)┘                            └► KPIs dashboard
```

* **Bronze** — raw ingestion
  * `transactions`: Kafka stream (SASL_SSL) of raw transaction events into `finguard.bronze.transactions`
  * `fraud_watchlist`: Auto Loader ingestion of JSON watchlist files from `/Volumes/finguard/source/fraud_watchlist/source_data/`
* **Silver** — cleaned & conformed data (typed timestamps, parsed watchlist fields, deduplication)
  * Transactions and fraud watchlist tables; `customers_silver` loaded from the separate `finguard_customers_silver_load` module
* **Gold** — fraud analytics & alerting
  * `fraud_card_alert`: joins transactions to the watchlist on card number to flag cards on the fraud watchlist
  * `high_value_transactions_alert`: flags transactions above the high-value threshold
  * `transaction_count_by_minute` and `transaction_count_by_sliding_window`: streaming windowed aggregates for volume monitoring
* **Alert** — `foreach_batch` sinks (via append flows) that send email notifications through Gmail SMTP whenever fraud or high-value alerts fire

All streaming tables and materialized views are defined as **Lakeflow Spark Declarative Pipeline (SDP)** code and live in the Unity Catalog `finguard` catalog under the `bronze`, `silver`, `gold`, and `alert` schemas.

## Repository structure

```
finguard-streaming/
├── databricks notebooks and pipelines/
│   ├── 01_kafka_streaming_test        # Kafka producer/consumer connectivity test
│   ├── 02_Setup_Secret_Scope          # one-time setup of the `finguard-scope` secret scope
│   ├── 03_test_email                  # SMTP email send test
│   ├── 04_Autoloader_test             # Auto Loader ingestion test
│   ├── finguard_streaming/            # main SDP pipeline code
│   │   ├── bronze/                    #   raw Kafka + Auto Loader ingestion
│   │   ├── silver/                    #   cleaned, conformed tables
│   │   ├── gold/                      #   fraud alerts + windowed aggregates
│   │   └── alert/                     #   email notifier sinks
│   ├── finguard_customers_silver_load/ # customers silver table module
│   └── fraud_watchlist_file_generator/# CSV + notebook that generate watchlist test data
└── dashboard/
    └── FinGuard Fraud Detection KPIs.lvdash.json
```

## Prerequisites

* A Databricks workspace with Unity Catalog and a `finguard` catalog
* A secret scope **`finguard-scope`** containing:
  * `kafka_connection_details` — JSON with `bootstrap_servers`, `api_key`, `api_secret`, `topic`
  * `gmail_api_key` — Gmail SMTP credentials for alert emails
* A Kafka topic streaming transaction events
* Watchlist JSON files landing in `/Volumes/finguard/source/fraud_watchlist/source_data/`

Run `02_Setup_Secret_Scope` once to create the scope, and use the numbered `0x_*` notebooks to verify each dependency (Kafka, email, Auto Loader) before starting the pipeline.

## Running the pipeline

1. Generate sample watchlist data with `fraud_watchlist_file_generator` and drop the JSON files into the source volume (or point Auto Loader at your own files).
2. Create a Lakeflow Spark Declarative Pipeline and attach the Python files from `finguard_streaming/` (bronze → silver → gold → alert).
3. Start a pipeline update. Tables materialize into `finguard.bronze`, `finguard.silver`, `finguard.gold`, and alerts begin flowing as soon as the Kafka topic receives events.
4. Import `dashboard/FinGuard Fraud Detection KPIs.lvdash.json` into Databricks dashboards to monitor KPIs.

## Fraud Detection KPIs dashboard

The included dashboard tracks the fraud posture in real time:

* **KPI counters** — total transactions, transaction volume (₹), distinct fraud alerts, fraud rate, flagged amount, high-value alert count
* **Trend** — daily transactions vs. alerts (dual axis)
* **Breakdowns** — risk level, merchant category, reason code, and hourly distributions
* **Tables** — top merchants, top locations, and the latest 20 deduplicated alerts

## Tech stack

<<<<<<< Updated upstream
Databricks · Lakeflow Spark Declarative Pipelines (SDP) · Spark Structured Streaming · Kafka · Auto Loader · Unity Catalog · Gmail SMTP · AI/BI Dashboards
=======
Databricks · Lakeflow Spark Declarative Pipelines (SDP) · Spark Structured Streaming · Kafka · Auto Loader · Unity Catalog · Gmail SMTP · AI/BI Dashboards
>>>>>>> Stashed changes
