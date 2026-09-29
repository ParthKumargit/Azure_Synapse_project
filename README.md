🚀 End-to-End Azure Synapse Retail Data Engineering Pipeline


📌 Project Overview --

This project implements an end-to-end retail data engineering pipeline built natively on Microsoft Azure Synapse Analytics.
The solution ingests high-velocity, multi-event transaction streams from an external REST API (JSON format) and orchestrates data movement through a Medallion Architecture (Bronze, Silver, Gold) using Azure Synapse Integration Pipelines, Azure Data Lake Storage Gen2 (ADLS Gen2), and an on-demand Synapse Apache Spark Pool (PySpark).
The final Gold-layer analytical fact table is exposed through Azure Synapse Serverless SQL Pools using external tables for direct consumption by Power BI dashboards and executive reporting.

🎯 Problem Statement --

*Source Heterogeneity & High-Volume Noise:

*Raw retail event streams arrive dynamically via an external HTTP REST API endpoint in JSON format.

*Payloads contain mixed operational records: valid purchases, order cancellations, refund requests, and corrupted entries missing identifiers.

*Centralization & Processing Needs:

*Ingest the raw dynamic API stream continuously into a centralized storage account without compute overhead.

*Preserve immutable raw records for historical audit and schema re-evaluation.

*Filter out non-purchase noise (cancellations and refunds) and drop invalid null records.

*Standardize casing, cleanse schema types (resolve string-to-decimal/double conversion failures), and normalize timestamps into calendar dates.

*Aggregate daily financial Key Performance Indicators (KPIs) to monitor store health.

*Serve the aggregated metrics using a low-cost, serverless query model that eliminates 24/7 dedicated SQL cluster expenses.

🛠️ Technical Requirements & Stack --
Cloud Platform: Microsoft Azure

Unified Workspace: Azure Synapse Analytics Studio

Data Storage: Azure Data Lake Storage Gen2 (ADLS Gen2) with Hierarchical Namespace enabled

Orchestration & Ingestion: Azure Synapse Integration Pipelines (Copy Data Activity with schema type mapping)

Compute Engine: Azure Synapse Apache Spark Pool (3-node cluster, small worker sizing, 15-minute auto-pause enabled)

Processing Language: Python / PySpark

Serving Layer: Azure Synapse Serverless SQL Pool 

---------------

[ External Retail REST API (HTTP JSON) ]
                   │
                   ▼  (Synapse Pipeline - Copy Data Activity)
 ┌───────────────────────────────────────────────┐
 │        ADLS Gen2: Bronze Layer (Raw)          │
 │  Path: retail/bronze/                         │
 │  Format: Raw Snappy-Compressed Parquet        │
 └───────────────────────┬───────────────────────┘
                         │
                         ▼  (Synapse Spark Pool - PySpark Transformation)
 ┌───────────────────────────────────────────────┐
 │       ADLS Gen2: Silver Layer (Cleaned)       │
 │  Path: retail/silver/                         │
 │  Logic: Filter purchase, drop nulls, cast types│
 └───────────────────────┬───────────────────────┘
                         │
                         ▼  (Synapse Spark Pool - PySpark Aggregation)
 ┌───────────────────────────────────────────────┐
 │        ADLS Gen2: Gold Layer (Curated)        │
 │  Path: retail/gold/daily_revenue/             │
 │  Metrics: Grouped by event_date               │
 └───────────────────────┬───────────────────────┘
                         │
                         ▼  (Synapse Serverless SQL - OPENROWSET)
 ┌───────────────────────────────────────────────┐
 │        Serving Layer (External Table)         │
 │  Database: RetailAnalyticsDB                  │
 │  Table: dbo.FactDailyRevenue                  │
 └───────────────────────┬───────────────────────┘
                         │
                         ▼
                 [ Power BI Reports ]

---------------

⚙️ Medallion Layer Implementation --

1. Ingestion Layer (Raw $\rightarrow$ Bronze)Mechanism: Synapse Integration Pipeline with a parameterized Copy Activity.Source: HTTP REST API (Anonymous GET request to raw JSON endpoint).Sink: ADLS Gen2 container (retail/bronze/).Format: Apache Parquet.Operational Fix: Configured explicit tabular mapping inside the Copy Activity to cast the incoming amount field directly to Double precision, preventing pipeline failure caused by unexpected decimal values in raw text.
2. Cleansing Layer (Bronze $\rightarrow$ Silver)Engine: Synapse Apache Spark Pool (PC spark).Source Path: abfss://retail@<storage_account>.dfs.core.windows.net/bronze/Transformations Performed:Business Scope Filtering: Kept only records where col("event_type") == "purchase", excluding refund and cancellation events.Missing Value Handling: Dropped records with null values in essential columns (customer_id, amount, event_id) using .dropna().Standardization: Converted payment_method and product_category to lowercase.Date Normalization: Converted string timestamp event_timestamp into a standard date event_date using to_date().Data Typing: Enforced DoubleType on transaction amount.Output Path: abfss://retail@<storage_account>.dfs.core.windows.net/silver/ (Partitioned by event_date).
3. Aggregation Layer (Silver $\rightarrow$ Gold)Engine: Synapse Apache Spark Pool.Source Path: abfss://retail@<storage_account>.dfs.core.windows.net/silver/Grouping Dimensions:event_date (Transaction Date)Metrics Calculated:Daily Total Revenue: round(sum("amount"), 2)Total Number of Purchases: count("event_id")Output Path: abfss://retail@<storage_account>.dfs.core.windows.net/gold/daily_revenue/ (Written as Parquet).
4. Serving Layer (Synapse Serverless SQL Pool)Database Created: RetailAnalyticsDBMechanism: External Table dbo.FactDailyRevenue backed by an external data source pointing to the Gold container path.Cost & Performance Optimization: Uses Serverless SQL's on-demand query engine—zero continuous hardware costs, billed strictly on data processed ($5 per TB scanned).
