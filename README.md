# FinGuard — Real-Time Credit Card Fraud Detection Platform

> **End-to-end data engineering project using Confluent Kafka, Python, PySpark Structured Streaming, Databricks Lakeflow, Delta/Unity Catalog, PostgreSQL, Auto Loader, stream-static joins, stream-stream joins, watermarks, window aggregations, email alerts and Databricks dashboards.**

![Architecture](docs/images/databricks-pipeline.png)

## 1. Business Problem

Financial transaction systems generate a continuous stream of credit-card activity. A traditional batch-only approach can delay fraud detection, make operational monitoring harder, and require repeated manual investigation.

FinGuard addresses this by building a near-real-time processing platform that can:

- ingest transaction events continuously;
- preserve raw Kafka events and lineage;
- parse and validate transaction JSON;
- enrich transactions with customer master data;
- identify transactions above a customer's configured transaction limit;
- ingest a fraud watchlist from JSON files;
- match streaming transactions against the watchlist;
- generate operational alerts;
- calculate transaction-volume metrics using event-time windows; and
- expose monitoring information through a Databricks dashboard.

## 2. Solution

The solution uses a **Medallion architecture** on Databricks.

```text
Python Producer
      |
      v
Confluent Kafka
      |
      | credit_card_transactions
      v
Spark Structured Streaming
      |
      +-----------------------------+
      |                             |
      v                             v
Bronze Transactions           Bronze Watchlist
      |                             |
      v                             v
Silver Transactions           Silver Watchlist
      |                             |
      +------------+----------------+
                   |
        +----------+----------+
        |                     |
        v                     v
 Stream-Static Join     Stream-Stream Join
        |                     |
        v                     v
High-Value Alert       Fraud Card Alert
        |                     |
        +----------+----------+
                   |
                   v
                 Email

Silver Transactions
        |
        v
Window Aggregations
        |
        v
Databricks Dashboard

PostgreSQL Customer Master
        |
        v
Lakeflow Connect
        |
        v
Customer Bronze → Customer Silver
```

## 3. Technology Stack

| Area | Technology |
|---|---|
| Event streaming | Confluent Kafka |
| Producer | Python, confluent-kafka |
| Distributed processing | PySpark |
| Streaming | Spark Structured Streaming |
| Cloud data platform | Databricks |
| Pipeline orchestration | Lakeflow Spark Declarative Pipelines |
| Data governance | Unity Catalog |
| Storage | Delta Lake / Unity Catalog tables and volumes |
| Reference data | PostgreSQL |
| File streaming | Auto Loader |
| Data architecture | Bronze / Silver / Gold |
| Stateful processing | Watermarks, stream-stream joins, windows |
| Notifications | Gmail SMTP for demo |
| Monitoring | Databricks Dashboard |

## 4. Key Engineering Concepts Demonstrated

- Kafka topics, partitions and offsets
- Kafka authentication using SASL_SSL / PLAIN
- Spark batch read from Kafka
- Spark Structured Streaming read from Kafka
- Kafka binary key/value conversion to string
- JSON parsing with explicit `StructType` schemas
- Delta table writes
- Streaming checkpoints
- `trigger(availableNow=True)` for controlled backlog processing
- Lakeflow managed pipeline tables
- Medallion architecture
- Data quality expectations
- PostgreSQL incremental ingestion
- Stream-static joins
- Auto Loader
- Stateful vs stateless processing
- Event-time timestamps
- Watermarks
- Stream-stream joins
- Tumbling windows
- Sliding windows
- Operational email notifications
- Near-real-time dashboarding
- Lakeflow Jobs dependencies

## 5. Repository Structure

```text
finguard-real-time-fraud-detection/
│
├── README.md
├── LICENSE
├── CONTRIBUTING.md
├── .gitignore
│
├── producer/
│   ├── .env.example
│   ├── requirements.txt
│   ├── config.py
│   ├── models.py
│   ├── utils.py
│   ├── fraud_engine.py
│   ├── customer_generator.py
│   ├── merchant_generator.py
│   ├── transaction_generator.py
│   ├── producer_normal.py
│   ├── producer_fraud_transaction.py
│   ├── producer_fraud_card.py
│   └── consumer.py
│
├── databricks/
│   ├── notebooks/
│   │   ├── 01_kafka_streaming_test.py
│   │   ├── 02_Setup_Secret_Scope.py
│   │   ├── 03_Send_Email.py
│   │   └── 04_Autoloader_test.py
│   │
│   └── pipeline/
│       ├── bronze/
│       ├── silver/
│       ├── gold/
│       └── alerts/
│
├── sql/
│   ├── customers_historic.sql
│   ├── customers_incremental.sql
│   └── fraud_watchlist.csv
│
├── dashboard/
│   └── FinGuard_Fraud_Detection_Monitoring.lvdash.json
│
└── docs/
    ├── architecture.md
    ├── SECURITY.md
    ├── Project_Architecture_and_Concepts.md
    ├── FinGuard_Click_by_Click_Build_Guide.docx
    └── images/
```

## 6. Step-by-Step Build

### Phase 1 — Confluent Kafka

1. Open Confluent Cloud.
2. Select the environment and Kafka cluster.
3. Open **Topics**.
4. Create topic `credit_card_transactions`.
5. Set partitions to **6** for this project.
6. Create a Kafka API key and API secret.
7. Save the credentials securely.
8. Open the Kafka client configuration.
9. Select Python.
10. Copy the bootstrap server.

You now have:

```text
BOOTSTRAP_SERVERS
API_KEY
API_SECRET
TOPIC_NAME
```

### Phase 2 — Local Python Producer

```bash
cd producer
python -m venv .venv
```

Windows:

```powershell
.\.venv\Scripts\Activate.ps1
```

Install packages:

```bash
pip install -r requirements.txt
```

Create `.env` from `.env.example` and enter your own Kafka values.

Run:

```bash
python producer_normal.py
```

Then open Confluent Cloud → Topic → Messages and confirm records are arriving.

### Phase 3 — Databricks Workspace

Create a workspace folder:

```text
FinGuard_Real_Time_Fraud_Detection
```

Create project folders:

```text
notebooks/
pipeline/
bronze/
silver/
gold/
alerts/
utilities/
```

### Phase 4 — Unity Catalog

Create:

```text
finguard
├── bronze
├── silver
├── gold
└── source
```

Inside `finguard.source`, create volumes for:

```text
transactions
fraud_watchlist
```

Example checkpoint locations:

```text
/Volumes/finguard/source/transactions/checkpoint/
/Volumes/finguard/source/fraud_watchlist/checkpoint/
```

### Phase 5 — Kafka Batch Connectivity Test

Create Databricks notebook:

```text
01_kafka_streaming_test
```

Read Kafka using `spark.read.format("kafka")`.

Validate:

```python
sample_batch.count()
display(sample_batch)
```

### Phase 6 — Parse Kafka Binary Data

Kafka exposes `key` and `value` as binary in Spark.

```python
parsed_batch = sample_batch.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")
)
```

Save the test output:

```python
parsed_batch.write.saveAsTable(
    "finguard.bronze.transactions_batch_test"
)
```

### Phase 7 — Streaming Test

Switch from:

```python
spark.read
```

to:

```python
spark.readStream
```

Use a dedicated checkpoint:

```text
/Volumes/finguard/source/transactions/checkpoint/
```

For the controlled initial test:

```python
.trigger(availableNow=True)
```

This processes the currently available backlog and then stops.

### Phase 8 — Lakeflow Pipeline

Create:

```text
FinGuard_Streaming_Pipeline
```

Create source files:

```text
bronze/transactions_bronze.py
silver/transactions_silver.py
silver/customers_silver.py
gold/high_value_transactions_alert.py
bronze/fraud_watchlist_bronze.py
silver/fraud_watchlist_silver.py
gold/fraud_card_alert.py
gold/transaciton_count_by_minute.py
gold/transaciton_count_by_minute_sliding_window.py
alerts/high_value_transaction_email_notifier.py
alerts/fraud_card_alert_email_notifier.py
```

### Phase 9 — Transaction Bronze

Read Kafka as a stream and preserve:

- key
- value
- topic
- partition
- offset
- timestamp
- timestampType
- ingestion timestamp

Target:

```text
finguard.bronze.transactions
```

### Phase 10 — Transaction Silver

Read Bronze incrementally.

Parse JSON with an explicit schema.

Apply data-quality rules.

Create typed business columns.

Target:

```text
finguard.silver.transactions
```

### Phase 11 — Customer Data

PostgreSQL provides customer reference/master data.

The customer flow is:

```text
PostgreSQL
   ↓
Lakeflow Connect
   ↓
Customer Bronze
   ↓
Customer Silver
```

The reference data includes the customer's configured transaction limit and contact information used by the fraud rule/notification.

### Phase 12 — High-Value Fraud Rule

Join the transaction stream with customer reference data:

```text
Transactions
      |
      | customer_id
      v
Customer Silver
```

Business condition:

```text
transaction amount > customer transaction limit
```

Output:

```text
finguard.gold.high_value_transactions_alert
```

### Phase 13 — High-Value Email Alert

A demo SMTP notifier reads the alert output and sends an email.

Credentials are retrieved from Databricks secrets rather than stored in source code.

### Phase 14 — Fraud Watchlist

The second streaming source is a JSON file feed.

```text
CSV / watchlist data
        ↓
JSON files
        ↓
Unity Catalog Volume
        ↓
Auto Loader
        ↓
Watchlist Bronze
        ↓
Watchlist Silver
```

### Phase 15 — Watermark + Stream-Stream Join

Convert event-time strings into real timestamps.

Apply watermarks to bound state:

```python
.withWatermark("transaction_timestamp", "5 minutes")
```

Join the transaction stream with the watchlist stream using the matching card/entity key.

Output:

```text
finguard.gold.fraud_card_alert
```

### Phase 16 — Window Aggregations

Tumbling window:

```text
1-minute fixed, non-overlapping windows
```

Sliding window:

```text
5-minute window
1-minute slide
```

These provide transaction-volume monitoring for the dashboard.

### Phase 17 — Dashboard

The dashboard focuses on operational monitoring such as:

- transaction volume;
- average transaction amount;
- alert counts;
- alert trends;
- high-risk customers;
- top customers by alerts;
- top merchants;
- transaction volume by minute;
- merchant category;
- country;
- payment channel; and
- risk level.

Dashboard export is included under `dashboard/`.

### Phase 18 — Lakeflow Jobs

Use Lakeflow Jobs to schedule the customer reference-data path:

```text
PostgreSQL ingestion
        ↓
Customer Bronze
        ↓
Customer Silver
```

The Silver task should depend on successful Bronze/source ingestion.

## 7. Data Flow Summary

```text
                         +----------------------+
                         | PostgreSQL Customers |
                         +----------+-----------+
                                    |
                                    v
                              Customer Bronze
                                    |
                                    v
                              Customer Silver
                                    |
                                    |
+-----------+             +---------v----------+
|  Python   |             | Stream-Static Join |
| Producer  |             +---------+----------+
+-----+-----+                       |
      |                             v
      v                    High-Value Gold Alert
+-----------+                       |
|  Kafka    |                       v
|  Topic    |                     Email
+-----+-----+
      |
      v
Transaction Bronze
      |
      v
Transaction Silver
      |
      +---------------------> Window Aggregations
      |                              |
      |                              v
      |                          Dashboard
      |
      |
      +---- Stream-Stream Join <---- Watchlist Silver
                                      ^
                                      |
                                 Watchlist Bronze
                                      ^
                                      |
                                  Auto Loader
                                      ^
                                      |
                              JSON files / Volume
```

## 8. Expected Business Impact

This project is designed to demonstrate the following business outcomes:

| Business area | Expected improvement |
|---|---|
| Fraud detection | Move from delayed/batch-style detection toward near-real-time alerting |
| Operational response | Surface suspicious transactions automatically through alerts |
| Data lineage | Preserve Kafka topic, partition and offset metadata |
| Data quality | Centralize parsing and validation in Silver |
| Scalability | Use Kafka partitions and distributed Spark processing |
| Reliability | Use streaming checkpoints for restart/recovery |
| Reusability | Separate raw, standardized and business-ready layers |
| Monitoring | Provide near-real-time operational metrics |
| Reference enrichment | Combine continuously arriving transactions with customer master data |
| Fraud intelligence | Combine transactions with a streaming watchlist |

> **Important:** The impact values above are architectural/business goals demonstrated by the project, not measured production KPIs. Production ROI, latency reduction, fraud-loss reduction and alert precision would need real baseline and production measurements.

## 9. Validation Checklist

- [ ] Kafka topic exists
- [ ] Topic has 6 partitions
- [ ] API credentials work
- [ ] Python producer publishes events
- [ ] Databricks batch read returns records
- [ ] Kafka key/value converted from binary to string
- [ ] Batch test table created
- [ ] Streaming test succeeds
- [ ] Checkpoint exists
- [ ] Transaction Bronze updates
- [ ] Transaction Silver updates
- [ ] Customer Bronze/Silver updates
- [ ] High-value alert is generated
- [ ] High-value email is delivered
- [ ] Auto Loader detects watchlist files
- [ ] Watchlist Bronze/Silver updates
- [ ] Watermark works
- [ ] Stream-stream join produces a match
- [ ] Fraud-card email is delivered
- [ ] Tumbling window updates
- [ ] Sliding window updates
- [ ] Dashboard reflects Gold/Silver data
- [ ] Lakeflow Job dependency works
- [ ] Restart/recovery test passes
- [ ] No secrets or real payment-card data are committed

## 10. Security Notes

This repository is a sanitized public portfolio version.

**Never commit:**

- Confluent API keys/secrets
- Databricks tokens
- Gmail app passwords
- Cloud credentials
- Production customer PII
- Full card numbers

Use:

```text
producer/.env
```

for local credentials and keep it ignored by Git. In Databricks, use secret storage for Kafka/email credentials.

See [`docs/SECURITY.md`](docs/SECURITY.md).

## 11. Detailed Build Manual

For the full click-by-click implementation guide, including exactly where to click, what object to create, what file to create, what code to paste, what to run and what output to validate:

**[`docs/FinGuard_Click_by_Click_Build_Guide.docx`](docs/FinGuard_Click_by_Click_Build_Guide.docx)**

## 12. Portfolio / Resume Summary

### Short project description

**FinGuard — Real-Time Credit Card Fraud Detection Platform**

Designed and implemented an end-to-end streaming data platform using Python, Confluent Kafka, PySpark Structured Streaming and Databricks Lakeflow to process credit-card transactions in near real time. Built Bronze/Silver/Gold Delta pipelines, PostgreSQL customer enrichment, Auto Loader-based fraud-watchlist ingestion, stream-static and stream-stream joins, watermarks, windowed aggregations, automated email alerts and operational dashboards.

### Resume-style bullets

- Built a real-time credit-card transaction pipeline using **Python, Confluent Kafka, PySpark Structured Streaming and Databricks Lakeflow**, supporting continuous event ingestion and downstream fraud analytics.
- Implemented **Bronze/Silver/Gold Medallion architecture** with Delta/Unity Catalog, preserving Kafka topic, partition, offset and ingestion metadata for lineage and troubleshooting.
- Integrated **PostgreSQL customer master data** and implemented a stream-static enrichment rule to identify transactions exceeding configured customer transaction limits.
- Implemented **Auto Loader, event-time watermarks and stream-stream joins** to match live transactions against a fraud watchlist and generate alert records.
- Added **tumbling/sliding window aggregations, email notifications and Databricks dashboard monitoring** for near-real-time operational visibility.

## 13. Future Enhancements

- Schema Registry / stronger schema contracts
- Secret rotation and service accounts
- Kafka lag and pipeline-latency monitoring
- Dead-letter/quarantine handling
- Automated tests and CI/CD
- Idempotent notification delivery
- Enterprise notification service
- Data retention policies
- Alert deduplication
- Fraud model/ML scoring
- Production-grade observability

## 14. GitHub Portfolio Setup

Recommended repository name:

```text
finguard-real-time-fraud-detection
```

Recommended description:

> End-to-end real-time credit-card fraud detection platform using Confluent Kafka, PySpark Structured Streaming, Databricks Lakeflow, Delta/Unity Catalog, PostgreSQL, Auto Loader and fraud alerting.

Suggested GitHub topics:

```text
python
pyspark
spark-structured-streaming
kafka
confluent-kafka
databricks
lakeflow
delta-lake
unity-catalog
postgresql
etl
data-engineering
streaming
fraud-detection
```

For remote setup instructions, see [`docs/GITHUB_SETUP.md`](docs/GITHUB_SETUP.md).
