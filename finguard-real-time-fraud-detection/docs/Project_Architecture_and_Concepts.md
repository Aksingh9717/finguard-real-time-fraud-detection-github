# FinGuard – End-to-End Spark Streaming + Kafka + Databricks

## Production-Style Project Approach & Interview Playbook

> Current hands-on checkpoint: through Stream-Static Join (~02:45:42). Later video chapters are documented as the planned roadmap.

## Project Status

| Area | Status |
|---|---|
| Kafka + Confluent | Completed |
| Python producer | Completed |
| Spark batch/streaming Kafka read | Completed |
| Bronze/Silver transactions | Completed |
| PostgreSQL customer ingestion | Completed |
| Customer Silver | Completed |
| Stream-Static Join | Completed |
| Remaining alert/watchlist/dashboard/orchestration | Planned |

## Business Problem

FinGuard needs to process credit-card transactions continuously and identify suspicious activity quickly. Transactions arrive as streaming events, while customer master data is relatively static/reference data. The solution therefore combines Kafka, Spark Structured Streaming, Databricks Lakeflow pipelines, PostgreSQL reference data, Medallion architecture, and business-rule-driven Gold outputs.

## Architecture

```text
Python Producer
      ↓
Confluent Kafka Cluster
      ↓
credit_card_transactions
      ↓
Spark Structured Streaming
      ↓
Lakeflow Spark Declarative Pipeline
      ├── Bronze transactions
      ├── Silver transactions
      └── Gold alerts

PostgreSQL customer master
      ↓
Lakeflow Connect
      ├── Bronze customers
      └── Silver customers
                ↓
        Stream-Static Join
                ↓
      High-Value Transaction Alert
```

## Key Engineering Choices

- **PySpark:** distributed processing + Structured Streaming + DataFrame APIs.
- **Kafka:** scalable event backbone and decoupling between producers and consumers.
- **Databricks:** managed Spark environment with Lakeflow pipelines, Delta tables, Unity Catalog, notebooks and operational tooling.
- **Bronze/Silver/Gold:** separates ingestion, cleansing/validation and business outputs.
- **Checkpoints:** allow streaming progress/state to recover after restart.
- **Secrets:** keep credentials outside pipeline source code.

## Current Code

### `transactions_bronze.py`

```python
from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql.functions import col
from pyspark.sql import functions as F



@dp.table(
    name="finguard.bronze.transactions",
    comment="Transactions raw stream data ingested by kafka"
)
def transactions_bronze()->DataFrame:
    kafka_connection_json=dbutils.secrets.get(scope="finguard-scope",key="kafka_connection_details")
    kafka_config = json.loads(kafka_connection_json)
    bootstrap_servers=kafka_config['bootstrap_servers']
    api_key=kafka_config['api_key']
    api_secret=kafka_config['api_secret']
    topic=kafka_config['topic']

    jaas_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'

    streaming_df = (spark.readStream.format("kafka")
                    .option("kafka.bootstrap.servers",bootstrap_servers)
                    .option("subscribe",topic)
                    .option("kafka.security.protocol","SASL_SSL")
                    .option("kafka.sasl.mechanism","PLAIN")
                    .option("kafka.sasl.jaas.config",jaas_config)
                    .option("startingOffsets","earliest")
                    .load()
                )

    parsed_streaming_df = streaming_df.select(
        col("key").cast("string"),
        col("value").cast("string"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("timestampType"),
        F.current_timestamp().alias("ingestion_timestamp")
    )

    return parsed_streaming_df
```

### Line-by-line learning points

- `from pyspark import pipelines as dp` — Import Databricks Lakeflow Declarative Pipelines APIs and use `dp` as the short name.
- `from pyspark.sql.dataframe import DataFrame` — Import the DataFrame type so the function return type is explicit.
- `import json` — Import Python's JSON module so stored connection details can be parsed.
- `from pyspark.sql.functions import col` — Import `col()` for direct Spark column references.
- `from pyspark.sql import functions as F` — Import Spark SQL functions using the readable `F` alias.
- `@dp.table(` — Declare that the function below defines a managed Lakeflow pipeline table.
- `    name="finguard.bronze.transactions",` — Set the Unity Catalog fully qualified target: catalog.schema.table.
- `    comment="Transactions raw stream data ingested by kafka"` — Add table metadata explaining the purpose of the dataset.
- `)` — Configure or transform the pipeline for the step described in this section.
- `def transactions_bronze()->DataFrame:` — Define the transformation function that Lakeflow evaluates.
- `    kafka_connection_json=dbutils.secrets.get(scope="finguard-scope",key="kafka_connection_details")` — Retrieve Kafka connection details from Databricks secret storage instead of embedding credentials in pipeline code.
- `    kafka_config = json.loads(kafka_connection_json)` — Convert the JSON secret string into a Python dictionary.
- `    bootstrap_servers=kafka_config['bootstrap_servers']` — Read the Kafka cluster endpoint from the configuration.
- `    api_key=kafka_config['api_key']` — Read the Kafka API key/username from the configuration.
- `    api_secret=kafka_config['api_secret']` — Read the Kafka API secret/password from the configuration.
- `    topic=kafka_config['topic']` — Read the Kafka topic name from the configuration.
- `    jaas_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'` — Read the Kafka API key/username from the configuration.
- `    streaming_df = (spark.readStream.format("kafka")` — Create a Spark Structured Streaming reader using Kafka as the source.
- `                    .option("kafka.bootstrap.servers",bootstrap_servers)` — Read the Kafka cluster endpoint from the configuration.
- `                    .option("subscribe",topic)` — Read the Kafka topic name from the configuration.
- `                    .option("kafka.security.protocol","SASL_SSL")` — Use SASL over SSL for encrypted/authenticated Kafka communication.
- `                    .option("kafka.sasl.mechanism","PLAIN")` — Use the PLAIN SASL mechanism required by the Confluent connection.
- `                    .option("kafka.sasl.jaas.config",jaas_config)` — Build the Kafka SASL/PLAIN authentication string.
- `                    .option("startingOffsets","earliest")` — Choose where the Kafka consumer starts reading.
- `                    .load()` — Create the configured DataFrame reader.
- `                )` — Configure or transform the pipeline for the step described in this section.
- `    parsed_streaming_df = streaming_df.select(` — Select only the fields needed for the next layer.
- `        col("key").cast("string"),` — Convert Kafka's binary key into a readable string.
- `        col("value").cast("string"),` — Convert Kafka's binary message value into a JSON string.
- `        col("topic"),` — Read the Kafka topic name from the configuration.
- `        col("partition"),` — Keep the Kafka partition number for lineage and troubleshooting.
- `        col("offset"),` — Keep the Kafka offset so the exact Kafka message position is traceable.
- `        col("timestamp"),` — Keep Kafka's event/record timestamp.
- `        col("timestampType"),` — Keep Kafka timestamp-type metadata.
- `        F.current_timestamp().alias("ingestion_timestamp")` — Add the timestamp at which Databricks ingested the record.
- `    )` — Configure or transform the pipeline for the step described in this section.
- `    return parsed_streaming_df` — Return the final DataFrame so the pipeline can materialize the table.

### `transactions_silver.py`

```python
from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType
from pyspark.sql.functions import *



@dp.table(
    name="finguard.silver.transactions",
    comment="Parsed and cleaned transactions Data"
)
#data quality checks
@dp.expect_or_drop("valid_transaction_id","transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id","customer_id IS NOT NULL")
@dp.expect_or_drop("valid_card_number","card_number IS NOT NULL")
@dp.expect_or_drop("valid_merchant_id","merchant_id IS NOT NULL")
@dp.expect("valid_amount","amount > 0")

def transactions_bronze()->DataFrame:
    bronze_df=spark.readStream.table("finguard.bronze.transactions")

    

    #transform the bronze data
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("card_number", StringType()),
        StructField("merchant_id", StringType()),
        StructField("merchant_name", StringType()),
        StructField("merchant_category", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("transaction_type", StringType()),
        StructField("payment_channel", StringType()),
        StructField("device_id", StringType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("transaction_timestamp", StringType()),
        StructField("is_international", BooleanType()),
        StructField("status", StringType())
    ])
    transformed_df = bronze_df.select(
        F.from_json(col("value"), schema).alias("data")
        ,F.col("topic").alias("kafka_topic")
        ,F.col("partition").alias("kafka_partition")
        ,F.col("offset").alias("kafka_offset")
        ,F.col("timestamp").alias("kafka_timestamp")
        ,F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp")
    ).select(
        F.col("data.*")
        ,F.col("kafka_topic")
        ,F.col("kafka_partition")
        ,F.col("kafka_offset")
        ,F.col("kafka_timestamp")
        ,F.col("bronze_ingestion_timestamp")
        ,F.current_timestamp().alias("silver_ingestion_timestamp")
    )

    return transformed_df
```

### Line-by-line learning points

- `from pyspark import pipelines as dp` — Import Databricks Lakeflow Declarative Pipelines APIs and use `dp` as the short name.
- `from pyspark.sql.dataframe import DataFrame` — Import the DataFrame type so the function return type is explicit.
- `import json` — Import Python's JSON module so stored connection details can be parsed.
- `from pyspark.sql.functions import col` — Import `col()` for direct Spark column references.
- `from pyspark.sql import functions as F` — Import Spark SQL functions using the readable `F` alias.
- `from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType` — Import Spark SQL data types used by the explicit schema.
- `from pyspark.sql.functions import *` — Import Spark functions directly; this works, although explicit `F.` usage is usually easier to maintain.
- `@dp.table(` — Declare that the function below defines a managed Lakeflow pipeline table.
- `    name="finguard.silver.transactions",` — Set the Unity Catalog fully qualified target: catalog.schema.table.
- `    comment="Parsed and cleaned transactions Data"` — Add table metadata explaining the purpose of the dataset.
- `)` — Configure or transform the pipeline for the step described in this section.
- `#data quality checks` — Configure or transform the pipeline for the step described in this section.
- `@dp.expect_or_drop("valid_transaction_id","transaction_id IS NOT NULL")` — Add a data-quality rule; records failing the rule are removed from the output.
- `@dp.expect_or_drop("valid_customer_id","customer_id IS NOT NULL")` — Add a data-quality rule; records failing the rule are removed from the output.
- `@dp.expect_or_drop("valid_card_number","card_number IS NOT NULL")` — Add a data-quality rule; records failing the rule are removed from the output.
- `@dp.expect_or_drop("valid_merchant_id","merchant_id IS NOT NULL")` — Add a data-quality rule; records failing the rule are removed from the output.
- `@dp.expect("valid_amount","amount > 0")` — Add a data-quality rule that records failures without dropping the row.
- `def transactions_bronze()->DataFrame:` — Define the transformation function that Lakeflow evaluates.
- `    bronze_df=spark.readStream.table("finguard.bronze.transactions")` — Read an upstream Delta/Lakeflow table incrementally as a streaming DataFrame.
- `    #transform the bronze data` — Configure or transform the pipeline for the step described in this section.
- `    schema = StructType([` — Create the top-level Spark schema for the JSON payload.
- `        StructField("transaction_id", StringType()),` — Define one JSON field and its Spark data type.
- `        StructField("customer_id", StringType()),` — Define one JSON field and its Spark data type.
- `        StructField("card_number", StringType()),` — Define one JSON field and its Spark data type.
- `        StructField("merchant_id", StringType()),` — Define one JSON field and its Spark data type.
- `        StructField("merchant_name", StringType()),` — Define one JSON field and its Spark data type.
- `        StructField("merchant_category", StringType()),` — Define one JSON field and its Spark data type.
- `        StructField("amount", DoubleType()),` — Define one JSON field and its Spark data type.
- `        StructField("currency", StringType()),` — Define one JSON field and its Spark data type.
- `        StructField("transaction_type", StringType()),` — Define one JSON field and its Spark data type.
- `        StructField("payment_channel", StringType()),` — Define one JSON field and its Spark data type.
- `        StructField("device_id", StringType()),` — Define one JSON field and its Spark data type.
- `        StructField("city", StringType()),` — Define one JSON field and its Spark data type.
- `        StructField("country", StringType()),` — Define one JSON field and its Spark data type.
- `        StructField("transaction_timestamp", StringType()),` — Define one JSON field and its Spark data type.
- `        StructField("is_international", BooleanType()),` — Define one JSON field and its Spark data type.
- `        StructField("status", StringType())` — Define one JSON field and its Spark data type.
- `    ])` — Configure or transform the pipeline for the step described in this section.
- `    transformed_df = bronze_df.select(` — Select only the fields needed for the next layer.
- `        F.from_json(col("value"), schema).alias("data")` — Parse the JSON string into a structured Spark column using the explicit schema.
- `        ,F.col("topic").alias("kafka_topic")` — Read the Kafka topic name from the configuration.
- `        ,F.col("partition").alias("kafka_partition")` — Keep the Kafka partition number for lineage and troubleshooting.
- `        ,F.col("offset").alias("kafka_offset")` — Keep the Kafka offset so the exact Kafka message position is traceable.
- `        ,F.col("timestamp").alias("kafka_timestamp")` — Keep Kafka's event/record timestamp.
- `        ,F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp")` — Rename a derived expression to a clear business or lineage column name.
- `    ).select(` — Select only the fields needed for the next layer.
- `        F.col("data.*")` — Expand the parsed JSON struct so each JSON attribute becomes a normal column.
- `        ,F.col("kafka_topic")` — Read the Kafka topic name from the configuration.
- `        ,F.col("kafka_partition")` — Select the `kafka_partition` column from the DataFrame and carry it into the target layer.
- `        ,F.col("kafka_offset")` — Select the `kafka_offset` column from the DataFrame and carry it into the target layer.
- `        ,F.col("kafka_timestamp")` — Select the `kafka_timestamp` column from the DataFrame and carry it into the target layer.
- `        ,F.col("bronze_ingestion_timestamp")` — Select the `bronze_ingestion_timestamp` column from the DataFrame and carry it into the target layer.
- `        ,F.current_timestamp().alias("silver_ingestion_timestamp")` — Add the timestamp at which the record entered the Silver transformation.
- `    )` — Configure or transform the pipeline for the step described in this section.
- `    return transformed_df` — Return the final DataFrame so the pipeline can materialize the table.

### `customers_silver.py`

```python
from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import *



@dp.table(
    name="finguard.silver.customers",
    comment="Parsed and cleaned transactionscustomer Data"
)

#data quality checks
@dp.expect_or_drop("valid_customer_id","customer_id IS NOT NULL")

def customers_silver() -> DataFrame:
    bronze_df = spark.readStream.table("finguard.bronze.customers")

    transformed_df=bronze_df.select(
        F.col("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("gender"),
        F.col("age"),
        F.col("city"),
        F.col("state"),
        F.col("country"),
        F.col("annual_income"),
        F.col("customer_segment"),
        F.to_date(F.col("account_open_date"), "yyyy-MM-dd").alias("account_open_date"),
        F.col("risk_score"),
        F.col("preferred_spending_min"),
        F.col("preferred_spending_max"),
        F.col("preferred_city"),
        F.col("preferred_country"),
        F.col("trusted_device_id"),
        F.col("card_number"),
        F.col("card_type"),
        F.col("email"),
        F.col("transaction_limit"),
        F.current_timestamp().alias("silver_ingestion_timestamp")
        
    )
    return transformed_df
```

### Line-by-line learning points

- `from pyspark import pipelines as dp` — Import Databricks Lakeflow Declarative Pipelines APIs and use `dp` as the short name.
- `from pyspark.sql.dataframe import DataFrame` — Import the DataFrame type so the function return type is explicit.
- `from pyspark.sql import functions as F` — Import Spark SQL functions using the readable `F` alias.
- `from pyspark.sql.functions import *` — Import Spark functions directly; this works, although explicit `F.` usage is usually easier to maintain.
- `@dp.table(` — Declare that the function below defines a managed Lakeflow pipeline table.
- `    name="finguard.silver.customers",` — Set the Unity Catalog fully qualified target: catalog.schema.table.
- `    comment="Parsed and cleaned transactionscustomer Data"` — Add table metadata explaining the purpose of the dataset.
- `)` — Configure or transform the pipeline for the step described in this section.
- `#data quality checks` — Configure or transform the pipeline for the step described in this section.
- `@dp.expect_or_drop("valid_customer_id","customer_id IS NOT NULL")` — Add a data-quality rule; records failing the rule are removed from the output.
- `def customers_silver() -> DataFrame:` — Define the transformation function that Lakeflow evaluates.
- `    bronze_df = spark.readStream.table("finguard.bronze.customers")` — Read an upstream Delta/Lakeflow table incrementally as a streaming DataFrame.
- `    transformed_df=bronze_df.select(` — Select only the fields needed for the next layer.
- `        F.col("customer_id"),` — Select the `customer_id` column from the DataFrame and carry it into the target layer.
- `        F.col("first_name"),` — Select the `first_name` column from the DataFrame and carry it into the target layer.
- `        F.col("last_name"),` — Select the `last_name` column from the DataFrame and carry it into the target layer.
- `        F.col("gender"),` — Select the `gender` column from the DataFrame and carry it into the target layer.
- `        F.col("age"),` — Select the `age` column from the DataFrame and carry it into the target layer.
- `        F.col("city"),` — Select the `city` column from the DataFrame and carry it into the target layer.
- `        F.col("state"),` — Select the `state` column from the DataFrame and carry it into the target layer.
- `        F.col("country"),` — Select the `country` column from the DataFrame and carry it into the target layer.
- `        F.col("annual_income"),` — Select the `annual_income` column from the DataFrame and carry it into the target layer.
- `        F.col("customer_segment"),` — Select the `customer_segment` column from the DataFrame and carry it into the target layer.
- `        F.to_date(F.col("account_open_date"), "yyyy-MM-dd").alias("account_open_date"),` — Convert the customer account-open date into a Spark DateType using the supplied format.
- `        F.col("risk_score"),` — Select the `risk_score` column from the DataFrame and carry it into the target layer.
- `        F.col("preferred_spending_min"),` — Select the `preferred_spending_min` column from the DataFrame and carry it into the target layer.
- `        F.col("preferred_spending_max"),` — Select the `preferred_spending_max` column from the DataFrame and carry it into the target layer.
- `        F.col("preferred_city"),` — Select the `preferred_city` column from the DataFrame and carry it into the target layer.
- `        F.col("preferred_country"),` — Select the `preferred_country` column from the DataFrame and carry it into the target layer.
- `        F.col("trusted_device_id"),` — Select the `trusted_device_id` column from the DataFrame and carry it into the target layer.
- `        F.col("card_number"),` — Select the `card_number` column from the DataFrame and carry it into the target layer.
- `        F.col("card_type"),` — Select the `card_type` column from the DataFrame and carry it into the target layer.
- `        F.col("email"),` — Select the `email` column from the DataFrame and carry it into the target layer.
- `        F.col("transaction_limit"),` — Select the `transaction_limit` column from the DataFrame and carry it into the target layer.
- `        F.current_timestamp().alias("silver_ingestion_timestamp")` — Add the timestamp at which the record entered the Silver transformation.
- `    )` — Configure or transform the pipeline for the step described in this section.
- `    return transformed_df` — Return the final DataFrame so the pipeline can materialize the table.

### `high_value_transactions_alert.py`

```python
from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType
from pyspark.sql.functions import *
from pyspark.sql import functions as F


@dp.table(
    name="finguard.gold.high_value_transactions_alert",
    comment="Alert Details where transaction has been performed with value higher than what is configured by customer"
)

def high_value_transactions_alert()->DataFrame:

    transactions=spark.readStream.table("finguard.silver.transactions")
    customers=spark.read.table("finguard.silver.customers")

    joined_df = (transactions.join(customers, transactions.customer_id == customers.customer_id,"left")
                    .filter(transactions.amount > F.col("transaction_limit"))
                    .select(
                        F.concat_ws("-",F.lit("ALERT"),F.col("transaction_id")).alias("alert_id"),
                        F.lit("HIGH VALUE TRANSACTION").alias("alert_type"),
                        F.current_timestamp().alias("alert_timestamp"),

                        transactions.transaction_id,
                        transactions.customer_id,
                        customers.email.alias("customer_email"),
                        F.concat_ws(" ",F.col("first_name"),F.col("last_name")).alias("customer_name"),
                    transactions.amount.alias("transaction_amount"),
                    customers.transaction_limit,
                    transactions.currency,
                    transactions.merchant_name,
                    transactions.merchant_category,
                    transactions.transaction_type,
                    transactions.payment_channel,
                    transactions.city,
                    transactions.country,
                    transactions.is_international,
                    transactions.transaction_timestamp,
                    transactions.status
             
                )
    )
    return joined_df
```

### Line-by-line learning points

- `from pyspark import pipelines as dp` — Import Databricks Lakeflow Declarative Pipelines APIs and use `dp` as the short name.
- `from pyspark.sql.dataframe import DataFrame` — Import the DataFrame type so the function return type is explicit.
- `import json` — Import Python's JSON module so stored connection details can be parsed.
- `from pyspark.sql.functions import col` — Import `col()` for direct Spark column references.
- `from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType` — Import Spark SQL data types used by the explicit schema.
- `from pyspark.sql.functions import *` — Import Spark functions directly; this works, although explicit `F.` usage is usually easier to maintain.
- `from pyspark.sql import functions as F` — Import Spark SQL functions using the readable `F` alias.
- `@dp.table(` — Declare that the function below defines a managed Lakeflow pipeline table.
- `    name="finguard.gold.high_value_transactions_alert",` — Set the Unity Catalog fully qualified target: catalog.schema.table.
- `    comment="Alert Details where transaction has been performed with value higher than what is configured by customer"` — Add table metadata explaining the purpose of the dataset.
- `)` — Configure or transform the pipeline for the step described in this section.
- `def high_value_transactions_alert()->DataFrame:` — Define the transformation function that Lakeflow evaluates.
- `    transactions=spark.readStream.table("finguard.silver.transactions")` — Read an upstream Delta/Lakeflow table incrementally as a streaming DataFrame.
- `    customers=spark.read.table("finguard.silver.customers")` — Read a reference table as static data for the stream-static join.
- `    joined_df = (transactions.join(customers, transactions.customer_id == customers.customer_id,"left")` — Join the transaction stream to the customer reference data.
- `                    .filter(transactions.amount > F.col("transaction_limit"))` — Keep only rows that satisfy the fraud/business condition.
- `                    .select(` — Select only the fields needed for the next layer.
- `                        F.concat_ws("-",F.lit("ALERT"),F.col("transaction_id")).alias("alert_id"),` — Rename a derived expression to a clear business or lineage column name.
- `                        F.lit("HIGH VALUE TRANSACTION").alias("alert_type"),` — Rename a derived expression to a clear business or lineage column name.
- `                        F.current_timestamp().alias("alert_timestamp"),` — Rename a derived expression to a clear business or lineage column name.
- `                        transactions.transaction_id,` — Select a source column explicitly so the output schema remains clear and controlled.
- `                        transactions.customer_id,` — Select a source column explicitly so the output schema remains clear and controlled.
- `                        customers.email.alias("customer_email"),` — Rename a derived expression to a clear business or lineage column name.
- `                        F.concat_ws(" ",F.col("first_name"),F.col("last_name")).alias("customer_name"),` — Rename a derived expression to a clear business or lineage column name.
- `                    transactions.amount.alias("transaction_amount"),` — Rename a derived expression to a clear business or lineage column name.
- `                    customers.transaction_limit,` — Select a source column explicitly so the output schema remains clear and controlled.
- `                    transactions.currency,` — Select a source column explicitly so the output schema remains clear and controlled.
- `                    transactions.merchant_name,` — Select a source column explicitly so the output schema remains clear and controlled.
- `                    transactions.merchant_category,` — Select a source column explicitly so the output schema remains clear and controlled.
- `                    transactions.transaction_type,` — Select a source column explicitly so the output schema remains clear and controlled.
- `                    transactions.payment_channel,` — Select a source column explicitly so the output schema remains clear and controlled.
- `                    transactions.city,` — Select a source column explicitly so the output schema remains clear and controlled.
- `                    transactions.country,` — Select a source column explicitly so the output schema remains clear and controlled.
- `                    transactions.is_international,` — Select a source column explicitly so the output schema remains clear and controlled.
- `                    transactions.transaction_timestamp,` — Select a source column explicitly so the output schema remains clear and controlled.
- `                    transactions.status` — Select a source column explicitly so the output schema remains clear and controlled.
- `                )` — Configure or transform the pipeline for the step described in this section.
- `    )` — Configure or transform the pipeline for the step described in this section.
- `    return joined_df` — Return the final DataFrame so the pipeline can materialize the table.

## Interview Questions

**Q: Why did you use streaming instead of batch?**

**A:** Because credit-card transactions arrive continuously and fraud detection is time-sensitive. A daily/hourly batch would increase detection latency. Structured Streaming lets us process newly arriving events continuously or in micro-batches.

**Q: Why Kafka?**

**A:** Kafka provides a durable, scalable event-streaming layer between transaction producers and downstream consumers. It decouples the producer from the Spark processing layer.

**Q: What is a Kafka topic?**

**A:** A topic is a logical channel where messages are published and consumed. In this project the transaction events are published to credit_card_transactions.

**Q: Why partitions?**

**A:** Partitions allow Kafka to scale reads/writes and support parallel processing. Each partition is an ordered log and each record has an offset within that partition.

**Q: What is an offset?**

**A:** An offset is the position of a record inside a Kafka partition. Partition plus offset identifies a record position in the topic.

**Q: Why startingOffsets = earliest?**

**A:** For the initial test we wanted to read the records already present in the topic, not only records arriving after the query starts.

**Q: Why is Kafka key/value binary in Spark?**

**A:** The Kafka connector exposes the raw key and value as binary. We cast them to string because the key is a transaction ID and the value contains JSON text.

**Q: Why StructType/StructField?**

**A:** The JSON contract is known, so an explicit schema gives deterministic field names and Spark data types and avoids relying on inference in a streaming pipeline.

**Q: What is @dp.table?**

**A:** It declares that the function defines a managed table in a Lakeflow Spark Declarative Pipeline. The function returns the DataFrame that represents the table's transformation.

**Q: Why use a pipeline instead of only a notebook?**

**A:** A notebook is excellent for development and testing. A managed pipeline adds dependency management, table materialization, data-quality expectations, monitoring, and operationalization.

**Q: Why Bronze/Silver/Gold?**

**A:** Bronze preserves source-level data and lineage, Silver provides clean/structured validated data, and Gold provides business-ready outputs such as fraud alerts.

**Q: What is a checkpoint?**

**A:** Persistent streaming metadata that tracks progress and state so a streaming query can recover and continue correctly after restart.

**Q: What is a stream-static join?**

**A:** A continuously arriving stream is joined with a relatively stable reference dataset. Here transactions are the stream and customers are the static reference.

**Q: Why left join?**

**A:** We want to retain the transaction even if a customer record is missing, so the transaction remains visible for data-quality investigation.

**Q: What is the business rule in your current Gold table?**

**A:** If a transaction amount is greater than the customer's configured transaction limit, we create a high-value transaction alert record.

**Q: What does @dp.expect_or_drop do?**

**A:** It defines a data-quality expectation and drops records that fail it.

**Q: What does @dp.expect do?**

**A:** It records whether the expectation was met but does not automatically drop the row.

**Q: Why use secrets?**

**A:** Credentials should not be embedded in source code or notebooks. Secret management separates sensitive configuration from transformation logic.

**Q: What would you improve before production?**

**A:** Credential rotation/secret management, stronger DQ, timestamp typing, naming cleanup, tests, monitoring, alerting, CI/CD, least privilege, environment separation, and a documented recovery/replay strategy.

## Scenario Questions

**Scenario:** Kafka producer is sending messages, but Spark reads zero. What do you check?

**Approach:** Verify cluster endpoint, topic, API credentials, SASL_SSL settings, topic existence, starting offsets, and whether the producer and Spark consumer point to the same Confluent environment.

**Scenario:** The pipeline processed 100,000 transactions yesterday and after restart it processes them again. What could be wrong?

**Approach:** Check whether the checkpoint location changed, was deleted, or belongs to another query. A stable checkpoint is required for correct progress tracking.

**Scenario:** A transaction arrives but customer enrichment is null. What do you investigate?

**Approach:** Check customer_id data type and formatting, whether the customer exists in Silver, whether the customer pipeline has refreshed, and whether the join is using the intended key.

**Scenario:** A malformed JSON transaction arrives. What should happen?

**Approach:** The parsing layer should be designed to handle malformed data explicitly: capture/quarantine bad records, monitor the failure count, and prevent corrupted data from silently entering Gold.

**Scenario:** The business says fraud alerts are arriving 10 minutes late. How do you troubleshoot?

**Approach:** Measure source-to-Kafka latency, Kafka backlog/consumer lag, Spark processing latency, trigger interval, stateful operations, join performance, checkpoint/storage latency, and downstream email latency.

**Scenario:** The customer table grows from 1 million to 100 million rows. Is the same stream-static join automatically safe?

**Approach:** Not necessarily. Review reference-table size, join strategy, refresh frequency, partitioning/data layout, memory pressure, and whether a more specialized enrichment architecture is required.

**Scenario:** A developer changes the transaction schema and the pipeline fails. What do you do?

**Approach:** Compare source schema, explicit StructType, target schema, and checkpoint/state compatibility. Determine whether the change is additive, breaking, or requires a controlled full refresh/rebuild.

## Remaining Roadmap

- `02:45:42` — Gmail SMTP App password set up

- `02:53:24` — Real time alert using SDP pipeline

- `03:09:10` — Streaming JSON files setup

- `03:15:17` — Streaming file ingestion using Auto Loader

- `03:23:15` — SDP - Streaming files load source to bronze

- `03:32:25` — fraud watchlist - bronze to silver load

- `03:39:24` — Stateless vs Stateful stream processing

- `03:43:15` — Using watermark in streaming

- `03:47:13` — Stream Stream Join

- `03:55:44` — Email alert for fraud watchlist

- `04:05:32` — Types of windows in Streaming data

- `04:08:46` — Aggregations on Streaming Data

- `04:20:34` — End to End Streaming Data Flow

- `04:25:01` — Dashboard with near real time data refresh

- `04:33:45` — End to end orchestration with Lakeflow jobs

- `04:38:00` — Conclusion


## GitHub Security Rule
Never commit `.env`, API keys, API secrets, passwords, tokens or private connection strings. Rotate any credential that was exposed during development.
