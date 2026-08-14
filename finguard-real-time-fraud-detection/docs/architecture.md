# FinGuard Architecture

```text
Python Transaction Producers
        |
        v
Confluent Kafka
credit_card_transactions (6 partitions)
        |
        v
Spark Structured Streaming / Lakeflow
        |
        +--> Bronze Transactions
        |        |
        |        v
        |    Silver Transactions <----- Customer Silver
        |        |
        |        +--> High-Value Rule --> Gold Alert --> Email
        |
        +--> Window Aggregations --> Dashboard

Fraud Watchlist JSON Files
        |
        v
Auto Loader
        |
        +--> Watchlist Bronze
        |        |
        |        v
        |    Watchlist Silver
        |        |
        |        v
        +--> Watermark + Stream-Stream Join
                    |
                    v
              Fraud Card Alert
                    |
                    v
                  Email

PostgreSQL Customer Master
        |
        v
Lakeflow Connect
        |
        +--> Customer Bronze
        +--> Customer Silver
```
