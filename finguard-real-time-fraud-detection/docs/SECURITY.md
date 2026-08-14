# Security and Data Handling

This repository is intentionally sanitized for public sharing.

## Never commit

- Confluent API keys or API secrets
- Databricks PATs/tokens
- Gmail passwords or app passwords
- Cloud credentials
- Full payment-card numbers
- Production customer PII
- Private connection strings

## Local configuration

Copy `producer/.env.example` to `producer/.env` and fill in your own development credentials. `.env` is ignored by Git.

## Databricks

Store Kafka and email credentials in Databricks secret storage. The pipeline code should retrieve secrets at runtime rather than hard-code them.

## Card data

Use masked/test card values only. This project is a learning/demo implementation and must not be used with real cardholder data without appropriate security, compliance and governance controls.
