# GitHub Repository Setup

## Recommended repository name

`finguard-real-time-fraud-detection`

## Recommended description

End-to-end real-time credit-card fraud detection platform using Confluent Kafka, PySpark Structured Streaming, Databricks Lakeflow, Delta/Unity Catalog, PostgreSQL, Auto Loader and fraud alerting.

## Recommended visibility

Public — this is a portfolio project. Confirm that all data and credentials are synthetic/sanitized before publishing.

## Recommended topics

`python` `pyspark` `spark-structured-streaming` `kafka` `confluent-kafka` `databricks` `lakeflow` `delta-lake` `unity-catalog` `postgresql` `etl` `data-engineering` `streaming` `fraud-detection`

## Create the remote repository

On GitHub:

1. Click **New repository**.
2. Repository name: `finguard-real-time-fraud-detection`.
3. Description: use the description above.
4. Select **Public** if you want to use it as a portfolio project.
5. Do **not** initialize it with another README, `.gitignore`, or license because this local repository already contains them.
6. Create the repository.

## Connect this local repository

From the repository folder:

```bash
git remote add origin https://github.com/<YOUR_GITHUB_USERNAME>/finguard-real-time-fraud-detection.git
git push -u origin main
```

Verify:

```bash
git remote -v
git status
git log --oneline -1
```

## What should appear on the GitHub front page

GitHub will automatically render `README.md`. The first screen should show:

1. Project title
2. One-line technology summary
3. Architecture image
4. Business problem
5. Solution
6. Technology stack
7. Architecture/data flow
8. Step-by-step build
9. Business impact
10. Validation checklist
11. Security notes
12. Resume-ready project bullets

## What should NOT be pushed

Do not push:

- `.env`
- API keys/secrets
- Databricks tokens
- Gmail app passwords
- real customer PII
- real card numbers
- private cloud connection strings
- local IDE folders
- generated runtime logs

The repository includes `.gitignore` and `docs/SECURITY.md` for this purpose.
