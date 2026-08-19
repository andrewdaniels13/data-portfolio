# Bellabeat Pipeline — Step Functions, EMR, and PySpark

**Stack:** AWS Step Functions · Amazon EMR · PySpark · AWS Lambda · CloudFormation
**Status:** Not started

Orchestrated pipeline that reconciles multi-grain Fitbit CSVs into a single analytics-ready Parquet dataset. A five-state Step Functions state machine inventories raw files, provisions an EMR cluster, runs the PySpark join job, validates output, and terminates the cluster.

## Planned contents

- `state_machine.json` — full Step Functions ASL definition
- `fitbit_etl.py` — the PySpark job
- `inventory_lambda.py` / `validate_lambda.py` — helper Lambdas
- `emr_config.json` — cluster configuration
- `template.yaml` — CloudFormation
- `screenshots/`

Write-up: Medium post link added on publish.
