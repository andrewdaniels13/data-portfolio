# Cyclistic Pipeline — Event-Driven ETL with AWS Lambda

**Stack:** AWS Lambda · Amazon S3 · Python (pandas) · CloudFormation
**Status:** In progress

Serverless pipeline that cleans monthly Cyclistic (Divvy) bike-share trip CSVs the moment they land in S3. An S3 event notification invokes a Lambda function that normalizes schemas, derives `ride_length` and `day_of_week`, filters outlier rides, and writes cleaned output to a processed zone.

## Planned contents

- `lambda_function.py` — the Lambda handler (pandas cleaning logic)
- `requirements.txt` — Python dependencies
- `template.yaml` — CloudFormation: S3 bucket + prefixes, least-privilege Lambda role, function, S3 event trigger
- `iam_policy.json` / `s3_event_config.json` — exported configuration artifacts
- `screenshots/` — console and CloudWatch evidence
- Architecture diagram, deployment steps, and sample log output (this README, expanded)

Write-up: Medium post link added on publish.
