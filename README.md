# Data Portfolio

End-to-end AWS data engineering and analytics projects, built for real and documented as a series of Medium posts. Each project deliberately uses a different AWS architecture and analytics stack — the point is knowing when to reach for which combination.

| # | Project | Stack | Status | Post |
|---|---------|-------|--------|------|
| 1 | [Cyclistic Pipeline](./cyclistic-pipeline/) | AWS Lambda · S3 · Python (pandas) | Published | [Medium](https://andrewndaniels0.medium.com/event-driven-etl-for-bike-share-data-with-aws-lambda-and-python-9fa838c089cd) |
| 2 | [Cyclistic Analytics](./cyclistic-analytics/) | Amazon RDS (PostgreSQL) · R · Tableau | In progress | — |
| 3 | [Bellabeat Pipeline](./bellabeat-pipeline/) | AWS Step Functions · EMR · PySpark | Planned | — |
| 4 | [Bellabeat Analytics](./bellabeat-analytics/) | Amazon Redshift · Jupyter (Python) · Power BI | Planned | — |

Medium links land in the table as each post publishes.

## The projects

**1. Cyclistic Pipeline — event-driven serverless ETL.** Twelve months of Chicago bike-share trip data, cleaned the moment it lands: an S3 event notification triggers a Lambda function that normalizes schemas across releases, derives ride metrics, filters bad records, and writes to a processed zone.

**2. Cyclistic Analytics — relational warehouse analysis.** The cleaned trip data loaded into RDS PostgreSQL, analyzed with SQL and R, and presented as a Tableau dashboard answering the business question: how do annual members and casual riders use the bikes differently?

**3. Bellabeat Pipeline — orchestrated distributed processing.** Multi-grain Fitbit CSVs reconciled into a single analytics-ready Parquet dataset by a Step Functions state machine that provisions an EMR cluster, runs a PySpark join job, validates output, and tears the cluster down.

**4. Bellabeat Analytics — cloud warehouse + BI.** The joined wellness data loaded into Redshift, explored in Jupyter, and delivered as a Power BI dashboard with marketing recommendations for the Bellabeat Leaf.

## Principles

- Everything published is real: committed code, pipelines that actually ran, real screenshots, real numbers from real output.
- Infrastructure is provisioned via CloudFormation, with templates committed in each project folder.
- No credentials, account IDs, or live endpoint hostnames in code or screenshots.

## Links

- Medium: [andrewndaniels0.medium.com](https://andrewndaniels0.medium.com)

*Business scenarios adapted from the Google Data Analytics Capstone case studies (Cyclistic, Bellabeat).*
