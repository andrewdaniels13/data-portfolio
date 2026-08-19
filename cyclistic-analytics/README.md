# Cyclistic Analytics — RDS PostgreSQL, R, and Tableau

**Stack:** Amazon RDS (PostgreSQL) · SQL · R · Tableau · CloudFormation
**Status:** Not started (begins after the Cyclistic Pipeline post publishes)

Analysis layer for the cleaned Cyclistic trip data: loaded into RDS PostgreSQL, aggregated with SQL (including percentile/median work), analyzed in R for distribution and statistical testing, and presented as a five-view Tableau dashboard.

## Planned contents

- `schema.sql` — table creation, indexes, views
- `load_data.sh` — COPY load from S3
- `analysis_queries.sql` — every aggregation query referenced in the post
- `analysis.R` — distribution analysis and statistical tests
- `cyclistic_dashboard.twbx` — packaged Tableau workbook
- `template.yaml` — CloudFormation for the RDS instance
- `screenshots/`

Write-up: Medium post link added on publish. Tableau Public link added when the dashboard goes live.
