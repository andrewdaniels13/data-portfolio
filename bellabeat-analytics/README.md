# Bellabeat Analytics — Redshift, Jupyter, and Power BI

**Stack:** Amazon Redshift · Redshift Spectrum · SQL · Python (Jupyter) · Power BI
**Status:** Not started

Analysis layer for the joined Bellabeat wellness data: validated with Redshift Spectrum, bulk-loaded via COPY, aggregated in SQL, explored in a Jupyter notebook (pandas, seaborn, scipy), and delivered as a four-view Power BI dashboard.

## Planned contents

- `schema.sql` — Redshift DDL with sort/dist key choices
- `copy_from_s3.sql` / `spectrum_external_table.sql` — load and validation scripts
- `analysis_queries.sql` — aggregation queries referenced in the post
- `bellabeat_analysis.ipynb` — full exploratory notebook
- `bellabeat_dashboard.pbix` — Power BI report file
- `screenshots/`

Write-up: Medium post link added on publish. Notebook nbviewer link added when committed.
