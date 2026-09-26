# Enterprise Metadata-Driven Incremental Ingestion Pipeline (ADF & Azure SQL)

## Project Overview
This repository contains a production-grade, end-to-end cloud data ingestion pipeline built using **Azure Data Factory (ADF)**, **Azure SQL Database**, and **Azure Data Lake Storage Gen2 (ADLS Gen2)**.

Instead of creating hardcoded, high-maintenance pipelines for individual tables, this project implements a scalable, **metadata-driven framework** that uses a dynamic watermark control loop and a database view abstraction layer to identify and capture delta changes (incremental/CDC-style logic) across an enterprise dataset scaled to **1,000,000+ records**.

---
## Architecture
architecture/ architecture.md

## Key Technical Achievements & Architecture
- **High Volume Scalability:** Built and validated the ingestion logic against a synthetic insurance transactions dataset scaled to over **1,000,000 rows**, confirmed via pipeline monitoring (75.245 MB read, 1,000,006 rows read/written in a single run).
- **Metadata-Driven Framework:** Orchestrated the pipeline using a centralized SQL control table (`WatermarkTable`) that tracks the last successfully processed extraction boundary per source table — no hardcoded date filters.
- **Database View Abstraction Layer:** Implemented a presentation-layer view (`Customer_Transactions_View`) to decouple the ADF orchestration layer from the underlying table schema, improving security and query performance.
- **Self-Updating Watermark:** After every successful load, a stored procedure (`usp_write_watermark`) writes the new high-watermark value back to the control table, so the next run automatically picks up only new/changed rows.
- **Optimized Columnar Storage:** Sink layer writes the delta output directly into compressed, analytics-ready `.parquet` format in ADLS Gen2 (23.071 MB written from 75.245 MB source — ~69% compression).
- **Validated Incrementality:** Manually inserted a new test row into the source table and re-ran the pipeline to confirm only the new record was picked up on the next incremental run.

---

## Data Architecture & Pipeline Flow

**Pipeline: `p_ingestion_incremental`**

1. **`Lkp_get_old_watermark`** (Lookup) — queries `WatermarkTable` to fetch the last successfully processed timestamp boundary (`WatermarkValue`) for `Customer_Transactions`.
2. **`Lkp_get_new_watermark`** (Lookup) — runs `SELECT MAX(LastModifiedDate) AS NewWatermarkValue FROM Customer_Transactions_View` to calculate the current upper boundary for this run.
3. **`Copy delta_data`** (Copy Data) — dynamically filters and copies only the delta rows using the two watermark values, writing the output as Parquet into ADLS Gen2:
   ```sql
   SELECT * FROM Customer_Transactions_View
   WHERE LastModifiedDate > '@{activity('Lkp_get_old_watermark').output.firstRow.WatermarkValue}'
   AND LastModifiedDate <= '@{activity('Lkp_get_new_watermark').output.firstRow.NewWatermarkValue}'
   ```
4. **`Stored procedure1`** (`usp_write_watermark`) — writes the new watermark value back to `WatermarkTable`, using `@LastModifiedDate` and `@TableName` as input parameters, closing the loop for the next run.

**Source → Sink:** Azure SQL Database (`sanjaydb`) → Azure Data Lake Storage Gen2 (Parquet)

---

## Repository Structure
```
├── architecture/          # Pipeline flow diagram(s)
├── azure-data-factory/    # ADF pipeline JSON / exported definitions
├── sql/                   # WatermarkTable DDL, Customer_Transactions_View, usp_write_watermark
├── screenshots/           # Pipeline canvas, monitoring runs, SQL query editor, linked services
├── docs/                  # Additional notes
├── LICENSE
└── README.md
```

---

## Tech Stack
| Layer | Service |
|---|---|
| Orchestration | Azure Data Factory |
| Source | Azure SQL Database |
| Sink | Azure Data Lake Storage Gen2 (Parquet) |
| Control table | Azure SQL (`WatermarkTable`) |
| Linked services configured | 3× ADLS Gen2, 1× Azure SQL Database, 1× Azure Databricks (Delta Lake) — set up for a future Databricks transformation layer |

---

## Testing & Validation
- Ran the pipeline in Debug mode; all four activities (`Lkp_get_old_watermark`, `Lkp_get_new_watermark`, `Copy delta_data`, `Stored procedure1`) succeeded.
- Verified copy activity performance via ADF's monitoring "Details" view:
  - Rows read/written: 1,000,006
  - Data read: 75.245 MB → Data written: 23.071 MB
  - Copy duration: 22s, Throughput: 5.788 MB/s
- Manually inserted a new row into `Customer_Transactions` via the Azure SQL Query editor and confirmed the next pipeline run picked it up as a new delta, proving the watermark logic works end-to-end.

---

## Future Enhancements
- Add a Databricks transformation activity (linked service already provisioned) between Copy Data and the sink for cleansing/enrichment before landing in ADLS Gen2.
- Parameterize the pipeline to loop over multiple source tables using the same watermark framework (ForEach + Lookup on a table-list config).
- Add Power BI reporting on top of the Parquet output for business-facing insights.

---

## Screenshots

All screenshots live in the `screenshots/` folder. Save each one using the filename shown below so the images render correctly.

**1. `01-pipeline-canvas.png` — Pipeline Overview**
The `p_ingestion_incremental` pipeline canvas showing all four activities wired together: the two parallel watermark Lookups feeding into `Copy delta_data`, which feeds into `Stored procedure1`.
```
![Pipeline overview](screenshots/ 01-pipeline-canvas.png)
```

**2. `02-source-dataset.png` — Source Dataset Configuration**
The `AzureSqlTable1` dataset pointing at the `AzureSqlDatabase1` linked service, with the table dropdown set to `dbo/WatermarkTable` — confirms the Lookup activities are wired to the correct source.
```
![Source dataset](screenshots/ 02-source-dataset.png)
```

**3. `03-source-data-preview.png` — Source Table Preview**
Query editor preview of `Customer_Transactions`, showing the raw schema (`TransactionID`, `CustomerName`, `PolicyType`, `PremiumAmount`, `LastModifiedDate`) and sample rows before scaling up the dataset.
```
![Source table preview](screenshots/ 03-source-data-preview.png)
```

**4. `04-watermark-table.png` — Watermark Table State**
`WatermarkTable` showing a single control row (`Customer_Transactions`, with its stored `WatermarkValue` timestamp) — this is what `Lkp_get_old_watermark` reads on every run.
```
![Watermark table](screenshots/ 04-watermark-table.png)
```

**5. `05-view-and-procedure.png` — View & Stored Procedure Definitions**
Explorer view of `Customer_Transactions_View` and the `usp_write_watermark` stored procedure with its two input parameters (`@LastModifiedDate`, `@TableName`) — the two objects that make the metadata-driven loop possible.
```
![View and stored procedure](screenshots/ 05-view-and-procedure.png)
```

**6. `06-view-query-result.png` — View Query at Scale**
`SELECT * FROM Customer_Transactions_View` returning 50,000 rows — confirms the view correctly exposes the scaled-up dataset used for load testing.
```
![View query result](screenshots/06-view-query-result.png)
```

**7. `07-manual-test-insert.png` — Incrementality Test Insert**
A manual `INSERT` of a new test row (`TransactionID 1000006`) directly into `Customer_Transactions`, executed to later verify that only this new row gets picked up by the next pipeline run.
```
![Manual test insert](screenshots/07-manual-test-insert.png)
```

**8. `08-debug-run-success.png` — Successful Debug Run**
ADF Output tab showing all four activities (`Lkp_get_old_watermark`, `Lkp_get_new_watermark`, `Copy delta_data`, `Stored procedure1`) completed with a green ✔ Succeeded status.
```
![Debug run success](screenshots/08-debug-run-success.png)
```

**9. `09-copy-details-initial.png` — Copy Activity Details (Initial Run)**
Early-stage run details for `Copy delta_data`: 3 rows read/written, Azure SQL Database → ADLS Gen2 — used to confirm the copy path worked correctly before scaling the dataset.
```
![Copy activity details, initial run](screenshots/09-copy-details-initial.png)
```

**10. `10-new-watermark-query.png` — New Watermark Lookup Query**
Settings tab for `Lkp_get_new_watermark`, showing the query `select max(LastModifiedDate) as NewWatermarkValue from Customer_transactions_view` used to compute the current run's upper boundary.
```
![New watermark query](screenshots/10-new-watermark-query.png)
```

**11. `11-dynamic-expression-builder.png` — Dynamic Delta Query (Expression Builder)**
The Copy Data source query built with ADF's Pipeline Expression Builder, referencing both Lookup activities' outputs (`Lkp_get_old_watermark` / `Lkp_get_new_watermark`) to dynamically filter the delta window.
```
![Dynamic expression builder](screenshots/11-dynamic-expression-builder.png)
```

**12. `12-expression-builder-commented.png` — Expression Builder (Debug State)**
Same expression builder with the `WHERE` filter lines commented out (`//`) — a debugging step used while isolating whether the base view query alone returned data correctly.
```
![Expression builder, debug state](screenshots/12-expression-builder-commented.png)
```

**13. `13-query-settings-editable.png` — Query Settings, Editable View**
Scrolled Settings tab for the new-watermark Lookup, showing the query field in edit mode — included to document the exact query text used.
```
![Query settings editable](screenshots/13-query-settings-editable.png)
```

**14. `14-copy-details-scaled.png` — Copy Activity Details (Full-Scale Run)**
Final run details after scaling the source to 1M+ rows: **75.245 MB / 1,000,006 rows read**, **23.071 MB / 1,000,006 rows written**, 22-second duration, 5.788 MB/s throughput — the key performance evidence for this project.
```
![Copy activity details, scaled run](screenshots/14-copy-details-scaled.png)
```

**15. `15-linked-services.png` — Linked Services (Manage Hub)**
Full list of linked services configured in the factory: three ADLS Gen2 connections, one Azure SQL Database connection, and one Azure Databricks Delta Lake connection — the last of which is provisioned for the planned Databricks enrichment step (see Future Enhancements).
```
![Linked services](screenshots/15-linked-services.png)
```

---

## Author
Sanjay Narayanan —  MDM/ETL background (Informatica PowerCenter, IICS/IDMC), transitioning into cloud data engineering.
