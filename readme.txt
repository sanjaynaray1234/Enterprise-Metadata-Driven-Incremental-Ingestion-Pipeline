# Enterprise Metadata-Driven Incremental Ingestion Pipeline (ADF & Azure SQL)

## Project Overview
This repository contains a production-grade, end-to-end cloud data ingestion pipeline built using **Azure Data Factory (ADF)**, **Azure SQL Database**, and **Azure Data Lake Storage Gen2 (ADLS Gen2)**. 

Instead of creating hardcoded, high-maintenance pipelines for individual tables, this project implements a scalable, **metadata-driven framework** that utilizes a dynamic control loop and database view layers to identify and capture high-volume delta changes (Change Data Capture/CDC logic) across an enterprise dataset of **1,000,000+ records**.



## Key Technical Achievements & Architecture
* **High Volume Scalability:** Built and validated data logic using a synthetic insurance dataset exceeding **1,000,000 records**.
* **Metadata-Driven Framework:** Orchestrated pipeline pipelines using abstract parameterization, utilizing a centralized SQL control table (`WatermarkTable`) to keep track of extraction boundaries.
* **Database View Abstraction Layer:** Implemented a presentation layer view (`v_Customer_Transactions`) to decouple the storage processing engine from the cloud orchestration layer, ensuring security and query optimization.
* **Optimized Columnar Storage:** Engineered the sink layer to output high-volume transactions directly into highly-compressed, enterprise-standard `.parquet` format.

---

## Data Architecture & Pipeline Flow

1. **Get Old Watermark:** A Lookup activity queries the `WatermarkTable` to fetch the last successfully processed timestamp boundary (`WatermarkValue`) for the data stream.
2. **Get New Watermark:** A parallel Lookup activity calculates the current maximum runtime timestamp (`MAX(LastModifiedDate)`) directly from the database presentation view.
3. **Dynamic Delta Copy:** The Copy Data activity executes a dynamic SQL query that filters data boundaries on the fly:
   ```sql
   SELECT * FROM v_Customer_Transactions 
   WHERE LastModifiedDate > '@{activity('Lkp_get_old_watermark').output.firstRow.WatermarkValue}' 
   AND LastModifiedDate <= '@{activity('Lkp_get_new_watermark').output.firstRow.NewWatermarkValue}'