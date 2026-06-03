# ETL Strategy Document

## 1. Architecture Overview
The Enterprise Patient 360 Data Platform is built on **Databricks** utilizing **PySpark** and **Delta Lake**. It follows the **Medallion Architecture** to progressively structure, cleanse, and enrich the data.

## 2. Medallion Layers Strategy

### 2.1 Bronze Layer (Raw Ingestion)
- **Objective**: Ingest raw data from external systems as quickly and reliably as possible while preserving the original state.
- **Strategy**: 
  - **Primary**: **Databricks Auto Loader (`cloudFiles`)** is used for structured streaming. It tracks processed files via a checkpoint directory, enabling efficient incremental loads (processing only new files). Schema evolution is handled using `rescue` mode to capture schema drift without failing the pipeline.
  - **Fallback**: A batch-mode approach utilizing `_metadata.file_path` to cross-reference existing `source_file` records in the Bronze table to filter out already-processed files.
- **Audit Columns**: `bronze_load_timestamp`, `source_file`, `bronze_batch_id`.
- **Format**: Delta format, append-only.

### 2.2 Silver Layer (Cleansing & Standardization)
- **Objective**: Provide a cleansed, filtered, and standardized view of the data.
- **Strategy**: **Watermark-based Incremental Processing**.
  1. The pipeline reads the high-watermark (`MAX(silver_load_timestamp)`) from the existing Silver table.
  2. It pulls only new Bronze records where `bronze_load_timestamp > watermark`.
  3. Transformations are applied: Data type casting (Strings to Doubles/Integers), categorical standardization (e.g., Gender), and regex-based string cleansing.
  4. Deduplication is performed within the batch.
- **Write Mode**: `MERGE INTO` (Upsert). The pipeline merges the new batch into the Silver table using a composite business key to update existing records and insert new ones, preventing duplicates.
- **Audit Columns**: `silver_load_timestamp`, `source_system`, `silver_batch_id`.

### 2.3 Gold Layer (Patient 360 Aggregation)
- **Objective**: Provide business-level aggregations and a unified "Patient Golden Record" optimized for BI and Reporting.
- **Strategy**: **Affected-Patient Recompute + MERGE**.
  - **Challenge**: The Gold layer relies heavily on PySpark Window functions to calculate longitudinal metrics (e.g., `cumulative_length_of_stay`, `lifetime_total_charges`, `encounter_sequence_number`). A simple append of new encounters would break these cumulative calculations.
  - **Solution**: 
    1. Identify the specific patient cohorts (surrogate IDs) affected by the new incremental Silver records.
    2. Pull the *entire* historical Silver data for *only* those affected patients.
    3. Recompute the Patient 360 dimensions and window functions for this targeted subset.
    4. Execute a `MERGE INTO` statement that deletes the stale rows for the affected patients and inserts the freshly recomputed rows.
- **Optimization**: The Gold table is optimized post-load using `OPTIMIZE` (with Z-Ordering/Auto-Compact if configured) and `ANALYZE TABLE COMPUTE STATISTICS`. It is physically partitioned by `encounter_discharge_year` and `patient_age_group` for rapid query performance by BI tools.

## 3. Pipeline Orchestration
- Workflows are designed to run sequentially: `Bronze -> Silver -> Gold`.
- Batch IDs (`etl_batch_id`) are generated at runtime to track data lineage across all three layers.
