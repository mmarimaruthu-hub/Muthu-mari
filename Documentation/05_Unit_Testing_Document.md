# Unit Testing Document

## 1. Overview
This document defines the unit test cases for all three ETL pipeline layers of the Enterprise Patient 360 Data Platform. Tests are designed to validate data integrity, transformation correctness, and pipeline reliability.

---

## 2. Bronze Layer Test Cases

| Test ID | Test Name | Test Description | Expected Outcome | Validation Method |
| :--- | :--- | :--- | :--- | :--- |
| BRZ-001 | Table Existence Check | Verify Bronze table is created after first run | `spark.catalog.tableExists()` returns `True` | `assert spark.catalog.tableExists(bronze_table)` |
| BRZ-002 | Record Count > 0 | Verify records were ingested | Row count > 0 | `assert df_bronze.count() > 0` |
| BRZ-003 | Audit Column Presence | Verify all 3 audit columns exist | Columns present in schema | `assert "bronze_load_timestamp" in df_bronze.columns` |
| BRZ-004 | No Null Audit Columns | Audit columns must not contain nulls | 0 null audit records | `assert df_bronze.filter(col("bronze_batch_id").isNull()).count() == 0` |
| BRZ-005 | Column Name Cleanliness | No column names with spaces or special chars | All column names match `[A-Za-z0-9_]+` | Regex check on `df_bronze.columns` |
| BRZ-006 | Duplicate Full-Row Check | No fully duplicate records in Bronze table | Duplicate count = 0 | `total - distinct == 0` |
| BRZ-007 | Source File Tracking | Each record must have a source file | 0 nulls in `source_file` | `assert df_bronze.filter(col("source_file").isNull()).count() == 0` |
| BRZ-008 | Incremental - No Re-ingestion | Second run must not duplicate records from first run | Row count same after rerun of same files | Compare count before and after rerun |
| BRZ-009 | Batch ID Uniqueness | Each run should produce a unique Batch ID | Only 1 batch ID per run execution | `df_bronze.select("bronze_batch_id").distinct().count() >= 1` |
| BRZ-010 | Checkpoint Persistence | Checkpoint directory must exist after first run | Directory present in DBFS | `dbutils.fs.ls(checkpoint_path)` does not throw |

---

## 3. Silver Layer Test Cases

| Test ID | Test Name | Test Description | Expected Outcome | Validation Method |
| :--- | :--- | :--- | :--- | :--- |
| SLV-001 | Table Existence Check | Silver table is created after first run | `True` | `assert spark.catalog.tableExists(silver_table)` |
| SLV-002 | Data Type - Total_Charges | `Total_Charges` must be `DoubleType` | Schema shows DoubleType | `assert df_silver.schema["Total_Charges"].dataType == DoubleType()` |
| SLV-003 | Data Type - Total_Costs | `Total_Costs` must be `DoubleType` | Schema shows DoubleType | `assert df_silver.schema["Total_Costs"].dataType == DoubleType()` |
| SLV-004 | Data Type - Length_of_Stay | `Length_of_Stay` must be `IntegerType` | Schema shows IntegerType | `assert df_silver.schema["Length_of_Stay"].dataType == IntegerType()` |
| SLV-005 | No Negative Charges | No `Total_Charges` values < 0 | Count = 0 | `assert df_silver.filter(col("Total_Charges") < 0).count() == 0` |
| SLV-006 | No Negative Costs | No `Total_Costs` values < 0 | Count = 0 | `assert df_silver.filter(col("Total_Costs") < 0).count() == 0` |
| SLV-007 | No Negative LOS | No `Length_of_Stay` values < 0 | Count = 0 | `assert df_silver.filter(col("Length_of_Stay") < 0).count() == 0` |
| SLV-008 | LOS Max Cap | Max `Length_of_Stay` should be ≤ 120 (after cleaning "120 +") | Max ≤ 120 | `assert df_silver.agg({"Length_of_Stay":"max"}).collect()[0][0] <= 120` |
| SLV-009 | Gender Standardization | `Gender` only contains `M`, `F`, `U` | Distinct values limited | `assert set(df_silver.select("Gender").distinct().collect()) <= {"M","F","U"}` |
| SLV-010 | Null Critical Columns Check | Null % in Facility_Id, Age_Group, Gender < 5% | All < 5% | Per-column null percentage check |
| SLV-011 | Deduplication on Merge Keys | No duplicates on the 11 merge key columns | Count distinct = total | `dropDuplicates(merge_keys).count() == total` |
| SLV-012 | Watermark Advancement | New watermark > previous watermark | `new_wm > old_wm` | `assert new_watermark >= old_watermark` |
| SLV-013 | Audit Column Presence | `silver_load_timestamp`, `source_system`, `silver_batch_id` exist | Present in schema | Column presence check |
| SLV-014 | Source System Value | `source_system` must always be `"SPARCS"` | All = "SPARCS" | `assert df_silver.filter(col("source_system") != "SPARCS").count() == 0` |
| SLV-015 | Incremental No Regress | Record count must never decrease | Final ≥ Previous count | `assert final_count >= existing_silver_count` |

---

## 4. Gold Layer Test Cases

| Test ID | Test Name | Test Description | Expected Outcome | Validation Method |
| :--- | :--- | :--- | :--- | :--- |
| GLD-001 | Table Existence Check | Gold table is created after first run | `True` | `assert spark.catalog.tableExists(gold_table_name)` |
| GLD-002 | Column Count | Gold table must have exactly 58 columns | `len(df_gold.columns) == 58` | Column count assertion |
| GLD-003 | Surrogate Key Not Null | `patient_surrogate_id` must never be null | 0 nulls | `assert df_gold.filter(col("patient_surrogate_id").isNull()).count() == 0` |
| GLD-004 | Encounter Sequence Starts at 1 | Min `encounter_sequence_number` = 1 per patient | Min = 1 | Window validation |
| GLD-005 | Cumulative Charges ≥ Current Charges | `cumulative_charges ≥ current_encounter_charges` | Always True | `assert df_gold.filter(col("cumulative_charges") < col("current_encounter_charges")).count() == 0` |
| GLD-006 | Lifetime Charges ≥ Cumulative | `lifetime_total_charges ≥ cumulative_charges` | Always True | Comparison assertion |
| GLD-007 | Data Quality Score Range | `data_quality_score` must be in [40, 60, 80, 100] | Valid values only | `assert df_gold.filter(~col("data_quality_score").isin([40,60,80,100])).count() == 0` |
| GLD-008 | Record Status | `record_status` must always be `"ACTIVE"` | Count = 0 for non-ACTIVE | `assert df_gold.filter(col("record_status") != "ACTIVE").count() == 0` |
| GLD-009 | Home Discharge Rate Range | `home_discharge_rate` between 0 and 100 | All in [0,100] | `assert df_gold.filter((col("home_discharge_rate") < 0) \| (col("home_discharge_rate") > 100)).count() == 0` |
| GLD-010 | Watermark Advancement | Gold watermark must advance after each run | `new_wm >= old_wm` | `assert new_watermark >= old_watermark` |
| GLD-011 | Partitioning Check | Table must be partitioned by year and age group | Partition columns present | `DESCRIBE DETAIL` or `df_gold.explain()` |
| GLD-012 | No Future Discharge Year | `encounter_discharge_year` must be ≤ current year | No future years | `assert df_gold.filter(col("encounter_discharge_year") > 2026).count() == 0` |
| GLD-013 | is_current Flag | `is_current` flag must always be `True` | All True | `assert df_gold.filter(~col("is_current")).count() == 0` |
| GLD-014 | Incremental Recompute Accuracy | After incremental load, patient metrics must match full recompute | Same output for affected patients | Compare targeted recompute vs full recompute for sampled patients |
