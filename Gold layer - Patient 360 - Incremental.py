# Databricks notebook source
# DBTITLE 1,Gold Layer - Patient 360 Incremental Load Configuration
# MAGIC %run "/Users/mmarimaruthu@gmail.com/Enterprise patient 360 data platform/Config - Pipeline Configuration"

# COMMAND ----------

# DBTITLE 1,Gold Layer - Patient 360 Incremental Load Configuration
# ============================================================================
# GOLD LAYER — PATIENT 360 INCREMENTAL LOAD WITH FACILITY ENRICHMENT
# ============================================================================
# Strategy: Affected-Patient Recompute + MERGE
#
#   1. Identify patients (surrogate IDs) affected by new Silver records
#   2. Pull ALL Silver history for those patients (needed for window functions)
#   3. Recompute Patient 360 for those patients
#   4. MERGE back into Gold table (upsert)
#
# This approach:
#   - Handles late-arriving data correctly
#   - Recalculates aggregates when new encounters arrive
#   - Uses MERGE to update existing patients or insert new ones
#   - Scales efficiently: only processes affected patients
# ============================================================================

from pyspark.sql.functions import (
    col, count, sum as spark_sum, max as spark_max, min as spark_min,
    avg, countDistinct, when, lit, current_timestamp, coalesce,
    first, last, row_number, dense_rank, concat_ws, collect_set,
    round as spark_round, desc
)
from pyspark.sql.window import Window
from datetime import datetime

# Source and target tables
silver_table = f"{CATALOG}.{SCHEMA}.silver_hospital_discharges_enriched"
gold_table_name = GOLD_PATIENT_360
silver_facility_table = SILVER_HEALTH_FACILITY

# Generate batch ID
batch_id_gold = generate_batch_id("gold")

# Patient ID columns (surrogate key for patient cohorts)
patient_id_cols = ["Facility_Id", "Age_Group", "Gender"]

print("=" * 80)
print("GOLD LAYER — PATIENT 360 INCREMENTAL LOAD")
print("=" * 80)
print(f"  Source Table : {silver_table}")
print(f"  Target Table : {gold_table_name}")
print(f"  Batch ID     : {batch_id_gold}")

# COMMAND ----------

# DBTITLE 1,Cleanup — Drop Gold Table for Fresh Run
# ============================================================================
# CLEANUP: Drop Gold Table
# ============================================================================
# Run this cell to drop the Gold table and start fresh
# This will trigger a FIRST RUN (full compute) in the next execution
# ============================================================================

print("\n=== Dropping Gold Table ===\n")

try:
    spark.sql(f"DROP TABLE IF EXISTS {gold_table_name}")
    print(f"  ✓ Dropped table: {gold_table_name}")
    print(f"  ✓ Next run will be a FIRST RUN (full compute)")
except Exception as e:
    print(f"  ⚠ Error dropping table: {e}")

# COMMAND ----------

# DBTITLE 1,Identify Affected Patients
# ============================================================================
# STEP 1: Determine incremental scope
# ============================================================================
print("\n=== Step 1: Identifying Affected Patients ===\n")

is_first_run = not spark.catalog.tableExists(gold_table_name)

if is_first_run:
    print("  Mode: FIRST RUN (full compute)")
    df_silver_to_process = spark.table(silver_table)
    affected_patient_count = df_silver_to_process.select(*patient_id_cols).distinct().count()
    total_records = df_silver_to_process.count()
    print(f"  Total Silver records: {total_records:,}")
    print(f"  Unique patient cohorts: {affected_patient_count:,}")
    old_watermark = None

else:
    # Get the high watermark from Gold table
    old_watermark = spark.sql(f"""
        SELECT MAX(load_timestamp) AS hwm
        FROM {gold_table_name}
    """).collect()[0]["hwm"]

    print(f"  Mode: INCREMENTAL")
    print(f"  Last Gold watermark: {old_watermark}")

    # Find Silver records that arrived after the watermark
    silver_cols = [f.name for f in spark.table(silver_table).schema.fields]

    if "silver_load_timestamp" in silver_cols:
        new_silver_records = spark.table(silver_table).filter(
            col("silver_load_timestamp") > lit(old_watermark)
        )
    else:
        # Fallback if silver_load_timestamp doesn't exist
        print("  ⚠ silver_load_timestamp not found — full recompute triggered")
        new_silver_records = spark.table(silver_table)

    new_record_count = new_silver_records.count()
    print(f"  New Silver records since last Gold load: {new_record_count:,}")

    if new_record_count == 0:
        print("\n  ℹ No new Silver data. Gold layer is up to date.")
        print("=" * 80)
        print("✓ GOLD INCREMENTAL LOAD COMPLETE — No changes")
        print("=" * 80)
        dbutils.notebook.exit("NO_NEW_DATA")

    # Identify the affected patient cohorts
    affected_patients = new_silver_records.select(*patient_id_cols).distinct()
    affected_patient_count = affected_patients.count()
    print(f"  Affected patient cohorts: {affected_patient_count:,}")

    # Pull ALL Silver history for affected patients
    # (required for correct window function computation)
    df_silver_to_process = spark.table(silver_table).join(
        affected_patients,
        on=patient_id_cols,
        how="inner"
    )
    total_records = df_silver_to_process.count()
    print(f"  Total Silver records for affected patients: {total_records:,}")
    print(f"  (Includes historical records needed for window functions)")

# Cast string code columns to integers for downstream processing
df_silver_to_process = df_silver_to_process \
    .withColumn("CCS_Diagnosis_Code", col("CCS_Diagnosis_Code").cast("int")) \
    .withColumn("CCS_Procedure_Code", col("CCS_Procedure_Code").cast("int")) \
    .withColumn("APR_DRG_Code", col("APR_DRG_Code").cast("int")) \
    .withColumn("APR_Severity_of_Illness_Code", col("APR_Severity_of_Illness_Code").cast("int"))

print(f"\n  ✓ Scope determined: {total_records:,} records for {affected_patient_count:,} patient cohorts")

# COMMAND ----------

# DBTITLE 1,STEP 2: Create Patient Demographics with Window Specifications
# ============================================================================
# STEP 2: Build Patient 360 for the identified subset
# (Same logic as original Gold notebook, applied to subset)
# ============================================================================
print("\n=== Step 2: Building Patient Demographics with Encounter Aggregations ===\n")

# Define window specifications
patient_window = Window.partitionBy(*patient_id_cols).orderBy("Discharge_Year")

patient_lifetime_window = Window.partitionBy(*patient_id_cols)

# Create enriched patient demographics
patient_demographics = df_silver_to_process.withColumn(
    "patient_surrogate_id",
    concat_ws("_",
        col("Age_Group"),
        col("Gender"),
        col("Race"),
        col("Ethnicity"),
        col("`Zip_Code_-_3_digits`")
    )
).withColumn(
    "encounter_sequence_number",
    row_number().over(patient_window)
).withColumn(
    "total_lifetime_encounters",
    count("*").over(patient_lifetime_window)
).withColumn(
    "cumulative_length_of_stay",
    spark_sum("Length_of_Stay").over(
        patient_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
).withColumn(
    "avg_patient_length_of_stay",
    spark_round(avg("Length_of_Stay").over(patient_lifetime_window), 2)
)

print("  ✓ Patient demographics enriched with encounter history")

# COMMAND ----------

# DBTITLE 1,STEP 2.5: Join Health Facility Dimension Data
# ============================================================================
# STEP 2.5: Join Health Facility Dimension Data
# ============================================================================
# Enrich patient encounter data with facility information from the Health
# Facility dimension table. This adds facility characteristics like location,
# services, bed count, etc. to provide complete context for each encounter.
# ============================================================================
print("\n=== Step 2.5: Enriching with Health Facility Information ===\n")

# Load facility dimension data
if spark.catalog.tableExists(silver_facility_table):
    df_facility = spark.table(silver_facility_table)
    
    # Select key facility columns for join (adjust based on actual columns)
    facility_cols_to_join = ["Facility_Id"]
    
    # Add all other facility columns except audit columns
    for col_name in df_facility.columns:
        if col_name not in ["bronze_load_timestamp", "source_file", "bronze_batch_id", 
                            "silver_load_timestamp", "silver_batch_id", "Facility_Id"]:
            facility_cols_to_join.append(col_name)
    
    # Prefix facility columns to avoid naming conflicts
    df_facility_renamed = df_facility.select(
        col("Facility_Id"),
        *[col(c).alias(f"facility_{c}") for c in facility_cols_to_join[1:]]
    )
    
    facility_count = df_facility_renamed.count()
    print(f"  Facility records available: {facility_count:,}")
    
    # Left join: keep all patient encounters even if facility info is missing
    patient_demographics = patient_demographics.join(
        df_facility_renamed,
        on="Facility_Id",
        how="left"
    )
    
    # Check join coverage
    total_encounters = patient_demographics.count()
    encounters_with_facility = patient_demographics.filter(col("facility_Facility_Name").isNotNull()).count() \
        if "facility_Facility_Name" in [c.name for c in patient_demographics.schema.fields] \
        else 0
    
    coverage_pct = (encounters_with_facility / total_encounters * 100) if total_encounters > 0 else 0
    
    print(f"  ✓ Facility data joined successfully")
    print(f"  Join coverage: {encounters_with_facility:,} / {total_encounters:,} encounters ({coverage_pct:.1f}%)")
    
else:
    print(f"  ⚠ Facility table not found: {silver_facility_table}")
    print(f"  Continuing without facility enrichment")
    print("  ✓ Skipping facility join")

# COMMAND ----------

# DBTITLE 1,STEP 3: Enrich with Diagnosis and Clinical Information
# ============================================================================
# STEP 3: Enrich with Diagnosis and Clinical Information
# ============================================================================
print("\n=== Step 3: Adding Diagnosis and Clinical Dimensions ===\n")

# Compute unique metrics per patient cohort
patient_unique_metrics = df_silver_to_process.groupBy(*patient_id_cols).agg(
    countDistinct("CCS_Diagnosis_Code").alias("unique_diagnoses_count"),
    countDistinct("Facility_Id").alias("unique_facilities_visited"),
    countDistinct("Payment_Typology_1").alias("unique_payer_count")
)

print(f"  Unique patient cohorts computed: {patient_unique_metrics.count():,}")

# Join unique metrics
patient_360 = patient_demographics.join(
    patient_unique_metrics,
    on=patient_id_cols,
    how="left"
)

# Add diagnosis-specific columns
patient_360 = patient_360.withColumn(
    "most_recent_diagnosis",
    last("CCS_Diagnosis_Description", ignorenulls=True).over(patient_window)
).withColumn(
    "most_recent_diagnosis_code",
    last("CCS_Diagnosis_Code", ignorenulls=True).over(patient_window)
).withColumn(
    "most_recent_procedure",
    last("CCS_Procedure_Description", ignorenulls=True).over(patient_window)
).withColumn(
    "most_recent_procedure_code",
    last("CCS_Procedure_Code", ignorenulls=True).over(patient_window)
).withColumn(
    "highest_severity_ever",
    first(
        when(col("APR_Severity_of_Illness_Description") == "Extreme", "Extreme")
        .when(col("APR_Severity_of_Illness_Description") == "Major", "Major")
        .when(col("APR_Severity_of_Illness_Description") == "Moderate", "Moderate")
        .otherwise("Minor"),
        ignorenulls=True
    ).over(patient_window.orderBy(desc("APR_Severity_of_Illness_Code")))
).withColumn(
    "highest_mortality_risk_ever",
    first("APR_Risk_of_Mortality", ignorenulls=True).over(
        patient_window.orderBy(
            when(col("APR_Risk_of_Mortality") == "Extreme", 4)
            .when(col("APR_Risk_of_Mortality") == "Major", 3)
            .when(col("APR_Risk_of_Mortality") == "Moderate", 2)
            .when(col("APR_Risk_of_Mortality") == "Minor", 1)
            .otherwise(0).desc()
        )
    )
).withColumn(
    "emergency_encounter_count",
    spark_sum(
        when(col("Type_of_Admission") == "Emergency", 1).otherwise(0)
    ).over(patient_lifetime_window)
).withColumn(
    "ed_visit_count",
    spark_sum(
        when(col("Emergency_Department_Indicator") == "Y", 1).otherwise(0)
    ).over(patient_lifetime_window)
)

print("  ✓ Diagnosis and clinical dimensions added")

# COMMAND ----------

# DBTITLE 1,STEP 4: Enrich with Billing and Financial Information
# ============================================================================
# STEP 4: Billing and Financial Dimensions
# ============================================================================
print("\n=== Step 4: Adding Billing and Financial Dimensions ===\n")

patient_360 = patient_360.withColumn(
    "lifetime_total_charges",
    spark_round(spark_sum("Total_Charges").over(patient_lifetime_window), 2)
).withColumn(
    "lifetime_total_costs",
    spark_round(spark_sum("Total_Costs").over(patient_lifetime_window), 2)
).withColumn(
    "avg_charge_per_encounter",
    spark_round(avg("Total_Charges").over(patient_lifetime_window), 2)
).withColumn(
    "avg_cost_per_encounter",
    spark_round(avg("Total_Costs").over(patient_lifetime_window), 2)
).withColumn(
    "cumulative_charges",
    spark_round(
        spark_sum("Total_Charges").over(
            patient_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ), 2
    )
).withColumn(
    "cumulative_costs",
    spark_round(
        spark_sum("Total_Costs").over(
            patient_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ), 2
    )
).withColumn(
    "most_frequent_payer",
    first("Payment_Typology_1", ignorenulls=True).over(patient_window)
).withColumn(
    "lifetime_gross_margin",
    spark_round(
        spark_sum("Total_Charges").over(patient_lifetime_window) -
        spark_sum("Total_Costs").over(patient_lifetime_window),
        2
    )
)

print("  ✓ Billing and financial dimensions added")

# COMMAND ----------

# DBTITLE 1,STEP 5: Enrich with Facility and Outcome Information
# ============================================================================
# STEP 5: Facility and Outcome Dimensions
# ============================================================================
print("\n=== Step 5: Adding Facility and Outcome Dimensions ===\n")

patient_360 = patient_360.withColumn(
    "most_recent_facility_name",
    last("Facility_Name", ignorenulls=True).over(patient_window)
).withColumn(
    "most_recent_facility_id",
    last("Facility_Id", ignorenulls=True).over(patient_window)
).withColumn(
    "most_recent_disposition",
    last("Patient_Disposition", ignorenulls=True).over(patient_window)
).withColumn(
    "home_discharge_count",
    spark_sum(
        when(col("Patient_Disposition") == "Home or Self Care", 1).otherwise(0)
    ).over(patient_lifetime_window)
).withColumn(
    "snf_transfer_count",
    spark_sum(
        when(col("Patient_Disposition").contains("Skilled Nursing"), 1).otherwise(0)
    ).over(patient_lifetime_window)
).withColumn(
    "hospital_transfer_count",
    spark_sum(
        when(col("Patient_Disposition").contains("Hospital"), 1).otherwise(0)
    ).over(patient_lifetime_window)
).withColumn(
    "ever_expired",
    when(
        spark_sum(
            when(col("Patient_Disposition") == "Expired", 1).otherwise(0)
        ).over(patient_lifetime_window) > 0,
        "Yes"
    ).otherwise("No")
).withColumn(
    "home_discharge_rate",
    spark_round(
        (spark_sum(
            when(col("Patient_Disposition") == "Home or Self Care", 1).otherwise(0)
        ).over(patient_lifetime_window) /
        count("*").over(patient_lifetime_window)) * 100,
        2
    )
).withColumn(
    "most_recent_drg_description",
    last("APR_DRG_Description", ignorenulls=True).over(patient_window)
).withColumn(
    "most_recent_drg_code",
    last("APR_DRG_Code", ignorenulls=True).over(patient_window)
)

print("  ✓ Facility and outcome dimensions added")

# COMMAND ----------

# DBTITLE 1,STEP 6: Handle Null Values
# ============================================================================
# STEP 6: Handle Null Values and Data Quality
# ============================================================================
print("\n=== Step 6: Handling Null Values ===\n")

# Materialize to ensure correct computation
patient_360_count = patient_360.count()
print(f"  Total records in subset: {patient_360_count:,}")

patient_360_cleaned = patient_360 \
    .fillna({
        "Race": "Unknown",
        "Ethnicity": "Unknown",
        "most_recent_diagnosis": "Not Specified",
        "most_recent_diagnosis_code": 0,
        "most_recent_procedure": "NO PROC",
        "most_recent_procedure_code": 0,
        "highest_severity_ever": "Unknown",
        "highest_mortality_risk_ever": "Unknown",
        "most_recent_facility_name": "Unknown Facility",
        "most_recent_facility_id": "Unknown",
        "most_recent_disposition": "Unknown",
        "most_frequent_payer": "Unknown",
        "Payment_Typology_1": "Unknown",
        "most_recent_drg_description": "Not Specified",
        "most_recent_drg_code": 0,
        "unique_diagnoses_count": 0,
        "emergency_encounter_count": 0,
        "ed_visit_count": 0,
        "home_discharge_count": 0,
        "snf_transfer_count": 0,
        "hospital_transfer_count": 0,
        "unique_facilities_visited": 0,
        "unique_payer_count": 0,
        "Total_Charges": 0.0,
        "Total_Costs": 0.0,
        "lifetime_total_charges": 0.0,
        "lifetime_total_costs": 0.0,
        "avg_charge_per_encounter": 0.0,
        "avg_cost_per_encounter": 0.0,
        "cumulative_charges": 0.0,
        "cumulative_costs": 0.0,
        "lifetime_gross_margin": 0.0,
        "Length_of_Stay": 0,
        "cumulative_length_of_stay": 0,
        "avg_patient_length_of_stay": 0.0,
        "home_discharge_rate": 0.0
    })

print("  ✓ Null values handled")

# COMMAND ----------

# DBTITLE 1,STEP 7: Dedup and Add Audit Columns
# ============================================================================
# STEP 7: Deduplicate and Add Audit Columns
# ============================================================================
print("\n=== Step 7: Deduplication and Audit Columns ===\n")

# Dedup on composite key
records_before = patient_360_cleaned.count()
patient_360_deduped = patient_360_cleaned.dropDuplicates([
    "Facility_Id", "Operating_Certificate_Number", "Discharge_Year",
    "Age_Group", "Gender", "Race", "Ethnicity",
    "CCS_Diagnosis_Code", "CCS_Procedure_Code",
    "Total_Charges", "Total_Costs", "Length_of_Stay"
])
records_after = patient_360_deduped.count()
print(f"  Dedup: {records_before:,} → {records_after:,} ({records_before - records_after:,} removed)")

# Add audit columns
patient_360_final = patient_360_deduped \
    .withColumn("created_timestamp",
        current_timestamp() if is_first_run else current_timestamp()
    ) \
    .withColumn("updated_timestamp", current_timestamp()) \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("source_table", lit(silver_table)) \
    .withColumn("etl_batch_id", lit(batch_id_gold)) \
    .withColumn("data_quality_score",
        when(
            (col("Facility_Id").isNotNull()) &
            (col("Total_Charges") > 0) &
            (col("Total_Costs") > 0) &
            (col("Length_of_Stay") >= 0),
            100
        ).when(
            (col("Facility_Id").isNotNull()) &
            ((col("Total_Charges") > 0) | (col("Total_Costs") > 0)),
            80
        ).when(
            col("Facility_Id").isNotNull(),
            60
        ).otherwise(40)
    ) \
    .withColumn("record_status", lit("ACTIVE")) \
    .withColumn("is_current", lit(True))

print("  ✓ Audit columns added")

# COMMAND ----------

# DBTITLE 1,STEP 8: Select Final Columns
# ============================================================================
# STEP 8: Select Final Columns (including facility enrichment)
# ============================================================================
print("\n=== Step 8: Selecting Final Columns ===\n")

# Get available columns from the DataFrame
available_columns = patient_360_final.columns

# Helper function to safely select columns
def safe_col(col_name, alias=None):
    if col_name in available_columns:
        return col(col_name).alias(alias) if alias else col(col_name)
    else:
        return lit(None).alias(alias if alias else col_name)

gold_patient_360_final = patient_360_final.select(
    # PATIENT IDENTIFIER
    col("patient_surrogate_id"),

    # PATIENT DEMOGRAPHICS
    col("Age_Group").alias("patient_age_group"),
    col("Gender").alias("patient_gender"),
    col("Race").alias("patient_race"),
    col("Ethnicity").alias("patient_ethnicity"),
    col("`Zip_Code_-_3_digits`").alias("patient_zip_code_3_digits"),

    # ENCOUNTER DETAILS
    col("encounter_sequence_number"),
    col("total_lifetime_encounters"),
    col("Discharge_Year").cast("int").alias("encounter_discharge_year"),
    col("Type_of_Admission").alias("encounter_admission_type"),
    col("Length_of_Stay").alias("encounter_length_of_stay"),
    col("cumulative_length_of_stay"),
    col("avg_patient_length_of_stay"),
    col("Emergency_Department_Indicator").alias("encounter_ed_indicator"),
    col("emergency_encounter_count"),
    col("ed_visit_count"),

    # CURRENT ENCOUNTER FACILITY INFORMATION (from encounter record)
    col("Facility_Id").alias("current_encounter_facility_id"),
    col("Facility_Name").alias("current_encounter_facility_name"),
    col("Hospital_County").alias("current_encounter_hospital_county"),
    col("Health_Service_Area").alias("current_encounter_service_area"),
    col("Operating_Certificate_Number").cast("int").alias("current_encounter_cert_number"),
    
    # ENRICHED FACILITY INFORMATION (from facility dimension table)
    safe_col("facility_Facility_Name", "facility_name_enriched"),
    safe_col("facility_Facility_Type", "facility_type"),
    safe_col("facility_Hospital_County", "facility_county"),
    safe_col("facility_Health_Service_Area", "facility_service_area"),
    safe_col("facility_Hospital_Ownership", "facility_ownership"),
    safe_col("facility_Total_Beds", "facility_total_beds"),
    safe_col("facility_Description", "facility_description"),
    safe_col("facility_Address", "facility_address"),
    safe_col("facility_City", "facility_city"),
    safe_col("facility_State", "facility_state"),
    safe_col("facility_Zip_Code", "facility_zip_code"),
    safe_col("facility_Phone", "facility_phone"),
    
    # MOST RECENT FACILITY (from patient history)
    col("most_recent_facility_id"),
    col("most_recent_facility_name"),
    col("unique_facilities_visited"),

    # DIAGNOSIS INFORMATION
    col("CCS_Diagnosis_Code").cast("int").alias("current_encounter_diagnosis_code"),
    col("CCS_Diagnosis_Description").alias("current_encounter_diagnosis_desc"),
    col("most_recent_diagnosis_code"),
    col("most_recent_diagnosis"),
    col("unique_diagnoses_count"),

    col("APR_DRG_Code").cast("int").alias("current_encounter_drg_code"),
    col("APR_DRG_Description").alias("current_encounter_drg_desc"),
    col("most_recent_drg_code"),
    col("most_recent_drg_description"),

    col("APR_MDC_Code").cast("int").alias("current_encounter_mdc_code"),
    col("APR_MDC_Description").alias("current_encounter_mdc_desc"),

    # PROCEDURE INFORMATION
    col("CCS_Procedure_Code").cast("int").alias("current_encounter_procedure_code"),
    col("CCS_Procedure_Description").alias("current_encounter_procedure_desc"),
    col("most_recent_procedure_code"),
    col("most_recent_procedure"),

    # CLINICAL SEVERITY & RISK
    col("APR_Severity_of_Illness_Code").cast("int").alias("current_encounter_severity_code"),
    col("APR_Severity_of_Illness_Description").alias("current_encounter_severity_desc"),
    col("highest_severity_ever"),

    col("APR_Risk_of_Mortality").alias("current_encounter_mortality_risk"),
    col("highest_mortality_risk_ever"),

    col("APR_Medical_Surgical_Description").alias("current_encounter_med_surg_type"),

    # BILLING & FINANCIAL
    col("Total_Charges").alias("current_encounter_charges"),
    col("Total_Costs").alias("current_encounter_costs"),
    col("cumulative_charges"),
    col("cumulative_costs"),
    col("lifetime_total_charges"),
    col("lifetime_total_costs"),
    col("lifetime_gross_margin"),
    col("avg_charge_per_encounter"),
    col("avg_cost_per_encounter"),

    # PAYER INFORMATION
    col("Payment_Typology_1").alias("current_encounter_primary_payer"),
    col("Payment_Typology_2").alias("current_encounter_secondary_payer"),
    col("Payment_Typology_3").alias("current_encounter_tertiary_payer"),
    col("most_frequent_payer"),
    col("unique_payer_count"),

    # OUTCOMES & DISPOSITION
    col("Patient_Disposition").alias("current_encounter_disposition"),
    col("most_recent_disposition"),
    col("home_discharge_count"),
    col("snf_transfer_count"),
    col("hospital_transfer_count"),
    col("home_discharge_rate"),
    col("ever_expired"),

    # SPECIALTY INDICATORS
    col("Birth_Weight").cast("int").alias("birth_weight"),
    col("Abortion_Edit_Indicator").alias("abortion_indicator"),

    # AUDIT & METADATA
    col("created_timestamp"),
    col("updated_timestamp"),
    col("load_timestamp"),
    col("source_table"),
    col("etl_batch_id"),
    col("data_quality_score"),
    col("record_status"),
    col("is_current")
)

print(f"  Total columns: {len(gold_patient_360_final.columns)}")
print(f"  ✓ Final column selection complete (includes facility enrichment)")

# COMMAND ----------

# DBTITLE 1,STEP 9: Write to Gold — CREATE or MERGE
# ============================================================================
# STEP 9: Write to Gold Layer
#   First run  → CREATE TABLE with partitioning and Delta optimizations
#   Subsequent → MERGE INTO (delete old rows for affected patients, insert new)
# ============================================================================
print("\n=== Step 9: Writing to Gold Layer ===\n")

if is_first_run:
    # ---- FIRST RUN: Create the table ----
    print("  Mode: CREATE TABLE (first run)")
    print(f"  Writing to: {gold_table_name}")
    print("  Partitioned by: encounter_discharge_year, patient_age_group")

    gold_patient_360_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("delta.autoOptimize.optimizeWrite", "true") \
        .option("delta.autoOptimize.autoCompact", "true") \
        .partitionBy("encounter_discharge_year", "patient_age_group") \
        .saveAsTable(gold_table_name)

    final_count = spark.table(gold_table_name).count()
    print(f"\n  ✓ Table created: {gold_table_name}")
    print(f"  ✓ Records written: {final_count:,}")

else:
    # ---- INCREMENTAL: MERGE INTO ----
    # Strategy: For affected patients, we replace ALL their rows with the
    # freshly recomputed ones. This ensures window function values
    # (cumulative, lifetime, sequence) are correct across all encounters.
    print("  Mode: MERGE INTO (incremental upsert)")
    print(f"  Affected patient cohorts: {affected_patient_count:,}")

    # Get existing Gold count before merge
    existing_gold_count = spark.table(gold_table_name).count()
    print(f"  Existing Gold records: {existing_gold_count:,}")

    # Step A: Delete existing rows for affected patients
    # (They will be replaced with recomputed rows)
    gold_patient_360_final.createOrReplaceTempView("gold_incremental_batch")

    # Get distinct affected surrogate IDs
    affected_ids = gold_patient_360_final.select("patient_surrogate_id").distinct()
    affected_ids.createOrReplaceTempView("affected_patient_ids")

    affected_id_count = affected_ids.count()
    print(f"  Affected surrogate IDs: {affected_id_count:,}")

    # Delete old rows for affected patients
    print("  Deleting stale rows for affected patients...")
    spark.sql(f"""
        DELETE FROM {gold_table_name}
        WHERE patient_surrogate_id IN (
            SELECT patient_surrogate_id FROM affected_patient_ids
        )
    """)

    post_delete_count = spark.table(gold_table_name).count()
    rows_deleted = existing_gold_count - post_delete_count
    print(f"  Rows deleted: {rows_deleted:,}")

    # Step B: Insert the freshly recomputed rows
    print("  Inserting recomputed rows...")
    new_rows = gold_patient_360_final.count()

    gold_patient_360_final.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(gold_table_name)

    final_count = spark.table(gold_table_name).count()
    net_change = final_count - existing_gold_count

    print(f"\n  ✓ MERGE complete")
    print(f"     Previous count  : {existing_gold_count:,}")
    print(f"     Rows deleted    : {rows_deleted:,}")
    print(f"     Rows inserted   : {new_rows:,}")
    print(f"     Final count     : {final_count:,}")
    print(f"     Net change      : {'+' if net_change >= 0 else ''}{net_change:,}")

# COMMAND ----------

# DBTITLE 1,STEP 10: Optimize and Compute Statistics
# ============================================================================
# STEP 10: Optimize Delta Table
# ============================================================================
print("\n=== Step 10: Optimizing Delta Table ===\n")

print("  Running OPTIMIZE...")
spark.sql(f"OPTIMIZE {gold_table_name}")
print("  ✓ Table optimized")

print("  Computing statistics...")
spark.sql(f"ANALYZE TABLE {gold_table_name} COMPUTE STATISTICS")
print("  ✓ Statistics computed")

# COMMAND ----------

# DBTITLE 1,STEP 11: Final Validation and Summary
# ============================================================================
# STEP 11: Final Validation and Summary
# ============================================================================
print("\n" + "=" * 80)
print("GOLD PATIENT 360 — FINAL VALIDATION")
print("=" * 80)

df_gold = spark.table(gold_table_name)

# 1. Total records
final_total = df_gold.count()
print(f"\n  1. Total Gold records: {final_total:,}")

# 2. Column count
print(f"  2. Total columns: {len(df_gold.columns)}")

# 3. Unique patients
unique_patients = df_gold.select("patient_surrogate_id").distinct().count()
print(f"  3. Unique patient cohorts: {unique_patients:,}")

# 4. Facility enrichment coverage
if "facility_name_enriched" in df_gold.columns:
    enriched_count = df_gold.filter(col("facility_name_enriched").isNotNull()).count()
    enrichment_pct = (enriched_count / final_total * 100) if final_total > 0 else 0
    print(f"  4. Facility enrichment coverage: {enriched_count:,} / {final_total:,} ({enrichment_pct:.1f}%)")

# 5. Records by batch
print("\n  5. Records by ETL Batch:")
df_gold.groupBy("etl_batch_id") \
    .count() \
    .orderBy("etl_batch_id", ascending=False) \
    .show(10, truncate=False)

# 6. Watermark check
new_watermark = df_gold.selectExpr("MAX(load_timestamp) as latest").collect()[0]["latest"]
print(f"  6. New high watermark: {new_watermark}")
if old_watermark:
    print(f"     Previous watermark: {old_watermark}")
    assert new_watermark >= old_watermark, "ERROR: Watermark did not advance!"
    print("     ✓ Watermark advanced correctly")

# 7. Data quality summary
print("\n  7. Data Quality Score Distribution:")
df_gold.groupBy("data_quality_score") \
    .count() \
    .orderBy("data_quality_score", ascending=False) \
    .show(10, truncate=False)

# 8. Key metrics
print("  8. Key Metrics Summary:")
summary = df_gold.agg(
    countDistinct("patient_surrogate_id").alias("unique_patients"),
    avg("lifetime_total_charges").alias("avg_lifetime_charges"),
    avg("lifetime_total_costs").alias("avg_lifetime_costs"),
    avg("total_lifetime_encounters").alias("avg_encounters"),
    avg("home_discharge_rate").alias("avg_home_discharge_rate"),
    avg("data_quality_score").alias("avg_quality_score"),
    countDistinct("current_encounter_facility_id").alias("unique_facilities")
).collect()[0]

print(f"     Unique Patients:        {summary['unique_patients']:,}")
print(f"     Unique Facilities:      {summary['unique_facilities']:,}")
print(f"     Avg Lifetime Charges:   ${summary['avg_lifetime_charges']:,.2f}")
print(f"     Avg Lifetime Costs:     ${summary['avg_lifetime_costs']:,.2f}")
print(f"     Avg Encounters/Patient: {summary['avg_encounters']:.2f}")
print(f"     Avg Home Discharge %:   {summary['avg_home_discharge_rate']:.2f}%")
print(f"     Avg Data Quality Score: {summary['avg_quality_score']:.1f}/100")

# 9. Facility enrichment sample
if "facility_name_enriched" in df_gold.columns:
    print("\n  9. Sample Records with Facility Enrichment:")
    df_gold.select(
        "patient_surrogate_id",
        "encounter_sequence_number",
        "current_encounter_facility_name",
        "facility_name_enriched",
        "lifetime_total_charges",
        "etl_batch_id"
    ).show(5, truncate=True)
else:
    print("\n  9. Sample Records:")
    df_gold.select(
        "patient_surrogate_id",
        "encounter_sequence_number",
        "total_lifetime_encounters",
        "most_recent_diagnosis",
        "lifetime_total_charges",
        "home_discharge_rate",
        "data_quality_score",
        "etl_batch_id"
    ).show(5, truncate=True)

# 10. Delta history
print("  10. Recent Delta History:")
spark.sql(f"DESCRIBE HISTORY {gold_table_name}").select(
    "version", "timestamp", "operation", "operationMetrics"
).show(5, truncate=False)

# 11. Update control table
print("\n  11. Updating Control Table:")
update_watermark(
    table_name=gold_table_name,
    layer="gold",
    watermark_column="load_timestamp",
    watermark_value=new_watermark,
    batch_id=batch_id_gold,
    batch_record_count=final_total if is_first_run else gold_patient_360_final.count(),
    total_record_count=final_total,
    status="SUCCESS",
    metadata={
        "unique_patients": str(unique_patients),
        "facility_enrichment": "enabled",
        "source_table": silver_table
    }
)

print("\n" + "=" * 80)
print(f"✓ GOLD PATIENT 360 WITH FACILITY ENRICHMENT COMPLETE — Batch: {batch_id_gold}")
print(f"  Source: {silver_table} (already enriched with facility data)")
print("=" * 80)