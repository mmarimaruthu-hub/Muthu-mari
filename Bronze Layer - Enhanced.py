# Databricks notebook source
# DBTITLE 1,Load Configuration
# MAGIC %run "/Users/mmarimaruthu@gmail.com/Enterprise patient 360 data platform/Config - Pipeline Configuration"

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
# ============================================================================
# IMPORT REQUIRED LIBRARIES
# ============================================================================

from pyspark.sql.functions import (
    col, current_timestamp, lit, 
    concat, unix_timestamp, regexp_extract
)
import re
from datetime import datetime

print("✓ Libraries imported successfully")

# COMMAND ----------

# DBTITLE 1,Column Name Cleaner Function
# ============================================================================
# COLUMN NAME CLEANER FUNCTION
# ============================================================================
# Clean column names to remove invalid characters for Delta Lake tables.
# Replaces spaces, special characters with underscores, removes duplicates.
# ============================================================================

def clean_column_names(df):
    """
    Clean column names: replace invalid Delta Lake characters with underscores.
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame with cleaned column names
    """
    for column in df.columns:
        # Replace spaces, commas, semicolons, braces, parentheses, newlines, tabs, equals
        new_column = re.sub(r'[ ,;{}()\n\t=]', '_', column)
        # Remove consecutive underscores
        new_column = re.sub(r'_+', '_', new_column)
        # Remove leading/trailing underscores
        new_column = new_column.strip('_')
        df = df.withColumnRenamed(column, new_column)
    return df

print("✓ Column name cleaner function loaded")

# COMMAND ----------

# DBTITLE 1,Dataset 1: Hospital Discharges — Auto Loader with mergeSchema
# ============================================================================
# DATASET 1: HOSPITAL DISCHARGES — BATCH PROCESSING
# ============================================================================
# Ingests hospital inpatient discharge data using simple batch CSV read.
# Key features:
#   - Reads specific patient discharge file (not all CSVs in directory)
#   - Simple batch processing (no streaming, no checkpoints)
#   - Adds audit columns: bronze_load_timestamp, source_file, bronze_batch_id
# ============================================================================

print("=" * 80)
print("DATASET 1: HOSPITAL INPATIENT DISCHARGES")
print("=" * 80)

# Generate batch ID for this run
batch_id_discharges = generate_batch_id("bronze")

print(f"Source File: {SOURCE_HOSPITAL_DISCHARGES_FILE}")
print(f"Target Table: {BRONZE_HOSPITAL_DISCHARGES}")
print(f"Batch ID: {batch_id_discharges}")
print(f"Processing Mode: BATCH (Append)")

try:
    # --- READ PATIENT DISCHARGE CSV FILE ---
    # Reads the specific patient discharge file (not all CSVs)
    df_discharges = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")  # Keep as strings initially
        .option("multiLine", "true")
        .option("escape", '"')
        .load(SOURCE_HOSPITAL_DISCHARGES_FILE)
    )
    
    # Clean column names to ensure Delta compatibility
    # Converts "Facility Id" -> "Facility_Id", "Age Group" -> "Age_Group", etc.
    df_discharges = clean_column_names(df_discharges)
    
    # Add audit columns for tracking
    df_discharges = df_discharges \
        .withColumn("bronze_load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("Hospital_Inpatient_Discharges_(SPARCS_De-Identified)__2016_20260523.csv")) \
        .withColumn("bronze_batch_id", lit(batch_id_discharges))
    
    # Get record count
    record_count = df_discharges.count()
    print(f"\n  Records to load: {record_count:,}")
    
    # --- WRITE TO BRONZE TABLE ---
    # Using append mode for incremental loads
    df_discharges.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(BRONZE_HOSPITAL_DISCHARGES)
    
    print(f"\n✓ Batch ingestion complete for Hospital Discharges")
    print(f"  Records loaded: {record_count:,}")
    autoloader_success_discharges = True
    
except Exception as e:
    print(f"\n✘ Batch ingestion failed: {e}")
    import traceback
    traceback.print_exc()
    autoloader_success_discharges = False
    raise

# COMMAND ----------

# DBTITLE 1,Dataset 1: Update Control Table
# ============================================================================
# DATASET 1: UPDATE CONTROL TABLE WITH WATERMARK
# ============================================================================
# Record the ingestion metadata in the control table for tracking.
# ============================================================================

if autoloader_success_discharges:
    # Get table statistics
    df_discharges = spark.table(BRONZE_HOSPITAL_DISCHARGES)
    
    # Get record count for this batch
    batch_record_count = df_discharges.filter(
        col("bronze_batch_id") == batch_id_discharges
    ).count()
    
    # Get total record count
    total_record_count = df_discharges.count()
    
    # Get max watermark (latest bronze_load_timestamp)
    max_watermark = df_discharges.agg(
        {"bronze_load_timestamp": "max"}
    ).collect()[0][0]
    
    # Update control table
    update_watermark(
        table_name=BRONZE_HOSPITAL_DISCHARGES,
        layer="bronze",
        watermark_column="bronze_load_timestamp",
        watermark_value=max_watermark,
        batch_id=batch_id_discharges,
        batch_record_count=batch_record_count,
        total_record_count=total_record_count,
        status="SUCCESS",
        metadata={"dataset": "hospital_discharges", "source": SOURCE_HOSPITAL_DISCHARGES_FILE}
    )
    
    print(f"\n✓ Control table updated for {BRONZE_HOSPITAL_DISCHARGES}")

# COMMAND ----------

# DBTITLE 1,Dataset 2: Health Facility Info — Batch Load with mergeSchema
# ============================================================================
# DATASET 2: HEALTH FACILITY INFO — BATCH LOAD WITH MERGESCHEMA
# ============================================================================
# Ingests health facility information from a single CSV file.
# Key features:
#   - Batch read with mergeSchema for schema evolution
#   - Full refresh pattern (overwrite mode)
#   - Adds audit columns: bronze_load_timestamp, source_file, bronze_batch_id
# ============================================================================

print("\n" + "=" * 80)
print("DATASET 2: HEALTH FACILITY GENERAL INFORMATION")
print("=" * 80)

# Generate batch ID for this dataset
batch_id_facility = generate_batch_id("bronze")

print(f"Source File: {SOURCE_HEALTH_FACILITY_FILE}")
print(f"Target Table: {BRONZE_HEALTH_FACILITY}")
print(f"Batch ID: {batch_id_facility}")
print(f"mergeSchema: ENABLED")

try:
    # --- READ CSV FILE WITH MERGESCHEMA ---
    # Single file batch read with schema evolution support
    df_facility = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")  # Keep as strings initially
        .option("multiLine", "true")
        .option("escape", '"')
        .option("mergeSchema", "true")  # ENABLE SCHEMA EVOLUTION
        .load(SOURCE_HEALTH_FACILITY_FILE)
    )
    
    # Clean column names
    df_facility = clean_column_names(df_facility)
    
    # Add audit columns
    df_facility = df_facility \
        .withColumn("bronze_load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("Health_Facility_General_Information_20260531.csv")) \
        .withColumn("bronze_batch_id", lit(batch_id_facility))
    
    # Get record count before write
    record_count_facility = df_facility.count()
    
    # --- WRITE TO BRONZE TABLE ---
    # Using overwrite mode for full refresh pattern
    # mergeSchema allows schema changes between loads
    df_facility.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .saveAsTable(BRONZE_HEALTH_FACILITY)
    
    print(f"\n✓ Batch ingestion complete for Health Facility Info")
    print(f"  Records loaded: {record_count_facility:,}")
    facility_success = True
    
except Exception as e:
    print(f"\n✘ Health Facility ingestion failed: {e}")
    facility_success = False
    raise

# COMMAND ----------

# DBTITLE 1,Dataset 2: Update Control Table
# ============================================================================
# DATASET 2: UPDATE CONTROL TABLE WITH WATERMARK
# ============================================================================
# Record the ingestion metadata in the control table for tracking.
# ============================================================================

if facility_success:
    # Get table statistics
    df_facility_check = spark.table(BRONZE_HEALTH_FACILITY)
    
    # Get total record count
    total_record_count_facility = df_facility_check.count()
    
    # Get max watermark
    max_watermark_facility = df_facility_check.agg(
        {"bronze_load_timestamp": "max"}
    ).collect()[0][0]
    
    # Update control table
    update_watermark(
        table_name=BRONZE_HEALTH_FACILITY,
        layer="bronze",
        watermark_column="bronze_load_timestamp",
        watermark_value=max_watermark_facility,
        batch_id=batch_id_facility,
        batch_record_count=total_record_count_facility,  # Full refresh, all records are new
        total_record_count=total_record_count_facility,
        status="SUCCESS",
        metadata={"dataset": "health_facility", "source": SOURCE_HEALTH_FACILITY_FILE}
    )
    
    print(f"\n✓ Control table updated for {BRONZE_HEALTH_FACILITY}")

# COMMAND ----------

# DBTITLE 1,Validation — Bronze Layer Summary
# ============================================================================
# VALIDATION — BRONZE LAYER SUMMARY
# ============================================================================
# Display summary statistics for both bronze tables.
# ============================================================================

print("\n" + "=" * 80)
print("BRONZE LAYER VALIDATION")
print("=" * 80)

# Dataset 1: Hospital Discharges
if autoloader_success_discharges:
    df_discharges_final = spark.table(BRONZE_HOSPITAL_DISCHARGES)
    print(f"\n1. {BRONZE_HOSPITAL_DISCHARGES}")
    print(f"   Total Records: {df_discharges_final.count():,}")
    print(f"   Columns: {len(df_discharges_final.columns)}")
    print(f"   Latest Batch: {batch_id_discharges}")
    
    # Show records by batch
    print("\n   Records by Batch ID:")
    df_discharges_final.groupBy("bronze_batch_id") \
        .count() \
        .orderBy("bronze_batch_id", ascending=False) \
        .show(5, truncate=False)

# Dataset 2: Health Facility
if facility_success:
    df_facility_final = spark.table(BRONZE_HEALTH_FACILITY)
    print(f"\n2. {BRONZE_HEALTH_FACILITY}")
    print(f"   Total Records: {df_facility_final.count():,}")
    print(f"   Columns: {len(df_facility_final.columns)}")
    print(f"   Latest Batch: {batch_id_facility}")
    
    # Show sample records
    print("\n   Sample Records:")
    df_facility_final.show(5, truncate=False)

# Display control table
print("\n" + "=" * 80)
print("CONTROL TABLE STATUS")
print("=" * 80)
spark.table(CONTROL_TABLE).orderBy("last_updated", ascending=False).show(truncate=False)

print("\n✓ Bronze layer validation complete")