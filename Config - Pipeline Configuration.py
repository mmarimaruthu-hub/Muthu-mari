# Databricks notebook source
# DBTITLE 1,Pipeline Configuration — Centralized Settings
# ============================================================================
# PIPELINE CONFIGURATION — CENTRALIZED SETTINGS
# ============================================================================
# This notebook contains all configuration parameters for the Enterprise
# Patient 360 Data Platform. Import this at the beginning of Bronze, Silver,
# and Gold notebooks to ensure consistent settings across all layers.
# ============================================================================

from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

# ============================================================================
# UNITY CATALOG CONFIGURATION
# ============================================================================

# Catalog and Schema
CATALOG = "workspace"
SCHEMA = "default"

# ============================================================================
# TABLE NAMES
# ============================================================================

# Control Table (for watermark tracking)
CONTROL_TABLE = f"{CATALOG}.{SCHEMA}.control_table_watermark"

# Bronze Tables
BRONZE_HOSPITAL_DISCHARGES = f"{CATALOG}.{SCHEMA}.bronze_hospital_inpatient_discharges"
BRONZE_HEALTH_FACILITY = f"{CATALOG}.{SCHEMA}.bronze_health_facility_info"

# Silver Tables
SILVER_HOSPITAL_DISCHARGES = f"{CATALOG}.{SCHEMA}.silver_hospital_inpatient_discharges"
SILVER_HEALTH_FACILITY = f"{CATALOG}.{SCHEMA}.silver_health_facility_info"

# Gold Tables
GOLD_PATIENT_360 = f"{CATALOG}.{SCHEMA}.gold_patient_360"

# ============================================================================
# SOURCE DATA PATHS
# ============================================================================

# Volume paths for source data
SOURCE_HOSPITAL_DISCHARGES_FILE = "/Volumes/workspace/default/test/Hospital_Inpatient_Discharges_(SPARCS_De-Identified)__2016_20260523.csv"
SOURCE_HEALTH_FACILITY_FILE = "/Volumes/workspace/default/test/Health_Facility_General_Information_20260531.csv"

# ============================================================================
# CHECKPOINT PATHS (for Auto Loader)
# ============================================================================

CHECKPOINT_BASE = "/Volumes/workspace/default/checkpoints"
CHECKPOINT_HOSPITAL_DISCHARGES = f"{CHECKPOINT_BASE}/bronze_hospital_discharges"
CHECKPOINT_HEALTH_FACILITY = f"{CHECKPOINT_BASE}/bronze_health_facility"

# ============================================================================
# DATA QUALITY THRESHOLDS (for hard gate checks)
# ============================================================================

# Silver Layer QA Thresholds
QA_THRESHOLDS = {
    # Maximum allowed null percentage in critical columns
    "max_null_percentage": 5.0,  # 5% threshold
    
    # Maximum allowed duplicate percentage
    "max_duplicate_percentage": 10.0,  # 10% threshold
    
    # Minimum record count for batch (set to 0 to allow empty batches)
    "min_batch_record_count": 1,
    
    # Maximum allowed negative values percentage in financial columns
    "max_negative_values_percentage": 0.5  # 0.5% threshold
}

# Critical columns that must not exceed null threshold
CRITICAL_COLUMNS = [
    "Facility_Id",
    "Facility_Name",
    "Discharge_Year",
    "Age_Group",
    "Gender",
    "Patient_Disposition"
]

# ============================================================================
# BATCH ID GENERATION
# ============================================================================

def generate_batch_id(layer_name):
    """
    Generate a unique batch ID for tracking pipeline runs.
    Format: {layer}_batch_YYYYMMDD_HHMMSS
    
    Args:
        layer_name (str): Name of the layer (bronze, silver, gold)
    
    Returns:
        str: Unique batch identifier
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{layer_name}_batch_{timestamp}"

# ============================================================================
# DISPLAY CONFIGURATION
# ============================================================================

print("=" * 80)
print("PIPELINE CONFIGURATION LOADED")
print("=" * 80)
print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"\nControl Table: {CONTROL_TABLE}")
print(f"\nBronze Tables:")
print(f"  - {BRONZE_HOSPITAL_DISCHARGES}")
print(f"  - {BRONZE_HEALTH_FACILITY}")
print(f"\nSilver Tables:")
print(f"  - {SILVER_HOSPITAL_DISCHARGES}")
print(f"  - {SILVER_HEALTH_FACILITY}")
print(f"\nGold Tables:")
print(f"  - {GOLD_PATIENT_360}")
print(f"\nQA Thresholds:")
print(f"  - Max Null %: {QA_THRESHOLDS['max_null_percentage']}%")
print(f"  - Max Duplicate %: {QA_THRESHOLDS['max_duplicate_percentage']}%")
print(f"  - Min Batch Records: {QA_THRESHOLDS['min_batch_record_count']}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Control Table Setup — Watermark Tracking
# ============================================================================
# CONTROL TABLE SETUP — WATERMARK TRACKING
# ============================================================================
# This cell creates the control table if it doesn't exist. The control table
# tracks watermarks and metadata for each pipeline layer, enabling efficient
# incremental processing.
# ============================================================================

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, MapType
from pyspark.sql.functions import current_timestamp, lit

# Define control table schema
control_table_schema = StructType([
    StructField("table_name", StringType(), False),           # Fully qualified table name
    StructField("layer", StringType(), False),                # bronze, silver, or gold
    StructField("watermark_column", StringType(), False),     # Column used for watermarking
    StructField("watermark_value", TimestampType(), True),    # Last processed timestamp
    StructField("last_batch_id", StringType(), True),         # Last batch identifier
    StructField("last_batch_record_count", LongType(), True), # Records in last batch
    StructField("total_record_count", LongType(), True),      # Total records in table
    StructField("last_updated", TimestampType(), False),      # When this entry was updated
    StructField("status", StringType(), False),               # SUCCESS, FAILED, RUNNING
    StructField("metadata", MapType(StringType(), StringType()), True)  # Additional metadata
])

# Create control table if it doesn't exist
if not spark.catalog.tableExists(CONTROL_TABLE):
    print(f"Creating control table: {CONTROL_TABLE}")
    
    # Create empty DataFrame with schema
    df_control = spark.createDataFrame([], control_table_schema)
    
    # Write as Delta table
    df_control.write.format("delta").mode("overwrite").saveAsTable(CONTROL_TABLE)
    
    print(f"✓ Control table created successfully")
else:
    print(f"✓ Control table already exists: {CONTROL_TABLE}")

# Display current control table contents
print("\nCurrent Control Table Contents:")
print("=" * 80)
spark.table(CONTROL_TABLE).orderBy("last_updated", ascending=False).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Control Table Helper Functions
# ============================================================================
# CONTROL TABLE HELPER FUNCTIONS
# ============================================================================
# These functions provide easy-to-use methods for reading and updating
# watermarks in the control table.
# ============================================================================

from pyspark.sql.functions import col, current_timestamp, lit
from datetime import datetime

def get_watermark(table_name, watermark_column="bronze_load_timestamp"):
    """
    Retrieve the current watermark value for a specific table.
    
    Args:
        table_name (str): Fully qualified table name
        watermark_column (str): Name of the watermark column
    
    Returns:
        datetime or None: The watermark timestamp, or None if not found
    """
    df_control = spark.table(CONTROL_TABLE)
    
    # Filter for the specific table
    watermark_row = df_control.filter(
        (col("table_name") == table_name) & 
        (col("watermark_column") == watermark_column)
    ).select("watermark_value").first()
    
    if watermark_row and watermark_row["watermark_value"]:
        return watermark_row["watermark_value"]
    else:
        return None


def update_watermark(table_name, layer, watermark_column, watermark_value, 
                    batch_id, batch_record_count, total_record_count, 
                    status="SUCCESS", metadata=None):
    """
    Update or insert watermark entry in the control table.
    
    Args:
        table_name (str): Fully qualified table name
        layer (str): bronze, silver, or gold
        watermark_column (str): Name of the watermark column
        watermark_value (datetime): New watermark timestamp
        batch_id (str): Unique batch identifier
        batch_record_count (int): Records processed in this batch
        total_record_count (int): Total records in the table
        status (str): SUCCESS, FAILED, or RUNNING
        metadata (dict): Optional additional metadata
    """
    from pyspark.sql import Row
    
    # Create the new row
    new_row = Row(
        table_name=table_name,
        layer=layer,
        watermark_column=watermark_column,
        watermark_value=watermark_value,
        last_batch_id=batch_id,
        last_batch_record_count=batch_record_count,
        total_record_count=total_record_count,
        last_updated=datetime.now(),
        status=status,
        metadata=metadata or {}
    )
    
    df_new = spark.createDataFrame([new_row])
    
    # Use MERGE to upsert
    df_new.createOrReplaceTempView("control_updates")
    
    merge_sql = f"""
    MERGE INTO {CONTROL_TABLE} AS target
    USING control_updates AS source
    ON target.table_name = source.table_name 
       AND target.watermark_column = source.watermark_column
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    
    spark.sql(merge_sql)
    
    print(f"✓ Watermark updated for {table_name}")
    print(f"  Watermark Value: {watermark_value}")
    print(f"  Batch ID: {batch_id}")
    print(f"  Batch Records: {batch_record_count:,}")
    print(f"  Total Records: {total_record_count:,}")
    print(f"  Status: {status}")


def get_last_batch_info(table_name):
    """
    Get the last batch information for a table.
    
    Args:
        table_name (str): Fully qualified table name
    
    Returns:
        dict: Dictionary with batch info, or None if not found
    """
    df_control = spark.table(CONTROL_TABLE)
    
    batch_info = df_control.filter(col("table_name") == table_name).first()
    
    if batch_info:
        return {
            "batch_id": batch_info["last_batch_id"],
            "record_count": batch_info["last_batch_record_count"],
            "total_records": batch_info["total_record_count"],
            "watermark": batch_info["watermark_value"],
            "status": batch_info["status"],
            "last_updated": batch_info["last_updated"]
        }
    else:
        return None


print("✓ Control table helper functions loaded:")
print("  - get_watermark(table_name, watermark_column)")
print("  - update_watermark(table_name, layer, watermark_column, watermark_value, ...)")
print("  - get_last_batch_info(table_name)")

# COMMAND ----------

# DBTITLE 1,Create Reporting Tables for Power BI
# ============================================================================
# REPORTING TABLES FOR POWER BI DASHBOARDS
# ============================================================================
# These aggregated tables provide optimized views of the Patient 360 data
# for Power BI dashboards. They are much more efficient than importing the
# entire gold_patient_360 table.
# ============================================================================

print("Creating reporting tables for Power BI...")
print("=" * 80)

# Dashboard Patient Summary - aggregate by age group and gender
print("\n1. Creating dashboard_patient_summary...")
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.dashboard_patient_summary AS
SELECT
    patient_age_group,
    patient_gender,
    COUNT(*) AS patient_count,
    AVG(lifetime_total_charges) AS avg_charges
FROM {GOLD_PATIENT_360}
GROUP BY patient_age_group, patient_gender
""")
print("✓ dashboard_patient_summary created")

# Dashboard Diagnosis Summary - aggregate by diagnosis
print("\n2. Creating dashboard_diagnosis_summary...")
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.dashboard_diagnosis_summary AS
SELECT
    most_recent_diagnosis,
    COUNT(*) AS patient_count
FROM {GOLD_PATIENT_360}
GROUP BY most_recent_diagnosis
""")
print("✓ dashboard_diagnosis_summary created")

# Dashboard Facility Summary - aggregate by facility
print("\n3. Creating dashboard_facility_summary...")
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.dashboard_facility_summary AS
SELECT
    most_recent_facility_name,
    COUNT(*) AS patient_count,
    AVG(lifetime_total_charges) AS avg_charges
FROM {GOLD_PATIENT_360}
GROUP BY most_recent_facility_name
""")
print("✓ dashboard_facility_summary created")

print("\n" + "=" * 80)
print("All reporting tables created successfully!")
print("\nTables available for Power BI:")
print(f"  • {CATALOG}.{SCHEMA}.dashboard_patient_summary")
print(f"  • {CATALOG}.{SCHEMA}.dashboard_diagnosis_summary")
print(f"  • {CATALOG}.{SCHEMA}.dashboard_facility_summary")
print("=" * 80)