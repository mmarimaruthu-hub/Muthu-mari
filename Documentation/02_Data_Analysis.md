# Data Analysis Document

## 1. Overview
This document details the analysis of the raw healthcare data ingested into the Enterprise Patient 360 Data Platform. The dataset consists of high-volume hospital inpatient discharges and related patient encounters.

## 2. Data Domains & Entities
Based on the raw data structures, the following key entities and attributes were identified:

### 2.1 Demographics
- **Attributes**: Age Group, Gender, Race, Ethnicity, Zip Code (3-digits).
- **Analysis**: These attributes form the core demographic profile. In the absence of a universal Patient ID, a composite surrogate key is generated using these fields to track patients across multiple encounters.

### 2.2 Encounter Details
- **Attributes**: Facility ID, Facility Name, Operating Certificate Number, Discharge Year, Length of Stay, Type of Admission, Patient Disposition.
- **Analysis**: Captures the operational "where, when, and how" of the patient's visit.

### 2.3 Clinical & Diagnosis Information
- **Attributes**: CCS Diagnosis Code/Description, CCS Procedure Code/Description, APR DRG Code/Description, APR MDC Code, APR Severity of Illness, APR Risk of Mortality.
- **Analysis**: Rich clinical dimensions indicating why the patient was admitted, what procedures were performed, and the severity/risk associated with the case.

### 2.4 Financial & Billing
- **Attributes**: Total Charges, Total Costs, Payment Typology 1/2/3 (Primary, Secondary, Tertiary Payers).
- **Analysis**: Crucial for gross margin analysis and identifying payer distributions.

## 3. Data Quality Issues Identified & Resolved

During the profiling phase, several anomalies and data quality issues were identified in the raw CSV files. These are handled programmatically in the PySpark ETL pipelines:

| Issue Identified | Resolution in Pipeline |
| :--- | :--- |
| **Special Characters in Columns** | Used Regex to replace spaces, commas, brackets, and equal signs with underscores to comply with Delta Lake naming constraints. |
| **Financial Formatting** | `Total_Charges` and `Total_Costs` came as strings with `$` and `,`. Regex was used to strip characters and cast to `DoubleType`. |
| **String Categorical limits** | `Length_of_Stay` contained values like `"120 +"`. This was explicitly mapped to `120` and cast to `IntegerType`. |
| **Inconsistent Categorical Values** | `Gender` contained mixed values (`M`, `Male`, `F`, `Female`, `U`, `Unknown`). Standardized to `M`, `F`, and `U`. |
| **Missing Values (Nulls)** | Missing values in critical analytical dimensions were filled with default business values (e.g., "Unknown Facility", "Not Specified", `0` for numeric aggregations). |
| **Duplicate Records** | Deduplication applied in both Silver (batch deduplication) and Gold layers using a composite set of business keys. |

## 4. Statistical Profile Highlights
*Metrics are generated dynamically during the Gold layer materialization:*
- **Quality Scoring**: A custom `data_quality_score` (0-100) is calculated per record based on the presence of critical identifiers (Facility ID) and valid financial/stay metrics.
- **Risk Distribution**: Patients are categorized by Mortality Risk and Severity of Illness (Minor, Moderate, Major, Extreme).
- **Outcome Metrics**: The pipeline calculates derived metrics such as `home_discharge_rate`, tracking the percentage of encounters resulting in a safe discharge home versus transfers to skilled nursing facilities.
