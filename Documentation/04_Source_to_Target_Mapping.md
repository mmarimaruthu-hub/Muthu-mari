# Source to Target Mapping Document

## 1. Overview
This document maps raw source fields from the ingested CSV files (Bronze Layer) through the Silver (Cleansed) layer and finally to the Gold (Patient 360) output table.

---

## 2. Bronze → Silver Mapping

| Source Column (Bronze / Raw CSV) | Target Column (Silver) | Transformation Applied |
| :--- | :--- | :--- |
| `Facility Id` | `Facility_Id` | Column rename (spaces → underscores) |
| `Facility Name` | `Facility_Name` | Column rename |
| `Health Service Area` | `Health_Service_Area` | Column rename |
| `Hospital County` | `Hospital_County` | Column rename |
| `Operating Certificate Number` | `Operating_Certificate_Number` | Column rename |
| `Permanent Facility Id` | `Permanent_Facility_Id` | Column rename |
| `Age Group` | `Age_Group` | Column rename |
| `Zip Code - 3 digits` | `Zip_Code_3_digits` | Column rename + special character removal (`-` → `_`) |
| `Gender` | `Gender` | Standardize: `M/Male → M`, `F/Female → F`, `U/Unknown → U` |
| `Race` | `Race` | Column rename |
| `Ethnicity` | `Ethnicity` | Column rename |
| `Length of Stay` | `Length_of_Stay` | `"120 +"` → `120`, cast to `IntegerType` |
| `Type of Admission` | `Type_of_Admission` | Column rename |
| `Patient Disposition` | `Patient_Disposition` | Column rename |
| `Discharge Year` | `Discharge_Year` | Column rename |
| `CCS Diagnosis Code` | `CCS_Diagnosis_Code` | Column rename |
| `CCS Diagnosis Description` | `CCS_Diagnosis_Description` | Column rename |
| `CCS Procedure Code` | `CCS_Procedure_Code` | Column rename |
| `CCS Procedure Description` | `CCS_Procedure_Description` | Column rename |
| `APR DRG Code` | `APR_DRG_Code` | Column rename |
| `APR DRG Description` | `APR_DRG_Description` | Column rename |
| `APR MDC Code` | `APR_MDC_Code` | Column rename |
| `APR MDC Description` | `APR_MDC_Description` | Column rename |
| `APR Severity of Illness Code` | `APR_Severity_of_Illness_Code` | Column rename |
| `APR Severity of Illness Description` | `APR_Severity_of_Illness_Description` | Column rename |
| `APR Risk of Mortality` | `APR_Risk_of_Mortality` | Column rename |
| `APR Medical Surgical Description` | `APR_Medical_Surgical_Description` | Column rename |
| `Payment Typology 1` | `Payment_Typology_1` | Column rename |
| `Payment Typology 2` | `Payment_Typology_2` | Column rename |
| `Payment Typology 3` | `Payment_Typology_3` | Column rename |
| `Emergency Department Indicator` | `Emergency_Department_Indicator` | Column rename |
| `Total Charges` | `Total_Charges` | Strip `$` and `,`, cast to `DoubleType` |
| `Total Costs` | `Total_Costs` | Strip `$` and `,`, cast to `DoubleType` |
| `Birth Weight` | `Birth_Weight` | Column rename |
| `Abortion Edit Indicator` | `Abortion_Edit_Indicator` | Column rename |
| *(system)* | `silver_load_timestamp` | **DERIVED**: `current_timestamp()` |
| *(system)* | `source_system` | **DERIVED**: Literal `"SPARCS"` |
| *(system)* | `silver_batch_id` | **DERIVED**: `silver_batch_<YYYYMMDD_HHMMSS>` |

---

## 3. Silver → Gold Mapping (Patient 360)

| Source Column (Silver) | Target Column (Gold) | Transformation / Derivation |
| :--- | :--- | :--- |
| `Age_Group`, `Gender`, `Race`, `Ethnicity`, `Zip_Code_3_digits` | `patient_surrogate_id` | **DERIVED**: Concatenated with `_` separator |
| `Age_Group` | `patient_age_group` | Direct alias |
| `Gender` | `patient_gender` | Direct alias |
| `Race` | `patient_race` | Direct alias |
| `Ethnicity` | `patient_ethnicity` | Direct alias |
| `Zip_Code_3_digits` | `patient_zip_code_3_digits` | Direct alias |
| *(Window)* | `encounter_sequence_number` | **DERIVED**: `ROW_NUMBER()` partitioned by patient, ordered by `Discharge_Year` |
| *(Window)* | `total_lifetime_encounters` | **DERIVED**: `COUNT(*)` over patient lifetime window |
| `Discharge_Year` | `encounter_discharge_year` | Cast to `IntegerType` |
| `Type_of_Admission` | `encounter_admission_type` | Direct alias |
| `Length_of_Stay` | `encounter_length_of_stay` | Direct alias |
| *(Window)* | `cumulative_length_of_stay` | **DERIVED**: Cumulative `SUM(Length_of_Stay)` ordered by year |
| *(Window)* | `avg_patient_length_of_stay` | **DERIVED**: `AVG(Length_of_Stay)` over lifetime |
| `Emergency_Department_Indicator` | `encounter_ed_indicator` | Direct alias |
| *(Window)* | `emergency_encounter_count` | **DERIVED**: `SUM` of admissions where `Type_of_Admission = 'Emergency'` |
| *(Window)* | `ed_visit_count` | **DERIVED**: `SUM` of visits where `Emergency_Department_Indicator = 'Y'` |
| `Facility_Id` | `current_encounter_facility_id` | Direct alias |
| `Facility_Name` | `current_encounter_facility_name` | Direct alias |
| `Hospital_County` | `current_encounter_hospital_county` | Direct alias |
| `Health_Service_Area` | `current_encounter_service_area` | Direct alias |
| `Operating_Certificate_Number` | `current_encounter_cert_number` | Cast to `IntegerType` |
| *(Window)* | `most_recent_facility_id` | **DERIVED**: `LAST(Facility_Id)` over ordered window |
| *(Window)* | `most_recent_facility_name` | **DERIVED**: `LAST(Facility_Name)` over ordered window |
| *(GroupBy)* | `unique_facilities_visited` | **DERIVED**: `COUNT_DISTINCT(Facility_Id)` per patient |
| `CCS_Diagnosis_Code` | `current_encounter_diagnosis_code` | Cast to `IntegerType` |
| `CCS_Diagnosis_Description` | `current_encounter_diagnosis_desc` | Direct alias |
| *(Window)* | `most_recent_diagnosis_code` | **DERIVED**: `LAST(CCS_Diagnosis_Code)` |
| *(Window)* | `most_recent_diagnosis` | **DERIVED**: `LAST(CCS_Diagnosis_Description)` |
| *(GroupBy)* | `unique_diagnoses_count` | **DERIVED**: `COUNT_DISTINCT(CCS_Diagnosis_Code)` |
| `APR_DRG_Code` | `current_encounter_drg_code` | Cast to `IntegerType` |
| `APR_DRG_Description` | `current_encounter_drg_desc` | Direct alias |
| *(Window)* | `most_recent_drg_code` | **DERIVED**: `LAST(APR_DRG_Code)` |
| *(Window)* | `most_recent_drg_description` | **DERIVED**: `LAST(APR_DRG_Description)` |
| `Total_Charges` | `current_encounter_charges` | Direct alias |
| `Total_Costs` | `current_encounter_costs` | Direct alias |
| *(Window)* | `cumulative_charges` | **DERIVED**: Cumulative `SUM(Total_Charges)` ordered by year |
| *(Window)* | `cumulative_costs` | **DERIVED**: Cumulative `SUM(Total_Costs)` ordered by year |
| *(Window)* | `lifetime_total_charges` | **DERIVED**: `SUM(Total_Charges)` over lifetime |
| *(Window)* | `lifetime_total_costs` | **DERIVED**: `SUM(Total_Costs)` over lifetime |
| *(Window)* | `lifetime_gross_margin` | **DERIVED**: `lifetime_total_charges - lifetime_total_costs` |
| *(Window)* | `avg_charge_per_encounter` | **DERIVED**: `AVG(Total_Charges)` over lifetime |
| *(Window)* | `avg_cost_per_encounter` | **DERIVED**: `AVG(Total_Costs)` over lifetime |
| `Payment_Typology_1` | `current_encounter_primary_payer` | Direct alias |
| *(Window)* | `most_frequent_payer` | **DERIVED**: `FIRST(Payment_Typology_1)` over patient window |
| *(GroupBy)* | `unique_payer_count` | **DERIVED**: `COUNT_DISTINCT(Payment_Typology_1)` |
| `Patient_Disposition` | `current_encounter_disposition` | Direct alias |
| *(Window)* | `most_recent_disposition` | **DERIVED**: `LAST(Patient_Disposition)` |
| *(Window)* | `home_discharge_count` | **DERIVED**: `SUM` where `Disposition = 'Home or Self Care'` |
| *(Window)* | `home_discharge_rate` | **DERIVED**: `(home_discharge_count / total_encounters) * 100` |
| *(Window)* | `ever_expired` | **DERIVED**: `"Yes"` if `SUM(expired encounters) > 0` |
| *(system)* | `data_quality_score` | **DERIVED**: Rule-based score (0-100) |
| *(system)* | `load_timestamp` | **DERIVED**: `current_timestamp()` |
| *(system)* | `etl_batch_id` | **DERIVED**: `gold_batch_<YYYYMMDD_HHMMSS>` |
| *(system)* | `record_status` | **DERIVED**: Literal `"ACTIVE"` |
| *(system)* | `is_current` | **DERIVED**: Literal `True` |
