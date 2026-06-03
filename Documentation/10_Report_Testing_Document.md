# Report Testing Document

## 1. Overview
This document defines test cases for validating all BI dashboards and reports built on the `gold_patient_360` Delta table. The goal is to ensure every metric displayed on the dashboard exactly matches the underlying Gold table data.

---

## 2. Testing Approach
- **Method**: Each BI KPI is independently verified by running an equivalent SQL query against the Gold table in Databricks and comparing results to the report values.
- **Tool**: Databricks SQL Editor / Databricks Notebooks.
- **Pass Criteria**: Report value must match SQL query result within ±0.1% (to account for floating-point rounding).

---

## 3. Dashboard 1: Executive Summary — Test Cases

| Test ID | KPI / Visual Tested | Validation SQL | Expected Match |
| :--- | :--- | :--- | :--- |
| RPT-001 | Total Encounters KPI | `SELECT COUNT(*) FROM gold_patient_360` | Matches dashboard card value |
| RPT-002 | Total Lifetime Charges KPI | `SELECT ROUND(SUM(lifetime_total_charges),2) FROM gold_patient_360` | Matches dashboard card value |
| RPT-003 | Total Lifetime Costs KPI | `SELECT ROUND(SUM(lifetime_total_costs),2) FROM gold_patient_360` | Matches dashboard card value |
| RPT-004 | Gross Margin KPI | `SELECT ROUND(SUM(lifetime_gross_margin),2) FROM gold_patient_360` | Matches dashboard card value |
| RPT-005 | Avg Length of Stay KPI | `SELECT ROUND(AVG(avg_patient_length_of_stay),2) FROM gold_patient_360` | Matches dashboard card value |
| RPT-006 | Home Discharge Rate Gauge | `SELECT ROUND(AVG(home_discharge_rate),2) FROM gold_patient_360` | Matches gauge percentage |
| RPT-007 | Top Diagnosis (1st bar) | `SELECT current_encounter_diagnosis_desc, COUNT(*) as cnt FROM gold_patient_360 GROUP BY 1 ORDER BY cnt DESC LIMIT 1` | First bar label & value match |
| RPT-008 | Charges by Year (2016) | `SELECT SUM(current_encounter_charges) FROM gold_patient_360 WHERE encounter_discharge_year = 2016` | 2016 bar value matches |

---

## 4. Dashboard 2: Demographics — Test Cases

| Test ID | KPI / Visual Tested | Validation SQL | Expected Match |
| :--- | :--- | :--- | :--- |
| RPT-009 | Unique Patient Cohorts | `SELECT COUNT(DISTINCT patient_surrogate_id) FROM gold_patient_360` | Matches KPI card |
| RPT-010 | Age Group Distribution | `SELECT patient_age_group, COUNT(*) FROM gold_patient_360 GROUP BY 1 ORDER BY 2 DESC` | Each segment percentage matches donut chart |
| RPT-011 | Gender Distribution | `SELECT patient_gender, COUNT(*) FROM gold_patient_360 GROUP BY 1 ORDER BY 2 DESC` | Each slice matches pie chart |
| RPT-012 | Race Distribution | `SELECT patient_race, COUNT(*) FROM gold_patient_360 GROUP BY 1 ORDER BY 2 DESC` | Bar heights match |
| RPT-013 | Top Zip Code | `SELECT patient_zip_code_3_digits, COUNT(*) as cnt FROM gold_patient_360 GROUP BY 1 ORDER BY cnt DESC LIMIT 1` | Top bar matches |
| RPT-014 | Avg Encounters by Age/Gender | `SELECT patient_age_group, patient_gender, AVG(total_lifetime_encounters) FROM gold_patient_360 GROUP BY 1,2` | Heat map values match |

---

## 5. Dashboard 3: Clinical Outcomes — Test Cases

| Test ID | KPI / Visual Tested | Validation SQL | Expected Match |
| :--- | :--- | :--- | :--- |
| RPT-015 | Emergency Encounters Count | `SELECT SUM(emergency_encounter_count) FROM gold_patient_360` | Matches KPI card |
| RPT-016 | ED Visits Count | `SELECT SUM(ed_visit_count) FROM gold_patient_360` | Matches KPI card |
| RPT-017 | % Ever Expired | `SELECT ROUND(COUNT(CASE WHEN ever_expired='Yes' THEN 1 END) * 100.0 / COUNT(*), 2) FROM gold_patient_360` | Matches percentage |
| RPT-018 | Avg Data Quality Score | `SELECT ROUND(AVG(data_quality_score),1) FROM gold_patient_360` | Matches KPI card |
| RPT-019 | Severity Distribution | `SELECT current_encounter_severity_desc, ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(),1) FROM gold_patient_360 GROUP BY 1` | Each donut segment matches |
| RPT-020 | Home Discharge Rate by Age | `SELECT patient_age_group, ROUND(AVG(home_discharge_rate),2) FROM gold_patient_360 GROUP BY 1 ORDER BY 1` | Each bar matches |
| RPT-021 | Top Procedure by Volume | `SELECT current_encounter_procedure_desc, COUNT(*) FROM gold_patient_360 GROUP BY 1 ORDER BY 2 DESC LIMIT 1` | First bar matches |
| RPT-022 | SNF Transfer by Diagnosis | `SELECT current_encounter_diagnosis_desc, SUM(snf_transfer_count) FROM gold_patient_360 GROUP BY 1 ORDER BY 2 DESC LIMIT 10` | Top values match bars |

---

## 6. Dashboard 4: Financial & Payer — Test Cases

| Test ID | KPI / Visual Tested | Validation SQL | Expected Match |
| :--- | :--- | :--- | :--- |
| RPT-023 | Total Gross Margin | `SELECT ROUND(SUM(lifetime_gross_margin),2) FROM gold_patient_360` | Matches KPI |
| RPT-024 | Avg Cost per Encounter | `SELECT ROUND(AVG(avg_cost_per_encounter),2) FROM gold_patient_360` | Matches KPI |
| RPT-025 | Avg Charge per Encounter | `SELECT ROUND(AVG(avg_charge_per_encounter),2) FROM gold_patient_360` | Matches KPI |
| RPT-026 | Payer Mix (Top Payer) | `SELECT most_frequent_payer, COUNT(*) as cnt FROM gold_patient_360 GROUP BY 1 ORDER BY cnt DESC LIMIT 1` | Top payer label & % matches |
| RPT-027 | Charges by Payer (Medicare) | `SELECT SUM(current_encounter_charges) FROM gold_patient_360 WHERE current_encounter_primary_payer = 'Medicare'` | Medicare bar matches |
| RPT-028 | Top High-Cost Facility | `SELECT most_recent_facility_name, SUM(lifetime_total_costs) as tc FROM gold_patient_360 GROUP BY 1 ORDER BY tc DESC LIMIT 1` | Matches first bar |

---

## 7. Cross-Dashboard Consistency Tests

| Test ID | Test Description | Validation |
| :--- | :--- | :--- |
| RPT-029 | Total encounters consistent across dashboards | All dashboards showing "Total Encounters" must display the same value |
| RPT-030 | Charge totals consistent | Executive vs Financial dashboard charge totals match |
| RPT-031 | Demographics slices sum to 100% | All pie/donut charts must total exactly 100% |
| RPT-032 | Year filter consistency | Applying year=2016 filter on all dashboards reduces counts consistently |
| RPT-033 | No nulls displayed | No dashboard KPI or chart should display "null" or blank labels without a default |
