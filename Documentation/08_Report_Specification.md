# Report Specification Document

## 1. Overview
This document defines the specifications for all BI dashboards and reports powered by the Gold layer `gold_patient_360` Delta table.

---

## 2. Dashboard 1: Executive Summary Dashboard

**Purpose**: Provide C-suite and hospital administrators a high-level view of patient volume, financial performance, and care quality.

**Target Audience**: Healthcare CXOs, Hospital Administrators, Finance Directors

**Data Source**: `workspace.default.gold_patient_360`

**Refresh Cadence**: Daily

| Metric / Visual | Type | Field(s) Used | Calculation | Filter |
| :--- | :--- | :--- | :--- | :--- |
| Total Patient Encounters | KPI Card | `total_lifetime_encounters` | `COUNT(patient_surrogate_id)` | Year |
| Total Lifetime Charges | KPI Card | `lifetime_total_charges` | `SUM(lifetime_total_charges)` | Year |
| Total Lifetime Costs | KPI Card | `lifetime_total_costs` | `SUM(lifetime_total_costs)` | Year |
| Gross Margin | KPI Card | `lifetime_gross_margin` | `SUM(lifetime_gross_margin)` | Year |
| Avg Length of Stay | KPI Card | `avg_patient_length_of_stay` | `AVG(avg_patient_length_of_stay)` | Year |
| Home Discharge Rate | Gauge | `home_discharge_rate` | `AVG(home_discharge_rate)` | Year |
| Avg Data Quality Score | KPI Card | `data_quality_score` | `AVG(data_quality_score)` | None |
| Patient Volume Trend | Line Chart | `encounter_discharge_year`, `patient_surrogate_id` | `COUNT(patient_surrogate_id)` by year | None |
| Charges vs Costs by Year | Grouped Bar | `encounter_discharge_year`, charges, costs | `SUM` by year | None |
| Top 10 Diagnoses by Volume | Horizontal Bar | `current_encounter_diagnosis_desc` | `COUNT(*)` by diagnosis | Year, Age Group |

---

## 3. Dashboard 2: Patient Demographics Dashboard

**Purpose**: Understand the patient population composition — who they are, where they are from, and how they are distributed across age, gender, race, and ethnicity.

**Target Audience**: Clinical Teams, Equity & Diversity Officers, Population Health Managers

**Data Source**: `workspace.default.gold_patient_360`

| Metric / Visual | Type | Field(s) Used | Calculation | Filter |
| :--- | :--- | :--- | :--- | :--- |
| Unique Patient Cohorts | KPI Card | `patient_surrogate_id` | `COUNT(DISTINCT patient_surrogate_id)` | Year |
| Patient Distribution by Age Group | Donut Chart | `patient_age_group` | `COUNT(*)` by age group | Year |
| Patient Distribution by Gender | Pie Chart | `patient_gender` | `COUNT(*)` by gender | Year |
| Patient Distribution by Race | Bar Chart | `patient_race` | `COUNT(*)` by race | Year |
| Patient Distribution by Ethnicity | Bar Chart | `patient_ethnicity` | `COUNT(*)` by ethnicity | Year |
| Geographic Distribution | Map / Bar | `patient_zip_code_3_digits` | `COUNT(*)` by zip | Year |
| Avg Encounters by Demographic | Heat Map | `patient_age_group`, `patient_gender` | `AVG(total_lifetime_encounters)` | Year |
| Race vs Avg Length of Stay | Bar Chart | `patient_race`, `avg_patient_length_of_stay` | `AVG(avg_patient_length_of_stay)` by race | Year |

---

## 4. Dashboard 3: Clinical Outcomes Dashboard

**Purpose**: Monitor clinical severity, risk, emergency visits, and patient outcome patterns.

**Target Audience**: Clinical Directors, Chief Medical Officers, Care Quality Teams

**Data Source**: `workspace.default.gold_patient_360`

| Metric / Visual | Type | Field(s) Used | Calculation | Filter |
| :--- | :--- | :--- | :--- | :--- |
| Total Emergency Encounters | KPI Card | `emergency_encounter_count` | `SUM(emergency_encounter_count)` | Year, Age Group |
| Total ED Visits | KPI Card | `ed_visit_count` | `SUM(ed_visit_count)` | Year |
| % Ever Expired | KPI Card | `ever_expired` | `COUNT(ever_expired='Yes') / COUNT(*)` | Year |
| Severity Distribution | Donut Chart | `current_encounter_severity_desc` | `COUNT(*)` by severity | Year |
| Mortality Risk Distribution | Bar Chart | `current_encounter_mortality_risk` | `COUNT(*)` by risk | Year |
| Top 10 Procedures by Volume | Bar Chart | `current_encounter_procedure_desc` | `COUNT(*)` by procedure | Year |
| Home Discharge Rate by Age | Bar Chart | `patient_age_group`, `home_discharge_rate` | `AVG(home_discharge_rate)` by age | Year |
| SNF Transfer Count by Diagnosis | Bar Chart | `current_encounter_diagnosis_desc`, `snf_transfer_count` | `SUM(snf_transfer_count)` by diagnosis | Year |
| Hospital Transfer Trend | Line Chart | `encounter_discharge_year`, `hospital_transfer_count` | `SUM` by year | None |
| Avg Severity by Facility | Bar Chart | `most_recent_facility_name`, severity code | `AVG(current_encounter_severity_code)` | Year |

---

## 5. Dashboard 4: Financial & Payer Dashboard

**Purpose**: Analyze cost drivers, payer mix, and financial efficiency across patient cohorts and facilities.

**Target Audience**: CFO, Revenue Cycle Management, Finance Analysts

**Data Source**: `workspace.default.gold_patient_360`

| Metric / Visual | Type | Field(s) Used | Calculation | Filter |
| :--- | :--- | :--- | :--- | :--- |
| Total Charges by Payer | Stacked Bar | `current_encounter_primary_payer`, `current_encounter_charges` | `SUM(charges)` by payer | Year |
| Avg Cost per Encounter by Age | Bar Chart | `patient_age_group`, `avg_cost_per_encounter` | `AVG(avg_cost_per_encounter)` | Year |
| Top 5 High-Cost Facilities | Bar Chart | `most_recent_facility_name`, `lifetime_total_costs` | `SUM(costs)` by facility | Year |
| Payer Mix Distribution | Pie Chart | `most_frequent_payer` | `COUNT(*)` by payer | Year |
| Charges vs Costs Scatter | Scatter Plot | `current_encounter_charges`, `current_encounter_costs` | Direct plot per record | Severity, Age Group |
| Gross Margin by Service Area | Map / Bar | `current_encounter_service_area`, `lifetime_gross_margin` | `SUM(margin)` by area | Year |
| Avg Charge by DRG | Bar Chart | `current_encounter_drg_desc`, `current_encounter_charges` | `AVG(charges)` by DRG | Year |

---

## 6. Global Filters (All Dashboards)
- **Discharge Year**: Multi-select dropdown (`encounter_discharge_year`)
- **Age Group**: Multi-select (`patient_age_group`)
- **Gender**: Multi-select (`patient_gender`)
- **Facility**: Multi-select (`current_encounter_facility_name`)
- **Service Area**: Multi-select (`current_encounter_service_area`)
- **Admission Type**: Multi-select (`encounter_admission_type`)
