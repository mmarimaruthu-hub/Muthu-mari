# Power BI Dashboard Design & Relationship Analysis

This document provides a comprehensive dashboard design, layout wireframes, visual specifications, and relationship analysis for the **Enterprise Patient 360** Power BI data model.

---

## 1. Power BI Data Model Audit & Review

Based on the relationship diagrams in the model view (`image_0.png` and `image_1.png`), the data model comprises 3 Dimension tables and 5 Fact/Summary tables:

### Dimension Tables
1. **`dim_diagnosis`**: Contains `CCS_Diagnosis_Code` (Primary Key), `CCS_Diagnosis_Description`, and a system `Column`.
2. **`dim_payer`**: Contains `Payment_Typology_1` (Primary Key).
3. **`dim_facility1`**: Contains `Facility_Id` (Primary Key) and `Facility_Name`.

### Fact / Summary Tables
1. **`gold_billing_analytics`**: Connected to `dim_diagnosis[CCS_Diagnosis_Code]` (1-to-many, single direction).
2. **`gold_diagnosis_analytics`**: Connected to `dim_diagnosis[CCS_Diagnosis_Code]` (1-to-many, single direction).
3. **`gold_facility_performance`**: Connected to `dim_facility1[Facility_Id]` (1-to-many, single direction).
4. **`gold_payer_mix_analytics`**: Connected to `dim_payer[Payment_Typology_1]` (1-to-many, single direction).
5. **`gold_patient_demographics`**: **Isolated (Disconnected)**. No relationship lines are established.

---

### Critical Model Issues & Structural Observations

> [!WARNING]
> **1. Disconnected Patient Demographics Table**
> The `gold_patient_demographics` table is completely isolated. If a user adds a slicer for `Age_Group`, `Gender`, or `Ethnicity` from this table, it will **not** filter any visuals built from `gold_billing_analytics`, `gold_diagnosis_analytics`, `gold_facility_performance`, or `gold_payer_mix_analytics`. 
>
> In the current architecture, demographics are pre-aggregated in separate tables or hardcoded as columns (e.g., `pediatric_cases`, `senior_cases` in `gold_diagnosis_analytics`). Consequently, cross-functional demographic filtering is impossible without restructuring the upstream PySpark ETL output.

> [!IMPORTANT]
> **2. Siloed Star Schemas (No Shared Dimensions)**
> The data model consists of four separate star schemas. 
> * You cannot slice `gold_facility_performance` by Payer or Diagnosis.
> * You cannot slice `gold_payer_mix_analytics` by Facility or Diagnosis.
> This prevents cross-dimensional analysis (e.g., "What is the Payer Mix for a specific Diagnosis at a specific Facility?").

> [!NOTE]
> **3. Redundant Field in Fact Table**
> `gold_billing_analytics` contains the column `CCS_Diagnosis_Description`. Since this field is already present in `dim_diagnosis`, it is redundant in the fact table. Keeping it violates clean dimensional modeling principles, increases model size, and can confuse report builders. It should be hidden or removed from the fact table in the Power BI model.

---

## 2. Professional Healthcare Design System

To establish a premium, clean clinical feel, the dashboard should avoid default primary colors and adopt a modern, trustworthy clinical palette.

### Color Palette (Teal & Slate Modern)
* **Primary (60%)**: `#0F172A` (Deep Slate Blue) - Used for background containers, headers, text, and structure.
* **Secondary (30%)**: `#0EA5E9` (Vibrant Teal/Cyan) - Used for primary metrics, trend lines, and positive indicators.
* **Accent (10%)**: `#10B981` (Emerald Green) - Used for positive margins, home discharge rates, and targets.
* **Alert/Warning**: `#F43F5E` (Rose Red) - Used for high-cost outliers, extreme cases, and mortality rates.
* **Neutral Light**: `#F8FAFC` (Off-white/Light Gray) - Canvas background.
* **Neutral Dark**: `#1E293B` (Cool Gray) - Visual card backgrounds (glassmorphism/flat design).

### Typography
* **Primary Font**: `Segoe UI` or `Inter` (if available via custom theme).
* **Metric Callouts**: Bold, `DIN` or `Segoe UI Semibold` (Font size: 28-36 pt).
* **Visual Headers**: Semi-bold, `Segoe UI` (Font size: 11-12 pt, uppercase or title case, `#475569` text color).

---

## 3. Four-Page Dashboard Design & Visual Specifications

### Page 1: Executive Summary
**Purpose**: High-level operational, financial, and clinical overview for hospital administrators.
**Global Slicers (Note: These are local to their specific schemas due to the disconnected model)**:
* `dim_facility1[Facility_Name]` (Filters Facility Performance visuals)
* `dim_payer[Payment_Typology_1]` (Filters Payer Mix visuals)
* `dim_diagnosis[CCS_Diagnosis_Description]` (Filters Billing & Diagnosis visuals)

#### Key DAX Measures to Create
```dax
Total Charges (Dx) = SUM(gold_diagnosis_analytics[total_charges])
Total Costs (Dx) = SUM(gold_diagnosis_analytics[total_costs])
Net Margin (Dx) = [Total Charges (Dx)] - [Total Costs (Dx)]
Margin % (Dx) = DIVIDE([Net Margin (Dx)], [Total Charges (Dx)], 0)
Average LOS = AVERAGE(gold_facility_performance[avg_length_of_stay])
Total Cases (Dx) = SUM(gold_diagnosis_analytics[pediatric_cases]) + SUM(gold_diagnosis_analytics[young_adult_cases]) + SUM(gold_diagnosis_analytics[senior_cases])
```

#### Visual Layout Placement (Executive Summary)
```
┌─────────────────────────────────────────────────────────────────────────────┐
│  ENTERPRISE PATIENT 360 — EXECUTIVE SUMMARY       [Facility ▼] [Payer ▼] [Dx ▼] │
├──────────────┬──────────────┬──────────────┬──────────────┬─────────────────┤
│  Total Cases │  Tot Charges │  Total Costs │  Net Margin  │  Avg Length     │
│  2.4M        │  $9.8B       │  $7.2B       │  $2.6B       │  5.3 Days       │
│  [Measure]   │  [Measure]   │  [Measure]   │  [Measure]   │  [Measure]      │
├──────────────┴──────────────┴──────────────┴──────────────┴─────────────────┤
│  [Visual 1] Financial Overview (Area Chart)      │ [Visual 2] Operational    │
│  X-Axis: dim_diagnosis[CCS_Diagnosis_Description]│            KPIs (Card Grid)│
│  Values: [Total Charges (Dx)], [Total Costs (Dx)]│  - CMI: [Avg CMI]        │
│                                                  │  - ED Visits: [Tot ED]   │
├──────────────────────────────────────────────────┼──────────────────────────┤
│  [Visual 3] Top 10 Diagnoses by Case Volume      │ [Visual 4] Payer Market  │
│             (Horizontal Bar Chart)               │            Share (Donut) │
│  Y-Axis: dim_diagnosis[CCS_Diagnosis_Description]│  Legend: dim_payer       │
│  X-Axis: [Total Cases (Dx)]                      │  Values: market_share_%  │
└──────────────────────────────────────────────────┴──────────────────────────┘
```

#### Visual Details:
1. **Financial Overview (Area/Line Chart)**:
   * **Fields**: Axis = `dim_diagnosis[CCS_Diagnosis_Description]`, Values = `[Total Charges (Dx)]` and `[Total Costs (Dx)]`.
   * **Aesthetics**: High contrast area chart with slate fill for costs and a cyan line for charges.
2. **Operational KPIs (Card Grid)**:
   * **Fields**: `AVERAGE(gold_facility_performance[case_mix_index])`, `SUM(gold_patient_demographics[ed_visits])` (Note: ED Visits visual is static and won't respond to Facility/Payer/Dx filters).
3. **Top 10 Diagnoses by Volume (Horizontal Bar)**:
   * **Fields**: Y-Axis = `dim_diagnosis[CCS_Diagnosis_Description]`, X-Axis = `[Total Cases (Dx)]`.
4. **Payer Market Share (Donut)**:
   * **Fields**: Legend = `dim_payer[Payment_Typology_1]`, Value = `AVERAGE(gold_payer_mix_analytics[market_share_percentage])`.

---

### Page 2: Diagnosis Analytics
**Purpose**: Detailed clinical and cost analysis of patient diagnosis groups.
**Slicers**:
* `dim_diagnosis[CCS_Diagnosis_Description]` (Conformed drop-down filter - will filter all visuals on this page).

#### Visual Layout Placement (Diagnosis Analytics)
```
┌─────────────────────────────────────────────────────────────────────────────┐
│  CLINICAL DIAGNOSIS ANALYTICS                           [Diagnosis Slicer ▼]│
├──────────────┬──────────────┬──────────────┬──────────────┬─────────────────┤
│  Total Dx    │  Total Dx    │  Avg Cost    │  Urgent      │  SNF Transfers  │
│  Charges     │  Costs       │  per Case    │  Admissions  │  Count          │
│  $9.8B       │  $7.2B       │  $12.4K      │  340K        │  45K            │
├──────────────┴──────────────┴──────────────┴──────────────┴─────────────────┤
│  [Visual 1] Diagnosis Volume by Age Group        │ [Visual 2] Cost vs. Charge │
│             (100% Stacked Column Chart)          │            by MDC (Bar)     │
│  X-Axis: dim_diagnosis[CCS_Diagnosis_Description]│  Y-Axis: APR_MDC_Desc       │
│  Values: pediatric_cases, young_adult_cases,     │  X-Axis: avg_charge,        │
│          senior_cases (Stacked Legend)           │          avg_cost           │
├──────────────────────────────────────────────────┼──────────────────────────┤
│  [Visual 3] Urgent Admissions vs SNF Transfers   │ [Visual 4] Billing & LOS │
│             (Dual-Axis Column & Line Chart)      │            Detail (Table)│
│  X-Axis: dim_diagnosis[CCS_Diagnosis_Description]│  - Diagnosis Desc        │
│  Column: urgent_admissions                       │  - Avg Length of Stay    │
│  Line:   snf_transfers                           │  - Extreme Cases Count   │
└──────────────────────────────────────────────────┴──────────────────────────┘
```

#### Visual Details:
1. **Diagnosis Volume by Age Group (100% Stacked Column)**:
   * **Fields**: X-Axis = `dim_diagnosis[CCS_Diagnosis_Description]`, Y-Axis/Values = `SUM(gold_diagnosis_analytics[pediatric_cases])`, `SUM(gold_diagnosis_analytics[young_adult_cases])`, and `SUM(gold_diagnosis_analytics[senior_cases])`.
   * **Aesthetics**: Color-coded segments representing life stages (Teal for pediatric, Slate for young adult, Emerald for senior).
2. **Cost vs. Charge by Major Diagnostic Category (MDC) (Clustered Bar Chart)**:
   * **Fields**: Y-Axis = `gold_billing_analytics[APR_MDC_Description]`, X-Axis = `AVERAGE(gold_billing_analytics[avg_charge])` and `AVERAGE(gold_billing_analytics[avg_cost])`.
3. **Urgent Admissions vs. SNF Transfers (Dual Axis Chart)**:
   * **Fields**: X-Axis = `dim_diagnosis[CCS_Diagnosis_Description]`, Column (Y-Axis 1) = `SUM(gold_diagnosis_analytics[urgent_admissions])`, Line (Y-Axis 2) = `SUM(gold_diagnosis_analytics[snf_transfers])`.
4. **Billing & LOS Detail (Matrix/Table)**:
   * **Fields**: Rows = `dim_diagnosis[CCS_Diagnosis_Description]`, Values = `AVERAGE(gold_billing_analytics[avg_length_of_stay])`, `SUM(gold_billing_analytics[extreme_cases])`, `SUM(gold_billing_analytics[facilities_treating])`.

---

### Page 3: Facility Performance Analytics
**Purpose**: Benchmarking clinical efficiency, complexity, and admission profiles across hospital sites.
**Slicers**:
* `dim_facility1[Facility_Name]` (Conformed multi-select filter).

#### Visual Layout Placement (Facility Performance)
```
┌─────────────────────────────────────────────────────────────────────────────┐
│  FACILITY PERFORMANCE & OPERATIONAL EFFICIENCY            [Facility Name ▼] │
├──────────────┬──────────────┬──────────────┬──────────────┬─────────────────┤
│  Average     │  Case Mix    │  Emergency   │  Home        │  Discharged     │
│  LOS (Days)  │  Index (CMI) │  Admissions  │  Discharges  │  to SNF         │
│  5.3 Days    │  1.42        │  340K        │  67.4%       │  45K            │
├──────────────┴──────────────┴──────────────┴──────────────┴─────────────────┤
│  [Visual 1] Operational Efficiency Grid          │ [Visual 2] Admission Profile│
│             (Scatter Chart)                      │            (Stacked Bar)    │
│  X-Axis: avg_length_of_stay                      │  Y-Axis: Facility_Name      │
│  Y-Axis: case_mix_index                          │  Values: elective_admissions│
│  Details/Bubble: dim_facility1[Facility_Name]    │          emergency_admissions│
├──────────────────────────────────────────────────┼──────────────────────────┤
│  [Visual 3] Cost & Charge Efficiency by Facility │ [Visual 4] Discharge     │
│             (Clustered Column Chart)             │            Outcomes (Bar)│
│  X-Axis: dim_facility1[Facility_Name]            │  X-Axis: Facility_Name      │
│  Values: avg_charge_per_discharge,               │  Values: discharged_home,   │
│          avg_cost_per_discharge                  │          discharged_to_snf  │
└──────────────────────────────────────────────────┴──────────────────────────┘
```

#### Visual Details:
1. **Operational Efficiency Grid (Scatter Chart)**:
   * **Fields**: X-Axis = `AVERAGE(gold_facility_performance[avg_length_of_stay])`, Y-Axis = `AVERAGE(gold_facility_performance[case_mix_index])`, Details = `dim_facility1[Facility_Name]`.
   * **Insight**: Quick visual to spot facilities with high Length of Stay relative to patient complexity (Case Mix Index).
2. **Admission Profile (Stacked Bar Chart)**:
   * **Fields**: Y-Axis = `dim_facility1[Facility_Name]`, X-Axis = `SUM(gold_facility_performance[elective_admissions])` and `SUM(gold_facility_performance[emergency_admissions])`.
3. **Cost & Charge Efficiency (Clustered Column)**:
   * **Fields**: X-Axis = `dim_facility1[Facility_Name]`, Values = `AVERAGE(gold_facility_performance[avg_charge_per_discharge])` and `AVERAGE(gold_facility_performance[avg_cost_per_discharge])`.
4. **Discharge Outcomes (Clustered Column)**:
   * **Fields**: X-Axis = `dim_facility1[Facility_Name]`, Values = `SUM(gold_facility_performance[discharged_home])` and `SUM(gold_facility_performance[discharged_to_snf])`.

---

### Page 4: Payer Mix Analytics
**Purpose**: Financial performance, margins, and market share by payer typology.
**Slicers**:
* `dim_payer[Payment_Typology_1]` (Conformed multi-select filter).

#### Visual Layout Placement (Payer Mix Analytics)
```
┌─────────────────────────────────────────────────────────────────────────────┐
│  PAYER MIX & REVENUE CYCLE PERFORMANCE                    [Payer Typology ▼]│
├──────────────┬──────────────┬──────────────┬──────────────┬─────────────────┤
│  Total Payer │  Total Payer │  Average     │  Mortality   │  Home           │
│  Charges     │  Costs       │  Margin %    │  Count       │  Discharges     │
│  $9.8B       │  $7.2B       │  26.5%       │  2.1%        │  67.4%          │
├──────────────┴──────────────┴──────────────┴──────────────┴─────────────────┤
│  [Visual 1] Payer Market Share vs. Profit Margin │ [Visual 2] Payer Margin     │
│             (Bubble Chart)                       │            Breakdown (Bar)  │
│  X-Axis: market_share_percentage                 │  Y-Axis: dim_payer          │
│  Y-Axis: margin_percentage                       │  X-Axis: total_charges,     │
│  Size:   total_charges | Label: dim_payer        │          total_costs        │
├──────────────────────────────────────────────────┼──────────────────────────┤
│  [Visual 3] Payer Outcomes (Mortality & Home)    │ [Visual 4] Revenue Matrix│
│             (Dual-Axis Line & Column Chart)      │            by Payer (Table)│
│  X-Axis: dim_payer[Payment_Typology_1]           │  - Payer Typology        │
│  Column: home_discharges                         │  - Total Charges / Costs │
│  Line:   mortality_rate                          │  - Margin / Share %      │
└──────────────────────────────────────────────────┴──────────────────────────┘
```

#### Visual Details:
1. **Payer Market Share vs. Profit Margin (Bubble Chart)**:
   * **Fields**: X-Axis = `AVERAGE(gold_payer_mix_analytics[market_share_percentage])`, Y-Axis = `AVERAGE(gold_payer_mix_analytics[margin_percentage])`, Size = `SUM(gold_payer_mix_analytics[total_charges])`, Label = `dim_payer[Payment_Typology_1]`.
2. **Payer Margin Breakdown (Clustered Bar Chart)**:
   * **Fields**: Y-Axis = `dim_payer[Payment_Typology_1]`, X-Axis = `SUM(gold_payer_mix_analytics[total_charges])` and `SUM(gold_payer_mix_analytics[total_costs])`.
3. **Payer Outcomes (Dual-Axis Column & Line)**:
   * **Fields**: X-Axis = `dim_payer[Payment_Typology_1]`, Column (Y-Axis 1) = `SUM(gold_payer_mix_analytics[home_discharges])`, Line (Y-Axis 2) = `AVERAGE(gold_payer_mix_analytics[mortality_rate])`.
4. **Revenue Matrix by Payer (Matrix Table)**:
   * **Fields**: Rows = `dim_payer[Payment_Typology_1]`, Values = `SUM(gold_payer_mix_analytics[total_charges])`, `SUM(gold_payer_mix_analytics[total_costs])`, `AVERAGE(gold_payer_mix_analytics[margin_percentage])`, `AVERAGE(gold_payer_mix_analytics[market_share_percentage])`, `SUM(gold_payer_mix_analytics[mortality_count])`.

---

## 4. Recommendations for Model Optimization

To transform this from separate silos into an integrated, enterprise-grade data model, implement the following changes:

### 1. Fix the Disconnected Demographics Table
To utilize demographic filters (`Age_Group`, `Gender`, `Ethnicity`) across all pages, establish relationships:
* **Recommendation**: If demographic attributes exist in the underlying Bronze/Silver tables for all records, avoid aggregating them in PySpark into a single isolated table (`gold_patient_demographics`). Instead, keep demographic attributes in a conformed `dim_patient` or `dim_demographics` table, and relate it to `gold_billing_analytics`, `gold_diagnosis_analytics`, `gold_facility_performance`, and `gold_payer_mix_analytics` using a surrogate key (`Patient_Demographic_Id`).

### 2. Introduce a Conformed Date/Calendar Table
Currently, there is no shared Date table to slice metrics across time.
* **Recommendation**: Create a standard `dim_date` table in Power BI (via DAX or Power Query) and relate it to:
  * `gold_billing_analytics[gold_load_timestamp]`
  * `gold_diagnosis_analytics[source_table]` (or extract the year field)
  * `gold_payer_mix_analytics[source_table]`
  * `gold_facility_performance`
  This will enable a single global Date slicer on the Executive Summary.

### 3. Remove Redundant Columns
* **Recommendation**: Hide or delete `gold_billing_analytics[CCS_Diagnosis_Description]` in the model and use `dim_diagnosis[CCS_Diagnosis_Description]` instead. This ensures a clean star-schema design and reduces the column footprint.

### 4. Create a Central Patient Encounter Bridge Table
To enable multi-dimensional analysis (e.g. slicing Payer Mix by Diagnosis), restructure the data model:
* **Recommendation**: Instead of creating heavily pre-aggregated summary tables (`gold_..._analytics`) in the PySpark layer, load a granular transaction-level table (`fact_encounter`) containing:
  * `Encounter_Id` (Key)
  * `Facility_Id` (FK)
  * `CCS_Diagnosis_Code` (FK)
  * `Payment_Typology_1` (FK)
  * `Patient_Demographic_Id` (FK)
  * Measures: `Charges`, `Costs`, `Length_of_Stay`, `Discharge_Status`, etc.
  
With a central `fact_encounter` table, Power BI can resolve all cross-dimensional queries seamlessly through simple 1-to-many relationships from conformed dimensions (`dim_diagnosis`, `dim_facility`, `dim_payer`, `dim_patient`, `dim_date`). This dramatically improves flexibility and matches industry-standard warehousing best practices.
