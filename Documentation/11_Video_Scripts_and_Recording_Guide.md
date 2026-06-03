# 4 Videos — Scripts & Recording Guide

## Overview
This document provides detailed scripts, talking points, screen-recording instructions, and shot lists for the 4 required submission videos.

---

# VIDEO 1: Explainer Video — Dataset & Problem Statement
**Duration**: 3–5 minutes | **Format**: Narrated presentation / slides + talking head

## Objective
Explain the business problem, the data landscape, and the motivation for building the Patient 360 platform.

## Script

### [INTRO — 0:00–0:30]
> "Healthcare organizations today are drowning in data — but starving for insights. Imagine a patient who visits three different hospitals, uses two insurance providers, and manages a chronic condition through a digital health app. Their data exists — but it's scattered across a dozen disconnected systems. No single doctor, administrator, or analyst can see the full picture.
>
> This is the challenge we set out to solve."

### [PROBLEM STATEMENT — 0:30–1:30]
> "Healthcare data is fragmented across:
> - **EHR Systems** — holding clinical encounters and diagnoses
> - **CRM Platforms** — holding patient engagement and communication history
> - **Billing Applications** — holding charges, costs, and payer information
> - **Patient Engagement Systems** — holding appointment and feedback data
>
> This fragmentation leads to:
> - Delayed clinical decisions due to incomplete patient history
> - Billing errors and revenue leakage
> - Inability to identify high-risk patients proactively
> - Poor patient experience and low engagement"

### [DATASET INTRODUCTION — 1:30–3:00]
> "For this platform, we work with a high-volume inpatient discharge dataset containing over **500 million patient-related records**. Each record captures:
> - **Demographics**: Age Group, Gender, Race, Ethnicity, Zip Code
> - **Clinical Details**: Diagnosis codes (CCS), Procedure codes, DRG codes, Severity of Illness, Risk of Mortality
> - **Encounter Information**: Facility, Length of Stay, Type of Admission, Discharge Disposition
> - **Financial Data**: Total Charges, Total Costs, Primary/Secondary/Tertiary Payer types"

### [USE CASE — 3:00–4:30]
> "Our solution: the **Enterprise Patient 360 Data Platform**. By unifying all this data on a single Databricks Lakehouse, we enable:
> - A complete **Patient Golden Record** — one view of every patient across all encounters
> - **Real-time Analytics** on clinical outcomes, financial performance, and care quality
> - **Proactive Risk Identification** — flagging patients with extreme mortality risk or high readmission patterns"

### [OUTRO — 4:30–5:00]
> "In the next videos, we'll walk you through the technical architecture, the data transformation pipeline, and the interactive dashboards that bring this Patient 360 vision to life."

## Recording Instructions
- Use PowerPoint/Google Slides with the project architecture overview slide
- Show a table/list of dataset fields on screen during the dataset section
- Use callout animations to highlight key statistics (500M records, 8 data domains)

---

# VIDEO 2: Explainer Video — Project Environment, Architecture & Implementation
**Duration**: 5–8 minutes | **Format**: Screen recording + narration

## Objective
Walk through the technical architecture — Databricks environment, Medallion layers, Auto Loader, Delta Lake, and BI integration.

## Script & Screen Recording Shots

### [INTRO — 0:00–0:30]
> "In this video, we walk through the complete technical architecture of the Enterprise Patient 360 Data Platform — from raw data ingestion all the way to interactive BI dashboards."

### [ENVIRONMENT SETUP — 0:30–1:30]
**Screen**: Show Databricks workspace homepage
> "The platform is built on **Databricks** — a unified analytics platform built on Apache Spark. Our environment includes:
> - A **Unity Catalog** with `workspace.default` as our primary schema
> - **Delta Lake** as the open-source storage format — providing ACID transactions, versioning, and time travel
> - **DBFS Volumes** for raw CSV file storage and Auto Loader checkpoints"

### [TECHNICAL ARCHITECTURE — 1:30–2:30]
**Screen**: Show architecture diagram (draw on whiteboard or show slide)
> "We follow the **Medallion Architecture** — three progressive layers:
> - **Bronze** — Raw ingestion, append-only, full fidelity
> - **Silver** — Cleansed, standardized, deduplicated
> - **Gold** — Business-level aggregations: the Patient 360 Golden Record"

### [BRONZE LAYER DEMO — 2:30–4:00]
**Screen**: Open `Bronze layer - Incremental.py` in Databricks
> "The Bronze layer uses **Databricks Auto Loader** — a structured streaming approach that monitors a source directory and automatically picks up new CSV files without reprocessing old ones. The checkpoint directory tracks exactly which files have been processed.
>
> On first run, it ingests ALL files. On subsequent runs — only new arrivals."
- Show the `cloudFiles` configuration
- Show the audit columns being added: `bronze_load_timestamp`, `source_file`, `bronze_batch_id`
- Show the post-validation output

### [SILVER LAYER DEMO — 4:00–5:30]
**Screen**: Open `Silver layer - Incremental.py`
> "The Silver layer reads from Bronze and applies a **watermark-based incremental filter** — only processing records that arrived after the last Silver load. Transformations include type casting, gender standardization, and column renaming. Records are upserted using a **MERGE INTO** statement on 11 business keys — preventing duplicates."
- Show the MERGE SQL being constructed
- Show the post-transformation validation results

### [GOLD LAYER DEMO — 5:30–7:00]
**Screen**: Open `Gold layer - Patient 360 - Incremental.py`
> "The Gold layer builds the Patient 360 using **PySpark Window functions** — computing cumulative charges, lifetime encounter counts, and severity history across all visits. Because window functions need full history, we recompute only the *affected patients* and MERGE the results."
- Show the Window specification code
- Show the 58-column final select
- Show the `MERGE INTO` / delete + insert pattern

### [VISUALIZATION — 7:00–8:00]
**Screen**: Show Databricks SQL Dashboard or Power BI connected to Gold table
> "The Gold table is partitioned and optimized — ready for BI tools like Power BI, Tableau, or Databricks SQL Dashboards to deliver real-time insights to clinical and financial stakeholders."

---

# VIDEO 3: Storytelling with Data — Dashboards & Infographics
**Duration**: 4–6 minutes | **Format**: Dashboard walkthrough + narration

## Objective
Tell a compelling data story using the dashboards — insights about patient outcomes, financial performance, and care quality.

## Script & Story Arc

### [OPENING HOOK — 0:00–0:30]
> "What does 500 million patient records tell us? Today, we uncover the story hidden in the data — a story about who our patients are, what care they receive, and what it costs."

### [SCENE 1: Who Are Our Patients? — 0:30–2:00]
**Screen**: Demographics Dashboard
> "Our patient population spans all age groups — with the **50-69 cohort** representing the largest share. **54% are female**. Geographic concentration is highest in urban zip codes.
>
> Crucially — Medicare is the dominant payer at **38%**, reflecting a predominantly senior and chronic-care population. This has direct implications for reimbursement strategy."

### [SCENE 2: What Care Are They Receiving? — 2:00–3:30]
**Screen**: Clinical Outcomes Dashboard
> "**Septicemia** is the top primary diagnosis — a serious blood infection requiring intensive care. **67% of patients** discharge safely home — a positive outcome, but **22% require skilled nursing facility transfers**, indicating significant post-acute care demand.
>
> The average length of stay is **5.3 days** — but for patients with Extreme severity, this climbs dramatically.
>
> **6% of encounters** carry the highest mortality risk — these are the patients who need the most immediate attention."

### [SCENE 3: What Does It Cost? — 3:30–5:00]
**Screen**: Financial Dashboard
> "Total lifetime charges across our patient cohort: **$9.8 billion**. Against costs of **$7.2 billion**, the gross margin is **$2.6 billion**.
>
> But the story isn't uniform. Self-pay patients — **18% of encounters** — represent a disproportionate cost burden with the lowest reimbursement. And facilities in certain service areas have significantly higher cost-per-encounter ratios.
>
> This is where the Patient 360 view unlocks real strategic value — connecting the clinical severity of a patient's condition directly to the financial impact on the organization."

### [OUTRO — 5:00–5:30]
> "Data without context is noise. But with the Patient 360 platform, every record becomes a chapter in a patient's story — and together, those stories tell us how to deliver better, more efficient, and more equitable care."

---

# VIDEO 4: Making-of Video — Screen Recording (End-to-End)
**Duration**: 8–12 minutes | **Format**: Continuous screen recording with narration

## Objective
Show the complete workflow — from identifying the dataset to final storytelling — as a process demonstration.

## Recording Checklist & Sequence

| Timestamp | Screen / Action | Narration |
| :--- | :--- | :--- |
| 0:00–1:00 | Show dataset source (SPARCS/data portal) | "We begin by identifying our dataset..." |
| 1:00–2:00 | Upload CSV files to Databricks Volume | "Loading raw data into our Databricks environment..." |
| 2:00–3:30 | Run Bronze Layer notebook — show output logs | "The Bronze layer ingests and tracks every file..." |
| 3:30–5:00 | Run Silver Layer notebook — show transformation logs | "Silver applies cleansing and standardization..." |
| 5:00–6:30 | Run Gold Layer notebook — show Patient 360 creation | "Gold builds the complete Patient 360 record..." |
| 6:30–8:00 | Query Gold table in Databricks SQL | "We validate our data with SQL queries..." |
| 8:00–10:00 | Open dashboard — show live charts | "The dashboards come alive with real insights..." |
| 10:00–11:30 | Walk through one key insight end-to-end | "Let's trace one story: emergency encounters by age..." |
| 11:30–12:00 | Closing — show the full pipeline flow | "From raw CSV to Patient 360 — in three automated steps." |

## Recording Tips
- Use OBS Studio or Windows Game Bar (Win+G) for screen recording
- Record at 1920×1080 resolution minimum
- Speak at a measured pace — pause between sections
- Highlight mouse clicks for clarity
- Add a subtle background music track (royalty-free)
