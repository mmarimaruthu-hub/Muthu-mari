# Reports & Dashboards — Visual Mockups & Metric Definitions

## 1. Overview
This document provides structured layout mockups and metric definitions for the four BI dashboards built on the `gold_patient_360` Delta table.

---

## Dashboard 1: Executive Summary

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  ENTERPRISE PATIENT 360 — EXECUTIVE SUMMARY         [Filter: Year ▼] [Age ▼]│
├──────────────┬──────────────┬──────────────┬──────────────┬─────────────────┤
│  Total       │  Lifetime    │  Lifetime    │  Gross       │  Avg Length     │
│  Encounters  │  Charges     │  Costs       │  Margin      │  of Stay        │
│  2.4M        │  $9.8B       │  $7.2B       │  $2.6B       │  5.3 Days       │
├──────────────┴──────────────┴──────────────┴──────────────┴─────────────────┤
│  Patient Volume Trend (Line)            │  Home Discharge Rate (Gauge)        │
│  ▁▂▄▆█ 2013→2016                        │        ██████░░░░ 67.4%             │
├─────────────────────────────────────────┼─────────────────────────────────────┤
│  Top 10 Diagnoses by Volume (H-Bar)     │  Charges vs Costs by Year (Bar)     │
│  Septicemia        ████████ 120K        │  2016 ████(Chg) ███(Cost)           │
│  Pneumonia         ██████   95K         │  2015 ████(Chg) ███(Cost)           │
│  Heart Failure     █████    82K         │  2014 ███(Chg)  ██(Cost)            │
│  ...                                    │                                      │
└─────────────────────────────────────────┴─────────────────────────────────────┘
```

**Key KPI Definitions**:
| KPI | Formula | Target |
| :--- | :--- | :--- |
| Total Encounters | `COUNT(patient_surrogate_id)` | Baseline |
| Lifetime Charges | `SUM(lifetime_total_charges)` | Baseline |
| Lifetime Costs | `SUM(lifetime_total_costs)` | Minimize |
| Gross Margin | `SUM(lifetime_gross_margin)` | Maximize |
| Avg LOS | `AVG(avg_patient_length_of_stay)` | ≤ National Avg |
| Home Discharge Rate | `AVG(home_discharge_rate)` | > 65% |

---

## Dashboard 2: Patient Demographics

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  PATIENT DEMOGRAPHICS                     [Year ▼] [Gender ▼] [Zip ▼]       │
├─────────────────┬───────────────────┬─────────────────────────────────────── │
│ Age Group       │ Gender            │ Race                                    │
│ (Donut Chart)   │ (Pie Chart)       │ (Bar Chart)                             │
│                 │                   │                                          │
│ 18-29  22%  ●   │ Female 54%  ●     │ White         ████████ 58%              │
│ 30-49  28%  ●   │ Male   42%  ●     │ Black/Afr.Am  █████    32%              │
│ 50-69  30%  ●   │ Unknown 4% ●      │ Other         ██       10%              │
│ 70+    20%  ●   │                   │                                          │
├─────────────────┴───────────────────┴─────────────────────────────────────── │
│ Geographic Distribution by Zip (Bar)    │ Avg Encounters by Age & Gender      │
│ Zip 100 ████████ 45K                    │ Heat Map: Rows=Age, Cols=Gender      │
│ Zip 112 ██████   38K                    │         M       F       U            │
│ Zip 114 █████    30K                    │ 18-29  1.2     1.4     1.1           │
│ ...                                     │ 70+    3.1     2.8     2.5           │
└─────────────────────────────────────────┴─────────────────────────────────────┘
```

---

## Dashboard 3: Clinical Outcomes

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  CLINICAL OUTCOMES                           [Year ▼] [Severity ▼] [Age ▼]  │
├──────────────┬───────────────┬───────────────┬──────────────────────────────┤
│  Emergency   │  ED Visits    │  % Ever       │  Avg Data Quality            │
│  Encounters  │  Total        │  Expired      │  Score                       │
│  340K        │  128K         │  2.1%         │  87.3 / 100                  │
├──────────────┴───────────────┴───────────────┴──────────────────────────────┤
│  Severity Distribution (Donut)          │  Mortality Risk Distribution (Bar)  │
│  Minor    40%  ●                        │  Minor     ████████ 48%             │
│  Moderate 32%  ●                        │  Moderate  █████    30%             │
│  Major    20%  ●                        │  Major     ███      16%             │
│  Extreme   8%  ●                        │  Extreme   █        6%              │
├─────────────────────────────────────────┼─────────────────────────────────────┤
│  Home Discharge Rate by Age (Bar)       │  Top 10 Procedures by Volume (Bar)  │
│  18-29  ████████████ 81%                │  No Proc Performed  ██████████ 200K │
│  30-49  ███████████  75%                │  Physical Therapy   ████     80K    │
│  50-69  █████████    65%                │  Cardiac Monitoring ███      60K    │
│  70+    ██████       45%                │  ...                                │
└─────────────────────────────────────────┴─────────────────────────────────────┘
```

---

## Dashboard 4: Financial & Payer Analysis

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  FINANCIAL & PAYER ANALYSIS                [Year ▼] [Facility ▼] [Payer ▼] │
├──────────────┬───────────────┬───────────────┬──────────────────────────────┤
│  Total       │  Avg Cost /   │  Avg Charge / │  Top Payer                   │
│  Gross Margin│  Encounter    │  Encounter    │  Medicare 38%                │
│  $2.6B       │  $12,450      │  $16,800      │                              │
├──────────────┴───────────────┴───────────────┴──────────────────────────────┤
│  Charges by Payer Type (Stacked Bar)    │  Payer Mix (Pie)                    │
│  Medicare    ████████████ $3.8B         │  Medicare      38%  ●               │
│  Medicaid    ████████     $2.9B         │  Medicaid      28%  ●               │
│  Commercial  █████        $2.1B         │  Self-Pay      18%  ●               │
│  Self-Pay    ████         $1.5B         │  Commercial    14%  ●               │
│  Other       ██           $0.5B         │  Other          2%  ●               │
├─────────────────────────────────────────┼─────────────────────────────────────┤
│  Charges vs Costs Scatter (by Severity) │  Avg Charge by DRG (Bar)            │
│  ●●●● Major (high charges)              │  DRG 190 ████████ $45K              │
│  ●●  Minor (lower charges)              │  DRG 291 ███████  $38K              │
│                  Cost →                 │  DRG 392 ██████   $30K              │
└─────────────────────────────────────────┴─────────────────────────────────────┘
```

---

## 2. Infographic Summary (For Video 3 & Professional Report)

```
╔══════════════════════════════════════════════════════════════════════════╗
║        ENTERPRISE PATIENT 360 DATA PLATFORM — KEY INSIGHTS              ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  📊 SCALE           📋 DATA QUALITY       🏥 OUTCOMES                   ║
║  500M+ Records      Avg Score: 87/100     Home Discharge: 67%            ║
║  3-Layer Pipeline   < 5% Null Rate        ED Visits: 128K                ║
║  Daily Incremental  0 Duplicates          SNF Transfers: 45K             ║
║                                                                          ║
║  💰 FINANCIALS      👥 DEMOGRAPHICS       🔬 CLINICAL                   ║
║  $9.8B Charges      Unique Cohorts: 850K  Top Dx: Septicemia             ║
║  $7.2B Costs        Avg Age: 50-69        High Risk: 6%                  ║
║  $2.6B Margin       Medicare: 38%         Avg LOS: 5.3 Days              ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
```
