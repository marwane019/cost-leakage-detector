# Operations Cost Leakage Detector

> **Automated detection, scoring, and escalation of procurement anomalies — from raw transaction data to executive output in a single pipeline run.**

[![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)](https://python.org)
[![Pandas](https://img.shields.io/badge/Pandas-2.0+-green)](https://pandas.pydata.org)
[![Plotly](https://img.shields.io/badge/Plotly-5.18+-purple)](https://plotly.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)

---

## Problem Statement

Operations teams bleed money through undetected process failures: duplicate invoices that slip through AP, suppliers quietly inflating rates above contract baselines, SLA breaches that go unpenalised, and procurement volume spikes that signal unauthorised purchases or system errors.

**Manual detection is too slow.** By the time a finance analyst spots a pattern, the leakage has compounded across weeks of transactions.

This tool automates the full detection lifecycle:

```
Raw transactions → 4-rule anomaly engine → severity scoring → Excel report + dashboard → Slack escalation
```

In a real deployment over a 90-day procurement period, the engine surfaces **£X potential leakage across Y flagged transactions** — findings that would take a team of analysts several days to identify manually.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     CONFIG LAYER                            │
│              config.yaml  /  .env                           │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                     DATA LAYER                              │
│   data_generator.py  ──►  data/raw/transactions.csv         │
│   (90-day procurement: 8 suppliers, 5 categories, ~4k txns) │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  DETECTION ENGINE                           │
│                    detector.py                              │
│                                                             │
│  Rule 1 │ Duplicate Transactions  (same supplier+amt±1d)    │
│  Rule 2 │ Price Variance          (>15% above baseline)     │
│  Rule 3 │ SLA Breach              (actual > expected date)  │
│  Rule 4 │ Volume Spike            (>2σ from rolling mean)   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   SCORING LAYER                             │
│                     scorer.py                               │
│                                                             │
│   Financial impact × Rule weight → Composite Score (0-100) │
│   Low  │ Medium  │ High  │ Critical                         │
└──────┬──────────────────┬─────────────────────┬────────────┘
       │                  │                     │
       ▼                  ▼                     ▼
┌──────────────┐  ┌─────────────┐  ┌──────────────────────┐
│  REPORTING   │  │  DASHBOARD  │  │    ALERT LAYER       │
│ reporter.py  │  │dashboard.py │  │    alerter.py        │
│              │  │             │  │                      │
│ Excel report │  │ Plotly HTML │  │  Slack webhook POST  │
│ 3-sheet WB   │  │ 4 charts    │  │  (Critical only)     │
│ KPI tiles    │  │ responsive  │  │  Block Kit format    │
└──────────────┘  └─────────────┘  └──────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│                    SCHEDULER LAYER                          │
│   scheduler.py (APScheduler)  +  n8n_workflow.json          │
│   Daily 07:00 London → detect → report → dashboard → alert  │
└─────────────────────────────────────────────────────────────┘
```

---

## Quickstart

> A senior engineer should be able to run this from scratch in under 10 minutes.

### 1. Clone and set up environment

```bash
git clone https://github.com/YOUR_USERNAME/cost-leakage-detector.git
cd cost-leakage-detector

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure (optional)

All thresholds, paths, and supplier definitions live in `config.yaml`. The defaults work out of the box.

For Slack alerts, copy the environment template:

```bash
cp .env.example .env
# Edit .env and set SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

### 3. Run the full pipeline

```bash
# Generate data, detect anomalies, score, report, dashboard, alert — all at once
python main.py --full-run
```

Outputs land in `data/output/`:
- `leakage_report_YYYY-MM-DD.xlsx`  — 3-sheet Excel workbook
- `leakage_dashboard_YYYY-MM-DD.html` — interactive Plotly dashboard (open in browser)

### 4. Run individual stages

```bash
python main.py --generate-data              # Create synthetic dataset only
python main.py --detect --report            # Detect anomalies + generate Excel
python main.py --detect --dashboard         # Detect + build interactive dashboard
python main.py --detect --alert             # Detect + send Slack (if webhook set)
python main.py --full-run --log-level DEBUG # Verbose pipeline run
```

### 5. Run tests

```bash
pytest tests/ -v
pytest tests/ -v --cov=src --cov-report=term-missing
```

### 6. Start the daily scheduler (production)

```bash
# Daemon mode — runs daily at 07:00 London time
python scheduler.py

# Test one immediate run without daemonising
python scheduler.py --run-now
```

---

## Project Structure

```
cost-leakage-detector/
│
├── main.py                      # CLI entry point (argparse)
├── scheduler.py                 # APScheduler daily daemon
├── config.yaml                  # All thresholds, paths, supplier config
├── .env.example                 # Secret template (copy to .env)
├── requirements.txt
├── .gitignore
├── n8n_workflow.json            # n8n: cron → pipeline → Slack
│
├── src/
│   ├── __init__.py
│   ├── data_generator.py        # 90-day synthetic procurement dataset
│   ├── detector.py              # 4-rule anomaly detection engine
│   ├── scorer.py                # Severity scoring (Low/Medium/High/Critical)
│   ├── reporter.py              # openpyxl Excel workbook (3 sheets)
│   ├── dashboard.py             # Plotly HTML dashboard (4 charts)
│   └── alerter.py               # Slack Block Kit webhook alerter
│
├── data/
│   ├── raw/                     # transactions.csv (gitignored; generated at runtime)
│   └── output/                  # Reports and dashboards (gitignored)
│
├── logs/                        # Rotating log files (gitignored)
│
└── tests/
    ├── test_detector.py         # 18 unit tests for detection rules
    └── test_scorer.py           # 14 unit tests for scoring engine
```

---

## Detection Rules

| # | Rule | Trigger Condition | Leakage Estimate |
|---|------|-------------------|-----------------|
| 1 | **Duplicate Transaction** | Same `supplier_id` + `invoice_amount` (±£1) within ±1 day | Full invoice amount |
| 2 | **Price Variance** | Invoice > `baseline_rate × 1.15` (configurable threshold) | Excess above threshold |
| 3 | **SLA Breach** | `actual_delivery_date > expected_delivery_date + grace_days` | £150/day penalty rate |
| 4 | **Volume Spike** | Daily count > 14-day rolling mean + 2σ | Flagged for review |

All thresholds are configurable in `config.yaml` — no code changes required.

---

## Severity Scoring

Each flagged transaction receives a **composite score (0–100)**:

```
composite_score = base_rule_score + financial_impact_score

base_rule_score:     duplicate=70, price_variance=60, sla_breach=45, volume_spike=40
financial_impact:    0–5 (< £500) → 5–10 (< £2k) → 10–20 (< £10k) → 20–30 (> £10k)
```

| Score | Severity | Action |
|-------|----------|--------|
| ≥ 80  | **Critical** | Immediate escalation. Freeze supplier payments. Slack alert triggered. |
| ≥ 60  | **High** | Same-day review by senior analyst. |
| ≥ 35  | **Medium** | Weekly ops review queue. Request supplier clarification. |
| < 35  | **Low** | Log for trend analysis. Review end of month. |

---

## Data Dictionary

### `data/raw/transactions.csv`

| Column | Type | Description |
|--------|------|-------------|
| `transaction_id` | string | Unique identifier (`TXN-XXXXXX`) |
| `date` | date | Transaction date (YYYY-MM-DD) |
| `supplier_id` | string | Supplier reference (`SUP001`–`SUP008`) |
| `supplier_name` | string | Full supplier legal name |
| `category` | string | Spend category (Logistics, Facilities, IT, Manufacturing, Professional Services, Procurement) |
| `baseline_rate` | float | Contracted baseline rate for this supplier (£) |
| `invoice_amount` | float | Actual invoiced amount (£) |
| `expected_delivery_date` | date | SLA-calculated expected delivery |
| `actual_delivery_date` | date | Recorded actual delivery date |
| `po_number` | string | Purchase order reference |
| `region` | string | Operational region |
| `approved_by` | string | Approving manager |
| `is_anomaly` | bool | Ground-truth anomaly flag (for validation) |
| `anomaly_type` | string | Injected anomaly type(s) (pipe-separated) |

### Scored output columns (added by `scorer.py`)

| Column | Type | Description |
|--------|------|-------------|
| `rule_triggered` | string | Detection rule that fired |
| `rule_detail` | string | Human-readable explanation |
| `leakage_amount_gbp` | float | Estimated financial leakage (£) |
| `base_score` | float | Rule-specific base score (0–70) |
| `financial_score` | float | Financial impact score (0–30) |
| `composite_score` | float | Total severity score (0–100) |
| `severity` | string | `Low` / `Medium` / `High` / `Critical` |
| `severity_rank` | int | 1 (Low) → 4 (Critical), for sorting |
| `action_required` | string | Prescriptive next-step instruction |

---

## Output Samples

### Excel Workbook (3 sheets)

**Sheet 1 — Summary**
- KPI tiles: Total Leakage, Transactions, Flags, Critical/High/Medium/Low counts
- Leakage by detection rule (table with % contribution)
- Top 5 suppliers by leakage exposure

**Sheet 2 — Flagged Items**
- Full detail for every flagged (transaction, rule) pair
- Row-level conditional formatting (red = Critical, orange = High, yellow = Medium, green = Low)
- Auto-filter enabled on all columns
- Frozen header row

**Sheet 3 — Statistics**
- Rule performance metrics: flag counts, avg/max leakage per rule, severity breakdown
- Daily leakage trend table
- Embedded bar chart: daily leakage over the analysis period

### Interactive Dashboard (HTML)

Four Plotly charts in a responsive two-column grid:

1. **Leakage by Category** — Stacked horizontal bar, split by detection rule
2. **Daily Trend** — Area chart with Critical flag count overlay (dual Y-axis)
3. **Severity Heatmap** — Rule × Severity flag density grid
4. **Top Supplier Exposure** — Stacked bar for top 8 suppliers by leakage

KPI header tiles mirror the Excel Summary sheet. Fully interactive — hover, zoom, filter.

### Slack Alert (Critical findings only)

```
🚨 CRITICAL: Cost Leakage Detected — Acme Operations Ltd

Total Estimated Leakage:  💸 £47,230.50
Critical Flags:           🔴 12  |  🟠 High: 8
Transactions Analysed:    4,127
Analysis Period:          2024-01-15 → 2024-04-14

Top Critical Findings:
• TXN-000842 | Apex Logistics Ltd | Rule: duplicate | Est. leakage: £3,400.00
• TXN-001203 | Ironside Manufacturing | Rule: price_variance | Est. leakage: £2,106.00
• TXN-002891 | BlueWave Consulting | Rule: sla_breach | Est. leakage: £2,100.00

ℹ️ Generated by Operations Cost Leakage Detector v1.0 | #ops-alerts
```

---

## n8n Workflow

Import `n8n_workflow.json` into your n8n instance for a no-code orchestration layer:

```
[Cron: Mon–Fri 07:00] → [Execute: python main.py --full-run]
                              │
              ┌───────────────┴───────────────┐
              ▼ (exit code 0)                 ▼ (exit code ≠ 0)
    [Slack: Pipeline OK]            [Slack: Pipeline FAILED]
              │
    [Check stdout for "Critical"]
              │ (yes)
    [Slack: Critical Alert]
```

Set `SLACK_WEBHOOK_URL` in your n8n environment variables and update the execute command path.

---

## Configuration Reference

All settings in `config.yaml`. Key sections:

```yaml
detection:
  duplicate_window_days: 1        # Days within which same supplier+amount = duplicate
  price_variance_threshold: 1.15  # 15% over baseline triggers flag
  sla_grace_days: 0               # Extra days before SLA breach is flagged
  volume_spike_sigma: 2.0         # σ multiplier for daily volume baseline
  volume_rolling_window: 14       # Look-back days for rolling mean/std

scoring:
  rule_base_scores:
    duplicate: 70
    price_variance: 60
    sla_breach: 45
    volume_spike: 40
  severity_bands:
    critical: 80
    high: 60
    medium: 35
    low: 0

scheduler:
  run_time: "07:00"
  timezone: "Europe/London"
  max_retries: 3
```

---

## Tech Stack

| Tool | Version | Why this tool |
|------|---------|---------------|
| **Python** | 3.11+ | Type hints, match-case, performance improvements |
| **pandas** | 2.0+ | Core data manipulation; vectorised operations for large datasets |
| **NumPy** | 1.26+ | Statistical calculations for spike detection; seeded RNG |
| **Plotly** | 5.18+ | Interactive HTML charts; no JS knowledge needed; self-contained output |
| **openpyxl** | 3.1+ | Programmatic Excel with full formatting control (no COM automation) |
| **requests** | 2.31+ | Lightweight Slack webhook POST; no Slack SDK dependency |
| **APScheduler** | 3.10+ | Production-grade scheduling with timezone support and misfire handling |
| **PyYAML** | 6.0+ | Human-readable config management; runtime-adjustable thresholds |

**Deliberately excluded:**
- `sklearn` / `scipy` — rules-based detection is explainable; black-box models aren't appropriate for financial audit tooling
- `sqlalchemy` / databases — CSV input keeps the tool portable and infra-free
- `celery` / `redis` — APScheduler is sufficient for single-node daily scheduling

---

## Scalability Roadmap

This tool is designed to graduate from internal script to enterprise platform:

### Phase 2 — Cloud Data Integration
- **Azure Data Factory** pipeline to pull from ERP/procurement APIs directly
- **Azure Blob Storage** as the raw data layer (replace `data/raw/` CSV)
- **Azure SQL Database** for historical flag storage and trend analysis

### Phase 3 — BI Platform Integration
- **Power BI Service** to replace the Plotly HTML dashboard
  - Scheduled dataset refresh via Power BI REST API
  - Row-level security for regional ops managers
- **Power BI Embedded** for portal integration

### Phase 4 — ML Enhancement
- **Isolation Forest** / **LSTM autoencoder** for unsupervised anomaly detection
  - Rules-based engine becomes the validation layer; ML provides candidate flags
- **Azure Machine Learning** managed endpoints for real-time scoring
- **Feedback loop**: analyst accept/reject decisions as labelled training data

### Phase 5 — Enterprise Governance
- **Azure Active Directory** authentication for the dashboard
- **Audit log**: every flag, every analyst action, timestamped and immutable
- **JIRA/ServiceNow integration**: auto-create tickets for Critical findings
- **Multi-tenant**: per-business-unit config, consolidated group reporting

---

## Development

### Running tests with coverage

```bash
pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html
open htmlcov/index.html
```

### Adding a new detection rule

1. Add the detection function to `src/detector.py` following the existing pattern
2. Register it in `run_detection()` and add its results to the `results` list
3. Add a base score for the new rule in `config.yaml` under `scoring.rule_base_scores`
4. Add a colour for the rule in `src/dashboard.py` under `RULE_COLOURS`
5. Write unit tests in `tests/test_detector.py`

### Code quality standards

- Every function has a docstring (Args, Returns, Raises)
- `logging` used throughout — no `print()` statements in production code
- All configurable values live in `config.yaml` — nothing hardcoded
- Errors fail loudly with meaningful messages

---

## License

MIT — see [LICENSE](LICENSE)

---

*Built to demonstrate production-grade Python automation for operations and BI teams.*
