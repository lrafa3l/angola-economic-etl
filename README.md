# 🇦🇴 Angola Economic ETL Pipeline

A production-style ETL pipeline that **extracts** macroeconomic data for Angola
from the World Bank Open Data API, **transforms** it with Pandas, and **loads**
it incrementally into a SQLite data warehouse — complete with analytical SQL
views and automated data quality checks.

Built as a portfolio project to demonstrate hands-on Data Engineering skills:
ETL design, SQL modelling, Python automation, and data governance.

---

## 📐 Architecture

```
World Bank API
     │
     ▼
┌──────────────┐    raw dict[indicator → DataFrame]
│  Extractor   │────────────────────────────────────┐
│  (requests)  │                                    │
└──────────────┘                                    ▼
                                           ┌──────────────────┐
                                           │   Transformer    │
                                           │   (Pandas)       │
                                           │  - Type casting  │
                                           │  - Dedup         │
                                           │  - Anomaly flags │
                                           └───────┬──────────┘
                                                   │ clean DataFrame
                                                   ▼
                                          ┌─────────────────────┐
                                          │      Loader         │
                                          │   (SQLite + SQL)    │
                                          │  - Incremental      │
                                          │  - Upsert logic     │
                                          │  - Views & indexes  │
                                          └────────┬────────────┘
                                                   │
                                                   ▼
                                          ┌─────────────────────┐
                                          │   Quality Checker   │
                                          │  - Completeness     │
                                          │  - Freshness        │
                                          │  - Anomalies        │
                                          │  - Gap detection    │
                                          └─────────────────────┘
```

---

## 📂 Project Structure

```
angola-economic-etl/
├── src/
│   ├── __init__.py
│   ├── pipeline.py      ← Orchestrator (entry point)
│   ├── extractor.py     ← World Bank API client (retry, pagination)
│   ├── transformer.py   ← Pandas cleaning, normalisation, anomaly detection
│   ├── loader.py        ← SQLite upsert, schema DDL, SQL views
│   └── quality.py       ← Automated data quality checks + JSON report
├── sql/
│   └── analytical_queries.sql  ← Business queries (GDP, inflation, FDI…)
├── data/                ← Created at runtime
│   ├── angola_economics.db
│   ├── logs/
│   └── reports/
├── tests/
│   └── test_transformer.py
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start

### 1. Clone & install dependencies

```bash
git clone https://github.com/landorafael/angola-economic-etl.git
cd angola-economic-etl
pip install -r requirements.txt
```

### 2. Run the full pipeline

```bash
python -m src.pipeline
```

### 3. Run with custom indicators

```bash
python -m src.pipeline --indicators NY.GDP.MKTP.CD FP.CPI.TOTL.ZG BX.KLT.DINV.CD.WD
```

### 4. Query the database

```bash
sqlite3 data/angola_economics.db
```

```sql
-- Latest values for all indicators
SELECT * FROM v_latest_values;

-- GDP growth year-on-year
SELECT year, ROUND(value/1e9,2) AS gdp_bn_usd, yoy_pct_change
FROM v_year_on_year_growth
WHERE indicator_code = 'NY.GDP.MKTP.CD';

-- Data quality summary
SELECT * FROM v_indicator_summary;
```

---

## 📊 Indicators Tracked

| Code | Description |
|------|-------------|
| `NY.GDP.MKTP.CD` | GDP (current USD) |
| `FP.CPI.TOTL.ZG` | Inflation rate (%) |
| `BN.CAB.XOKA.CD` | Current account balance |
| `BX.KLT.DINV.CD.WD` | Foreign direct investment |
| `SL.UEM.TOTL.ZS` | Unemployment rate (%) |
| `NY.GDP.PCAP.CD` | GDP per capita |
| `NE.EXP.GNFS.ZS` | Exports (% of GDP) |
| `NE.IMP.GNFS.ZS` | Imports (% of GDP) |

---

## 🗄️ Database Schema

### Table: `economic_indicators`

| Column | Type | Description |
|--------|------|-------------|
| `country_code` | TEXT | ISO3 country code (e.g. `AGO`) |
| `country_name` | TEXT | Full country name |
| `indicator_code` | TEXT | World Bank indicator code |
| `indicator_label` | TEXT | Human-readable label |
| `year` | INTEGER | Reference year |
| `value` | REAL | Indicator value |
| `unit` | TEXT | Unit of measurement |
| `obs_status` | TEXT | Observation status flag |
| `is_anomaly` | INTEGER | 1 if value exceeds expected bounds |
| `loaded_at` | TEXT | UTC timestamp of load |
| `source` | TEXT | Data source label |

### Views

| View | Description |
|------|-------------|
| `v_latest_values` | Most recent non-null value per indicator |
| `v_year_on_year_growth` | YoY % change for all indicators |
| `v_anomalies` | All flagged anomalous rows |
| `v_indicator_summary` | Aggregated stats per indicator |

---

## ✅ Data Quality Checks

The `DataQualityChecker` runs 6 automated checks after every load:

| Check | What it validates |
|-------|------------------|
| Row count | Database is not empty |
| Completeness | ≥80% non-null values per indicator |
| Freshness | Data is not more than 3 years stale |
| Anomalies | Values within expected statistical bounds |
| Duplicates | No duplicate (country, indicator, year) keys |
| Year gaps | No gaps > 2 consecutive years |

Reports are saved as JSON in `data/reports/`.

---

## 🧪 Running Tests

```bash
python -m pytest tests/ -v
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11+ |
| Data manipulation | Pandas |
| HTTP client | Requests (with retry logic) |
| Database | SQLite (schema mirrors SQL Server patterns) |
| SQL | DDL, DML, Views, Indexes |
| Logging | Python `logging` (file + stdout) |
| Testing | pytest |

> **Note on SQL Server:** The SQL views and upsert logic in this project are
> designed to be portable to **Microsoft SQL Server** (the target stack at
> Selenium). SQLite is used here for zero-dependency portability; the same
> patterns apply directly in SSMS with minor syntax adjustments.

---

## 📄 Data Source

[World Bank Open Data](https://data.worldbank.org/) — free, public API.
No API key required.

---

## 👤 Author

**Lando Rafael** — [linkedin.com/in/landorafael](https://linkedin.com/in/landorafael)
