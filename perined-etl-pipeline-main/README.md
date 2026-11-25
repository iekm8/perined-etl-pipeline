# Perined ETL pipeline – privacy-by-design data preparation

An ETL pipeline that turns (synthetic) Dutch perinatal registry data from **Perined** into a clean, privacy-friendly **DuckDB** database, orchestrated with **Prefect**.

Built by **Ismael El Khadmaoui Mahir** as part of the Applied Data Science & AI programme at Rotterdam University of Applied Sciences (2026).

> 🇳🇱 The original Dutch documentation is in [README.nl.md](README.nl.md). Code and comments are in Dutch.

## Research question

*To what extent does the mode of delivery for an abnormal fetal position differ between maternal age groups?*

The pipeline prepares the data to answer this question, among other things by creating the columns `ligging_afwijkend` (abnormal position) and `leeftijdsgroep` (age group).

## Tech stack

Python · pandas · Prefect · Parquet · DuckDB · Jupyter

## Pipeline

```
CSV ──extract──▶ raw.parquet ──transform──▶ clean.parquet ──load──▶ perined.duckdb
                 (deleted after load)
```

| Step | What it does |
|---|---|
| **Extract** ([`extract.py`](scripts/extract.py)) | Reads the CSV, checks that all 47 expected columns are present (stops otherwise), saves `raw.parquet`. Retries twice on failure. |
| **Transform** ([`transform.py`](scripts/transform.py)) | Cleans the data and applies the privacy steps (see below). No rows are deleted: invalid values become `NaN` so the rest of the record stays usable. |
| **Load** ([`load.py`](scripts/load.py)) | Loads `clean.parquet` into DuckDB, checks the row count matches, then deletes `raw.parquet`. |
| **Flow** ([`main.py`](scripts/main.py)) | Prefect flow with all settings, code lists and label mappings in one place. |

### Data quality (DAMA dimensions)

All cleaning choices come from my exploratory analysis in [`eda/eda_ismael.ipynb`](eda/eda_ismael.ipynb).

- **Completeness** – drop empty/constant columns; flag `provincie_bekend` because 37.6% of records have an unknown province.
- **Validity** – impossible values set to `NaN` (birth weight 0, parity > 15, gravidity > 20, parity ≥ gravidity, mothers under 14 with a previous birth).
- **Accuracy** – fix a typo in birth weight (3,694,000 g → 3,694 g).
- **Consistency** – flag rows that are internally inconsistent (e.g. home birth with a caesarean section).
- **Missing data** – analysed as MCAR/MAR in the EDA; only values that are certain are derived (`beginbaring_afgeleid` flag).

### Privacy by design

- **Minimise** – postcode (`pc4`) and exact age (`lft`) are removed; an allow-list makes the pipeline stop if any other column ends up in the output.
- **Abstract** – age becomes an age group; groups of province × year × age group with fewer than 5 people are suppressed (184 rows).
- **Enforce** – `raw.parquet`, which still holds the identifying columns, is deleted automatically after loading.
- **Demonstrate** – logs only show counts, never personal data.

Result: **100,000 rows × 41 columns** in DuckDB.

## How to run

```bash
python3 -m venv etl-venv
source etl-venv/bin/activate
pip install -r requirements.txt
```

Place `etl-synthetic_perined.csv` in `data/` (the data is **not** included in this repository).

```bash
# terminal 1
prefect server start --host 0.0.0.0 --port 8888
# terminal 2
cd scripts
python3 main.py
```

The Prefect UI on port 8888 shows whether extract, transform and load succeeded.

## Repository structure

```
scripts/   ETL code (main, extract, transform, load)
eda/       exploratory data analysis notebook
data/      local data folder (ignored by Git)
```
