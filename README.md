# SOC Alert Triage on Databricks

**FEBAR (Field Engineering Build-A-Rama) — Cybersecurity / Managed Detection & Response**

An end-to-end Databricks build that helps an MDR provider's SOC analysts cut through alert fatigue:
raw security telemetry is ingested incrementally, governed in Unity Catalog, scored by an ML severity
model, summarized by an LLM, served at low latency to an analyst workbench app through Lakebase, and
made queryable in natural language with Genie and in an AI/BI dashboard.

> All data is **synthetic** (500,000 events, 100 customers, September 2026). No customer data is used.

---

## The problem

SOC analysts at MDR providers triage thousands of alerts per day across many customers. Most are false
positives (≈70% false-positive/benign in this dataset's triage history), which drives alert fatigue, slow
time-to-respond, and inconsistent triage quality.

## The solution

```
Daily JSON telemetry files ──▶ UC Volume (landing)
        │  Lakeflow Job "soc-triage-pipeline" (file-arrival trigger)
        ▼
 02a  Auto Loader ───────────▶ raw_security_events (bronze)
 02b  Spark transforms ──────▶ silver_normalized_events ─▶ gold_enriched_alerts (threat intel, assets, risk score)
 03   Unity Catalog ─────────▶ comments, column tags, row filter (analyst ↔ customer assignments), lineage
 04   MLflow + XGBoost ──────▶ alert_severity_classifier (UC model, @production) ─▶ gold_alert_predictions
 05   ai_query (Llama 3.3) ──▶ gold_investigation_summaries
 06   Lakebase (Postgres) ───▶ synced alert queue + app.analyst_actions (OLTP) ─▶ merged back to Delta
 07   DBSQL / AI/BI ─────────▶ "SOC Alert Triage — Manager Overview" dashboard
 08   Genie ─────────────────▶ "SOC Alert Triage" space for SOC managers
 app  Databricks App ────────▶ Streamlit analyst workbench (reads/writes Lakebase)
```

## How each required technology is used

| # | Requirement | Where | What it does |
|---|---|---|---|
| 1 | **Lakeflow** | `notebooks/02a_autoloader_ingest.ipynb`, `jobs/soc_triage_pipeline.json` | **Auto Loader** incrementally ingests daily JSON files from a UC Volume into bronze (explicit schema, `_rescued_data`, `source_file` lineage, exactly-once via checkpoint). A **Lakeflow Job** orchestrates all 6 stages with a file-arrival trigger, queueing and retries. |
| 2 | **Unity Catalog** | `notebooks/03_governance.ipynb` | Table comments, column classification tags (`confidential`/`restricted`/`internal`), a **row filter** driven by an `analyst_customer_assignments` table (SOC managers see all; analysts see assigned customers), model registry, volumes, lineage. |
| 3 | **Lakebase** | `notebooks/06_lakebase.ipynb`, `app/app.py` | Lakebase Autoscaling project `soc-triage`. **Synced tables** push the prioritized alert queue (≈147K critical/high alerts) and assignments into Postgres. The app reads through `app.my_alert_queue` (a view filtered by `current_user`) and writes triage decisions to `app.analyst_actions`, which are merged back to Delta for analytics. |
| 4 | **ML + Gen AI** | `notebooks/04_ml_classifier.ipynb`, `notebooks/05_genai_summaries.ipynb` | XGBoost severity classifier trained on 50K historical triage decisions, tracked in MLflow, registered in UC with a `production` alias, scoring all 500K alerts. `ai_query()` with `databricks-meta-llama-3-3-70b-instruct` writes investigation summaries for top alerts. |
| 5 | **Genie** | `notebooks/08_genie_agent_setup.ipynb`, `genie/soc_alert_triage_space.json` | Genie space over 6 gold/reference tables with domain instructions, 8 example SQL queries, sample questions and benchmarks. Notebook 08 calls the Genie Conversation API and records the answers. |
| 6 | **Databricks App** | `app/` | Streamlit analyst workbench: prioritized queue, alert context + AI summary, triage form. Authenticates as the app's **service principal** (no tokens); resources: SQL warehouse + Lakebase. |
| 7 | **DBSQL** | `notebooks/07_dashboard_queries.ipynb`, `dashboards/soc_alert_triage.lvdash.json` | Analytical queries on a serverless SQL warehouse and a published AI/BI dashboard (2 pages, 19 widgets, source/date filters). |

## Results (from committed notebook outputs)

| Metric | Value | Source |
|---|---|---|
| Events ingested by Auto Loader | 500,000 from 30 files; immediate re-run ingests **0** new files | `02a` |
| ML model, held-out test set (10,000 decisions) | **accuracy 0.672, macro-F1 0.678** vs severity-only rule baseline 0.439 / 0.442 | `04` |
| Real threats the model ranks critical/high | **89.5%** (22,399 of 25,017 ground-truth true positives) | `04`, `08` |
| Alerts the model ranks low/informational | **47.4%** — candidates for batch review or auto-suppression | `04` |
| Lakebase read latency (50 runs from a notebook) | queue query p50 27 ms / p95 41 ms; point lookup p50 2.7 ms / p95 3.1 ms | `06` |
| Alert queue synced to Lakebase | 147,317 critical/high alerts; app SP scoped to 10 of 100 customers | `06` |

Numbers are from the committed notebook outputs; they shift slightly on each full regeneration (new random data).

## Repository layout

```
notebooks/   01_config … 08_genie_agent_setup   (.ipynb with outputs committed; run in order)
app/         Databricks App (app.py, app.yaml, requirements.txt)
jobs/        Lakeflow Job definition (soc_triage_pipeline.json)
genie/       Genie space definition (serialized_space export)
dashboards/  AI/BI dashboard definition (.lvdash.json)
```

## How to run it

**Prerequisites:** a Unity Catalog catalog you can create schemas in (set `CATALOG_NAME` in `01_config`),
serverless compute and a SQL warehouse, Foundation Model APIs, and Lakebase.

1. **Notebooks** — import `notebooks/` to a workspace folder and run in order on serverless, **one at a time**
   (each notebook depends on the previous one's tables):
   `02_setup_and_generate_data` → `02a_autoloader_ingest` → `02b_silver_gold_transform` → `03_governance`
   → `04_ml_classifier` → `05_genai_summaries` → `06_lakebase` → `07_dashboard_queries` → `08_genie_agent_setup`.
   Notebook 02 lands the raw files; everything else derives from them.
2. **Lakebase (one-time)** — create an Autoscaling project `soc-triage` (branch `production`, endpoint `primary`)
   with database `soc_triage`, and add the app's service principal as a Postgres role. Notebook 06 creates the
   synced tables, the Postgres objects and grants.
3. **Lakeflow Job** — `databricks jobs create --json @jobs/soc_triage_pipeline.json` (update notebook paths).
   Unpause the file-arrival trigger to run the pipeline whenever new files land.
4. **App** — create a Databricks App, attach resources `sql-warehouse` (Can use) and `lakebase`
   (Postgres database, Can connect and create), grant its service principal `USE CATALOG` / `USE SCHEMA` /
   `SELECT` on the schema, then:
   ```bash
   databricks sync ./app /Workspace/Users/<you>/soc-triage-app
   databricks apps deploy soc-triage --source-code-path /Workspace/Users/<you>/soc-triage-app
   ```
5. **Genie / Dashboard** — create from `genie/soc_alert_triage_space.json` (`POST /api/2.0/genie/spaces`)
   and `dashboards/soc_alert_triage.lvdash.json` (import in the Dashboards UI or `POST /api/2.0/lakeview/dashboards`).

## Security notes

- **No credentials in this repo.** The app authenticates as its Databricks service principal
  (`DATABRICKS_CLIENT_ID`/`SECRET` are injected by Databricks Apps); Lakebase uses short-lived OAuth
  credentials generated at runtime.
- **Row-level security** is enforced in Unity Catalog by `rls_customer_filter`. Synced tables in Lakebase are
  plain Postgres tables (UC filters do not apply there), so the same assignments are enforced in Postgres by the
  `app.my_alert_queue` view keyed on `current_user`; the app's role is granted only that view.
  If Lakebase is unreachable, the app falls back to the SQL warehouse and reads through the row-filtered
  `gold_enriched_alerts` table, so the same customer scoping applies on both paths.
- **Scope of the row filter:** it is applied to `gold_enriched_alerts`. Derived tables (`gold_alert_predictions`,
  `gold_investigation_summaries`, `analyst_actions`) and the `gold_alert_queue` serving copy are not filtered;
  the Genie space and dashboard are therefore positioned for SOC managers. Extending per-analyst scoping to
  them would mean adding `customer_id` and the same filter function to each.

## Honest caveats

- **Synthetic labels.** Historical analyst severities are generated from a latent policy (signals + noise), so the
  model's metrics show it recovers that policy — they are not a real-world accuracy claim. `is_true_positive`
  is likewise a synthetic ground-truth flag.
- **Silver/gold are batch rebuilds** (overwrite) after incremental bronze ingestion — simple and correct at
  this scale; a production version would use Lakeflow Declarative Pipelines or `MERGE` for incremental silver/gold.
- **Lakebase → Delta** analyst actions are merged by notebook 06 rather than queried live, because registering a
  Lakebase catalog requires `CREATE CATALOG`, which the build workspace did not grant.
