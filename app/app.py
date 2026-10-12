"""SOC Alert Triage Workbench — Databricks App (Streamlit).

Operational path (Lakebase Postgres, when the `lakebase` resource is attached):
  - reads the prioritized alert queue from app.my_alert_queue (synced from Unity Catalog,
    filtered to this app's assigned customers via current_user)
  - writes analyst triage decisions to app.analyst_actions (OLTP)

Fallback path (Lakebase not configured or unreachable): reads the row-filtered gold tables and
writes analyst_actions through the SQL warehouse.

Auth: the app's service principal (injected by Databricks Apps). No PATs.
"""
import os
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

st.set_page_config(page_title="SOC Triage Workbench", page_icon="🛡️", layout="wide")

CATALOG = os.getenv("SOC_CATALOG", "rajarora_febar")
SCHEMA = os.getenv("SOC_SCHEMA", "soc_alert_triage")
FQ = f"{CATALOG}.{SCHEMA}"
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
LAKEBASE_ENDPOINT = os.getenv("LAKEBASE_ENDPOINT")  # projects/<p>/branches/<b>/endpoints/<e>
TOKEN_TTL_SECONDS = 45 * 60  # Lakebase OAuth credentials last 1h; reconnect before expiry

cfg = Config()  # picks up DATABRICKS_HOST / CLIENT_ID / CLIENT_SECRET


# ---------------------------------------------------------------- Lakebase (Postgres)
@st.cache_resource(ttl=TOKEN_TTL_SECONDS)
def get_pg_conn():
    import psycopg

    w = WorkspaceClient(config=cfg)
    host = os.getenv("PGHOST") or w.api_client.do(
        "GET", f"/api/2.0/postgres/{LAKEBASE_ENDPOINT}")["status"]["hosts"]["host"]
    token = w.api_client.do(
        "POST", "/api/2.0/postgres/credentials", body={"endpoint": LAKEBASE_ENDPOINT})["token"]
    return psycopg.connect(
        host=host,
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "soc_triage"),
        user=os.getenv("PGUSER", cfg.client_id),
        password=token,
        sslmode=os.getenv("PGSSLMODE", "require"),
        autocommit=True,
    )


def pg_query(query: str, params: dict | None = None) -> pd.DataFrame:
    conn = get_pg_conn()
    if conn.closed:
        get_pg_conn.clear()
        conn = get_pg_conn()
    cur = conn.execute(query, params or {})
    if cur.description is None:  # INSERT/UPDATE etc. return no result set
        return pd.DataFrame()
    return pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])


# ---------------------------------------------------------------- SQL warehouse (fallback)
@st.cache_resource
def get_sql_conn():
    return dbsql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,  # a callable, not a string
    )


def sql_query(query: str, params: dict | None = None) -> pd.DataFrame:
    with get_sql_conn().cursor() as cur:
        cur.execute(query, params or {})
        return cur.fetchall_arrow().to_pandas() if cur.description else pd.DataFrame()


@st.cache_resource(ttl=300, show_spinner=False)
def lakebase_error() -> str | None:
    """None if Lakebase is configured and reachable, otherwise why not."""
    if not LAKEBASE_ENDPOINT:
        return "LAKEBASE_ENDPOINT is not set"
    try:
        pg_query("SELECT 1 FROM app.my_alert_queue LIMIT 1")
        return None
    except Exception as e:  # noqa: BLE001 — any failure means use the fallback
        get_pg_conn.clear()
        return str(e)


LAKEBASE_ERROR = lakebase_error()
USE_LAKEBASE = LAKEBASE_ERROR is None


# ---------------------------------------------------------------- data access
QUEUE_COLUMNS = """event_id, event_timestamp, customer_name, source_type, risk_tier,
                   composite_risk_score, threat_intel_match, mitre_tactic, alert_type, src_ip,
                   username, ml_predicted_severity, ml_confidence,
                   investigation_summary IS NOT NULL AS has_ai_summary"""


@st.cache_data(ttl=60, show_spinner=False)
def load_queue(tiers: tuple, sources: tuple, limit: int) -> tuple[pd.DataFrame, float]:
    t0 = time.perf_counter()
    if USE_LAKEBASE:
        df = pg_query(
            f"""SELECT {QUEUE_COLUMNS} FROM app.my_alert_queue
                WHERE risk_tier = ANY(%(tiers)s) AND source_type = ANY(%(sources)s)
                ORDER BY has_ai_summary DESC, composite_risk_score DESC, event_id
                LIMIT %(limit)s""",
            {"tiers": list(tiers), "sources": list(sources), "limit": limit},
        )
    else:
        # gold_enriched_alerts carries the UC row filter, so the app SP only sees assigned customers
        df = sql_query(
            f"""SELECT {QUEUE_COLUMNS} FROM (
                    SELECT e.*, p.ml_predicted_severity, p.ml_confidence, s.investigation_summary
                    FROM {FQ}.gold_enriched_alerts e
                    JOIN {FQ}.gold_alert_predictions p ON e.event_id = p.event_id
                    LEFT JOIN {FQ}.gold_investigation_summaries s ON e.event_id = s.event_id
                    WHERE e.risk_tier IN ('critical', 'high') OR p.ml_predicted_severity IN ('critical', 'high'))
                WHERE array_contains(:tiers, risk_tier) AND array_contains(:sources, source_type)
                ORDER BY has_ai_summary DESC, composite_risk_score DESC, event_id
                LIMIT :limit""",
            {"tiers": list(tiers), "sources": list(sources), "limit": limit},
        )
    return df, (time.perf_counter() - t0) * 1000


def load_summary(event_id: str) -> str | None:
    if USE_LAKEBASE:
        df = pg_query("SELECT investigation_summary FROM app.my_alert_queue WHERE event_id = %(eid)s",
                      {"eid": event_id})
    else:
        df = sql_query(f"""SELECT s.investigation_summary
                             FROM {FQ}.gold_enriched_alerts e
                             JOIN {FQ}.gold_investigation_summaries s ON e.event_id = s.event_id
                             WHERE e.event_id = :eid""", {"eid": event_id})
    return None if df.empty else df.iloc[0, 0]


def load_recent_actions() -> pd.DataFrame:
    if USE_LAKEBASE:
        return pg_query("""SELECT action_timestamp, analyst_id, event_id, action_type, severity_override, notes
                           FROM app.analyst_actions ORDER BY action_timestamp DESC LIMIT 20""")
    return sql_query(f"""SELECT action_timestamp, analyst_id, event_id, action_type, severity_override, notes
                         FROM {FQ}.analyst_actions ORDER BY action_timestamp DESC LIMIT 20""")


def record_action(row: dict) -> None:
    if USE_LAKEBASE:
        pg_query(
            """INSERT INTO app.analyst_actions (action_id, event_id, analyst_id, action_type,
                   severity_override, notes, time_to_triage_seconds)
               VALUES (%(action_id)s, %(event_id)s, %(analyst_id)s, %(action_type)s,
                       %(severity_override)s, %(notes)s, %(time_to_triage_seconds)s)""",
            row,
        )
    else:
        sql_query(
            f"""INSERT INTO {FQ}.analyst_actions VALUES (
                   :action_id, :event_id, :analyst_id, :action_type, :severity_override,
                   :notes, :action_timestamp, :time_to_triage_seconds)""",
            row,
        )


# ---------------------------------------------------------------- UI
st.title("🛡️ SOC Alert Triage Workbench")
backend = "Lakebase (Postgres)" if USE_LAKEBASE else f"SQL warehouse · `{FQ}`"

if not USE_LAKEBASE:
    if not WAREHOUSE_ID:
        st.error(f"No backend available — Lakebase: {LAKEBASE_ERROR}; no `sql-warehouse` resource attached.")
        st.stop()
    if LAKEBASE_ENDPOINT:
        st.warning(f"Lakebase unavailable, using the SQL warehouse fallback. Reason: {LAKEBASE_ERROR}")

with st.sidebar:
    st.header("Filters")
    tiers = st.multiselect("Risk tier", ["critical", "high", "medium", "low"], ["critical", "high"])
    sources = st.multiselect(
        "Source", ["firewall", "edr", "auth", "email", "cloud_audit"],
        ["firewall", "edr", "auth", "email", "cloud_audit"],
    )
    limit = st.slider("Queue size", 25, 500, 100, step=25)
    analyst_id = st.text_input("Analyst ID", st.context.headers.get("X-Forwarded-Email", "analyst_1"))

if not tiers or not sources:
    st.info("Select at least one risk tier and source.")
    st.stop()

try:
    queue, latency_ms = load_queue(tuple(tiers), tuple(sources), limit)
except Exception as e:
    st.error(f"Query failed: {e}")
    st.stop()

st.caption(f"Backend: {backend} · queue query {latency_ms:.0f} ms")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Alerts in queue", f"{len(queue):,}")
c2.metric("Critical", int((queue.risk_tier == "critical").sum()))
c3.metric("Threat-intel matches", int(queue.threat_intel_match.sum()))
c4.metric("With AI summary", int(queue.has_ai_summary.sum()))

tab_queue, tab_triage, tab_log = st.tabs(["📋 Alert queue", "🔎 Investigate & triage", "📝 Recent actions"])

with tab_queue:
    st.dataframe(queue, use_container_width=True, hide_index=True)

with tab_log:
    try:
        st.dataframe(load_recent_actions(), use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Could not load actions: {e}")

with tab_triage:
    if queue.empty:
        st.info("No alerts match the filters.")
        st.stop()
    indexed = queue.set_index("event_id")
    event_id = st.selectbox(
        "Alert",
        list(queue.event_id),
        key="selected_alert",  # stable widget identity so the selection survives reruns
        format_func=lambda eid: (
            f"{indexed.at[eid, 'risk_tier'].upper()} · {indexed.at[eid, 'composite_risk_score']} · "
            f"{indexed.at[eid, 'customer_name']} · {indexed.at[eid, 'source_type']} · "
            f"{str(indexed.at[eid, 'event_timestamp'])[:16]} · {eid[:8]}"
        ),
    )
    alert = indexed.loc[event_id]
    # Handling time = from when this analyst opened the alert to when they submit a decision
    opened = st.session_state.setdefault("opened_at", {})
    opened.setdefault(event_id, datetime.now(timezone.utc))

    left, right = st.columns([1, 1])
    with left:
        st.subheader("Alert context")
        st.json({k: (None if pd.isna(v) else str(v)) for k, v in alert.items()})
    with right:
        st.subheader("🧠 AI investigation summary")
        summary = load_summary(event_id)
        if not summary:
            st.info("No AI summary generated for this alert (summaries exist for top high/critical alerts).")
        else:
            st.markdown(summary)

    st.subheader("Triage decision")
    with st.form("triage"):
        action_type = st.selectbox(
            "Action", ["escalate", "investigate", "suppress", "false_positive", "resolve"]
        )
        override = st.selectbox(
            "Severity override", ["(keep ML prediction)", "critical", "high", "medium", "low"]
        )
        notes = st.text_area("Notes")
        if st.form_submit_button("Submit", type="primary"):
            now = datetime.now(timezone.utc)
            row = {
                "action_id": str(uuid.uuid4()),
                "event_id": event_id,
                "analyst_id": analyst_id,
                "action_type": action_type,
                "severity_override": None if override.startswith("(") else override,
                "notes": notes or None,
                "action_timestamp": now.strftime("%Y-%m-%dT%H:%M:%S"),
                "time_to_triage_seconds": max(0, int((now - opened[event_id]).total_seconds())),
            }
            try:
                t0 = time.perf_counter()
                record_action(row)
                st.success(f"Recorded `{action_type}` for {event_id} in {(time.perf_counter() - t0) * 1000:.0f} ms")
            except Exception as e:
                st.error(f"Write failed: {e}")
