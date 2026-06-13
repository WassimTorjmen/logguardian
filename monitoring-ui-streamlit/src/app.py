import os
import json
import time
import random
import threading
from datetime import datetime
from collections import deque

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from confluent_kafka import Consumer


# =========================
# CONFIG
# =========================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "logs-anomalies-ml")
MAX_ROWS = int(os.getenv("MAX_ROWS", "2000"))
ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "2.0"))
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "3"))

FEEDBACK_PATH = os.getenv("FEEDBACK_PATH", "rag_feedback.jsonl")

buffer = deque(maxlen=MAX_ROWS)
lock = threading.Lock()
total_received = 0


# =========================
# KAFKA CONSUMER
# =========================

def kafka_consumer():
    global total_received

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"streamlit-monitoring-ui-{random.randint(0, 999999)}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([KAFKA_TOPIC])

    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            continue

        try:
            r = json.loads(msg.value().decode("utf-8"))

            row = {
                "id": f"{r.get('detected_at', '')}_{random.randint(0, 999999)}",
                "Timestamp": r.get("detected_at", "")[:19].replace("T", " "),
                "Source": r.get("source", "unknown"),
                "Host": r.get("host", "unknown"),
                "Message": r.get("sequence", [{}])[-1].get("message", "")[:180],
                "Score IA": float(r.get("anomaly_score", 0)),
                "Ratio": float(r.get("severity_ratio", 0)),
                "Threshold": float(r.get("threshold", 0)),
                "Statut": "ANOMALIE" if float(r.get("severity_ratio", 0)) > ALERT_THRESHOLD else "NORMAL",
            }

            with lock:
                buffer.appendleft(row)
                total_received += 1

        except Exception:
            pass


@st.cache_resource
def start_kafka_thread():
    thread = threading.Thread(target=kafka_consumer, daemon=True)
    thread.start()
    return thread


start_kafka_thread()


# =========================
# PAGE CONFIG
# =========================

st.set_page_config(
    page_title="LogGuardian — AIOps Platform",
    page_icon="🛡️",
    layout="wide"
)


# =========================
# CSS
# =========================

st.markdown("""
<style>
    .stApp {
        background-color: #0b0d14;
        color: #c8d0e7;
    }

    section[data-testid="stSidebar"] {
        background-color: #0d0f1a;
        border-right: 1px solid #1e2236;
    }

    .metric-card {
        background: #111420;
        border: 1px solid #1e2236;
        border-radius: 14px;
        padding: 18px;
        height: 120px;
    }

    .metric-title {
        color: #6b7494;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }

    .metric-value {
        font-size: 30px;
        font-weight: 800;
        color: #c8d0e7;
        margin-top: 10px;
    }

    .card {
        background: #111420;
        border: 1px solid #1e2236;
        border-radius: 14px;
        padding: 18px;
        margin-bottom: 16px;
    }

    .alert-card {
        background: rgba(239, 68, 68, 0.08);
        border: 1px solid rgba(239, 68, 68, 0.3);
        border-radius: 14px;
        padding: 18px;
    }

    .rag-card {
        background: #111420;
        border: 1px solid #252a42;
        border-radius: 14px;
        padding: 18px;
    }

    .rag-title {
        color: #6b7494;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin-bottom: 8px;
    }

    .rag-box {
        background: #161929;
        border: 1px solid #252a42;
        border-left: 4px solid #6366f1;
        border-radius: 10px;
        padding: 14px;
        margin-bottom: 16px;
        font-size: 14px;
    }

    div[data-testid="stMetricValue"] {
        color: #c8d0e7;
    }

    div[data-testid="stDataFrame"] {
        border: 1px solid #1e2236;
        border-radius: 14px;
    }

    .small-muted {
        color: #6b7494;
        font-size: 12px;
    }
</style>
""", unsafe_allow_html=True)


# =========================
# DATA HELPERS
# =========================

def get_rows():
    with lock:
        rows = list(buffer)
        total = total_received
    return rows, total


def rows_to_df(rows):
    if not rows:
        return pd.DataFrame(columns=[
            "id", "Timestamp", "Source", "Host", "Message",
            "Score IA", "Ratio", "Threshold", "Statut"
        ])

    return pd.DataFrame(rows)


def filter_df(df, search, source, statut, limit):
    if df.empty:
        return df

    filtered = df.copy()

    if search:
        search = search.lower()
        filtered = filtered[
            filtered["Message"].str.lower().str.contains(search, na=False)
            | filtered["Source"].str.lower().str.contains(search, na=False)
            | filtered["Host"].str.lower().str.contains(search, na=False)
        ]

    if source != "Toutes":
        filtered = filtered[filtered["Source"] == source]

    if statut != "Tous":
        filtered = filtered[filtered["Statut"] == statut]

    return filtered.head(limit)


# =========================
# CHARTS
# =========================

def dark_layout(fig, height=320):
    fig.update_layout(
        height=height,
        paper_bgcolor="#111420",
        plot_bgcolor="#111420",
        font=dict(color="#c8d0e7"),
        margin=dict(l=30, r=20, t=40, b=30),
        xaxis=dict(gridcolor="#252a42"),
        yaxis=dict(gridcolor="#252a42"),
        legend=dict(bgcolor="#111420"),
    )
    return fig


def chart_anomaly_distribution(df):
    if df.empty:
        return go.Figure()

    counts = df["Statut"].value_counts()

    fig = go.Figure(data=[
        go.Pie(
            labels=counts.index,
            values=counts.values,
            hole=0.55,
            marker=dict(colors=["#ef4444", "#22c55e"])
        )
    ])

    fig.update_layout(title="Répartition Normal / Anomalie")
    return dark_layout(fig)


def chart_score_timeline(df):
    if df.empty:
        return go.Figure()

    recent = df.head(50).iloc[::-1]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=list(range(1, len(recent) + 1)),
        y=recent["Score IA"],
        mode="lines+markers",
        name="Score IA",
        line=dict(color="#6366f1", width=3)
    ))

    fig.add_hline(
        y=ALERT_THRESHOLD,
        line_dash="dash",
        line_color="#ef4444",
        annotation_text="Seuil"
    )

    fig.update_layout(title="Évolution du score IA — derniers logs")
    return dark_layout(fig)


def chart_top_sources(df):
    if df.empty:
        return go.Figure()

    anomaly_df = df[df["Statut"] == "ANOMALIE"]

    if anomaly_df.empty:
        return go.Figure()

    counts = anomaly_df["Source"].value_counts().head(8)

    fig = go.Figure(data=[
        go.Bar(
            x=counts.values,
            y=counts.index,
            orientation="h",
            marker=dict(color="#38bdf8")
        )
    ])

    fig.update_layout(title="Top sources touchées")
    return dark_layout(fig)


def chart_risk_gauge(df):
    if df.empty:
        risk = 0
    else:
        anomaly_rate = len(df[df["Statut"] == "ANOMALIE"]) / len(df)
        avg_ratio = df["Ratio"].mean()
        risk = min(100, int((anomaly_rate * 60) + (avg_ratio * 20)))

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=risk,
        title={"text": "Risk Score"},
        gauge={
            "axis": {"range": [0, 100]},
            "bar": {"color": "#ef4444" if risk > 70 else "#f59e0b" if risk > 40 else "#22c55e"},
            "bgcolor": "#111420",
            "borderwidth": 1,
            "bordercolor": "#252a42",
        }
    ))

    return dark_layout(fig, height=280)


# =========================
# RAG + FEEDBACK
# =========================

def generate_rag_explanation(row):
    is_anomaly = row.get("Statut") == "ANOMALIE"

    if is_anomaly:
        return {
            "description": row.get("Message", ""),
            "analysis": (
                f"Ce log présente un comportement anormal détecté par le modèle IA. "
                f"Le ratio de sévérité est de {row.get('Ratio'):.2f}, ce qui dépasse "
                f"le seuil configuré à {ALERT_THRESHOLD:.1f}. "
                f"La source concernée est {row.get('Source')} sur l’hôte {row.get('Host')}."
            ),
            "action": (
                "Analyser le service concerné, vérifier les erreurs récentes, "
                "corréler avec les autres logs et envisager une action de remédiation "
                "comme un redémarrage du pod ou une inspection Kubernetes."
            )
        }

    return {
        "description": row.get("Message", ""),
        "analysis": (
            f"Ce log est considéré comme normal. Son ratio de sévérité est de "
            f"{row.get('Ratio'):.2f}, donc inférieur au seuil critique."
        ),
        "action": "Aucune action immédiate n’est nécessaire. Continuer la surveillance."
    }


def save_feedback(row, feedback):
    record = {
        "timestamp": datetime.now().isoformat(),
        "feedback": feedback,
        "log_id": row.get("id"),
        "source": row.get("Source"),
        "host": row.get("Host"),
        "message": row.get("Message"),
        "score_ia": row.get("Score IA"),
        "ratio": row.get("Ratio"),
        "statut": row.get("Statut"),
    }

    with open(FEEDBACK_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def rag_panel(row):
    st.markdown('<div class="rag-card">', unsafe_allow_html=True)
    st.markdown("### 🤖 Explication IA / RAG")

    if row is None:
        st.info("Sélectionne un log dans le tableau pour afficher l’analyse RAG.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    explanation = generate_rag_explanation(row)

    st.markdown('<div class="rag-title">Description du log</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="rag-box">{explanation["description"]}</div>', unsafe_allow_html=True)

    st.markdown('<div class="rag-title">Analyse du modèle</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="rag-box">{explanation["analysis"]}</div>', unsafe_allow_html=True)

    st.markdown('<div class="rag-title">Action suggérée</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="rag-box">{explanation["action"]}</div>', unsafe_allow_html=True)

    st.markdown("#### Feedback utilisateur")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("👍 Utile", use_container_width=True):
            save_feedback(row, "positive")
            st.success("Feedback positif enregistré.")

    with col2:
        if st.button("👎 Pas utile", use_container_width=True):
            save_feedback(row, "negative")
            st.warning("Feedback négatif enregistré.")

    st.caption("Ces retours peuvent être utilisés pour l’évaluation du RAG ou l’amélioration du modèle.")

    st.markdown("</div>", unsafe_allow_html=True)


# =========================
# SIDEBAR
# =========================

with st.sidebar:
    st.markdown("## 🛡️ LogGuardian")
    st.caption("AIOps Platform")

    page = st.radio(
        "Navigation",
        ["Dashboard", "Historique des logs", "Alertes"],
        label_visibility="collapsed"
    )

    st.divider()

    st.markdown("### ⚙️ Kafka")
    st.caption(f"Broker : `{KAFKA_BOOTSTRAP_SERVERS}`")
    st.caption(f"Topic : `{KAFKA_TOPIC}`")
    st.caption(f"Refresh : {REFRESH_SECONDS}s")

    st.divider()

    auto_refresh = st.toggle("Auto-refresh", value=True)


if auto_refresh:
    time.sleep(0.2)
    st.rerun() if False else None


# =========================
# MAIN DATA
# =========================

rows, total = get_rows()
df = rows_to_df(rows)


# =========================
# DASHBOARD PAGE
# =========================

if page == "Dashboard":
    st.title("📊 Dashboard")
    st.caption("Vue globale temps réel des anomalies détectées par le modèle IA")

    total_logs = total
    anomalies = len(df[df["Statut"] == "ANOMALIE"]) if not df.empty else 0
    avg_score = df["Score IA"].mean() if not df.empty else 0
    sources = df["Source"].nunique() if not df.empty else 0
    risk = round((anomalies / len(df) * 100), 1) if not df.empty else 0

    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric("Logs reçus", total_logs)
    c2.metric("Anomalies", anomalies)
    c3.metric("Score IA moyen", f"{avg_score:.2f}")
    c4.metric("Sources", sources)
    c5.metric("Taux anomalie", f"{risk}%")

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.plotly_chart(
            chart_score_timeline(df),
            use_container_width=True,
            key="score_timeline"
    )

    with col2:
        st.plotly_chart(
            chart_anomaly_distribution(df),
            use_container_width=True,
            key="anomaly_distribution"
)

    col3, col4 = st.columns([2, 1])

    with col3:
        st.plotly_chart(
            chart_top_sources(df),
            use_container_width=True,
            key="top_sources"
)

    with col4:
        st.plotly_chart(
            chart_risk_gauge(df),
            use_container_width=True,
            key="risk_gauge"
)

    st.markdown("### Dernières alertes critiques")

    alert_preview = df[df["Statut"] == "ANOMALIE"].head(10)

    if alert_preview.empty:
        st.info("Aucune alerte critique pour le moment.")
    else:
        st.dataframe(
            alert_preview[["Timestamp", "Source", "Host", "Message", "Score IA", "Ratio"]],
            use_container_width=True,
            hide_index=True
        )


# =========================
# HISTORIQUE PAGE
# =========================

elif page == "Historique des logs":
    st.title("📜 Historique des logs")
    st.caption("Exploration temps réel des logs analysés")

    f1, f2, f3, f4 = st.columns([3, 1, 1, 1])

    with f1:
        search = st.text_input("Recherche", placeholder="timeout, ssh, error, injection...")

    with f2:
        sources = ["Toutes"] + sorted(df["Source"].dropna().unique().tolist()) if not df.empty else ["Toutes"]
        source_filter = st.selectbox("Source", sources)

    with f3:
        statut_filter = st.selectbox("Statut", ["Tous", "ANOMALIE", "NORMAL"])

    with f4:
        limit = st.selectbox("Lignes", [100, 200, 500, 1000, MAX_ROWS], index=1)

    filtered = filter_df(df, search, source_filter, statut_filter, limit)

    col_table, col_rag = st.columns([2.4, 1])

    with col_table:
        st.markdown("### Logs")

        if filtered.empty:
            st.info("Aucun log disponible pour le moment.")
        else:
            view_df = filtered[[
                "Timestamp", "Source", "Host", "Message",
                "Score IA", "Ratio", "Statut"
            ]].copy()

            selected = st.dataframe(
                view_df,
                use_container_width=True,
                hide_index=True,
                selection_mode="single-row",
                on_select="rerun"
            )

            selected_row = None

            if selected and selected.selection.rows:
                selected_index = selected.selection.rows[0]
                selected_row = filtered.iloc[selected_index].to_dict()
                st.session_state["selected_log"] = selected_row

    with col_rag:
        selected_row = st.session_state.get("selected_log", None)
        rag_panel(selected_row)


# =========================
# ALERTS PAGE
# =========================

elif page == "Alertes":
    st.title("🚨 Alertes critiques")
    st.caption(f"Logs avec ratio de sévérité > {ALERT_THRESHOLD}")

    alerts = df[df["Ratio"] > ALERT_THRESHOLD] if not df.empty else pd.DataFrame()

    if alerts.empty:
        st.success("Aucune alerte critique active.")
    else:
        st.markdown(f"""
        <div class="alert-card">
            <h3>🚨 {len(alerts)} alerte(s) critique(s)</h3>
            <p>Dernier événement : {alerts.iloc[0]["Timestamp"]}</p>
        </div>
        """, unsafe_allow_html=True)

        st.divider()

        st.dataframe(
            alerts[["Timestamp", "Source", "Host", "Message", "Score IA", "Ratio", "Statut"]],
            use_container_width=True,
            hide_index=True
        )


# =========================
# AUTO REFRESH
# =========================

if auto_refresh:
    time.sleep(REFRESH_SECONDS)
    st.rerun()