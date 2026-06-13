"""
LogGuardian — AIOps Command Center UI
Radical Dash redesign: static pages, stable callbacks, logs table, RAG explanation + recommendation + feedback.
"""
import json
import logging
import os
import random
import threading
from collections import Counter, deque
from datetime import datetime

import dash
from dash import dash_table, dcc, html, callback_context
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import plotly.graph_objects as go
from confluent_kafka import Consumer

import smtplib
from email.mime.text import MIMEText

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("logguardian-ui")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "logs-anomalies-ml")
MAX_ROWS = int(os.getenv("MAX_ROWS", "2000"))
REFRESH_INTERVAL_MS = int(os.getenv("REFRESH_INTERVAL_MS", "3000"))
ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "2.0"))
FEEDBACK_PATH = os.getenv("FEEDBACK_PATH", "/app/feedback/rag_feedback.jsonl")

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
MAIL_TO = os.getenv("MAIL_TO", "")
EMAIL_ALERTS_ENABLED = os.getenv("EMAIL_ALERTS_ENABLED", "false").lower() == "true"

_buffer: deque = deque(maxlen=MAX_ROWS)
_lock = threading.Lock()
_total_received = 0
_email_cache = set()
_email_lock = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# THEME — light, clean, monitoring grid inspired by Postgres/Grafana panels
# ─────────────────────────────────────────────────────────────────────────────
T = {
    "bg": "#f3f6fb",
    "paper": "#ffffff",
    "paper2": "#f8fafc",
    "sidebar": "#0f172a",
    "sidebar2": "#111c33",
    "border": "#d8e0ec",
    "border2": "#edf1f7",
    "text": "#111827",
    "muted": "#667085",
    "muted2": "#94a3b8",
    "blue": "#2563eb",
    "cyan": "#0891b2",
    "green": "#16a34a",
    "red": "#dc2626",
    "orange": "#f97316",
    "purple": "#7c3aed",
    "yellow": "#eab308",
    "black": "#020617",
}

TABLE_COLS = ["Timestamp", "Source", "Host", "Message", "Score IA", "Ratio", "Statut"]
ALERT_COLS = ["Timestamp", "Source", "Host", "Message", "Score IA", "Ratio", "Model"]

# ─────────────────────────────────────────────────────────────────────────────
# KAFKA THREAD
# ─────────────────────────────────────────────────────────────────────────────
def _safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def _extract_message(r):
    seq = r.get("sequence") or []
    if isinstance(seq, list) and seq:
        last = seq[-1]
        if isinstance(last, dict):
            return str(last.get("message", ""))[:240]
        return str(last)[:240]
    return str(r.get("message", ""))[:240]


def _build_row(r):
    detected = str(r.get("detected_at", ""))
    score = _safe_float(r.get("anomaly_score", 0))
    ratio = _safe_float(r.get("severity_ratio", 0))
    threshold = _safe_float(r.get("threshold", ALERT_THRESHOLD))
    status = "ANOMALIE" if ratio > ALERT_THRESHOLD else "NORMAL"
    return {
        "id": f"{detected}_{random.randint(0, 999999)}",
        "Timestamp": detected[:19].replace("T", " "),
        "Source": str(r.get("source", "unknown")),
        "Host": str(r.get("host", "unknown")),
        "Message": _extract_message(r),
        "Score IA": f"{score:.2f}",
        "Ratio": f"{ratio:.2f}x",
        "Model": str(r.get("model_version", "lstm_v1")),
        "Statut": status,
        "_score_val": score,
        "_ratio_val": ratio,
        "_threshold_val": threshold,
        "_raw": r,
    }



def _parse_security_context(row):
    import re
    message = row.get("Message", "")
    ip_match = re.search(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", message)
    user_match = re.search(r"user=([a-zA-Z0-9_.-]+)", message)
    return {
        "ip": ip_match.group(0) if ip_match else "non détectée",
        "user": user_match.group(1) if user_match else "non détecté",
    }


def _email_recommendation(row):
    msg = row.get("Message", "").lower()
    if "authentication" in msg or "ssh" in msg or "kerberos" in msg:
        return "Vérifier les authentifications récentes, identifier l'IP source et bloquer l'accès si nécessaire."
    if "timeout" in msg or "connection" in msg:
        return "Vérifier la connectivité réseau, la disponibilité du service et les timeouts applicatifs."
    if "error" in msg or "failed" in msg:
        return "Inspecter le service concerné, corréler avec les logs voisins et redémarrer si l'erreur se répète."
    return "Analyser le service concerné, corréler avec les logs voisins et vérifier l'état de l'hôte."


def _send_email_alert(row):
    if not EMAIL_ALERTS_ENABLED:
        return
    if not SMTP_USER or not SMTP_PASSWORD or not MAIL_TO:
        log.warning("Email alert skipped — SMTP config incomplete.")
        return

    ctx = _parse_security_context(row)
    subject = f"[LogGuardian] Incident détecté - {row.get('Source','unknown')} / {row.get('Host','unknown')}"
    body = f"""Bonjour,

Un incident a été détecté par LogGuardian.

Timestamp : {row.get('Timestamp', '—')}
Source    : {row.get('Source', '—')}
Host      : {row.get('Host', '—')}
Statut    : {row.get('Statut', '—')}
Score IA  : {row.get('Score IA', '—')}
Ratio     : {row.get('Ratio', '—')}
Modèle    : {row.get('Model', '—')}

Log détecté :
{row.get('Message', '—')}

Parsing automatique :
IP détectée          : {ctx['ip']}
Utilisateur détecté : {ctx['user']}

Recommandation :
{_email_recommendation(row)}

LogGuardian — AIOps Command Center
"""
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = MAIL_TO

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)

    log.info("Email alert sent — source=%s host=%s ratio=%s", row.get("Source"), row.get("Host"), row.get("Ratio"))


def _maybe_send_email_alert(row):
    if row.get("Statut") != "ANOMALIE":
        return
    incident_key = f"{row.get('Timestamp')}|{row.get('Host')}|{row.get('Message')}"
    with _email_lock:
        if incident_key in _email_cache:
            return
        _email_cache.add(incident_key)
    threading.Thread(target=_send_email_alert, args=(row,), daemon=True).start()



def _kafka_thread():
    global _total_received
    log.info("Kafka consumer start — broker=%s topic=%s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"monitoring-ui-{random.randint(0, 999999)}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([KAFKA_TOPIC])
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            log.warning("Kafka error: %s", msg.error())
            continue
        try:
            payload = json.loads(msg.value().decode("utf-8"))
            row = _build_row(payload)
            with _lock:
                _buffer.appendleft(row)
                _total_received += 1

            _maybe_send_email_alert(row)
        except Exception as e:
            log.exception("Message parsing error: %s", e)


threading.Thread(target=_kafka_thread, daemon=True).start()

# ─────────────────────────────────────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────────────────────────────────────
app = dash.Dash(__name__, title="LogGuardian — Command Center", suppress_callback_exceptions=True)
app.server.config["SECRET_KEY"] = "logguardian-command-center"

app.index_string = """<!DOCTYPE html>
<html>
<head>{%metas%}<title>{%title%}</title>{%favicon%}{%css%}
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;600;700&display=swap" rel="stylesheet">
<style>
  *{box-sizing:border-box} html,body{margin:0;height:100%;background:#f3f6fb;font-family:Inter,Arial,sans-serif;}
  ::-webkit-scrollbar{width:8px;height:8px} ::-webkit-scrollbar-track{background:#edf1f7} ::-webkit-scrollbar-thumb{background:#b8c2d3;border-radius:99px}
  .nav-item:hover{background:#1d2b4a !important;transform:translateX(2px)}
  .mini-card:hover,.panel:hover{box-shadow:0 14px 30px rgba(15,23,42,.08);transform:translateY(-1px)}
  .pulse{animation:pulse 1.5s infinite}@keyframes pulse{0%{opacity:1}50%{opacity:.35}100%{opacity:1}}
  .Select-control{border:1px solid #d8e0ec !important;border-radius:10px !important;min-height:38px !important;background:white !important;}
  .Select-value-label,.Select-placeholder{font-size:12px;color:#475569 !important;}
  .dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner tr:hover td{background:#eef6ff !important;cursor:pointer;}
</style>
</head>
<body>{%app_entry%}<footer>{%config%}{%scripts%}{%renderer%}</footer></body>
</html>"""

# ─────────────────────────────────────────────────────────────────────────────
# UI HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _badge(text, color, bg=None):
    return html.Span(text, style={
        "display": "inline-flex", "alignItems": "center", "gap": "6px",
        "padding": "4px 9px", "borderRadius": "999px", "fontSize": "11px",
        "fontWeight": "700", "color": color, "backgroundColor": bg or f"{color}18",
        "fontFamily": "JetBrains Mono, monospace",
    })


def _sidebar():
    return html.Div(style={
        "width": "252px", "minWidth": "252px", "height": "100vh",
        "background": "linear-gradient(180deg,#0f172a 0%,#111c33 70%,#172554 100%)",
        "color": "white", "display": "flex", "flexDirection": "column",
        "boxShadow": "10px 0 30px rgba(15,23,42,.18)", "zIndex": 10,
    }, children=[
        html.Div(style={"padding": "24px 22px 18px"}, children=[
            html.Div(style={"display": "flex", "alignItems": "center", "gap": "12px"}, children=[
                html.Div("🛡", style={
                    "width": "42px", "height": "42px", "borderRadius": "14px",
                    "display": "grid", "placeItems": "center", "fontSize": "22px",
                    "background": "linear-gradient(135deg,#38bdf8,#2563eb)",
                    "boxShadow": "0 12px 28px rgba(37,99,235,.35)",
                }),
                html.Div([
                    html.Div("LogGuardian", style={"fontSize": "19px", "fontWeight": "800"}),
                    html.Div("AIOps Command Center", style={"fontSize": "11px", "color": "#93a4c4", "marginTop": "2px"}),
                ])
            ]),
        ]),
        html.Div(style={"height": "1px", "background": "rgba(255,255,255,.09)", "margin": "0 18px 12px"}),
        html.Div(style={"padding": "0 14px", "flex": 1}, children=[
            _nav_button("dashboard", "▦", "Vue cockpit", "KPIs, graphes, services"),
            _nav_button("logs", "≡", "Flux logs", "Recherche + RAG"),
            _nav_button("alerts", "⚠", "Incident board", "Anomalies critiques"),
        ]),
        html.Div(style={"padding": "18px", "borderTop": "1px solid rgba(255,255,255,.09)"}, children=[
            html.Div(style={"display": "flex", "alignItems": "center", "gap": "8px", "marginBottom": "8px"}, children=[
                html.Span(className="pulse", style={"width": "9px", "height": "9px", "borderRadius": "50%", "background": "#22c55e", "display": "inline-block"}),
                html.Span("Kafka connecté", style={"fontSize": "12px", "color": "#cbd5e1", "fontWeight": 700}),
            ]),
            html.Div(KAFKA_TOPIC, style={"fontSize": "11px", "fontFamily": "JetBrains Mono", "color": "#93a4c4", "wordBreak": "break-all"}),
        ]),
    ])


def _nav_button(page_id, icon, title, subtitle):
    return html.Button(id=f"nav-{page_id}", n_clicks=0, className="nav-item", style={
        "width": "100%", "border": 0, "background": "transparent", "color": "white",
        "padding": "12px 12px", "borderRadius": "14px", "display": "flex",
        "alignItems": "center", "gap": "12px", "textAlign": "left", "cursor": "pointer",
        "transition": "all .18s ease", "marginBottom": "7px", "fontFamily": "Inter",
    }, children=[
        html.Div(icon, style={"width": "32px", "height": "32px", "borderRadius": "10px", "display": "grid", "placeItems": "center", "background": "rgba(255,255,255,.09)", "fontSize": "17px"}),
        html.Div([html.Div(title, style={"fontWeight": 800, "fontSize": "13px"}), html.Div(subtitle, style={"fontSize": "10px", "color": "#93a4c4", "marginTop": "2px"})])
    ])


def _topbar(title, subtitle):
    return html.Div(style={
        "height": "74px", "display": "flex", "alignItems": "center", "justifyContent": "space-between",
        "padding": "0 26px", "background": "rgba(255,255,255,.88)", "backdropFilter": "blur(10px)",
        "borderBottom": f"1px solid {T['border']}", "position": "sticky", "top": 0, "zIndex": 5,
    }, children=[
        html.Div([html.H1(title, style={"margin": 0, "fontSize": "22px", "fontWeight": "850", "letterSpacing": "-.03em", "color": T["text"]}),
                  html.Div(subtitle, style={"fontSize": "12px", "color": T["muted"], "marginTop": "3px"})]),
        html.Div(style={"display": "flex", "alignItems": "center", "gap": "10px"}, children=[
            _badge("● LIVE", T["green"]),
            html.Div(id="clock", style={"fontFamily": "JetBrains Mono", "fontSize": "12px", "color": T["muted"], "background": T["paper2"], "border": f"1px solid {T['border']}", "padding": "8px 12px", "borderRadius": "12px"}),
        ])
    ])


def _metric_card(label, value_id, accent, icon, hint=""):
    return html.Div(className="mini-card", style={
        "background": T["paper"], "border": f"1px solid {T['border']}", "borderRadius": "18px",
        "padding": "16px", "boxShadow": "0 8px 22px rgba(15,23,42,.05)", "transition": "all .18s ease",
        "borderTop": f"4px solid {accent}", "minHeight": "126px",
    }, children=[
        html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}, children=[
            html.Div(icon, style={"fontSize": "22px"}),
            html.Div(hint, style={"fontSize": "10px", "fontWeight": 800, "color": accent, "fontFamily": "JetBrains Mono"}),
        ]),
        html.Div(id=value_id, style={"fontSize": "32px", "fontWeight": "900", "color": T["text"], "marginTop": "12px", "letterSpacing": "-.04em"}, children="0"),
        html.Div(label, style={"fontSize": "11px", "fontWeight": 800, "color": T["muted"], "textTransform": "uppercase", "letterSpacing": ".08em"}),
    ])


def _panel(title, children, footer=None, height=None):
    return html.Div(className="panel", style={
        "background": T["paper"], "border": f"1px solid {T['border']}", "borderRadius": "18px",
        "boxShadow": "0 8px 22px rgba(15,23,42,.05)", "overflow": "hidden", "transition": "all .18s ease", "height": height or "auto",
    }, children=[
        html.Div(style={"padding": "14px 16px", "borderBottom": f"1px solid {T['border2']}", "display": "flex", "justifyContent": "space-between", "alignItems": "center"}, children=[
            html.Div(title, style={"fontSize": "12px", "fontWeight": 900, "color": T["text"], "textTransform": "uppercase", "letterSpacing": ".08em"}),
            footer or html.Div(),
        ]),
        html.Div(style={"padding": "14px 16px"}, children=children),
    ])


def _empty_fig(text="En attente de données"):
    fig = go.Figure()
    fig.update_layout(
        paper_bgcolor="white", plot_bgcolor="white", height=260,
        margin=dict(l=20, r=20, t=20, b=20),
        xaxis=dict(visible=False), yaxis=dict(visible=False),
        annotations=[dict(text=text, x=.5, y=.5, xref="paper", yref="paper", showarrow=False, font=dict(color="#94a3b8", size=14))]
    )
    return fig


def _plot_layout(height=260):
    return dict(
        height=height, paper_bgcolor="white", plot_bgcolor="white",
        margin=dict(l=36, r=16, t=18, b=34),
        font=dict(family="Inter", color="#475569", size=11),
        xaxis=dict(gridcolor="#eef2f7", zeroline=False),
        yaxis=dict(gridcolor="#eef2f7", zeroline=False),
        legend=dict(orientation="h", y=1.08, x=0, bgcolor="rgba(0,0,0,0)")
    )

# ─────────────────────────────────────────────────────────────────────────────
# PAGE LAYOUTS
# ─────────────────────────────────────────────────────────────────────────────
def _dashboard_page():
    return html.Div(id="page-dashboard", style={"display": "none", "height": "100%", "overflow": "auto"}, children=[
        _topbar("Cockpit observabilité", "Vue radicale temps réel — anomalies, risque et santé des services"),
        html.Div(style={"padding": "22px", "display": "grid", "gap": "18px"}, children=[
            html.Div(style={"display": "grid", "gridTemplateColumns": "repeat(5, minmax(0, 1fr))", "gap": "14px"}, children=[
                _metric_card("logs reçus", "m-total", T["blue"], "📥", "STREAM"),
                _metric_card("anomalies", "m-anom", T["red"], "🔥", f"> {ALERT_THRESHOLD}x"),
                _metric_card("score moyen", "m-score", T["purple"], "🧠", "ML"),
                _metric_card("sources touchées", "m-sources", T["cyan"], "🖥", "SVC"),
                _metric_card("risk level", "m-risk", T["orange"], "⚡", "AIOPS"),
            ]),
            html.Div(style={"display": "grid", "gridTemplateColumns": "1.25fr .75fr", "gap": "18px"}, children=[
                _panel("Flux anomalies / normal", dcc.Graph(id="fig-stream", config={"displayModeBar": False})),
                _panel("Jauge risque global", dcc.Graph(id="fig-risk", config={"displayModeBar": False})),
            ]),
            html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr 1fr", "gap": "18px"}, children=[
                _panel("Top services impactés", dcc.Graph(id="fig-services", config={"displayModeBar": False})),
                _panel("Score IA — derniers événements", dcc.Graph(id="fig-score", config={"displayModeBar": False})),
                _panel("Briefing analyste IA", html.Div(id="ai-briefing")),
            ]),
        ])
    ])


def _filters_bar():
    dropdown_style = {"fontSize": "12px"}
    return html.Div(style={"display": "grid", "gridTemplateColumns": "1.5fr .7fr .7fr .55fr", "gap": "10px", "alignItems": "center"}, children=[
        dcc.Input(id="search-text", placeholder="Rechercher : ssh, timeout, kerberos, error, host...", debounce=False, style={
            "height": "40px", "borderRadius": "12px", "border": f"1px solid {T['border']}", "padding": "0 14px", "outline": "none", "fontSize": "13px", "background": "white",
        }),
        dcc.Dropdown(id="source-filter", placeholder="Source", clearable=True, style=dropdown_style),
        dcc.Dropdown(id="level-filter", value="all", clearable=False, style=dropdown_style, options=[
            {"label": "Tous", "value": "all"}, {"label": f"Anomalies > {ALERT_THRESHOLD}x", "value": "high"}, {"label": f"Normaux ≤ {ALERT_THRESHOLD}x", "value": "normal"},
        ]),
        dcc.Dropdown(id="limit-filter", value=200, clearable=False, style=dropdown_style, options=[
            {"label": "200", "value": 200}, {"label": "500", "value": 500}, {"label": "1000", "value": 1000}, {"label": "Max", "value": MAX_ROWS},
        ]),
    ])


def _logs_page():
    return html.Div(id="page-logs", style={"display": "block", "height": "100%", "overflow": "hidden"}, children=[
        _topbar("Flux logs augmenté", "Table temps réel + panneau analyste IA avec recommandation et feedback"),
        html.Div(style={"height": "calc(100% - 74px)", "display": "grid", "gridTemplateColumns": "minmax(0, 1fr) 380px", "gap": "18px", "padding": "18px", "overflow": "hidden"}, children=[
            html.Div(style={"minWidth": 0, "display": "flex", "flexDirection": "column", "gap": "14px"}, children=[
                _panel("Recherche & filtrage", _filters_bar(), footer=html.Div(id="logs-count", style={"fontSize": "12px", "fontWeight": 800, "color": T["muted"]})),
                html.Div(style={"flex": 1, "minHeight": 0}, children=[
                    dash_table.DataTable(
                        id="main-table",
                        columns=[{"name": c, "id": c} for c in TABLE_COLS],
                        data=[],
                        cell_selectable=True,
                        active_cell=None,
                        # row_selectable="single",
                        # selected_row_ids=[],
                        sort_action="native",
                        page_action="native",
                        page_size=22,
                        fixed_rows={"headers": True},
                        style_table={"height": "560px", "overflowY": "auto", "overflowX": "auto", "border": f"1px solid {T['border']}", "borderRadius": "18px"},
                        style_header={"backgroundColor": "#eaf0f8", "fontWeight": "900", "fontSize": "11px", "color": T["muted"], "border": "none", "padding": "11px", "textTransform": "uppercase", "fontFamily": "JetBrains Mono"},
                        style_cell={"backgroundColor": "white", "fontSize": "12px", "color": T["text"], "border": "none", "borderBottom": f"1px solid {T['border2']}", "padding": "10px", "fontFamily": "Inter", "whiteSpace": "nowrap", "overflow": "hidden", "textOverflow": "ellipsis"},
                        style_cell_conditional=[
                            {"if": {"column_id": "Message"}, "minWidth": "420px", "whiteSpace": "normal"},
                            {"if": {"column_id": "Timestamp"}, "fontFamily": "JetBrains Mono", "color": T["muted"], "minWidth": "150px"},
                            {"if": {"column_id": "Score IA"}, "fontFamily": "JetBrains Mono", "fontWeight": "800", "textAlign": "center", "minWidth": "85px"},
                            {"if": {"column_id": "Ratio"}, "fontFamily": "JetBrains Mono", "fontWeight": "800", "textAlign": "center", "minWidth": "80px"},
                            {"if": {"column_id": "Statut"}, "fontWeight": "900", "textAlign": "center", "minWidth": "110px"},
                        ],
                        style_data_conditional=[
                            {"if": {"row_index": "odd"}, "backgroundColor": T["paper2"]},
                            {"if": {"filter_query": '{Statut} = "ANOMALIE"', "column_id": "Statut"}, "color": T["red"]},
                            {"if": {"filter_query": '{Statut} = "NORMAL"', "column_id": "Statut"}, "color": T["green"]},
                            {"if": {"state": "selected"}, "backgroundColor": "#dbeafe", "border": f"1px solid {T['blue']}"},
                        ],
                    )
                ])
            ]),
            _rag_side_panel(),
        ])
    ])


def _rag_side_panel():
    return html.Div(style={"height": "100%", "overflow": "auto"}, children=[
        html.Div(style={"background": "linear-gradient(180deg,#0f172a 0%,#172554 100%)", "borderRadius": "22px", "padding": "18px", "color": "white", "minHeight": "100%", "boxShadow": "0 18px 40px rgba(15,23,42,.22)"}, children=[
            html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "marginBottom": "16px"}, children=[
                html.Div([html.Div("🤖 Analyste IA", style={"fontSize": "18px", "fontWeight": 900}), html.Div("RAG • recommandation • feedback", style={"fontSize": "11px", "color": "#9fb2d8", "marginTop": "2px"})]),
                html.Div(id="rag-status-chip", children=_badge("Aucun log", "#93c5fd", "rgba(147,197,253,.13)")),
            ]),
            _rag_block("Log sélectionné", "rag-desc"),
            html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "10px", "marginBottom": "12px"}, children=[
                html.Div(style={"background": "rgba(255,255,255,.08)", "border": "1px solid rgba(255,255,255,.12)", "borderRadius": "16px", "padding": "12px"}, children=[html.Div("Score IA", style={"fontSize": "10px", "color": "#9fb2d8", "fontWeight": 800}), html.Div(id="rag-score", style={"fontSize": "24px", "fontWeight": 900, "fontFamily": "JetBrains Mono"}, children="—")]),
                html.Div(style={"background": "rgba(255,255,255,.08)", "border": "1px solid rgba(255,255,255,.12)", "borderRadius": "16px", "padding": "12px"}, children=[html.Div("Ratio", style={"fontSize": "10px", "color": "#9fb2d8", "fontWeight": 800}), html.Div(id="rag-ratio", style={"fontSize": "24px", "fontWeight": 900, "fontFamily": "JetBrains Mono"}, children="—")]),
            ]),
            _rag_block("Analyse RAG", "rag-analysis"),
            _rag_block("Action recommandée", "rag-action"),
            html.Div(style={"marginTop": "14px", "paddingTop": "14px", "borderTop": "1px solid rgba(255,255,255,.12)"}, children=[
                html.Div("Feedback utilisateur", style={"fontSize": "11px", "fontWeight": 900, "color": "#cbd5e1", "textTransform": "uppercase", "letterSpacing": ".08em", "marginBottom": "10px"}),
                html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "10px"}, children=[
                    html.Button("👍 Utile", id="fb-up", n_clicks=0, style={"height": "38px", "borderRadius": "12px", "border": "1px solid rgba(34,197,94,.45)", "background": "rgba(34,197,94,.12)", "color": "#bbf7d0", "fontWeight": 900, "cursor": "pointer"}),
                    html.Button("👎 Pas utile", id="fb-down", n_clicks=0, style={"height": "38px", "borderRadius": "12px", "border": "1px solid rgba(248,113,113,.45)", "background": "rgba(248,113,113,.12)", "color": "#fecaca", "fontWeight": 900, "cursor": "pointer"}),
                ]),
                html.Div(id="feedback-status", style={"fontSize": "12px", "color": "#9fb2d8", "marginTop": "10px"}, children="Le feedback sera sauvegardé pour évaluer le RAG."),
            ])
        ])
    ])


def _rag_block(title, component_id):
    return html.Div(style={"marginBottom": "12px"}, children=[
        html.Div(title, style={"fontSize": "10px", "fontWeight": 900, "color": "#9fb2d8", "textTransform": "uppercase", "letterSpacing": ".08em", "marginBottom": "7px"}),
        html.Div(id=component_id, style={"background": "rgba(255,255,255,.08)", "border": "1px solid rgba(255,255,255,.12)", "borderLeft": "4px solid #38bdf8", "borderRadius": "16px", "padding": "13px", "fontSize": "13px", "lineHeight": "1.55", "color": "#e2e8f0"}, children="Sélectionnez un log dans le tableau."),
    ])


def _alerts_page():
    return html.Div(id="page-alerts", style={"display": "none", "height": "100%", "overflow": "auto"}, children=[
        _topbar("Incident board", f"Événements dont le ratio dépasse {ALERT_THRESHOLD}x"),
        html.Div(style={"padding": "22px", "display": "grid", "gap": "18px"}, children=[
            html.Div(id="alert-strip"),
            html.Div(style={"background": "white", "border": f"1px solid {T['border']}", "borderRadius": "18px", "overflow": "hidden"}, children=[
                dash_table.DataTable(
                    id="alert-table", columns=[{"name": c, "id": c} for c in ALERT_COLS], data=[],
                    sort_action="native", page_action="native", page_size=30,
                    style_table={"overflowX": "auto"},
                    style_header={"backgroundColor": "#fee2e2", "fontWeight": "900", "fontSize": "11px", "color": T["red"], "border": "none", "padding": "12px", "textTransform": "uppercase", "fontFamily": "JetBrains Mono"},
                    style_cell={"backgroundColor": "white", "fontSize": "12px", "color": T["text"], "border": "none", "borderBottom": f"1px solid {T['border2']}", "padding": "11px", "fontFamily": "Inter"},
                    style_cell_conditional=[{"if": {"column_id": "Message"}, "minWidth": "520px", "whiteSpace": "normal"}, {"if": {"column_id": "Ratio"}, "color": T["red"], "fontWeight": 900, "fontFamily": "JetBrains Mono"}],
                )
            ])
        ])
    ])

# ─────────────────────────────────────────────────────────────────────────────
# ROOT LAYOUT — all pages mounted once, only hidden/shown
# ─────────────────────────────────────────────────────────────────────────────
app.layout = html.Div(style={"height": "100vh", "display": "flex", "background": T["bg"], "overflow": "hidden"}, children=[
    _sidebar(),
    html.Div(style={"flex": 1, "minWidth": 0, "height": "100vh", "overflow": "hidden"}, children=[
        _dashboard_page(),
        _logs_page(),
        _alerts_page(),
    ]),
    dcc.Interval(id="interval", interval=REFRESH_INTERVAL_MS, n_intervals=0),
    dcc.Store(id="current-page", data="logs"),
    dcc.Store(id="rows-store", data=[]),
    dcc.Store(id="selected-log-store", data=None),
])

# ─────────────────────────────────────────────────────────────────────────────
# DATA + FIGURES
# ─────────────────────────────────────────────────────────────────────────────
def _visible_rows():
    with _lock:
        return list(_buffer), _total_received


def _filter_rows(rows, search, source, level, limit):
    search = (search or "").lower().strip()
    level = level or "all"
    limit = int(limit or 200)
    out = []
    for r in rows:
        if search:
            hay = " ".join([r.get("Timestamp", ""), r.get("Source", ""), r.get("Host", ""), r.get("Message", ""), r.get("Statut", "")]).lower()
            if search not in hay:
                continue
        if source and r.get("Source") != source:
            continue
        if level == "high" and r.get("_ratio_val", 0) <= ALERT_THRESHOLD:
            continue
        if level == "normal" and r.get("_ratio_val", 0) > ALERT_THRESHOLD:
            continue
        out.append(r)
    return out[:limit]


def _display_row(r, include_model=False):
    cols = TABLE_COLS + (["Model"] if include_model else [])
    d = {k: r.get(k, "") for k in cols}
    d["id"] = r.get("id")
    return d


def _stream_fig(rows):
    if not rows:
        return _empty_fig()
    recent = list(reversed(rows[:60]))
    x = list(range(1, len(recent) + 1))
    normal = [1 if r.get("Statut") == "NORMAL" else 0 for r in recent]
    anom = [1 if r.get("Statut") == "ANOMALIE" else 0 for r in recent]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=x, y=normal, name="Normal", marker_color="#86efac"))
    fig.add_trace(go.Bar(x=x, y=anom, name="Anomalie", marker_color="#f87171"))
    fig.update_layout(**_plot_layout(280), barmode="stack", yaxis_title="events", xaxis_title="derniers logs")
    return fig


def _score_fig(rows):
    if not rows:
        return _empty_fig()
    recent = list(reversed(rows[:60]))
    x = list(range(1, len(recent) + 1))
    y = [r.get("_score_val", 0) for r in recent]
    fig = go.Figure(go.Scatter(x=x, y=y, mode="lines+markers", line=dict(color=T["purple"], width=3), marker=dict(size=5), fill="tozeroy", fillcolor="rgba(124,58,237,.10)", name="Score IA"))
    fig.add_hline(y=ALERT_THRESHOLD, line_dash="dash", line_color=T["red"], annotation_text="seuil")
    fig.update_layout(**_plot_layout(240))
    return fig


def _services_fig(rows):
    if not rows:
        return _empty_fig()
    cnt = Counter(r.get("Source", "unknown") for r in rows if r.get("Statut") == "ANOMALIE")
    if not cnt:
        return _empty_fig("Aucune anomalie critique")
    top = cnt.most_common(8)
    fig = go.Figure(go.Bar(x=[v for _, v in top], y=[k for k, _ in top], orientation="h", marker_color=T["cyan"]))
    layout = _plot_layout(240)
    layout["margin"] = dict(l=90, r=16, t=18, b=34)
    fig.update_layout(**layout)
    return fig


def _risk_fig(rows):
    if not rows:
        val = 0
    else:
        anom = sum(1 for r in rows if r.get("Statut") == "ANOMALIE")
        avg_ratio = sum(r.get("_ratio_val", 0) for r in rows[:200]) / max(1, len(rows[:200]))
        val = min(100, round((anom / max(1, len(rows)) * 70) + avg_ratio * 12, 1))
    color = T["green"] if val < 35 else T["orange"] if val < 70 else T["red"]
    fig = go.Figure(go.Indicator(mode="gauge+number", value=val, number={"suffix": "%", "font": {"size": 42, "color": color}}, gauge={"axis": {"range": [0, 100]}, "bar": {"color": color}, "bgcolor": "#f8fafc", "bordercolor": T["border"]}, title={"text": "Risk score"}))
    fig.update_layout(**_plot_layout(280))
    return fig


def _ai_briefing(rows):
    if not rows:
        return html.Div("Aucun événement reçu pour le moment.", style={"color": T["muted"], "fontSize": "13px"})
    recent = rows[:80]
    anom = [r for r in recent if r.get("Statut") == "ANOMALIE"]
    top_src = Counter(r.get("Source", "unknown") for r in anom).most_common(1)
    src_txt = top_src[0][0] if top_src else "aucune source critique"
    risk = "ÉLEVÉ" if len(anom) > 20 else "MOYEN" if len(anom) > 5 else "FAIBLE"
    color = T["red"] if risk == "ÉLEVÉ" else T["orange"] if risk == "MOYEN" else T["green"]
    return html.Div(style={"display": "grid", "gap": "10px"}, children=[
        html.Div([html.Div("Synthèse automatique", style={"fontWeight": 900, "fontSize": "13px"}), html.Div(f"{len(recent)} événements récents analysés", style={"color": T["muted"], "fontSize": "12px", "marginTop": "2px"})]),
        html.Div(style={"padding": "12px", "background": T["paper2"], "borderRadius": "14px", "border": f"1px solid {T['border2']}"}, children=[
            html.Div(f"Risque global : {risk}", style={"fontWeight": 900, "color": color}),
            html.Div(f"Anomalies détectées : {len(anom)}", style={"fontSize": "12px", "color": T["muted"], "marginTop": "5px"}),
            html.Div(f"Source la plus touchée : {src_txt}", style={"fontSize": "12px", "color": T["muted"], "marginTop": "3px"}),
        ]),
        html.Div("Recommandation : prioriser les logs avec ratio élevé, vérifier les services impactés et utiliser le feedback RAG pour améliorer les explications.", style={"fontSize": "12px", "lineHeight": "1.55", "color": T["text"]}),
    ])


def _rag_for(row):
    if not row:
        return {
            "desc": "Sélectionnez un log dans le tableau.", "score": "—", "ratio": "—", "status": _badge("Aucun log", "#93c5fd", "rgba(147,197,253,.13)"),
            "analysis": "L'analyse RAG apparaîtra ici dès qu'une ligne sera sélectionnée.",
            "action": "Aucune action pour le moment.",
        }
    is_anom = row.get("Statut") == "ANOMALIE"
    msg = row.get("Message", "—")
    score = row.get("Score IA", "0.00")
    ratio = row.get("Ratio", "0.00x")
    src = row.get("Source", "unknown")
    host = row.get("Host", "unknown")
    if is_anom:
        analysis = f"Le modèle classe ce log comme anomalie car son ratio de sévérité ({ratio}) dépasse le seuil configuré ({ALERT_THRESHOLD:.1f}x). Le service {src} sur l'hôte {host} présente un comportement inhabituel par rapport au profil appris."
        action = "Action recommandée : inspecter le service, corréler avec les logs voisins, vérifier les accès récents et redémarrer le pod si l'erreur se répète."
        status = _badge("ANOMALIE", "#fecaca", "rgba(220,38,38,.22)")
    else:
        analysis = f"Le log est considéré comme normal : le ratio {ratio} reste sous le seuil critique. Le score IA peut paraître élevé, mais la décision repose sur le ratio normalisé."
        action = "Aucune action immédiate. Continuer la surveillance et conserver le log pour analyse historique."
        status = _badge("NORMAL", "#bbf7d0", "rgba(34,197,94,.18)")
    return {"desc": msg, "score": str(score), "ratio": str(ratio), "status": status, "analysis": analysis, "action": action}

# ─────────────────────────────────────────────────────────────────────────────
# CALLBACKS
# ─────────────────────────────────────────────────────────────────────────────
@app.callback(
    Output("current-page", "data"),
    Input("nav-dashboard", "n_clicks"), Input("nav-logs", "n_clicks"), Input("nav-alerts", "n_clicks"),
    State("current-page", "data"), prevent_initial_call=True,
)
def navigate(a, b, c, current):
    ctx = callback_context
    if not ctx.triggered:
        return current
    return {"nav-dashboard": "dashboard", "nav-logs": "logs", "nav-alerts": "alerts"}.get(ctx.triggered[0]["prop_id"].split(".")[0], current)


@app.callback(
    Output("page-dashboard", "style"), Output("page-logs", "style"), Output("page-alerts", "style"),
    Input("current-page", "data"),
)
def show_page(page):
    base_visible = {"display": "block", "height": "100%", "overflow": "auto"}
    logs_visible = {"display": "block", "height": "100%", "overflow": "hidden"}
    hidden = {"display": "none"}
    return (base_visible if page == "dashboard" else hidden,
            logs_visible if page == "logs" else hidden,
            base_visible if page == "alerts" else hidden)


@app.callback(
    Output("clock", "children"),
    Input("interval", "n_intervals"),
)
def tick(_):
    return datetime.now().strftime("%A %d %B %Y • %H:%M:%S").upper()


@app.callback(
    Output("metrics", "data", allow_duplicate=True) if False else Output("m-total", "children"),
    Output("m-anom", "children"), Output("m-score", "children"), Output("m-sources", "children"), Output("m-risk", "children"),
    Output("fig-stream", "figure"), Output("fig-risk", "figure"), Output("fig-services", "figure"), Output("fig-score", "figure"), Output("ai-briefing", "children"),
    Input("interval", "n_intervals"),
)
def update_dashboard(_):
    rows, total = _visible_rows()
    anom = [r for r in rows if r.get("Statut") == "ANOMALIE"]
    avg = sum(r.get("_score_val", 0) for r in rows) / max(1, len(rows))
    sources = len({r.get("Source") for r in anom if r.get("Source")})
    risk = round(len(anom) / max(1, len(rows)) * 100, 1) if rows else 0
    return str(total), str(len(anom)), f"{avg:.2f}", str(sources), f"{risk}%", _stream_fig(rows), _risk_fig(rows), _services_fig(rows), _score_fig(rows), _ai_briefing(rows)

@app.callback(
    Output("main-table", "data"),
    Output("source-filter", "options"),
    Output("rows-store", "data"),
    Output("logs-count", "children"),
    Input("interval", "n_intervals"),
    Input("current-page", "data"),
    Input("search-text", "value"),
    Input("source-filter", "value"),
    Input("level-filter", "value"),
    Input("limit-filter", "value"),
)
def update_logs(_, page, search, source, level, limit):
    rows, _total = _visible_rows()

    # sécurité : si pas de page ou filtre vide
    level = level or "all"
    limit = int(limit or 200)

    filtered = _filter_rows(rows, search, source, level, limit)

    source_values = sorted({
        r.get("Source")
        for r in rows
        if r.get("Source")
    })

    options = [{"label": s, "value": s} for s in source_values]

    table_rows = []
    for r in filtered:
        table_rows.append({
            "id": r.get("id"),
            "Timestamp": r.get("Timestamp", ""),
            "Source": r.get("Source", ""),
            "Host": r.get("Host", ""),
            "Message": r.get("Message", ""),
            "Score IA": r.get("Score IA", ""),
            "Ratio": r.get("Ratio", ""),
            "Statut": r.get("Statut", ""),
        })

    return table_rows, options, table_rows, f"{len(filtered)} / {len(rows)} logs"

# @app.callback(
#     Output("main-table", "data"),
#     Output("source-filter", "options"),
#     Output("rows-store", "data"),
#     Output("logs-count", "children"),
#     Input("interval", "n_intervals"),
#     Input("current-page", "data"),
#     Input("search-text", "value"),
#     Input("source-filter", "value"),
#     Input("level-filter", "value"),
#     Input("limit-filter", "value"),
# )
# def update_logs(_, page, search, source, level, limit):
#     rows, _total = _visible_rows()
#     filtered = _filter_rows(rows, search, source, level, limit)
#     options = [{"label": s, "value": s} for s in sorted({r.get("Source") for r in rows if r.get("Source")})]
#     return [_display_row(r) for r in filtered], options, filtered, f"{len(filtered)} / {len(rows)} logs"
# @app.callback(
#     Output("main-table", "data"), Output("source-filter", "options"), Output("rows-store", "data"), Output("logs-count", "children"),
#     Input("interval", "n_intervals"), Input("search-text", "value"), Input("source-filter", "value"), Input("level-filter", "value"), Input("limit-filter", "value"),
# )
# def update_logs(_, search, source, level, limit):
#     rows, _total = _visible_rows()
#     filtered = _filter_rows(rows, search, source, level, limit)
#     options = [{"label": s, "value": s} for s in sorted({r.get("Source") for r in rows if r.get("Source")})]
#     return [_display_row(r) for r in filtered], options, filtered, f"{len(filtered)} / {len(rows)} logs"

@app.callback(
    Output("selected-log-store", "data"),
    Input("main-table", "active_cell"),
    State("main-table", "data"),
    prevent_initial_call=True,
)
def store_selected(active_cell, table_data):
    if not active_cell or not table_data:
        return None

    row_index = active_cell.get("row")

    if row_index is None or row_index >= len(table_data):
        return None

    return table_data[row_index]

# @app.callback(
#     Output("selected-log-store", "data"),
#     Input("main-table", "selected_row_ids"), State("rows-store", "data"), prevent_initial_call=True,
# )
# def store_selected(row_ids, rows):
#     if not row_ids or not rows:
#         return None
#     selected_id = row_ids[0]
#     return next((r for r in rows if r.get("id") == selected_id), None)


@app.callback(
    Output("rag-desc", "children"), Output("rag-score", "children"), Output("rag-ratio", "children"),
    Output("rag-status-chip", "children"), Output("rag-analysis", "children"), Output("rag-action", "children"), Output("feedback-status", "children"),
    Input("selected-log-store", "data"),
)
def render_rag(row):
    r = _rag_for(row)
    return r["desc"], r["score"], r["ratio"], r["status"], r["analysis"], r["action"], "Le feedback sera sauvegardé pour évaluer le RAG."


@app.callback(
    Output("feedback-status", "children", allow_duplicate=True),
    Input("fb-up", "n_clicks"), Input("fb-down", "n_clicks"), State("selected-log-store", "data"), prevent_initial_call=True,
)
def save_feedback(up, down, row):
    if not row:
        return "Sélectionne d'abord un log avant de donner un feedback."
    ctx = callback_context
    if not ctx.triggered:
        raise PreventUpdate
    trigger = ctx.triggered[0]["prop_id"].split(".")[0]
    feedback = "positive" if trigger == "fb-up" else "negative"
    record = {
        "timestamp": datetime.now().isoformat(), "feedback": feedback,
        "log_id": row.get("id"), "source": row.get("Source"), "host": row.get("Host"),
        "message": row.get("Message"), "score_ia": row.get("Score IA"), "ratio": row.get("Ratio"), "statut": row.get("Statut"),
    }
    try:
        os.makedirs(os.path.dirname(FEEDBACK_PATH), exist_ok=True)
        with open(FEEDBACK_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        return "✅ Feedback enregistré pour l'évaluation du RAG."
    except Exception as e:
        return f"⚠️ Feedback non sauvegardé : {e}"


@app.callback(
    Output("alert-table", "data"), Output("alert-strip", "children"),
    Input("interval", "n_intervals"),
)
def update_alerts(_):
    rows, _ = _visible_rows()
    critical = [r for r in rows if r.get("_ratio_val", 0) > ALERT_THRESHOLD]
    strip = html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr 1fr", "gap": "14px"}, children=[
        _metric_card("alertes critiques", "alert-dummy-1", T["red"], "🚨", "ACTIVE"),
        _metric_card("dernier événement", "alert-dummy-2", T["orange"], "⏱", "LAST"),
        _metric_card("source principale", "alert-dummy-3", T["cyan"], "🖥", "TOP"),
    ])
    # Fill static IDs inside dynamic strip is problematic if callbacks target them; no callbacks target them.
    last = critical[0].get("Timestamp", "—") if critical else "—"
    top = Counter(r.get("Source", "unknown") for r in critical).most_common(1)
    top_src = top[0][0] if top else "—"
    strip.children[0].children[1].children = str(len(critical))
    strip.children[1].children[1].children = last[-8:] if last != "—" else "—"
    strip.children[2].children[1].children = top_src
    return [_display_row(r, include_model=True) for r in critical], strip




if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
