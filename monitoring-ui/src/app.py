"""
LogGuardian — AIOps Command Center
Design premium : sidebar dark élégante, dashboard dense, filtres fixes, RAG panel.
"""
import json
import logging
import os
import random
import threading
from collections import Counter, deque
from datetime import datetime

import dash
from dash import (
    dash_table,
    dcc,
    html,
    callback_context,
    no_update,
)

from groq import Groq
#from dash import dash_table, dcc, html, callback_context
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import plotly.graph_objects as go
from confluent_kafka import Consumer

# ─── CONFIG ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("logguardian-ui")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "logs-anomalies-ml")
MAX_ROWS                = int(os.getenv("MAX_ROWS", "2000"))
REFRESH_INTERVAL_MS     = int(os.getenv("REFRESH_INTERVAL_MS", "3000"))
ALERT_THRESHOLD         = float(os.getenv("ALERT_THRESHOLD", "2.0"))
FEEDBACK_PATH           = os.getenv("FEEDBACK_PATH", "/app/feedback/rag_feedback.jsonl")

SMTP_HOST            = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT            = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER            = os.getenv("SMTP_USER", "")
SMTP_PASSWORD        = os.getenv("SMTP_PASSWORD", "")
MAIL_TO              = os.getenv("MAIL_TO", "")
EMAIL_ALERTS_ENABLED = os.getenv("EMAIL_ALERTS_ENABLED", "false").lower() == "true"

GROQ_API_KEY = os.getenv(
    "GROQ_API_KEY",
    "",
).strip()

GROQ_MODEL = os.getenv(
    "GROQ_MODEL",
    "openai/gpt-oss-20b",
).strip()

_buffer: deque = deque(maxlen=MAX_ROWS)
_seen: set = set()
_lock = threading.Lock()

_total_received = 0
_total_alerts_received = 0

# ─── PALETTE ──────────────────────────────────────────────────────────────────
# Fond global clair bleuté, sidebar dark navy, accents colorés
BG    = "#f0f4fa"         # fond global
PAPER = "#ffffff"         # cartes
SIDE  = "#0d1526"         # sidebar dark
SIDE2 = "#111e35"
BD    = "#dde4f0"         # border standard
BD2   = "#eaf0f8"         # border légère
TXT   = "#111827"         # texte principal
MUT   = "#64748b"         # texte muet
MUT2  = "#94a3b8"
BLUE  = "#2563eb"
CYAN  = "#0891b2"
GREEN = "#059669"
RED   = "#dc2626"
ORAN  = "#ea580c"
PURP  = "#7c3aed"
YELL  = "#d97706"

TABLE_COLS = ["Timestamp", "Source", "Host", "Message", "Score IA", "Ratio", "Statut"]
ALERT_COLS = ["Timestamp", "Source", "Host", "Message", "Score IA", "Ratio", "Model"]

# ─── KAFKA ────────────────────────────────────────────────────────────────────
def _safe_float(v, default=0.0):
    try:    return float(v)
    except: return default

def _extract_message(r):
    seq = r.get("sequence") or []
    if isinstance(seq, list) and seq:
        last = seq[-1]
        if isinstance(last, dict): return str(last.get("message", ""))[:240]
        return str(last)[:240]
    return str(r.get("message", ""))[:240]

def _build_row(r):
    detected  = str(r.get("detected_at", ""))
    score     = _safe_float(r.get("anomaly_score", 0))
    ratio     = _safe_float(r.get("severity_ratio", 0))
    threshold = _safe_float(r.get("threshold", ALERT_THRESHOLD))
    status    = "ANOMALIE" if ratio > ALERT_THRESHOLD else "NORMAL"
    return {
        "id":              f"{detected}_{random.randint(0, 999999)}",
        "Timestamp":       detected[:19].replace("T", " "),
        "Source":          str(r.get("source", "unknown")),
        "Host":            str(r.get("host", "unknown")),
        "Message":         _extract_message(r),
        "Score IA":        f"{score:.2f}",
        "Ratio":           f"{ratio:.2f}x",
        "Model":           str(r.get("model_version", "lstm_v1")),
        "Statut":          status,
        "_score_val":      score,
        "_ratio_val":      ratio,
        "_threshold_val":  threshold,
        "_raw":            r,
    }

def _parse_security_context(row):
    import re
    message   = row.get("Message", "")
    ip_match  = re.search(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", message)
    usr_match = re.search(r"user=([a-zA-Z0-9_.-]+)", message)
    return {
        "ip":   ip_match.group(0)  if ip_match  else "non détectée",
        "user": usr_match.group(1) if usr_match else "non détecté",
    }
def _kafka_thread():
    global _total_received, _total_alerts_received

    log.info(
        "Kafka consumer start — broker=%s topic=%s",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
    )

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
                # Le buffer ne garde que les derniers logs
                _buffer.appendleft(row)

                # Total de tous les logs reçus
                _total_received += 1

                # Total cumulé des alertes :
                # ce compteur ne baisse jamais pendant l'exécution
                if row.get("Statut") == "ANOMALIE":
                    _total_alerts_received += 1

        except Exception as error:
            log.exception("Message parsing error: %s", error)
# def _kafka_thread():
#     global _total_received
#     log.info("Kafka consumer start — broker=%s topic=%s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
#     consumer = Consumer({
#         "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
#         "group.id":           f"monitoring-ui-{random.randint(0, 999999)}",
#         "auto.offset.reset":  "earliest",
#         "enable.auto.commit": True,
#     })
#     consumer.subscribe([KAFKA_TOPIC])
#     while True:
#         msg = consumer.poll(timeout=1.0)
#         if msg is None: continue
#         if msg.error(): log.warning("Kafka error: %s", msg.error()); continue
#         try:
#             payload = json.loads(msg.value().decode("utf-8"))
#             row = _build_row(payload)
#             with _lock:
#                 _buffer.appendleft(row)
#                 _total_received += 1
#         except Exception as e:
#             log.exception("Message parsing error: %s", e)

threading.Thread(target=_kafka_thread, daemon=True).start()

# ─── APP ──────────────────────────────────────────────────────────────────────
app = dash.Dash(__name__, title="LogGuardian — Command Center", suppress_callback_exceptions=True)
app.server.config["SECRET_KEY"] = "logguardian-command-center"

app.index_string = """<!DOCTYPE html>
<html>
<head>{%metas%}<title>{%title%}</title>{%favicon%}{%css%}
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:ital,wght@0,300;0,400;0,500;0,600;0,700;0,800;0,900;1,400&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
/* ── Reset ── */
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100%; font-family: "Inter", system-ui, sans-serif; background: """ + BG + """; overflow: hidden; -webkit-font-smoothing: antialiased; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: #c8d4e8; border-radius: 99px; }
::-webkit-scrollbar-thumb:hover { background: #a0b0cc; }

/* ── Sidebar nav hover ── */
.nav-item { transition: background .15s, color .15s, border-color .15s; }
.nav-item:hover { background: rgba(255,255,255,.07) !important; }
.nav-item.active { background: rgba(99,133,255,.18) !important; border-color: rgba(99,133,255,.45) !important; }

/* ── Card hover ── */
.card-hover { transition: box-shadow .18s, transform .18s; }
.card-hover:hover { box-shadow: 0 12px 32px rgba(15,30,80,.1) !important; transform: translateY(-1px); }

/* ── Table row hover ── */
.dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner tr:hover td {
    background: #e8f0fe !important; cursor: pointer;
}
.dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner td[data-active=true],
.dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner tr.selected td {
    background: #dbeafe !important;
    outline: none !important;
    box-shadow: inset 0 0 0 1px """ + BLUE + """ !important;
}

/* ── Animations ── */
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
.pulse { animation: pulse 1.8s ease-in-out infinite; }
@keyframes slide-in { from{opacity:0;transform:translateY(8px)} to{opacity:1;transform:translateY(0)} }
.slide-in { animation: slide-in .28s ease both; }

/* ── Dropdown react-select v1 ── */
.dash-dropdown .Select-control {
    height: 40px !important; min-height: 40px !important;
    border: 1.5px solid """ + BD + """ !important; border-radius: 10px !important;
    background: white !important; box-shadow: none !important;
    transition: border-color .15s !important;
}
.dash-dropdown .Select-control:hover { border-color: """ + BLUE + """ !important; }
.dash-dropdown .Select-placeholder, .dash-dropdown .Select-value-label {
    line-height: 38px !important; font-size: 13px !important; color: """ + MUT + """ !important;
}
.dash-dropdown .Select-arrow-zone { padding-right: 10px !important; }
.dash-dropdown .Select-menu-outer {
    border: 1.5px solid """ + BD + """ !important; border-radius: 10px !important;
    box-shadow: 0 8px 32px rgba(15,30,80,.13) !important; overflow: hidden; z-index: 9999 !important;
}
.dash-dropdown .VirtualizedSelectOption { font-size: 13px !important; color: """ + TXT + """ !important; padding: 9px 14px !important; }
.dash-dropdown .VirtualizedSelectFocusedOption { background: #eef4ff !important; color: """ + BLUE + """ !important; }
.dash-dropdown .Select-input input { font-size: 13px !important; color: """ + TXT + """ !important; }

/* ── Dropdown react-select v2+ ── */
.dash-dropdown .Select__control {
    min-height: 40px !important; height: 40px !important;
    border: 1.5px solid """ + BD + """ !important; border-radius: 10px !important;
    background: white !important; box-shadow: none !important;
}
.dash-dropdown .Select__control:hover,
.dash-dropdown .Select__control--is-focused { border-color: """ + BLUE + """ !important; box-shadow: 0 0 0 3px rgba(37,99,235,.1) !important; }
.dash-dropdown .Select__single-value { font-size: 13px !important; color: """ + TXT + """ !important; }
.dash-dropdown .Select__placeholder { font-size: 13px !important; color: """ + MUT2 + """ !important; }
.dash-dropdown .Select__menu { border: 1.5px solid """ + BD + """ !important; border-radius: 10px !important; box-shadow: 0 8px 32px rgba(15,30,80,.13) !important; }
.dash-dropdown .Select__option { font-size: 13px !important; color: """ + TXT + """ !important; }
.dash-dropdown .Select__option--is-focused { background: #eef4ff !important; color: """ + BLUE + """ !important; }
.dash-dropdown .Select__indicator-separator { display: none; }
.dash-dropdown .Select__dropdown-indicator svg { color: """ + MUT2 + """ !important; }
.dash-dropdown .Select__input input { font-size: 13px !important; color: """ + TXT + """ !important; }

/* ── Search input ── */
.lg-search:focus { border-color: """ + BLUE + """ !important; box-shadow: 0 0 0 3px rgba(37,99,235,.1) !important; outline: none !important; }
.lg-search::placeholder { color: """ + MUT2 + """; font-size: 13px; }

/* ── Topbar glass ── */
.topbar-glass {
    background: rgba(255,255,255,.82);
    backdrop-filter: blur(14px);
    -webkit-backdrop-filter: blur(14px);
    border-bottom: 1px solid """ + BD + """;
}

/* ── Pill badge ── */
.pill {
    display: inline-flex; align-items: center; gap: 5px;
    padding: 3px 10px; border-radius: 999px;
    font-size: 11px; font-weight: 700; letter-spacing: .04em;
    font-family: "JetBrains Mono", monospace;
}
</style>
</head>
<body>{%app_entry%}<footer>{%config%}{%scripts%}{%renderer%}</footer></body>
</html>"""

# ─── FIGURE HELPERS ───────────────────────────────────────────────────────────
def _empty_fig(text="En attente de données", h=260):
    fig = go.Figure()
    fig.update_layout(
        paper_bgcolor="white", plot_bgcolor="white", height=h,
        margin=dict(l=16, r=16, t=16, b=16),
        xaxis=dict(visible=False), yaxis=dict(visible=False),
        annotations=[dict(
            text=text, x=.5, y=.5, xref="paper", yref="paper",
            showarrow=False, font=dict(color=MUT2, size=13, family="Inter"),
        )],
    )
    return fig

def _plot_layout(h=260, lm=40):
    return dict(
        height=h, paper_bgcolor="white", plot_bgcolor="white",
        margin=dict(l=lm, r=16, t=16, b=32),
        font=dict(family="Inter", color=MUT, size=11),
        xaxis=dict(gridcolor="#f0f4fa", zeroline=False, showline=False),
        yaxis=dict(gridcolor="#f0f4fa", zeroline=False, showline=False),
        legend=dict(orientation="h", y=1.06, x=0, bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
        hovermode="x unified",
        hoverlabel=dict(bgcolor="white", bordercolor=BD, font=dict(color=TXT, size=12, family="Inter")),
    )

def _stream_fig(rows):
    if not rows: return _empty_fig()
    recent = list(reversed(rows[:80]))
    x = list(range(len(recent)))
    norm = [1 if r.get("Statut") == "NORMAL"   else 0 for r in recent]
    anom = [1 if r.get("Statut") == "ANOMALIE" else 0 for r in recent]
    fig  = go.Figure()
    fig.add_trace(go.Bar(x=x, y=norm, name="Normal",   marker_color="#34d399", marker_line_width=0))
    fig.add_trace(go.Bar(x=x, y=anom, name="Anomalie", marker_color="#f87171", marker_line_width=0))
    fig.update_layout(**_plot_layout(280), barmode="stack")
    return fig

def _score_fig(rows):
    if not rows: return _empty_fig()
    recent = list(reversed(rows[:80]))
    x = list(range(len(recent)))
    y = [r.get("_score_val", 0) for r in recent]
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=x, y=y, mode="lines", name="Score IA",
        line=dict(color=PURP, width=2.5, shape="spline", smoothing=1.1),
        fill="tozeroy", fillcolor="rgba(124,58,237,.07)",
    ))
    fig.add_hline(y=ALERT_THRESHOLD, line_dash="dot", line_color=RED,
                  annotation_text=f"seuil {ALERT_THRESHOLD}x",
                  annotation_font=dict(color=RED, size=10))
    fig.update_layout(**_plot_layout(240))
    return fig

def _services_fig(rows):
    if not rows: return _empty_fig()
    cnt = Counter(r.get("Source", "?") for r in rows if r.get("Statut") == "ANOMALIE")
    if not cnt: return _empty_fig("Aucune anomalie critique")
    top    = cnt.most_common(8)
    colors = [BLUE, CYAN, PURP, GREEN, ORAN, RED, YELL, MUT]
    fig    = go.Figure(go.Bar(
        x=[v for _, v in top], y=[k for k, _ in top], orientation="h",
        marker_color=colors[:len(top)], marker_line_width=0,
        text=[str(v) for _, v in top], textposition="outside",
        textfont=dict(size=11, color=MUT),
    ))
    l = _plot_layout(240, lm=110)
    l["margin"]["r"] = 36
    fig.update_layout(**l)
    return fig

def _risk_fig(rows):
    if not rows: val = 0
    else:
        anom     = sum(1 for r in rows if r.get("Statut") == "ANOMALIE")
        avg_r    = sum(r.get("_ratio_val", 0) for r in rows[:200]) / max(1, len(rows[:200]))
        val      = min(100, round((anom / max(1, len(rows)) * 70) + avg_r * 12, 1))
    color = GREEN if val < 35 else ORAN if val < 70 else RED
    fig   = go.Figure(go.Indicator(
        mode="gauge+number", value=val,
        number={"suffix": "%", "font": {"size": 40, "color": color, "family": "JetBrains Mono, monospace"}},
        gauge={
            "axis":      {"range": [0, 100], "tickwidth": 1, "tickcolor": BD, "tickfont": {"size": 9}},
            "bar":       {"color": color, "thickness": .22},
            "bgcolor":   "white",
            "bordercolor": BD,
            "steps":     [
                {"range": [0,  35], "color": "#f0fdf4"},
                {"range": [35, 70], "color": "#fff7ed"},
                {"range": [70, 100],"color": "#fef2f2"},
            ],
            "threshold": {"line": {"color": color, "width": 2}, "value": val},
        },
    ))
    fig.update_layout(height=260, paper_bgcolor="white", plot_bgcolor="white",
                      margin=dict(l=20, r=20, t=20, b=20),
                      font=dict(family="Inter, sans-serif"))
    return fig

# ─── UI COMPONENTS ────────────────────────────────────────────────────────────

def _pill(text, color):
    return html.Span(text, className="pill", style={"color": color, "background": f"{color}18", "border": f"1px solid {color}33"})

def _kpi(label, vid, accent, icon, hint):
    return html.Div(className="card-hover", style={
        "background": PAPER, "border": f"1px solid {BD}", "borderRadius": "16px",
        "padding": "18px 20px", "boxShadow": "0 2px 12px rgba(15,30,80,.06)",
        "borderTop": f"3px solid {accent}", "minHeight": "118px", "position": "relative",
    }, children=[
        html.Div(style={"display": "flex", "justifyContent": "space-between", "marginBottom": "14px"}, children=[
            html.Span(icon, style={"fontSize": "22px"}),
            html.Span(hint, style={"fontSize": "9px", "fontWeight": "800", "color": accent,
                                    "fontFamily": "JetBrains Mono, monospace", "letterSpacing": ".1em",
                                    "background": f"{accent}12", "padding": "2px 7px", "borderRadius": "6px"}),
        ]),
        html.Div(id=vid, children="0", style={
            "fontSize": "34px", "fontWeight": "900", "color": TXT, "lineHeight": "1",
            "fontFamily": "JetBrains Mono, monospace", "letterSpacing": "-.04em", "marginBottom": "6px",
        }),
        html.Div(label, style={"fontSize": "11px", "fontWeight": "600", "color": MUT,
                                "textTransform": "uppercase", "letterSpacing": ".08em"}),
    ])

def _panel(title, children, badge=None):
    return html.Div(style={
        "background": PAPER, "border": f"1px solid {BD}", "borderRadius": "16px",
        "overflow": "hidden", "boxShadow": "0 2px 12px rgba(15,30,80,.05)",
    }, children=[
        html.Div(style={
            "padding": "12px 16px", "borderBottom": f"1px solid {BD2}",
            "display": "flex", "justifyContent": "space-between", "alignItems": "center",
            "background": "#fafcff",
        }, children=[
            html.Span(title, style={"fontSize": "11px", "fontWeight": "800", "color": MUT,
                                     "textTransform": "uppercase", "letterSpacing": ".08em"}),
            badge or html.Div(),
        ]),
        html.Div(style={"padding": "16px"}, children=children),
    ])

def _sidebar():
    def _dot():
        return html.Span(className="pulse", style={
            "width": "8px", "height": "8px", "borderRadius": "50%",
            "background": "#22c55e", "display": "inline-block", "flexShrink": "0",
        })

    def _nav(pid, icon, label, sub):
        return html.Button(
            id=f"nav-{pid}", n_clicks=0, className="nav-item",
            style={
                "width": "100%", "border": "1px solid transparent",
                "background": "transparent", "color": "white",
                "padding": "10px 12px", "borderRadius": "12px",
                "display": "flex", "alignItems": "center", "gap": "11px",
                "textAlign": "left", "cursor": "pointer", "marginBottom": "5px",
                "fontFamily": "Inter, sans-serif",
            },
            children=[
                html.Div(icon, style={
                    "width": "34px", "height": "34px", "borderRadius": "10px",
                    "display": "grid", "placeItems": "center",
                    "background": "rgba(255,255,255,.08)", "fontSize": "16px", "flexShrink": "0",
                    "border": "1px solid rgba(255,255,255,.1)",
                }),
                html.Div([
                    html.Div(label, style={"fontWeight": "700", "fontSize": "13px", "lineHeight": "1.2"}),
                    html.Div(sub,   style={"fontSize": "10px", "color": "#8899bb", "marginTop": "2px"}),
                ]),
            ],
        )

    return html.Div(style={
        "width": "248px", "minWidth": "248px", "height": "100vh",
        "background": f"linear-gradient(180deg,{SIDE} 0%,{SIDE2} 60%,#101f40 100%)",
        "display": "flex", "flexDirection": "column",
        "boxShadow": "4px 0 24px rgba(10,20,60,.18)", "zIndex": "10",
    }, children=[

        # Logo
        html.Div(style={"padding": "22px 20px 16px"}, children=[
            html.Div(style={"display": "flex", "alignItems": "center", "gap": "13px"}, children=[
                html.Div("🛡", style={
                    "width": "44px", "height": "44px", "borderRadius": "14px",
                    "display": "grid", "placeItems": "center", "fontSize": "22px",
                    "background": "linear-gradient(135deg,#60a5fa,#2563eb)",
                    "boxShadow": "0 10px 28px rgba(37,99,235,.38)", "flexShrink": "0",
                }),
                html.Div([
                    html.Div("LogGuardian", style={"fontSize": "18px", "fontWeight": "800",
                                                    "color": "white", "letterSpacing": "-.02em"}),
                    html.Div("AIOps Command Center", style={"fontSize": "10px", "color": "#7a8fb5", "marginTop": "2px"}),
                ]),
            ]),
        ]),

        # Divider
        html.Div(style={"height": "1px", "background": "rgba(255,255,255,.08)", "margin": "0 18px 14px"}),

        # Nav
        html.Div("NAVIGATION", style={
            "fontSize": "9px", "fontWeight": "800", "color": "#3a4f72",
            "letterSpacing": ".14em", "padding": "0 18px 10px",
            "fontFamily": "JetBrains Mono, monospace",
        }),
        html.Div(style={"padding": "0 12px", "flex": "1"}, children=[
            _nav("dashboard", "▦", "Vue cockpit",    "KPIs · graphes · risk"),
            _nav("logs",      "≡", "Flux logs",       "Recherche · RAG · feedback"),
            _nav("alerts",    "⚠", "Incident board",  "Anomalies critiques"),
        ]),

        # Kafka status
        html.Div(style={
            "margin": "0 14px 16px",
            "padding": "12px 14px",
            "background": "rgba(255,255,255,.04)",
            "border": "1px solid rgba(255,255,255,.07)",
            "borderRadius": "12px",
        }, children=[
            html.Div(style={"display": "flex", "alignItems": "center", "gap": "8px", "marginBottom": "6px"}, children=[
                _dot(),
                html.Span("Kafka connecté", style={"fontSize": "12px", "fontWeight": "700", "color": "#a0bce0"}),
            ]),
            html.Div(KAFKA_TOPIC, style={
                "fontSize": "11px", "color": "#516080",
                "fontFamily": "JetBrains Mono, monospace",
                "overflow": "hidden", "textOverflow": "ellipsis", "whiteSpace": "nowrap",
            }),
            html.Div(style={"marginTop": "8px", "display": "flex", "justifyContent": "space-between"}, children=[
                html.Span("refresh", style={"fontSize": "10px", "color": "#3a4f72"}),
                html.Span(f"{REFRESH_INTERVAL_MS//1000}s", style={
                    "fontSize": "11px", "fontWeight": "700", "color": "#60a5fa",
                    "fontFamily": "JetBrains Mono, monospace",
                }),
            ]),
        ]),
    ])

def _topbar(title, subtitle):
    return html.Div(className="topbar-glass", style={
        "height": "68px", "display": "flex", "alignItems": "center",
        "justifyContent": "space-between", "padding": "0 28px",
        "position": "sticky", "top": "0", "zIndex": "5", "flexShrink": "0",
    }, children=[
        html.Div([
            html.H1(title, style={
                "margin": "0", "fontSize": "21px", "fontWeight": "800",
                "color": TXT, "letterSpacing": "-.03em",
            }),
            html.Div(subtitle, style={"fontSize": "12px", "color": MUT, "marginTop": "3px"}),
        ]),
        html.Div(style={"display": "flex", "alignItems": "center", "gap": "10px"}, children=[
            html.Div(className="pill", style={
                "color": GREEN, "background": "#f0fdf4", "border": f"1px solid #bbf7d0",
            }, children=[
                html.Span(style={"width": "6px", "height": "6px", "borderRadius": "50%",
                                  "background": GREEN, "display": "inline-block"}, className="pulse"),
                html.Span("LIVE", style={"fontSize": "10px", "fontWeight": "800",
                                          "letterSpacing": ".1em", "color": GREEN}),
            ]),
            html.Div(id="clock", style={
                "fontFamily": "JetBrains Mono, monospace", "fontSize": "12px",
                "color": MUT, "background": BD2, "border": f"1px solid {BD}",
                "padding": "7px 13px", "borderRadius": "10px",
            }),
        ]),
    ])

# ─── DASHBOARD PAGE ───────────────────────────────────────────────────────────
def _ai_briefing(rows):
    if not rows:
        return html.Div("Aucun événement reçu.", style={"color": MUT, "fontSize": "13px", "padding": "4px 0"})
    recent  = rows[:80]
    anom    = [r for r in recent if r.get("Statut") == "ANOMALIE"]
    top_src = Counter(r.get("Source", "?") for r in anom).most_common(1)
    src_txt = top_src[0][0] if top_src else "—"
    risk    = "ÉLEVÉ" if len(anom) > 20 else "MOYEN" if len(anom) > 5 else "FAIBLE"
    rc      = RED if risk == "ÉLEVÉ" else ORAN if risk == "MOYEN" else GREEN
    return html.Div(className="slide-in", style={"display": "flex", "flexDirection": "column", "gap": "12px"}, children=[
        html.Div([
            html.Div("Synthèse automatique", style={"fontWeight": "800", "fontSize": "13px", "color": TXT, "marginBottom": "2px"}),
            html.Div(f"{len(recent)} événements récents analysés", style={"fontSize": "12px", "color": MUT}),
        ]),
        html.Div(style={
            "padding": "12px 14px", "borderRadius": "12px",
            "background": f"{rc}0d", "border": f"1px solid {rc}25",
            "borderLeft": f"3px solid {rc}",
        }, children=[
            html.Div(f"Risque global : {risk}", style={"fontWeight": "800", "color": rc, "fontSize": "13px", "marginBottom": "8px"}),
            html.Div(f"Anomalies : {len(anom)}", style={"fontSize": "12px", "color": MUT, "marginBottom": "3px"}),
            html.Div(f"Source critique : {src_txt}", style={"fontSize": "12px", "color": MUT}),
        ]),
        html.Div(
            "Recommandation : prioriser les logs à ratio élevé, inspecter les services impactés "
            "et utiliser le feedback RAG pour améliorer les explications.",
            style={"fontSize": "12px", "lineHeight": "1.65", "color": MUT},
        ),
    ])

def _dashboard_page():
    return html.Div(id="page-dashboard", style={"display": "none", "height": "100%", "overflow": "auto"}, children=[
        _topbar("Cockpit observabilité", "Vue temps réel — anomalies, risque et santé des services"),
        html.Div(style={"padding": "22px", "display": "grid", "gap": "18px"}, children=[

            # KPIs
            html.Div(style={"display": "grid", "gridTemplateColumns": "repeat(5, minmax(0, 1fr))", "gap": "14px"}, children=[
                _kpi("logs reçus",       "m-total",   BLUE,  "📥", "STREAM"),
                _kpi("anomalies",        "m-anom",    RED,   "🔥", f"> {ALERT_THRESHOLD}x"),
                _kpi("score moyen",      "m-score",   PURP,  "🧠", "ML"),
                _kpi("sources touchées", "m-sources", CYAN,  "🖥", "SVC"),
                _kpi("risk level",       "m-risk",    ORAN,  "⚡", "AIOPS"),
            ]),

            # Row 2
            html.Div(style={"display": "grid", "gridTemplateColumns": "1.4fr .6fr", "gap": "18px"}, children=[
                _panel("Flux anomalies / normal", dcc.Graph(id="fig-stream", config={"displayModeBar": False})),
                _panel("Jauge risque global",     dcc.Graph(id="fig-risk",   config={"displayModeBar": False})),
            ]),

            # Row 3
            html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr 1fr", "gap": "18px"}, children=[
                _panel("Top services impactés",       dcc.Graph(id="fig-services", config={"displayModeBar": False})),
                _panel("Score IA — derniers events",  dcc.Graph(id="fig-score",    config={"displayModeBar": False})),
                _panel("Briefing analyste IA",        html.Div(id="ai-briefing", style={"minHeight": "220px"})),
            ]),
        ]),
    ])

# ─── LOGS PAGE ────────────────────────────────────────────────────────────────
def _filter_bar():
    """Barre de filtres totalement fixe — height + minHeight sur chaque item."""
    inp_style = {
        "height": "40px", "width": "100%",
        "border": f"1.5px solid {BD}", "borderRadius": "10px",
        "padding": "0 14px", "fontSize": "13px", "fontFamily": "Inter, sans-serif",
        "background": PAPER, "color": TXT, "outline": "none",
        "transition": "border-color .15s, box-shadow .15s",
    }
    dd_style = {"fontSize": "13px"}

    return html.Div(style={
        # grille stricte, largeurs figées, pas de flex-wrap
        "display":               "grid",
        "gridTemplateColumns":   "1fr 160px 180px 110px",
        "gap":                   "10px",
        "alignItems":            "center",
        "width":                 "100%",
        "padding":               "0",
        # hauteur fixe pour que ça ne bouge jamais
        "height":                "40px",
    }, children=[
        dcc.Input(
            id="search-text", type="text", debounce=False,
            placeholder="Rechercher : ssh, timeout, error, kerberos…",
            className="lg-search", style=inp_style,
        ),
        dcc.Dropdown(id="source-filter", placeholder="Source", clearable=True,
                     className="dash-dropdown", style=dd_style),
        dcc.Dropdown(id="level-filter",  value="all", clearable=False,
                     className="dash-dropdown", style=dd_style,
                     options=[
                         {"label": "Tous",                        "value": "all"},
                         {"label": f"Anomalies > {ALERT_THRESHOLD}x", "value": "high"},
                         {"label": f"Normaux ≤ {ALERT_THRESHOLD}x",   "value": "normal"},
                     ]),
        dcc.Dropdown(id="limit-filter", value=200, clearable=False,
                     className="dash-dropdown", style=dd_style,
                     options=[
                         {"label": "200",  "value": 200},
                         {"label": "500",  "value": 500},
                         {"label": "1 000","value": 1000},
                         {"label": "Max",  "value": MAX_ROWS},
                     ]),
    ])

def _rag_block(title, cid, accent=CYAN):
    return html.Div(style={"marginBottom": "12px"}, children=[
        html.Div(title, style={
            "fontSize": "10px", "fontWeight": "800", "color": MUT2,
            "textTransform": "uppercase", "letterSpacing": ".09em", "marginBottom": "6px",
            "fontFamily": "JetBrains Mono, monospace",
        }),
        html.Div(id=cid, style={
            "background": "rgba(255,255,255,.06)",
            "border": "1px solid rgba(255,255,255,.12)",
            "borderLeft": f"3px solid {accent}",
            "borderRadius": "12px", "padding": "11px 14px",
            "fontSize": "13px", "lineHeight": "1.6", "color": "#d1ddf5",
        }, children="Sélectionnez un log."),
    ])

def _rag_side_panel():
    return html.Div(style={"height": "100%", "overflow": "auto"}, children=[
        html.Div(style={
            "background": "linear-gradient(170deg,#0d1526 0%,#0f1e3c 55%,#121d36 100%)",
            "borderRadius": "18px", "padding": "20px 18px",
            "color": "white", "minHeight": "100%",
            "boxShadow": "0 16px 48px rgba(10,20,60,.22)",
        }, children=[

            # Header
            html.Div(style={
                "display": "flex", "justifyContent": "space-between",
                "alignItems": "flex-start", "marginBottom": "18px",
            }, children=[
                html.Div([
                    html.Div(style={"display": "flex", "alignItems": "center", "gap": "8px", "marginBottom": "3px"}, children=[
                        html.Span("⚡", style={"fontSize": "18px"}),
                        html.Div("Analyste IA", style={"fontSize": "17px", "fontWeight": "900"}),
                    ]),
                    html.Div("RAG · recommandation · feedback", style={"fontSize": "11px", "color": "#7a8fb5"}),
                ]),
                html.Div(id="rag-status-chip"),
            ]),

            # Score + Ratio cards
            html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "10px", "marginBottom": "16px"}, children=[
                html.Div(style={
                    "background": "rgba(255,255,255,.06)", "border": "1px solid rgba(255,255,255,.1)",
                    "borderRadius": "14px", "padding": "13px",
                }, children=[
                    html.Div("Score IA", style={"fontSize": "10px", "color": "#7a8fb5", "fontWeight": "800",
                                                 "textTransform": "uppercase", "letterSpacing": ".08em", "marginBottom": "6px"}),
                    html.Div(id="rag-score", children="—", style={
                        "fontSize": "26px", "fontWeight": "900",
                        "fontFamily": "JetBrains Mono, monospace", "color": "#c4b5fd",
                    }),
                ]),
                html.Div(style={
                    "background": "rgba(255,255,255,.06)", "border": "1px solid rgba(255,255,255,.1)",
                    "borderRadius": "14px", "padding": "13px",
                }, children=[
                    html.Div("Ratio", style={"fontSize": "10px", "color": "#7a8fb5", "fontWeight": "800",
                                              "textTransform": "uppercase", "letterSpacing": ".08em", "marginBottom": "6px"}),
                    html.Div(id="rag-ratio", children="—", style={
                        "fontSize": "26px", "fontWeight": "900",
                        "fontFamily": "JetBrains Mono, monospace", "color": "#fdba74",
                    }),
                ]),
            ]),

            _rag_block("Log sélectionné",  "rag-desc",     "#38bdf8"),
            _rag_block("Analyse RAG",       "rag-analysis", "#a78bfa"),
            _rag_block("Action recommandée","rag-action",   "#34d399"),

            # Feedback
            html.Div(style={"paddingTop": "14px", "borderTop": "1px solid rgba(255,255,255,.1)"}, children=[
                html.Div("Feedback utilisateur", style={
                    "fontSize": "10px", "fontWeight": "800", "color": "#7a8fb5",
                    "textTransform": "uppercase", "letterSpacing": ".09em", "marginBottom": "10px",
                    "fontFamily": "JetBrains Mono, monospace",
                }),
                html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "10px"}, children=[
                    html.Button("👍 Utile", id="fb-up", n_clicks=0, style={
                        "height": "38px", "borderRadius": "10px", "cursor": "pointer",
                        "border": "1px solid rgba(52,211,153,.4)",
                        "background": "rgba(52,211,153,.1)", "color": "#6ee7b7",
                        "fontWeight": "800", "fontSize": "12px", "fontFamily": "Inter, sans-serif",
                    }),
                    html.Button("👎 Pas utile", id="fb-down", n_clicks=0, style={
                        "height": "38px", "borderRadius": "10px", "cursor": "pointer",
                        "border": "1px solid rgba(248,113,113,.4)",
                        "background": "rgba(248,113,113,.1)", "color": "#fca5a5",
                        "fontWeight": "800", "fontSize": "12px", "fontFamily": "Inter, sans-serif",
                    }),
                ]),
                html.Div(id="feedback-status", style={"fontSize": "12px", "color": "#7a8fb5", "marginTop": "10px", "lineHeight": "1.5"},
                         children="Le feedback sera sauvegardé pour évaluer le RAG."),
            ]),
        ]),
    ])

def _logs_page():
    return html.Div(id="page-logs", style={"display": "block", "height": "100%", "overflow": "hidden"}, children=[
        _topbar("Flux logs augmenté", "Table temps réel · analyste IA · recommandation · feedback"),

        html.Div(style={
            "height": "calc(100% - 68px)",
            "display": "grid",
            "gridTemplateColumns": "minmax(0, 1fr) 360px",
            "gap": "0",
            "overflow": "hidden",
        }, children=[

            # ── Left : filtres + table ───────────────────────────────────
            html.Div(style={
                "display": "flex", "flexDirection": "column",
                "minWidth": "0", "height": "100%", "overflow": "hidden",
                "borderRight": f"1px solid {BD}",
            }, children=[

                # Barre de filtres — FIXE : padding constant, pas de panel wrapper
                html.Div(style={
                    "flexShrink": "0",                  # ne rétrécit jamais
                    "height": "68px",                   # hauteur figée
                    "minHeight": "68px",
                    "padding": "14px 18px",
                    "background": "#fafcff",
                    "borderBottom": f"1px solid {BD2}",
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "0",
                }, children=[
                    html.Div(style={"flex": "1", "minWidth": "0"}, children=[_filter_bar()]),
                    html.Div(id="logs-count", style={
                        "marginLeft": "16px", "fontSize": "12px", "fontWeight": "700",
                        "color": MUT2, "whiteSpace": "nowrap", "flexShrink": "0",
                        "fontFamily": "JetBrains Mono, monospace",
                    }),
                ]),

                # Table
                html.Div(style={"flex": "1", "minHeight": "0", "overflow": "auto"}, children=[
                    dash_table.DataTable(
                        id="main-table",
                        columns=[{"name": c, "id": c} for c in TABLE_COLS],
                        data=[],
                        cell_selectable=True,
                        active_cell=None,
                        sort_action="native",
                        page_action="native",
                        page_size=22,
                        fixed_rows={"headers": True},
                        style_table={
                            "height": "100%", "overflowY": "auto", "overflowX": "auto",
                            "borderRadius": "0",
                        },
                        style_header={
                            "backgroundColor": "#f0f4fa",
                            "fontWeight": "800", "fontSize": "10px", "color": MUT,
                            "border": "none", "borderBottom": f"2px solid {BD}",
                            "padding": "10px 14px", "textTransform": "uppercase",
                            "letterSpacing": ".07em", "fontFamily": "JetBrains Mono, monospace",
                        },
                        style_cell={
                            "backgroundColor": PAPER, "fontSize": "12px", "color": TXT,
                            "border": "none", "borderBottom": f"1px solid {BD2}",
                            "padding": "9px 14px", "fontFamily": "Inter, sans-serif",
                            "whiteSpace": "nowrap", "overflow": "hidden", "textOverflow": "ellipsis",
                        },
                        style_cell_conditional=[
                            {"if": {"column_id": "Message"},  "minWidth": "380px", "whiteSpace": "normal", "lineHeight": "1.5"},
                            {"if": {"column_id": "Timestamp"},"fontFamily": "JetBrains Mono, monospace", "color": MUT, "minWidth": "148px", "fontSize": "11px"},
                            {"if": {"column_id": "Source"},   "fontFamily": "JetBrains Mono, monospace", "fontWeight": "700", "color": BLUE, "minWidth": "110px"},
                            {"if": {"column_id": "Host"},     "fontFamily": "JetBrains Mono, monospace", "fontSize": "11px", "color": MUT, "minWidth": "100px"},
                            {"if": {"column_id": "Score IA"}, "fontFamily": "JetBrains Mono, monospace", "fontWeight": "800", "textAlign": "right", "minWidth": "80px"},
                            {"if": {"column_id": "Ratio"},    "fontFamily": "JetBrains Mono, monospace", "fontWeight": "800", "textAlign": "center", "minWidth": "75px"},
                            {"if": {"column_id": "Statut"},   "fontWeight": "900", "textAlign": "center", "minWidth": "100px", "fontSize": "11px", "letterSpacing": ".04em"},
                        ],
                        style_data_conditional=[
                            {"if": {"row_index": "odd"},  "backgroundColor": "#f8faff"},
                            {"if": {"filter_query": '{Statut} = "ANOMALIE"', "column_id": "Statut"},  "color": RED},
                            {"if": {"filter_query": '{Statut} = "NORMAL"',   "column_id": "Statut"},  "color": GREEN},
                            {"if": {"filter_query": '{Statut} = "ANOMALIE"', "column_id": "Score IA"},"color": RED,  "fontWeight": "800"},
                            {"if": {"filter_query": '{Statut} = "ANOMALIE"', "column_id": "Ratio"},   "color": ORAN, "fontWeight": "800"},
                            {"if": {"state": "selected"}, "backgroundColor": "#dbeafe", "border": f"1px solid {BLUE}"},
                        ],
                    ),
                ]),
            ]),

            # ── Right : RAG panel ────────────────────────────────────────
            html.Div(style={"height": "100%", "overflow": "auto", "padding": "14px"}, children=[
                _rag_side_panel(),
            ]),
        ]),
    ])

# ─── ALERTS PAGE ──────────────────────────────────────────────────────────────
def _metric_card(label, vid, accent, icon, hint=""):
    """Version allégée pour la page alertes."""
    return html.Div(className="card-hover", style={
        "background": PAPER, "border": f"1px solid {BD}", "borderRadius": "16px",
        "padding": "16px 18px", "boxShadow": "0 2px 12px rgba(15,30,80,.05)",
        "borderTop": f"3px solid {accent}",
    }, children=[
        html.Div(style={"display": "flex", "justifyContent": "space-between", "marginBottom": "10px"}, children=[
            html.Span(icon, style={"fontSize": "20px"}),
            html.Span(hint, style={"fontSize": "9px", "fontWeight": "800", "color": accent,
                                    "fontFamily": "JetBrains Mono, monospace", "letterSpacing": ".1em"}),
        ]),
        html.Div(id=vid, children="—", style={
            "fontSize": "28px", "fontWeight": "900", "color": TXT,
            "fontFamily": "JetBrains Mono, monospace", "letterSpacing": "-.03em",
        }),
        html.Div(label, style={"fontSize": "11px", "fontWeight": "600", "color": MUT,
                                "textTransform": "uppercase", "letterSpacing": ".07em", "marginTop": "4px"}),
    ])

def _alerts_page():
    return html.Div(id="page-alerts", style={"display": "none", "height": "100%", "overflow": "auto"}, children=[
        _topbar("Incident board", f"Événements dont le ratio dépasse {ALERT_THRESHOLD}x"),
        html.Div(style={"padding": "22px", "display": "grid", "gap": "18px"}, children=[
            html.Div(id="alert-strip"),
            html.Div(style={
                "background": PAPER, "border": f"1px solid {BD}",
                "borderRadius": "16px", "overflow": "hidden",
                "boxShadow": "0 2px 12px rgba(15,30,80,.05)",
            }, children=[
                dash_table.DataTable(
                    id="alert-table", columns=[{"name": c, "id": c} for c in ALERT_COLS], data=[],
                    sort_action="native", page_action="native", page_size=30,
                    style_table={"overflowX": "auto"},
                    style_header={
                        "backgroundColor": "#fff1f2", "fontWeight": "800", "fontSize": "10px",
                        "color": RED, "border": "none", "borderBottom": f"2px solid #fecaca",
                        "padding": "11px 14px", "textTransform": "uppercase", "letterSpacing": ".07em",
                        "fontFamily": "JetBrains Mono, monospace",
                    },
                    style_cell={
                        "backgroundColor": PAPER, "fontSize": "12px", "color": TXT,
                        "border": "none", "borderBottom": f"1px solid {BD2}",
                        "padding": "10px 14px", "fontFamily": "Inter, sans-serif",
                    },
                    style_cell_conditional=[
                        {"if": {"column_id": "Message"}, "minWidth": "480px", "whiteSpace": "normal"},
                        {"if": {"column_id": "Ratio"},   "color": RED,  "fontWeight": "900", "fontFamily": "JetBrains Mono, monospace"},
                        {"if": {"column_id": "Source"},  "color": BLUE, "fontWeight": "700", "fontFamily": "JetBrains Mono, monospace"},
                    ],
                    style_data_conditional=[
                        {"if": {"row_index": "odd"}, "backgroundColor": "#fff8f8"},
                    ],
                ),
            ]),
        ]),
    ])

# ─── ROOT LAYOUT ──────────────────────────────────────────────────────────────
app.layout = html.Div(style={"height": "100vh", "display": "flex", "background": BG, "overflow": "hidden"}, children=[
    _sidebar(),
    html.Div(style={"flex": "1", "minWidth": "0", "height": "100vh", "overflow": "hidden", "display": "flex", "flexDirection": "column"}, children=[
        _dashboard_page(),
        _logs_page(),
        _alerts_page(),
    ]),
    dcc.Interval(id="interval", interval=REFRESH_INTERVAL_MS, n_intervals=0),
    dcc.Store(id="current-page",       data="logs"),
    dcc.Store(id="rows-store",         data=[]),
    dcc.Store(id="selected-log-store", data=None),
])

# ─── DATA HELPERS ─────────────────────────────────────────────────────────────
def _visible_rows():
    with _lock: return list(_buffer), _total_received

def _filter_rows(rows, search, source, level, limit):
    search = (search or "").lower().strip()
    level  = level or "all"
    limit  = int(limit or 200)
    out    = []
    for r in rows:
        if search:
            hay = " ".join([r.get("Timestamp",""), r.get("Source",""),
                             r.get("Host",""), r.get("Message",""), r.get("Statut","")]).lower()
            if search not in hay: continue
        if source and r.get("Source") != source: continue
        if level == "high"   and r.get("_ratio_val", 0) <= ALERT_THRESHOLD: continue
        if level == "normal" and r.get("_ratio_val", 0) >  ALERT_THRESHOLD: continue
        out.append(r)
    return out[:limit]

def _display_row(r, include_model=False):
    cols = TABLE_COLS + (["Model"] if include_model else [])
    d = {k: r.get(k, "") for k in cols}
    d["id"] = r.get("id")
    return d

def _badge(text, color, bg=None):
    return html.Span(text, style={
        "display": "inline-flex", "alignItems": "center", "gap": "5px",
        "padding": "3px 9px", "borderRadius": "999px", "fontSize": "11px",
        "fontWeight": "800", "color": color, "backgroundColor": bg or f"{color}18",
        "fontFamily": "JetBrains Mono, monospace", "letterSpacing": ".04em",
    })

def _rag_for(row):
    if not row:
        return {"desc":"Sélectionnez un log.","score":"—","ratio":"—",
                "status":_badge("Aucun log","#93c5fd","rgba(147,197,253,.13)"),
                "analysis":"L'analyse RAG apparaîtra ici.","action":"Aucune action."}
    is_anom = row.get("Statut") == "ANOMALIE"
    score   = row.get("Score IA", "0.00")
    ratio   = row.get("Ratio",    "0.00x")
    src     = row.get("Source",   "unknown")
    host    = row.get("Host",     "unknown")
    if is_anom:
        analysis = (f"Le modèle classe ce log comme anomalie car son ratio ({ratio}) "
                    f"dépasse le seuil configuré ({ALERT_THRESHOLD:.1f}x). Le service {src} "
                    f"sur {host} présente un comportement inhabituel.")
        action   = ("Inspecter le service, corréler avec les logs voisins, "
                    "vérifier les accès récents et redémarrer le pod si nécessaire.")
        status   = _badge("ANOMALIE", "#fca5a5", "rgba(220,38,38,.2)")
    else:
        analysis = (f"Comportement nominal : ratio {ratio} sous le seuil. "
                    f"Score IA {score} dans la plage normale pour {src}.")
        action   = "Aucune action immédiate. Conserver pour l'analyse historique."
        status   = _badge("NORMAL", "#6ee7b7", "rgba(5,150,105,.18)")
    return {"desc":row.get("Message","—"),"score":str(score),"ratio":str(ratio),
            "status":status,"analysis":analysis,"action":action}

def _generate_alternative_rag(
    row,
    rejected_analysis,
    rejected_action,
):
    if not GROQ_API_KEY:
        raise RuntimeError(
            "GROQ_API_KEY n'est pas configurée."
        )

    client = Groq(api_key=GROQ_API_KEY)

    response = client.chat.completions.create(
        model=GROQ_MODEL,
        temperature=0.2,
        max_completion_tokens=1200,
        reasoning_effort="low",
        include_reasoning=False,
        messages=[
            {
                "role": "system",
                "content": (
                    "Tu es un analyste AIOps spécialisé dans "
                    "l'analyse de logs. Tu dois proposer une "
                    "explication claire, prudente et différente "
                    "de la réponse rejetée. N'invente jamais "
                    "d'attaque ou de vulnérabilité absente du log."
                ),
            },
            {
                "role": "user",
                "content": f"""
Un utilisateur a jugé la réponse précédente inutile.

LOG
Message : {row.get("Message", "inconnu")}
Source : {row.get("Source", "inconnue")}
Host : {row.get("Host", "inconnu")}
Score IA : {row.get("Score IA", "inconnu")}
Ratio : {row.get("Ratio", "inconnu")}
Statut : {row.get("Statut", "inconnu")}
Seuil : {ALERT_THRESHOLD}

RÉPONSE REJETÉE
Analyse :
{rejected_analysis}

Recommandation :
{rejected_action}

Produis une nouvelle analyse réellement différente.

Contraintes :
- répondre en français ;
- expliquer le log sans inventer d'informations ;
- tenir compte du score, du ratio et du seuil ;
- proposer une action concrète et proportionnée ;
- ne pas recommander systématiquement un redémarrage ;
- analyse de 4 phrases maximum ;
- recommandation de 3 phrases maximum.
""",
            },
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "alternative_rag",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": {
                        "analysis": {
                            "type": "string",
                        },
                        "action": {
                            "type": "string",
                        },
                    },
                    "required": [
                        "analysis",
                        "action",
                    ],
                    "additionalProperties": False,
                },
            },
        },
    )

    content = response.choices[0].message.content

    if not content:
        raise RuntimeError(
            "Groq a retourné une réponse vide."
        )

    result = json.loads(content)

    return {
        "analysis": result["analysis"].strip(),
        "action": result["action"].strip(),
    }

# ─── CALLBACKS ────────────────────────────────────────────────────────────────
@app.callback(
    Output("current-page","data"),
    Input("nav-dashboard","n_clicks"), Input("nav-logs","n_clicks"), Input("nav-alerts","n_clicks"),
    State("current-page","data"), prevent_initial_call=True,
)
def navigate(a, b, c, current):
    ctx = callback_context
    if not ctx.triggered: return current
    return {"nav-dashboard":"dashboard","nav-logs":"logs","nav-alerts":"alerts"}.get(
        ctx.triggered[0]["prop_id"].split(".")[0], current)

@app.callback(
    Output("page-dashboard","style"), Output("page-logs","style"), Output("page-alerts","style"),
    Input("current-page","data"),
)
def show_page(page):
    on   = {"display":"block","height":"100%","overflow":"auto"}
    logs = {"display":"block","height":"100%","overflow":"hidden"}
    off  = {"display":"none"}
    return (on if page=="dashboard" else off,
            logs if page=="logs" else off,
            on if page=="alerts" else off)

@app.callback(Output("clock","children"), Input("interval","n_intervals"))
def tick(_): return datetime.now().strftime("%A %d %B %Y  •  %H:%M:%S").upper()

@app.callback(
    Output("m-total","children"), Output("m-anom","children"),
    Output("m-score","children"), Output("m-sources","children"), Output("m-risk","children"),
    Output("fig-stream","figure"), Output("fig-risk","figure"),
    Output("fig-services","figure"), Output("fig-score","figure"),
    Output("ai-briefing","children"),
    Input("interval","n_intervals"),
)
def update_dashboard(_):
    rows, total = _visible_rows()
    anom    = [r for r in rows if r.get("Statut")=="ANOMALIE"]
    avg     = sum(r.get("_score_val",0) for r in rows)/max(1,len(rows))
    sources = len({r.get("Source") for r in anom if r.get("Source")})
    risk    = round(len(anom)/max(1,len(rows))*100,1) if rows else 0
    return (str(total), str(len(anom)), f"{avg:.2f}", str(sources), f"{risk}%",
            _stream_fig(rows), _risk_fig(rows), _services_fig(rows), _score_fig(rows),
            _ai_briefing(rows))
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
    rows, _t = _visible_rows()

    filtered = _filter_rows(
        rows,
        search,
        source,
        level or "all",
        limit or 200,
    )

    opts = [
        {
            "label": s,
            "value": s,
        }
        for s in sorted({
            r.get("Source")
            for r in rows
            if r.get("Source")
        })
    ]

    table = [
        {
            "id": r.get("id"),
            "Timestamp": r.get("Timestamp", ""),
            "Source": r.get("Source", ""),
            "Host": r.get("Host", ""),
            "Message": r.get("Message", ""),
            "Score IA": r.get("Score IA", ""),
            "Ratio": r.get("Ratio", ""),
            "Statut": r.get("Statut", ""),
            "Model": r.get("Model", "unknown"),
        }
        for r in filtered
    ]

    return (
        table,
        opts,
        table,
        f"{len(filtered)} / {len(rows)} logs",
    )

# @app.callback(
#     Output("main-table","data"), Output("source-filter","options"),
#     Output("rows-store","data"), Output("logs-count","children"),
#     Input("interval","n_intervals"), Input("current-page","data"),
#     Input("search-text","value"), Input("source-filter","value"),
#     Input("level-filter","value"), Input("limit-filter","value"),
# )
# def update_logs(_, page, search, source, level, limit):
#     rows, _t = _visible_rows()
#     filtered = _filter_rows(rows, search, source, level or "all", limit or 200)
#     opts     = [{"label":s,"value":s} for s in sorted({r.get("Source") for r in rows if r.get("Source")})]
#     table = [
#     {
#         "id": r.get("id"),
#         "Timestamp": r.get("Timestamp", ""),
#         "Source": r.get("Source", ""),
#         "Host": r.get("Host", ""),
#         "Message": r.get("Message", ""),
#         "Score IA": r.get("Score IA", ""),
#         "Ratio": r.get("Ratio", ""),
#         "Statut": r.get("Statut", ""),

#         # Conservé dans les données sans être affiché
#         "Model": r.get("Model", "unknown"),
#     }
#     for r in filtered]
#     # table    = [{"id":r.get("id"),"Timestamp":r.get("Timestamp",""),"Source":r.get("Source",""),
#     #               "Host":r.get("Host",""),"Message":r.get("Message",""),
#     #               "Score IA":r.get("Score IA",""),"Ratio":r.get("Ratio",""),"Statut":r.get("Statut","")}
#     #              for r in filtered]
#     return table, opts, table, f"{len(filtered)} / {len(rows)} logs"

@app.callback(
    Output("selected-log-store","data"),
    Input("main-table","active_cell"),
    State("main-table","data"), prevent_initial_call=True,
)
def store_selected(active_cell, table_data):
    if not active_cell or not table_data: return None
    idx = active_cell.get("row")
    return table_data[idx] if (idx is not None and idx < len(table_data)) else None

@app.callback(
    Output("rag-desc","children"), Output("rag-score","children"), Output("rag-ratio","children"),
    Output("rag-status-chip","children"), Output("rag-analysis","children"),
    Output("rag-action","children"), Output("feedback-status","children"),
    Input("selected-log-store","data"),
)
def render_rag(row):
    r = _rag_for(row)
    return r["desc"],r["score"],r["ratio"],r["status"],r["analysis"],r["action"],"Le feedback sera sauvegardé."

@app.callback(
    Output(
        "rag-analysis",
        "children",
        allow_duplicate=True,
    ),
    Output(
        "rag-action",
        "children",
        allow_duplicate=True,
    ),
    Output(
        "feedback-status",
        "children",
        allow_duplicate=True,
    ),
    Input("fb-up", "n_clicks"),
    Input("fb-down", "n_clicks"),
    State("selected-log-store", "data"),
    State("rag-analysis", "children"),
    State("rag-action", "children"),
    prevent_initial_call=True,
)
def save_feedback(
    up,
    down,
    row,
    current_analysis,
    current_action,
):
    if not row:
        return (
            no_update,
            no_update,
            "Sélectionne d'abord un log.",
        )

    ctx = callback_context

    if not ctx.triggered:
        raise PreventUpdate

    trigger = (
        ctx.triggered[0]["prop_id"]
        .split(".")[0]
    )

    # Pouce positif :
    # on garde la réponse actuellement affichée.
    # Aucun appel Groq et aucun enregistrement JSON.
    if trigger == "fb-up":
        positive_record = {
            "feedback_timestamp": datetime.now().isoformat(
                timespec="seconds"
            ),
            "feedback": "positive",
            "accepted": True,
            "log_id": row.get("id"),
            "model_version": row.get(
                "Model",
                "unknown",
            ),
            "accepted_rag_analysis": (
                str(current_analysis).strip()
                if current_analysis
                else None
            ),
            "accepted_rag_recommendation": (
                str(current_action).strip()
                if current_action
                else None
            ),
        }

        try:
            feedback_directory = os.path.dirname(
                FEEDBACK_PATH
            )

            if feedback_directory:
                os.makedirs(
                    feedback_directory,
                    exist_ok=True,
                )

            with open(
                FEEDBACK_PATH,
                "a",
                encoding="utf-8",
            ) as file:
                file.write(
                    json.dumps(
                        positive_record,
                        ensure_ascii=False,
                    )
                    + "\n"
                )

            return (
                no_update,
                no_update,
                "✅ Explication validée et acceptation enregistrée.",
            )

        except Exception as error:
            log.exception(
                "Erreur pendant l'enregistrement "
                "du feedback positif"
            )

            return (
                no_update,
                no_update,
                f"⚠️ Validation non sauvegardée : {error}",
            )
    # if trigger == "fb-up":
    #     return (
    #         no_update,
    #         no_update,
    #         "✅ Explication validée.",
    #     )

    if trigger != "fb-down":
        raise PreventUpdate

    initial_rag = _rag_for(row)

    # La réponse actuellement affichée devient la réponse rejetée.
    # Au premier clic, c'est la réponse initiale.
    # Aux clics suivants, c'est la dernière réponse Groq.
    rejected_analysis = (
        str(current_analysis).strip()
        if current_analysis
        else initial_rag["analysis"]
    )

    rejected_action = (
        str(current_action).strip()
        if current_action
        else initial_rag["action"]
    )

    record = {
        "feedback_timestamp": datetime.now().isoformat(
            timespec="seconds"
        ),
        "feedback": "negative",

        # Informations du log
        "log_id": row.get("id"),
        "source": row.get("Source"),
        "host": row.get("Host"),
        "message": row.get("Message"),
        "score_ia": row.get("Score IA"),
        "ratio": row.get("Ratio"),
        "statut": row.get("Statut"),
        "model_version": row.get(
            "Model",
            "unknown",
        ),

        # Réponse que l'utilisateur vient de rejeter
        "rag_analysis": rejected_analysis,
        "rag_recommendation": rejected_action,

        # Versions
        "rag_version": "rag_v1",
        "prompt_version": "prompt_v1",
        "regeneration_prompt_version": "groq_regeneration_v1",
        "alert_threshold": ALERT_THRESHOLD,
    }

    alternative = None

    # Appel Groq uniquement après un pouce négatif
    try:
        alternative = _generate_alternative_rag(
            row=row,
            rejected_analysis=rejected_analysis,
            rejected_action=rejected_action,
        )

        record["replacement_rag_analysis"] = (
            alternative["analysis"]
        )
        record["replacement_rag_recommendation"] = (
            alternative["action"]
        )
        record["replacement_generator"] = GROQ_MODEL
        record["groq_success"] = True
        record["groq_error"] = None

    except Exception as error:
        log.exception(
            "Erreur pendant la génération Groq"
        )

        record["replacement_rag_analysis"] = None
        record["replacement_rag_recommendation"] = None
        record["replacement_generator"] = GROQ_MODEL
        record["groq_success"] = False
        record["groq_error"] = str(error)

    # Un seul enregistrement JSON par clic négatif
    try:
        feedback_directory = os.path.dirname(
            FEEDBACK_PATH
        )

        if feedback_directory:
            os.makedirs(
                feedback_directory,
                exist_ok=True,
            )

        with open(
            FEEDBACK_PATH,
            "a",
            encoding="utf-8",
        ) as file:
            file.write(
                json.dumps(
                    record,
                    ensure_ascii=False,
                )
                + "\n"
            )

    except Exception as error:
        log.exception(
            "Erreur pendant l'enregistrement "
            "du feedback négatif"
        )

        return (
            no_update,
            no_update,
            f"⚠️ Feedback non sauvegardé : {error}",
        )

    # L'appel Groq a échoué, mais le feedback est conservé.
    if alternative is None:
        return (
            no_update,
            no_update,
            (
                "👎 Feedback enregistré, mais la nouvelle "
                "analyse n'a pas pu être générée : "
                f"{record['groq_error']}"
            ),
        )

    # Remplacement de l'analyse et de la recommandation
    return (
        alternative["analysis"],
        alternative["action"],
        (
            "🔄 Nouvelle réponse générée avec "
            f"{GROQ_MODEL}. Tu peux encore donner "
            "un pouce négatif pour obtenir une autre réponse."
        ),
    )



# @app.callback(
#     Output(
#         "rag-analysis",
#         "children",
#         allow_duplicate=True,
#     ),
#     Output(
#         "rag-action",
#         "children",
#         allow_duplicate=True,
#     ),
#     Output(
#         "feedback-status",
#         "children",
#         allow_duplicate=True,
#     ),
#     Input("fb-up", "n_clicks"),
#     Input("fb-down", "n_clicks"),
#     State("selected-log-store", "data"),
#     State("rag-analysis", "children"),
#     State("rag-action", "children"),
#     prevent_initial_call=True,
# )
# def save_feedback(
#     up,
#     down,
#     row,
#     current_analysis,
#     current_action,
# ):
#     if not row:
#         return (
#             no_update,
#             no_update,
#             "Sélectionne d'abord un log.",
#         )

#     ctx = callback_context

#     if not ctx.triggered:
#         raise PreventUpdate

#     trigger = (
#         ctx.triggered[0]["prop_id"]
#         .split(".")[0]
#     )

#     # Pouce positif :
#     # rien n'est enregistré et Groq n'est pas appelé.
#     if trigger == "fb-up":
#         return (
#             no_update,
#             no_update,
#             "✅ Explication validée.",
#         )

#     if trigger != "fb-down":
#         raise PreventUpdate

#     initial_rag = _rag_for(row)

#     rejected_analysis = (
#         str(current_analysis).strip()
#         if current_analysis
#         else initial_rag["analysis"]
#     )

#     rejected_action = (
#         str(current_action).strip()
#         if current_action
#         else initial_rag["action"]
#     )

#     # 1. Enregistrement du feedback négatif
#     record = {
#     "feedback_timestamp": datetime.now().isoformat(
#         timespec="seconds"
#     ),
#     "feedback": "negative",

#     "log_id": row.get("id"),
#     "source": row.get("Source"),
#     "host": row.get("Host"),
#     "message": row.get("Message"),
#     "score_ia": row.get("Score IA"),
#     "ratio": row.get("Ratio"),
#     "statut": row.get("Statut"),

#     "model_version": row.get(
#         "Model",
#         "unknown",
#     ),

#     "rag_analysis": rejected_analysis,
#     "rag_recommendation": rejected_action,

#     "rag_version": "rag_v1",
#     "prompt_version": "prompt_v1",
#     "regeneration_prompt_version": "groq_regeneration_v1",
#     "alert_threshold": ALERT_THRESHOLD,
# }
#     # record = {
#     #     "feedback_timestamp": datetime.now().isoformat(
#     #         timespec="seconds"
#     #     ),
#     #     "feedback": "negative",

#     #     "log_id": row.get("id"),
#     #     "source": row.get("Source"),
#     #     "host": row.get("Host"),
#     #     "message": row.get("Message"),
#     #     "score_ia": row.get("Score IA"),
#     #     "ratio": row.get("Ratio"),
#     #     "statut": row.get("Statut"),

#     #     "model_version": row.get(
#     #         "Model",
#     #         "unknown",
#     #     ),

#     #     # Réponse que l'utilisateur vient de rejeter
#     #     "rag_analysis": rejected_analysis,
#     #     "rag_recommendation": rejected_action,

#     #     "rag_version": "rag_v1",
#     #     "prompt_version": "groq_regeneration_v1",
#     #     "alert_threshold": ALERT_THRESHOLD,
#     # }

#     try:
#         feedback_directory = os.path.dirname(
#             FEEDBACK_PATH
#         )

#         if feedback_directory:
#             os.makedirs(
#                 feedback_directory,
#                 exist_ok=True,
#             )

#         with open(
#             FEEDBACK_PATH,
#             "a",
#             encoding="utf-8",
#         ) as file:
#             file.write(
#                 json.dumps(
#                     record,
#                     ensure_ascii=False,
#                 )
#                 + "\n"
#             )

#     except Exception as error:
#         log.exception(
#             "Erreur pendant l'enregistrement "
#             "du feedback négatif"
#         )

#         return (
#             no_update,
#             no_update,
#             f"⚠️ Feedback non sauvegardé : {error}",
#         )
    
#     try:
#     # Appel Groq uniquement après un pouce négatif
#     alternative = _generate_alternative_rag(
#         row=row,
#         rejected_analysis=rejected_analysis,
#         rejected_action=rejected_action,
#     )

#     # On ajoute la nouvelle réponse au même enregistrement
#     record["replacement_rag_analysis"] = alternative["analysis"]
#     record["replacement_rag_recommendation"] = alternative["action"]
#     record["replacement_generator"] = GROQ_MODEL
#     record["groq_success"] = True
#     record["groq_error"] = None

# except Exception as error:
#     log.exception("Erreur pendant l'appel à Groq")

#     alternative = None

#     record["replacement_rag_analysis"] = None
#     record["replacement_rag_recommendation"] = None
#     record["replacement_generator"] = GROQ_MODEL
#     record["groq_success"] = False
#     record["groq_error"] = str(error)


#     try:
#         feedback_directory = os.path.dirname(FEEDBACK_PATH)

#         if feedback_directory:
#             os.makedirs(
#                 feedback_directory,
#                 exist_ok=True,
#             )

#         with open(
#             FEEDBACK_PATH,
#             "a",
#             encoding="utf-8",
#         ) as file:
#             file.write(
#                 json.dumps(
#                     record,
#                     ensure_ascii=False,
#                 )
#                 + "\n"
#             )

#     except Exception as error:
#         log.exception(
#             "Erreur pendant l'enregistrement du feedback négatif"
#         )

#         return (
#             no_update,
#             no_update,
#             f"⚠️ Feedback non sauvegardé : {error}",
#         )


#     if alternative is None:
#         return (
#             no_update,
#             no_update,
#             (
#                 "👎 Feedback enregistré, mais la nouvelle "
#                 f"analyse n'a pas pu être générée : "
#                 f"{record['groq_error']}"
#             ),
#         )


#     return (
#         alternative["analysis"],
#         alternative["action"],
#         (
#             "🔄 Une nouvelle analyse a été générée "
#             f"avec {GROQ_MODEL}."
#         ),
#     )
#     # # 2. Appel Groq uniquement après le pouce négatif
#     # try:
#     #     alternative = _generate_alternative_rag(
#     #         row=row,
#     #         rejected_analysis=rejected_analysis,
#     #         rejected_action=rejected_action,
#     #     )

#     #     return (
#     #         alternative["analysis"],
#     #         alternative["action"],
#     #         (
#     #             "🔄 Une nouvelle analyse a été générée "
#     #             f"avec {GROQ_MODEL}."
#     #         ),
#     #     )

#     # except Exception as error:
#     #     log.exception(
#     #         "Erreur pendant l'appel à Groq"
#     #     )

#     #     # Le feedback reste enregistré même si Groq échoue.
#     #     return (
#     #         no_update,
#     #         no_update,
#     #         (
#     #             "👎 Feedback enregistré, mais la nouvelle "
#     #             f"analyse n'a pas pu être générée : {error}"
#     #         ),
#     #     )


# @app.callback(
#     Output("feedback-status","children",allow_duplicate=True),
#     Input("fb-up","n_clicks"), Input("fb-down","n_clicks"),
#     State("selected-log-store","data"), prevent_initial_call=True,
# )
# def save_feedback(up, down, row):
#     if not row:
#         return "Sélectionne d'abord un log."

#     ctx = callback_context

#     if not ctx.triggered:
#         raise PreventUpdate

#     trigger = ctx.triggered[0]["prop_id"].split(".")[0]

#     if trigger == "fb-up":
#         feedback = "positive"
#     elif trigger == "fb-down":
#         feedback = "negative"
#     else:
#         raise PreventUpdate

#     # Recrée exactement l'analyse et la recommandation affichées
#     rag_result = _rag_for(row)

#     record = {
#         # Informations sur le feedback
#         "feedback_timestamp": datetime.now().isoformat(timespec="seconds"),
#         "feedback": feedback,

#         # Informations du log
#         "log_id": row.get("id"),
#         "source": row.get("Source"),
#         "host": row.get("Host"),
#         "message": row.get("Message"),
#         "score_ia": row.get("Score IA"),
#         "ratio": row.get("Ratio"),
#         "statut": row.get("Statut"),

#         # Réponse RAG réellement évaluée
#         "rag_analysis": rag_result.get("analysis"),
#         "rag_recommendation": rag_result.get("action"),

#         # Versions utiles pour le suivi
#         "rag_version": "rag_v1",
#         "prompt_version": "prompt_v1",
#         #"detection_model_version": row.get("Model", "unknown"),
#         "alert_threshold": ALERT_THRESHOLD,
#     }

#     try:
#         feedback_directory = os.path.dirname(FEEDBACK_PATH)

#         if feedback_directory:
#             os.makedirs(feedback_directory, exist_ok=True)

#         with open(FEEDBACK_PATH, "a", encoding="utf-8") as file:
#             file.write(
#                 json.dumps(record, ensure_ascii=False) + "\n"
#             )

#         return "✅ Feedback et réponse RAG enregistrés."

#     except Exception as error:
#         log.exception("Erreur pendant l'enregistrement du feedback")
#         return f"⚠️ Feedback non sauvegardé : {error}"
# def save_feedback(up, down, row):
#     if not row: return "Sélectionne d'abord un log."
#     ctx = callback_context
#     if not ctx.triggered: raise PreventUpdate
#     trigger  = ctx.triggered[0]["prop_id"].split(".")[0]
#     feedback = "positive" if trigger == "fb-up" else "negative"
#     record   = {"timestamp":datetime.now().isoformat(),"feedback":feedback,
#                  "log_id":row.get("id"),"source":row.get("Source"),"host":row.get("Host"),
#                  "message":row.get("Message"),"score_ia":row.get("Score IA"),
#                  "ratio":row.get("Ratio"),"statut":row.get("Statut")}
#     try:
#         os.makedirs(os.path.dirname(FEEDBACK_PATH), exist_ok=True)
#         with open(FEEDBACK_PATH,"a",encoding="utf-8") as f:
#             f.write(json.dumps(record,ensure_ascii=False)+"\n")
#         return "✅ Feedback enregistré."
#     except Exception as e:
#         return f"⚠️ Non sauvegardé : {e}"

@app.callback(
    Output("alert-table", "data"),
    Output("alert-strip", "children"),
    Input("interval", "n_intervals"),
)
def update_alerts(_):
    rows, _ = _visible_rows()

    # Alertes encore disponibles dans le buffer :
    # utilisées uniquement pour remplir le tableau.
    critical = [
        row
        for row in rows
        if row.get("_ratio_val", 0) > ALERT_THRESHOLD
    ]

    # Compteur historique indépendant du buffer.
    with _lock:
        total_alerts = _total_alerts_received

    last = (
        critical[0].get("Timestamp", "—")[-8:]
        if critical
        else "—"
    )

    top = Counter(
        row.get("Source", "unknown")
        for row in critical
    ).most_common(1)

    top_src = top[0][0] if top else "—"

    strip = html.Div(
        style={
            "display": "grid",
            "gridTemplateColumns": "1fr 1fr 1fr",
            "gap": "14px",
        },
        children=[
            _metric_card(
                "total des alertes",
                "alert-dummy-1",
                RED,
                "🚨",
                "TOTAL",
            ),
            _metric_card(
                "dernier événement",
                "alert-dummy-2",
                ORAN,
                "⏱",
                "LAST",
            ),
            _metric_card(
                "source principale",
                "alert-dummy-3",
                CYAN,
                "🖥",
                "TOP",
            ),
        ],
    )

    # On affiche le compteur total et non len(critical).
    strip.children[0].children[1].children = str(total_alerts)
    strip.children[1].children[1].children = last
    strip.children[2].children[1].children = top_src

    return (
        [
            _display_row(row, include_model=True)
            for row in critical
        ],
        strip,
    )


# @app.callback(
#     Output("alert-table","data"), Output("alert-strip","children"),
#     Input("interval","n_intervals"),
# )
# def update_alerts(_):
#     rows, _ = _visible_rows()
#     critical = [r for r in rows if r.get("_ratio_val",0) > ALERT_THRESHOLD]
#     last     = critical[0].get("Timestamp","—")[-8:] if critical else "—"
#     top      = Counter(r.get("Source","?") for r in critical).most_common(1)
#     top_src  = top[0][0] if top else "—"
#     strip    = html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr 1fr","gap":"14px"}, children=[
#         _metric_card("alertes critiques","alert-dummy-1",RED,  "🚨","ACTIVE"),
#         _metric_card("dernier événement","alert-dummy-2",ORAN, "⏱","LAST"),
#         _metric_card("source principale","alert-dummy-3",CYAN, "🖥","TOP"),
#     ])
#     strip.children[0].children[1].children = str(len(critical))
#     strip.children[1].children[1].children = last
#     strip.children[2].children[1].children = top_src
#     return [_display_row(r,include_model=True) for r in critical], strip

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)