"""
LogGuardian — AIOps Platform UI  (multi-pages : Dashboard / Logs / Alertes)
"""
import json, logging, os, threading, random
from collections import deque
from datetime import datetime, timedelta

import dash
from dash import dash_table, dcc, html, callback_context
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("monitoring-ui")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "logs-anomalies-ml")
MAX_ROWS                = int(os.getenv("MAX_ROWS", "2000"))
REFRESH_INTERVAL_MS     = int(os.getenv("REFRESH_INTERVAL_MS", "3000"))

_buffer: deque = deque(maxlen=MAX_ROWS)
_seen: set = set()
_lock = threading.Lock()
_total_received = 0


def _kafka_thread():
    log.info("Thread Kafka démarré — connexion à %s", KAFKA_BOOTSTRAP_SERVERS)
    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
        "group.id":           "monitoring-ui",
        "auto.offset.reset":  "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([KAFKA_TOPIC])
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            log.warning("Erreur Kafka : %s", msg.error())
            continue
        try:
            global _total_received
            r = json.loads(msg.value().decode("utf-8"))
            dedup_key = (
                r.get("detected_at", "")[:19],
                r.get("source", ""),
                r.get("host", ""),
                f"{r.get('anomaly_score', 0):.4f}",
            )
            with _lock:
                if dedup_key in _seen:
                    log.debug("Message dupliqué ignoré : %s", dedup_key)
                    continue
                row = {
                    "Timestamp":  r.get("detected_at", "")[:19].replace("T", " "),
                    "Source":     r.get("source", ""),
                    "Host":       r.get("host", ""),
                    "Message":    r.get("sequence", [{}])[-1].get("message", "")[:120],
                    "Score IA":   f"{r.get('anomaly_score', 0):.2f}",
                    "Ratio":      f"{r.get('severity_ratio', 0):.2f}x",
                    "Statut":     "ANOMALIE" if r.get("severity_ratio", 0) > 1.3 else "NORMAL",
                    "_ratio_val": r.get("severity_ratio", 0),
                    "_score_val": r.get("anomaly_score", 0),
                    "_ts":        r.get("detected_at", ""),
                }
                _seen.add(dedup_key)
                if len(_seen) > MAX_ROWS * 2:
                    for _ in range(MAX_ROWS):
                        _seen.pop()
                _buffer.appendleft(row)
                _total_received += 1
        except Exception as e:
            log.error("Erreur parsing message : %s", e)


threading.Thread(target=_kafka_thread, daemon=True).start()

# ─────────────────────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    title="LogGuardian — AIOps Platform",
    suppress_callback_exceptions=True,
)
app.server.config["SECRET_KEY"] = "logguardian"

app.index_string = '''<!DOCTYPE html>
<html>
<head>{%metas%}<title>{%title%}</title>{%favicon%}{%css%}
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { height: 100%; background: #0b0d14; overflow: hidden; }
  ::-webkit-scrollbar { width: 4px; height: 4px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: #2a2f45; border-radius: 2px; }

  /* Dropdown */
  .Select-control { background: #0e1428 !important; border: 1px solid #3b82f6 !important; border-radius: 6px !important; color: #c8d0e7 !important; min-height: 36px !important; height: 36px !important; }
  .Select-control:hover { border-color: #6366f1 !important; }
  .Select-value-label { color: #c8d0e7 !important; line-height: 34px !important; }
  .Select-placeholder { color: #4a6fa5 !important; line-height: 34px !important; }
  .Select-arrow { border-top-color: #3b82f6 !important; }
  .Select-menu-outer { background: #0e1428 !important; border: 1px solid #3b82f6 !important; border-radius: 6px !important; }
  .VirtualizedSelectOption { background: #0e1428 !important; color: #c8d0e7 !important; font-size: 12px !important; }
  .VirtualizedSelectFocusedOption { background: #1a2a50 !important; color: #fff !important; }
  .Select-input input { color: #c8d0e7 !important; }
  /* Dropdown modern Dash */
  .dash-dropdown .Select-control, .dash-dropdown > div > div { background-color: #0e1428 !important; border: 1px solid #3b82f6 !important; border-radius: 6px !important; }
  .dash-dropdown .Select__control { background-color: #0e1428 !important; border-color: #3b82f6 !important; border-radius: 6px !important; min-height: 36px !important; }
  .dash-dropdown .Select__control:hover { border-color: #6366f1 !important; }
  .dash-dropdown .Select__single-value, .dash-dropdown .Select__placeholder { color: #c8d0e7 !important; }
  .dash-dropdown .Select__placeholder { color: #4a6fa5 !important; }
  .dash-dropdown .Select__menu { background-color: #0e1428 !important; border: 1px solid #3b82f6 !important; border-radius: 6px !important; }
  .dash-dropdown .Select__option { background-color: #0e1428 !important; color: #c8d0e7 !important; }
  .dash-dropdown .Select__option--is-focused { background-color: #1a2a50 !important; color: #fff !important; }
  .dash-dropdown .Select__dropdown-indicator svg, .dash-dropdown .Select__indicator svg { color: #3b82f6 !important; }
  .dash-dropdown .Select__input input, .dash-dropdown input { color: #c8d0e7 !important; background: transparent !important; }

  /* Plotly */
  .js-plotly-plot .plotly .modebar { background: transparent !important; }
  .js-plotly-plot .plotly .modebar-btn path { fill: #4a5270 !important; }

  @keyframes pulse-dot { 0%,100% { opacity:1; } 50% { opacity:.4; } }
  .live-dot { animation: pulse-dot 1.5s ease-in-out infinite; }
  @keyframes pulse-ring { 0% { box-shadow: 0 0 0 0 rgba(239,68,68,.4); } 70% { box-shadow: 0 0 0 8px rgba(239,68,68,0); } 100% { box-shadow: 0 0 0 0 rgba(239,68,68,0); } }
  .alert-pulse { animation: pulse-ring 2s ease-out infinite; }

  .dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner tr:hover td { background-color: #1a1f35 !important; cursor: pointer; }
  .dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner tr.selected td { background-color: #1a2540 !important; }

  .nav-btn { transition: all .15s; }
  .nav-btn:hover { background-color: #161929 !important; color: #c8d0e7 !important; }
</style>
</head>
<body>{%app_entry%}<footer>{%config%}{%scripts%}{%renderer%}</footer></body>
</html>'''

# ── Palette ───────────────────────────────────────────────────────────────────
C = {
    "bg":      "#0b0d14", "sidebar": "#0d0f1a",
    "surface": "#111420", "surface2": "#161929",
    "border":  "#1e2236", "border2": "#252a42",
    "text":    "#c8d0e7", "muted":   "#4a5270", "muted2": "#6b7494",
    "danger":  "#ef4444", "success": "#22c55e",
    "warning": "#f59e0b", "cyan":    "#38bdf8",
    "blue":    "#3b82f6", "accent":  "#6366f1",
}

TABLE_COLS        = ["Timestamp", "Source", "Message", "Score IA", "Statut"]
ALERT_TABLE_COLS  = ["Timestamp", "Source", "Host", "Message", "Score IA", "Ratio"]

_HEADER = {
    "backgroundColor": C["surface2"], "color": C["muted"],
    "fontWeight": "600", "fontSize": "10px", "border": "none",
    "borderBottom": f"1px solid {C['border']}",
    "textTransform": "uppercase", "letterSpacing": "0.08em",
    "padding": "10px 14px", "fontFamily": "'Space Mono', monospace", "whiteSpace": "nowrap",
}
_CELL = {
    "backgroundColor": C["surface"], "color": C["text"],
    "fontSize": "12px", "border": "none",
    "borderBottom": f"1px solid {C['border']}",
    "padding": "10px 14px", "overflow": "hidden",
    "textOverflow": "ellipsis", "whiteSpace": "nowrap",
    "fontFamily": "'DM Sans', sans-serif",
}

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

def _nav_btn(icon, label, page_id, active_page):
    active = (active_page == page_id)
    return html.Button(
        [html.Span(icon, style={"fontSize": "16px", "lineHeight": "1"}), html.Span(label)],
        id=f"nav-btn-{page_id}",
        n_clicks=0,
        className="nav-btn",
        style={
            "display": "flex", "alignItems": "center", "gap": "10px",
            "width": "100%", "padding": "10px 14px", "borderRadius": "8px",
            "border": "none", "cursor": "pointer", "textAlign": "left",
            "backgroundColor": C["surface"] if active else "transparent",
            "color": C["text"] if active else C["muted2"],
            "fontSize": "13px", "fontWeight": "500",
            "fontFamily": "'DM Sans', sans-serif",
            "marginBottom": "2px",
            "borderLeft": f"2px solid {C['accent']}" if active else "2px solid transparent",
        },
    )


def _sidebar(active_page="logs"):
    return html.Div(style={
        "width": "200px", "minWidth": "200px",
        "backgroundColor": C["sidebar"],
        "borderRight": f"1px solid {C['border']}",
        "display": "flex", "flexDirection": "column",
        "height": "100%",
    }, children=[
        # Logo
        html.Div(style={
            "padding": "20px 16px 18px",
            "borderBottom": f"1px solid {C['border']}",
            "display": "flex", "alignItems": "center", "gap": "10px",
        }, children=[
            html.Span("🛡️", style={"fontSize": "20px"}),
            html.Div([
                html.Span("LogGuardian", style={
                    "fontSize": "13px", "fontWeight": "700", "color": C["text"],
                    "fontFamily": "'DM Sans', sans-serif", "letterSpacing": "-0.01em",
                }),
                html.Div("AIOps Platform", style={
                    "fontSize": "9px", "color": C["muted"], "letterSpacing": "0.08em",
                    "textTransform": "uppercase", "fontFamily": "'Space Mono', monospace",
                    "marginTop": "1px",
                }),
            ]),
        ]),

        # Nav
        html.Div(style={"padding": "14px 10px", "flex": "1"}, children=[
            _nav_btn("▦", "Dashboard",          "dashboard", active_page),
            _nav_btn("☰", "Historique des Logs","logs",      active_page),
            _nav_btn("⚠", "Alertes",            "alerts",    active_page),
        ]),

        # Kafka status
        html.Div(id="kafka-status-indicator", style={
            "padding": "14px 16px", "borderTop": f"1px solid {C['border']}",
            "display": "flex", "alignItems": "center", "gap": "8px",
        }, children=[
            html.Div(className="live-dot", style={
                "width": "7px", "height": "7px", "borderRadius": "50%",
                "backgroundColor": C["success"],
                "boxShadow": f"0 0 6px {C['success']}", "flexShrink": "0",
            }),
            html.Span("Kafka connecté", style={
                "fontSize": "11px", "color": C["muted2"],
                "fontFamily": "'Space Mono', monospace",
            }),
        ]),
    ])


# ─────────────────────────────────────────────────────────────────────────────
# TOPBAR helper
# ─────────────────────────────────────────────────────────────────────────────

def _topbar(title, subtitle):
    return html.Div(style={
        "padding": "14px 20px",
        "borderBottom": f"1px solid {C['border']}",
        "backgroundColor": C["surface"],
        "display": "flex", "alignItems": "center",
        "justifyContent": "space-between", "flexShrink": "0",
    }, children=[
        html.Div([
            html.H1(title, style={
                "fontSize": "13px", "fontWeight": "700", "letterSpacing": "0.08em",
                "color": C["text"], "fontFamily": "'Space Mono', monospace",
            }),
            html.P(subtitle, style={
                "fontSize": "10px", "color": C["muted"],
                "fontFamily": "'Space Mono', monospace", "marginTop": "1px",
            }),
        ]),
        html.Div(style={
            "display": "flex", "alignItems": "center", "gap": "8px",
            "backgroundColor": C["surface2"], "border": f"1px solid {C['border2']}",
            "borderRadius": "8px", "padding": "6px 12px",
            "fontSize": "11px", "fontFamily": "'Space Mono', monospace", "color": C["muted2"],
        }, children=[
            html.Span("📅"),
            html.Span(datetime.now().strftime("%A %d %B %Y").upper()),
            html.Span("🕐"),
            html.Span(id="time-display"),
        ]),
    ])


# ─────────────────────────────────────────────────────────────────────────────
# KPI CARD
# ─────────────────────────────────────────────────────────────────────────────

def _kpi(icon, label, value_id, color=None, badge=None, initial_value="—"):
    return html.Div(style={
        "backgroundColor": C["surface"],
        "border": f"1px solid {C['border']}",
        "borderRadius": "10px", "padding": "16px 18px",
        "flex": "1", "minWidth": "140px",
        "borderTop": f"3px solid {color or C['accent']}",
    }, children=[
        html.Div(style={
            "display": "flex", "justifyContent": "space-between",
            "alignItems": "flex-start", "marginBottom": "10px",
        }, children=[
            html.Span(icon, style={"fontSize": "20px"}),
            html.Span(badge or "", style={
                "fontSize": "9px", "color": color or C["muted2"],
                "fontFamily": "'Space Mono', monospace",
                "letterSpacing": "0.06em",
            }),
        ]),
        html.Div(id=value_id, style={
            "fontSize": "26px", "fontWeight": "700",
            "color": color or C["text"],
            "fontFamily": "'Space Mono', monospace", "lineHeight": "1",
        }, children=initial_value),
        html.Div(label, style={
            "fontSize": "10px", "color": C["muted"], "marginTop": "6px",
            "textTransform": "uppercase", "letterSpacing": "0.07em",
            "fontFamily": "'Space Mono', monospace",
        }),
    ])


# ─────────────────────────────────────────────────────────────────────────────
# PLOTLY chart helpers
# ─────────────────────────────────────────────────────────────────────────────

_PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Space Mono, monospace", color="#6b7494", size=10),
    margin=dict(l=40, r=16, t=16, b=36),
    xaxis=dict(gridcolor="#1e2236", showgrid=True, zeroline=False,
               tickfont=dict(size=9), linecolor="#1e2236"),
    yaxis=dict(gridcolor="#1e2236", showgrid=True, zeroline=False,
               tickfont=dict(size=9)),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=9)),
    hovermode="x unified",
)

def _empty_fig(msg="En attente de données…"):
    fig = go.Figure()
    fig.update_layout(**_PLOT_LAYOUT,
        annotations=[dict(text=msg, x=0.5, y=0.5, xref="paper",
                          yref="paper", showarrow=False,
                          font=dict(color="#4a5270", size=12))])
    return fig

def _anomaly_timeline_fig(rows):
    """Anomalies sur les derniers événements reçus."""
    if not rows:
        return _empty_fig()

    recent = list(reversed(rows[:50]))

    labels = [str(i + 1) for i in range(len(recent))]
    anomalies = [1 if r.get("Statut") == "ANOMALIE" else 0 for r in recent]
    normals = [1 if r.get("Statut") == "NORMAL" else 0 for r in recent]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Normal",
        x=labels,
        y=normals,
        marker_color="#22c55e33",
        marker_line_color="#22c55e",
        marker_line_width=1
    ))
    fig.add_trace(go.Bar(
        name="Anomalie",
        x=labels,
        y=anomalies,
        marker_color="#ef444466",
        marker_line_color="#ef4444",
        marker_line_width=1
    ))

    fig.update_layout(
        **_PLOT_LAYOUT,
        barmode="stack",
        yaxis_title="Événements",
        xaxis_title="Derniers logs reçus"
    )
    return fig
# def _anomaly_timeline_fig(rows):
#     """Anomalies par minute sur les 30 dernières minutes."""
#     if not rows:
#         return _empty_fig()
#     now = datetime.now()
#     buckets = {}
#     for i in range(30):
#         t = (now - timedelta(minutes=i)).strftime("%H:%M")
#         buckets[t] = {"anomalie": 0, "normal": 0}

#     for r in rows:
#         try:
#             ts = datetime.strptime(r["Timestamp"], "%Y-%m-%d %H:%M:%S")
#             diff = (now - ts).total_seconds() / 60
#             if 0 <= diff < 30:
#                 key = ts.strftime("%H:%M")
#                 if key in buckets:
#                     if r.get("Statut") == "ANOMALIE":
#                         buckets[key]["anomalie"] += 1
#                     else:
#                         buckets[key]["normal"] += 1
#         except Exception:
#             pass

#     labels = sorted(buckets.keys())
#     anomalies = [buckets[k]["anomalie"] for k in labels]
#     normals   = [buckets[k]["normal"]   for k in labels]

#     fig = go.Figure()
#     fig.add_trace(go.Bar(name="Normal",   x=labels, y=normals,
#                          marker_color="#22c55e33", marker_line_color="#22c55e",
#                          marker_line_width=1))
#     fig.add_trace(go.Bar(name="Anomalie", x=labels, y=anomalies,
#                          marker_color="#ef444466", marker_line_color="#ef4444",
#                          marker_line_width=1))
#     fig.update_layout(**_PLOT_LAYOUT, barmode="stack",
#                       yaxis_title="Événements")
#     return fig

def _score_timeline_fig(rows):
    """Score IA sur les derniers événements reçus."""
    if not rows:
        return _empty_fig()

    recent = list(reversed(rows[:50]))

    xs = [str(i + 1) for i in range(len(recent))]
    ys = [float(r.get("Score IA", 0)) for r in recent]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=xs,
        y=ys,
        mode="lines+markers",
        name="Score IA",
        line=dict(color="#6366f1", width=2),
        fill="tozeroy",
        fillcolor="rgba(99,102,241,0.08)",
    ))

    fig.add_hline(
        y=0.8,
        line_dash="dash",
        line_color="#ef4444",
        annotation_text="Seuil critique",
        annotation_font_size=9,
        annotation_font_color="#ef4444"
    )

    layout = dict(**_PLOT_LAYOUT)
    layout["yaxis"] = dict(**_PLOT_LAYOUT["yaxis"], range=[0, 1.05])

    fig.update_layout(**layout, xaxis_title="Derniers logs reçus")
    return fig

# def _score_timeline_fig(rows):
#     """Score IA moyen glissant."""
#     if not rows:
#         return _empty_fig()
#     now = datetime.now()
#     pts = []
#     for r in rows:
#         try:
#             ts = datetime.strptime(r["Timestamp"], "%Y-%m-%d %H:%M:%S")
#             diff = (now - ts).total_seconds() / 60
#             if 0 <= diff < 30:
#                 pts.append((ts, float(r.get("Score IA", 0))))
#         except Exception:
#             pass
#     if not pts:
#         return _empty_fig()
#     pts.sort(key=lambda x: x[0])
#     xs = [p[0].strftime("%H:%M:%S") for p in pts]
#     ys = [p[1] for p in pts]

#     fig = go.Figure()
#     fig.add_trace(go.Scatter(
#         x=xs, y=ys, mode="lines", name="Score IA",
#         line=dict(color="#6366f1", width=2),
#         fill="tozeroy", fillcolor="rgba(99,102,241,0.08)",
#     ))
#     fig.add_hline(y=0.8, line_dash="dash", line_color="#ef4444",
#                   annotation_text="Seuil critique", annotation_font_size=9,
#                   annotation_font_color="#ef4444")
#     fig.update_layout(**_PLOT_LAYOUT, yaxis=dict(range=[0, 1.05], **_PLOT_LAYOUT["yaxis"]))
#     return fig


def _services_fig(rows):
    """Top services touchés par des anomalies."""
    if not rows:
        return _empty_fig()
    counts = {}
    for r in rows:
        if r.get("Statut") == "ANOMALIE":
            s = r.get("Source", "unknown")
            counts[s] = counts.get(s, 0) + 1
    if not counts:
        return _empty_fig("Aucune anomalie détectée")
    top = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:8]
    labels = [t[0] for t in top]
    values = [t[1] for t in top]

    colors = ["#6366f1", "#3b82f6", "#38bdf8", "#22c55e",
              "#f59e0b", "#ef4444", "#ec4899", "#8b5cf6"]

    fig = go.Figure(go.Bar(
        x=values, y=labels, orientation="h",
        marker_color=colors[:len(labels)],
        marker_line_width=0,
    ))
    layout = dict(**_PLOT_LAYOUT)
    layout["margin"] = dict(l=120, r=16, t=16, b=36)
    fig.update_layout(**layout, xaxis_title="Nb anomalies")
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD PAGE
# ─────────────────────────────────────────────────────────────────────────────

def _health_badge(label, ok=True):
    color  = C["success"] if ok else C["danger"]
    status = "OK" if ok else "DOWN"
    return html.Div(style={
        "display": "flex", "alignItems": "center", "gap": "10px",
        "backgroundColor": C["surface2"],
        "border": f"1px solid {C['border']}",
        "borderLeft": f"3px solid {color}",
        "borderRadius": "8px", "padding": "10px 14px", "flex": "1",
    }, children=[
        html.Div(style={
            "width": "8px", "height": "8px", "borderRadius": "50%",
            "backgroundColor": color,
            "boxShadow": f"0 0 6px {color}",
        }),
        html.Div([
            html.Div(label, style={
                "fontSize": "11px", "color": C["text"], "fontWeight": "600",
                "fontFamily": "'Space Mono', monospace",
            }),
            html.Div(status, style={
                "fontSize": "10px", "color": color,
                "fontFamily": "'Space Mono', monospace", "letterSpacing": "0.06em",
            }),
        ]),
    ])


def _chart_card(title, graph_id, initial_figure=None):
    return html.Div(style={
        "backgroundColor": C["surface"],
        "border": f"1px solid {C['border']}",
        "borderRadius": "10px", "overflow": "hidden", "flex": "1",
    }, children=[
        html.Div(title, style={
            "padding": "12px 16px",
            "borderBottom": f"1px solid {C['border']}",
            "fontSize": "10px", "fontWeight": "700",
            "color": C["muted"], "letterSpacing": "0.08em",
            "textTransform": "uppercase", "fontFamily": "'Space Mono', monospace",
        }),
        dcc.Graph(id=graph_id, config={"displayModeBar": False},
                  style={"height": "200px"},
                  figure=initial_figure if initial_figure is not None else _empty_fig()),
    ])


def _page_dashboard(init_data=None):
    d = init_data or {}
    return html.Div(style={
        "flex": "1", "display": "flex", "flexDirection": "column",
        "overflow": "hidden", "minWidth": "0",
    }, children=[
        _topbar("DASHBOARD", "Vue globale — temps réel"),

        html.Div(style={
            "flex": "1", "overflow": "auto", "padding": "20px",
        }, children=[

            # KPI row
            html.Div(style={"display": "flex", "gap": "14px", "marginBottom": "20px"}, children=[
                _kpi("📊", "Total anomalies",   "kpi-total",     C["accent"],  "TOTAL",    d.get("total", "—")),
                _kpi("🔴", "Sévérité haute",    "kpi-high",      C["danger"],  "> 1.3x",   d.get("high", "—")),
                _kpi("⚡", "Score IA moyen",    "kpi-avg-score", C["blue"],    "MOY.",     d.get("avg_score", "—")),
                _kpi("⏱️", "Anomalies total",   "kpi-rate",      C["warning"], "RATE",     d.get("rate", "—")),
                _kpi("🖥️", "Services touchés",  "kpi-services",  C["cyan"],    "SERVICES", d.get("services", "—")),
            ]),

            # Charts row 1
            html.Div(style={"display": "flex", "gap": "14px", "marginBottom": "14px"}, children=[
                _chart_card("Anomalies — derniers logs reçus", "graph-timeline", d.get("fig_timeline")),
                _chart_card("Score IA — derniers logs reçus","graph-score", d.get("fig_score")),
            ]),

            # Charts row 2 + Health
            html.Div(style={"display": "flex", "gap": "14px"}, children=[
                _chart_card("Top services touchés", "graph-services", d.get("fig_services")),

                # Health panel
                html.Div(style={
                    "backgroundColor": C["surface"],
                    "border": f"1px solid {C['border']}",
                    "borderRadius": "10px", "flex": "1", "overflow": "hidden",
                }, children=[
                    html.Div("Health — Infrastructure", style={
                        "padding": "12px 16px",
                        "borderBottom": f"1px solid {C['border']}",
                        "fontSize": "10px", "fontWeight": "700",
                        "color": C["muted"], "letterSpacing": "0.08em",
                        "textTransform": "uppercase", "fontFamily": "'Space Mono', monospace",
                    }),
                    html.Div(id="health-panel", style={"padding": "14px", "display": "flex", "flexDirection": "column", "gap": "10px"},
                             children=d.get("health", [])),
                ]),
            ]),
        ]),
    ])


# ─────────────────────────────────────────────────────────────────────────────
# LOGS PAGE
# ─────────────────────────────────────────────────────────────────────────────

def _filter_bar():
    return html.Div(style={
        "padding": "14px 20px",
        "borderBottom": f"1px solid {C['border']}",
        "backgroundColor": C["surface2"], "flexShrink": "0",
    }, children=[
        html.Div("Recherche et filtrage dynamiques", style={
            "fontSize": "10px", "color": C["muted"], "letterSpacing": "0.1em",
            "textTransform": "uppercase", "fontFamily": "'Space Mono', monospace",
            "marginBottom": "10px",
        }),
        html.Div(style={
            "display": "grid",
            "gridTemplateColumns": "1fr 160px 160px 140px", "gap": "10px",
        }, children=[
            dcc.Input(
                id="search-text", type="text", debounce=False,
                placeholder="Filtrer par mots-clés : timeout, 404, injection, host...",
                style={
                    "width": "100%", "height": "36px",
                    "backgroundColor": "#0e1428", "border": "1px solid #3b82f6",
                    "borderRadius": "6px", "color": C["text"], "padding": "0 12px",
                    "outline": "none", "fontSize": "12px",
                    "fontFamily": "'DM Sans', sans-serif",
                },
            ),
            dcc.Dropdown(id="source-filter", placeholder="SERVICE", clearable=True,
                         style={"fontSize": "11px"}),
            dcc.Dropdown(id="level-filter", placeholder="NIVEAU", clearable=True,
                         options=[
                             {"label": "Toutes sévérités", "value": "all"},
                             {"label": "ANOMALIE (> 1.3x)", "value": "high"},
                             {"label": "NORMAL (≤ 1.3x)",  "value": "normal"},
                         ], value="all", style={"fontSize": "11px"}),
            dcc.Dropdown(id="limit-filter", placeholder="PÉRIODE", clearable=False,
                         options=[
                             {"label": "Temps réel", "value": 200},
                             {"label": "1 heure",    "value": 500},
                             {"label": "24 heures",  "value": MAX_ROWS},
                         ], value=200, style={"fontSize": "11px"}),
        ]),
    ])


def _rag_panel():
    return html.Div(id="rag-panel", style={
        "width": "300px", "minWidth": "300px",
        "backgroundColor": C["surface"],
        "borderLeft": f"1px solid {C['border']}",
        "display": "flex", "flexDirection": "column", "height": "100%",
    }, children=[
        html.Div(style={
            "padding": "14px 16px", "borderBottom": f"1px solid {C['border']}",
            "display": "flex", "alignItems": "center", "justifyContent": "space-between",
        }, children=[
            html.Div(style={"display": "flex", "alignItems": "center", "gap": "8px"}, children=[
                html.Div("⚡", style={
                    "width": "26px", "height": "26px", "borderRadius": "6px",
                    "background": f"linear-gradient(135deg, {C['accent']}, {C['blue']})",
                    "display": "flex", "alignItems": "center", "justifyContent": "center",
                    "fontSize": "13px",
                }),
                html.Span("EXPLICATION IA (RAG)", style={
                    "fontSize": "10px", "fontWeight": "700", "color": C["text"],
                    "letterSpacing": "0.08em", "fontFamily": "'Space Mono', monospace",
                }),
            ]),
        ]),
        html.Div(id="rag-content", style={"flex": "1", "overflow": "auto", "padding": "16px"},
                 children=_rag_empty()),
    ])


def _rag_empty():
    return html.Div(style={
        "display": "flex", "flexDirection": "column",
        "alignItems": "center", "justifyContent": "center",
        "height": "100%", "color": C["muted"], "textAlign": "center",
    }, children=[
        html.Div("⚡", style={"fontSize": "32px", "marginBottom": "12px", "opacity": "0.3"}),
        html.P("Sélectionnez un log dans le tableau pour voir l'analyse IA",
               style={"fontSize": "12px", "lineHeight": "1.6",
                      "fontFamily": "'DM Sans', sans-serif"}),
    ])


def _rag_card(row):
    is_anomaly = row.get("Statut") == "ANOMALIE"
    score = float(row.get("Score IA", 0))
    return html.Div(style={"fontFamily": "'DM Sans', sans-serif"}, children=[
        # Description
        html.Div(style={"marginBottom": "14px"}, children=[
            html.Div("DESCRIPTION DU LOG SÉLECTIONNÉ", style={
                "fontSize": "9px", "color": C["muted"], "letterSpacing": "0.1em",
                "textTransform": "uppercase", "fontFamily": "'Space Mono', monospace",
                "marginBottom": "8px",
            }),
            html.Div(style={
                "backgroundColor": C["surface2"], "border": f"1px solid {C['border2']}",
                "borderRadius": "6px", "padding": "10px 12px",
                "fontSize": "12px", "color": C["text"], "lineHeight": "1.6",
            }, children=row.get("Message", "—")[:150]),
        ]),
        # Score bar
        html.Div(style={"marginBottom": "14px"}, children=[
            html.Div(style={"display": "flex", "justifyContent": "space-between", "marginBottom": "6px"}, children=[
                html.Span("Score IA", style={"fontSize": "11px", "color": C["muted2"]}),
                html.Span(f"{score:.2f}", style={
                    "fontSize": "11px", "fontWeight": "700",
                    "color": C["danger"] if is_anomaly else C["success"],
                    "fontFamily": "'Space Mono', monospace",
                }),
            ]),
            html.Div(style={
                "height": "4px", "backgroundColor": C["border2"],
                "borderRadius": "2px", "overflow": "hidden",
            }, children=[
                html.Div(style={
                    "height": "100%", "width": f"{min(score * 100, 100):.0f}%",
                    "background": f"linear-gradient(90deg, {C['blue']}, {C['danger'] if is_anomaly else C['success']})",
                    "borderRadius": "2px",
                }),
            ]),
        ]),
        # Analysis
        html.Div(style={"marginBottom": "14px"}, children=[
            html.Div("ANALYSE DU MODÈLE RAG", style={
                "fontSize": "9px", "color": C["muted"], "letterSpacing": "0.1em",
                "textTransform": "uppercase", "fontFamily": "'Space Mono', monospace",
                "marginBottom": "8px",
            }),
            html.Div(style={
                "backgroundColor": C["surface2"],
                "border": f"1px solid {'rgba(239,68,68,0.25)' if is_anomaly else C['border2']}",
                "borderRadius": "6px", "padding": "10px 12px 10px 18px",
                "fontSize": "12px", "color": C["text"], "lineHeight": "1.7",
                "borderLeft": f"3px solid {C['danger'] if is_anomaly else C['success']}",
            }, children=(
                f"Ce log indique une tentative d'injection SQL sur le port 80. "
                f"Score d'anomalie élevé ({score:.2f}/1.0). "
                f"Pattern récurrent détecté sur le service {row.get('Source', 'inconnu')}."
            ) if is_anomaly else (
                f"Log de routine — comportement nominal détecté. "
                f"Score {score:.2f} en dessous du seuil d'alerte. Aucune action requise."
            )),
        ]),
        # Action
        html.Div(children=[
            html.Div("ACTION SUGGÉRÉE", style={
                "fontSize": "9px", "color": C["muted"], "letterSpacing": "0.1em",
                "textTransform": "uppercase", "fontFamily": "'Space Mono', monospace",
                "marginBottom": "8px",
            }),
            html.Div(style={
                "background": f"linear-gradient(135deg, {C['accent']}22, {C['blue']}22)",
                "border": f"1px solid {C['accent']}44",
                "borderRadius": "6px", "padding": "10px 12px",
            }, children=[
                html.Span(
                    "🔄 RESTART DU POD RECOMMANDÉ" if is_anomaly else "✅ AUCUNE ACTION REQUISE",
                    style={
                        "fontSize": "11px", "fontWeight": "700", "color": C["text"],
                        "letterSpacing": "0.04em", "fontFamily": "'Space Mono', monospace",
                    }
                ),
                html.P(
                    "Lien direct vers l'action Kubernetes" if is_anomaly else "Continuer la surveillance normale",
                    style={"fontSize": "11px", "color": C["muted2"], "marginTop": "4px"}
                ),
            ]),
        ]),
    ])


def _page_logs():
    return html.Div(style={
        "flex": "1", "display": "flex", "flexDirection": "column",
        "overflow": "hidden", "minWidth": "0",
    }, children=[
        _topbar("HISTORIQUE DES LOGS", "(Spark Streaming)"),

        # Metrics mini bar
        html.Div(id="metrics-bar", style={
            "display": "flex", "gap": "20px", "alignItems": "center",
            "padding": "8px 20px",
            "backgroundColor": C["surface2"],
            "borderBottom": f"1px solid {C['border']}",
            "flexShrink": "0",
        }),

        _filter_bar(),

        html.Div(style={
            "flex": "1", "display": "flex", "flexDirection": "row",
            "overflow": "hidden", "minHeight": "0",
        }, children=[
            # Table
            html.Div(style={"flex": "1", "overflow": "auto", "minWidth": "0"}, children=[
                dash_table.DataTable(
                    id="main-table",
                    columns=[{"name": c, "id": c} for c in TABLE_COLS],
                    data=[],
                    style_table={"overflowX": "auto", "width": "100%"},
                    style_header=_HEADER, style_cell=_CELL,
                    style_cell_conditional=[
                        {"if": {"column_id": "Message"}, "whiteSpace": "normal", "textOverflow": "clip", "maxWidth": "0"},
                        {"if": {"column_id": "Timestamp"}, "width": "155px", "minWidth": "155px",
                         "fontFamily": "'Space Mono', monospace", "fontSize": "11px", "color": C["muted2"]},
                        {"if": {"column_id": "Source"}, "width": "160px", "minWidth": "120px",
                         "color": C["cyan"], "fontFamily": "'Space Mono', monospace", "fontSize": "11px"},
                        {"if": {"column_id": "Score IA"}, "width": "80px", "minWidth": "80px",
                         "textAlign": "center", "fontFamily": "'Space Mono', monospace", "fontWeight": "700"},
                        {"if": {"column_id": "Statut"}, "width": "110px", "minWidth": "110px", "textAlign": "center"},
                    ],
                    style_data_conditional=[
                        {"if": {"filter_query": '{Statut} = "ANOMALIE"', "column_id": "Statut"},
                         "color": C["danger"], "fontWeight": "700",
                         "fontFamily": "'Space Mono', monospace", "fontSize": "10px", "letterSpacing": "0.06em"},
                        {"if": {"filter_query": '{Statut} = "NORMAL"', "column_id": "Statut"},
                         "color": C["success"], "fontFamily": "'Space Mono', monospace",
                         "fontSize": "10px", "letterSpacing": "0.06em"},
                        {"if": {"filter_query": '{Statut} = "ANOMALIE"', "column_id": "Score IA"}, "color": C["danger"]},
                        {"if": {"filter_query": '{Statut} = "NORMAL"', "column_id": "Score IA"}, "color": C["success"]},
                        {"if": {"row_index": "odd"}, "backgroundColor": C["surface2"]},
                    ],
                    page_size=60, sort_action="native",
                    row_selectable="single", selected_rows=[],
                ),
            ]),
            _rag_panel(),
        ]),
    ])


# ─────────────────────────────────────────────────────────────────────────────
# ALERTS PAGE
# ─────────────────────────────────────────────────────────────────────────────

def _page_alerts():
    return html.Div(style={
        "flex": "1", "display": "flex", "flexDirection": "column",
        "overflow": "hidden", "minWidth": "0",
    }, children=[
        _topbar("ALERTES CRITIQUES", "Anomalies avec ratio > 1.3x uniquement"),

        # Alert summary bar
        html.Div(style={
            "padding": "12px 20px",
            "backgroundColor": "rgba(239,68,68,0.06)",
            "borderBottom": f"1px solid rgba(239,68,68,0.2)",
            "display": "flex", "alignItems": "center", "gap": "16px",
            "flexShrink": "0",
        }, children=[
            html.Div(style={
                "width": "10px", "height": "10px", "borderRadius": "50%",
                "backgroundColor": C["danger"],
                "boxShadow": f"0 0 8px {C['danger']}",
            }, className="alert-pulse"),
            html.Span(id="alert-summary", style={
                "fontSize": "12px", "color": C["danger"],
                "fontFamily": "'Space Mono', monospace", "fontWeight": "700",
            }),
            html.Span("—", style={"color": C["muted"]}),
            html.Span(id="alert-last-seen", style={
                "fontSize": "11px", "color": C["muted2"],
                "fontFamily": "'Space Mono', monospace",
            }),
        ]),

        # Alerts table
        html.Div(style={"flex": "1", "overflow": "auto"}, children=[
            dash_table.DataTable(
                id="alert-table",
                columns=[{"name": c, "id": c} for c in ALERT_TABLE_COLS],
                data=[],
                style_table={"overflowX": "auto", "width": "100%"},
                style_header=_HEADER,
                style_cell={**_CELL, "backgroundColor": "#0f0a0a"},
                style_cell_conditional=[
                    {"if": {"column_id": "Message"}, "whiteSpace": "normal", "textOverflow": "clip", "maxWidth": "0"},
                    {"if": {"column_id": "Timestamp"}, "width": "155px", "minWidth": "155px",
                     "fontFamily": "'Space Mono', monospace", "fontSize": "11px", "color": C["muted2"]},
                    {"if": {"column_id": "Source"}, "width": "160px",
                     "color": C["danger"], "fontFamily": "'Space Mono', monospace", "fontSize": "11px"},
                    {"if": {"column_id": "Host"}, "width": "120px",
                     "fontFamily": "'Space Mono', monospace", "fontSize": "11px"},
                    {"if": {"column_id": "Score IA"}, "width": "80px", "textAlign": "center",
                     "fontFamily": "'Space Mono', monospace", "fontWeight": "700", "color": C["danger"]},
                    {"if": {"column_id": "Ratio"}, "width": "80px", "textAlign": "center",
                     "fontFamily": "'Space Mono', monospace", "fontWeight": "700", "color": C["warning"]},
                ],
                style_data_conditional=[
                    {"if": {"row_index": "odd"}, "backgroundColor": "#120a0a"},
                ],
                page_size=50, sort_action="native",
            ),
        ]),
    ])


# ─────────────────────────────────────────────────────────────────────────────
# ROOT LAYOUT
# ─────────────────────────────────────────────────────────────────────────────

app.layout = html.Div(style={
    "backgroundColor": C["bg"], "height": "100vh",
    "display": "flex", "flexDirection": "row",
    "fontFamily": "'DM Sans', sans-serif", "color": C["text"],
    "overflow": "hidden",
}, children=[
    # Sidebar — rebuilt via callback to update active state
    html.Div(id="sidebar-container", style={"display": "flex"}),

    # Page content
    html.Div(id="page-content", style={
        "flex": "1", "display": "flex", "overflow": "hidden", "minWidth": "0",
    }),

    # Stores & intervals
    dcc.Interval(id="interval", interval=REFRESH_INTERVAL_MS, n_intervals=0),
    dcc.Store(id="current-page", data="logs"),
    dcc.Store(id="rows-store",   data=[]),
    dcc.Store(id="selected-row-store", data=None),
])


# ─────────────────────────────────────────────────────────────────────────────
# CALLBACKS
# ─────────────────────────────────────────────────────────────────────────────

@app.callback(
    Output("current-page", "data"),
    Input("nav-btn-dashboard", "n_clicks"),
    Input("nav-btn-logs",      "n_clicks"),
    Input("nav-btn-alerts",    "n_clicks"),
    State("current-page", "data"),
    prevent_initial_call=True,
)
def navigate(n_dashboard, n_logs, n_alerts, current):
    ctx = callback_context
    if not ctx.triggered:
        return current
    btn_id = ctx.triggered[0]["prop_id"].split(".")[0]
    mapping = {
        "nav-btn-dashboard": "dashboard",
        "nav-btn-logs":      "logs",
        "nav-btn-alerts":    "alerts",
    }
    return mapping.get(btn_id, current)


def _compute_dashboard_data():
    with _lock:
        rows  = list(_buffer)
        total = _total_received

    high_rows  = [r for r in rows if r.get("_ratio_val", 0) > 1.3]
    scores     = [r.get("_score_val", 0) for r in rows if "_score_val" in r]
    avg_score  = sum(scores) / len(scores) if scores else 0
    services   = len({r["Source"] for r in high_rows if r.get("Source")})
    rate       = len(high_rows)

    health = [
        _health_badge("Kafka Broker",       ok=True),
        _health_badge("Kubernetes Cluster", ok=True),
        _health_badge("ML Model API",       ok=total > 0),
        _health_badge("Spark Streaming",    ok=total > 0),
    ]

    display_rows = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]

    return {
        "total":        str(total),
        "high":         str(len(high_rows)),
        "avg_score":    f"{avg_score:.3f}",
        "rate":         str(rate),
        "services":     str(services),
        "fig_timeline": _anomaly_timeline_fig(display_rows),
        "fig_score":    _score_timeline_fig(display_rows),
        "fig_services": _services_fig(rows),
        "health":       health,
    }


@app.callback(
    Output("sidebar-container", "children"),
    Output("page-content",      "children"),
    Input("current-page", "data"),
)
def render_page(page):
    sidebar = _sidebar(active_page=page)
    if page == "dashboard":
        content = _page_dashboard(init_data=_compute_dashboard_data())
    elif page == "alerts":
        content = _page_alerts()
    else:
        content = _page_logs()
    return sidebar, content


# ── Helpers ───────────────────────────────────────────────────────────────────

def _apply_filters(rows, search_text, source_filter, level_filter, limit_filter):
    search_text  = (search_text or "").lower().strip()
    level_filter = level_filter or "all"
    limit_filter = int(limit_filter or 200)
    filtered = []
    for r in rows:
        searchable = " ".join([r.get("Timestamp",""), r.get("Source",""),
                               r.get("Message",""), r.get("Score IA","")]).lower()
        if search_text and search_text not in searchable:
            continue
        if source_filter and r.get("Source") != source_filter:
            continue
        if level_filter == "high"   and r.get("_ratio_val", 0) <= 1.3: continue
        if level_filter == "normal" and r.get("_ratio_val", 0) >  1.3: continue
        filtered.append(r)
    return filtered[:limit_filter]


def _mini_metric(label, value, color=None):
    return html.Div(style={"textAlign": "center"}, children=[
        html.Div(value, style={
            "fontSize": "18px", "fontWeight": "700",
            "color": color or C["text"], "fontFamily": "'Space Mono', monospace",
        }),
        html.Div(label, style={
            "fontSize": "9px", "color": C["muted"], "letterSpacing": "0.08em",
            "textTransform": "uppercase", "fontFamily": "'Space Mono', monospace",
        }),
    ])


# ── Logs page callbacks ───────────────────────────────────────────────────────

@app.callback(
    Output("metrics-bar",   "children"),
    Output("time-display",  "children"),
    Output("main-table",    "data"),
    Output("source-filter", "options"),
    Output("rows-store",    "data"),
    Input("interval",       "n_intervals"),
    Input("search-text",    "value"),
    Input("source-filter",  "value"),
    Input("level-filter",   "value"),
    Input("limit-filter",   "value"),
    prevent_initial_call=False,
)
def refresh_logs(_, search_text, source_filter, level_filter, limit_filter):
    with _lock:
        rows  = [r for r in _buffer if "_ratio_val" in r]
        total = _total_received

    source_options = [{"label": s, "value": s}
                      for s in sorted({r["Source"] for r in rows if r.get("Source")})]

    filtered     = _apply_filters(rows, search_text, source_filter, level_filter, limit_filter)
    high_count   = sum(1 for r in filtered if r.get("_ratio_val", 0) > 1.3)
    sources_count = len({r["Source"] for r in filtered}) if filtered else 0
    display_rows = [{k: v for k, v in r.items() if not k.startswith("_")} for r in filtered]

    metrics = [
        _mini_metric("Total",     str(total)),
        html.Div(style={"width":"1px","backgroundColor":C["border"],"margin":"0 4px"}),
        _mini_metric("Anomalies", str(high_count),    C["danger"] if high_count else C["muted"]),
        html.Div(style={"width":"1px","backgroundColor":C["border"],"margin":"0 4px"}),
        _mini_metric("Sources",   str(sources_count)),
        html.Div(style={"width":"1px","backgroundColor":C["border"],"margin":"0 4px"}),
        _mini_metric("Topic",     KAFKA_TOPIC[:14],   C["muted"]),
    ]

    store_rows = [{k: v for k, v in r.items() if k != "_seq"} for r in filtered]
    now_time   = datetime.now().strftime("%H:%M:%S")
    return metrics, now_time, display_rows, source_options, store_rows


@app.callback(
    Output("main-table", "selected_rows"),
    Input("main-table",  "selected_rows"),
    State("selected-row-store", "data"),
    prevent_initial_call=True,
)
def preserve_selection(selected_rows, stored):
    if selected_rows:
        return selected_rows
    if stored is not None:
        return [stored]
    return []


@app.callback(
    Output("selected-row-store", "data"),
    Input("main-table", "selected_rows"),
    prevent_initial_call=True,
)
def save_selection(selected_rows):
    if selected_rows:
        return selected_rows[0]
    return dash.no_update


@app.callback(
    Output("rag-content", "children"),
    Input("main-table",   "selected_rows"),
    State("rows-store",   "data"),
)
def show_rag(selected_rows, store_data):
    if not selected_rows or not store_data:
        return _rag_empty()
    idx = selected_rows[0]
    if idx >= len(store_data):
        return html.P("Données non disponibles.", style={"color": C["muted"], "fontSize": "12px"})
    return _rag_card(store_data[idx])


# ── Dashboard callbacks ───────────────────────────────────────────────────────
@app.callback(
    Output("kpi-total",     "children"),
    Output("kpi-high",      "children"),
    Output("kpi-avg-score", "children"),
    Output("kpi-rate",      "children"),
    Output("kpi-services",  "children"),
    Output("graph-timeline","figure"),
    Output("graph-score",   "figure"),
    Output("graph-services","figure"),
    Output("health-panel",  "children"),
    Input("interval", "n_intervals"),
    State("current-page", "data"),
)
def refresh_dashboard(_, page):
    if page != "dashboard":
        raise dash.exceptions.PreventUpdate

    d = _compute_dashboard_data()
    return (
        d["total"], d["high"], d["avg_score"], d["rate"], d["services"],
        d["fig_timeline"], d["fig_score"], d["fig_services"], d["health"],
    )
# @app.callback(
#     Output("kpi-total",     "children"),
#     Output("kpi-high",      "children"),
#     Output("kpi-avg-score", "children"),
#     Output("kpi-rate",      "children"),
#     Output("kpi-services",  "children"),
#     Output("graph-timeline","figure"),
#     Output("graph-score",   "figure"),
#     Output("graph-services","figure"),
#     Output("health-panel",  "children"),
#     Input("interval", "n_intervals"),
# )
# def refresh_dashboard(_):
#     with _lock:
#         rows  = list(_buffer)
#         total = _total_received

#     high_rows   = [r for r in rows if r.get("_ratio_val", 0) > 1.3]
#     scores      = [r.get("_score_val", 0) for r in rows if "_score_val" in r]
#     avg_score   = sum(scores) / len(scores) if scores else 0
#     services    = len({r["Source"] for r in high_rows if r.get("Source")})

#     # Anomalies per minute (last 5 min)
#     now  = datetime.now()
#     rate = sum(1 for r in rows if r.get("_ratio_val", 0) > 1.3 and _within_minutes(r, now, 1))

#     health = [
#         _health_badge("Kafka Broker",      ok=True),
#         _health_badge("Kubernetes Cluster",ok=True),
#         _health_badge("ML Model API",      ok=total > 0),
#         _health_badge("Spark Streaming",   ok=total > 0),
#     ]

#     display_rows = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]

#     return (
#         str(total),
#         str(len(high_rows)),
#         f"{avg_score:.3f}",
#         f"{rate}/min",
#         str(services),
#         _anomaly_timeline_fig(display_rows),
#         _score_timeline_fig(display_rows),
#         _services_fig(rows),
#         health,
#     )


def _within_minutes(row, now, minutes):
    try:
        ts = datetime.strptime(row["Timestamp"], "%Y-%m-%d %H:%M:%S")
        return (now - ts).total_seconds() <= minutes * 60
    except Exception:
        return False


# ── Alerts callbacks ──────────────────────────────────────────────────────────

@app.callback(
    Output("alert-table",    "data"),
    Output("alert-summary",  "children"),
    Output("alert-last-seen","children"),
    Input("interval", "n_intervals"),
)
def refresh_alerts(_):
    with _lock:
        rows = list(_buffer)

    critical = [r for r in rows if r.get("_ratio_val", 0) > 1.3]

    alert_rows = [{
        "Timestamp": r.get("Timestamp", ""),
        "Source":    r.get("Source", ""),
        "Host":      r.get("Host", ""),
        "Message":   r.get("Message", ""),
        "Score IA":  r.get("Score IA", ""),
        "Ratio":     r.get("Ratio", ""),
    } for r in critical]

    summary   = f"{len(critical)} ALERTE(S) CRITIQUE(S) ACTIVE(S)"
    last_seen = f"Dernier événement : {critical[0]['Timestamp'] if critical else '—'}"
    return alert_rows, summary, last_seen


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)