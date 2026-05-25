"""
LogGuardian — AIOps Platform UI
Consomme logs-anomalies-ml depuis Kafka et affiche en temps réel.
Interface redesignée : sidebar nav, table enrichie, panneau RAG latéral.
"""
import json
import logging
import os
import threading
from collections import deque
from datetime import datetime

import dash
from dash import dash_table, dcc, html
from dash.dependencies import Input, Output, State
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("monitoring-ui")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "logs-anomalies-ml")
MAX_ROWS                = int(os.getenv("MAX_ROWS", "2000"))
REFRESH_INTERVAL_MS     = int(os.getenv("REFRESH_INTERVAL_MS", "3000"))

_buffer: deque = deque(maxlen=MAX_ROWS)
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
    log.info("Consumer confluent-kafka abonné à %s", KAFKA_TOPIC)
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
            row = {
                "Timestamp":    r.get("detected_at", "")[:19].replace("T", " "),
                "Source":       r.get("source", ""),
                "Host":         r.get("host", ""),
                "Message":      r.get("sequence", [{}])[-1].get("message", "")[:120],
                "Score IA":     f"{r.get('anomaly_score', 0):.2f}",
                "Ratio":        f"{r.get('severity_ratio', 0):.2f}x",
                "Statut":       "ANOMALIE" if r.get("severity_ratio", 0) > 1.3 else "NORMAL",
                "_ratio_val":   r.get("severity_ratio", 0),
                "_score_val":   r.get("anomaly_score", 0),
                "_seq":         r.get("sequence", [{}]),
            }
            with _lock:
                _buffer.appendleft(row)
                _total_received += 1
        except Exception as e:
            log.error("Erreur parsing message : %s", e)


threading.Thread(target=_kafka_thread, daemon=True).start()

# ── App ───────────────────────────────────────────────────────────────────────

app = dash.Dash(__name__, title="LogGuardian — AIOps Platform", suppress_callback_exceptions=True)
app.server.config["SECRET_KEY"] = "logguardian"

app.index_string = '''<!DOCTYPE html>
<html>
<head>{%metas%}<title>{%title%}</title>{%favicon%}{%css%}
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { height: 100%; background: #0b0d14; overflow: hidden; }

  /* Scrollbar */
  ::-webkit-scrollbar { width: 4px; height: 4px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: #2a2f45; border-radius: 2px; }

  /* Dropdown overrides */
  .Select-control {
    background: #111420 !important;
    border: 1px solid #1e2236 !important;
    border-radius: 6px !important;
    color: #c8d0e7 !important;
    min-height: 36px !important;
    height: 36px !important;
  }
  .Select-value-label { color: #c8d0e7 !important; line-height: 34px !important; }
  .Select-placeholder { color: #4a5270 !important; line-height: 34px !important; }
  .Select-arrow { border-top-color: #4a5270 !important; }
  .Select-menu-outer {
    background: #111420 !important;
    border: 1px solid #1e2236 !important;
    border-radius: 6px !important;
  }
  .VirtualizedSelectOption { background: #111420 !important; color: #c8d0e7 !important; font-size: 12px !important; }
  .VirtualizedSelectFocusedOption { background: #1e2236 !important; color: #fff !important; }
  .Select-input input { color: #c8d0e7 !important; }

  /* Active nav item pulse */
  @keyframes pulse-dot {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.4; }
  }
  .live-dot { animation: pulse-dot 1.5s ease-in-out infinite; }

  /* Row highlight flash */
  @keyframes row-flash {
    0% { background-color: rgba(239,68,68,0.15); }
    100% { background-color: transparent; }
  }

  /* Table row hover */
  .dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner tr:hover td {
    background-color: #1a1f35 !important;
    cursor: pointer;
  }

  /* Selected row */
  .dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner tr.selected td {
    background-color: #1a2540 !important;
  }

  /* Tag badges */
  .tag-anomalie {
    display: inline-flex; align-items: center; gap: 5px;
    background: rgba(239,68,68,0.12);
    color: #ef4444;
    border: 1px solid rgba(239,68,68,0.3);
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 10px;
    font-family: 'Space Mono', monospace;
    font-weight: 700;
    letter-spacing: 0.06em;
  }
  .tag-normal {
    display: inline-flex; align-items: center; gap: 5px;
    background: rgba(34,197,94,0.08);
    color: #22c55e;
    border: 1px solid rgba(34,197,94,0.2);
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 10px;
    font-family: 'Space Mono', monospace;
    font-weight: 700;
    letter-spacing: 0.06em;
  }
</style>
</head>
<body>{%app_entry%}<footer>{%config%}{%scripts%}{%renderer%}</footer></body>
</html>'''

# ── Color palette ─────────────────────────────────────────────────────────────
C = {
    "bg":        "#0b0d14",
    "sidebar":   "#0d0f1a",
    "surface":   "#111420",
    "surface2":  "#161929",
    "border":    "#1e2236",
    "border2":   "#252a42",
    "text":      "#c8d0e7",
    "muted":     "#4a5270",
    "muted2":    "#6b7494",
    "danger":    "#ef4444",
    "success":   "#22c55e",
    "warning":   "#f59e0b",
    "cyan":      "#38bdf8",
    "blue":      "#3b82f6",
    "accent":    "#6366f1",
}

TABLE_COLS = ["Timestamp", "Source", "Message", "Score IA", "Statut"]

# ── Sidebar icons (SVG inline) ────────────────────────────────────────────────
def _icon_dashboard():
    return html.Span("▦", style={"fontSize": "18px", "width": "18px", "textAlign": "center"})

    # return html.Svg(viewBox="0 0 24 24", fill="none", stroke="currentColor",
    #     style={"width": "18px", "height": "18px", "strokeWidth": "1.8"},
    #     children=[
    #         html.Path(d="M3 3h7v9H3zM14 3h7v5h-7zM14 12h7v9h-7zM3 16h7v5H3z",
    #                   strokeLinecap="round", strokeLinejoin="round")
    #     ])

def _icon_logs():
    return html.Span("☰", style={"fontSize": "18px", "width": "18px", "textAlign": "center"})

    # return html.Svg(viewBox="0 0 24 24", fill="none", stroke="currentColor",
    #     style={"width": "18px", "height": "18px", "strokeWidth": "1.8"},
    #     children=[
    #         html.Path(d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-3 7h3m-3 4h3m-6-4h.01m-.01 4h.01",
    #                   strokeLinecap="round", strokeLinejoin="round")
    #     ])

def _icon_alert():
    return html.Span("⚠", style={"fontSize": "18px", "width": "18px", "textAlign": "center"})

    # return html.Svg(viewBox="0 0 24 24", fill="none", stroke="currentColor",
    #     style={"width": "18px", "height": "18px", "strokeWidth": "1.8"},
    #     children=[
    #         html.Path(d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z",
    #                   strokeLinecap="round", strokeLinejoin="round")
    #     ])

def _icon_settings():
    return html.Span("⚙", style={"fontSize": "18px", "width": "18px", "textAlign": "center"})

    # return html.Svg(viewBox="0 0 24 24", fill="none", stroke="currentColor",
    #     style={"width": "18px", "height": "18px", "strokeWidth": "1.8"},
    #     children=[
    #         html.Path(d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z",
    #                   strokeLinecap="round", strokeLinejoin="round"),
    #         html.Circle(cx="12", cy="12", r="3", strokeLinecap="round", strokeLinejoin="round")
    #     ])

def _icon_export():
    return html.Span("⇧", style={"fontSize": "18px", "width": "18px", "textAlign": "center"})
    # return html.Svg(viewBox="0 0 24 24", fill="none", stroke="currentColor",
    #     style={"width": "18px", "height": "18px", "strokeWidth": "1.8"},
    #     children=[
    #         html.Path(d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12",
    #                   strokeLinecap="round", strokeLinejoin="round")
    #     ])

def _nav_item(icon, label, active=False):
    return html.Div(style={
        "display": "flex",
        "alignItems": "center",
        "gap": "10px",
        "padding": "10px 14px",
        "borderRadius": "8px",
        "cursor": "pointer",
        "backgroundColor": C["surface"] if active else "transparent",
        "color": C["text"] if active else C["muted2"],
        "fontSize": "13px",
        "fontWeight": "500",
        "fontFamily": "'DM Sans', sans-serif",
        "transition": "all 0.15s",
        "marginBottom": "2px",
        "borderLeft": f"2px solid {C['accent']}" if active else "2px solid transparent",
    }, children=[icon, html.Span(label)])

def _sidebar():
    # Shield icon for logo
    shield_svg = html.Div("🛡️", style={
        "width": "22px",
        "height": "22px",
        "display": "flex",
        "alignItems": "center",
        "justifyContent": "center",
        "fontSize": "18px",
    })
    # # shield_svg = html.Svg(viewBox="0 0 24 24", fill="none",
    #     style={"width": "22px", "height": "22px"},
    #     children=[
    #         html.Path(d="M12 2L3 7v5c0 5.25 3.75 10.15 9 11.35C17.25 22.15 21 17.25 21 12V7L12 2z",
    #                   fill=C["accent"], opacity="0.9"),
    #         html.Path(d="M9 12l2 2 4-4", stroke="white", strokeWidth="1.5",
    #                   strokeLinecap="round", strokeLinejoin="round")
    #     ])

    return html.Div(style={
        "width": "200px",
        "minWidth": "200px",
        "backgroundColor": C["sidebar"],
        "borderRight": f"1px solid {C['border']}",
        "display": "flex",
        "flexDirection": "column",
        "padding": "0",
        "height": "100%",
    }, children=[
        # Logo
        html.Div(style={
            "padding": "20px 16px 18px",
            "borderBottom": f"1px solid {C['border']}",
            "display": "flex",
            "alignItems": "center",
            "gap": "10px",
        }, children=[
            shield_svg,
            html.Div([
                html.Span("LogGuardian", style={
                    "fontSize": "13px", "fontWeight": "700",
                    "color": C["text"], "fontFamily": "'DM Sans', sans-serif",
                    "letterSpacing": "-0.01em",
                }),
                html.Div("AIOps Platform", style={
                    "fontSize": "9px", "color": C["muted"], "letterSpacing": "0.08em",
                    "textTransform": "uppercase", "fontFamily": "'Space Mono', monospace",
                    "marginTop": "1px",
                }),
            ]),
        ]),

        # Nav items
        html.Div(style={"padding": "14px 10px", "flex": "1"}, children=[
            _nav_item(_icon_dashboard(), "Dashboard"),
            _nav_item(_icon_logs(), "Historique des Logs", active=True),
            _nav_item(_icon_alert(), "Alertes"),
            _nav_item(_icon_settings(), "Paramètres"),
            _nav_item(_icon_export(), "Export"),
        ]),

        # Live indicator
        html.Div(style={
            "padding": "14px 16px",
            "borderTop": f"1px solid {C['border']}",
            "display": "flex",
            "alignItems": "center",
            "gap": "8px",
        }, children=[
            html.Div(className="live-dot", style={
                "width": "7px", "height": "7px",
                "borderRadius": "50%",
                "backgroundColor": C["success"],
                "boxShadow": f"0 0 6px {C['success']}",
                "flexShrink": "0",
            }),
            html.Span("Kafka connecté", style={
                "fontSize": "11px", "color": C["muted2"],
                "fontFamily": "'Space Mono', monospace",
            }),
        ]),
    ])


def _filter_bar():
    return html.Div(style={
        "padding": "14px 20px",
        "borderBottom": f"1px solid {C['border']}",
        "backgroundColor": C["surface2"],
        "flexShrink": "0",
    }, children=[
        html.Div(style={
            "fontSize": "10px", "color": C["muted"], "letterSpacing": "0.1em",
            "textTransform": "uppercase", "fontFamily": "'Space Mono', monospace",
            "marginBottom": "10px",
        }, children="Recherche et filtrage dynamiques"),

        html.Div(style={
            "display": "grid",
            "gridTemplateColumns": "1fr 160px 160px 140px",
            "gap": "10px",
        }, children=[
            dcc.Input(
                id="search-text", type="text", debounce=False,
                placeholder="Filtrer par mots-clés : timeout, 404, injection, host...",
                style={
                    "width": "100%", "height": "36px",
                    "backgroundColor": C["bg"],
                    "border": f"1px solid {C['border2']}",
                    "borderRadius": "6px",
                    "color": C["text"], "padding": "0 12px",
                    "outline": "none", "fontSize": "12px",
                    "fontFamily": "'DM Sans', sans-serif",
                },
            ),
            dcc.Dropdown(
                id="source-filter", placeholder="SERVICE (ex: auth-service...)",
                clearable=True,
                style={"fontSize": "11px"},
            ),
            dcc.Dropdown(
                id="level-filter", placeholder="NIVEAU (INFO, WARN...)",
                clearable=True,
                options=[
                    {"label": "Toutes sévérités", "value": "all"},
                    {"label": "ANOMALIE (> 1.3x)", "value": "high"},
                    {"label": "NORMAL (≤ 1.3x)", "value": "normal"},
                ],
                value="all",
                style={"fontSize": "11px"},
            ),
            dcc.Dropdown(
                id="limit-filter", placeholder="PÉRIODE",
                clearable=False,
                options=[
                    {"label": "Temps réel", "value": 200},
                    {"label": "1 heure", "value": 500},
                    {"label": "24 heures", "value": MAX_ROWS},
                ],
                value=200,
                style={"fontSize": "11px"},
            ),
        ]),
    ])


def _rag_panel():
    return html.Div(id="rag-panel", style={
        "width": "300px",
        "minWidth": "300px",
        "backgroundColor": C["surface"],
        "borderLeft": f"1px solid {C['border']}",
        "display": "flex",
        "flexDirection": "column",
        "height": "100%",
    }, children=[
        # Header
        html.Div(style={
            "padding": "14px 16px",
            "borderBottom": f"1px solid {C['border']}",
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "space-between",
        }, children=[
            html.Div(style={"display": "flex", "alignItems": "center", "gap": "8px"}, children=[
                html.Div(style={
                    "width": "26px", "height": "26px",
                    "borderRadius": "6px",
                    "background": f"linear-gradient(135deg, {C['accent']}, {C['blue']})",
                    "display": "flex", "alignItems": "center", "justifyContent": "center",
                    "fontSize": "13px",
                }, children="⚡"),
                html.Span("EXPLICATION IA (RAG)", style={
                    "fontSize": "10px", "fontWeight": "700",
                    "color": C["text"], "letterSpacing": "0.08em",
                    "fontFamily": "'Space Mono', monospace",
                }),
            ]),
            html.Div(style={
                "width": "20px", "height": "20px",
                "borderRadius": "4px",
                "border": f"1px solid {C['border2']}",
                "display": "flex", "alignItems": "center", "justifyContent": "center",
                "cursor": "pointer", "color": C["muted"], "fontSize": "12px",
            }, children="×"),
        ]),

        # Placeholder content (updated by callback when row selected)
        html.Div(id="rag-content", style={"flex": "1", "overflow": "auto", "padding": "16px"}, children=[
            html.Div(style={
                "display": "flex", "flexDirection": "column",
                "alignItems": "center", "justifyContent": "center",
                "height": "100%", "color": C["muted"], "textAlign": "center",
            }, children=[
                html.Div(style={"fontSize": "32px", "marginBottom": "12px", "opacity": "0.3"}, children="⚡"),
                html.P("Sélectionnez un log dans le tableau pour voir l'analyse IA", style={
                    "fontSize": "12px", "lineHeight": "1.6",
                    "fontFamily": "'DM Sans', sans-serif",
                }),
            ])
        ]),
    ])


def _rag_card(row):
    is_anomaly = row.get("Statut") == "ANOMALIE"
    score = float(row.get("Score IA", 0))

    return html.Div(style={"fontFamily": "'DM Sans', sans-serif"}, children=[
        # Description section
        html.Div(style={"marginBottom": "14px"}, children=[
            html.Div("DESCRIPTION DU LOG SÉLECTIONNÉ", style={
                "fontSize": "9px", "color": C["muted"], "letterSpacing": "0.1em",
                "textTransform": "uppercase", "fontFamily": "'Space Mono', monospace",
                "marginBottom": "8px",
            }),
            html.Div(style={
                "backgroundColor": C["surface2"],
                "border": f"1px solid {C['border2']}",
                "borderRadius": "6px",
                "padding": "10px 12px",
                "fontSize": "12px",
                "color": C["text"],
                "lineHeight": "1.6",
            }, children=row.get("Message", "—")[:150]),
        ]),

        # Score bar
        html.Div(style={"marginBottom": "14px"}, children=[
            html.Div(style={
                "display": "flex", "justifyContent": "space-between",
                "marginBottom": "6px",
            }, children=[
                html.Span("Score IA", style={"fontSize": "11px", "color": C["muted2"]}),
                html.Span(f"{score:.2f}", style={
                    "fontSize": "11px", "fontWeight": "700",
                    "color": C["danger"] if is_anomaly else C["success"],
                    "fontFamily": "'Space Mono', monospace",
                }),
            ]),
            html.Div(style={
                "height": "4px",
                "backgroundColor": C["border2"],
                "borderRadius": "2px",
                "overflow": "hidden",
            }, children=[
                html.Div(style={
                    "height": "100%",
                    "width": f"{min(score * 100, 100):.0f}%",
                    "background": f"linear-gradient(90deg, {C['blue']}, {C['danger'] if is_anomaly else C['success']})",
                    "borderRadius": "2px",
                    "transition": "width 0.4s ease",
                }),
            ]),
        ]),

        # RAG Analysis
        html.Div(style={"marginBottom": "14px"}, children=[
            html.Div("ANALYSE DU MODÈLE RAG", style={
                "fontSize": "9px", "color": C["muted"], "letterSpacing": "0.1em",
                "textTransform": "uppercase", "fontFamily": "'Space Mono', monospace",
                "marginBottom": "8px",
            }),
            html.Div(style={
                "backgroundColor": C["surface2"],
                "border": f"1px solid {'rgba(239,68,68,0.25)' if is_anomaly else C['border2']}",
                "borderRadius": "6px",
                "padding": "10px 12px",
                "fontSize": "12px",
                "color": C["text"],
                "lineHeight": "1.7",
                "position": "relative",
            }, children=[
                html.Div(style={
                    "position": "absolute", "top": "0", "left": "0",
                    "width": "3px", "height": "100%", "borderRadius": "6px 0 0 6px",
                    "backgroundColor": C["danger"] if is_anomaly else C["success"],
                }),
                html.Div(style={"paddingLeft": "8px"}, children=(
                    f"Ce log indique une tentative d'injection SQL sur le port 80. "
                    f"Score d'anomalie élevé ({score:.2f}/1.0). "
                    f"Pattern récurrent détecté sur le service {row.get('Source', 'inconnu')}."
                ) if is_anomaly else (
                    f"Log de routine — comportement nominal détecté. "
                    f"Score {score:.2f} en dessous du seuil d'alerte. Aucune action requise."
                )),
            ]),
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
                "borderRadius": "6px",
                "padding": "10px 12px",
            }, children=[
                html.Div(style={
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "space-between",
                    "marginBottom": "4px",
                }, children=[
                    html.Span(
                        "🔄 RESTART DU POD RECOMMANDÉ" if is_anomaly else "✅ AUCUNE ACTION REQUISE",
                        style={
                            "fontSize": "11px", "fontWeight": "700",
                            "color": C["text"], "letterSpacing": "0.04em",
                            "fontFamily": "'Space Mono', monospace",
                        }
                    ),
                    html.Span("↗", style={"color": C["muted"], "fontSize": "14px"}) if is_anomaly else None,
                ]),
                html.P(
                    "Lien direct vers l'action Kubernetes" if is_anomaly else "Continuer la surveillance normale",
                    style={"fontSize": "11px", "color": C["muted2"], "marginTop": "2px"}
                ),
            ]),
        ]),
    ])


_HEADER = {
    "backgroundColor": C["surface2"],
    "color": C["muted"],
    "fontWeight": "600",
    "fontSize": "10px",
    "border": "none",
    "borderBottom": f"1px solid {C['border']}",
    "textTransform": "uppercase",
    "letterSpacing": "0.08em",
    "padding": "10px 14px",
    "fontFamily": "'Space Mono', monospace",
    "whiteSpace": "nowrap",
}
_CELL = {
    "backgroundColor": C["surface"],
    "color": C["text"],
    "fontSize": "12px",
    "border": "none",
    "borderBottom": f"1px solid {C['border']}",
    "padding": "10px 14px",
    "overflow": "hidden",
    "textOverflow": "ellipsis",
    "whiteSpace": "nowrap",
    "fontFamily": "'DM Sans', sans-serif",
}


def _main_table(rows):
    return dash_table.DataTable(
        id="main-table",
        columns=[{"name": c, "id": c} for c in TABLE_COLS],
        data=rows,
        style_table={"overflowX": "auto", "width": "100%", "height": "100%"},
        style_header=_HEADER,
        style_cell=_CELL,
        style_cell_conditional=[
            {"if": {"column_id": "Message"}, "whiteSpace": "normal", "textOverflow": "clip", "maxWidth": "0"},
            {"if": {"column_id": "Timestamp"}, "width": "155px", "minWidth": "155px",
             "fontFamily": "'Space Mono', monospace", "fontSize": "11px", "color": C["muted2"]},
            {"if": {"column_id": "Source"},    "width": "160px", "minWidth": "120px",
             "color": C["cyan"], "fontFamily": "'Space Mono', monospace", "fontSize": "11px"},
            {"if": {"column_id": "Score IA"},  "width": "80px",  "minWidth": "80px",
             "textAlign": "center", "fontFamily": "'Space Mono', monospace", "fontWeight": "700"},
            {"if": {"column_id": "Statut"},    "width": "110px", "minWidth": "110px", "textAlign": "center"},
        ],
        style_data_conditional=[
            # Statut coloring
            {"if": {"filter_query": '{Statut} = "ANOMALIE"', "column_id": "Statut"},
             "color": C["danger"], "fontWeight": "700",
             "fontFamily": "'Space Mono', monospace", "fontSize": "10px", "letterSpacing": "0.06em"},
            {"if": {"filter_query": '{Statut} = "NORMAL"', "column_id": "Statut"},
             "color": C["success"], "fontFamily": "'Space Mono', monospace",
             "fontSize": "10px", "letterSpacing": "0.06em"},
            # Score IA coloring
            {"if": {"filter_query": '{Statut} = "ANOMALIE"', "column_id": "Score IA"},
             "color": C["danger"]},
            {"if": {"filter_query": '{Statut} = "NORMAL"', "column_id": "Score IA"},
             "color": C["success"]},
            # Alternating rows
            {"if": {"row_index": "odd"}, "backgroundColor": C["surface2"]},
            # Anomaly row highlight
            {"if": {"filter_query": '{Statut} = "ANOMALIE"'},
             "borderLeft": f"2px solid {C['danger']}"},
        ],
        page_size=60,
        sort_action="native",
        row_selectable="single",
        selected_rows=[],
    )


# ── Layout ────────────────────────────────────────────────────────────────────

app.layout = html.Div(style={
    "backgroundColor": C["bg"],
    #"height": "100vh",
    "minHeight": "100vh",
    "display": "flex",
    #"flexDirection": "row",
    "fontFamily": "'DM Sans', sans-serif",
    "color": C["text"],
    "overflow": "hidden",
}, children=[
    # Sidebar
    #_sidebar(),

    # Main content
    html.Div(style={
        "flex": "1",
        "display": "flex",
        "flexDirection": "column",
        "overflow": "hidden",
        "minWidth": "0",
    }, children=[
        # Top bar
        html.Div(style={
            "padding": "14px 20px",
            "borderBottom": f"1px solid {C['border']}",
            "backgroundColor": C["surface"],
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "space-between",
            "flexShrink": "0",
        }, children=[
            html.Div(style={"display": "flex", "flexDirection": "column"}, children=[
                html.H1("HISTORIQUE DES LOGS", style={
                    "fontSize": "13px", "fontWeight": "700",
                    "letterSpacing": "0.08em",
                    "color": C["text"],
                    "fontFamily": "'Space Mono', monospace",
                }),
                html.P("(Spark Streaming)", style={
                    "fontSize": "10px", "color": C["muted"],
                    "fontFamily": "'Space Mono', monospace",
                    "marginTop": "1px",
                }),
            ]),
            html.Div(style={"display": "flex", "alignItems": "center", "gap": "20px"}, children=[
                html.Div(id="metrics-bar", style={"display": "flex", "gap": "20px"}),
                html.Div(style={
                    "display": "flex", "alignItems": "center", "gap": "8px",
                    "backgroundColor": C["surface2"],
                    "border": f"1px solid {C['border2']}",
                    "borderRadius": "8px",
                    "padding": "6px 12px",
                    "fontSize": "11px",
                    "fontFamily": "'Space Mono', monospace",
                    "color": C["muted2"],
                }, children=[
                    html.Span("📅"),
                    html.Span(id="date-display", children=datetime.now().strftime("%A %d %B %Y").upper()),
                    html.Span("🕐"),
                    html.Span(id="time-display"),
                ]),
            ]),
        ]),

        # Filter bar
        _filter_bar(),

        # Table + RAG panel row
        html.Div(style={
            "flex": "1",
            "display": "flex",
            "flexDirection": "row",
            "overflow": "hidden",
            "minHeight": "0",
        }, children=[
            # Table area
            html.Div(style={
                "flex": "1",
                "overflow": "auto",
                "minWidth": "0",
            }, id="table-container"),

            # RAG panel
            _rag_panel(),
        ]),
    ]),

    dcc.Interval(id="interval", interval=REFRESH_INTERVAL_MS, n_intervals=0),
    dcc.Store(id="rows-store", data=[]),
])


# ── Callbacks ─────────────────────────────────────────────────────────────────

def _apply_filters(rows, search_text, source_filter, level_filter, limit_filter):
    search_text = (search_text or "").lower().strip()
    level_filter = level_filter or "all"
    limit_filter = int(limit_filter or 200)
    filtered = []
    for r in rows:
        searchable = " ".join([
            str(r.get("Timestamp", "")), str(r.get("Source", "")),
            str(r.get("Message", "")), str(r.get("Score IA", "")),
        ]).lower()
        if search_text and search_text not in searchable:
            continue
        if source_filter and r.get("Source") != source_filter:
            continue
        if level_filter == "high" and r.get("_ratio_val", 0) <= 1.3:
            continue
        if level_filter == "normal" and r.get("_ratio_val", 0) > 1.3:
            continue
        filtered.append(r)
    return filtered[:limit_filter]


def _metric(label, value, color=None):
    return html.Div(style={"textAlign": "center"}, children=[
        html.Div(value, style={
            "fontSize": "20px", "fontWeight": "700",
            "color": color or C["text"],
            "fontFamily": "'Space Mono', monospace",
            "lineHeight": "1.1",
        }),
        html.Div(label, style={
            "fontSize": "9px", "color": C["muted"],
            "letterSpacing": "0.08em", "textTransform": "uppercase",
            "fontFamily": "'Space Mono', monospace",
            "marginTop": "2px",
        }),
    ])


@app.callback(
    Output("metrics-bar", "children"),
    Output("time-display", "children"),
    Output("table-container", "children"),
    Output("source-filter", "options"),
    Output("rows-store", "data"),
    Input("interval", "n_intervals"),
    Input("search-text", "value"),
    Input("source-filter", "value"),
    Input("level-filter", "value"),
    Input("limit-filter", "value"),
)
def refresh(_, search_text, source_filter, level_filter, limit_filter):
    with _lock:
        rows  = [r for r in _buffer if "_ratio_val" in r]
        total = _total_received

    source_options = [
        {"label": s, "value": s}
        for s in sorted({r["Source"] for r in rows if r.get("Source")})
    ]

    filtered = _apply_filters(rows, search_text, source_filter, level_filter, limit_filter)
    high_count = sum(1 for r in filtered if r.get("_ratio_val", 0) > 1.3)
    sources_count = len({r["Source"] for r in filtered}) if filtered else 0

    display_rows = [{k: v for k, v in r.items() if not k.startswith("_")} for r in filtered]

    metrics = [
        _metric("Total", str(total)),
        html.Div(style={"width": "1px", "backgroundColor": C["border"], "margin": "0 4px"}),
        _metric("Anomalies", str(high_count), C["danger"] if high_count else C["muted"]),
        html.Div(style={"width": "1px", "backgroundColor": C["border"], "margin": "0 4px"}),
        _metric("Sources", str(sources_count)),
        html.Div(style={"width": "1px", "backgroundColor": C["border"], "margin": "0 4px"}),
        _metric("Topic", KAFKA_TOPIC[:14], C["muted"]),
    ]

    table = _main_table(display_rows)
    now_time = datetime.now().strftime("%H:%M:%S")

    # Store full rows for RAG lookup
    store_rows = [{k: v for k, v in r.items() if k != "_seq"} for r in filtered]

    return metrics, now_time, table, source_options, store_rows


@app.callback(
    Output("rag-content", "children"),
    Input("main-table", "selected_rows"),
    State("rows-store", "data"),
)
def show_rag(selected_rows, store_data):
    if not selected_rows or not store_data:
        return html.Div(style={
            "display": "flex", "flexDirection": "column",
            "alignItems": "center", "justifyContent": "center",
            "height": "100%", "color": C["muted"], "textAlign": "center",
        }, children=[
            html.Div(style={"fontSize": "32px", "marginBottom": "12px", "opacity": "0.3"}, children="⚡"),
            html.P("Sélectionnez un log dans le tableau pour voir l'analyse IA", style={
                "fontSize": "12px", "lineHeight": "1.6",
                "fontFamily": "'DM Sans', sans-serif",
            }),
        ])

    idx = selected_rows[0]
    if idx >= len(store_data):
        return html.P("Données non disponibles.", style={"color": C["muted"], "fontSize": "12px"})

    row = store_data[idx]
    return _rag_card(row)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)