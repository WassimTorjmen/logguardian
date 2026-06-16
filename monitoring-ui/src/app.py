"""
LogGuardian — AIOps Command Center

Fonctionnalités :
- consommation Kafka temps réel ;
- cockpit de supervision ;
- tableau des logs avec filtres stables ;
- sélection stable d'un log grâce à son identifiant ;
- panneau Analyste IA affiché uniquement à la demande ;
- première analyse Groq contextualisée ;
- régénération Groq après un feedback négatif ;
- sauvegarde des feedbacks positifs et négatifs en JSONL ;
- incident board avec compteur cumulé des alertes.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
from collections import Counter, deque
from difflib import SequenceMatcher
from datetime import datetime
from typing import Any

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    # Docker Compose peut injecter les variables sans python-dotenv.
    pass

import dash
from confluent_kafka import Consumer
from dash import callback_context, dash_table, dcc, html, no_update
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from groq import Groq
import plotly.graph_objects as go
from flask import session


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
log = logging.getLogger("logguardian-ui")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "logs-anomalies-ml",
)
MAX_ROWS = int(os.getenv("MAX_ROWS", "2000"))
REFRESH_INTERVAL_MS = int(os.getenv("REFRESH_INTERVAL_MS", "3000"))
KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    f"monitoring-ui-{os.getpid()}-{int(datetime.now().timestamp())}",
)
ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "1.3"))
FEEDBACK_PATH = os.getenv(
    "FEEDBACK_PATH",
    "/app/feedback/rag_feedback.jsonl",
)

# La variable est toujours définie, même si la clé est absente.
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.getenv(
    "GROQ_MODEL",
    "openai/gpt-oss-20b",
).strip()

# Limite volontairement basse : l'interface demande seulement quelques phrases.
# Cela réduit fortement la consommation quotidienne de tokens.
GROQ_MAX_COMPLETION_TOKENS = int(
    os.getenv("GROQ_MAX_COMPLETION_TOKENS", "550")
)
GROQ_RETRY_COMPLETION_TOKENS = int(
    os.getenv("GROQ_RETRY_COMPLETION_TOKENS", "850")
)

# Facultatif : un autre modèle Groq peut être utilisé si le modèle principal
# a atteint sa limite. Laisser vide pour utiliser directement le fallback local.
GROQ_FALLBACK_MODEL = os.getenv(
    "GROQ_FALLBACK_MODEL",
    "",
).strip()

LOGIN_USERNAME = os.getenv("LOGIN_USERNAME", "admin").strip()
LOGIN_PASSWORD = os.getenv("LOGIN_PASSWORD", "admin").strip()

_buffer: deque[dict[str, Any]] = deque(maxlen=MAX_ROWS)
_lock = threading.Lock()
_total_received = 0
_total_alerts_received = 0
_seen_event_ids: set[str] = set()


# ─────────────────────────────────────────────────────────────────────────────
# THÈME
# ─────────────────────────────────────────────────────────────────────────────
BG = "#eef3fb"
PAPER = "#ffffff"
SIDE = "#071225"
SIDE2 = "#0c1b36"
BD = "#dbe5f1"
BD2 = "#edf2f8"
TXT = "#0f172a"
MUT = "#64748b"
MUT2 = "#94a3b8"
BLUE = "#2563eb"
CYAN = "#0ea5e9"
GREEN = "#10b981"
RED = "#ef4444"
ORAN = "#f97316"
PURP = "#8b5cf6"
YELL = "#f59e0b"

TABLE_COLS = [
    "Timestamp",
    "Source",
    "Host",
    "Message",
    "Score IA",
    "Ratio",
    "Statut",
]
ALERT_COLS = [
    "Timestamp",
    "Source",
    "Host",
    "Message",
    "Score IA",
    "Ratio",
    "Model",
]


# ─────────────────────────────────────────────────────────────────────────────
# OUTILS KAFKA / DONNÉES
# ─────────────────────────────────────────────────────────────────────────────
def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _extract_message(payload: dict[str, Any]) -> str:
    sequence = payload.get("sequence") or []

    if isinstance(sequence, list) and sequence:
        last = sequence[-1]

        if isinstance(last, dict):
            return str(last.get("message", ""))[:300]

        return str(last)[:300]

    return str(payload.get("message", ""))[:300]


def _stable_event_id(payload: dict[str, Any]) -> str:
    """Identifiant stable pendant les rafraîchissements de la table."""
    try:
        serialized = json.dumps(
            payload,
            sort_keys=True,
            ensure_ascii=False,
            default=str,
        )
    except TypeError:
        serialized = str(payload)

    digest = hashlib.sha1(
        serialized.encode("utf-8", errors="ignore")
    ).hexdigest()[:14]

    detected = str(payload.get("detected_at", "unknown"))
    return f"{detected}_{digest}"


def _build_row(payload: dict[str, Any]) -> dict[str, Any]:
    detected = str(payload.get("detected_at", ""))
    score = _safe_float(payload.get("anomaly_score", 0.0))
    ratio = _safe_float(payload.get("severity_ratio", 0.0))
    threshold = _safe_float(
        payload.get("threshold", ALERT_THRESHOLD),
        ALERT_THRESHOLD,
    )
    status = "ANOMALIE" if ratio > threshold else "NORMAL"

    return {
        "id": _stable_event_id(payload),
        "Timestamp": detected[:19].replace("T", " "),
        "Source": str(payload.get("source", "unknown")),
        "Host": str(payload.get("host", "unknown")),
        "Message": _extract_message(payload),
        "Score IA": f"{score:.2f}",
        "Ratio": f"{ratio:.2f}x",
        "Model": str(payload.get("model_version", "unknown")),
        "Statut": status,
        "_score_val": score,
        "_ratio_val": ratio,
        "_threshold_val": threshold,
        "_raw": payload,
    }


def _kafka_thread() -> None:
    global _total_received, _total_alerts_received

    log.info(
        "Kafka consumer start — broker=%s topic=%s",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
    )

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])

    while True:
        message = consumer.poll(timeout=1.0)

        if message is None:
            continue

        if message.error():
            log.warning("Kafka error: %s", message.error())
            continue

        try:
            payload = json.loads(message.value().decode("utf-8"))
            row = _build_row(payload)

            with _lock:
                # Évite d'ajouter plusieurs fois exactement le même événement
                # lorsque Kafka rejoue une partie du topic.
                if row["id"] in _seen_event_ids:
                    continue

                _seen_event_ids.add(row["id"])
                _buffer.appendleft(row)
                _total_received += 1

                if row["Statut"] == "ANOMALIE":
                    _total_alerts_received += 1

        except Exception as error:  # noqa: BLE001
            log.exception("Message parsing error: %s", error)


threading.Thread(
    target=_kafka_thread,
    daemon=True,
).start()


# ─────────────────────────────────────────────────────────────────────────────
# APPLICATION DASH
# ─────────────────────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    title="LogGuardian — Command Center",
    suppress_callback_exceptions=True,
)
app.server.config["SECRET_KEY"] = os.getenv(
    "DASH_SECRET_KEY",
    "logguardian-command-center",
)

app.index_string = f"""<!DOCTYPE html>
<html>
<head>
{{%metas%}}
<title>{{%title%}}</title>
{{%favicon%}}
{{%css%}}
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root {{
  --bg:{BG}; --paper:{PAPER}; --border:{BD}; --border-soft:{BD2};
  --text:{TXT}; --muted:{MUT}; --blue:{BLUE}; --green:{GREEN};
  --red:{RED}; --orange:{ORAN}; --purple:{PURP};
}}
*,*::before,*::after{{box-sizing:border-box}}
html,body{{
  height:100%;margin:0;overflow:hidden;color:var(--text);
  font-family:"Inter",system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
  background:
    radial-gradient(circle at 12% 8%,rgba(37,99,235,.10),transparent 27%),
    radial-gradient(circle at 88% 10%,rgba(139,92,246,.10),transparent 30%),
    linear-gradient(180deg,#f8faff 0%,var(--bg) 100%);
  -webkit-font-smoothing:antialiased;
}}
button,input{{font-family:inherit}}
button{{transition:transform .16s ease,box-shadow .16s ease,background .16s ease,border-color .16s ease}}
button:not(:disabled):hover{{transform:translateY(-1px)}}
button:not(:disabled):active{{transform:translateY(0)}}
::selection{{background:rgba(37,99,235,.16)}}
::-webkit-scrollbar{{width:8px;height:8px}}
::-webkit-scrollbar-track{{background:transparent}}
::-webkit-scrollbar-thumb{{background:#b8c5d8;border-radius:999px;border:2px solid transparent;background-clip:content-box}}
.nav-item{{position:relative;overflow:hidden;transition:all .18s ease!important}}
.nav-item::before{{
  content:"";position:absolute;inset:0 auto 0 0;width:3px;
  background:linear-gradient(180deg,#60a5fa,#8b5cf6);opacity:0;
}}
.nav-item:hover{{
  background:linear-gradient(90deg,rgba(96,165,250,.14),rgba(139,92,246,.06))!important;
  border-color:rgba(148,163,184,.16)!important;transform:translateX(2px);
}}
.nav-item:hover::before{{opacity:1}}
.card-hover{{transition:transform .2s ease,box-shadow .2s ease,border-color .2s ease}}
.card-hover:hover{{transform:translateY(-3px);box-shadow:0 22px 50px rgba(15,30,80,.12)!important}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.45}}}}
.pulse{{animation:pulse 1.9s ease-in-out infinite}}
@keyframes fadeUp{{from{{opacity:0;transform:translateY(8px)}}to{{opacity:1;transform:translateY(0)}}}}
.page-enter{{animation:fadeUp .24s ease both}}

.dash-dropdown .Select-control{{
  height:44px!important;min-height:44px!important;border:1px solid var(--border)!important;
  border-radius:12px!important;background:rgba(255,255,255,.96)!important;
  box-shadow:0 4px 12px rgba(15,23,42,.035)!important;
}}
.dash-dropdown .Select-control:hover,
.dash-dropdown .is-focused:not(.is-open)>.Select-control{{
  border-color:rgba(37,99,235,.42)!important;
  box-shadow:0 0 0 4px rgba(37,99,235,.08)!important;
}}
.dash-dropdown .Select-placeholder,.dash-dropdown .Select-value-label{{
  line-height:42px!important;font-size:12px!important;font-weight:600!important;color:var(--muted)!important;
}}
.dash-dropdown .Select-menu-outer{{
  z-index:9999!important;border:1px solid var(--border)!important;border-radius:14px!important;
  overflow:hidden;box-shadow:0 18px 45px rgba(15,23,42,.14)!important;
}}
.dash-dropdown .VirtualizedSelectOption{{font-size:12px!important;padding:10px 12px!important}}
.dash-dropdown .VirtualizedSelectFocusedOption{{background:#eff6ff!important;color:var(--blue)!important}}

.dash-table-container,.dash-spreadsheet-container,.dash-spreadsheet-inner{{
  width:100%!important;max-width:100%!important;
}}
.dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner tr:hover td{{
  background:linear-gradient(90deg,#eff6ff,#f8fbff)!important;cursor:pointer;
}}
.dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner td[data-active=true]{{
  background:linear-gradient(90deg,#dbeafe,#eef6ff)!important;outline:none!important;
  box-shadow:inset 3px 0 0 var(--blue),inset 0 0 0 1px rgba(37,99,235,.16)!important;
}}
.dash-table-container .previous-next-container{{
  padding:12px 14px!important;border-top:1px solid var(--border-soft);background:#fbfdff;
}}
.rag-panel-enter{{animation:ragIn .24s cubic-bezier(.2,.8,.2,1) both}}
@keyframes ragIn{{from{{opacity:0;transform:translateX(22px) scale(.985)}}to{{opacity:1;transform:none}}}}

@media(max-width:1180px){{
  .responsive-sidebar{{width:220px!important;min-width:220px!important}}
}}
@media(max-width:980px){{
  .responsive-sidebar{{width:86px!important;min-width:86px!important}}
  .sidebar-text{{display:none!important}}
}}
</style>
</head>
<body>
{{%app_entry%}}
<footer>{{%config%}}{{%scripts%}}{{%renderer%}}</footer>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# COMPOSANTS UI
# ─────────────────────────────────────────────────────────────────────────────
def _badge(text: str, color: str, background: str | None = None) -> html.Span:
    return html.Span(
        text,
        style={
            "display": "inline-flex",
            "alignItems": "center",
            "padding": "4px 10px",
            "borderRadius": "999px",
            "fontSize": "10px",
            "fontWeight": "800",
            "color": color,
            "backgroundColor": background or f"{color}18",
            "fontFamily": "JetBrains Mono, monospace",
            "letterSpacing": ".05em",
        },
    )



def _topbar(title: str, subtitle: str, clock_id: str) -> html.Div:
    return html.Div(
        style={
            "height": "76px",
            "minHeight": "76px",
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "space-between",
            "padding": "0 28px",
            "background": "rgba(255,255,255,.76)",
            "backdropFilter": "blur(18px)",
            "WebkitBackdropFilter": "blur(18px)",
            "borderBottom": f"1px solid {BD}",
            "boxShadow": "0 8px 28px rgba(15,23,42,.035)",
            "flexShrink": "0",
            "zIndex": "5",
        },
        children=[
            html.Div([
                html.Div(
                    title,
                    style={
                        "fontSize": "23px",
                        "fontWeight": "900",
                        "color": TXT,
                        "letterSpacing": "-.04em",
                        "lineHeight": "1.05",
                    },
                ),
                html.Div(
                    subtitle,
                    style={
                        "fontSize": "11px",
                        "fontWeight": "500",
                        "color": MUT,
                        "marginTop": "6px",
                    },
                ),
            ]),
            html.Div(
                style={"display": "flex", "alignItems": "center", "gap": "10px"},
                children=[
                    html.Div(
                        style={
                            "display": "flex",
                            "alignItems": "center",
                            "gap": "7px",
                            "color": GREEN,
                            "background": "linear-gradient(180deg,#f0fdf4,#ecfdf5)",
                            "border": "1px solid #bbf7d0",
                            "borderRadius": "999px",
                            "padding": "7px 12px",
                            "fontSize": "10px",
                            "fontWeight": "900",
                            "letterSpacing": ".08em",
                            "boxShadow": "0 5px 16px rgba(16,185,129,.10)",
                        },
                        children=[
                            html.Span(
                                className="pulse",
                                style={
                                    "width": "7px",
                                    "height": "7px",
                                    "display": "inline-block",
                                    "borderRadius": "50%",
                                    "background": GREEN,
                                },
                            ),
                            html.Span("LIVE"),
                        ],
                    ),
                    html.Div(
                        id=clock_id,
                        style={
                            "fontFamily": "JetBrains Mono, monospace",
                            "fontSize": "10px",
                            "fontWeight": "600",
                            "color": MUT,
                            "background": "#f8fafc",
                            "border": f"1px solid {BD}",
                            "padding": "9px 12px",
                            "borderRadius": "11px",
                            "boxShadow": "0 5px 16px rgba(15,23,42,.04)",
                        },
                    ),
                ],
            ),
        ],
    )



def _sidebar() -> html.Div:
    def nav_button(page_id: str, icon: str, label: str, subtitle: str) -> html.Button:
        return html.Button(
            id=f"nav-{page_id}",
            n_clicks=0,
            className="nav-item",
            style={
                "width": "100%",
                "border": "1px solid rgba(148,163,184,.08)",
                "background": "rgba(255,255,255,.025)",
                "color": "white",
                "padding": "11px 12px",
                "borderRadius": "14px",
                "display": "flex",
                "alignItems": "center",
                "gap": "12px",
                "textAlign": "left",
                "cursor": "pointer",
                "marginBottom": "7px",
            },
            children=[
                html.Div(
                    icon,
                    style={
                        "width": "38px",
                        "height": "38px",
                        "minWidth": "38px",
                        "borderRadius": "12px",
                        "display": "grid",
                        "placeItems": "center",
                        "background": "linear-gradient(145deg,rgba(96,165,250,.18),rgba(139,92,246,.12))",
                        "fontSize": "16px",
                        "border": "1px solid rgba(148,163,184,.13)",
                    },
                ),
                html.Div(
                    className="sidebar-text",
                    children=[
                        html.Div(label, style={"fontWeight": "800", "fontSize": "13px"}),
                        html.Div(
                            subtitle,
                            style={
                                "fontSize": "9px",
                                "fontWeight": "500",
                                "color": "#7f91b3",
                                "marginTop": "3px",
                            },
                        ),
                    ],
                ),
            ],
        )

    return html.Div(
        className="responsive-sidebar",
        style={
            "width": "262px",
            "minWidth": "262px",
            "height": "100vh",
            "position": "relative",
            "overflow": "hidden",
            "background": f"linear-gradient(180deg,{SIDE} 0%,{SIDE2} 58%,#0b1730 100%)",
            "display": "flex",
            "flexDirection": "column",
            "boxShadow": "10px 0 40px rgba(7,18,37,.22)",
            "zIndex": "10",
        },
        children=[
            html.Div(
                style={
                    "position": "absolute",
                    "width": "180px",
                    "height": "180px",
                    "borderRadius": "50%",
                    "background": "rgba(37,99,235,.16)",
                    "filter": "blur(52px)",
                    "top": "-70px",
                    "right": "-70px",
                }
            ),
            html.Div(
                style={"padding": "24px 19px 18px", "position": "relative"},
                children=[
                    html.Div(
                        style={"display": "flex", "alignItems": "center", "gap": "13px"},
                        children=[
                            html.Div(
                                "◈",
                                style={
                                    "width": "48px",
                                    "height": "48px",
                                    "minWidth": "48px",
                                    "borderRadius": "20px",
                                    "display": "grid",
                                    "placeItems": "center",
                                    "fontSize": "24px",
                                    "color": "white",
                                    "background": "linear-gradient(135deg,#60a5fa 0%,#2563eb 52%,#7c3aed 100%)",
                                    "boxShadow": "0 14px 34px rgba(37,99,235,.42)",
                                },
                            ),
                            html.Div(
                                className="sidebar-text",
                                children=[
                                    html.Div(
                                        "LogGuardian",
                                        style={
                                            "fontSize": "18px",
                                            "fontWeight": "900",
                                            "color": "white",
                                            "letterSpacing": "-.035em",
                                        },
                                    ),
                                    html.Div(
                                        "AIOps Command Center",
                                        style={
                                            "fontSize": "9px",
                                            "fontWeight": "600",
                                            "color": "#7f91b3",
                                            "marginTop": "3px",
                                            "letterSpacing": ".05em",
                                            "textTransform": "uppercase",
                                        },
                                    ),
                                ],
                            ),
                        ],
                    )
                ],
            ),
            html.Div(
                style={
                    "height": "1px",
                    "background": "linear-gradient(90deg,transparent,rgba(148,163,184,.18),transparent)",
                    "margin": "0 16px 17px",
                }
            ),
            html.Div(
                "Navigation",
                className="sidebar-text",
                style={
                    "fontSize": "9px",
                    "fontWeight": "800",
                    "color": "#42577d",
                    "letterSpacing": ".16em",
                    "padding": "0 20px 11px",
                    "textTransform": "uppercase",
                    "fontFamily": "JetBrains Mono, monospace",
                },
            ),
            html.Div(
                style={"padding": "0 12px", "flex": "1", "minHeight": "0", "overflowY": "auto"},
                children=[
                    nav_button("dashboard", "▦", "Vue cockpit", "KPIs · graphes · risque"),
                    nav_button("logs", "≡", "Flux logs", "Recherche · IA · feedback"),
                    nav_button("alerts", "⚠", "Incident board", "Anomalies critiques"),
                ],
            ),
            html.Button(
                id="logout-button",
                n_clicks=0,
                title="Se déconnecter",
                style={
                    "width": "calc(100% - 28px)",
                    "height": "44px",
                    "margin": "8px 14px 10px",
                    "padding": "0 13px",
                    "borderRadius": "13px",
                    "border": "1px solid rgba(248,113,113,.22)",
                    "background": "linear-gradient(145deg,rgba(248,113,113,.11),rgba(239,68,68,.055))",
                    "color": "#fca5a5",
                    "fontWeight": "800",
                    "fontSize": "11px",
                    "cursor": "pointer",
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "center",
                    "gap": "9px",
                    "boxShadow": "0 8px 20px rgba(127,29,29,.08)",
                },
                children=[
                    html.Span("⏻", style={"fontSize": "16px", "lineHeight": "1"}),
                    html.Span("Déconnexion", className="sidebar-text"),
                ],
            ),
            html.Div(
                style={
                    "margin": "0 14px 16px",
                    "padding": "13px 14px",
                    "background": "linear-gradient(145deg,rgba(255,255,255,.055),rgba(255,255,255,.025))",
                    "border": "1px solid rgba(148,163,184,.12)",
                    "borderRadius": "15px",
                },
                children=[
                    html.Div(
                        style={"display": "flex", "alignItems": "center", "gap": "8px", "marginBottom": "7px"},
                        children=[
                            html.Span(
                                className="pulse",
                                style={
                                    "width": "8px",
                                    "height": "8px",
                                    "borderRadius": "50%",
                                    "background": GREEN,
                                    "display": "inline-block",
                                },
                            ),
                            html.Span(
                                "Kafka connecté",
                                className="sidebar-text",
                                style={"fontSize": "11px", "fontWeight": "800", "color": "#b8c7df"},
                            ),
                        ],
                    ),
                    html.Div(
                        KAFKA_TOPIC,
                        className="sidebar-text",
                        style={
                            "fontSize": "9px",
                            "color": "#60708d",
                            "fontFamily": "JetBrains Mono, monospace",
                            "overflow": "hidden",
                            "textOverflow": "ellipsis",
                            "whiteSpace": "nowrap",
                        },
                    ),
                    html.Div(
                        f"Actualisation · {REFRESH_INTERVAL_MS // 1000}s",
                        className="sidebar-text",
                        style={"fontSize": "9px", "color": "#60a5fa", "marginTop": "7px", "fontWeight": "700"},
                    ),
                ],
            ),
        ],
    )



def _metric_card(label: str, value_id: str, accent: str, icon: str, hint: str) -> html.Div:
    return html.Div(
        className="card-hover",
        style={
            "position": "relative",
            "overflow": "hidden",
            "background": "linear-gradient(145deg,rgba(255,255,255,.99),rgba(248,251,255,.96))",
            "border": f"1px solid {BD}",
            "borderRadius": "20px",
            "padding": "18px 20px",
            "boxShadow": "0 10px 30px rgba(15,30,80,.07)",
            "minHeight": "130px",
        },
        children=[
            html.Div(
                style={
                    "position": "absolute",
                    "width": "92px",
                    "height": "92px",
                    "borderRadius": "50%",
                    "right": "-34px",
                    "top": "-34px",
                    "background": f"{accent}16",
                }
            ),
            html.Div(
                style={
                    "display": "flex",
                    "justifyContent": "space-between",
                    "alignItems": "center",
                    "marginBottom": "15px",
                    "position": "relative",
                },
                children=[
                    html.Div(
                        icon,
                        style={
                            "width": "38px",
                            "height": "38px",
                            "display": "grid",
                            "placeItems": "center",
                            "borderRadius": "12px",
                            "fontSize": "18px",
                            "background": f"{accent}14",
                            "border": f"1px solid {accent}22",
                        },
                    ),
                    html.Span(
                        hint,
                        style={
                            "fontSize": "8px",
                            "fontWeight": "900",
                            "color": accent,
                            "background": f"{accent}10",
                            "border": f"1px solid {accent}1f",
                            "borderRadius": "999px",
                            "padding": "5px 8px",
                            "fontFamily": "JetBrains Mono, monospace",
                            "letterSpacing": ".08em",
                        },
                    ),
                ],
            ),
            html.Div(
                id=value_id,
                children="0",
                style={
                    "fontSize": "31px",
                    "fontWeight": "900",
                    "color": TXT,
                    "fontFamily": "JetBrains Mono, monospace",
                    "letterSpacing": "-.045em",
                    "lineHeight": "1",
                    "marginBottom": "8px",
                    "position": "relative",
                },
            ),
            html.Div(
                label,
                style={
                    "fontSize": "10px",
                    "fontWeight": "800",
                    "color": MUT,
                    "textTransform": "uppercase",
                    "letterSpacing": ".09em",
                    "position": "relative",
                },
            ),
            html.Div(
                style={
                    "position": "absolute",
                    "left": "0",
                    "bottom": "0",
                    "width": "100%",
                    "height": "3px",
                    "background": f"linear-gradient(90deg,{accent},{accent}22 60%,transparent)",
                }
            ),
        ],
    )



def _panel(title: str, children: Any) -> html.Div:
    return html.Div(
        className="card-hover",
        style={
            "background": "rgba(255,255,255,.92)",
            "border": f"1px solid {BD}",
            "borderRadius": "20px",
            "overflow": "hidden",
            "boxShadow": "0 12px 34px rgba(15,30,80,.065)",
            "backdropFilter": "blur(10px)",
        },
        children=[
            html.Div(
                style={
                    "padding": "15px 17px",
                    "borderBottom": f"1px solid {BD2}",
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "9px",
                    "background": "linear-gradient(180deg,#ffffff,#fbfdff)",
                },
                children=[
                    html.Span(
                        style={
                            "width": "8px",
                            "height": "8px",
                            "borderRadius": "50%",
                            "background": "linear-gradient(135deg,#60a5fa,#8b5cf6)",
                            "boxShadow": "0 0 0 4px rgba(37,99,235,.08)",
                        }
                    ),
                    html.Span(
                        title,
                        style={
                            "fontSize": "10px",
                            "fontWeight": "900",
                            "color": MUT,
                            "textTransform": "uppercase",
                            "letterSpacing": ".09em",
                        },
                    ),
                ],
            ),
            html.Div(style={"padding": "15px"}, children=children),
        ],
    )


def _empty_fig(text: str = "En attente de données", height: int = 260) -> go.Figure:
    figure = go.Figure()
    figure.update_layout(
        height=height,
        paper_bgcolor="white",
        plot_bgcolor="white",
        margin=dict(l=20, r=20, t=20, b=20),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        annotations=[
            dict(
                text=text,
                x=.5,
                y=.5,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(color=MUT2, size=13),
            )
        ],
    )
    return figure



def _plot_layout(height: int = 260) -> dict[str, Any]:
    return {
        "height": height,
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        "margin": dict(l=42, r=18, t=22, b=38),
        "font": dict(family="Inter", color=MUT, size=10),
        "xaxis": dict(gridcolor="#eef3f8", zeroline=False, linecolor="#e7edf5"),
        "yaxis": dict(gridcolor="#eef3f8", zeroline=False, linecolor="#e7edf5"),
        "legend": dict(orientation="h", y=1.10, x=0, bgcolor="rgba(0,0,0,0)"),
        "hoverlabel": dict(bgcolor="white", bordercolor=BD, font=dict(color=TXT, family="Inter")),
    }


def _stream_fig(rows: list[dict[str, Any]]) -> go.Figure:
    if not rows:
        return _empty_fig()

    recent = list(reversed(rows[:80]))
    indexes = list(range(len(recent)))
    normal = [1 if row["Statut"] == "NORMAL" else 0 for row in recent]
    anomaly = [1 if row["Statut"] == "ANOMALIE" else 0 for row in recent]

    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=indexes,
            y=normal,
            name="Normal",
            marker_color="#34d399",
        )
    )
    figure.add_trace(
        go.Bar(
            x=indexes,
            y=anomaly,
            name="Anomalie",
            marker_color="#f87171",
        )
    )
    figure.update_layout(**_plot_layout(280), barmode="stack")
    return figure


def _score_fig(rows: list[dict[str, Any]]) -> go.Figure:
    if not rows:
        return _empty_fig()

    recent = list(reversed(rows[:80]))
    values = [row.get("_score_val", 0.0) for row in recent]

    figure = go.Figure(
        go.Scatter(
            x=list(range(len(values))),
            y=values,
            mode="lines",
            name="Score IA",
            line=dict(color=PURP, width=2.5),
            fill="tozeroy",
            fillcolor="rgba(124,58,237,.08)",
        )
    )
    figure.add_hline(
        y=ALERT_THRESHOLD,
        line_dash="dot",
        line_color=RED,
        annotation_text=f"seuil {ALERT_THRESHOLD}",
    )
    figure.update_layout(**_plot_layout(240))
    return figure


def _services_fig(rows: list[dict[str, Any]]) -> go.Figure:
    if not rows:
        return _empty_fig()

    counts = Counter(
        row.get("Source", "unknown")
        for row in rows
        if row.get("Statut") == "ANOMALIE"
    )

    if not counts:
        return _empty_fig("Aucune anomalie")

    top = counts.most_common(8)
    figure = go.Figure(
        go.Bar(
            x=[value for _, value in top],
            y=[name for name, _ in top],
            orientation="h",
            marker_color=CYAN,
        )
    )
    layout = _plot_layout(240)
    layout["margin"] = dict(l=100, r=18, t=20, b=36)
    figure.update_layout(**layout)
    return figure


def _risk_fig(rows: list[dict[str, Any]]) -> go.Figure:
    if not rows:
        value = 0.0
    else:
        anomalies = sum(1 for row in rows if row["Statut"] == "ANOMALIE")
        value = round(anomalies / len(rows) * 100, 1)

    color = GREEN if value < 35 else ORAN if value < 70 else RED
    figure = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=value,
            number={"suffix": "%", "font": {"color": color, "size": 38}},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": color},
                "steps": [
                    {"range": [0, 35], "color": "#f0fdf4"},
                    {"range": [35, 70], "color": "#fff7ed"},
                    {"range": [70, 100], "color": "#fef2f2"},
                ],
            },
        )
    )
    figure.update_layout(
        height=260,
        margin=dict(l=20, r=20, t=20, b=20),
        paper_bgcolor="white",
    )
    return figure


# ─────────────────────────────────────────────────────────────────────────────
# PAGE COCKPIT
# ─────────────────────────────────────────────────────────────────────────────
def _dashboard_page() -> html.Div:
    return html.Div(
        id="page-dashboard",
        className="page-enter",
        style={"display": "none", "height": "100%", "overflow": "auto"},
        children=[
            _topbar(
                "Cockpit observabilité",
                "Vue temps réel — anomalies, risque et santé des services",
                "clock-dashboard",
            ),
            html.Div(
                style={"padding": "22px", "display": "grid", "gap": "18px"},
                children=[
                    html.Div(
                        style={
                            "display": "grid",
                            "gridTemplateColumns": "repeat(5, minmax(0, 1fr))",
                            "gap": "14px",
                        },
                        children=[
                            _metric_card("logs reçus", "m-total", BLUE, "📥", "STREAM"),
                            _metric_card("anomalies", "m-anom", RED, "🔥", "ML"),
                            _metric_card("score moyen", "m-score", PURP, "🧠", "MODEL"),
                            _metric_card("sources touchées", "m-sources", CYAN, "🖥", "SVC"),
                            _metric_card("risk level", "m-risk", ORAN, "⚡", "AIOPS"),
                        ],
                    ),
                    html.Div(
                        style={
                            "display": "grid",
                            "gridTemplateColumns": "1.4fr .6fr",
                            "gap": "18px",
                        },
                        children=[
                            _panel(
                                "Flux anomalies / normal",
                                dcc.Graph(
                                    id="fig-stream",
                                    config={"displayModeBar": False},
                                ),
                            ),
                            _panel(
                                "Jauge risque global",
                                dcc.Graph(
                                    id="fig-risk",
                                    config={"displayModeBar": False},
                                ),
                            ),
                        ],
                    ),
                    html.Div(
                        style={
                            "display": "grid",
                            "gridTemplateColumns": "1fr 1fr 1fr",
                            "gap": "18px",
                        },
                        children=[
                            _panel(
                                "Top services impactés",
                                dcc.Graph(
                                    id="fig-services",
                                    config={"displayModeBar": False},
                                ),
                            ),
                            _panel(
                                "Score IA — derniers événements",
                                dcc.Graph(
                                    id="fig-score",
                                    config={"displayModeBar": False},
                                ),
                            ),
                            _panel(
                                "Briefing analyste IA",
                                html.Div(id="ai-briefing"),
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )


# ─────────────────────────────────────────────────────────────────────────────
# PAGE LOGS
# ─────────────────────────────────────────────────────────────────────────────

def _filter_bar() -> html.Div:
    input_style = {
        "height": "44px",
        "width": "100%",
        "border": f"1px solid {BD}",
        "borderRadius": "12px",
        "padding": "0 14px 0 39px",
        "fontSize": "12px",
        "fontWeight": "500",
        "background": "rgba(255,255,255,.96)",
        "color": TXT,
        "outline": "none",
        "boxShadow": "0 4px 12px rgba(15,23,42,.035)",
    }
    return html.Div(
        style={
            "display": "grid",
            "gridTemplateColumns": "minmax(210px,1fr) 125px 140px 95px",
            "gap": "9px",
            "alignItems": "center",
            "minWidth": "0",
        },
        children=[
            html.Div(
                style={"position": "relative", "minWidth": "0"},
                children=[
                    html.Span(
                        "⌕",
                        style={
                            "position": "absolute",
                            "left": "14px",
                            "top": "50%",
                            "transform": "translateY(-50%)",
                            "fontSize": "18px",
                            "color": MUT2,
                            "zIndex": "2",
                            "pointerEvents": "none",
                        },
                    ),
                    dcc.Input(
                        id="search-text",
                        type="text",
                        debounce=False,
                        placeholder="Rechercher dans les logs…",
                        style=input_style,
                    ),
                ],
            ),
            dcc.Dropdown(id="source-filter", placeholder="Source", clearable=True, className="dash-dropdown"),
            dcc.Dropdown(
                id="level-filter",
                value="all",
                clearable=False,
                className="dash-dropdown",
                options=[
                    {"label": "Tous les statuts", "value": "all"},
                    {"label": "Anomalies", "value": "high"},
                    {"label": "Normaux", "value": "normal"},
                ],
            ),
            dcc.Dropdown(
                id="limit-filter",
                value=200,
                clearable=False,
                className="dash-dropdown",
                options=[
                    {"label": "200", "value": 200},
                    {"label": "500", "value": 500},
                    {"label": "1 000", "value": 1000},
                    {"label": "Max", "value": MAX_ROWS},
                ],
            ),
        ],
    )



def _analyze_button_style(disabled: bool, generated: bool = False) -> dict[str, Any]:
    if generated and not disabled:
        border = "#86efac"
        background = "linear-gradient(180deg,#ecfdf5,#dcfce7)"
        color = "#047857"
        shadow = "0 8px 18px rgba(16,185,129,.14)"
    else:
        border = "#c4b5fd" if not disabled else BD
        background = "linear-gradient(135deg,#7c3aed,#2563eb)" if not disabled else "#f1f5f9"
        color = "white" if not disabled else MUT2
        shadow = "0 10px 24px rgba(99,102,241,.22)" if not disabled else "none"
    return {
        "height": "44px",
        "padding": "0 16px",
        "borderRadius": "12px",
        "border": f"1px solid {border}",
        "background": background,
        "color": color,
        "fontSize": "11px",
        "fontWeight": "900",
        "cursor": "pointer" if not disabled else "not-allowed",
        "whiteSpace": "nowrap",
        "opacity": "1" if not disabled else ".72",
        "boxShadow": shadow,
    }



def _feedback_button_style(
    kind: str,
    disabled: bool = False,
    accepted: bool = False,
) -> dict[str, Any]:
    """Style cohérent des boutons de feedback selon leur état."""
    is_positive = kind == "positive"

    if accepted:
        if is_positive:
            return {
                "height": "42px",
                "borderRadius": "12px",
                "cursor": "not-allowed",
                "border": "1px solid rgba(52,211,153,.55)",
                "background": "linear-gradient(180deg,rgba(52,211,153,.28),rgba(16,185,129,.18))",
                "color": "#bbf7d0",
                "fontWeight": "900",
                "fontSize": "11px",
                "opacity": "1",
                "boxShadow": "0 8px 18px rgba(16,185,129,.14)",
            }

        return {
            "height": "42px",
            "borderRadius": "12px",
            "cursor": "not-allowed",
            "border": "1px solid rgba(148,163,184,.16)",
            "background": "rgba(148,163,184,.08)",
            "color": "#64748b",
            "fontWeight": "900",
            "fontSize": "11px",
            "opacity": ".62",
            "boxShadow": "none",
        }

    if disabled:
        return {
            "height": "42px",
            "borderRadius": "12px",
            "cursor": "not-allowed",
            "border": "1px solid rgba(148,163,184,.14)",
            "background": "rgba(148,163,184,.06)",
            "color": "#64748b",
            "fontWeight": "900",
            "fontSize": "11px",
            "opacity": ".58",
            "boxShadow": "none",
        }

    if is_positive:
        return {
            "height": "42px",
            "borderRadius": "12px",
            "cursor": "pointer",
            "border": "1px solid rgba(52,211,153,.38)",
            "background": "linear-gradient(180deg,rgba(52,211,153,.14),rgba(16,185,129,.08))",
            "color": "#86efac",
            "fontWeight": "900",
            "fontSize": "11px",
            "opacity": "1",
            "boxShadow": "none",
        }

    return {
        "height": "42px",
        "borderRadius": "12px",
        "cursor": "pointer",
        "border": "1px solid rgba(248,113,113,.42)",
        "background": "linear-gradient(180deg,rgba(248,113,113,.13),rgba(239,68,68,.08))",
        "color": "#fecaca",
        "fontWeight": "900",
        "fontSize": "11px",
        "opacity": "1",
        "boxShadow": "none",
    }


def _rag_block(title: str, component_id: str, accent: str, initial_text: str) -> html.Div:
    return html.Div(
        style={"marginBottom": "13px"},
        children=[
            html.Div(
                style={"display": "flex", "alignItems": "center", "gap": "7px", "marginBottom": "7px"},
                children=[
                    html.Span(
                        style={
                            "width": "6px",
                            "height": "6px",
                            "borderRadius": "50%",
                            "background": accent,
                            "boxShadow": f"0 0 0 4px {accent}1c",
                        }
                    ),
                    html.Div(
                        title,
                        style={
                            "fontSize": "9px",
                            "fontWeight": "900",
                            "color": "#91a4c8",
                            "textTransform": "uppercase",
                            "letterSpacing": ".11em",
                            "fontFamily": "JetBrains Mono, monospace",
                        },
                    ),
                ],
            ),
            html.Div(
                id=component_id,
                children=initial_text,
                style={
                    "background": "linear-gradient(145deg,rgba(255,255,255,.075),rgba(255,255,255,.045))",
                    "border": "1px solid rgba(148,163,184,.14)",
                    "borderLeft": f"3px solid {accent}",
                    "borderRadius": "14px",
                    "padding": "12px 13px",
                    "fontSize": "12px",
                    "fontWeight": "500",
                    "lineHeight": "1.66",
                    "color": "#dde7f7",
                    "whiteSpace": "normal",
                    "overflowWrap": "anywhere",
                },
            ),
        ],
    )



def _rag_side_panel() -> html.Div:
    return html.Div(
        className="rag-panel-enter",
        style={
            "height": "100%",
            "minHeight": "0",
            "position": "relative",
            "overflowY": "auto",
            "overflowX": "hidden",
            "background": (
                "radial-gradient(circle at 85% 0%,rgba(124,58,237,.22),transparent 28%),"
                "radial-gradient(circle at 0% 55%,rgba(14,165,233,.12),transparent 30%),"
                "linear-gradient(170deg,#071225 0%,#0d1b34 52%,#101b33 100%)"
            ),
            "border": "1px solid rgba(148,163,184,.13)",
            "borderRadius": "20px",
            "padding": "18px",
            "color": "white",
            "boxShadow": "0 24px 60px rgba(7,18,37,.32)",
        },
        children=[
            html.Div(
                style={
                    "display": "flex",
                    "justifyContent": "space-between",
                    "alignItems": "flex-start",
                    "marginBottom": "16px",
                },
                children=[
                    html.Div([
                        html.Div(
                            style={"display": "flex", "alignItems": "center", "gap": "9px"},
                            children=[
                                html.Div(
                                    "✦",
                                    style={
                                        "width": "34px",
                                        "height": "34px",
                                        "display": "grid",
                                        "placeItems": "center",
                                        "borderRadius": "11px",
                                        "background": "linear-gradient(135deg,#8b5cf6,#2563eb)",
                                        "boxShadow": "0 10px 24px rgba(99,102,241,.30)",
                                    },
                                ),
                                html.Div(
                                    "Analyste IA",
                                    style={"fontSize": "17px", "fontWeight": "900", "letterSpacing": "-.025em"},
                                ),
                            ],
                        ),
                        html.Div(
                            "Analyse contextualisée · recommandation · feedback",
                            style={"fontSize": "9px", "fontWeight": "600", "color": "#7f94bb", "marginTop": "7px"},
                        ),
                    ]),
                    html.Div(
                        style={"display": "flex", "alignItems": "center", "gap": "8px"},
                        children=[
                            html.Div(id="rag-status-chip"),
                            html.Button(
                                "×",
                                id="close-rag-btn",
                                n_clicks=0,
                                title="Fermer",
                                style={
                                    "width": "32px",
                                    "height": "32px",
                                    "borderRadius": "10px",
                                    "border": "1px solid rgba(255,255,255,.14)",
                                    "background": "rgba(255,255,255,.06)",
                                    "color": "#cbd5e1",
                                    "fontSize": "18px",
                                    "cursor": "pointer",
                                },
                            ),
                        ],
                    ),
                ],
            ),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "10px", "marginBottom": "15px"},
                children=[
                    html.Div(
                        style={
                            "background": "linear-gradient(145deg,rgba(139,92,246,.16),rgba(255,255,255,.045))",
                            "border": "1px solid rgba(196,181,253,.18)",
                            "borderRadius": "15px",
                            "padding": "13px",
                        },
                        children=[
                            html.Div(
                                "Score IA",
                                style={
                                    "fontSize": "8px",
                                    "color": "#9aaed2",
                                    "fontWeight": "900",
                                    "textTransform": "uppercase",
                                    "letterSpacing": ".10em",
                                },
                            ),
                            html.Div(
                                id="rag-score",
                                children="—",
                                style={
                                    "fontSize": "25px",
                                    "fontWeight": "900",
                                    "fontFamily": "JetBrains Mono, monospace",
                                    "color": "#c4b5fd",
                                    "marginTop": "7px",
                                },
                            ),
                        ],
                    ),
                    html.Div(
                        style={
                            "background": "linear-gradient(145deg,rgba(249,115,22,.15),rgba(255,255,255,.045))",
                            "border": "1px solid rgba(253,186,116,.18)",
                            "borderRadius": "15px",
                            "padding": "13px",
                        },
                        children=[
                            html.Div(
                                "Ratio",
                                style={
                                    "fontSize": "8px",
                                    "color": "#9aaed2",
                                    "fontWeight": "900",
                                    "textTransform": "uppercase",
                                    "letterSpacing": ".10em",
                                },
                            ),
                            html.Div(
                                id="rag-ratio",
                                children="—",
                                style={
                                    "fontSize": "25px",
                                    "fontWeight": "900",
                                    "fontFamily": "JetBrains Mono, monospace",
                                    "color": "#fdba74",
                                    "marginTop": "7px",
                                },
                            ),
                        ],
                    ),
                ],
            ),
            _rag_block("Log sélectionné", "rag-desc", "#38bdf8", "Sélectionnez un log."),
            _rag_block("Analyse IA", "rag-analysis", "#a78bfa", "Cliquez sur « Analyser avec l’IA »."),
            _rag_block("Action recommandée", "rag-action", "#34d399", "La recommandation apparaîtra après l’analyse."),
            html.Div(
                style={"paddingTop": "14px", "borderTop": "1px solid rgba(148,163,184,.12)"},
                children=[
                    html.Div(
                        style={
                            "display": "flex",
                            "alignItems": "center",
                            "justifyContent": "space-between",
                            "marginBottom": "10px",
                        },
                        children=[
                            html.Div(
                                "Feedback utilisateur",
                                style={
                                    "fontSize": "9px",
                                    "fontWeight": "900",
                                    "color": "#8fa3c7",
                                    "textTransform": "uppercase",
                                    "letterSpacing": ".10em",
                                    "fontFamily": "JetBrains Mono, monospace",
                                },
                            ),
                            html.Div("Aide à améliorer les réponses", style={"fontSize": "8px", "color": "#60779d"}),
                        ],
                    ),
                    html.Div(
                        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "9px"},
                        children=[
                            html.Button(
                                "👍 Utile",
                                id="fb-up",
                                n_clicks=0,
                                disabled=True,
                                style=_feedback_button_style(
                                    "positive",
                                    disabled=True,
                                ),
                            ),
                            html.Button(
                                "👎 Pas utile",
                                id="fb-down",
                                n_clicks=0,
                                disabled=True,
                                style=_feedback_button_style(
                                    "negative",
                                    disabled=True,
                                ),
                            ),
                        ],
                    ),
                    html.Div(
                        id="feedback-status",
                        children="Aucune analyse lancée.",
                        style={
                            "fontSize": "10px",
                            "fontWeight": "500",
                            "color": "#8da1c5",
                            "marginTop": "10px",
                            "lineHeight": "1.55",
                            "padding": "9px 10px",
                            "borderRadius": "10px",
                            "background": "rgba(255,255,255,.035)",
                            "border": "1px solid rgba(148,163,184,.08)",
                        },
                    ),
                ],
            ),
        ],
    )



def _logs_workspace_style(opened: bool) -> dict[str, Any]:
    return {
        "height": "calc(100% - 76px)",
        "display": "grid",
        "gridTemplateColumns": "minmax(0,1fr) minmax(390px,430px)" if opened else "minmax(0,1fr)",
        "gap": "14px" if opened else "0",
        "padding": "16px",
        "overflow": "hidden",
        "background": "transparent",
        "transition": "grid-template-columns .22s ease, gap .22s ease",
    }



def _rag_wrapper_style(opened: bool) -> dict[str, Any]:
    return {
        "display": "block" if opened else "none",
        "height": "100%",
        "minHeight": "0",
        "minWidth": "0",
        "overflow": "hidden",
        "filter": "drop-shadow(0 16px 28px rgba(15,23,42,.12))",
    }


def _logs_page() -> html.Div:
    return html.Div(
        id="page-logs",
        className="page-enter",
        style={"display": "block", "height": "100%", "overflow": "hidden"},
        children=[
            _topbar(
                "Flux logs augmenté",
                "Table temps réel · analyse IA · recommandation · feedback",
                "clock-logs",
            ),
            html.Div(
                id="logs-workspace",
                style=_logs_workspace_style(False),
                children=[
                    html.Div(
                        style={
                            "display": "flex",
                            "flexDirection": "column",
                            "height": "100%",
                            "minWidth": "0",
                            "minHeight": "0",
                            "overflow": "hidden",
                            "background": PAPER,
                            "border": f"1px solid {BD}",
                            "borderRadius": "15px",
                        },
                        children=[
                            html.Div(
                                style={
                                    "minHeight": "76px",
                                    "padding": "14px 15px",
                                    "display": "grid",
                                    "gridTemplateColumns": "minmax(0,1fr) 92px 178px",
                                    "gap": "10px",
                                    "alignItems": "center",
                                    "borderBottom": f"1px solid {BD2}",
                                    "background": "#fafcff",
                                    "overflow": "hidden",
                                },
                                children=[
                                    _filter_bar(),
                                    html.Div(
                                        id="logs-count",
                                        style={
                                            "fontSize": "10px",
                                            "fontWeight": "800",
                                            "color": MUT2,
                                            "whiteSpace": "nowrap",
                                            "textAlign": "center",
                                            "fontFamily": (
                                                "JetBrains Mono, monospace"
                                            ),
                                        },
                                    ),
                                    html.Button(
                                        "✨ Analyser avec l’IA",
                                        id="show-rag-btn",
                                        n_clicks=0,
                                        disabled=True,
                                        style=_analyze_button_style(True),
                                    ),
                                ],
                            ),
                            html.Div(
                                style={
                                    "flex": "1",
                                    "minHeight": "0",
                                    "minWidth": "0",
                                    "overflow": "hidden",
                                },
                                children=[
                                    dash_table.DataTable(
                                        id="main-table",
                                        columns=[
                                            {"name": column, "id": column}
                                            for column in TABLE_COLS
                                        ],
                                        data=[],
                                        cell_selectable=True,
                                        active_cell=None,
                                        sort_action="native",
                                        page_action="native",
                                        page_size=22,
                                        fixed_rows={"headers": True},
                                        style_table={
                                            "height": "100%",
                                            "width": "100%",
                                            "maxWidth": "100%",
                                            "overflowX": "auto",
                                            "overflowY": "auto",
                                        },
                                        style_header={
                                            "backgroundColor": "#f0f4fa",
                                            "fontWeight": "800",
                                            "fontSize": "9px",
                                            "color": MUT,
                                            "border": "none",
                                            "borderBottom": f"2px solid {BD}",
                                            "padding": "10px 10px",
                                            "textTransform": "uppercase",
                                            "letterSpacing": ".06em",
                                            "fontFamily": (
                                                "JetBrains Mono, monospace"
                                            ),
                                        },
                                        style_cell={
                                            "backgroundColor": PAPER,
                                            "fontSize": "11px",
                                            "color": TXT,
                                            "border": "none",
                                            "borderBottom": f"1px solid {BD2}",
                                            "padding": "9px 10px",
                                            "whiteSpace": "nowrap",
                                            "overflow": "hidden",
                                            "textOverflow": "ellipsis",
                                            "maxWidth": "0",
                                        },
                                        style_cell_conditional=[
                                            {
                                                "if": {"column_id": "Timestamp"},
                                                "width": "145px",
                                                "minWidth": "145px",
                                                "maxWidth": "145px",
                                                "fontFamily": (
                                                    "JetBrains Mono, monospace"
                                                ),
                                                "color": MUT,
                                            },
                                            {
                                                "if": {"column_id": "Source"},
                                                "width": "85px",
                                                "minWidth": "85px",
                                                "maxWidth": "85px",
                                                "color": BLUE,
                                                "fontWeight": "800",
                                            },
                                            {
                                                "if": {"column_id": "Host"},
                                                "width": "85px",
                                                "minWidth": "85px",
                                                "maxWidth": "85px",
                                                "color": MUT,
                                            },
                                            {
                                                "if": {"column_id": "Message"},
                                                "width": "330px",
                                                "minWidth": "330px",
                                                "maxWidth": "520px",
                                                "whiteSpace": "normal",
                                                "lineHeight": "1.45",
                                            },
                                            {
                                                "if": {"column_id": "Score IA"},
                                                "width": "75px",
                                                "minWidth": "75px",
                                                "maxWidth": "75px",
                                                "fontFamily": (
                                                    "JetBrains Mono, monospace"
                                                ),
                                                "fontWeight": "800",
                                                "textAlign": "right",
                                            },
                                            {
                                                "if": {"column_id": "Ratio"},
                                                "width": "70px",
                                                "minWidth": "70px",
                                                "maxWidth": "70px",
                                                "fontFamily": (
                                                    "JetBrains Mono, monospace"
                                                ),
                                                "fontWeight": "800",
                                                "textAlign": "center",
                                            },
                                            {
                                                "if": {"column_id": "Statut"},
                                                "width": "90px",
                                                "minWidth": "90px",
                                                "maxWidth": "90px",
                                                "fontWeight": "900",
                                                "textAlign": "center",
                                                "fontSize": "10px",
                                            },
                                        ],
                                        style_data_conditional=[
                                            {
                                                "if": {"row_index": "odd"},
                                                "backgroundColor": "#f8faff",
                                            },
                                            {
                                                "if": {
                                                    "filter_query": (
                                                        '{Statut} = "ANOMALIE"'
                                                    ),
                                                    "column_id": "Statut",
                                                },
                                                "color": RED,
                                            },
                                            {
                                                "if": {
                                                    "filter_query": (
                                                        '{Statut} = "NORMAL"'
                                                    ),
                                                    "column_id": "Statut",
                                                },
                                                "color": GREEN,
                                            },
                                            {
                                                "if": {
                                                    "filter_query": (
                                                        '{Statut} = "ANOMALIE"'
                                                    ),
                                                    "column_id": "Score IA",
                                                },
                                                "color": RED,
                                            },
                                            {
                                                "if": {
                                                    "filter_query": (
                                                        '{Statut} = "ANOMALIE"'
                                                    ),
                                                    "column_id": "Ratio",
                                                },
                                                "color": ORAN,
                                            },
                                            {
                                                "if": {"state": "selected"},
                                                "backgroundColor": "#dbeafe",
                                                "border": f"1px solid {BLUE}",
                                            },
                                        ],
                                    )
                                ],
                            ),
                        ],
                    ),
                    html.Div(
                        id="rag-panel-wrapper",
                        style=_rag_wrapper_style(False),
                        children=[_rag_side_panel()],
                    ),
                ],
            ),
        ],
    )


# ─────────────────────────────────────────────────────────────────────────────
# PAGE INCIDENTS
# ─────────────────────────────────────────────────────────────────────────────
def _alerts_page() -> html.Div:
    return html.Div(
        id="page-alerts",
        className="page-enter",
        style={"display": "none", "height": "100%", "overflow": "auto"},
        children=[
            _topbar(
                "Incident board",
                "Historique des événements classés comme anomalies",
                "clock-alerts",
            ),
            html.Div(
                style={"padding": "22px", "display": "grid", "gap": "18px"},
                children=[
                    html.Div(id="alert-strip"),
                    html.Div(
                        style={
                            "background": PAPER,
                            "border": f"1px solid {BD}",
                            "borderRadius": "16px",
                            "overflow": "hidden",
                        },
                        children=[
                            dash_table.DataTable(
                                id="alert-table",
                                columns=[
                                    {"name": column, "id": column}
                                    for column in ALERT_COLS
                                ],
                                data=[],
                                sort_action="native",
                                page_action="native",
                                page_size=30,
                                style_table={"overflowX": "auto"},
                                style_header={
                                    "backgroundColor": "#fff1f2",
                                    "fontWeight": "800",
                                    "fontSize": "10px",
                                    "color": RED,
                                    "border": "none",
                                    "padding": "11px 14px",
                                    "textTransform": "uppercase",
                                    "fontFamily": (
                                        "JetBrains Mono, monospace"
                                    ),
                                },
                                style_cell={
                                    "backgroundColor": PAPER,
                                    "fontSize": "12px",
                                    "color": TXT,
                                    "border": "none",
                                    "borderBottom": f"1px solid {BD2}",
                                    "padding": "10px 14px",
                                },
                                style_cell_conditional=[
                                    {
                                        "if": {"column_id": "Message"},
                                        "minWidth": "480px",
                                        "whiteSpace": "normal",
                                    },
                                    {
                                        "if": {"column_id": "Ratio"},
                                        "color": RED,
                                        "fontWeight": "900",
                                    },
                                ],
                            )
                        ],
                    ),
                ],
            ),
        ],
    )


# ─────────────────────────────────────────────────────────────────────────────
# ROOT LAYOUT
# ─────────────────────────────────────────────────────────────────────────────
def _login_page() -> html.Div:
    return html.Div(
        style={
            "minHeight": "100vh",
            "display": "grid",
            "placeItems": "center",
            "padding": "24px",
            "background": (
                "radial-gradient(circle at 15% 10%,rgba(37,99,235,.24),transparent 30%),"
                "radial-gradient(circle at 85% 15%,rgba(139,92,246,.22),transparent 32%),"
                "linear-gradient(145deg,#071225,#0c1b36)"
            ),
        },
        children=[
            html.Div(
                style={
                    "width": "100%",
                    "maxWidth": "430px",
                    "padding": "34px",
                    "borderRadius": "24px",
                    "background": "rgba(255,255,255,.97)",
                    "border": "1px solid rgba(255,255,255,.30)",
                    "boxShadow": "0 28px 80px rgba(0,0,0,.35)",
                },
                children=[
                    html.Div(
                        "◈",
                        style={
                            "width": "58px",
                            "height": "58px",
                            "display": "grid",
                            "placeItems": "center",
                            "margin": "0 auto 18px",
                            "borderRadius": "18px",
                            "fontSize": "28px",
                            "color": "white",
                            "background": "linear-gradient(135deg,#60a5fa,#2563eb,#7c3aed)",
                            "boxShadow": "0 15px 35px rgba(37,99,235,.35)",
                        },
                    ),
                    html.H1(
                        "LogGuardian",
                        style={"margin": "0", "textAlign": "center", "fontSize": "28px", "fontWeight": "900", "color": TXT},
                    ),
                    html.P(
                        "Connectez-vous à l'AIOps Command Center",
                        style={"textAlign": "center", "color": MUT, "fontSize": "13px", "marginBottom": "28px"},
                    ),
                    html.Label("Nom d'utilisateur", style={"display": "block", "fontSize": "11px", "fontWeight": "800", "color": MUT, "marginBottom": "7px"}),
                    dcc.Input(
                        id="login-username",
                        type="text",
                        placeholder="Votre identifiant",
                        autoComplete="username",
                        style={"width": "100%", "height": "46px", "padding": "0 14px", "borderRadius": "12px", "border": f"1px solid {BD}", "fontSize": "13px", "outline": "none", "marginBottom": "17px"},
                    ),
                    html.Label("Mot de passe", style={"display": "block", "fontSize": "11px", "fontWeight": "800", "color": MUT, "marginBottom": "7px"}),
                    dcc.Input(
                        id="login-password",
                        type="password",
                        placeholder="Votre mot de passe",
                        autoComplete="current-password",
                        style={"width": "100%", "height": "46px", "padding": "0 14px", "borderRadius": "12px", "border": f"1px solid {BD}", "fontSize": "13px", "outline": "none", "marginBottom": "20px"},
                    ),
                    html.Button(
                        "Se connecter",
                        id="login-button",
                        n_clicks=0,
                        style={
                            "width": "100%",
                            "height": "48px",
                            "border": "none",
                            "borderRadius": "13px",
                            "cursor": "pointer",
                            "color": "white",
                            "fontSize": "13px",
                            "fontWeight": "900",
                            "background": "linear-gradient(135deg,#7c3aed,#2563eb)",
                            "boxShadow": "0 12px 26px rgba(37,99,235,.25)",
                        },
                    ),
                    html.Div(id="login-error", style={"marginTop": "14px", "textAlign": "center", "fontSize": "11px", "fontWeight": "700", "color": RED, "minHeight": "18px"}),
                ],
            )
        ],
    )


def _authenticated_layout() -> html.Div:
    return html.Div(
        style={
            "height": "100vh",
            "display": "flex",
            "background": (
                "radial-gradient(circle at 15% 10%,rgba(37,99,235,.08),transparent 28%),"
                "radial-gradient(circle at 88% 8%,rgba(139,92,246,.08),transparent 30%),"
                f"{BG}"
            ),
            "overflow": "hidden",
        },
        children=[
            _sidebar(),
            html.Div(
                style={
                    "flex": "1",
                    "minWidth": "0",
                    "height": "100vh",
                    "overflow": "hidden",
                    "display": "flex",
                    "flexDirection": "column",
                },
                children=[
                    _dashboard_page(),
                    _logs_page(),
                    _alerts_page(),
                ],
            ),
            dcc.Interval(
                id="interval",
                interval=REFRESH_INTERVAL_MS,
                n_intervals=0,
            ),
            dcc.Store(id="current-page", data="logs"),
            dcc.Store(id="selected-log-store", data=None),
            dcc.Store(id="rag-open-store", data=False),
            dcc.Store(id="rag-generated-store", data=False),
            dcc.Store(id="feedback-accepted-store", data=False),
        ],
    )


app.layout = html.Div(
    children=[
        dcc.Location(id="auth-location", refresh=False),
        html.Div(id="auth-content"),
    ]
)


@app.callback(
    Output("auth-content", "children"),
    Input("auth-location", "pathname"),
)
def display_authenticated_content(pathname: str | None) -> html.Div:
    del pathname
    if session.get("authenticated"):
        return _authenticated_layout()
    return _login_page()


@app.callback(
    Output("login-error", "children"),
    Output("auth-location", "pathname", allow_duplicate=True),
    Input("login-button", "n_clicks"),
    State("login-username", "value"),
    State("login-password", "value"),
    prevent_initial_call=True,
)
def authenticate_user(n_clicks: int, username: str | None, password: str | None) -> tuple[str, Any]:
    if not n_clicks:
        raise PreventUpdate
    if str(username or "").strip() == LOGIN_USERNAME and str(password or "") == LOGIN_PASSWORD:
        session["authenticated"] = True
        session["username"] = str(username or "").strip()
        return "", "/"
    return "Identifiant ou mot de passe incorrect.", no_update


@app.callback(
    Output("auth-location", "pathname", allow_duplicate=True),
    Input("logout-button", "n_clicks"),
    prevent_initial_call=True,
)
def logout_user(n_clicks: int) -> str:
    if not n_clicks:
        raise PreventUpdate
    session.clear()
    return "/login"


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS MÉTIER
# ─────────────────────────────────────────────────────────────────────────────
def _visible_rows() -> tuple[list[dict[str, Any]], int]:
    with _lock:
        return list(_buffer), _total_received


def _filter_rows(
    rows: list[dict[str, Any]],
    search: str | None,
    source: str | None,
    level: str | None,
    limit: int | None,
) -> list[dict[str, Any]]:
    search_text = (search or "").lower().strip()
    selected_level = level or "all"
    selected_limit = int(limit or 200)
    output: list[dict[str, Any]] = []

    for row in rows:
        if search_text:
            searchable = " ".join(
                [
                    row.get("Timestamp", ""),
                    row.get("Source", ""),
                    row.get("Host", ""),
                    row.get("Message", ""),
                    row.get("Statut", ""),
                ]
            ).lower()

            if search_text not in searchable:
                continue

        if source and row.get("Source") != source:
            continue

        if selected_level == "high" and row.get("Statut") != "ANOMALIE":
            continue

        if selected_level == "normal" and row.get("Statut") != "NORMAL":
            continue

        output.append(row)

    return output[:selected_limit]


def _display_row(
    row: dict[str, Any],
    include_model: bool = False,
) -> dict[str, Any]:
    columns = TABLE_COLS + (["Model"] if include_model else [])
    result = {column: row.get(column, "") for column in columns}
    result["id"] = row.get("id")

    # Le modèle reste disponible dans les données de la table, même s'il
    # n'est pas affiché comme colonne.
    result["Model"] = row.get("Model", "unknown")
    return result


def _selected_metadata(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {
            "desc": "Sélectionnez un log.",
            "score": "—",
            "ratio": "—",
            "status": _badge(
                "AUCUN LOG",
                "#93c5fd",
                "rgba(147,197,253,.13)",
            ),
        }

    is_anomaly = row.get("Statut") == "ANOMALIE"
    return {
        "desc": row.get("Message", "—"),
        "score": str(row.get("Score IA", "—")),
        "ratio": str(row.get("Ratio", "—")),
        "status": _badge(
            row.get("Statut", "UNKNOWN"),
            "#fca5a5" if is_anomaly else "#6ee7b7",
            (
                "rgba(220,38,38,.20)"
                if is_anomaly
                else "rgba(5,150,105,.18)"
            ),
        ),
    }


def _parse_security_context(row: dict[str, Any]) -> dict[str, str]:
    message = str(row.get("Message", ""))
    ip_match = re.search(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", message)
    user_match = re.search(
        r"(?:user=|for user\s+)([a-zA-Z0-9_.-]+)",
        message,
        flags=re.IGNORECASE,
    )

    return {
        "ip": ip_match.group(0) if ip_match else "non détectée",
        "user": user_match.group(1) if user_match else "non détecté",
    }


def _format_log_sequence(
    row: dict[str, Any],
    max_items: int = 10,
) -> str:
    raw = row.get("_raw") or {}
    sequence = raw.get("sequence") or []

    if not isinstance(sequence, list) or not sequence:
        return "Aucun contexte de séquence disponible."

    lines: list[str] = []

    for item in sequence[-max_items:]:
        if not isinstance(item, dict):
            continue

        timestamp = item.get("timestamp", "date inconnue")
        level = item.get("level", "niveau inconnu")
        message = item.get("message", "")
        lines.append(f"- [{timestamp}] {level}: {message}")

    return "\n".join(lines) or "Aucun contexte de séquence disponible."



NORMAL_NO_ACTION = (
    "Aucune action corrective n’est requise. "
    "Conserver ce log dans l’historique et maintenir la surveillance habituelle."
)


def _text_similarity(left: str | None, right: str | None) -> float:
    left_text = " ".join(str(left or "").lower().split())
    right_text = " ".join(str(right or "").lower().split())

    if not left_text or not right_text:
        return 0.0

    return SequenceMatcher(
        None,
        left_text,
        right_text,
    ).ratio()


def _local_alternative_action(
    row: dict[str, Any],
    rejected_action: str | None,
) -> str:
    """
    Garantit une action différente lorsque Groq renvoie une recommandation
    trop proche de celle que l'utilisateur vient de rejeter.
    """
    if row.get("Statut") != "ANOMALIE":
        return NORMAL_NO_ACTION

    message = str(row.get("Message", "")).lower()

    if "exited abnormally" in message or "exit" in message:
        candidates = [
            (
                "Rechercher le code de sortie exact et comparer cet arrêt aux "
                "exécutions précédentes du même service avant toute relance."
            ),
            (
                "Inspecter les événements immédiatement antérieurs à l’arrêt afin "
                "d’identifier une dépendance manquante, une erreur de configuration "
                "ou une saturation de ressources."
            ),
        ]
    elif "session opened" in message or "session closed" in message:
        candidates = [
            (
                "Comparer l’utilisateur, l’horaire et la fréquence de cette session "
                "avec l’activité habituelle, puis vérifier les journaux "
                "d’authentification associés."
            ),
            (
                "Contrôler la cohérence entre les ouvertures et fermetures de session "
                "et rechercher des répétitions anormalement rapprochées."
            ),
        ]
    elif "cupsd" in message:
        candidates = [
            (
                "Vérifier l’état du service CUPS et comparer cet événement à son "
                "historique récent afin de déterminer pourquoi sa fréquence ou son "
                "ordre dans la séquence est inhabituel."
            ),
            (
                "Examiner les événements CUPS voisins sans redémarrer le service tant "
                "qu’aucun échec fonctionnel n’est confirmé."
            ),
        ]
    elif any(
        token in message
        for token in ["authentication", "kerberos", "ssh"]
    ):
        candidates = [
            (
                "Vérifier l’identité de l’utilisateur, l’origine de la connexion et "
                "la répétition des tentatives dans les journaux d’authentification."
            ),
            (
                "Comparer cette authentification aux habitudes récentes du compte et "
                "aux connexions provenant du même hôte ou de la même adresse IP."
            ),
        ]
    elif any(token in message for token in ["timeout", "connection"]):
        candidates = [
            (
                "Mesurer la disponibilité du service distant et la latence réseau, "
                "puis comparer les délais observés aux valeurs habituelles."
            ),
            (
                "Identifier le composant distant concerné et vérifier les erreurs de "
                "connexion voisines avant de modifier les paramètres de timeout."
            ),
        ]
    else:
        candidates = [
            (
                "Examiner les événements immédiatement avant et après ce log, puis "
                "comparer le comportement du service à son historique récent."
            ),
            (
                "Vérifier l’état du composant concerné et confirmer que l’écart "
                "statistique correspond bien à un incident avant toute intervention."
            ),
        ]

    for candidate in candidates:
        if _text_similarity(candidate, rejected_action) < 0.72:
            return candidate

    return (
        "Effectuer une vérification ciblée du contexte immédiat de ce log et "
        "documenter l’écart observé avant de décider d’une intervention."
    )


def _fallback_rag(row: dict[str, Any]) -> dict[str, str]:
    """Explication utile affichée si Groq est indisponible."""
    message = str(row.get("Message", ""))
    lower_message = message.lower()
    source = row.get("Source", "unknown")
    host = row.get("Host", "unknown")
    ratio = row.get("Ratio", "—")
    threshold = row.get("_threshold_val", ALERT_THRESHOLD)
    is_anomaly = row.get("Statut") == "ANOMALIE"

    if "exited abnormally" in lower_message or "exit" in lower_message:
        meaning = (
            "Le message indique qu’un processus ou un service s’est arrêté "
            "de manière anormale, avec un code de sortie non nul."
        )
        action = (
            "Identifier le processus concerné, consulter ses logs juste avant "
            "l’arrêt et vérifier sa configuration ou ses dépendances."
        )
    elif "session opened" in lower_message:
        meaning = (
            "Le message indique l’ouverture réussie d’une session utilisateur. "
            "Pris isolément, ce n’est pas nécessairement une erreur."
        )
        action = (
            "Vérifier que l’utilisateur, l’horaire et la fréquence des connexions "
            "sont cohérents avec l’activité attendue."
        )
    elif "session closed" in lower_message:
        meaning = (
            "Le message signale la fermeture d’une session utilisateur. "
            "Cet événement est habituellement normal dans le cycle d’une connexion."
        )
        action = (
            "Contrôler les événements d’ouverture associés et rechercher une "
            "fermeture répétée ou inhabituellement rapide."
        )
    elif "cupsd" in lower_message and "succeeded" in lower_message:
        meaning = (
            "Le message décrit une opération réussie du service d’impression CUPS. "
            "Son contenu est fonctionnellement normal."
        )
        action = (
            "Vérifier surtout pourquoi cet événement apparaît de façon inhabituelle "
            "dans la séquence, sans traiter automatiquement le service comme défaillant."
        )
    elif any(
        token in lower_message
        for token in ["authentication", "kerberos", "ssh"]
    ):
        meaning = (
            "Le message concerne une opération d’authentification. "
            "Il faut distinguer une tentative légitime d’une répétition anormale."
        )
        action = (
            "Contrôler l’utilisateur, l’adresse IP éventuelle, la fréquence des "
            "tentatives et les événements d’authentification voisins."
        )
    elif any(token in lower_message for token in ["timeout", "connection"]):
        meaning = (
            "Le message suggère un problème de délai ou de communication entre "
            "composants."
        )
        action = (
            "Vérifier la disponibilité du service distant, la connectivité réseau "
            "et les paramètres de timeout."
        )
    elif any(
        token in lower_message
        for token in ["error", "failed", "failure"]
    ):
        meaning = (
            "Le message contient un échec explicite qui peut signaler un problème "
            "applicatif, système ou de configuration."
        )
        action = (
            "Examiner les logs précédents et suivants, identifier le composant "
            "responsable et confirmer si l’échec se répète."
        )
    else:
        meaning = (
            "Le message décrit un événement système dont le sens précis dépend du "
            "service et des événements qui l’entourent."
        )
        action = (
            "Corréler ce message avec la séquence complète et vérifier l’état du "
            "service concerné avant toute intervention."
        )

    statistical_text = (
        f"Le modèle le classe comme anomalie car le ratio {ratio} dépasse "
        f"le seuil {threshold:.2f}x."
        if is_anomaly
        else (
            f"Le ratio {ratio} reste sous le seuil {threshold:.2f}x, "
            "le comportement est donc considéré comme nominal."
        )
    )

    if not is_anomaly:
        action = NORMAL_NO_ACTION

    return {
        "analysis": (
            f"{meaning} {statistical_text} "
            f"L’événement provient de {source} sur l’hôte {host}; "
            "le score statistique doit être interprété avec le contexte de la séquence."
        ),
        "action": action,
    }

def _is_groq_rate_limit_error(error: Exception) -> bool:
    error_text = str(error).lower()
    return (
        "rate_limit_exceeded" in error_text
        or "rate limit reached" in error_text
        or "error code: 429" in error_text
    )


def _groq_retry_wait_text(error: Exception) -> str:
    """Retourne une durée propre, par exemple « 6 min 34 s »."""
    match = re.search(
        r"try again in\s+(?:(\d+)m)?([\d.]+)s",
        str(error),
        flags=re.IGNORECASE,
    )

    if not match:
        return "quelques minutes"

    minutes = int(match.group(1) or 0)
    seconds = round(float(match.group(2)))

    if seconds >= 60:
        minutes += seconds // 60
        seconds %= 60

    if minutes:
        return f"{minutes} min {seconds} s"

    return f"{seconds} s"


def _local_regenerated_rag(
    row: dict[str, Any],
    rejected_analysis: str | None,
    rejected_action: str | None,
) -> dict[str, str]:
    """
    Produit une nouvelle réponse locale lorsque Groq est temporairement limité.
    Elle évite d'afficher une erreur brute et change réellement l'analyse/action.
    """
    fallback = _fallback_rag(row)
    message = str(row.get("Message", "inconnu"))
    is_anomaly = row.get("Statut") == "ANOMALIE"

    if is_anomaly:
        alternative_analysis = (
            f"Nouvelle lecture de l'événement « {message} » : le contenu du log "
            "doit être interprété séparément du score statistique. "
            "Le dépassement du seuil indique un comportement inhabituel, mais ne "
            "prouve pas à lui seul une panne ou une compromission. "
            "La priorité est donc de confirmer l'écart dans les événements voisins "
            "et dans l'historique du même service."
        )
        alternative_action = _local_alternative_action(
            row,
            rejected_action,
        )
    else:
        alternative_analysis = (
            f"Le message « {message} » ne présente pas d'échec explicite. "
            "Le modèle conserve cet événement dans la catégorie normale car son "
            "ratio reste sous le seuil configuré. "
            "Aucun élément disponible ne justifie une intervention corrective."
        )
        alternative_action = NORMAL_NO_ACTION

    if _text_similarity(alternative_analysis, rejected_analysis) >= 0.80:
        alternative_analysis = (
            fallback["analysis"]
            + " Cette reformulation locale remplace temporairement l'appel Groq "
              "indisponible."
        )

    return {
        "analysis": alternative_analysis,
        "action": alternative_action,
    }


def _groq_request(
    client: Groq,
    messages: list[dict[str, str]],
    max_tokens: int,
    model: str | None = None,
) -> Any:
    return client.chat.completions.create(
        model=model or GROQ_MODEL,
        messages=messages,
        temperature=0.2,
        max_completion_tokens=max_tokens,
        reasoning_effort="low",
        include_reasoning=False,
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "logguardian_analysis",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": {
                        "analysis": {"type": "string"},
                        "action": {"type": "string"},
                    },
                    "required": ["analysis", "action"],
                    "additionalProperties": False,
                },
            },
        },
    )



def _generate_rag_with_groq(
    row: dict[str, Any],
    rejected_analysis: str | None = None,
    rejected_action: str | None = None,
) -> dict[str, str]:
    if not GROQ_API_KEY:
        raise RuntimeError(
            "GROQ_API_KEY n'est pas configurée dans le conteneur monitoring-ui."
        )

    security = _parse_security_context(row)
    sequence_context = _format_log_sequence(row)
    is_anomaly = row.get("Statut") == "ANOMALIE"

    if rejected_analysis or rejected_action:
        rejection_context = f"""
L'utilisateur a rejeté cette réponse :

ANALYSE REJETÉE
{rejected_analysis or "Non renseignée"}

ACTION REJETÉE
{rejected_action or "Non renseignée"}

Produis une analyse réellement différente.
Si le statut est ANOMALIE, l'action proposée doit également être concrètement
différente de l'action rejetée : ne reformule pas simplement la même idée.
"""
    else:
        rejection_context = """
Il s'agit de la première analyse. Produis directement une explication précise.
Ne te contente pas de dire que le ratio dépasse le seuil.
"""

    action_rule = (
        """
Le statut est ANOMALIE.
Propose une action concrète, proportionnée et directement liée au message.
"""
        if is_anomaly
        else f"""
Le statut est NORMAL.
Ne propose aucune investigation, aucun redémarrage et aucune action corrective.
Le champ action doit indiquer uniquement :
"{NORMAL_NO_ACTION}"
"""
    )

    prompt = f"""
Analyse cet événement LogGuardian.

INFORMATIONS
- Message : {row.get("Message", "inconnu")}
- Source : {row.get("Source", "inconnue")}
- Host : {row.get("Host", "inconnu")}
- Score IA : {row.get("Score IA", "inconnu")}
- Ratio : {row.get("Ratio", "inconnu")}
- Statut : {row.get("Statut", "inconnu")}
- Seuil : {row.get("_threshold_val", ALERT_THRESHOLD)}
- Modèle de détection : {row.get("Model", "unknown")}
- IP détectée : {security["ip"]}
- Utilisateur détecté : {security["user"]}

CONTEXTE DE LA SÉQUENCE
{sequence_context}

{rejection_context}

{action_rule}

CONSIGNES
1. Explique le sens technique du message.
2. Distingue le sens fonctionnel du message de l'anomalie statistique.
3. Utilise le contexte de la séquence quand il apporte une information utile.
4. Ne présente pas automatiquement l'événement comme une attaque.
5. N'invente aucune information absente des données.
6. Indique clairement les limites de l'analyse lorsque le contexte est insuffisant.
7. Réponds en français.
8. Analyse : 3 à 5 phrases maximum.
9. Action : 1 à 3 phrases maximum.
"""

    messages = [
        {
            "role": "system",
            "content": (
                "Tu es un analyste AIOps spécialisé dans l'interprétation "
                "des logs et la réponse aux incidents."
            ),
        },
        {"role": "user", "content": prompt},
    ]

    client = Groq(api_key=GROQ_API_KEY)

    def request_and_parse(
        request_messages: list[dict[str, str]],
        first_tokens: int = GROQ_MAX_COMPLETION_TOKENS,
        retry_tokens: int = GROQ_RETRY_COMPLETION_TOKENS,
    ) -> dict[str, str]:
        try:
            response = _groq_request(
                client,
                request_messages,
                max_tokens=first_tokens,
            )
        except Exception as first_error:  # noqa: BLE001
            error_text = str(first_error).lower()

            # Une limite 429 ne doit pas provoquer une nouvelle requête identique.
            # On essaie éventuellement un modèle de secours configuré.
            if _is_groq_rate_limit_error(first_error):
                if (
                    GROQ_FALLBACK_MODEL
                    and GROQ_FALLBACK_MODEL != GROQ_MODEL
                ):
                    log.warning(
                        "Limite Groq atteinte sur %s. Essai du modèle de secours %s.",
                        GROQ_MODEL,
                        GROQ_FALLBACK_MODEL,
                    )
                    response = _groq_request(
                        client,
                        request_messages,
                        max_tokens=GROQ_MAX_COMPLETION_TOKENS,
                        model=GROQ_FALLBACK_MODEL,
                    )
                else:
                    raise

            elif (
                "json_validate_failed" in error_text
                or "max completion tokens" in error_text
            ):
                log.warning(
                    "Première génération Groq invalide. Nouvelle tentative."
                )
                response = _groq_request(
                    client,
                    request_messages,
                    max_tokens=retry_tokens,
                )
            else:
                raise

        content = response.choices[0].message.content

        if not content:
            raise RuntimeError("Groq a retourné une réponse vide.")

        result = json.loads(content)
        analysis = str(result.get("analysis", "")).strip()
        action = str(result.get("action", "")).strip()

        if not analysis or not action:
            raise RuntimeError("La réponse Groq est incomplète.")

        return {
            "analysis": analysis,
            "action": action,
        }

    result = request_and_parse(messages)

    # Pour un log NORMAL, on interdit toute recommandation corrective,
    # même si le LLM en propose une malgré le prompt.
    if not is_anomaly:
        result["action"] = NORMAL_NO_ACTION
        return result

    # Après un pouce négatif, l'action doit réellement changer.
    if (
        rejected_action
        and _text_similarity(result["action"], rejected_action) >= 0.72
    ):
        retry_messages = [
            *messages,
            {
                "role": "user",
                "content": (
                    "La nouvelle action est encore trop proche de l'action rejetée. "
                    "Génère une autre analyse et surtout une action opérationnelle "
                    "différente dans son objectif, pas seulement dans sa formulation."
                ),
            },
        ]

        second_result = request_and_parse(
            retry_messages,
            first_tokens=650,
            retry_tokens=900,
        )

        if (
            _text_similarity(
                second_result["action"],
                rejected_action,
            )
            < 0.72
        ):
            result = second_result
        else:
            result["action"] = _local_alternative_action(
                row,
                rejected_action,
            )

    return result

def _save_feedback_record(record: dict[str, Any]) -> None:
    directory = os.path.dirname(FEEDBACK_PATH)

    if directory:
        os.makedirs(directory, exist_ok=True)

    with open(FEEDBACK_PATH, "a", encoding="utf-8") as feedback_file:
        feedback_file.write(
            json.dumps(record, ensure_ascii=False) + "\n"
        )


def _briefing(rows: list[dict[str, Any]]) -> html.Div:
    if not rows:
        return html.Div(
            "Aucun événement reçu.",
            style={"color": MUT, "fontSize": "13px"},
        )

    recent = rows[:80]
    anomalies = [row for row in recent if row["Statut"] == "ANOMALIE"]
    source_counts = Counter(row["Source"] for row in anomalies)
    main_source = source_counts.most_common(1)[0][0] if source_counts else "—"

    return html.Div(
        style={"display": "grid", "gap": "12px"},
        children=[
            html.Div(
                f"{len(recent)} événements récents analysés.",
                style={"fontSize": "13px", "fontWeight": "800", "color": TXT},
            ),
            html.Div(
                f"{len(anomalies)} anomalies dans la fenêtre courante.",
                style={"fontSize": "12px", "color": MUT},
            ),
            html.Div(
                f"Source la plus concernée : {main_source}.",
                style={"fontSize": "12px", "color": MUT},
            ),
            html.Div(
                "Prioriser les ratios élevés puis utiliser l'analyse IA pour "
                "interpréter le message et son contexte.",
                style={"fontSize": "12px", "color": MUT, "lineHeight": "1.6"},
            ),
        ],
    )


# ─────────────────────────────────────────────────────────────────────────────
# CALLBACKS — NAVIGATION / TEMPS
# ─────────────────────────────────────────────────────────────────────────────
@app.callback(
    Output("current-page", "data"),
    Input("nav-dashboard", "n_clicks"),
    Input("nav-logs", "n_clicks"),
    Input("nav-alerts", "n_clicks"),
    State("current-page", "data"),
    prevent_initial_call=True,
)
def navigate(
    dashboard_clicks: int,
    logs_clicks: int,
    alerts_clicks: int,
    current_page: str,
) -> str:
    del dashboard_clicks, logs_clicks, alerts_clicks

    if not callback_context.triggered:
        return current_page

    trigger = callback_context.triggered[0]["prop_id"].split(".")[0]
    return {
        "nav-dashboard": "dashboard",
        "nav-logs": "logs",
        "nav-alerts": "alerts",
    }.get(trigger, current_page)


@app.callback(
    Output("page-dashboard", "style"),
    Output("page-logs", "style"),
    Output("page-alerts", "style"),
    Input("current-page", "data"),
)
def show_page(page: str) -> tuple[dict[str, str], ...]:
    dashboard_style = {
        "display": "block",
        "height": "100%",
        "overflow": "auto",
    }
    logs_style = {
        "display": "block",
        "height": "100%",
        "overflow": "hidden",
    }
    hidden = {"display": "none"}

    return (
        dashboard_style if page == "dashboard" else hidden,
        logs_style if page == "logs" else hidden,
        dashboard_style if page == "alerts" else hidden,
    )


@app.callback(
    Output("clock-dashboard", "children"),
    Output("clock-logs", "children"),
    Output("clock-alerts", "children"),
    Input("interval", "n_intervals"),
)
def update_clocks(_: int) -> tuple[str, str, str]:
    value = datetime.now().strftime("%d/%m/%Y • %H:%M:%S")
    return value, value, value


# ─────────────────────────────────────────────────────────────────────────────
# CALLBACKS — COCKPIT
# ─────────────────────────────────────────────────────────────────────────────
@app.callback(
    Output("m-total", "children"),
    Output("m-anom", "children"),
    Output("m-score", "children"),
    Output("m-sources", "children"),
    Output("m-risk", "children"),
    Output("fig-stream", "figure"),
    Output("fig-risk", "figure"),
    Output("fig-services", "figure"),
    Output("fig-score", "figure"),
    Output("ai-briefing", "children"),
    Input("interval", "n_intervals"),
)
def update_dashboard(_: int) -> tuple[Any, ...]:
    rows, total = _visible_rows()
    anomalies = [row for row in rows if row["Statut"] == "ANOMALIE"]
    average_score = (
        sum(row.get("_score_val", 0.0) for row in rows) / len(rows)
        if rows
        else 0.0
    )
    sources = len({row["Source"] for row in anomalies})
    risk = round(len(anomalies) / len(rows) * 100, 1) if rows else 0.0

    return (
        str(total),
        str(len(anomalies)),
        f"{average_score:.2f}",
        str(sources),
        f"{risk}%",
        _stream_fig(rows),
        _risk_fig(rows),
        _services_fig(rows),
        _score_fig(rows),
        _briefing(rows),
    )


# ─────────────────────────────────────────────────────────────────────────────
# CALLBACKS — TABLE / SÉLECTION
# ─────────────────────────────────────────────────────────────────────────────
@app.callback(
    Output("main-table", "data"),
    Output("source-filter", "options"),
    Output("logs-count", "children"),
    Input("interval", "n_intervals"),
    Input("current-page", "data"),
    Input("search-text", "value"),
    Input("source-filter", "value"),
    Input("level-filter", "value"),
    Input("limit-filter", "value"),
)
def update_logs(
    _: int,
    page: str,
    search: str | None,
    source: str | None,
    level: str | None,
    limit: int | None,
) -> tuple[list[dict[str, Any]], list[dict[str, str]], str]:
    del page
    rows, _total = _visible_rows()
    filtered = _filter_rows(rows, search, source, level, limit)
    options = [
        {"label": value, "value": value}
        for value in sorted(
            {row["Source"] for row in rows if row.get("Source")}
        )
    ]

    return (
        [_display_row(row) for row in filtered],
        options,
        f"{len(filtered)} / {len(rows)}",
    )


@app.callback(
    Output("selected-log-store", "data"),
    Input("main-table", "active_cell"),
    State("main-table", "data"),
    prevent_initial_call=True,
)
def store_selected_log(
    active_cell: dict[str, Any] | None,
    table_data: list[dict[str, Any]] | None,
) -> dict[str, Any] | None:
    if not active_cell or not table_data:
        return None

    selected_id = active_cell.get("row_id")

    if not selected_id:
        row_index = active_cell.get("row")

        if row_index is None or row_index >= len(table_data):
            return None

        selected_id = table_data[row_index].get("id")

    with _lock:
        full_row = next(
            (
                row
                for row in _buffer
                if row.get("id") == selected_id
            ),
            None,
        )

    if full_row:
        return full_row

    return next(
        (
            row
            for row in table_data
            if row.get("id") == selected_id
        ),
        None,
    )


@app.callback(
    Output("show-rag-btn", "disabled"),
    Output("show-rag-btn", "style"),
    Output("show-rag-btn", "children"),
    Input("selected-log-store", "data"),
    Input("rag-generated-store", "data"),
    Input("rag-open-store", "data"),
)
def update_analyze_button(
    row: dict[str, Any] | None,
    rag_generated: bool,
    rag_open: bool,
) -> tuple[bool, dict[str, Any], str]:
    disabled = not bool(row)

    if disabled:
        return (
            True,
            _analyze_button_style(True),
            "✨ Analyser avec l’IA",
        )

    if rag_generated:
        label = (
            "🙈 Masquer l’analyse"
            if rag_open
            else "👁 Afficher l’analyse"
        )

        return (
            False,
            _analyze_button_style(False, generated=True),
            label,
        )

    return (
        False,
        _analyze_button_style(False),
        "✨ Analyser avec l’IA",
    )


@app.callback(
    Output("rag-open-store", "data"),
    Input("show-rag-btn", "n_clicks"),
    Input("close-rag-btn", "n_clicks"),
    Input("selected-log-store", "data"),
    State("rag-open-store", "data"),
    prevent_initial_call=True,
)
def change_rag_visibility(
    show_clicks: int,
    close_clicks: int,
    selected_row: dict[str, Any] | None,
    current_open: bool,
) -> bool:
    del show_clicks, close_clicks

    if not callback_context.triggered:
        return bool(current_open)

    trigger = callback_context.triggered[0]["prop_id"].split(".")[0]

    if trigger == "show-rag-btn":
        if not selected_row:
            return False

        # Le même bouton ouvre ou masque le panneau.
        return not bool(current_open)

    # Ferme également le panneau lors de la sélection d'un nouveau log.
    if trigger in {"close-rag-btn", "selected-log-store"}:
        return False

    return bool(current_open)


@app.callback(
    Output("logs-workspace", "style"),
    Output("rag-panel-wrapper", "style"),
    Input("rag-open-store", "data"),
)
def apply_rag_visibility(
    opened: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    return _logs_workspace_style(bool(opened)), _rag_wrapper_style(bool(opened))


@app.callback(
    Output("rag-desc", "children"),
    Output("rag-score", "children"),
    Output("rag-ratio", "children"),
    Output("rag-status-chip", "children"),
    Output("rag-analysis", "children"),
    Output("rag-action", "children"),
    Output("feedback-status", "children"),
    Output("rag-generated-store", "data"),
    Output("feedback-accepted-store", "data"),
    Input("selected-log-store", "data"),
)
def render_selected_log(
    row: dict[str, Any] | None,
) -> tuple[Any, ...]:
    metadata = _selected_metadata(row)

    if not row:
        return (
            metadata["desc"],
            metadata["score"],
            metadata["ratio"],
            metadata["status"],
            "Sélectionnez un log puis cliquez sur « Analyser avec l’IA ».",
            "Aucune recommandation disponible.",
            "Aucune analyse lancée.",
            False,
            False,
        )

    return (
        metadata["desc"],
        metadata["score"],
        metadata["ratio"],
        metadata["status"],
        "Cliquez sur « Analyser avec l’IA » pour obtenir une explication contextualisée.",
        "La recommandation apparaîtra après l’analyse.",
        "Log sélectionné — analyse non lancée.",
        False,
        False,
    )


@app.callback(
    Output("fb-up", "disabled"),
    Output("fb-up", "style"),
    Output("fb-up", "children"),
    Output("fb-down", "disabled"),
    Output("fb-down", "style"),
    Output("fb-down", "children"),
    Input("selected-log-store", "data"),
    Input("rag-generated-store", "data"),
    Input("feedback-accepted-store", "data"),
)
def update_feedback_buttons(
    row: dict[str, Any] | None,
    rag_generated: bool,
    feedback_accepted: bool,
) -> tuple[bool, dict[str, Any], str, bool, dict[str, Any], str]:
    """
    Tant qu'aucune analyse n'existe, les deux boutons sont bloqués.
    Après un clic sur « Utile », les deux boutons sont verrouillés pour
    empêcher tout changement d'avis sur la même réponse.
    """
    feedback_available = bool(row) and bool(rag_generated)

    if not feedback_available:
        return (
            True,
            _feedback_button_style("positive", disabled=True),
            "👍 Utile",
            True,
            _feedback_button_style("negative", disabled=True),
            "👎 Pas utile",
        )

    if feedback_accepted:
        return (
            True,
            _feedback_button_style(
                "positive",
                disabled=True,
                accepted=True,
            ),
            "✅ Réponse validée",
            True,
            _feedback_button_style(
                "negative",
                disabled=True,
                accepted=True,
            ),
            "🔒 Pas utile",
        )

    return (
        False,
        _feedback_button_style("positive"),
        "👍 Utile",
        False,
        _feedback_button_style("negative"),
        "👎 Pas utile",
    )


# ─────────────────────────────────────────────────────────────────────────────
# CALLBACKS — PREMIÈRE ANALYSE / FEEDBACK
# ─────────────────────────────────────────────────────────────────────────────
@app.callback(
    Output("rag-analysis", "children", allow_duplicate=True),
    Output("rag-action", "children", allow_duplicate=True),
    Output("feedback-status", "children", allow_duplicate=True),
    Output("rag-generated-store", "data", allow_duplicate=True),
    Input("show-rag-btn", "n_clicks"),
    State("selected-log-store", "data"),
    State("rag-generated-store", "data"),
    prevent_initial_call=True,
)
def generate_initial_rag(
    n_clicks: int,
    row: dict[str, Any] | None,
    rag_generated: bool,
) -> tuple[Any, ...]:
    if not n_clicks:
        raise PreventUpdate

    if not row:
        return (
            no_update,
            no_update,
            "Sélectionnez d'abord un log.",
            False,
        )

    # Si l'analyse existe déjà, le bouton sert seulement à rouvrir
    # le panneau. Aucun nouvel appel Groq n'est effectué.
    if rag_generated:
        raise PreventUpdate

    try:
        result = _generate_rag_with_groq(row)
        return (
            result["analysis"],
            result["action"],
            f"✨ Analyse générée avec {GROQ_MODEL}.",
            True,
        )

    except Exception as error:  # noqa: BLE001
        log.exception("Erreur pendant l'analyse initiale Groq")
        fallback = _fallback_rag(row)

        # L'explication reste visible même si Groq est indisponible.
        return (
            fallback["analysis"],
            fallback["action"],
            f"⚠️ Analyse locale affichée — Groq indisponible : {error}",
            True,
        )


@app.callback(
    Output("rag-analysis", "children", allow_duplicate=True),
    Output("rag-action", "children", allow_duplicate=True),
    Output("feedback-status", "children", allow_duplicate=True),
    Output("feedback-accepted-store", "data", allow_duplicate=True),
    Input("fb-up", "n_clicks"),
    Input("fb-down", "n_clicks"),
    State("selected-log-store", "data"),
    State("rag-analysis", "children"),
    State("rag-action", "children"),
    State("rag-generated-store", "data"),
    State("feedback-accepted-store", "data"),
    prevent_initial_call=True,
)
def save_feedback(
    up_clicks: int,
    down_clicks: int,
    row: dict[str, Any] | None,
    current_analysis: Any,
    current_action: Any,
    rag_generated: bool,
    feedback_accepted: bool,
) -> tuple[Any, Any, str, Any]:
    del up_clicks, down_clicks

    if not row:
        return (
            no_update,
            no_update,
            "Sélectionnez d'abord un log.",
            no_update,
        )

    if not rag_generated:
        return (
            no_update,
            no_update,
            "Lancez d'abord l'analyse IA avant de donner un feedback.",
            no_update,
        )

    if not callback_context.triggered:
        raise PreventUpdate

    trigger = callback_context.triggered[0]["prop_id"].split(".")[0]
    analysis_text = str(current_analysis or "").strip()
    action_text = str(current_action or "").strip()

    # Garde-fou côté serveur : même si un clic tardif arrive après la
    # validation, aucun feedback négatif supplémentaire n'est accepté.
    if feedback_accepted:
        return (
            no_update,
            no_update,
            "✅ Cette réponse a déjà été validée. Le feedback est verrouillé.",
            True,
        )

    if trigger == "fb-up":
        positive_record = {
            "feedback_timestamp": datetime.now().isoformat(timespec="seconds"),
            "feedback": "positive",
            "accepted": True,
            "log_id": row.get("id"),
            "source": row.get("Source"),
            "host": row.get("Host"),
            "message": row.get("Message"),
            "score_ia": row.get("Score IA"),
            "ratio": row.get("Ratio"),
            "statut": row.get("Statut"),
            "model_version": row.get("Model", "unknown"),
            "accepted_rag_analysis": analysis_text,
            "accepted_rag_recommendation": action_text,
            "generator": GROQ_MODEL,
            "alert_threshold": row.get("_threshold_val", ALERT_THRESHOLD),
        }

        try:
            _save_feedback_record(positive_record)
            return (
                no_update,
                no_update,
                "✅ Explication validée et acceptation enregistrée.",
                True,
            )
        except Exception as error:  # noqa: BLE001
            log.exception("Erreur sauvegarde feedback positif")
            return (
                no_update,
                no_update,
                f"⚠️ Validation non sauvegardée : {error}",
                no_update,
            )

    if trigger != "fb-down":
        raise PreventUpdate

    record: dict[str, Any] = {
        "feedback_timestamp": datetime.now().isoformat(timespec="seconds"),
        "feedback": "negative",
        "log_id": row.get("id"),
        "source": row.get("Source"),
        "host": row.get("Host"),
        "message": row.get("Message"),
        "score_ia": row.get("Score IA"),
        "ratio": row.get("Ratio"),
        "statut": row.get("Statut"),
        "model_version": row.get("Model", "unknown"),
        "rag_analysis": analysis_text,
        "rag_recommendation": action_text,
        "rag_version": "rag_v2_groq",
        "prompt_version": "initial_or_previous_response",
        "regeneration_prompt_version": "groq_regeneration_v2",
        "alert_threshold": row.get("_threshold_val", ALERT_THRESHOLD),
    }

    replacement: dict[str, str] | None = None
    generation_notice: str | None = None

    try:
        replacement = _generate_rag_with_groq(
            row,
            rejected_analysis=analysis_text,
            rejected_action=action_text,
        )
        record.update(
            {
                "replacement_rag_analysis": replacement["analysis"],
                "replacement_rag_recommendation": replacement["action"],
                "replacement_generator": GROQ_MODEL,
                "groq_success": True,
                "groq_error": None,
                "fallback_used": False,
            }
        )

    except Exception as error:  # noqa: BLE001
        log.exception("Erreur pendant la régénération Groq")

        if _is_groq_rate_limit_error(error):
            wait_text = _groq_retry_wait_text(error)
            replacement = _local_regenerated_rag(
                row,
                rejected_analysis=analysis_text,
                rejected_action=action_text,
            )
            generation_notice = (
                "⏳ Quota Groq temporairement atteint. "
                "Une nouvelle réponse locale a été affichée. "
                f"Le modèle Groq devrait être réutilisable dans environ {wait_text}."
            )

            record.update(
                {
                    "replacement_rag_analysis": replacement["analysis"],
                    "replacement_rag_recommendation": replacement["action"],
                    "replacement_generator": "local_rate_limit_fallback",
                    "groq_success": False,
                    "groq_error": str(error),
                    "fallback_used": True,
                    "rate_limit_wait": wait_text,
                }
            )
        else:
            record.update(
                {
                    "replacement_rag_analysis": None,
                    "replacement_rag_recommendation": None,
                    "replacement_generator": GROQ_MODEL,
                    "groq_success": False,
                    "groq_error": str(error),
                    "fallback_used": False,
                }
            )

    try:
        _save_feedback_record(record)
    except Exception as error:  # noqa: BLE001
        log.exception("Erreur sauvegarde feedback négatif")
        return (
            no_update,
            no_update,
            f"⚠️ Feedback non sauvegardé : {error}",
            no_update,
        )

    if replacement is None:
        return (
            no_update,
            no_update,
            (
                "👎 Feedback enregistré, mais la nouvelle analyse n'a pas "
                f"pu être générée : {record['groq_error']}"
            ),
            no_update,
        )

    return (
        replacement["analysis"],
        replacement["action"],
        (
            generation_notice
            or (
                "🔄 Nouvelle réponse générée. Vous pouvez cliquer encore sur "
                "« Pas utile » si nécessaire."
            )
        ),
        False,
    )


# ─────────────────────────────────────────────────────────────────────────────
# CALLBACK — INCIDENT BOARD
# ─────────────────────────────────────────────────────────────────────────────
@app.callback(
    Output("alert-table", "data"),
    Output("alert-strip", "children"),
    Input("interval", "n_intervals"),
)
def update_alerts(_: int) -> tuple[list[dict[str, Any]], html.Div]:
    rows, _total = _visible_rows()
    critical = [row for row in rows if row["Statut"] == "ANOMALIE"]

    with _lock:
        total_alerts = _total_alerts_received

    last_time = critical[0]["Timestamp"][-8:] if critical else "—"
    top_sources = Counter(row["Source"] for row in critical).most_common(1)
    top_source = top_sources[0][0] if top_sources else "—"

    strip = html.Div(
        style={
            "display": "grid",
            "gridTemplateColumns": "1fr 1fr 1fr",
            "gap": "14px",
        },
        children=[
            _metric_card(
                "total des alertes",
                "alert-total",
                RED,
                "🚨",
                "TOTAL",
            ),
            _metric_card(
                "dernier événement",
                "alert-last",
                ORAN,
                "⏱",
                "LAST",
            ),
            _metric_card(
                "source principale",
                "alert-source",
                CYAN,
                "🖥",
                "TOP",
            ),
        ],
    )

    strip.children[0].children[2].children = str(total_alerts)
    strip.children[1].children[2].children = last_time
    strip.children[2].children[2].children = top_source

    return (
        [_display_row(row, include_model=True) for row in critical],
        strip,
    )


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=8050,
        debug=False,
    )
