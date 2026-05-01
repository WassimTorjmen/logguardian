"""
LogGuardian — Monitoring UI
Consomme logs-anomalies-ml depuis Kafka et affiche en temps réel.
"""
import json
import logging
import os
import threading
from collections import deque
from datetime import datetime

import dash
from dash import dash_table, dcc, html
from dash.dependencies import Input, Output
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
                "Heure":       r.get("detected_at", "")[:19].replace("T", " "),
                "Source":      r.get("source", ""),
                "Host":        r.get("host", ""),
                "Score":       f"{r.get('anomaly_score', 0):.4f}",
                "Ratio":       f"{r.get('severity_ratio', 0):.2f}x",
                "Seuil":       f"{r.get('threshold', 0):.4f}",
                "Dernier log": r.get("sequence", [{}])[-1].get("message", "")[:80],
                "_ratio_val":  r.get("severity_ratio", 0),
            }
            with _lock:
                _buffer.appendleft(row)
                _total_received += 1
            log.debug("Message reçu #%d", _total_received)
        except Exception as e:
            log.error("Erreur parsing message : %s", e)


threading.Thread(target=_kafka_thread, daemon=True).start()

# ── App ───────────────────────────────────────────────────────────────────────

app = dash.Dash(__name__, title="LogGuardian", suppress_callback_exceptions=True)
app.server.config["SECRET_KEY"] = "logguardian"
app.index_string = '''<!DOCTYPE html>
<html>
<head>{%metas%}<title>{%title%}</title>{%favicon%}{%css%}
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { background-color: #0f1117; overflow-y: scroll; }
  body::-webkit-scrollbar { width: 6px; }
  body::-webkit-scrollbar-track { background: #0f1117; }
  body::-webkit-scrollbar-thumb { background: #2a2d3a; border-radius: 3px; }
</style>
</head>
<body>{%app_entry%}<footer>{%config%}{%scripts%}{%renderer%}</footer></body>
</html>'''

C = {
    "bg":      "#0f1117",
    "surface": "#1a1d27",
    "border":  "#2a2d3a",
    "text":    "#e2e8f0",
    "muted":   "#64748b",
    "danger":  "#ef4444",
    "warning": "#f59e0b",
}

TABLE_COLS      = ["Heure", "Source", "Host", "Score", "Ratio", "Seuil", "Dernier log"]
TABLE_COLS_HIGH = ["Source", "Score", "Dernier log"]

def _card(label, value, color=None):
    return html.Div(style={
        "backgroundColor": C["surface"],
        "border": f"1px solid {C['border']}",
        "borderRadius": "8px",
        "padding": "16px 20px",
        "minWidth": "140px",
    }, children=[
        html.P(label, style={"margin": "0 0 6px", "fontSize": "11px", "color": C["muted"],
                              "textTransform": "uppercase", "letterSpacing": "0.06em"}),
        html.P(value, style={"margin": 0, "fontSize": "24px", "fontWeight": "600",
                              "color": color or C["text"]}),
    ])


_HEADER_STYLE = {
    "backgroundColor": C["surface"],
    "color": C["muted"],
    "fontWeight": "500",
    "fontSize": "11px",
    "border": "none",
    "borderBottom": f"1px solid {C['border']}",
    "textTransform": "uppercase",
    "letterSpacing": "0.05em",
    "padding": "10px 16px",
    "whiteSpace": "nowrap",
}
_CELL_STYLE = {
    "backgroundColor": C["surface"],
    "color": C["text"],
    "fontSize": "13px",
    "border": "none",
    "borderBottom": f"1px solid {C['border']}",
    "padding": "11px 16px",
    "overflow": "hidden",
    "textOverflow": "ellipsis",
    "whiteSpace": "nowrap",
}


def _table(table_id, rows):
    return dash_table.DataTable(
        id=table_id,
        columns=[{"name": c, "id": c} for c in TABLE_COLS],
        data=rows,
        style_table={"overflowX": "auto", "width": "100%"},
        style_header=_HEADER_STYLE,
        style_cell=_CELL_STYLE,
        style_cell_conditional=[
            {"if": {"column_id": "Dernier log"}, "whiteSpace": "normal", "textOverflow": "clip"},
            {"if": {"column_id": "Heure"},  "width": "160px", "minWidth": "160px"},
            {"if": {"column_id": "Source"}, "width": "90px",  "minWidth": "90px"},
            {"if": {"column_id": "Host"},   "width": "80px",  "minWidth": "80px"},
            {"if": {"column_id": "Score"},  "width": "80px",  "minWidth": "80px"},
            {"if": {"column_id": "Ratio"},  "width": "70px",  "minWidth": "70px"},
            {"if": {"column_id": "Seuil"},  "width": "80px",  "minWidth": "80px"},
        ],
        style_data_conditional=[
            {"if": {"filter_query": '{Ratio} > "1.3"', "column_id": "Ratio"},
             "color": C["danger"], "fontWeight": "600"},
            {"if": {"filter_query": '{Ratio} <= "1.3"', "column_id": "Ratio"},
             "color": C["warning"]},
            {"if": {"row_index": "odd"}, "backgroundColor": "#1e2130"},
        ],
        page_size=50,
        sort_action="native",
    )


def _table_high(table_id, rows):
    return dash_table.DataTable(
        id=table_id,
        columns=[{"name": c, "id": c} for c in TABLE_COLS_HIGH],
        data=rows,
        style_table={"overflowX": "auto", "width": "100%"},
        style_header=_HEADER_STYLE,
        style_cell=_CELL_STYLE,
        style_cell_conditional=[
            {"if": {"column_id": "Dernier log"}, "whiteSpace": "normal", "textOverflow": "clip"},
            {"if": {"column_id": "Source"}, "width": "90px",  "minWidth": "90px"},
            {"if": {"column_id": "Score"},  "width": "80px",  "minWidth": "80px"},
        ],
        style_data_conditional=[
            {"if": {"column_id": "Score"}, "color": C["danger"], "fontWeight": "600"},
            {"if": {"row_index": "odd"},   "backgroundColor": "#1e2130"},
        ],
        page_size=50,
        sort_action="native",
    )


def _panel(title, badge, badge_color, table_id):
    return html.Div(style={
        "backgroundColor": C["surface"],
        "border": f"1px solid {C['border']}",
        "borderRadius": "8px",
        "overflow": "hidden",
        "display": "flex",
        "flexDirection": "column",
        "flex": "1",
        "minWidth": "0",
    }, children=[
        html.Div(style={
            "padding": "14px 20px",
            "borderBottom": f"1px solid {C['border']}",
            "display": "flex",
            "justifyContent": "space-between",
            "alignItems": "center",
            "flexShrink": "0",
        }, children=[
            html.Span(title, style={"fontSize": "13px", "fontWeight": "500"}),
            html.Span(id=badge, style={"fontSize": "12px", "color": badge_color, "fontWeight": "600"}),
        ]),
        html.Div(id=table_id, style={"overflow": "auto", "flex": "1"}),
    ])


app.layout = html.Div(style={
    "backgroundColor": C["bg"],
    "height": "100vh",
    "display": "flex",
    "flexDirection": "column",
    "fontFamily": "'Inter', 'Segoe UI', sans-serif",
    "color": C["text"],
    "padding": "24px 32px",
    "overflow": "hidden",
}, children=[
    # Header
    html.Div(style={
        "marginBottom": "20px",
        "borderBottom": f"1px solid {C['border']}",
        "paddingBottom": "16px",
        "display": "flex",
        "justifyContent": "space-between",
        "alignItems": "flex-end",
        "flexShrink": "0",
    }, children=[
        html.Div(children=[
            html.H1("LogGuardian", style={"margin": 0, "fontSize": "20px", "fontWeight": "600"}),
            html.P("Détection d'anomalies en temps réel", style={"margin": "4px 0 0", "fontSize": "12px", "color": C["muted"]}),
        ]),
        html.Span(id="last-update", style={"fontSize": "12px", "color": C["muted"]}),
    ]),

    # Métriques
    html.Div(id="metrics", style={"display": "flex", "gap": "14px", "marginBottom": "20px", "flexShrink": "0"}),

    # Deux colonnes : tous les logs à gauche, sévérité haute à droite
    html.Div(style={
        "display": "flex",
        "gap": "16px",
        "flex": "1",
        "minHeight": "0",
    }, children=[
        _panel("Toutes les anomalies", "badge-all", C["muted"], "table-all"),
        _panel("Sévérité haute  —  ratio > 1.3x", "badge-high", C["danger"], "table-high"),
    ]),

    dcc.Interval(id="interval", interval=REFRESH_INTERVAL_MS, n_intervals=0),
])


@app.callback(
    Output("metrics", "children"),
    Output("last-update", "children"),
    Output("badge-high", "children"),
    Output("badge-all", "children"),
    Output("table-high", "children"),
    Output("table-all", "children"),
    Input("interval", "n_intervals"),
)
def refresh(_):
    with _lock:
        rows  = [r for r in _buffer if "_ratio_val" in r]
        total = _total_received

    display_rows = [{k: v for k, v in r.items() if k != "_ratio_val"} for r in rows]
    high_rows    = [{k: v for k, v in r.items() if k != "_ratio_val"}
                    for r in rows if r["_ratio_val"] > 1.3]

    high    = len(high_rows)
    sources = len({r["Source"] for r in rows}) if rows else 0

    metrics = [
        _card("Total anomalies", str(total)),
        _card("Sévérité haute",  str(high),   C["danger"] if high else C["text"]),
        _card("Sources",         str(sources)),
        _card("Topic",           KAFKA_TOPIC,  C["muted"]),
    ]

    now = datetime.now().strftime("Mis à jour %H:%M:%S")

    return (
        metrics,
        now,
        f"{high} événements",
        f"{total} événements",
        _table_high("tbl-high", high_rows[:100]),
        _table("tbl-all",  display_rows[:200]),
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
