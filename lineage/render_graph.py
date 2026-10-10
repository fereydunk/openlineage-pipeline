"""Render the Marquez lineage graph as a self-contained, typed-label HTML page.

The stock Marquez UI (http://localhost:3000) shows generic node labels. The
sibling openlineage-confluent project solved this with a forked marquez-web UI
that added typed pills (CONNECT / FLINK / TOPIC / …). That fork needs Node + a
webpack dev server; this is the no-Node equivalent — it pulls the lineage from
the Marquez REST API (server-side, so no browser CORS) and writes a static page
where every node is labelled by its kind.

Run:  .venv/bin/python lineage/render_graph.py   ->   lineage/graph.html
Env:  MARQUEZ_URL (default http://localhost:5002)
"""
from __future__ import annotations

import json
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path

MARQUEZ_URL = os.environ.get("MARQUEZ_URL", "http://localhost:5002")
OUT = Path(__file__).resolve().parent / "graph.html"

# root node to walk from — the DB2 source table (falls back to any no-input node)
ROOT = os.environ.get(
    "LINEAGE_ROOT",
    "dataset:db2://olpipe-db2:50000:TESTDB.DB2INST1.ORDERS",
)

# namespace prefix -> (pill label, css kind)
KINDS = [
    ("db2://",           "DB2 Table",     "db2"),
    ("kafka://",         "Kafka Topic",   "topic"),
    ("glue://",          "Iceberg Table", "iceberg"),
    ("kafka-connect://", "Kafka Connect", "connect"),
    ("flink://",         "Flink SQL",     "flink"),
    ("tableflow://",     "Tableflow",     "tableflow"),
    ("spark://",         "Spark",         "spark"),
]


def classify(namespace: str, node_type: str) -> tuple[str, str]:
    for prefix, label, kind in KINDS:
        if namespace.startswith(prefix):
            return label, kind
    return (node_type or "Node").title(), "other"


def fetch_graph() -> list[dict]:
    url = (f"{MARQUEZ_URL}/api/v1/lineage?nodeId="
           f"{urllib.parse.quote(ROOT, safe='')}&depth=25")
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.load(r)["graph"]


def order_nodes(nodes: list[dict]) -> list[dict]:
    by_id = {n["id"]: n for n in nodes}
    indeg = {n["id"]: len(n.get("inEdges") or []) for n in nodes}
    # start from roots (no inputs); walk outEdges preserving discovery order
    queue = [nid for nid in by_id if indeg[nid] == 0] or [ROOT]
    seen, ordered = set(), []
    while queue:
        nid = queue.pop(0)
        if nid in seen or nid not in by_id:
            continue
        seen.add(nid)
        ordered.append(by_id[nid])
        for e in by_id[nid].get("outEdges") or []:
            queue.append(e["destination"])
    for n in nodes:  # append anything not reachable from a root
        if n["id"] not in seen:
            ordered.append(n)
    return ordered


def node_card(n: dict) -> str:
    d = n.get("data", {})
    ns = d.get("namespace", "")
    name = d.get("name", "")
    label, kind = classify(ns, n.get("type", ""))
    is_job = (n.get("type") == "JOB")
    fields = d.get("fields") or []
    chips = ""
    if fields and not is_job:
        shown = ", ".join(f.get("name", "") for f in fields[:6])
        more = f" +{len(fields) - 6}" if len(fields) > 6 else ""
        chips = f'<div class="fields">{shown}{more}</div>'
    role = "job" if is_job else "dataset"
    return (
        f'<div class="node {role}">'
        f'<span class="pill {kind}">{label}</span>'
        f'<div class="name">{name}</div>'
        f'<div class="ns">{ns}</div>'
        f'{chips}'
        f'</div>'
    )


def main() -> None:
    try:
        nodes = fetch_graph()
    except Exception as e:  # noqa: BLE001
        sys.exit(f"Could not reach Marquez at {MARQUEZ_URL}: {e}\n"
                 f"Is it up?  make marquez-up")
    if not nodes:
        sys.exit("Marquez returned an empty graph — run `make lineage` first.")

    ordered = order_nodes(nodes)
    jobs = sum(1 for n in ordered if n.get("type") == "JOB")
    dsets = len(ordered) - jobs
    flow = '<div class="arrow">→</div>'.join(node_card(n) for n in ordered)

    html = TEMPLATE.replace("{{FLOW}}", flow)\
                   .replace("{{NODES}}", str(len(ordered)))\
                   .replace("{{JOBS}}", str(jobs))\
                   .replace("{{DSETS}}", str(dsets))\
                   .replace("{{MARQUEZ}}", MARQUEZ_URL)
    OUT.write_text(html)
    print(f"✓ wrote {OUT}  ({len(ordered)} nodes: {dsets} datasets, {jobs} jobs)")
    print(f"  open it:  open {OUT}")


TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pipeline Lineage</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap">
<style>
  :root{
    --bg:#F4F6F8; --surface:#FFF; --ink:#161B22; --ink-2:#48525E; --ink-3:#727D89;
    --border:#DCE2E8; --accent:#0E7490;
    --db2:#5B61D6; --topic:#0E7490; --connect:#B4660E; --flink:#8338A0;
    --tableflow:#0F7A5A; --iceberg:#2563A8; --spark:#C2410C; --other:#6B7682;
    --mono:"IBM Plex Mono",ui-monospace,Menlo,monospace;
    --sans:"IBM Plex Sans",system-ui,-apple-system,sans-serif;
  }
  @media (prefers-color-scheme:dark){:root:not([data-theme="light"]){
    --bg:#0D1117; --surface:#151B23; --ink:#E7ECF1; --ink-2:#A6B1BD; --ink-3:#78828E;
    --border:#27303B; --accent:#38B6CE;
    --db2:#8E93F0; --topic:#38B6CE; --connect:#E0A64F; --flink:#C98FE0;
    --tableflow:#54C79C; --iceberg:#6FA6E8; --spark:#F0894F; --other:#9AA4B0;
  }}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--sans);line-height:1.5}
  .wrap{max-width:1100px;margin:0 auto;padding:40px 24px 60px}
  .eyebrow{font-family:var(--mono);font-size:12px;letter-spacing:.14em;text-transform:uppercase;color:var(--accent);margin:0 0 10px}
  h1{font-size:30px;font-weight:700;letter-spacing:-.02em;margin:0 0 8px}
  .sub{color:var(--ink-2);margin:0 0 6px;max-width:66ch}
  .stat{font-family:var(--mono);font-size:13px;color:var(--ink-3)}
  .stat a{color:var(--accent)}
  .flow-scroll{overflow-x:auto;padding:26px 2px 12px;margin-top:22px}
  .flow{display:flex;align-items:stretch;min-width:max-content}
  .node{background:var(--surface);border:1px solid var(--border);border-radius:12px;
    padding:14px 16px;min-width:176px;max-width:196px;display:flex;flex-direction:column;gap:5px;
    box-shadow:0 1px 2px rgba(20,27,34,.05),0 6px 18px rgba(20,27,34,.05)}
  .node.job{border-top-width:3px;border-top-style:solid}
  .node.job .name{font-weight:600}
  .node.dataset{border-style:dashed}
  .pill{align-self:flex-start;font-family:var(--sans);font-size:11.5px;font-weight:600;
    letter-spacing:.01em;padding:3px 10px;border-radius:100px;color:#fff}
  .pill.db2{background:var(--db2)} .pill.topic{background:var(--topic)}
  .pill.connect{background:var(--connect)} .pill.flink{background:var(--flink)}
  .pill.tableflow{background:var(--tableflow)} .pill.iceberg{background:var(--iceberg)}
  .pill.spark{background:var(--spark)} .pill.other{background:var(--other)}
  .node.job.connect{border-top-color:var(--connect)}
  .node.job.flink{border-top-color:var(--flink)}
  .node.job.tableflow{border-top-color:var(--tableflow)}
  .node.job.spark{border-top-color:var(--spark)}
  .name{font-size:14px;font-weight:500;word-break:break-word;line-height:1.25}
  .ns{font-family:var(--mono);font-size:10.5px;color:var(--ink-3);word-break:break-all}
  .fields{font-size:11px;color:var(--ink-2);border-top:1px solid var(--border);padding-top:6px;margin-top:2px}
  .arrow{align-self:center;color:var(--ink-3);font-family:var(--mono);font-size:18px;padding:0 10px;flex:0 0 auto}
  .legend{display:flex;flex-wrap:wrap;gap:10px 16px;margin-top:26px;font-size:12.5px;color:var(--ink-2)}
  .legend span{display:inline-flex;align-items:center;gap:7px}
  .dot{width:11px;height:11px;border-radius:3px;display:inline-block}
  .note{margin-top:22px;font-size:13px;color:var(--ink-3);font-family:var(--mono)}
</style>
</head>
<body>
  <div class="wrap">
    <p class="eyebrow">openlineage-pipeline · marquez</p>
    <h1>Pipeline lineage</h1>
    <p class="sub">The full DB2&nbsp;→&nbsp;Spark chain as ingested by Marquez, every node labelled by
      its kind. Dashed = dataset, solid top-border = job.</p>
    <p class="stat">{{NODES}} nodes · {{DSETS}} datasets · {{JOBS}} jobs ·
      source <a href="http://localhost:3000">Marquez UI ↗</a> · api {{MARQUEZ}}</p>
    <div class="flow-scroll"><div class="flow">{{FLOW}}</div></div>
    <div class="legend">
      <span><i class="dot" style="background:var(--db2)"></i>DB2 table</span>
      <span><i class="dot" style="background:var(--connect)"></i>Kafka Connect</span>
      <span><i class="dot" style="background:var(--topic)"></i>Kafka topic</span>
      <span><i class="dot" style="background:var(--flink)"></i>Flink</span>
      <span><i class="dot" style="background:var(--tableflow)"></i>Tableflow</span>
      <span><i class="dot" style="background:var(--iceberg)"></i>Iceberg / Glue</span>
      <span><i class="dot" style="background:var(--spark)"></i>Spark</span>
    </div>
    <p class="note">regenerate: make lineage &amp;&amp; make lineage-view</p>
  </div>
</body>
</html>
"""


if __name__ == "__main__":
    main()
