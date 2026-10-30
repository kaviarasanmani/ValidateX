"""
Multi-Dataset Web Dashboard for ValidateX Observability & Data Contracts.

Provides:
  - ``build_dashboard_payload(store, registry)`` — structured multi-dataset & contract telemetry
  - ``render_dashboard_html(store, registry)`` / ``export_dashboard_html(filepath, store, registry)``
  - ``create_wsgi_app(store, registry)`` — zero-dependency WSGI server with JSON API + interactive UI
  - ``serve_dashboard(host, port, store, registry)`` — local HTTP server launcher
"""

from __future__ import annotations

import html
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Union
from urllib.parse import unquote

from validatex.contracts.registry import ContractRegistry
from validatex.observability.recorder import resolve_store
from validatex.storage.run_store import RunStore


def _resolve_dashboard_store(store: Any) -> RunStore:
    if isinstance(store, RunStore):
        return store
    resolved = resolve_store(store if store is not None else True)
    return resolved if isinstance(resolved, RunStore) else RunStore()


def _resolve_dashboard_registry(registry: Any) -> Any:
    if registry is None or registry is False:
        return None
    if isinstance(registry, ContractRegistry):
        return registry
    if isinstance(registry, (str, os.PathLike)):
        p = Path(registry)
        return ContractRegistry(p) if p.exists() else None
    return None


def build_dashboard_payload(
    store: Any = None,
    registry: Any = None,
    limit: int = 30,
) -> Dict[str, Any]:
    """
    Collect multi-dataset observability metrics from :class:`RunStore` and contract metadata
    from :class:`ContractRegistry`.
    """
    run_store = _resolve_dashboard_store(store)
    reg = _resolve_dashboard_registry(registry)

    all_runs = run_store.get_runs(limit=max(limit * 10, 200))
    dataset_names: List[str] = []
    for r in all_runs:
        ds = str(r.get("dataset_name") or "unknown")
        if ds not in dataset_names:
            dataset_names.append(ds)

    datasets_out: List[Dict[str, Any]] = []
    for ds in dataset_names:
        ds_runs = run_store.get_runs(dataset_name=ds, limit=limit)
        if not ds_runs:
            continue
        latest = ds_runs[0]
        trend = run_store.quality_trend(dataset_name=ds, limit=limit)
        fail_trends = run_store.failed_expectation_trends(dataset_name=ds, limit=limit)
        schema_diffs = run_store.schema_changes(ds)[-limit:]

        datasets_out.append(
            {
                "dataset_name": ds,
                "latest_run_id": latest.get("run_id"),
                "latest_score": float(latest.get("quality_score") or 0.0),
                "latest_timestamp": latest.get("timestamp"),
                "engine": latest.get("engine"),
                "suite_name": latest.get("suite_name"),
                "suite_version": latest.get("suite_version"),
                "row_count": latest.get("row_count"),
                "total_expectations": latest.get("total_expectations", 0),
                "passed_expectations": latest.get("passed_expectations", 0),
                "failed_expectations": latest.get("failed_expectations", 0),
                "trend": trend.to_dict(),
                "failed_trends": [ft.to_dict() for ft in fail_trends[:10]],
                "schema_changes": [sc.to_dict() for sc in schema_diffs[-10:]],
                "runs": ds_runs,
            }
        )

    contracts_out: List[Dict[str, Any]] = []
    if reg is not None:
        for cname in reg.list_contracts():
            versions = reg.versions(cname)
            if not versions:
                continue
            latest_dc = reg.get(cname, versions[-1])
            latest_diff = None
            if len(versions) >= 2:
                latest_diff = reg.diff(cname, versions[-2], versions[-1]).to_dict()
            contracts_out.append(
                {
                    "name": cname,
                    "latest_version": latest_dc.version,
                    "versions": versions,
                    "description": latest_dc.description,
                    "owner": latest_dc.owner.to_dict(),
                    "schema_count": len(latest_dc.schema),
                    "rules_count": len(latest_dc.quality_rules),
                    "thresholds": latest_dc.thresholds.to_dict(),
                    "schema": [f.to_dict() for f in latest_dc.schema],
                    "quality_rules": [r.to_dict() for r in latest_dc.quality_rules],
                    "latest_diff": latest_diff,
                }
            )

    latest_scores = [d["latest_score"] for d in datasets_out]
    avg_score = round(sum(latest_scores) / len(latest_scores), 2) if latest_scores else 0.0
    healthy_count = sum(1 for d in datasets_out if d["failed_expectations"] == 0 and d["latest_score"] >= 90.0)
    degrading_count = sum(1 for d in datasets_out if d["trend"].get("direction") == "degrading")

    summary = {
        "total_datasets": len(datasets_out),
        "total_runs": len(all_runs),
        "average_quality_score": avg_score,
        "healthy_datasets": healthy_count,
        "degrading_datasets": degrading_count,
        "total_contracts": len(contracts_out),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    return {
        "summary": summary,
        "datasets": datasets_out,
        "contracts": contracts_out,
    }


def _sparkline_svg(scores: List[float]) -> str:
    if not scores:
        return '<span class="dim">No runs</span>'
    w, h = 140, 36
    pad = 4
    n = len(scores)
    pts = []
    for i, sc in enumerate(scores):
        x = pad + (i / (n - 1)) * (w - 2 * pad) if n > 1 else w / 2.0
        clamped = max(0.0, min(100.0, float(sc)))
        y = pad + (h - 2 * pad) - (clamped / 100.0) * (h - 2 * pad)
        pts.append((x, y))
    poly = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    last_sc = float(scores[-1])
    stroke = "#00e676" if last_sc >= 90 else ("#ffc107" if last_sc >= 70 else "#ff5252")
    lx, ly = pts[-1]
    return (
        f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" class="sparkline">'
        f'<polyline points="{poly}" fill="none" stroke="{stroke}" stroke-width="2" stroke-linecap="round"/>'
        f'<circle cx="{lx:.1f}" cy="{ly:.1f}" r="3" fill="{stroke}"/>'
        f"</svg>"
    )


def render_dashboard_html(
    store: Any = None,
    registry: Any = None,
    limit: int = 30,
    title: str = "ValidateX Observability & Data Contracts Dashboard",
) -> str:
    """Render a standalone HTML dashboard for all datasets in RunStore and contracts in ContractRegistry."""
    payload = build_dashboard_payload(store=store, registry=registry, limit=limit)
    summary = payload["summary"]
    datasets = payload["datasets"]
    contracts = payload["contracts"]

    ds_rows: List[str] = []
    for idx, d in enumerate(datasets):
        ds_name = html.escape(str(d["dataset_name"]))
        score = d["latest_score"]
        score_cls = "passed-text" if score >= 90 else ("warn-text" if score >= 70 else "failed-text")
        direction = str(d["trend"].get("direction") or "stable")
        dir_badge = {
            "improving": '<span class="badge passed">IMPROVING</span>',
            "degrading": '<span class="badge failed">DEGRADING</span>',
            "stable": '<span class="badge info">STABLE</span>',
        }.get(direction, '<span class="badge info">STABLE</span>')
        spark = _sparkline_svg(d["trend"].get("scores") or [])
        row_cnt = d["row_count"] if d["row_count"] is not None else "—"
        ts = html.escape(str(d.get("latest_timestamp") or "")[:19])
        ds_rows.append(
            f'<tr class="ds-row" data-name="{ds_name.lower()}" onclick="selectDataset({idx})">'
            f"<td><strong>{ds_name}</strong><div class=\"dim-sm\">{html.escape(str(d.get('engine') or 'pandas'))}</div></td>"
            f'<td class="center"><span class="score-pill {score_cls}">{score:.1f}</span></td>'
            f'<td class="center">{dir_badge}</td>'
            f'<td class="center">{spark}</td>'
            f'<td class="center passed-text">{d["passed_expectations"]}</td>'
            f'<td class="center failed-text">{d["failed_expectations"]}</td>'
            f'<td class="center">{row_cnt}</td>'
            f"<td>{ts}</td>"
            f"</tr>"
        )

    contract_cards: List[str] = []
    for c in contracts:
        cname = html.escape(str(c["name"]))
        cver = html.escape(str(c["latest_version"]))
        owner_team = html.escape(str((c.get("owner") or {}).get("team") or "unassigned"))
        steward = html.escape(str((c.get("owner") or {}).get("steward") or "—"))
        min_qs = (c.get("thresholds") or {}).get("minimum_quality_score")
        min_qs_str = f"{min_qs}" if min_qs is not None else "—"
        contract_cards.append(
            f'<div class="contract-card">'
            f'<div class="cc-head"><strong>{cname}</strong><span class="badge info">v{cver}</span></div>'
            f'<div class="cc-meta">Team: <b>{owner_team}</b> &middot; Steward: {steward}</div>'
            f'<div class="cc-stats">'
            f'<span>Fields: <b>{c["schema_count"]}</b></span>'
            f'<span>Rules: <b>{c["rules_count"]}</b></span>'
            f"<span>Min Score: <b>{min_qs_str}</b></span>"
            f'<span>Versions: <b>{len(c["versions"])}</b></span>'
            f"</div>"
            f"</div>"
        )

    safe_json = html.escape(json.dumps(payload, default=str)).replace("</", "<\\/")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{html.escape(title)}</title>
<style>
:root {{
  --bg: #0f0f1a; --surface: #1a1a2e; --surface2: #232340; --border: #2d2d50;
  --text: #e0e0ff; --text-dim: #8888aa; --accent: #6c63ff;
  --success: #00e676; --failure: #ff5252; --warning: #ffc107; --info: #42a5f5;
  --radius: 12px;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family:'Segoe UI','Inter',sans-serif; background:var(--bg); color:var(--text); line-height:1.6; }}
.container {{ max-width:1340px; margin:0 auto; padding:28px 24px; }}
.header {{ display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:16px; margin-bottom:24px; }}
.logo {{ font-size:2rem; font-weight:800; background:linear-gradient(135deg,#6c63ff,#a78bfa,#f472b6); -webkit-background-clip:text; -webkit-text-fill-color:transparent; }}
.subtitle {{ color:var(--text-dim); font-size:.9rem; }}
.search-input {{ background:var(--surface); border:1px solid var(--border); color:var(--text); padding:9px 16px; border-radius:24px; min-width:260px; outline:none; }}
.search-input:focus {{ border-color:var(--accent); }}
.kpi-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(175px,1fr)); gap:14px; margin-bottom:24px; }}
.kpi-card {{ background:var(--surface); border:1px solid var(--border); border-radius:var(--radius); padding:16px 18px; text-align:center; }}
.kpi-val {{ font-size:1.85rem; font-weight:800; color:var(--accent); }}
.kpi-label {{ font-size:.75rem; text-transform:uppercase; letter-spacing:1px; color:var(--text-dim); }}
.layout {{ display:grid; grid-template-columns:1.35fr 1fr; gap:20px; margin-bottom:24px; }}
@media(max-width:960px){{ .layout {{ grid-template-columns:1fr; }} }}
.card {{ background:var(--surface); border:1px solid var(--border); border-radius:var(--radius); overflow:hidden; margin-bottom:24px; }}
.card h2 {{ padding:14px 18px; font-size:1.05rem; border-bottom:1px solid var(--border); background:var(--surface2); }}
.table-wrap {{ overflow-x:auto; }}
table {{ width:100%; border-collapse:collapse; }}
th {{ text-align:left; padding:10px 12px; font-size:.72rem; text-transform:uppercase; letter-spacing:.8px; color:var(--text-dim); background:var(--surface2); border-bottom:1px solid var(--border); }}
td {{ padding:10px 12px; border-bottom:1px solid var(--border); font-size:.86rem; vertical-align:middle; }}
.ds-row {{ cursor:pointer; transition:background .15s; }}
.ds-row:hover, .ds-row.active {{ background:rgba(108,99,255,.12); }}
.center {{ text-align:center; }}
.passed-text {{ color:var(--success); font-weight:700; }}
.warn-text {{ color:var(--warning); font-weight:700; }}
.failed-text {{ color:var(--failure); font-weight:700; }}
.score-pill {{ font-size:1.05rem; font-weight:800; }}
.badge {{ display:inline-block; padding:2px 10px; border-radius:12px; font-size:.7rem; font-weight:700; }}
.badge.passed {{ background:rgba(0,230,118,.12); color:var(--success); border:1px solid rgba(0,230,118,.3); }}
.badge.failed {{ background:rgba(255,82,82,.12); color:var(--failure); border:1px solid rgba(255,82,82,.3); }}
.badge.info {{ background:rgba(66,165,245,.12); color:var(--info); border:1px solid rgba(66,165,245,.3); }}
.dim {{ color:var(--text-dim); }}
.dim-sm {{ color:var(--text-dim); font-size:.75rem; }}
.drill-body {{ padding:16px 18px; }}
.drill-section {{ margin-top:14px; }}
.drill-section h3 {{ font-size:.88rem; text-transform:uppercase; letter-spacing:.7px; color:var(--text-dim); margin-bottom:8px; }}
.contracts-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(260px,1fr)); gap:14px; padding:16px 18px; }}
.contract-card {{ background:var(--surface2); border:1px solid var(--border); border-radius:10px; padding:14px; }}
.cc-head {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:6px; }}
.cc-meta {{ font-size:.8rem; color:var(--text-dim); margin-bottom:10px; }}
.cc-stats {{ display:flex; justify-content:space-between; font-size:.78rem; border-top:1px solid var(--border); padding-top:8px; }}
.footer {{ text-align:center; color:var(--text-dim); font-size:.78rem; padding:16px 0; }}
</style>
</head>
<body>
<div class="container">
  <header class="header">
    <div>
      <div class="logo">ValidateX Control Plane</div>
      <div class="subtitle">Multi-Dataset Quality Observability &amp; Data Contracts Dashboard</div>
    </div>
    <input type="search" class="search-input" id="dsSearch" placeholder="Filter datasets by name..." oninput="filterDatasets(this.value)"/>
  </header>

  <div class="kpi-grid">
    <div class="kpi-card">
      <div class="kpi-val">{summary['total_datasets']}</div>
      <div class="kpi-label">Monitored Datasets</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-val">{summary['total_runs']}</div>
      <div class="kpi-label">Validation Runs</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-val passed-text">{summary['average_quality_score']}</div>
      <div class="kpi-label">Avg Quality Score</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-val passed-text">{summary['healthy_datasets']}</div>
      <div class="kpi-label">Healthy Datasets</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-val failed-text">{summary['degrading_datasets']}</div>
      <div class="kpi-label">Degrading Trends</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-val">{summary['total_contracts']}</div>
      <div class="kpi-label">Data Contracts</div>
    </div>
  </div>

  <div class="layout">
    <div class="card">
      <h2>&#x1F4CA; Monitored Datasets ({len(datasets)})</h2>
      <div class="table-wrap">
        <table id="dsTable">
          <thead>
            <tr>
              <th>Dataset</th>
              <th class="center">Score</th>
              <th class="center">Trend</th>
              <th class="center">Sparkline</th>
              <th class="center">Pass</th>
              <th class="center">Fail</th>
              <th class="center">Rows</th>
              <th>Last Run</th>
            </tr>
          </thead>
          <tbody>
            {''.join(ds_rows) if ds_rows else '<tr><td colspan="8" class="center dim">No datasets recorded yet.</td></tr>'}
          </tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <h2 id="drillTitle">&#x1F50D; Dataset Observability Drilldown</h2>
      <div class="drill-body" id="drillBody">
        <p class="dim">Select a dataset row to inspect its historical quality trend, failing expectation streaks, and schema changes.</p>
      </div>
    </div>
  </div>

  <div class="card">
    <h2>&#x1F4DC; Data Contracts Registry ({len(contracts)})</h2>
    <div class="contracts-grid">
      {''.join(contract_cards) if contract_cards else '<p class="dim">No registered contracts found in registry.</p>'}
    </div>
  </div>

  <div class="footer">
    ValidateX Observability Dashboard &middot; Generated {html.escape(summary['generated_at'])}
  </div>
</div>

<script id="dashboard-data" type="application/json">{safe_json}</script>
<script>
const DASHBOARD = JSON.parse(document.getElementById('dashboard-data').textContent);

function filterDatasets(q) {{
  const needle = (q || '').toLowerCase().trim();
  document.querySelectorAll('#dsTable tbody tr.ds-row').forEach(tr => {{
    const name = tr.getAttribute('data-name') || '';
    tr.style.display = (!needle || name.includes(needle)) ? '' : 'none';
  }});
}}

function selectDataset(idx) {{
  const ds = (DASHBOARD.datasets || [])[idx];
  if (!ds) return;
  document.querySelectorAll('#dsTable tbody tr.ds-row').forEach((tr, i) => {{
    tr.classList.toggle('active', i === idx);
  }});
  document.getElementById('drillTitle').textContent = 'Dataset Drilldown — ' + ds.dataset_name;
  const trend = ds.trend || {{}};
  const fails = ds.failed_trends || [];
  const schemas = ds.schema_changes || [];

  let failHtml = '<p class="dim-sm">No failing expectations recorded.</p>';
  if (fails.length > 0) {{
    failHtml = '<table><thead><tr><th>Expectation</th><th>Col</th><th>Rate</th><th>Streak</th></tr></thead><tbody>' +
      fails.map(f => `<tr><td>${{f.expectation_type}}</td><td>${{f.column || '—'}}</td><td class="failed-text">${{f.failure_rate}}%</td><td>${{f.current_streak}}</td></tr>`).join('') +
      '</tbody></table>';
  }}

  let schemaHtml = '<p class="dim-sm">No schema changes detected across runs.</p>';
  if (schemas.length > 0) {{
    schemaHtml = '<table><thead><tr><th>Run</th><th>Added</th><th>Removed</th></tr></thead><tbody>' +
      schemas.map(s => `<tr><td>#${{s.run_id}}</td><td class="passed-text">${{(s.added || []).join(', ') || '—'}}</td><td class="failed-text">${{(s.removed || []).join(', ') || '—'}}</td></tr>`).join('') +
      '</tbody></table>';
  }}

  document.getElementById('drillBody').innerHTML = `
    <div><strong>Latest Score:</strong> ${{ds.latest_score}} / 100 &middot; <strong>Mean:</strong> ${{trend.mean ?? '—'}} &middot; <strong>Slope:</strong> ${{trend.slope ?? 0}} pts/run</div>
    <div class="drill-section">
      <h3>Failing Expectation Streaks</h3>
      ${{failHtml}}
    </div>
    <div class="drill-section">
      <h3>Schema Change History</h3>
      ${{schemaHtml}}
    </div>
  `;
}}

if ((DASHBOARD.datasets || []).length > 0) {{
  selectDataset(0);
}}
</script>
</body>
</html>"""


def export_dashboard_html(
    filepath: Union[str, os.PathLike],
    store: Any = None,
    registry: Any = None,
    limit: int = 30,
) -> Path:
    """Write a standalone multi-dataset observability & contracts HTML dashboard to *filepath*."""
    out = Path(filepath)
    out.parent.mkdir(parents=True, exist_ok=True)
    html_doc = render_dashboard_html(store=store, registry=registry, limit=limit)
    out.write_text(html_doc, encoding="utf-8")
    return out


def create_wsgi_app(
    store: Any = None,
    registry: Any = None,
    limit: int = 30,
) -> Callable[[Dict[str, Any], Callable[..., Any]], List[bytes]]:
    """
    Create a standard Python WSGI application serving the ValidateX Web Dashboard and JSON API.

    Routes:
      - ``GET /`` — Interactive HTML Dashboard
      - ``GET /health`` — Health check JSON
      - ``GET /api/summary`` — Global KPI summary JSON
      - ``GET /api/datasets`` — Monitored datasets JSON
      - ``GET /api/datasets/<name>`` — Single dataset detail JSON
      - ``GET /api/contracts`` — Registered data contracts JSON
    """

    def app(environ: Dict[str, Any], start_response: Callable[..., Any]) -> List[bytes]:
        path = environ.get("PATH_INFO", "/") or "/"

        def json_response(data: Any, status: str = "200 OK") -> List[bytes]:
            body = json.dumps(data, indent=2, default=str).encode("utf-8")
            start_response(
                status,
                [
                    ("Content-Type", "application/json; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]

        if path == "/health":
            return json_response({"status": "ok"})

        payload = build_dashboard_payload(store=store, registry=registry, limit=limit)

        if path == "/api/summary":
            return json_response(payload["summary"])
        if path == "/api/datasets":
            return json_response(payload["datasets"])
        if path.startswith("/api/datasets/"):
            target_name = unquote(path[len("/api/datasets/") :]).strip("/")
            for ds in payload["datasets"]:
                if ds["dataset_name"] == target_name:
                    return json_response(ds)
            return json_response({"error": f"Dataset {target_name!r} not found"}, status="404 Not Found")
        if path == "/api/contracts":
            return json_response(payload["contracts"])
        if path in ("/", "/index.html"):
            html_bytes = render_dashboard_html(store=store, registry=registry, limit=limit).encode("utf-8")
            start_response(
                "200 OK",
                [
                    ("Content-Type", "text/html; charset=utf-8"),
                    ("Content-Length", str(len(html_bytes))),
                ],
            )
            return [html_bytes]

        return json_response({"error": "Not found"}, status="404 Not Found")

    return app


def serve_dashboard(
    host: str = "127.0.0.1",
    port: int = 8501,
    store: Any = None,
    registry: Any = None,
    limit: int = 30,
) -> None:
    """Start the local ValidateX Observability & Contracts Web Dashboard server."""
    from wsgiref.simple_server import make_server

    app = create_wsgi_app(store=store, registry=registry, limit=limit)
    with make_server(host, port, app) as httpd:
        httpd.serve_forever()
