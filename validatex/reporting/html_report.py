"""
Rich HTML report generator for ValidateX.

Produces a standalone, self-contained HTML page with:
  - Quality Score gauge
  - Column Health Summary table
  - Severity-tagged expectation details
  - Mini CSS bar charts
  - Download buttons (JSON / CSV / Copy)
  - Human-readable observed values
  - Drift comparison placeholder
  - Filter / toggle interactions
"""

from __future__ import annotations

import csv
import html
import io
import json
from datetime import datetime
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from validatex.core.result import (
        ColumnHealthSummary,
        ExpectationResult,
        ValidationResult,
    )

from validatex.core.result import (
    SEVERITY_CRITICAL,
    SEVERITY_INFO,
    SEVERITY_WARNING,
    to_native,
)


class HTMLReportGenerator:
    """Generate a beautiful standalone HTML report from a ValidationResult."""

    def generate(self, result: "ValidationResult", filepath: str) -> None:
        result.compute_statistics()
        html_content = self._render(result)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html_content)

    # ------------------------------------------------------------------
    # Main renderer
    # ------------------------------------------------------------------

    def _render(self, result: "ValidationResult") -> str:
        stats = result.statistics
        quality_score = stats.get("quality_score", result.compute_quality_score())
        rows_html = self._render_rows(result.results)
        col_health_html = self._render_column_health(result.column_health())
        status_class = "success" if result.success else "failure"
        status_text = "ALL PASSED" if result.success else "VALIDATION FAILED"
        status_icon = "✅" if result.success else "❌"
        score_class = (
            "score-high"
            if quality_score >= 90
            else "score-mid" if quality_score >= 70 else "score-low"
        )
        run_time_str = (
            result.run_time.strftime("%Y-%m-%d %H:%M:%S") if result.run_time else "N/A"
        )

        # Pre-compute JSON data for download
        json_data = json.dumps(result.to_dict(), indent=2, default=str)
        csv_data = self._results_to_csv(result.results)

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ValidateX Report — {html.escape(result.suite_name)}</title>
<style>
{self._css()}
</style>
</head>
<body>
<div class="container">

  <!-- Header -->
  <header class="header animate">
    <div class="header-top">
      <div>
        <div class="logo">ValidateX</div>
        <div class="subtitle">Data Quality Validation Report</div>
      </div>
      <div class="download-bar">
        <button class="dl-btn" onclick="downloadJSON()" title="Download JSON">
          <span class="dl-icon">&#x2B07;</span> JSON
        </button>
        <button class="dl-btn" onclick="downloadCSV()" title="Download CSV">
          <span class="dl-icon">&#x2B07;</span> CSV
        </button>
        <button class="dl-btn" onclick="copySummary()" id="copyBtn" title="Copy Summary">
          <span class="dl-icon">&#x1F4CB;</span> Copy
        </button>
      </div>
    </div>
  </header>

  <!-- Status Banner + Quality Score -->
  <div class="top-row animate delay-1">
    <div class="status-banner {status_class}">
      <span class="status-icon">{status_icon}</span>
      <span>{status_text}</span>
    </div>
    <div class="quality-score-card {score_class}">
      <div class="qs-label">DATA QUALITY SCORE</div>
      <div class="qs-value">{quality_score}</div>
      <div class="qs-max">/ 100</div>
      <div class="qs-bar-bg"><div class="qs-bar-fill" style="width:{quality_score}%"></div></div>
    </div>
  </div>

  <!-- Stats Grid -->
  <div class="stats-grid animate delay-2">
    <div class="stat-card">
      <div class="stat-value total">{stats.get('total', 0)}</div>
      <div class="stat-label">Total</div>
    </div>
    <div class="stat-card">
      <div class="stat-value passed">{stats.get('passed', 0)}</div>
      <div class="stat-label">Passed</div>
    </div>
    <div class="stat-card">
      <div class="stat-value failed">{stats.get('failed', 0)}</div>
      <div class="stat-label">Failed</div>
    </div>
    <div class="stat-card">
      <div class="stat-value errors">{stats.get('errors', 0)}</div>
      <div class="stat-label">Errors</div>
    </div>
    <div class="stat-card">
      <div class="stat-value rate">{stats.get('success_percent', 0)}%</div>
      <div class="stat-label">Success Rate</div>
    </div>
  </div>

  <!-- Progress Bar -->
  <div class="progress-wrap animate delay-2">
    <div class="progress-labels">
      <span>Validation Progress</span>
      <span>{stats.get('success_percent', 0)}% passed</span>
    </div>
    <div class="progress-bar-bg">
      <div class="progress-bar-fill {'low' if stats.get('success_percent', 0) < 70 else ''}"
           style="width:{stats.get('success_percent', 0)}%"></div>
    </div>
  </div>

  <!-- Meta -->
  <div class="meta-grid animate delay-3">
    <div class="meta-item">
      <span class="meta-key">Suite</span>
      <span class="meta-val">{html.escape(result.suite_name)}</span>
    </div>
    <div class="meta-item">
      <span class="meta-key">Engine</span>
      <span class="meta-val">{html.escape(result.engine)}</span>
    </div>
    <div class="meta-item">
      <span class="meta-key">Run Time</span>
      <span class="meta-val">{run_time_str}</span>
    </div>
    <div class="meta-item">
      <span class="meta-key">Duration</span>
      <span class="meta-val">{stats.get('run_duration_seconds', 0):.3f}s</span>
    </div>
    {f'<div class="meta-item"><span class="meta-key">Data Source</span><span class="meta-val">{html.escape(str(result.data_source or ""))}</span></div>' if result.data_source else ""}
  </div>

  <!-- Column Health Summary -->
  <div class="section-card animate delay-3">
    <h2>&#x1F4CA; Column Health Summary</h2>
    <div class="col-health-wrap">
      <table class="col-health-table">
        <thead>
          <tr>
            <th>Column</th>
            <th>Checks</th>
            <th>Passed</th>
            <th>Failed</th>
            <th>Health</th>
            <th>Null %</th>
            <th>Unique %</th>
          </tr>
        </thead>
        <tbody>
          {col_health_html}
        </tbody>
      </table>
    </div>
  </div>

  <!-- Drift Placeholder -->
  <div class="drift-card animate delay-3">
    <h2>&#x1F4C8; Change From Last Run</h2>
    <p class="drift-hint">
      Enable drift detection by saving previous results and passing
      <code>--compare previous_report.json</code> on your next run.
      ValidateX will show row-count changes, new/dropped columns, and
      null-percentage drift automatically.
    </p>
  </div>

  <!-- Filters -->
  <div class="filters animate delay-3">
    <button class="filter-btn active" onclick="filterRows('all', this)">All</button>
    <button class="filter-btn" onclick="filterRows('passed', this)">&#x2705; Passed</button>
    <button class="filter-btn" onclick="filterRows('failed', this)">&#x274C; Failed</button>
    <button class="filter-btn" onclick="filterRows('error', this)">&#x26A0;&#xFE0F; Errors</button>
    <span class="filter-sep">|</span>
    <button class="filter-btn" onclick="filterRows('critical', this)">&#x1F534; Critical</button>
    <button class="filter-btn" onclick="filterRows('warning', this)">&#x1F7E1; Warning</button>
    <button class="filter-btn" onclick="filterRows('info', this)">&#x1F535; Info</button>
  </div>

  <!-- Results Table -->
  <div class="section-card animate delay-4">
    <h2>&#x1F4CB; Expectation Results</h2>
    <table class="results-table">
      <thead>
        <tr>
          <th style="width:36px">#</th>
          <th style="width:70px">Severity</th>
          <th style="width:80px">Status</th>
          <th>Expectation</th>
          <th>Column</th>
          <th>Observed</th>
          <th>Unexpected</th>
          <th style="width:60px">Details</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </div>

  <!-- Footer -->
  <div class="footer">
    Generated by <a href="#">ValidateX v1.0.0</a> &middot; {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
  </div>
</div>

<!-- Hidden data stores for download -->
<script id="json-data" type="application/json">{html.escape(json_data)}</script>
<script id="csv-data" type="text/plain">{html.escape(csv_data)}</script>
<script id="summary-text" type="text/plain">{html.escape(self._build_summary_text(result))}</script>

<script>
{self._js()}
</script>
</body>
</html>"""

    # ------------------------------------------------------------------
    # Column Health rows
    # ------------------------------------------------------------------

    def _render_column_health(self, summaries: "List[ColumnHealthSummary]") -> str:
        rows: List[str] = []
        for s in summaries:
            col_label = (
                html.escape(s.column)
                if s.column != "__table__"
                else "<em>Table-level</em>"
            )
            health = s.health_score
            health_cls = (
                "bar-high" if health >= 90 else "bar-mid" if health >= 70 else "bar-low"
            )
            null_str = f"{s.null_percent:.1f}%" if s.null_percent is not None else "—"
            null_bar = ""
            if s.null_percent is not None:
                np_val = min(s.null_percent, 100)
                bar_cls = (
                    "bar-low"
                    if np_val > 10
                    else ("bar-mid" if np_val > 2 else "bar-high")
                )
                null_bar = (
                    f'<div class="mini-bar-bg">'
                    f'<div class="mini-bar-fill {bar_cls}" style="width:{np_val}%"></div>'
                    f"</div>"
                )
            unique_str = (
                f"{s.unique_percent:.1f}%" if s.unique_percent is not None else "—"
            )
            unique_bar = ""
            if s.unique_percent is not None:
                up_val = min(s.unique_percent, 100)
                unique_bar = (
                    f'<div class="mini-bar-bg">'
                    f'<div class="mini-bar-fill bar-high" style="width:{up_val}%"></div>'
                    f"</div>"
                )

            rows.append(f"""<tr>
  <td class="col-name">{col_label}</td>
  <td class="center">{s.checks}</td>
  <td class="center passed-text">{s.passed}</td>
  <td class="center failed-text">{s.failed}</td>
  <td>
    <div class="health-cell">
      <span class="{health_cls}">{health}%</span>
      <div class="mini-bar-bg"><div class="mini-bar-fill {health_cls}" style="width:{health}%"></div></div>
    </div>
  </td>
  <td>{null_str}{null_bar}</td>
  <td>{unique_str}{unique_bar}</td>
</tr>""")
        return "\n".join(rows)

    # ------------------------------------------------------------------
    # Results table rows
    # ------------------------------------------------------------------

    def _render_rows(self, results: "List[ExpectationResult]") -> str:
        rows: List[str] = []
        for i, r in enumerate(results, 1):
            status_lower = r.status.lower()
            badge_class = status_lower if status_lower != "error" else "error"
            sev = r.severity
            sev_badge = {
                SEVERITY_CRITICAL: '<span class="sev-badge sev-critical">CRITICAL</span>',
                SEVERITY_WARNING: '<span class="sev-badge sev-warning">WARNING</span>',
                SEVERITY_INFO: '<span class="sev-badge sev-info">INFO</span>',
            }.get(sev, '<span class="sev-badge sev-warning">WARNING</span>')
            border_color = {
                SEVERITY_CRITICAL: "var(--failure)",
                SEVERITY_WARNING: "var(--warning)",
                SEVERITY_INFO: "var(--accent)",
            }.get(sev, "var(--warning)")

            col_str = html.escape(r.column or "—")
            obs_str = html.escape(r.human_observed)
            if len(obs_str) > 120:
                obs_str = obs_str[:117] + "..."

            # Unexpected column
            unexpected_html = self._render_unexpected(r)

            # Details JSON
            details_json = (
                json.dumps(to_native(r.details), indent=2, default=str)
                if r.details
                else "{}"
            )
            detail_id = f"detail-{i}"

            row_cls = f"row-{status_lower} row-sev-{sev}"
            rows.append(f"""
        <tr class="{row_cls}" style="border-left:4px solid {border_color}">
          <td class="center">{i}</td>
          <td>{sev_badge}</td>
          <td><span class="badge {badge_class}">{r.status}</span></td>
          <td><strong>{html.escape(r.expectation_type)}</strong></td>
          <td>{col_str}</td>
          <td class="obs-col">{obs_str}</td>
          <td>{unexpected_html}</td>
          <td>
            <span class="details-toggle" onclick="toggleDetails('{detail_id}')">View</span>
            <div id="{detail_id}" class="details-content">{html.escape(details_json)}</div>
          </td>
        </tr>""")
        return "\n".join(rows)

    # ------------------------------------------------------------------
    # Unexpected value rendering with mini bar
    # ------------------------------------------------------------------

    @staticmethod
    def _render_unexpected(r: "ExpectationResult") -> str:
        if r.exception_info:
            return f'<span class="err-text">{html.escape(r.exception_info[:80])}</span>'
        if r.unexpected_count == 0:
            return '<span class="dim">—</span>'
        pct = r.unexpected_percent
        count = r.unexpected_count
        bar_w = min(pct, 100)
        bar_cls = "bar-low" if pct > 20 else ("bar-mid" if pct > 5 else "bar-high")
        out = (
            f'{count} <span class="dim">({pct:.1f}%)</span>'
            f'<div class="mini-bar-bg mt4"><div class="mini-bar-fill {bar_cls}" '
            f'style="width:{bar_w}%"></div></div>'
        )
        if r.unexpected_values:
            vals = ", ".join(str(v) for v in r.unexpected_values[:5])
            out += f'<div class="unexpected-list">{html.escape(vals)}</div>'
        return out

    # ------------------------------------------------------------------
    # Build a plain-text summary for "Copy" button
    # ------------------------------------------------------------------

    @staticmethod
    def _build_summary_text(result: "ValidationResult") -> str:
        stats = result.statistics
        lines = [
            f"ValidateX Validation Report - {result.suite_name}",
            f"Status: {'ALL PASSED' if result.success else 'FAILED'}",
            f"Quality Score: {stats.get('quality_score', 'N/A')} / 100",
            f"Total: {stats.get('total', 0)} | Passed: {stats.get('passed', 0)} "
            f"| Failed: {stats.get('failed', 0)} | Errors: {stats.get('errors', 0)}",
            f"Success Rate: {stats.get('success_percent', 0)}%",
            f"Engine: {result.engine}",
            f"Duration: {stats.get('run_duration_seconds', 0)}s",
        ]
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Build CSV string for download
    # ------------------------------------------------------------------

    @staticmethod
    def _results_to_csv(results: "List[ExpectationResult]") -> str:
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(
            [
                "#",
                "Status",
                "Severity",
                "Expectation",
                "Column",
                "Observed",
                "Unexpected Count",
                "Unexpected %",
            ]
        )
        for i, r in enumerate(results, 1):
            writer.writerow(
                [
                    i,
                    r.status,
                    r.severity,
                    r.expectation_type,
                    r.column or "",
                    r.human_observed,
                    r.unexpected_count,
                    f"{r.unexpected_percent:.2f}",
                ]
            )
        return buf.getvalue()

    # ------------------------------------------------------------------
    # CSS
    # ------------------------------------------------------------------

    @staticmethod
    def _css() -> str:
        return """
:root {
  --bg: #0f0f1a;
  --surface: #1a1a2e;
  --surface2: #232340;
  --border: #2d2d50;
  --text: #e0e0ff;
  --text-dim: #8888aa;
  --accent: #6c63ff;
  --accent-glow: rgba(108, 99, 255, 0.3);
  --success: #00e676;
  --success-bg: rgba(0, 230, 118, 0.08);
  --failure: #ff5252;
  --failure-bg: rgba(255, 82, 82, 0.08);
  --warning: #ffc107;
  --warning-bg: rgba(255, 193, 7, 0.08);
  --info-blue: #42a5f5;
  --info-bg: rgba(66, 165, 245, 0.08);
  --radius: 12px;
  --shadow: 0 4px 24px rgba(0,0,0,0.4);
}
* { margin:0; padding:0; box-sizing:border-box; }
body {
  font-family: 'Segoe UI','Inter',-apple-system,BlinkMacSystemFont,sans-serif;
  background: var(--bg);
  color: var(--text);
  min-height: 100vh;
  line-height: 1.6;
}
.container { max-width:1280px; margin:0 auto; padding:32px 24px; }

/* Header */
.header { margin-bottom:28px; }
.header-top { display:flex; justify-content:space-between; align-items:flex-start; flex-wrap:wrap; gap:16px; }
.logo {
  font-size:2.4rem; font-weight:800;
  background: linear-gradient(135deg, var(--accent), #a78bfa, #f472b6);
  -webkit-background-clip:text; -webkit-text-fill-color:transparent;
  letter-spacing:-1px;
}
.subtitle { color:var(--text-dim); font-size:.95rem; }

/* Download bar */
.download-bar { display:flex; gap:8px; flex-wrap:wrap; }
.dl-btn {
  display:inline-flex; align-items:center; gap:6px;
  padding:8px 18px; border-radius:24px;
  border:1px solid var(--border); background:var(--surface);
  color:var(--text); cursor:pointer; font-size:.85rem;
  transition:all .2s;
}
.dl-btn:hover { background:var(--accent); border-color:var(--accent); color:#fff; box-shadow:0 0 16px var(--accent-glow); }
.dl-icon { font-size:1rem; }

/* Top row: status + quality score */
.top-row { display:grid; grid-template-columns:1fr 280px; gap:16px; margin-bottom:24px; }
@media(max-width:700px){ .top-row{grid-template-columns:1fr;} }

/* Status Banner */
.status-banner {
  padding:24px 32px; border-radius:var(--radius);
  display:flex; align-items:center; justify-content:center; gap:16px;
  font-size:1.5rem; font-weight:700; box-shadow:var(--shadow);
}
.status-banner.success {
  background:linear-gradient(135deg,rgba(0,230,118,.12),rgba(0,230,118,.04));
  border:1px solid rgba(0,230,118,.25); color:var(--success);
}
.status-banner.failure {
  background:linear-gradient(135deg,rgba(255,82,82,.12),rgba(255,82,82,.04));
  border:1px solid rgba(255,82,82,.25); color:var(--failure);
}
.status-icon { font-size:2rem; }

/* Quality Score Card */
.quality-score-card {
  background:var(--surface); border:1px solid var(--border);
  border-radius:var(--radius); padding:20px;
  display:flex; flex-direction:column; align-items:center; justify-content:center;
  box-shadow:var(--shadow); text-align:center;
}
.qs-label { font-size:.72rem; text-transform:uppercase; letter-spacing:2px; color:var(--text-dim); margin-bottom:4px; }
.qs-value { font-size:2.8rem; font-weight:900; line-height:1; }
.qs-max { font-size:.9rem; color:var(--text-dim); margin-bottom:8px; }
.score-high .qs-value { color:var(--success); }
.score-mid  .qs-value { color:var(--warning); }
.score-low  .qs-value { color:var(--failure); }
.qs-bar-bg { width:100%; height:6px; background:var(--surface2); border-radius:3px; overflow:hidden; }
.qs-bar-fill { height:100%; border-radius:3px; transition:width 1.5s ease; }
.score-high .qs-bar-fill { background:linear-gradient(90deg,var(--success),#66bb6a); }
.score-mid  .qs-bar-fill { background:linear-gradient(90deg,var(--warning),#ffca28); }
.score-low  .qs-bar-fill { background:linear-gradient(90deg,var(--failure),#ef5350); }

/* Stats Grid */
.stats-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:14px; margin-bottom:24px; }
.stat-card {
  background:var(--surface); border:1px solid var(--border);
  border-radius:var(--radius); padding:18px; text-align:center;
  box-shadow:var(--shadow); transition:transform .2s,box-shadow .2s;
}
.stat-card:hover { transform:translateY(-2px); box-shadow:0 8px 32px rgba(0,0,0,.5); }
.stat-value { font-size:1.8rem; font-weight:800; margin-bottom:2px; }
.stat-value.passed { color:var(--success); }
.stat-value.failed { color:var(--failure); }
.stat-value.errors { color:var(--warning); }
.stat-value.total  { color:var(--accent); }
.stat-value.rate   { background:linear-gradient(135deg,var(--accent),#a78bfa); -webkit-background-clip:text; -webkit-text-fill-color:transparent; }
.stat-label { color:var(--text-dim); font-size:.78rem; text-transform:uppercase; letter-spacing:1px; }

/* Progress Bar */
.progress-wrap { background:var(--surface); border:1px solid var(--border); border-radius:var(--radius); padding:16px 20px; margin-bottom:24px; box-shadow:var(--shadow); }
.progress-bar-bg { height:14px; background:var(--surface2); border-radius:7px; overflow:hidden; margin-top:6px; }
.progress-bar-fill { height:100%; border-radius:7px; background:linear-gradient(90deg,var(--success),#66bb6a); transition:width 1.5s ease; }
.progress-bar-fill.low { background:linear-gradient(90deg,var(--failure),#ef5350); }
.progress-labels { display:flex; justify-content:space-between; font-size:.82rem; color:var(--text-dim); }

/* Meta Grid */
.meta-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:10px; margin-bottom:24px; }
.meta-item { background:var(--surface); border:1px solid var(--border); border-radius:var(--radius); padding:12px 16px; display:flex; justify-content:space-between; align-items:center; }
.meta-key { color:var(--text-dim); font-size:.78rem; text-transform:uppercase; letter-spacing:.5px; }
.meta-val { font-weight:600; }

/* Section Card */
.section-card { background:var(--surface); border:1px solid var(--border); border-radius:var(--radius); overflow:hidden; box-shadow:var(--shadow); margin-bottom:24px; }
.section-card h2 { padding:16px 20px; font-size:1.1rem; border-bottom:1px solid var(--border); display:flex; align-items:center; gap:8px; }

/* Column Health Table */
.col-health-wrap { overflow-x:auto; }
.col-health-table { width:100%; border-collapse:collapse; }
.col-health-table thead th { text-align:left; padding:10px 14px; font-size:.72rem; text-transform:uppercase; letter-spacing:1px; color:var(--text-dim); background:var(--surface2); border-bottom:1px solid var(--border); }
.col-health-table tbody td { padding:10px 14px; border-bottom:1px solid var(--border); font-size:.88rem; }
.col-health-table tbody tr:hover { background:rgba(108,99,255,.04); }
.col-name { font-weight:600; }
.center { text-align:center; }
.passed-text { color:var(--success); }
.failed-text { color:var(--failure); }
.health-cell { display:flex; align-items:center; gap:8px; }

/* Mini bars */
.mini-bar-bg { height:6px; flex:1; min-width:50px; max-width:100px; background:var(--surface2); border-radius:3px; overflow:hidden; }
.mini-bar-fill { height:100%; border-radius:3px; transition:width .8s ease; }
.bar-high { background:var(--success); color:var(--success); }
.bar-mid  { background:var(--warning); color:var(--warning); }
.bar-low  { background:var(--failure); color:var(--failure); }
.mt4 { margin-top:4px; }

/* Drift card */
.drift-card { background:var(--surface); border:1px solid var(--border); border-radius:var(--radius); padding:20px; margin-bottom:24px; box-shadow:var(--shadow); }
.drift-card h2 { font-size:1.1rem; margin-bottom:8px; }
.drift-hint { color:var(--text-dim); font-size:.88rem; line-height:1.7; }
.drift-hint code { background:var(--surface2); padding:2px 8px; border-radius:4px; font-size:.82rem; color:var(--accent); }

/* Filter Buttons */
.filters { display:flex; gap:8px; margin-bottom:16px; flex-wrap:wrap; align-items:center; }
.filter-btn { padding:7px 18px; border-radius:24px; border:1px solid var(--border); background:var(--surface); color:var(--text); cursor:pointer; font-size:.85rem; transition:all .2s; }
.filter-btn:hover,.filter-btn.active { background:var(--accent); border-color:var(--accent); color:#fff; box-shadow:0 0 16px var(--accent-glow); }
.filter-sep { color:var(--text-dim); margin:0 4px; }

/* Results Table */
.results-table { width:100%; border-collapse:collapse; }
.results-table thead th { text-align:left; padding:12px 14px; font-size:.72rem; text-transform:uppercase; letter-spacing:1px; color:var(--text-dim); background:var(--surface2); border-bottom:1px solid var(--border); position:sticky; top:0; z-index:1; }
.results-table tbody tr { border-bottom:1px solid var(--border); transition:background .15s; }
.results-table tbody tr:hover { background:rgba(108,99,255,.04); }
.results-table td { padding:10px 14px; font-size:.88rem; vertical-align:top; }
.obs-col { max-width:260px; word-break:break-word; }

/* Severity badges */
.sev-badge { display:inline-block; padding:2px 10px; border-radius:12px; font-size:.7rem; font-weight:700; text-transform:uppercase; letter-spacing:.5px; }
.sev-critical { background:rgba(255,82,82,.12); color:var(--failure); border:1px solid rgba(255,82,82,.25); }
.sev-warning  { background:rgba(255,193,7,.12);  color:var(--warning); border:1px solid rgba(255,193,7,.25); }
.sev-info     { background:rgba(66,165,245,.12);  color:var(--info-blue); border:1px solid rgba(66,165,245,.25); }

/* Status badge */
.badge { display:inline-block; padding:3px 12px; border-radius:20px; font-size:.72rem; font-weight:700; text-transform:uppercase; }
.badge.passed { background:var(--success-bg); color:var(--success); border:1px solid rgba(0,230,118,.2); }
.badge.failed { background:var(--failure-bg); color:var(--failure); border:1px solid rgba(255,82,82,.2); }
.badge.error  { background:var(--warning-bg); color:var(--warning); border:1px solid rgba(255,193,7,.2); }

.details-toggle { color:var(--accent); cursor:pointer; font-size:.82rem; text-decoration:underline; }
.details-content { display:none; margin-top:8px; padding:10px; background:var(--surface2); border-radius:8px; font-family:'Cascadia Code','Fira Code',monospace; font-size:.78rem; white-space:pre-wrap; word-break:break-all; color:var(--text-dim); max-height:200px; overflow-y:auto; }
.details-content.open { display:block; }
.unexpected-list { margin-top:4px; padding:4px 10px; background:rgba(255,82,82,.06); border-radius:6px; font-size:.78rem; color:var(--failure); max-height:80px; overflow-y:auto; }
.err-text { color:var(--warning); font-size:.82rem; }
.dim { color:var(--text-dim); }

/* Footer */
.footer { text-align:center; padding:28px 0 12px; color:var(--text-dim); font-size:.78rem; }
.footer a { color:var(--accent); text-decoration:none; }

/* Animations */
@keyframes fadeUp { from{opacity:0;transform:translateY(20px)} to{opacity:1;transform:translateY(0)} }
.animate { animation:fadeUp .5s ease forwards; opacity:0; }
.delay-1 { animation-delay:.1s; }
.delay-2 { animation-delay:.2s; }
.delay-3 { animation-delay:.3s; }
.delay-4 { animation-delay:.4s; }

/* Toast notification */
.toast { position:fixed; bottom:24px; right:24px; background:var(--accent); color:#fff; padding:12px 24px; border-radius:var(--radius); font-size:.9rem; font-weight:600; box-shadow:0 4px 20px rgba(108,99,255,.4); opacity:0; transform:translateY(20px); transition:all .3s; z-index:1000; pointer-events:none; }
.toast.show { opacity:1; transform:translateY(0); }

@media(max-width:600px){
  .logo{font-size:1.6rem}
  .stat-value{font-size:1.3rem}
  .stats-grid{grid-template-columns:repeat(2,1fr)}
  .top-row{grid-template-columns:1fr}
  td{padding:6px 8px}
}
"""

    # ------------------------------------------------------------------
    # JavaScript
    # ------------------------------------------------------------------

    @staticmethod
    def _js() -> str:
        return """
function filterRows(type, el) {
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
  if (el) el.classList.add('active');
  document.querySelectorAll('.results-table tbody tr').forEach(row => {
    if (type === 'all') { row.style.display = ''; return; }
    // Status filters
    if (['passed','failed','error'].includes(type)) {
      row.style.display = row.classList.contains('row-' + type) ? '' : 'none';
      return;
    }
    // Severity filters
    row.style.display = row.classList.contains('row-sev-' + type) ? '' : 'none';
  });
}

function toggleDetails(id) {
  document.getElementById(id).classList.toggle('open');
}

function downloadJSON() {
  const data = document.getElementById('json-data').textContent;
  const blob = new Blob([data], {type:'application/json'});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a'); a.href = url;
  a.download = 'validatex_report.json'; a.click();
  URL.revokeObjectURL(url);
  showToast('JSON downloaded');
}

function downloadCSV() {
  const data = document.getElementById('csv-data').textContent;
  const blob = new Blob([data], {type:'text/csv'});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a'); a.href = url;
  a.download = 'validatex_report.csv'; a.click();
  URL.revokeObjectURL(url);
  showToast('CSV downloaded');
}

function copySummary() {
  const text = document.getElementById('summary-text').textContent;
  navigator.clipboard.writeText(text).then(() => {
    showToast('Summary copied to clipboard');
  }).catch(() => {
    // Fallback
    const ta = document.createElement('textarea');
    ta.value = text; document.body.appendChild(ta);
    ta.select(); document.execCommand('copy');
    ta.remove();
    showToast('Summary copied to clipboard');
  });
}

function showToast(msg) {
  let t = document.getElementById('vx-toast');
  if (!t) {
    t = document.createElement('div');
    t.id = 'vx-toast'; t.className = 'toast';
    document.body.appendChild(t);
  }
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 2200);
}

// Animate progress bars on load
window.addEventListener('load', () => {
  const bar = document.querySelector('.progress-bar-fill');
  if (bar) { const w = bar.style.width; bar.style.width = '0%'; setTimeout(() => bar.style.width = w, 200); }
  const qbar = document.querySelector('.qs-bar-fill');
  if (qbar) { const w = qbar.style.width; qbar.style.width = '0%'; setTimeout(() => qbar.style.width = w, 400); }
});
"""
