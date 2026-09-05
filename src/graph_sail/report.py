"""JSON, Graphviz, and self-contained HTML plan reporting."""

from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any

from graph_sail.analysis import PlanMetrics, analyze_plan
from graph_sail.errors import OutputError
from graph_sail.models import GraphSpec, PlanResult

_COLORS = ("#0ea5e9", "#8b5cf6", "#10b981", "#f59e0b", "#ef4444", "#14b8a6")


def write_report_bundle(
    graph: GraphSpec, plan: PlanResult, output_dir: str | Path
) -> dict[str, Path]:
    """Write stable machine and human-readable artifacts."""

    destination = Path(output_dir)
    metrics = analyze_plan(graph, plan)
    paths = {
        "plan": destination / "plan.json",
        "graph": destination / "plan.dot",
        "report": destination / "report.html",
    }
    try:
        destination.mkdir(parents=True, exist_ok=True)
        paths["plan"].write_text(
            json.dumps(
                {**plan.to_dict(), "metrics": _metrics_dict(metrics)},
                indent=2,
                sort_keys=True,
                allow_nan=False,
            )
            + "\n",
            encoding="utf-8",
            newline="\n",
        )
        paths["graph"].write_text(render_dot(graph, plan), encoding="utf-8", newline="\n")
        paths["report"].write_text(
            render_html(graph, plan, metrics), encoding="utf-8", newline="\n"
        )
    except (OSError, ValueError) as exc:
        raise OutputError(f"cannot write report bundle to {destination}: {exc}") from exc
    return paths


def render_dot(graph: GraphSpec, plan: PlanResult) -> str:
    """Render a Graphviz document without requiring Graphviz at runtime."""

    placements = plan.placements
    lines = [
        "digraph plan {",
        '  graph [rankdir="LR", bgcolor="transparent"];',
        '  node [shape="box", style="rounded,filled", fontname="Arial"];',
    ]
    for index, device in enumerate(sorted(graph.devices, key=lambda item: item.name)):
        color = _COLORS[index % len(_COLORS)]
        lines.append(f'  subgraph "cluster_{_dot(device.name)}" {{')
        lines.append(f'    label="{_dot(device.name)}"; color="{color}";')
        for node in sorted(graph.nodes, key=lambda item: item.id):
            if placements[node.id] == device.name:
                lines.append(
                    f'    "{_dot(node.id)}" [label="{_dot(node.id)}\\n{_dot(node.kind)}", '
                    f'fillcolor="{color}22", color="{color}"];'
                )
        lines.append("  }")
    for edge in graph.edges:
        label = edge.label or f"{edge.payload_mb:g} MB"
        lines.append(f'  "{_dot(edge.source)}" -> "{_dot(edge.target)}" [label="{_dot(label)}"];')
    lines.append("}")
    return "\n".join(lines) + "\n"


def render_html(graph: GraphSpec, plan: PlanResult, metrics: PlanMetrics | None = None) -> str:
    """Render a dependency-free report with an SVG schedule timeline."""

    metrics = metrics or analyze_plan(graph, plan)
    devices = sorted(graph.devices, key=lambda item: item.name)
    width = 940
    label_width = 140
    chart_width = width - label_width - 25
    row_height = 70
    height = 35 + row_height * len(devices)
    scale = chart_width / plan.makespan_ms if plan.makespan_ms else 1.0
    svg: list[str] = [
        f'<svg viewBox="0 0 {width} {height}" role="img" '
        f'aria-label="Schedule for {html.escape(graph.name)}">'
    ]
    for index, device in enumerate(devices):
        y = 25 + index * row_height
        color = _COLORS[index % len(_COLORS)]
        svg.append(
            f'<text x="8" y="{y + 24}" class="device-label">{html.escape(device.name)}</text>'
        )
        svg.append(
            f'<line x1="{label_width}" y1="{y + 30}" x2="{width - 15}" '
            f'y2="{y + 30}" stroke="#26334a" />'
        )
        for item in plan.schedule:
            if item.device != device.name:
                continue
            x = label_width + item.start_ms * scale
            item_width = max(item.compute_ms * scale, 3.0)
            svg.append(
                f'<rect x="{x:.2f}" y="{y + 8}" width="{item_width:.2f}" height="36" '
                f'rx="7" fill="{color}" opacity="0.88"><title>{html.escape(item.node)}: '
                f"{item.start_ms:.2f}-{item.finish_ms:.2f} ms</title></rect>"
            )
            if item_width > 42:
                svg.append(
                    f'<text x="{x + 7:.2f}" y="{y + 31}" class="node-label">'
                    f"{html.escape(item.node)}</text>"
                )
    svg.append("</svg>")

    schedule_rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(item.node)}</td><td>{html.escape(item.device)}</td>"
        f"<td>{item.start_ms:.3f}</td><td>{item.finish_ms:.3f}</td>"
        f"<td>{item.incoming_transfer_ms:.3f}</td><td>{item.memory_mb:.1f}</td>"
        "</tr>"
        for item in plan.schedule
    )
    memory_cards = "\n".join(
        f'<div class="metric"><span>{html.escape(device.name)} memory</span>'
        f"<strong>{plan.memory_used_mb[device.name]:.0f} / {device.memory_mb:.0f} MB</strong></div>"
        for device in devices
    )
    chain = " → ".join(metrics.critical_chain) or "none"
    modelling = _modelling_section(plan)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Graph Sail — {html.escape(graph.name)}</title>
  <style>
    :root {{ color-scheme: dark; font-family: Inter, ui-sans-serif, system-ui, sans-serif; }}
    body {{ margin: 0; background: #07111f; color: #e5edf8; }}
    main {{ max-width: 1080px; margin: auto; padding: 42px 24px 64px; }}
    .eyebrow {{ color: #38bdf8; font-weight: 700; letter-spacing: .14em;
      text-transform: uppercase; }}
    h1 {{ font-size: clamp(2rem, 5vw, 4rem); margin: .2em 0; }}
    .subtitle {{ color: #9fb0c8; max-width: 720px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit,minmax(190px,1fr));
      gap: 12px; margin: 28px 0; }}
    .metric, section {{ background: #0d1b2e; border: 1px solid #1e3150; border-radius: 14px; }}
    .metric {{ padding: 16px; display: grid; gap: 8px; }}
    .metric span {{ color: #8da2bd; font-size: .82rem; }}
    section {{ padding: 20px; margin-top: 18px; overflow-x: auto; }}
    svg {{ width: 100%; min-width: 720px; background: #091526; border-radius: 10px; }}
    .device-label {{ fill: #dbeafe; font: 600 13px system-ui; }}
    .node-label {{ fill: #06111f; font: 700 11px system-ui; pointer-events: none; }}
    table {{ border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }}
    th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #1b2b44; }}
    th {{ color: #7dd3fc; font-size: .8rem; text-transform: uppercase; }}
    code {{ color: #bae6fd; }}
  </style>
</head>
<body><main>
  <div class="eyebrow">Graph Sail plan</div>
  <h1>{html.escape(graph.name)}</h1>
  <p class="subtitle">Deterministic placement report generated from measured component
    estimates and explicit device constraints.</p>
  <div class="grid">
    <div class="metric"><span>Algorithm</span><strong>{html.escape(plan.algorithm)}</strong></div>
    <div class="metric"><span>Makespan</span><strong>{plan.makespan_ms:.2f} ms</strong></div>
    <div class="metric"><span>Cross-device edges</span>
      <strong>{metrics.cross_device_edges}</strong></div>
    <div class="metric"><span>Transfer estimate</span>
      <strong>{metrics.total_transfer_ms:.2f} ms</strong></div>
    {memory_cards}
  </div>
  <section><h2>Schedule</h2>{"".join(svg)}</section>
  <section><h2>Critical chain</h2><p><code>{html.escape(chain)}</code></p></section>{modelling}
  <section><h2>Node detail</h2><table><thead><tr>
    <th>Node</th><th>Device</th><th>Start ms</th><th>Finish ms</th>
    <th>Transfer ms</th><th>Memory MB</th>
  </tr></thead><tbody>{schedule_rows}</tbody></table></section>
</main></body></html>
"""


def _modelling_section(plan: PlanResult) -> str:
    """Render the latency-model section, or nothing when no model is configured."""

    rows = [
        item for item in plan.schedule if item.latency_scale != 1.0 or item.batch_window_ms != 0.0
    ]
    if not rows:
        return ""
    body = "".join(
        "<tr>"
        f"<td>{html.escape(item.node)}</td><td>{html.escape(item.device)}</td>"
        f"<td>{item.compute_ms / item.latency_scale:.3f}</td>"
        f"<td>{item.compute_ms:.3f}</td><td>{item.latency_scale:.4f}</td>"
        f"<td>{item.batch_window_ms:.3f}</td>"
        "</tr>"
        for item in rows
    )
    return (
        '<section><h2>Latency modelling</h2><p class="subtitle">Configured contention and '
        "batching rescale the caller&#x27;s isolated estimate and delay batched starts. These "
        "are declared model parameters, not measurements of this deployment.</p><table><thead>"
        "<tr><th>Node</th><th>Device</th><th>Isolated ms</th><th>Effective ms</th><th>Scale</th>"
        f"<th>Batch window ms</th></tr></thead><tbody>{body}</tbody></table></section>"
    )


def _metrics_dict(metrics: PlanMetrics) -> dict[str, Any]:
    return {
        "makespan_ms": round(metrics.makespan_ms, 6),
        "total_compute_ms": round(metrics.total_compute_ms, 6),
        "total_transfer_ms": round(metrics.total_transfer_ms, 6),
        "cross_device_edges": metrics.cross_device_edges,
        "critical_chain": list(metrics.critical_chain),
        "device_utilization": {
            key: round(value, 6) for key, value in metrics.device_utilization.items()
        },
        "memory_utilization": {
            key: round(value, 6) for key, value in metrics.memory_utilization.items()
        },
    }


def _dot(value: str) -> str:
    escaped: list[str] = []
    for character in value:
        codepoint = ord(character)
        if character == "\\":
            escaped.append("\\\\")
        elif character == '"':
            escaped.append('\\"')
        elif character == "\n":
            escaped.append("\\n")
        elif codepoint < 32 or codepoint == 127:
            escaped.append(f"\\\\u{codepoint:04x}")
        else:
            escaped.append(character)
    return "".join(escaped)
