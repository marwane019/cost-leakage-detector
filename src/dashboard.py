"""
dashboard.py — Interactive Plotly HTML Dashboard.

Generates a self-contained HTML file with four interactive charts:
    1. Leakage by Category      — Horizontal bar (£ leakage, colour = severity)
    2. Daily Leakage Trend      — Area chart with Critical count overlay
    3. Severity Heatmap         — Rule × Severity flag count grid
    4. Top Supplier Exposure    — Stacked bar (leakage per rule per supplier)

The output is a single portable HTML file with all JS embedded —
no server required, opens directly in any browser.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yaml

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Brand colour palette (matches Excel report)
# ---------------------------------------------------------------------------
SEVERITY_COLOURS = {
    "Critical": "#C00000",
    "High":     "#C65911",
    "Medium":   "#BF8F00",
    "Low":      "#375623",
}

RULE_COLOURS = {
    "duplicate":      "#1F4E79",
    "price_variance": "#C00000",
    "sla_breach":     "#C65911",
    "volume_spike":   "#7030A0",
}

RULE_LABELS = {
    "duplicate":      "Duplicate Transactions",
    "price_variance": "Price Variance / Overcharge",
    "sla_breach":     "SLA Breach",
    "volume_spike":   "Volume Spike",
}

DASHBOARD_TEMPLATE = "plotly_white"


def _chart_leakage_by_category(scored: pd.DataFrame) -> go.Figure:
    """Build a horizontal bar chart: estimated leakage by procurement category.

    Each bar is split by rule type (stacked) so analysts can see which rules
    dominate within each spend category.

    Args:
        scored: Scored flagged transactions.

    Returns:
        Plotly Figure object.
    """
    category_rule = (
        scored.groupby(["category", "rule_triggered"])["leakage_amount_gbp"]
        .sum()
        .reset_index()
    )

    fig = go.Figure()
    for rule in scored["rule_triggered"].unique():
        mask = category_rule["rule_triggered"] == rule
        subset = category_rule[mask].sort_values("leakage_amount_gbp")
        fig.add_trace(
            go.Bar(
                y=subset["category"],
                x=subset["leakage_amount_gbp"],
                name=RULE_LABELS.get(rule, rule),
                orientation="h",
                marker_color=RULE_COLOURS.get(rule, "#888888"),
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    f"Rule: {RULE_LABELS.get(rule, rule)}<br>"
                    "Leakage: £%{x:,.2f}<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        title={
            "text": "Estimated Leakage by Procurement Category",
            "font": {"size": 16, "color": "#1F4E79"},
        },
        barmode="stack",
        xaxis_title="Estimated Leakage (£)",
        yaxis_title="Category",
        template=DASHBOARD_TEMPLATE,
        legend={"title": "Detection Rule"},
        height=380,
        margin={"l": 160, "r": 30, "t": 60, "b": 40},
        xaxis={"tickformat": "£,.0f"},
    )
    return fig


def _chart_daily_trend(scored: pd.DataFrame) -> go.Figure:
    """Build a daily leakage area chart with Critical flag count as a bar overlay.

    Uses a secondary Y-axis for the Critical count so both series are
    readable at different scales.

    Args:
        scored: Scored flagged transactions.

    Returns:
        Plotly Figure with secondary y-axis.
    """
    scored_copy = scored.copy()
    if not pd.api.types.is_datetime64_any_dtype(scored_copy["date"]):
        scored_copy["date"] = pd.to_datetime(scored_copy["date"])

    daily = (
        scored_copy.groupby(scored_copy["date"].dt.date)
        .agg(
            leakage_gbp=("leakage_amount_gbp", "sum"),
            critical_count=("severity", lambda s: (s == "Critical").sum()),
            flag_count=("transaction_id", "count"),
        )
        .reset_index()
        .rename(columns={"date": "Date"})
        .sort_values("Date")
    )

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Area: total daily leakage
    fig.add_trace(
        go.Scatter(
            x=daily["Date"],
            y=daily["leakage_gbp"],
            name="Daily Leakage (£)",
            fill="tozeroy",
            mode="lines",
            line={"color": "#1F4E79", "width": 2},
            fillcolor="rgba(31, 78, 121, 0.15)",
            hovertemplate="Date: %{x}<br>Leakage: £%{y:,.2f}<extra></extra>",
        ),
        secondary_y=False,
    )

    # Bar: Critical count overlay
    fig.add_trace(
        go.Bar(
            x=daily["Date"],
            y=daily["critical_count"],
            name="Critical Flags",
            marker_color="#C00000",
            opacity=0.6,
            hovertemplate="Date: %{x}<br>Critical Flags: %{y}<extra></extra>",
        ),
        secondary_y=True,
    )

    fig.update_layout(
        title={
            "text": "Daily Leakage Trend with Critical Flag Count",
            "font": {"size": 16, "color": "#1F4E79"},
        },
        template=DASHBOARD_TEMPLATE,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
        height=380,
        margin={"l": 60, "r": 60, "t": 80, "b": 40},
    )
    fig.update_yaxes(title_text="Estimated Leakage (£)", tickformat="£,.0f", secondary_y=False)
    fig.update_yaxes(title_text="Critical Flag Count", secondary_y=True)
    return fig


def _chart_severity_heatmap(scored: pd.DataFrame) -> go.Figure:
    """Build a Rule × Severity heatmap showing flag count density.

    Provides an at-a-glance view of which rules generate the highest severity
    findings — essential for prioritisation.

    Args:
        scored: Scored flagged transactions.

    Returns:
        Plotly Figure heatmap.
    """
    pivot = (
        scored.groupby(["rule_triggered", "severity"])
        .size()
        .unstack(fill_value=0)
    )

    # Ensure all severity columns present and in order
    for sev in ["Low", "Medium", "High", "Critical"]:
        if sev not in pivot.columns:
            pivot[sev] = 0
    pivot = pivot[["Low", "Medium", "High", "Critical"]]
    pivot.index = [RULE_LABELS.get(r, r) for r in pivot.index]

    # Custom colour scale: green→red
    colorscale = [
        [0.0,  "#E2EFDA"],
        [0.33, "#FFFFE0"],
        [0.66, "#FFE5CC"],
        [1.0,  "#C00000"],
    ]

    hover_text = [
        [f"{rule}<br>Severity: {sev}<br>Flags: {pivot.at[rule, sev]}"
         for sev in pivot.columns]
        for rule in pivot.index
    ]

    fig = go.Figure(
        go.Heatmap(
            z=pivot.values,
            x=list(pivot.columns),
            y=list(pivot.index),
            colorscale=colorscale,
            text=pivot.values,
            texttemplate="%{text}",
            textfont={"size": 14, "color": "#1F4E79"},
            hovertext=hover_text,
            hoverinfo="text",
            showscale=True,
            colorbar={"title": "Flags"},
        )
    )
    fig.update_layout(
        title={
            "text": "Severity Heatmap: Rule × Severity Flag Density",
            "font": {"size": 16, "color": "#1F4E79"},
        },
        xaxis_title="Severity",
        yaxis_title="Detection Rule",
        template=DASHBOARD_TEMPLATE,
        height=360,
        margin={"l": 200, "r": 30, "t": 60, "b": 40},
    )
    return fig


def _chart_top_supplier_exposure(scored: pd.DataFrame, top_n: int = 8) -> go.Figure:
    """Build a stacked bar chart of leakage exposure for the top N suppliers.

    Breaks down each supplier's leakage by detection rule, enabling the
    procurement team to identify high-risk vendor relationships.

    Args:
        scored: Scored flagged transactions.
        top_n: Number of suppliers to display (by total leakage).

    Returns:
        Plotly Figure object.
    """
    top_suppliers = (
        scored.groupby("supplier_name")["leakage_amount_gbp"]
        .sum()
        .nlargest(top_n)
        .index.tolist()
    )
    subset = scored[scored["supplier_name"].isin(top_suppliers)]

    supplier_rule = (
        subset.groupby(["supplier_name", "rule_triggered"])["leakage_amount_gbp"]
        .sum()
        .reset_index()
    )

    # Sort suppliers by total leakage for clean ordering
    order = (
        supplier_rule.groupby("supplier_name")["leakage_amount_gbp"]
        .sum()
        .sort_values(ascending=True)
        .index.tolist()
    )

    fig = go.Figure()
    for rule in supplier_rule["rule_triggered"].unique():
        mask = supplier_rule["rule_triggered"] == rule
        rule_data = (
            supplier_rule[mask]
            .set_index("supplier_name")
            .reindex(order)
            .fillna(0)
        )
        fig.add_trace(
            go.Bar(
                y=order,
                x=rule_data["leakage_amount_gbp"],
                name=RULE_LABELS.get(rule, rule),
                orientation="h",
                marker_color=RULE_COLOURS.get(rule, "#888888"),
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    f"Rule: {RULE_LABELS.get(rule, rule)}<br>"
                    "Leakage: £%{x:,.2f}<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        title={
            "text": f"Top {top_n} Suppliers by Leakage Exposure",
            "font": {"size": 16, "color": "#1F4E79"},
        },
        barmode="stack",
        xaxis_title="Estimated Leakage (£)",
        yaxis_title="Supplier",
        template=DASHBOARD_TEMPLATE,
        legend={"title": "Detection Rule"},
        height=400,
        margin={"l": 200, "r": 30, "t": 60, "b": 40},
        xaxis={"tickformat": "£,.0f"},
    )
    return fig


def _build_kpi_header(summary: dict[str, Any]) -> str:
    """Generate an HTML KPI banner for the dashboard header.

    Args:
        summary: Executive summary dict.

    Returns:
        HTML string containing the KPI tile row.
    """
    sev = summary["severity_breakdown"]
    tiles = [
        ("Total Leakage",      f"£{summary['headline_gbp']:,.2f}",              "#C00000"),
        ("Transactions",       f"{summary['total_transactions_analysed']:,}",    "#1F4E79"),
        ("Flags Raised",       f"{summary['total_flags']:,}",                    "#1F4E79"),
        ("Critical",           str(sev.get("Critical", 0)),                      "#CC0000"),
        ("High",               str(sev.get("High", 0)),                          "#C65911"),
        ("Medium",             str(sev.get("Medium", 0)),                        "#BF8F00"),
        ("Low",                str(sev.get("Low", 0)),                           "#375623"),
    ]
    tile_html = ""
    for label, value, colour in tiles:
        tile_html += f"""
        <div style="
            background:{colour}; color:white; border-radius:8px;
            padding:12px 18px; min-width:120px; text-align:center;
            box-shadow: 2px 2px 6px rgba(0,0,0,0.2);
        ">
            <div style="font-size:11px; font-weight:600; letter-spacing:1px; opacity:0.85;">{label.upper()}</div>
            <div style="font-size:22px; font-weight:700; margin-top:4px;">{value}</div>
        </div>"""

    date_range = f"{summary['date_range']['start']} → {summary['date_range']['end']}"
    return f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; background:#1F4E79; padding:20px 30px;">
        <h1 style="color:white; margin:0 0 4px 0; font-size:22px;">
            Operations Cost Leakage Detector
        </h1>
        <p style="color:rgba(255,255,255,0.75); margin:0 0 16px 0; font-size:13px;">
            Analysis Period: {date_range} &nbsp;|&nbsp; Generated: {datetime.today().strftime('%Y-%m-%d %H:%M')}
        </p>
        <div style="display:flex; gap:12px; flex-wrap:wrap;">
            {tile_html}
        </div>
    </div>
    """


def generate_dashboard(
    scored: pd.DataFrame,
    summary: dict[str, Any],
    config_path: str = "config.yaml",
) -> Path:
    """Assemble the full interactive dashboard and write to HTML.

    Combines the KPI header with four Plotly charts in a responsive
    two-column layout. Output is a single self-contained HTML file.

    Args:
        scored: Scored flagged transactions DataFrame.
        summary: Executive summary dict.
        config_path: Path to configuration YAML.

    Returns:
        Path to the generated .html file.
    """
    with open(config_path, "r") as fh:
        cfg = yaml.safe_load(fh)

    run_date = datetime.today().strftime("%Y-%m-%d")
    output_dir = Path(cfg["paths"]["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = cfg["paths"]["dashboard_filename"].format(date=run_date)
    output_path = output_dir / filename

    logger.info("Building interactive dashboard — %d records to visualise", len(scored))

    fig_category    = _chart_leakage_by_category(scored)
    fig_trend       = _chart_daily_trend(scored)
    fig_heatmap     = _chart_severity_heatmap(scored)
    fig_suppliers   = _chart_top_supplier_exposure(scored)

    # Convert charts to HTML div snippets (no full HTML, just the div)
    chart_args = {"include_plotlyjs": False, "full_html": False}
    div_category    = fig_category.to_html(**chart_args)
    div_trend       = fig_trend.to_html(**chart_args)
    div_heatmap     = fig_heatmap.to_html(**chart_args)
    div_suppliers   = fig_suppliers.to_html(**chart_args)

    kpi_header = _build_kpi_header(summary)

    # Assemble final HTML
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Cost Leakage Detector — {run_date}</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #F5F5F5; }}
        .charts-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 16px;
            padding: 20px;
        }}
        .chart-card {{
            background: white;
            border-radius: 8px;
            padding: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }}
        .chart-card.full-width {{
            grid-column: 1 / -1;
        }}
        .footer {{
            text-align: center;
            padding: 16px;
            color: #888;
            font-size: 12px;
        }}
        @media (max-width: 900px) {{
            .charts-grid {{ grid-template-columns: 1fr; }}
            .chart-card.full-width {{ grid-column: 1; }}
        }}
    </style>
</head>
<body>
    {kpi_header}
    <div class="charts-grid">
        <div class="chart-card">{div_category}</div>
        <div class="chart-card">{div_trend}</div>
        <div class="chart-card">{div_heatmap}</div>
        <div class="chart-card">{div_suppliers}</div>
    </div>
    <div class="footer">
        Operations Cost Leakage Detector v1.0 &nbsp;|&nbsp;
        Generated {datetime.today().strftime('%Y-%m-%d %H:%M')} &nbsp;|&nbsp;
        For internal use only — Acme Operations Ltd
    </div>
</body>
</html>"""

    output_path.write_text(html, encoding="utf-8")
    logger.info("Dashboard saved to %s", output_path)
    return output_path
