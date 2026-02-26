"""
Report Generator — creates downloadable HTML dashboard reports with embedded Plotly charts.
"""

import pandas as pd
import plotly.express as px
from datetime import datetime


def _build_chart_html(df, chart_type, x_col, y_col):
    """Build an interactive Plotly chart and return its HTML."""
    if df is None or df.empty:
        return ""

    cols = df.columns.tolist()
    if x_col not in cols:
        x_col = cols[0]
    if y_col not in cols or y_col == x_col:
        nc = df.select_dtypes("number").columns.tolist()
        y_col = nc[0] if nc else cols[-1]

    COLORS = ["#6366f1", "#8b5cf6", "#a78bfa", "#c4b5fd", "#818cf8", "#e879f9"]
    layout = dict(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Segoe UI, sans-serif", color="#e0e0e0", size=12),
        margin=dict(l=30, r=20, t=40, b=30),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickfont=dict(color="#9ca3af")),
        yaxis=dict(gridcolor="rgba(255,255,255,0.08)", tickfont=dict(color="#9ca3af")),
    )

    if chart_type == "pie":
        fig = px.pie(df, names=x_col, values=y_col, color_discrete_sequence=COLORS, hole=0.35)
        fig.update_traces(textfont_color="#e0e0e0")
    elif chart_type == "line":
        fig = px.line(df, x=x_col, y=y_col, markers=True, color_discrete_sequence=COLORS)
        fig.update_traces(line=dict(width=3))
    else:
        fig = px.bar(df, x=x_col, y=y_col, color_discrete_sequence=COLORS, text_auto=True)
        fig.update_traces(textfont_size=11, textposition="outside", marker=dict(line=dict(width=0)))

    fig.update_layout(**layout)

    # Return self-contained HTML div (includes Plotly.js CDN)
    return fig.to_html(full_html=False, include_plotlyjs="cdn")


def generate_html_report(
    question: str,
    explanation: str,
    sql: str,
    df: pd.DataFrame,
    source_name: str,
    chart_type: str,
    model_name: str,
    x_col: str = "",
    y_col: str = "",
) -> str:
    """Generate a self-contained HTML report with embedded interactive chart + table."""

    # Build chart HTML
    chart_html = _build_chart_html(df, chart_type, x_col, y_col)

    # Build data table
    table_html = ""
    if df is not None and not df.empty:
        table_html = '<table class="data-table">'
        table_html += "<thead><tr>"
        for col in df.columns:
            table_html += f"<th>{col.replace('_', ' ').title()}</th>"
        table_html += "</tr></thead><tbody>"
        for _, row in df.head(20).iterrows():
            table_html += "<tr>"
            for val in row:
                formatted = f"{val:,.2f}" if isinstance(val, float) else str(val)
                table_html += f"<td>{formatted}</td>"
            table_html += "</tr>"
        table_html += "</tbody></table>"

    now = datetime.now().strftime("%B %d, %Y at %I:%M %p")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Agentic BI Report — {source_name}</title>
<style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
        font-family: 'Segoe UI', -apple-system, sans-serif;
        background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
        color: #e0e0e0; padding: 40px; min-height: 100vh;
    }}
    .container {{ max-width: 1000px; margin: auto; }}
    .header {{
        background: linear-gradient(135deg, rgba(99,102,241,0.3), rgba(139,92,246,0.3));
        border: 1px solid rgba(139,92,246,0.4);
        border-radius: 16px; padding: 28px 32px; margin-bottom: 24px;
    }}
    .header h1 {{ font-size: 1.8rem; font-weight: 700; }}
    .header .meta {{ font-size: 0.82rem; color: #a5b4fc; margin-top: 6px; }}
    .badge {{
        display: inline-block; background: rgba(139,92,246,0.3);
        border: 1px solid rgba(139,92,246,0.5);
        padding: 3px 10px; border-radius: 20px;
        font-size: 0.75rem; font-weight: 600; color: #c4b5fd;
    }}
    .section {{
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 14px; padding: 24px 28px; margin-bottom: 20px;
    }}
    .section-title {{
        font-size: 0.78rem; font-weight: 700; color: #a5b4fc;
        text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 14px;
    }}
    .question-box {{
        background: rgba(99,102,241,0.15); border: 1px solid rgba(99,102,241,0.3);
        border-radius: 10px; padding: 14px 18px; font-size: 1rem; margin-bottom: 8px;
    }}
    .explanation {{ font-size: 0.95rem; line-height: 1.7; }}
    .sql-block {{
        background: #1e1e2e; color: #a5b4fc; padding: 16px 20px; border-radius: 10px;
        font-family: 'Courier New', monospace; font-size: 0.82rem;
        white-space: pre-wrap; overflow-x: auto;
    }}
    .chart-container {{
        border-radius: 10px; overflow: hidden; margin-bottom: 8px;
    }}
    .data-table {{
        width: 100%; border-collapse: collapse; font-size: 0.85rem;
    }}
    .data-table th {{
        background: rgba(99,102,241,0.2); color: #a5b4fc;
        padding: 10px 14px; text-align: left; font-weight: 600;
        border-bottom: 1px solid rgba(255,255,255,0.1);
    }}
    .data-table td {{
        padding: 8px 14px; border-bottom: 1px solid rgba(255,255,255,0.05);
    }}
    .data-table tr:hover {{ background: rgba(99,102,241,0.08); }}
    .footer {{
        text-align: center; font-size: 0.75rem; color: #4b5563;
        margin-top: 30px; padding: 20px;
    }}
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>🤖 Agentic BI Report</h1>
        <div class="meta">
            <span class="badge">{source_name}</span>
            <span class="badge">{model_name}</span>
            <span class="badge">📊 {chart_type.title()} Chart</span>
            &nbsp;·&nbsp; Generated: {now}
        </div>
    </div>

    <div class="section">
        <div class="section-title">❓ Question</div>
        <div class="question-box">{question}</div>
    </div>

    <div class="section">
        <div class="section-title">🧠 AI Analysis</div>
        <div class="explanation">{explanation}</div>
    </div>

    <div class="section">
        <div class="section-title">📊 Interactive Chart</div>
        <div class="chart-container">
            {chart_html if chart_html else '<p style="color:#6b7280;">No chart data available</p>'}
        </div>
    </div>

    <div class="section">
        <div class="section-title">🔍 Generated SQL</div>
        <div class="sql-block">{sql}</div>
    </div>

    <div class="section">
        <div class="section-title">📋 Data ({len(df) if df is not None else 0} rows)</div>
        {table_html if table_html else '<p style="color:#6b7280;">No data available</p>'}
    </div>

    <div class="footer">
        Agentic BI Assistant · Powered by Vertex AI + BigQuery<br>
        Report generated on {now}
    </div>
</div>
</body>
</html>"""
    return html
