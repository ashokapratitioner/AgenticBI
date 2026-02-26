"""
Vertex AI Agent — generates SQL + explanations from natural language.
Local sources return mock responses. Live sources call Vertex AI.
"""

import os
import json
import re
from dotenv import load_dotenv
from backend.bq_client import get_schema_description

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GCP_LOCATION = os.getenv("GCP_LOCATION", "us-central1")


# ── Local demo responses per source ──────────────────────────────────────────

LOCAL_RESPONSES = {
    "salesforce": {
        "sql": "SELECT stage, COUNT(*) AS deal_count, SUM(amount) AS total_value\nFROM `erp_poc.sf_opportunities`\nGROUP BY stage ORDER BY total_value DESC",
        "explanation": "I analysed the Salesforce opportunity pipeline. **Closed Won** leads at $3.2M across 22 deals, while **Proposal** stage has $2.1M indicating a strong pipeline. The **Closed Lost** rate is relatively low, suggesting effective qualification.",
        "chart_type": "bar",
        "x_col": "stage",
        "y_col": "total_value",
        "followups": [
            "What is the average deal size by region?",
            "Show win rate trend over last 6 months",
            "Which sales rep has the highest close rate?",
        ],
    },
    "netsuite": {
        "sql": "SELECT department, SUM(amount) AS total_amount, COUNT(*) AS transaction_count\nFROM `erp_poc.ns_gl_transactions`\nGROUP BY department ORDER BY total_amount DESC",
        "explanation": "NetSuite GL analysis shows **Operations** has the highest spend at $320K with 120 transactions. **R&D** is second at $280K. All departments have budget remaining, with **R&D** having the most runway ($120K).",
        "chart_type": "bar",
        "x_col": "department",
        "y_col": "total_amount",
        "followups": [
            "Show accounts payable aging report",
            "Which vendors have overdue invoices?",
            "Monthly revenue vs expense trend",
        ],
    },
    "coupa": {
        "sql": "SELECT supplier, po_count, total_spend\nFROM `erp_poc.coupa_purchase_orders`\nGROUP BY supplier ORDER BY total_spend DESC",
        "explanation": "Coupa procurement analysis: **Global Parts** is the top supplier at $520K across 45 POs. **CloudServ** follows at $310K. Average delivery ranges from 1-8 days, with **CloudServ** (cloud services) being fastest at 1 day.",
        "chart_type": "bar",
        "x_col": "supplier",
        "y_col": "total_spend",
        "followups": [
            "Which suppliers have the fastest delivery times?",
            "Show procurement spend by category",
            "List POs pending approval over $50K",
        ],
    },
    "workday": {
        "sql": "SELECT department, headcount, attrition_rate, avg_tenure_years\nFROM `erp_poc.wd_employees`\nGROUP BY department",
        "explanation": "Workday headcount analysis: **Engineering** is the largest team (120 people). **Support** has the highest attrition at 15%, while **HR** has the lowest at 3%. Average tenure ranges from 2.5 to 5.0 years.",
        "chart_type": "bar",
        "x_col": "department",
        "y_col": "headcount",
        "followups": [
            "Which departments need to hire most urgently?",
            "Show payroll cost breakdown by department",
            "What is the time-off utilization rate?",
        ],
    },
    "jira": {
        "sql": "SELECT project, open_issues, completed_sprints, avg_velocity\nFROM `erp_poc.jira_sprints`\nGROUP BY project",
        "explanation": "JIRA project overview: **Frontend** has the most open issues (55) but strong velocity. **DevOps** leads in sprint completion (15 sprints) with the highest velocity (40 pts). **Security** has the fewest open issues — good sign.",
        "chart_type": "bar",
        "x_col": "project",
        "y_col": "open_issues",
        "followups": [
            "Which projects are behind schedule?",
            "Show sprint velocity trend for Platform team",
            "List critical/blocker issues across all projects",
        ],
    },
    "inhouse": {
        "sql": "SELECT metric_name, current_value, target_value\nFROM `erp_poc.app_kpi_dashboard`\nORDER BY metric_name",
        "explanation": "Internal KPI dashboard: **Uptime** is at 99.95% (target: 99.99%). **DAU** is 45K and trending up toward the 50K goal. **Error Rate** has improved to 0.3% from the 0.1% target. **NPS Score** is 72, showing good user satisfaction.",
        "chart_type": "bar",
        "x_col": "metric_name",
        "y_col": "current_value",
        "followups": [
            "Show API latency trend over last 30 days",
            "Which endpoints have the highest error rate?",
            "Compare current metrics vs last quarter",
        ],
    },
}


# ── Public API ────────────────────────────────────────────────────────────────

SYSTEM_PROMPT_TEMPLATE = """You are an expert BI analyst and SQL engineer working with enterprise data.
The data comes from {source_name} and is stored in BigQuery.

Schema:
{schema}

Your task:
1. Understand the user's business question
2. Generate a valid BigQuery SQL query
3. Choose best chart type: "bar", "line", or "pie"
4. Pick x and y columns for the chart
5. Write a clear 2-3 sentence business explanation
6. Suggest 3 relevant follow-up questions

Respond ONLY in this JSON format (no markdown, no extra text):
{{
  "sql": "<SQL>",
  "explanation": "<explanation>",
  "chart_type": "<bar|line|pie>",
  "x_col": "<column>",
  "y_col": "<column>",
  "followups": ["<q1>", "<q2>", "<q3>"]
}}
"""

SOURCE_NAMES = {
    "salesforce": "Salesforce CRM",
    "netsuite": "NetSuite ERP",
    "coupa": "Coupa Procurement",
    "workday": "Workday HCM",
    "jira": "JIRA Project Management",
    "inhouse": "In-House Systems",
}


def run_agent(question: str, model_id: str, source_id: str,
              is_local: bool = False) -> dict:
    """
    Run the agent pipeline.
    Args:
        question:  Natural language question
        model_id:  Vertex AI model ID
        source_id: Source system ID (may include "local_" prefix)
        is_local:  If True, return mock response instead of calling Vertex AI
    Returns:
        dict with: sql, explanation, chart_type, x_col, y_col, followups
    """
    base_id = source_id.replace("local_", "")

    if is_local:
        return LOCAL_RESPONSES.get(base_id, LOCAL_RESPONSES["salesforce"]).copy()

    # ── Live: call Vertex AI ──
    return _call_vertex_ai(question, model_id, base_id)


def _call_vertex_ai(question: str, model_id: str, base_source_id: str) -> dict:
    try:
        import vertexai
        from vertexai.generative_models import GenerativeModel, GenerationConfig

        vertexai.init(project=GCP_PROJECT_ID, location=GCP_LOCATION)

        schema = get_schema_description(base_source_id)
        source_name = SOURCE_NAMES.get(base_source_id, base_source_id)
        system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
            source_name=source_name,
            schema=schema,
        )

        model = GenerativeModel(model_name=model_id, system_instruction=system_prompt)
        config = GenerationConfig(temperature=0.1, max_output_tokens=2048)
        response = model.generate_content(
            f"Business question: {question}",
            generation_config=config,
        )

        raw = response.text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        return json.loads(raw)

    except Exception as e:
        return {
            "sql": "-- Error generating SQL",
            "explanation": f"⚠️ Vertex AI error: {e}",
            "chart_type": "bar", "x_col": "", "y_col": "",
            "followups": [], "error": str(e),
        }
