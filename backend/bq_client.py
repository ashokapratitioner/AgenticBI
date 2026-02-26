"""
BigQuery Client — schemas for all 6 enterprise systems + query execution.
Local sources use mock data. Live sources query real BigQuery.
"""

import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
BQ_DATASET = os.getenv("BQ_DATASET", "erp_poc")


# ── Local mock data for demo sources ─────────────────────────────────────────

MOCK_DATA = {
    "salesforce": pd.DataFrame({
        "stage": ["Prospecting", "Qualification", "Proposal", "Negotiation", "Closed Won", "Closed Lost"],
        "deal_count": [45, 32, 28, 15, 22, 8],
        "total_value": [890000, 1250000, 2100000, 1800000, 3200000, 450000],
        "avg_days_in_stage": [12, 18, 25, 30, 5, 10],
    }),
    "netsuite": pd.DataFrame({
        "department": ["Marketing", "Operations", "HR", "IT", "Finance", "R&D"],
        "total_amount": [150000, 320000, 95000, 210000, 130000, 280000],
        "transaction_count": [45, 120, 30, 80, 55, 95],
        "budget_remaining": [50000, 30000, 25000, 90000, 70000, 120000],
    }),
    "coupa": pd.DataFrame({
        "supplier": ["Acme Corp", "Global Parts", "TechVend", "Office Pro", "CloudServ", "DataFlow"],
        "po_count": [28, 45, 15, 62, 20, 12],
        "total_spend": [245000, 520000, 180000, 95000, 310000, 150000],
        "avg_delivery_days": [5, 8, 3, 2, 1, 4],
    }),
    "workday": pd.DataFrame({
        "department": ["Engineering", "Sales", "Marketing", "Support", "HR", "Finance"],
        "headcount": [120, 85, 45, 60, 15, 25],
        "avg_tenure_years": [3.2, 2.8, 4.1, 2.5, 5.0, 4.5],
        "attrition_rate": [8.5, 12.0, 6.5, 15.0, 3.0, 5.0],
    }),
    "jira": pd.DataFrame({
        "project": ["Platform", "Mobile App", "Data Pipeline", "DevOps", "Frontend", "Security"],
        "open_issues": [45, 32, 18, 12, 55, 8],
        "completed_sprints": [12, 10, 8, 15, 11, 6],
        "avg_velocity": [28, 22, 35, 40, 25, 18],
    }),
    "inhouse": pd.DataFrame({
        "metric_name": ["API Latency", "Uptime %", "DAU", "Error Rate", "Throughput", "NPS Score"],
        "current_value": [125, 99.95, 45000, 0.3, 12000, 72],
        "target_value": [100, 99.99, 50000, 0.1, 15000, 80],
        "trend": ["↓ Improving", "→ Stable", "↑ Growing", "↓ Improving", "↑ Growing", "↑ Growing"],
    }),
}


def get_mock_df(base_source_id: str) -> pd.DataFrame:
    """Return mock data for local demo sources."""
    return MOCK_DATA.get(base_source_id, MOCK_DATA["salesforce"])


# ── BigQuery Schemas per source system ────────────────────────────────────────

def get_schema_description(base_source_id: str) -> str:
    """Return BigQuery schema context for the LLM prompt."""
    p = GCP_PROJECT_ID or "YOUR_PROJECT"
    d = BQ_DATASET

    schemas = {
        "salesforce": f"""
BigQuery Tables (source: Salesforce CRM → BigQuery via connector)

Table: `{p}.{d}.sf_opportunities`
  - opportunity_id STRING, name STRING, stage STRING
  - amount FLOAT64, close_date DATE, probability FLOAT64
  - account_id STRING, account_name STRING, owner STRING, region STRING

Table: `{p}.{d}.sf_leads`
  - lead_id STRING, name STRING, company STRING, status STRING
  - source STRING, created_date DATE, converted_date DATE

Table: `{p}.{d}.sf_cases`
  - case_id STRING, subject STRING, status STRING, priority STRING
  - account_id STRING, created_date DATE, closed_date DATE, resolution_time_hours FLOAT64
""",
        "netsuite": f"""
BigQuery Tables (source: NetSuite ERP → BigQuery via connector)

Table: `{p}.{d}.ns_gl_transactions`
  - transaction_id STRING, transaction_date DATE, period STRING
  - account_code STRING, account_name STRING, department STRING
  - amount FLOAT64, currency STRING, memo STRING

Table: `{p}.{d}.ns_accounts_payable`
  - invoice_id STRING, vendor STRING, invoice_date DATE, due_date DATE
  - amount FLOAT64, status STRING, payment_date DATE

Table: `{p}.{d}.ns_accounts_receivable`
  - invoice_id STRING, customer STRING, invoice_date DATE, due_date DATE
  - amount FLOAT64, status STRING, days_outstanding INT64
""",
        "coupa": f"""
BigQuery Tables (source: Coupa Procurement → BigQuery via connector)

Table: `{p}.{d}.coupa_purchase_orders`
  - po_id STRING, po_date DATE, supplier STRING, supplier_id STRING
  - category STRING, total_amount FLOAT64, status STRING
  - requester STRING, department STRING, delivery_date DATE

Table: `{p}.{d}.coupa_invoices`
  - invoice_id STRING, po_id STRING, supplier STRING
  - invoice_date DATE, amount FLOAT64, status STRING, payment_terms STRING
""",
        "workday": f"""
BigQuery Tables (source: Workday HCM → BigQuery via connector)

Table: `{p}.{d}.wd_employees`
  - employee_id STRING, name STRING, department STRING, title STRING
  - hire_date DATE, location STRING, manager STRING
  - salary FLOAT64, employment_type STRING, status STRING

Table: `{p}.{d}.wd_time_off`
  - request_id STRING, employee_id STRING, type STRING
  - start_date DATE, end_date DATE, days FLOAT64, status STRING

Table: `{p}.{d}.wd_payroll`
  - payroll_id STRING, employee_id STRING, period DATE
  - gross_pay FLOAT64, deductions FLOAT64, net_pay FLOAT64, department STRING
""",
        "jira": f"""
BigQuery Tables (source: JIRA → BigQuery via connector)

Table: `{p}.{d}.jira_issues`
  - issue_key STRING, summary STRING, issue_type STRING, status STRING
  - priority STRING, project STRING, assignee STRING, reporter STRING
  - created DATE, resolved DATE, story_points FLOAT64, sprint STRING

Table: `{p}.{d}.jira_sprints`
  - sprint_id STRING, name STRING, project STRING
  - start_date DATE, end_date DATE
  - committed_points FLOAT64, completed_points FLOAT64, velocity FLOAT64
""",
        "inhouse": f"""
BigQuery Tables (source: In-House Systems → BigQuery ETL)

Table: `{p}.{d}.app_product_metrics`
  - date DATE, metric_name STRING, metric_value FLOAT64
  - product STRING, environment STRING

Table: `{p}.{d}.app_api_logs`
  - timestamp TIMESTAMP, endpoint STRING, method STRING
  - response_code INT64, latency_ms FLOAT64, user_id STRING

Table: `{p}.{d}.app_kpi_dashboard`
  - date DATE, kpi_name STRING, current_value FLOAT64
  - target_value FLOAT64, department STRING, status STRING
""",
    }
    return schemas.get(base_source_id, schemas["salesforce"])


# ── Query execution ──────────────────────────────────────────────────────────

def execute_sql(sql: str, source_id: str = "salesforce", question: str = "",
                is_local: bool = False) -> tuple:
    """
    Execute SQL on BigQuery or return mock data for local sources.
    Args:
        sql:        Generated SQL query
        source_id:  Full source ID (may include "local_" prefix)
        question:   Original user question (for mock data matching)
        is_local:   If True, return mock data instead of querying BigQuery
    Returns:
        (DataFrame, error_message) — error is None on success.
    """
    base_id = source_id.replace("local_", "")

    if is_local:
        df = get_mock_df(base_id)
        return df, None

    # ── Live BigQuery execution ──
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=GCP_PROJECT_ID)
        query_job = client.query(sql)
        result = query_job.result()
        df = result.to_dataframe()
        return df, None
    except ImportError:
        return pd.DataFrame(), "google-cloud-bigquery not installed. Run: pip install google-cloud-bigquery"
    except Exception as e:
        return pd.DataFrame(), str(e)
