"""
Create sample ERP-style datasets in BigQuery for all 6 enterprise systems.
Tables match the schemas defined in backend/bq_client.py.

Usage:
    python data/create_sample_data.py
"""

import os
import random
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
BQ_DATASET = os.getenv("BQ_DATASET", "erp_poc")

if not GCP_PROJECT_ID:
    print("❌ GCP_PROJECT_ID not set in .env file. Aborting.")
    exit(1)


def rdate(start_year=2024):
    s = date(start_year, 1, 1)
    return s + timedelta(days=random.randint(0, 364))


def rstr(lst):
    return random.choice(lst)


def main():
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)
    ds_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}"
    ds = bigquery.Dataset(ds_id)
    ds.location = "US"
    client.create_dataset(ds, exists_ok=True)
    print(f"\n🚀 Dataset `{BQ_DATASET}` ready\n")

    def create_table(name, schema, rows):
        ref = f"{ds_id}.{name}"
        table = bigquery.Table(ref, schema=schema)
        client.delete_table(ref, not_found_ok=True)
        client.create_table(table)
        errs = client.insert_rows_json(ref, rows)
        status = "✔" if not errs else f"⚠ {errs[:1]}"
        print(f"  {status} {name}: {len(rows)} rows")

    S = bigquery.SchemaField

    # ── 1. Salesforce: sf_opportunities ──────────────────────────────────────
    stages = ["Prospecting", "Qualification", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]
    regions = ["North", "South", "East", "West", "Central"]
    owners = ["Alice Kumar", "Bob Singh", "Carol Patel", "David Nair", "Eve Sharma"]
    create_table("sf_opportunities", [
        S("opportunity_id", "STRING"), S("name", "STRING"), S("stage", "STRING"),
        S("amount", "FLOAT64"), S("close_date", "DATE"), S("probability", "FLOAT64"),
        S("account_id", "STRING"), S("account_name", "STRING"),
        S("owner", "STRING"), S("region", "STRING"),
    ], [{
        "opportunity_id": f"OPP-{i}", "name": f"Deal {i}",
        "stage": rstr(stages), "amount": round(random.uniform(5000, 500000), 2),
        "close_date": str(rdate()), "probability": round(random.uniform(0.1, 1.0), 2),
        "account_id": f"ACC-{random.randint(100,999)}", "account_name": f"Company {random.randint(1,50)}",
        "owner": rstr(owners), "region": rstr(regions),
    } for i in range(200)])

    # ── 2. Salesforce: sf_leads ──────────────────────────────────────────────
    lead_statuses = ["New", "Contacted", "Qualified", "Converted", "Lost"]
    lead_sources = ["Web", "Referral", "Campaign", "Partner", "Cold Call"]
    create_table("sf_leads", [
        S("lead_id", "STRING"), S("name", "STRING"), S("company", "STRING"),
        S("status", "STRING"), S("source", "STRING"),
        S("created_date", "DATE"), S("converted_date", "DATE"),
    ], [{
        "lead_id": f"LEAD-{i}", "name": f"Lead {i}", "company": f"Corp {random.randint(1,80)}",
        "status": rstr(lead_statuses), "source": rstr(lead_sources),
        "created_date": str(rdate()), "converted_date": str(rdate()) if random.random() > 0.4 else None,
    } for i in range(150)])

    # ── 3. Salesforce: sf_cases ──────────────────────────────────────────────
    case_statuses = ["New", "In Progress", "Escalated", "Resolved", "Closed"]
    priorities = ["Low", "Medium", "High", "Critical"]
    create_table("sf_cases", [
        S("case_id", "STRING"), S("subject", "STRING"), S("status", "STRING"),
        S("priority", "STRING"), S("account_id", "STRING"),
        S("created_date", "DATE"), S("closed_date", "DATE"),
        S("resolution_time_hours", "FLOAT64"),
    ], [{
        "case_id": f"CASE-{i}", "subject": f"Issue {rstr(['Login','Billing','Bug','Feature','Access'])} #{i}",
        "status": rstr(case_statuses), "priority": rstr(priorities),
        "account_id": f"ACC-{random.randint(100,999)}", "created_date": str(rdate()),
        "closed_date": str(rdate()) if random.random() > 0.3 else None,
        "resolution_time_hours": round(random.uniform(0.5, 120), 1),
    } for i in range(100)])

    # ── 4. NetSuite: ns_gl_transactions ──────────────────────────────────────
    departments = ["Marketing", "Operations", "HR", "IT", "Finance", "R&D", "Logistics"]
    accounts = [("1001", "Revenue"), ("2001", "COGS"), ("3001", "Salaries"),
                ("4001", "Marketing Spend"), ("5001", "IT Infra"), ("6001", "Travel"), ("7001", "Depreciation")]
    create_table("ns_gl_transactions", [
        S("transaction_id", "STRING"), S("transaction_date", "DATE"), S("period", "STRING"),
        S("account_code", "STRING"), S("account_name", "STRING"), S("department", "STRING"),
        S("amount", "FLOAT64"), S("currency", "STRING"), S("memo", "STRING"),
    ], [{
        "transaction_id": f"TXN-{i}", "transaction_date": str(rdate()),
        "period": rstr(["2024-Q1","2024-Q2","2024-Q3","2024-Q4"]),
        "account_code": (a:=rstr(accounts))[0], "account_name": a[1],
        "department": rstr(departments), "amount": round(random.uniform(-50000, 100000), 2),
        "currency": "USD", "memo": f"{a[1]} entry",
    } for i in range(250)])

    # ── 5. NetSuite: ns_accounts_payable ─────────────────────────────────────
    vendors = ["Acme Corp", "Global Parts", "TechVend", "Office Pro", "CloudServ", "DataFlow"]
    ap_statuses = ["Open", "Paid", "Overdue", "Partially Paid"]
    create_table("ns_accounts_payable", [
        S("invoice_id", "STRING"), S("vendor", "STRING"),
        S("invoice_date", "DATE"), S("due_date", "DATE"),
        S("amount", "FLOAT64"), S("status", "STRING"), S("payment_date", "DATE"),
    ], [{
        "invoice_id": f"AP-{i}", "vendor": rstr(vendors),
        "invoice_date": str(d:=rdate()), "due_date": str(d + timedelta(days=30)),
        "amount": round(random.uniform(500, 80000), 2), "status": rstr(ap_statuses),
        "payment_date": str(d + timedelta(days=random.randint(10, 45))) if random.random() > 0.3 else None,
    } for i in range(120)])

    # ── 6. NetSuite: ns_accounts_receivable ──────────────────────────────────
    customers = [f"Customer {i}" for i in range(1, 40)]
    ar_statuses = ["Outstanding", "Paid", "Overdue"]
    create_table("ns_accounts_receivable", [
        S("invoice_id", "STRING"), S("customer", "STRING"),
        S("invoice_date", "DATE"), S("due_date", "DATE"),
        S("amount", "FLOAT64"), S("status", "STRING"), S("days_outstanding", "INT64"),
    ], [{
        "invoice_id": f"AR-{i}", "customer": rstr(customers),
        "invoice_date": str(rdate()), "due_date": str(rdate()),
        "amount": round(random.uniform(1000, 120000), 2), "status": rstr(ar_statuses),
        "days_outstanding": random.randint(0, 90),
    } for i in range(100)])

    # ── 7. Coupa: coupa_purchase_orders ──────────────────────────────────────
    categories = ["IT Hardware", "Software", "Office Supplies", "Services", "Marketing"]
    po_statuses = ["Approved", "Pending", "Received", "Cancelled"]
    create_table("coupa_purchase_orders", [
        S("po_id", "STRING"), S("po_date", "DATE"), S("supplier", "STRING"),
        S("supplier_id", "STRING"), S("category", "STRING"),
        S("total_amount", "FLOAT64"), S("status", "STRING"),
        S("requester", "STRING"), S("department", "STRING"), S("delivery_date", "DATE"),
    ], [{
        "po_id": f"PO-{i}", "po_date": str(d:=rdate()),
        "supplier": (v:=rstr(vendors)), "supplier_id": f"SUP-{hash(v) % 999}",
        "category": rstr(categories), "total_amount": round(random.uniform(200, 150000), 2),
        "status": rstr(po_statuses), "requester": rstr(owners),
        "department": rstr(departments), "delivery_date": str(d + timedelta(days=random.randint(3, 30))),
    } for i in range(180)])

    # ── 8. Coupa: coupa_invoices ─────────────────────────────────────────────
    inv_statuses = ["Pending", "Approved", "Paid", "Disputed"]
    terms = ["Net 15", "Net 30", "Net 45", "Net 60"]
    create_table("coupa_invoices", [
        S("invoice_id", "STRING"), S("po_id", "STRING"), S("supplier", "STRING"),
        S("invoice_date", "DATE"), S("amount", "FLOAT64"),
        S("status", "STRING"), S("payment_terms", "STRING"),
    ], [{
        "invoice_id": f"INV-{i}", "po_id": f"PO-{random.randint(0,179)}",
        "supplier": rstr(vendors), "invoice_date": str(rdate()),
        "amount": round(random.uniform(200, 100000), 2),
        "status": rstr(inv_statuses), "payment_terms": rstr(terms),
    } for i in range(150)])

    # ── 9. Workday: wd_employees ─────────────────────────────────────────────
    titles = ["Engineer", "Analyst", "Manager", "Director", "VP", "Specialist", "Associate"]
    locations = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Remote"]
    emp_types = ["Full-Time", "Contract", "Part-Time"]
    emp_statuses = ["Active", "On Leave", "Terminated"]
    create_table("wd_employees", [
        S("employee_id", "STRING"), S("name", "STRING"), S("department", "STRING"),
        S("title", "STRING"), S("hire_date", "DATE"), S("location", "STRING"),
        S("manager", "STRING"), S("salary", "FLOAT64"),
        S("employment_type", "STRING"), S("status", "STRING"),
    ], [{
        "employee_id": f"EMP-{i}", "name": f"Employee {i}",
        "department": rstr(departments), "title": rstr(titles),
        "hire_date": str(rdate(2020)), "location": rstr(locations),
        "manager": f"Manager {random.randint(1,20)}",
        "salary": round(random.uniform(40000, 250000), 2),
        "employment_type": rstr(emp_types), "status": rstr(emp_statuses),
    } for i in range(300)])

    # ── 10. Workday: wd_time_off ─────────────────────────────────────────────
    leave_types = ["Vacation", "Sick Leave", "Personal", "Parental", "Bereavement"]
    leave_statuses = ["Approved", "Pending", "Denied", "Cancelled"]
    create_table("wd_time_off", [
        S("request_id", "STRING"), S("employee_id", "STRING"), S("type", "STRING"),
        S("start_date", "DATE"), S("end_date", "DATE"),
        S("days", "FLOAT64"), S("status", "STRING"),
    ], [{
        "request_id": f"TO-{i}", "employee_id": f"EMP-{random.randint(0,299)}",
        "type": rstr(leave_types), "start_date": str(d:=rdate()),
        "end_date": str(d + timedelta(days=(dn:=random.randint(1,10)))),
        "days": float(dn), "status": rstr(leave_statuses),
    } for i in range(200)])

    # ── 11. Workday: wd_payroll ──────────────────────────────────────────────
    create_table("wd_payroll", [
        S("payroll_id", "STRING"), S("employee_id", "STRING"), S("period", "DATE"),
        S("gross_pay", "FLOAT64"), S("deductions", "FLOAT64"),
        S("net_pay", "FLOAT64"), S("department", "STRING"),
    ], [{
        "payroll_id": f"PAY-{i}", "employee_id": f"EMP-{random.randint(0,299)}",
        "period": str(date(2024, random.randint(1,12), 1)),
        "gross_pay": (g:=round(random.uniform(3000, 20000), 2)),
        "deductions": (d:=round(g * random.uniform(0.15, 0.35), 2)),
        "net_pay": round(g - d, 2), "department": rstr(departments),
    } for i in range(600)])

    # ── 12. JIRA: jira_issues ────────────────────────────────────────────────
    projects = ["Platform", "Mobile App", "Data Pipeline", "DevOps", "Frontend", "Security"]
    issue_types = ["Bug", "Story", "Task", "Epic", "Sub-task"]
    issue_statuses = ["To Do", "In Progress", "In Review", "Done", "Blocked"]
    assignees = ["Dev A", "Dev B", "Dev C", "Dev D", "Dev E", "Dev F"]
    sprints = [f"Sprint {i}" for i in range(1, 16)]
    create_table("jira_issues", [
        S("issue_key", "STRING"), S("summary", "STRING"), S("issue_type", "STRING"),
        S("status", "STRING"), S("priority", "STRING"), S("project", "STRING"),
        S("assignee", "STRING"), S("reporter", "STRING"),
        S("created", "DATE"), S("resolved", "DATE"),
        S("story_points", "FLOAT64"), S("sprint", "STRING"),
    ], [{
        "issue_key": f"{rstr(projects)[:3].upper()}-{i}",
        "summary": f"{rstr(['Fix','Implement','Update','Refactor','Test'])} {rstr(['login','API','UI','DB','auth'])} #{i}",
        "issue_type": rstr(issue_types), "status": rstr(issue_statuses),
        "priority": rstr(priorities), "project": rstr(projects),
        "assignee": rstr(assignees), "reporter": rstr(assignees),
        "created": str(rdate()), "resolved": str(rdate()) if random.random() > 0.4 else None,
        "story_points": float(rstr([1, 2, 3, 5, 8, 13])), "sprint": rstr(sprints),
    } for i in range(300)])

    # ── 13. JIRA: jira_sprints ───────────────────────────────────────────────
    create_table("jira_sprints", [
        S("sprint_id", "STRING"), S("name", "STRING"), S("project", "STRING"),
        S("start_date", "DATE"), S("end_date", "DATE"),
        S("committed_points", "FLOAT64"), S("completed_points", "FLOAT64"),
        S("velocity", "FLOAT64"),
    ], [{
        "sprint_id": f"SPR-{i}", "name": f"Sprint {i+1}", "project": rstr(projects),
        "start_date": str(d:=date(2024, max(1, (i*2)%12+1), 1)),
        "end_date": str(d + timedelta(days=13)),
        "committed_points": (cp:=float(random.randint(20, 50))),
        "completed_points": (done:=float(random.randint(10, int(cp)))),
        "velocity": done,
    } for i in range(60)])

    # ── 14. In-House: app_product_metrics ────────────────────────────────────
    metrics = ["API Latency", "Error Rate", "Throughput", "Page Load", "Cache Hit"]
    products = ["Web App", "Mobile App", "API Gateway", "Data Service"]
    envs = ["Production", "Staging"]
    create_table("app_product_metrics", [
        S("date", "DATE"), S("metric_name", "STRING"), S("metric_value", "FLOAT64"),
        S("product", "STRING"), S("environment", "STRING"),
    ], [{
        "date": str(rdate()), "metric_name": rstr(metrics),
        "metric_value": round(random.uniform(0.1, 500), 2),
        "product": rstr(products), "environment": rstr(envs),
    } for i in range(200)])

    # ── 15. In-House: app_api_logs ───────────────────────────────────────────
    endpoints = ["/api/users", "/api/orders", "/api/products", "/api/auth", "/api/reports"]
    methods = ["GET", "POST", "PUT", "DELETE"]
    create_table("app_api_logs", [
        S("timestamp", "TIMESTAMP"), S("endpoint", "STRING"), S("method", "STRING"),
        S("response_code", "INT64"), S("latency_ms", "FLOAT64"), S("user_id", "STRING"),
    ], [{
        "timestamp": f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z",
        "endpoint": rstr(endpoints), "method": rstr(methods),
        "response_code": rstr([200, 200, 200, 201, 400, 401, 404, 500]),
        "latency_ms": round(random.uniform(5, 800), 1),
        "user_id": f"USR-{random.randint(100,999)}",
    } for i in range(300)])

    # ── 16. In-House: app_kpi_dashboard ──────────────────────────────────────
    kpis = ["DAU", "Revenue", "NPS Score", "Uptime %", "Error Rate", "Churn Rate"]
    kpi_depts = ["Product", "Engineering", "Sales", "Support"]
    kpi_statuses = ["On Track", "At Risk", "Behind"]
    create_table("app_kpi_dashboard", [
        S("date", "DATE"), S("kpi_name", "STRING"), S("current_value", "FLOAT64"),
        S("target_value", "FLOAT64"), S("department", "STRING"), S("status", "STRING"),
    ], [{
        "date": str(rdate()), "kpi_name": rstr(kpis),
        "current_value": round(random.uniform(10, 100000), 2),
        "target_value": round(random.uniform(10, 100000), 2),
        "department": rstr(kpi_depts), "status": rstr(kpi_statuses),
    } for i in range(100)])

    print(f"\n✅ All 16 tables created in `{BQ_DATASET}`!")
    print(f"   You can now run: python -m streamlit run app.py\n")


if __name__ == "__main__":
    main()
