"""
Multi-Model Configuration + Enterprise Data Source Definitions
Supports both Local (demo) and Live BigQuery data sources.
"""

# ── Vertex AI Models ──────────────────────────────────────────────────────────

AVAILABLE_MODELS = [
    {
        "id": "gemini-2.0-flash-001",
        "label": "Gemini 2.0 Flash ⚡",
        "description": "Fastest model — ideal for demos and quick Q&A",
        "icon": "⚡",
    },
    {
        "id": "gemini-1.5-pro-002",
        "label": "Gemini 1.5 Pro 🧠",
        "description": "Most capable — best for complex multi-step reasoning",
        "icon": "🧠",
    },
    {
        "id": "gemini-1.5-flash-002",
        "label": "Gemini 1.5 Flash ⚖️",
        "description": "Balanced speed and quality",
        "icon": "⚖️",
    },
    {
        "id": "gemini-1.0-pro-002",
        "label": "Gemini 1.0 Pro 🔧",
        "description": "Stable production model",
        "icon": "🔧",
    },
]

DEFAULT_MODEL_ID = "gemini-2.0-flash-001"
MODEL_IDS = [m["id"] for m in AVAILABLE_MODELS]
MODEL_LABELS = [m["label"] for m in AVAILABLE_MODELS]


def get_model_by_id(model_id: str) -> dict:
    for m in AVAILABLE_MODELS:
        if m["id"] == model_id:
            return m
    return AVAILABLE_MODELS[0]


def get_model_id_by_label(label: str) -> str:
    for m in AVAILABLE_MODELS:
        if m["label"] == label:
            return m["id"]
    return DEFAULT_MODEL_ID


# ── Enterprise Data Sources ──────────────────────────────────────────────────
# "local" = True  → uses built-in mock data (no GCP needed)
# "local" = False → queries real BigQuery tables

DATA_SOURCES = [
    # ── Live BigQuery Sources ──
    {
        "id": "salesforce",
        "label": "Salesforce",
        "icon": "☁️",
        "description": "CRM — Leads, Opportunities, Accounts, Cases",
        "color": "#00A1E0",
        "local": False,
    },
    {
        "id": "netsuite",
        "label": "NetSuite",
        "icon": "📊",
        "description": "ERP — Finance, GL, Accounts Payable/Receivable",
        "color": "#1B3A5C",
        "local": False,
    },
    {
        "id": "coupa",
        "label": "Coupa",
        "icon": "🛒",
        "description": "Procurement — Purchase Orders, Invoices, Suppliers",
        "color": "#E74C3C",
        "local": False,
    },
    {
        "id": "workday",
        "label": "Workday",
        "icon": "👥",
        "description": "HCM — Employees, Payroll, Time Off, Headcount",
        "color": "#F5A623",
        "local": False,
    },
    {
        "id": "jira",
        "label": "JIRA",
        "icon": "🎯",
        "description": "Project Management — Issues, Sprints, Backlogs",
        "color": "#0052CC",
        "local": False,
    },
    {
        "id": "inhouse",
        "label": "In-House Systems",
        "icon": "🏢",
        "description": "Custom — Product Metrics, IoT, Internal KPIs",
        "color": "#8B5CF6",
        "local": False,
    },
    # ── Local Demo Sources (offline mock data) ──
    {
        "id": "local_salesforce",
        "label": "Local · Salesforce",
        "icon": "💾",
        "description": "Demo CRM data — no GCP needed",
        "color": "#6b7280",
        "local": True,
    },
    {
        "id": "local_netsuite",
        "label": "Local · NetSuite",
        "icon": "💾",
        "description": "Demo ERP data — no GCP needed",
        "color": "#6b7280",
        "local": True,
    },
    {
        "id": "local_coupa",
        "label": "Local · Coupa",
        "icon": "💾",
        "description": "Demo Procurement data — no GCP needed",
        "color": "#6b7280",
        "local": True,
    },
    {
        "id": "local_workday",
        "label": "Local · Workday",
        "icon": "💾",
        "description": "Demo HCM data — no GCP needed",
        "color": "#6b7280",
        "local": True,
    },
    {
        "id": "local_jira",
        "label": "Local · JIRA",
        "icon": "💾",
        "description": "Demo Project data — no GCP needed",
        "color": "#6b7280",
        "local": True,
    },
    {
        "id": "local_inhouse",
        "label": "Local · In-House",
        "icon": "💾",
        "description": "Demo Internal KPI data — no GCP needed",
        "color": "#6b7280",
        "local": True,
    },
]

SOURCE_IDS = [s["id"] for s in DATA_SOURCES]
SOURCE_LABELS = [f'{s["icon"]} {s["label"]}' for s in DATA_SOURCES]


def get_source_by_label(label: str) -> dict:
    for s in DATA_SOURCES:
        full = f'{s["icon"]} {s["label"]}'
        if full == label or s["label"] == label:
            return s
    return DATA_SOURCES[0]


def is_local_source(source: dict) -> bool:
    """Check if a source is local/demo (uses mock data)."""
    return source.get("local", False)


def get_bq_source_id(source_id: str) -> str:
    """Strip 'local_' prefix to get the base source ID for schema/mock lookup."""
    return source_id.replace("local_", "")
