"""
Agentic BI Assistant — Enterprise Edition
Multi-system data sources | Multi-model AI | Email Agent | Report Generation
"""

import os
import sys
import tempfile
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

from backend.models_config import (
    AVAILABLE_MODELS, DEFAULT_MODEL_ID, DATA_SOURCES,
    get_model_id_by_label, get_model_by_id, get_source_by_label,
    SOURCE_LABELS, is_local_source, get_bq_source_id,
)
from backend.agent import run_agent
from backend.bq_client import execute_sql
from backend.email_agent import send_report_email
from backend.report_generator import generate_html_report

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Agentic BI Assistant",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    .stApp { background: linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%); }

    [data-testid="stSidebar"] {
        background: rgba(255,255,255,0.04);
        border-right: 1px solid rgba(255,255,255,0.08);
    }
    [data-testid="stSidebar"] * { color: #e0e0e0 !important; }

    .header-bar {
        background: linear-gradient(90deg, rgba(99,102,241,0.3), rgba(139,92,246,0.3));
        border: 1px solid rgba(139,92,246,0.4);
        border-radius: 14px; padding: 18px 28px; margin-bottom: 20px;
        display: flex; align-items: center; gap: 14px;
    }
    .header-title { font-size: 1.6rem; font-weight: 700; color: #fff; margin: 0; }
    .header-sub   { font-size: 0.85rem; color: #a5b4fc; margin: 0; }

    .panel-card {
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 14px; padding: 20px; min-height: 520px;
    }
    .panel-title {
        font-size: 0.85rem; font-weight: 600; color: #a5b4fc;
        text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 14px;
    }

    .msg-user {
        background: rgba(99,102,241,0.25); border: 1px solid rgba(99,102,241,0.4);
        border-radius: 12px 12px 2px 12px; padding: 12px 16px; margin: 8px 0;
        color: #e0e0e0; font-size: 0.92rem;
    }
    .msg-ai {
        background: rgba(139,92,246,0.15); border: 1px solid rgba(139,92,246,0.3);
        border-radius: 12px 12px 12px 2px; padding: 12px 16px; margin: 8px 0;
        color: #e0e0e0; font-size: 0.92rem; line-height: 1.6;
    }
    .msg-label {
        font-size: 0.72rem; font-weight: 600; color: #a5b4fc; margin-bottom: 4px;
        text-transform: uppercase; letter-spacing: 0.06em;
    }

    .kpi-card {
        flex: 1; background: rgba(99,102,241,0.15); border: 1px solid rgba(99,102,241,0.3);
        border-radius: 10px; padding: 12px 14px; text-align: center;
    }
    .kpi-value { font-size: 1.3rem; font-weight: 700; color: #a5b4fc; }
    .kpi-label { font-size: 0.72rem; color: #9ca3af; margin-top: 2px; }

    .source-card {
        background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.1);
        border-radius: 10px; padding: 10px 14px; margin-bottom: 6px;
    }
    .source-card.active {
        background: rgba(99,102,241,0.2); border-color: rgba(99,102,241,0.5);
    }

    .demo-badge {
        background: linear-gradient(90deg, #f59e0b, #ef4444);
        color: white; font-size: 0.72rem; font-weight: 700;
        padding: 3px 10px; border-radius: 20px; letter-spacing: 0.08em;
    }
    .model-badge {
        background: rgba(139,92,246,0.25); border: 1px solid rgba(139,92,246,0.4);
        color: #c4b5fd; font-size: 0.72rem; font-weight: 600;
        padding: 3px 10px; border-radius: 20px;
    }
    .source-badge {
        background: rgba(99,102,241,0.2); border: 1px solid rgba(99,102,241,0.4);
        color: #818cf8; font-size: 0.72rem; font-weight: 600;
        padding: 3px 10px; border-radius: 20px;
    }
    .email-success {
        background: rgba(34,197,94,0.15); border: 1px solid rgba(34,197,94,0.3);
        color: #86efac; padding: 10px 14px; border-radius: 10px;
        font-size: 0.85rem; margin: 8px 0;
    }
    .email-error {
        background: rgba(239,68,68,0.15); border: 1px solid rgba(239,68,68,0.3);
        color: #fca5a5; padding: 10px 14px; border-radius: 10px;
        font-size: 0.85rem; margin: 8px 0;
    }

    .stButton > button {
        background: linear-gradient(135deg, #6366f1, #8b5cf6);
        color: white; border: none; border-radius: 10px; font-weight: 600;
        transition: all 0.2s ease;
    }
    .stButton > button:hover {
        transform: translateY(-1px); box-shadow: 0 4px 20px rgba(99,102,241,0.5);
    }
    .stTextInput > div > div > input {
        background: #111111 !important;
        border: 1px solid rgba(255,255,255,0.25) !important;
        border-radius: 10px !important; color: #ffffff !important;
    }
    .stTextInput > div > div > input::placeholder {
        color: #9ca3af !important;
    }
    .stSelectbox > div > div {
        background: rgba(255,255,255,0.06) !important;
        border: 1px solid rgba(255,255,255,0.15) !important;
        border-radius: 10px !important;
    }
    .chat-scroll { max-height: 380px; overflow-y: auto; padding-right: 4px; }
    #MainMenu, footer { visibility: hidden; }
    header[data-testid="stHeader"] {
        background: #000000 !important;
        color: #ffffff !important;
    }
    header[data-testid="stHeader"] * {
        color: #ffffff !important;
    }
</style>
""", unsafe_allow_html=True)


# ── Session state ─────────────────────────────────────────────────────────────
for key, default in {
    "messages": [],
    "last_df": None,
    "last_result": None,
    "selected_followup": None,
    "pending_followup": None,
    "email_status": None,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default


# ── Helpers ───────────────────────────────────────────────────────────────────

def build_chart(df, chart_type, x_col, y_col):
    if df is None or df.empty:
        return None
    cols = df.columns.tolist()
    if x_col not in cols:
        x_col = cols[0]
    if y_col not in cols or y_col == x_col:
        nc = df.select_dtypes("number").columns.tolist()
        y_col = nc[0] if nc else cols[-1]

    COLORS = ["#6366f1", "#8b5cf6", "#a78bfa", "#c4b5fd", "#818cf8", "#e879f9"]
    layout = dict(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color="#e0e0e0", size=12),
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
    return fig


def compute_kpis(df):
    kpis = [{"label": "Records", "value": f"{len(df):,}"}]
    for col in df.select_dtypes("number").columns[:2]:
        kpis.append({"label": col.replace("_", " ").title(), "value": f"{df[col].sum():,.0f}"})
    return kpis[:3]


def run_query(question, model_id, source):
    local = is_local_source(source)
    src_id = source["id"]
    mode_label = "LOCAL" if local else get_model_by_id(model_id)["label"]
    with st.spinner(f"🤖 Thinking with **{mode_label}**..."):
        result = run_agent(question, model_id, src_id, is_local=local)
        df, err = execute_sql(result.get("sql", ""), src_id, question, is_local=local)

    st.session_state.messages.append({"role": "user", "content": question})
    st.session_state.messages.append({
        "role": "ai", "content": result.get("explanation", ""),
        "model": "Local Demo" if local else get_model_by_id(model_id)["label"],
    })
    st.session_state.last_result = result
    st.session_state.last_df = df if err is None else None
    if err:
        st.session_state.messages[-1]["content"] += f"\n\n⚠️ Query error: {err}"


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🤖 Agentic BI")
    st.markdown("---")

    # ── Data Source Selector ──
    st.markdown("### 📡 Data Source")
    selected_source_label = st.selectbox(
        "Source System", options=SOURCE_LABELS, index=0, label_visibility="collapsed",
    )
    source = get_source_by_label(selected_source_label)
    st.markdown(f"""
    <div class="source-card active">
        <div style="font-size:1.2rem;">{source['icon']}</div>
        <div style="font-size:0.78rem;color:#9ca3af;margin-top:2px;">{source['description']}</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    # ── Model Selector ──
    st.markdown("### 🧠 AI Model")
    model_labels = [m["label"] for m in AVAILABLE_MODELS]
    selected_model_label = st.selectbox("Model", model_labels, index=0, label_visibility="collapsed")
    selected_model_id = get_model_id_by_label(selected_model_label)
    selected_model = get_model_by_id(selected_model_id)
    st.markdown(f"""
    <div style='background:rgba(139,92,246,0.1);border:1px solid rgba(139,92,246,0.25);
    border-radius:10px;padding:10px 14px;margin-bottom:10px;'>
        <div style='font-size:0.8rem;color:#a5b4fc;font-weight:600;'>Best for</div>
        <div style='font-size:0.82rem;color:#c7d2fe;margin-top:2px;'>{selected_model['description']}</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    # ── Settings ──
    st.markdown("### ⚙️ Settings")
    show_sql = st.toggle("Show generated SQL", value=True)
    show_data = st.toggle("Show raw data table", value=False)

    st.markdown("---")

    # ── Email Agent ──
    st.markdown("### 📧 Email Agent")
    email_to = st.text_input("Send report to:", placeholder="email@example.com", label_visibility="collapsed")
    if st.button("📤 Email Current Report", use_container_width=True):
        if st.session_state.last_result and email_to.strip():
            r = st.session_state.last_result
            last_q = ""
            for m in reversed(st.session_state.messages):
                if m["role"] == "user":
                    last_q = m["content"]
                    break
            success, msg = send_report_email(
                to_emails=[e.strip() for e in email_to.split(",")],
                subject=f"BI Report: {source['label']} — {last_q[:50]}",
                question=last_q,
                explanation=r.get("explanation", ""),
                sql=r.get("sql", ""),
                source_name=f"{source['icon']} {source['label']}",
            )
            st.session_state.email_status = (success, msg)
            st.rerun()
        elif not email_to.strip():
            st.session_state.email_status = (False, "⚠️ Please enter an email address")
            st.rerun()

    if st.session_state.email_status:
        ok, msg = st.session_state.email_status
        cls = "email-success" if ok else "email-error"
        st.markdown(f'<div class="{cls}">{msg}</div>', unsafe_allow_html=True)
        st.session_state.email_status = None

    st.markdown("---")

    # ── Clear ──
    if st.button("🗑️ Clear Conversation", use_container_width=True):
        st.session_state.messages = []
        st.session_state.last_df = None
        st.session_state.last_result = None
        st.rerun()

    # ── Data source legend ──
    st.markdown("---")
    st.markdown("### 🔗 Connected Systems")
    live_sources = [s for s in DATA_SOURCES if not s.get("local")]
    local_sources = [s for s in DATA_SOURCES if s.get("local")]
    for s in live_sources:
        st.markdown(f"<div style='font-size:0.78rem;color:#86efac;margin:2px 0;'>🟢 {s['icon']} {s['label']}</div>", unsafe_allow_html=True)
    if local_sources:
        st.markdown("<div style='font-size:0.7rem;color:#4b5563;margin:6px 0 2px;'>LOCAL DEMO:</div>", unsafe_allow_html=True)
        for s in local_sources:
            st.markdown(f"<div style='font-size:0.78rem;color:#6b7280;margin:2px 0;'>{s['icon']} {s['label']}</div>", unsafe_allow_html=True)

    st.markdown("""
    <div style='text-align:center;margin-top:16px;font-size:0.72rem;color:#4b5563;'>
        Powered by Vertex AI + BigQuery
    </div>""", unsafe_allow_html=True)


# ── Header ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="header-bar">
    <div style="font-size:2.2rem;">🤖</div>
    <div>
        <div class="header-title">Agentic BI Assistant</div>
        <div class="header-sub">
            Ask any business question &nbsp;·&nbsp;
            <span class="source-badge">{source['icon']} {source['label']}</span>
            &nbsp; <span class="model-badge">{selected_model['label']}</span>
            {'&nbsp;<span class="demo-badge">LOCAL</span>' if is_local_source(source) else ''}
</div>
""", unsafe_allow_html=True)


# ── Main layout ──────────────────────────────────────────────────────────────
col_chat, col_viz = st.columns([1, 1.1], gap="large")

# ── LEFT: Chat ────────────────────────────────────────────────────────────────
with col_chat:
    st.markdown('<div class="panel-title">💬 Conversation</div>', unsafe_allow_html=True)

    chat_html = '<div class="chat-scroll">'
    if not st.session_state.messages:
        chat_html += f"""
        <div style='text-align:center;padding:40px 20px;color:#4b5563;'>
            <div style='font-size:2.5rem;margin-bottom:10px;'>{source['icon']}</div>
            <div style='font-size:0.9rem;color:#6b7280;'>Ask a question about <b>{source['label']}</b> data</div>
            <div style='font-size:0.78rem;margin-top:6px;color:#374151;'>
                Powered by BigQuery + Vertex AI
            </div>
        </div>"""
    else:
        for msg in st.session_state.messages:
            if msg["role"] == "user":
                chat_html += f"""
                <div class='msg-label'>You</div>
                <div class='msg-user'>{msg['content']}</div>"""
            else:
                badge = f"<span class='model-badge'>{msg.get('model','AI')}</span>"
                chat_html += f"""
                <div class='msg-label'>AI Agent &nbsp;{badge}</div>
                <div class='msg-ai'>{msg['content']}</div>"""
    chat_html += "</div>"
    st.markdown(chat_html, unsafe_allow_html=True)

    st.markdown("---")

    # Auto-execute pending follow-up
    if st.session_state.pending_followup:
        fu_q = st.session_state.pending_followup
        st.session_state.pending_followup = None
        run_query(fu_q, selected_model_id, source)
        st.rerun()

    with st.form(key="query_form", clear_on_submit=True):
        user_input = st.text_input(
            "Your question", value="",
            placeholder=f'e.g. ask about {source["label"]} data...',
            label_visibility="collapsed",
        )
        submitted = st.form_submit_button("▶ Send", use_container_width=True)

    if submitted and user_input.strip():
        run_query(user_input.strip(), selected_model_id, source)
        st.rerun()

    if show_sql and st.session_state.last_result:
        sql_text = st.session_state.last_result.get("sql", "")
        if sql_text:
            with st.expander("🔍 Generated SQL", expanded=False):
                st.code(sql_text, language="sql")




# ── RIGHT: Visualization ─────────────────────────────────────────────────────
with col_viz:
    st.markdown('<div class="panel-title">📊 Visualization & Reports</div>', unsafe_allow_html=True)

    result = st.session_state.last_result
    df = st.session_state.last_df

    if result is None:
        st.markdown(f"""
        <div style='text-align:center;padding:60px 20px;color:#4b5563;'>
            <div style='font-size:3rem;margin-bottom:14px;'>📊</div>
            <div style='font-size:0.9rem;color:#6b7280;'>Charts & reports will appear here</div>
            <div style='font-size:0.78rem;margin-top:6px;color:#374151;'>
                The agent auto-selects the best chart type for your data
            </div>
        </div>""", unsafe_allow_html=True)
    else:
        # KPIs
        if df is not None and not df.empty:
            kpis = compute_kpis(df)
            kpi_cols = st.columns(len(kpis))
            for i, kpi in enumerate(kpis):
                with kpi_cols[i]:
                    st.markdown(f"""
                    <div class="kpi-card">
                        <div class="kpi-value">{kpi['value']}</div>
                        <div class="kpi-label">{kpi['label']}</div>
                    </div>""", unsafe_allow_html=True)

        # Chart
        ct = result.get("chart_type", "bar")
        xc = result.get("x_col", "")
        yc = result.get("y_col", "")
        if df is not None and not df.empty:
            fig = build_chart(df, ct, xc, yc)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data returned.")

        # Raw data
        if show_data and df is not None and not df.empty:
            with st.expander("📋 Raw Data", expanded=False):
                st.dataframe(df, use_container_width=True)

        # ── Download Report ──
        if df is not None and not df.empty:
            st.markdown("---")
            last_q = ""
            for m in reversed(st.session_state.messages):
                if m["role"] == "user":
                    last_q = m["content"]
                    break
            report_html = generate_html_report(
                question=last_q,
                explanation=result.get("explanation", ""),
                sql=result.get("sql", ""),
                df=df,
                source_name=f"{source['icon']} {source['label']}",
                chart_type=ct,
                model_name=selected_model["label"],
                x_col=xc,
                y_col=yc,
            )
            dl_col1, dl_col2 = st.columns(2)
            with dl_col1:
                st.download_button(
                    "📥 Download Report (HTML)",
                    data=report_html,
                    file_name=f"bi_report_{source['id']}.html",
                    mime="text/html",
                    use_container_width=True,
                )
            with dl_col2:
                csv_data = df.to_csv(index=False)
                st.download_button(
                    "📥 Download Data (CSV)",
                    data=csv_data,
                    file_name=f"bi_data_{source['id']}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

        # Follow-ups
        followups = result.get("followups", [])
        if followups:
            st.markdown("---")
            st.markdown('<div class="panel-title">💡 Suggested Follow-ups</div>', unsafe_allow_html=True)
            for fu in followups:
                if st.button(f"→ {fu}", key=f"fu_{fu[:30]}", use_container_width=True):
                    st.session_state.pending_followup = fu
                    st.rerun()



