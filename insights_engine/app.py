"""
Bio Insights Engine - Streamlit Application

Multi-page app with:
  1. Ask — NL-to-SQL chat interface
  2. Insights — 5 signature insight charts
  3. Weekly Report — latest generated report
"""

from __future__ import annotations

import streamlit as st
import pandas as pd

from insights_engine.config import CHART_CONFIG
from insights_engine.core.athena_client import AthenaClient
from insights_engine.core.nl_to_sql import NLToSQLEngine

# ── Page config ──────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Bio Insights Engine",
    page_icon="🧬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Dark theme CSS ───────────────────────────────────────────────────────
_palette = CHART_CONFIG["color_palette"]
st.markdown(f"""
<style>
    .stApp {{
        background-color: {_palette['background']};
    }}
    .block-container {{
        padding-top: 2rem;
    }}
    .metric-label {{
        color: {_palette['text_muted']};
        font-size: 0.85rem;
    }}
    .metric-value {{
        color: {_palette['text']};
        font-size: 1.8rem;
        font-weight: 700;
    }}
    .sql-block {{
        background: {_palette['surface']};
        border-radius: 8px;
        padding: 1rem;
        font-family: monospace;
        font-size: 0.85rem;
        overflow-x: auto;
    }}
    div[data-testid="stChatMessage"] {{
        background: {_palette['surface']};
        border-radius: 12px;
        margin-bottom: 0.5rem;
    }}
</style>
""", unsafe_allow_html=True)


# ── Cached resources ─────────────────────────────────────────────────────
@st.cache_resource
def get_athena():
    return AthenaClient()


@st.cache_resource
def get_engine():
    athena = get_athena()
    return NLToSQLEngine(athena)


# ── Sidebar ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🧬 Bio Insights")
    page = st.radio(
        "Navigate",
        ["💬 Ask", "📊 Insights", "📋 Weekly Report"],
        label_visibility="collapsed",
    )
    st.divider()

    # Quick metrics
    try:
        athena = get_athena()
        metrics_df = athena.execute_query("""
            SELECT
                ROUND(AVG(readiness_score), 0) AS readiness,
                ROUND(AVG(sleep_score), 0) AS sleep,
                SUM(CASE WHEN had_workout THEN 1 ELSE 0 END) AS workouts
            FROM bio_gold.dashboard_30day
            WHERE COALESCE(
                    TRY(CAST(date AS date)),
                    TRY(date_parse(date, '%Y-%m-%d %H:%i:%s'))
                  ) >= CURRENT_DATE - INTERVAL '7' DAY
        """)
        if not metrics_df.empty:
            row = metrics_df.iloc[0]
            st.metric("7-Day Readiness", f"{row['readiness']:.0f}")
            st.metric("7-Day Sleep", f"{row['sleep']:.0f}")
            st.metric("Workouts This Week", f"{int(row['workouts'])}")
    except Exception:
        st.caption("Metrics unavailable")

    st.divider()
    st.caption("Data: Oura Ring + Peloton")
    st.caption("Powered by Claude + Athena")


# ══════════════════════════════════════════════════════════════════════════
# PAGE 1: ASK (NL-to-SQL Chat)
# ══════════════════════════════════════════════════════════════════════════
if page == "💬 Ask":
    st.header("Ask Your Data")
    st.caption("Type a health or fitness question in plain English.")

    # Session state for chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "nl_history" not in st.session_state:
        st.session_state.nl_history = []

    # Display chat history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("sql"):
                with st.expander("Show SQL"):
                    st.code(msg["sql"], language="sql")
            if msg.get("data") is not None and not msg["data"].empty:
                with st.expander(f"Show Data ({len(msg['data'])} rows)"):
                    st.dataframe(msg["data"], width="stretch")

    # Chat input
    if question := st.chat_input("e.g., What was my average readiness last week?"):
        # Show user message
        st.session_state.messages.append({"role": "user", "content": question})
        with st.chat_message("user"):
            st.markdown(question)

        # Generate answer
        with st.chat_message("assistant"):
            with st.spinner("Querying your data lake..."):
                try:
                    engine = get_engine()
                    result = engine.ask(question, history=st.session_state.nl_history)

                    if result.error:
                        st.error(result.error)
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": f"⚠️ {result.error}",
                        })
                    else:
                        # Display answer
                        st.markdown(result.answer)

                        # Collapsible SQL
                        with st.expander("Show SQL"):
                            st.code(result.sql, language="sql")

                        # Data table
                        if not result.data.empty:
                            with st.expander(f"Show Data ({result.row_count} rows)"):
                                st.dataframe(result.data, width="stretch")

                        # Metadata
                        cols = st.columns(3)
                        cols[0].caption(f"⏱️ {result.execution_time_ms}ms")
                        cols[1].caption(f"📊 {result.row_count} rows")
                        cols[2].caption(f"🎯 {result.confidence:.0%} confidence")

                        # Save to history
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": result.answer,
                            "sql": result.sql,
                            "data": result.data,
                        })
                        st.session_state.nl_history.append({
                            "question": question,
                            "result": {
                                "sql": result.sql,
                                "explanation": result.explanation,
                            },
                        })

                except Exception as e:
                    st.error(f"Something went wrong: {e}")
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": f"⚠️ Error: {e}",
                    })

    # Example questions
    if not st.session_state.messages:
        st.markdown("#### Try asking:")
        example_qs = [
            "What was my average readiness score last week?",
            "Which workout type gives me the best next-day readiness?",
            "Am I overtraining?",
            "What's the correlation between my sleep and readiness?",
            "Show me days where my readiness dropped below 70",
        ]
        for q in example_qs:
            st.markdown(f"- *{q}*")


# ══════════════════════════════════════════════════════════════════════════
# PAGE 2: INSIGHTS (5 Signature Charts)
# ══════════════════════════════════════════════════════════════════════════
elif page == "📊 Insights":
    st.header("Signature Insights")
    st.caption("Automated analysis of your biometric data.")

    athena = get_athena()

    # Run analyzers with caching
    @st.cache_data(ttl=3600, show_spinner="Analyzing...")
    def run_all_insights():
        from insights_engine.insights.sleep_readiness import SleepReadinessAnalyzer
        from insights_engine.insights.workout_recovery import WorkoutRecoveryAnalyzer
        from insights_engine.insights.readiness_trend import ReadinessTrendAnalyzer
        from insights_engine.insights.anomaly_detection import AnomalyDetectionAnalyzer
        from insights_engine.insights.timing_correlation import TimingCorrelationAnalyzer

        _athena = get_athena()
        results = []
        for Cls in [
            SleepReadinessAnalyzer,
            WorkoutRecoveryAnalyzer,
            ReadinessTrendAnalyzer,
            AnomalyDetectionAnalyzer,
            TimingCorrelationAnalyzer,
        ]:
            try:
                results.append(Cls(_athena).analyze())
            except Exception as e:
                st.warning(f"{Cls.__name__} failed: {e}")
        return results

    insights = run_all_insights()

    for result in insights:
        st.subheader(result.title)
        st.markdown(result.narrative)

        if result.chart:
            st.plotly_chart(result.chart, width="stretch")

        if result.caveats:
            with st.expander("Statistical Notes"):
                for caveat in result.caveats:
                    st.caption(f"⚠️ {caveat}")

        st.divider()


# ══════════════════════════════════════════════════════════════════════════
# PAGE 3: WEEKLY REPORT
# ══════════════════════════════════════════════════════════════════════════
elif page == "📋 Weekly Report":
    st.header("Weekly Report")

    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("🔄 Generate New Report"):
            with st.spinner("Generating report (~30s)..."):
                try:
                    from insights_engine.reports.weekly_report import WeeklyReportGenerator
                    from insights_engine.reports.delivery import save_local
                    from datetime import date

                    generator = WeeklyReportGenerator(get_athena())
                    report = generator.generate()
                    local_path = save_local(report.html)
                    st.session_state["latest_report_html"] = report.html
                    st.session_state["report_metadata"] = report.metadata
                    st.success(f"Report generated in {report.metadata['generation_time_sec']}s")
                except Exception as e:
                    st.error(f"Report generation failed: {e}")

    # Display report
    report_html = st.session_state.get("latest_report_html")
    if report_html:
        meta = st.session_state.get("report_metadata", {})
        st.caption(
            f"Week: {meta.get('week_start', '?')} → {meta.get('week_end', '?')} | "
            f"Generated: {meta.get('generated_at', '?')}"
        )
        st.components.v1.html(report_html, height=1200, scrolling=True)
    else:
        # Try loading from disk
        from pathlib import Path
        local_report = Path("reports_output/weekly-report.html")
        if local_report.exists():
            html = local_report.read_text()
            st.components.v1.html(html, height=1200, scrolling=True)
        else:
            st.info("No report generated yet. Click 'Generate New Report' to create one.")
