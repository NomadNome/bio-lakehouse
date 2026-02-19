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

# ── Theme ────────────────────────────────────────────────────────────────
_palette = CHART_CONFIG["color_palette"]

# Dark mode toggle (persisted in session state)
if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = True

_dark = st.session_state.dark_mode
_bg = _palette["background"] if _dark else "#FFFFFF"
_surface = _palette["surface"] if _dark else "#F1F5F9"
_text = _palette["text"] if _dark else "#1E293B"
_text_muted = _palette["text_muted"] if _dark else "#64748B"

st.markdown(f"""
<style>
    .stApp {{
        background-color: {_bg};
    }}
    .block-container {{
        padding-top: 2rem;
    }}
    .metric-label {{
        color: {_text_muted};
        font-size: 0.85rem;
    }}
    .metric-value {{
        color: {_text};
        font-size: 1.8rem;
        font-weight: 700;
    }}
    .sql-block {{
        background: {_surface};
        border-radius: 8px;
        padding: 1rem;
        font-family: monospace;
        font-size: 0.85rem;
        overflow-x: auto;
    }}
    div[data-testid="stChatMessage"] {{
        background: {_surface};
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
        ["💬 Ask", "📊 Insights", "📋 Weekly Report", "🔮 What-If"],
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
    st.toggle("Dark Mode", value=st.session_state.dark_mode, key="dark_mode_toggle",
              on_change=lambda: st.session_state.update(dark_mode=not st.session_state.dark_mode))
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


# ══════════════════════════════════════════════════════════════════════════
# PAGE 4: WHAT-IF SIMULATOR
# ══════════════════════════════════════════════════════════════════════════
elif page == "🔮 What-If":
    st.header("What-If Simulator")
    st.caption(
        "Explore how sleep, workout choices, and training load affect your predicted readiness."
    )

    from insights_engine.insights.what_if import Scenario, WhatIfSimulator
    from insights_engine.viz.what_if_charts import readiness_gauge, scenario_comparison_chart

    # Cached simulator — loads historical models once per session
    if "whatif_simulator" not in st.session_state:
        try:
            with st.spinner("Loading your historical patterns..."):
                sim = WhatIfSimulator(get_athena())
                sim.load_historical_models()
                st.session_state.whatif_simulator = sim
        except Exception as e:
            st.error(f"Failed to load historical data: {e}")
            import traceback
            st.code(traceback.format_exc())
            st.stop()

    simulator = st.session_state.whatif_simulator
    models = simulator.load_historical_models()
    baseline = models["baseline"]
    streak = models["current_streak"]

    # ── Input controls & results side-by-side ────────────────────────
    col_input, col_results = st.columns([1, 2])

    with col_input:
        st.subheader("Scenario")
        sleep_score = st.slider(
            "Tonight's Sleep Score",
            min_value=0,
            max_value=100,
            value=int(baseline["avg_readiness_7d"]),
            help="Your predicted or target sleep score (0-100)",
        )
        workout_map = {
            "Rest Day": "rest",
            "Cycling": "cycling",
            "Strength": "strength",
            "Cycling + Strength": "cycling_and_strength",
        }
        workout_label = st.selectbox(
            "Tomorrow's Workout",
            list(workout_map.keys()),
        )
        workout_type = workout_map[workout_label]

        intensity_options = ["None", "Low", "Moderate", "High"]
        default_intensity = 0 if workout_type == "rest" else 2
        workout_intensity = st.select_slider(
            "Workout Intensity",
            options=intensity_options,
            value=intensity_options[default_intensity],
        )

        consecutive_days = st.number_input(
            "Consecutive Workout Days",
            min_value=0,
            max_value=14,
            value=streak["consecutive_workout_days"],
            help="Including tomorrow if you plan to work out",
        )

        simulate_clicked = st.button("🔮 Simulate", type="primary", use_container_width=True)

    # ── Run simulation (auto-run on first load, then on button click) ─
    run_sim = simulate_clicked or "whatif_latest" not in st.session_state
    if run_sim:
        scenario = Scenario(
            sleep_score=sleep_score,
            workout_type=workout_type,
            workout_intensity=workout_intensity.lower(),
            consecutive_workout_days=int(consecutive_days),
        )
        result = simulator.simulate(scenario)

        # Save to session for comparison
        if "whatif_scenarios" not in st.session_state:
            st.session_state.whatif_scenarios = []

        label = f"{workout_label}, Sleep {sleep_score}"
        st.session_state.whatif_scenarios.append({
            "label": label,
            "scenario": scenario,
            "result": result,
            "predicted_readiness": result.predicted_readiness,
            "confidence_range": result.confidence_range,
        })
        # Keep only last 3
        st.session_state.whatif_scenarios = st.session_state.whatif_scenarios[-3:]
        st.session_state.whatif_latest = result

    # ── Display results ──────────────────────────────────────────────
    latest = st.session_state.get("whatif_latest")
    if latest:
        result = latest
        with col_results:
            st.subheader("Predicted Outcome")

            # Gauge chart
            gauge_fig = readiness_gauge(
                result.predicted_readiness,
                result.confidence_range,
                baseline["avg_readiness_7d"],
            )
            st.plotly_chart(gauge_fig, use_container_width=True)

            # Metrics row
            m1, m2, m3 = st.columns(3)

            # Energy state badge
            energy_colors = {
                "peak": _palette["success"],
                "high": _palette["accent"],
                "moderate": _palette["primary"],
                "low": _palette["warning"],
                "recovery_needed": _palette["danger"],
            }
            energy_color = energy_colors.get(result.energy_state, _palette["text_muted"])
            m1.markdown(
                f'<div style="text-align:center">'
                f'<span style="background:{energy_color};color:#fff;padding:4px 12px;'
                f'border-radius:12px;font-weight:600;font-size:0.9rem">'
                f'{result.energy_state.replace("_", " ").title()}</span>'
                f'<br><span style="color:{_text_muted};font-size:0.8rem">Energy State</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

            # Overtraining risk
            risk_colors = {"low": _palette["success"], "moderate": _palette["warning"], "high": _palette["danger"]}
            risk_color = risk_colors.get(result.overtraining_risk, _palette["text_muted"])
            m2.markdown(
                f'<div style="text-align:center">'
                f'<span style="background:{risk_color};color:#fff;padding:4px 12px;'
                f'border-radius:12px;font-weight:600;font-size:0.9rem">'
                f'{result.overtraining_risk.title()}</span>'
                f'<br><span style="color:{_text_muted};font-size:0.8rem">Overtraining Risk</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

            # Delta vs baseline
            delta = result.comparison_to_baseline
            m3.metric(
                "vs 7-Day Avg",
                f"{result.predicted_readiness:.0f}",
                delta=f"{delta:+.1f}",
                delta_color="normal",
            )

            # Recommendation
            st.info(result.recommendation)

            # Confidence note
            sd = result.supporting_data
            with st.expander("Statistical Notes"):
                st.caption(
                    f"Confidence range: {result.confidence_range[0]:.0f}–{result.confidence_range[1]:.0f} "
                    f"(±1 std from your historical '{sd.get('sleep_bucket', '?')}' sleep bucket, "
                    f"n={sd.get('bucket_n', '?')})"
                )
                if sd.get("regression_r") is not None:
                    st.caption(
                        f"Sleep→readiness regression: r={sd['regression_r']:.2f}, "
                        f"n={sd['regression_n']}"
                    )
                st.caption(
                    f"Workout type adjustment: {sd.get('workout_delta', 0):+.1f} | "
                    f"Overtraining penalty: {sd.get('overtraining_penalty', 0):+.1f}"
                )
                st.caption(
                    f"Based on {sd.get('total_historical_days', '?')} days of your historical data. "
                    f"Correlation ≠ causation — these are pattern-based projections, not medical advice."
                )

    # ── Scenario comparison ──────────────────────────────────────────
    saved = st.session_state.get("whatif_scenarios", [])
    if len(saved) >= 2:
        st.divider()
        st.subheader("Compare Scenarios")
        comp_fig = scenario_comparison_chart(saved)
        st.plotly_chart(comp_fig, use_container_width=True)

    if saved:
        if st.button("Clear Scenarios"):
            st.session_state.whatif_scenarios = []
            st.session_state.pop("whatif_latest", None)
            st.rerun()
