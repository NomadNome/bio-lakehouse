"""
Bio Insights Engine - Streamlit Application

Multi-page app with:
  1. Ask — NL-to-SQL chat interface
  2. Insights — 5 signature insight charts
  3. Weekly Report — latest generated report
"""

from __future__ import annotations

import time
from datetime import date, timedelta

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


def csv_download(df, filename, label="Download CSV"):
    """Render a download button for a DataFrame as CSV."""
    st.download_button(
        label=label,
        data=df.to_csv(index=False),
        file_name=filename,
        mime="text/csv",
    )


# ── Sidebar ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🧬 Bio Insights")
    page = st.radio(
        "Navigate",
        ["💬 Ask", "📊 Insights", "📋 Weekly Report", "🔮 What-If", "🎯 Predictions", "🧪 Experiments", "🔍 Discoveries", "📤 Export"],
        label_visibility="collapsed",
    )
    st.divider()

    # Quick metrics — bypass AthenaClient cache so sidebar always reflects today
    try:
        athena = get_athena()
        # Clear cached sidebar queries so CURRENT_DATE re-evaluates on each app load
        stale_keys = [k for k, (_, ts) in athena._query_cache.items()
                      if time.time() - ts > 300]  # expire after 5 min for sidebar
        for k in stale_keys:
            athena._query_cache.pop(k, None)

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

    # HealthKit vitals summary
    try:
        hk_df = athena.execute_query("""
            SELECT
                ROUND(AVG(resting_heart_rate_bpm), 0) AS avg_rhr,
                ROUND(AVG(hrv_ms), 0) AS avg_hrv,
                ROUND(AVG(vo2_max), 1) AS avg_vo2
            FROM bio_gold.daily_readiness_performance
            WHERE resting_heart_rate_bpm IS NOT NULL
              AND COALESCE(
                    TRY(CAST(date AS date)),
                    TRY(date_parse(date, '%Y-%m-%d %H:%i:%s'))
                  ) >= CURRENT_DATE - INTERVAL '7' DAY
        """)
        if not hk_df.empty and hk_df.iloc[0]["avg_rhr"] is not None:
            hk_row = hk_df.iloc[0]
            st.divider()
            st.caption("Apple Health (7-day)")
            c1, c2 = st.columns(2)
            c1.metric("Resting HR", f"{hk_row['avg_rhr']:.0f} bpm")
            c2.metric("HRV", f"{hk_row['avg_hrv']:.0f} ms")
            if hk_row.get("avg_vo2") and float(hk_row["avg_vo2"]) > 0:
                st.metric("VO2 Max", f"{hk_row['avg_vo2']:.1f}")
    except Exception:
        pass

    # Nutrition summary (if MFP data exists)
    try:
        nutr_df = athena.execute_query("""
            SELECT
                ROUND(AVG(daily_calories), 0) AS avg_cal,
                ROUND(AVG(protein_g), 0) AS avg_protein
            FROM bio_gold.daily_readiness_performance
            WHERE daily_calories IS NOT NULL
              AND COALESCE(
                    TRY(CAST(date AS date)),
                    TRY(date_parse(date, '%Y-%m-%d %H:%i:%s'))
                  ) >= CURRENT_DATE - INTERVAL '7' DAY
        """)
        if not nutr_df.empty and nutr_df.iloc[0]["avg_cal"] is not None:
            nutr_row = nutr_df.iloc[0]
            st.divider()
            st.caption("Nutrition (7-day)")
            nc1, nc2 = st.columns(2)
            nc1.metric("Avg Cal", f"{nutr_row['avg_cal']:.0f}")
            nc2.metric("Avg Protein", f"{nutr_row['avg_protein']:.0f}g")
    except Exception:
        pass

    # Data freshness indicator
    try:
        freshness_df = athena.execute_query("""
            SELECT
                MAX(COALESCE(
                    TRY(CAST(date AS date)),
                    TRY(date_parse(date, '%Y-%m-%d %H:%i:%s'))
                )) AS latest_date
            FROM bio_gold.dashboard_30day
        """)
        if not freshness_df.empty and freshness_df.iloc[0]["latest_date"] is not None:
            import pandas as _pd
            latest = _pd.to_datetime(freshness_df.iloc[0]["latest_date"]).date()
            days_old = (date.today() - latest).days
            if days_old == 0:
                freshness_icon = "🟢"
                freshness_label = "today"
            elif days_old == 1:
                freshness_icon = "🟢"
                freshness_label = "yesterday"
            elif days_old <= 3:
                freshness_icon = "🟡"
                freshness_label = f"{days_old}d ago"
            else:
                freshness_icon = "🔴"
                freshness_label = f"{days_old}d ago"
            st.divider()
            st.caption(f"{freshness_icon} Latest data: **{latest}** ({freshness_label})")
    except Exception:
        pass

    st.divider()
    if st.button("🔄 Refresh Data"):
        # Clear all Streamlit caches + What-If simulator session state
        st.cache_data.clear()
        st.cache_resource.clear()
        for _k in ["whatif_simulator", "whatif_loaded_at", "whatif_latest",
                    "multiday_result"]:
            st.session_state.pop(_k, None)
        st.rerun()

    st.toggle("Dark Mode", value=st.session_state.dark_mode, key="dark_mode_toggle",
              on_change=lambda: st.session_state.update(dark_mode=not st.session_state.dark_mode))
    st.caption("Data: Oura Ring + Peloton + Apple Health")
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
                                csv_download(result.data, "query_results.csv", "Download Results CSV")

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
    @st.cache_resource(ttl=600, show_spinner="Analyzing...")
    def run_all_insights():
        from insights_engine.insights.sleep_readiness import SleepReadinessAnalyzer
        from insights_engine.insights.workout_recovery import WorkoutRecoveryAnalyzer
        from insights_engine.insights.readiness_trend import ReadinessTrendAnalyzer
        from insights_engine.insights.anomaly_detection import AnomalyDetectionAnalyzer
        from insights_engine.insights.timing_correlation import TimingCorrelationAnalyzer
        from insights_engine.insights.training_load import TrainingLoadAnalyzer
        from insights_engine.insights.progressive_overload import ProgressiveOverloadAnalyzer
        from insights_engine.insights.recovery_windows import RecoveryWindowAnalyzer
        from insights_engine.insights.temperature_trend import TemperatureTrendAnalyzer
        from insights_engine.insights.sleep_architecture import SleepArchitectureAnalyzer
        from insights_engine.insights.nutrition_analyzer import NutritionAnalyzer

        _athena = get_athena()
        results = []
        for Cls in [
            SleepReadinessAnalyzer,
            WorkoutRecoveryAnalyzer,
            RecoveryWindowAnalyzer,
            ReadinessTrendAnalyzer,
            TemperatureTrendAnalyzer,
            SleepArchitectureAnalyzer,
            AnomalyDetectionAnalyzer,
            TimingCorrelationAnalyzer,
            TrainingLoadAnalyzer,
            ProgressiveOverloadAnalyzer,
            NutritionAnalyzer,
        ]:
            try:
                results.append(Cls(_athena).analyze())
            except Exception as e:
                st.warning(f"{Cls.__name__} failed: {e}")
        return results

    insights = run_all_insights()

    for result in insights:
        st.subheader(result.title)

        # Metric cards (if the analyzer provides them)
        metric_cards = result.statistics.get("metrics")
        if metric_cards:
            cols = st.columns(len(metric_cards))
            for col, m in zip(cols, metric_cards):
                col.metric(label=m["label"], value=m["value"], delta=m.get("delta"))

        st.markdown(result.narrative)

        if result.chart:
            st.plotly_chart(result.chart, width="stretch")

        if result.caveats:
            with st.expander("Statistical Notes"):
                for caveat in result.caveats:
                    st.caption(f"⚠️ {caveat}")

        st.divider()

    # ── HealthKit Vitals Trends ──────────────────────────────────────
    st.subheader("Apple Health — Vitals Trends")
    st.caption("Resting heart rate, HRV, and VO2 max from Apple Health (last 90 days).")

    @st.cache_data(ttl=600, show_spinner="Loading HealthKit vitals...")
    def load_hk_vitals():
        _athena = get_athena()
        return _athena.execute_query("""
            SELECT
                date,
                resting_heart_rate_bpm,
                hrv_ms,
                vo2_max,
                blood_oxygen_pct,
                weight_lbs,
                readiness_score,
                sleep_score
            FROM bio_gold.daily_readiness_performance
            WHERE resting_heart_rate_bpm IS NOT NULL
              AND COALESCE(
                    TRY(CAST(date AS date)),
                    TRY(date_parse(date, '%Y-%m-%d %H:%i:%s'))
                  ) >= CURRENT_DATE - INTERVAL '90' DAY
            ORDER BY date
        """)

    hk_vitals = load_hk_vitals()

    if not hk_vitals.empty:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots

        hk_vitals["date"] = pd.to_datetime(hk_vitals["date"])
        for col in ["resting_heart_rate_bpm", "hrv_ms", "vo2_max", "readiness_score"]:
            hk_vitals[col] = pd.to_numeric(hk_vitals[col], errors="coerce")

        fig = make_subplots(
            rows=3, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.06,
            subplot_titles=("Resting Heart Rate (bpm)", "Heart Rate Variability (ms)", "VO2 Max"),
        )

        # Resting HR
        fig.add_trace(
            go.Scatter(
                x=hk_vitals["date"], y=hk_vitals["resting_heart_rate_bpm"],
                mode="lines+markers", name="Resting HR",
                line=dict(color=_palette["danger"], width=2),
                marker=dict(size=4),
            ), row=1, col=1,
        )

        # HRV
        fig.add_trace(
            go.Scatter(
                x=hk_vitals["date"], y=hk_vitals["hrv_ms"],
                mode="lines+markers", name="HRV",
                line=dict(color=_palette["accent"], width=2),
                marker=dict(size=4),
            ), row=2, col=1,
        )

        # VO2 Max
        vo2_data = hk_vitals[hk_vitals["vo2_max"].notna() & (hk_vitals["vo2_max"] > 0)]
        if not vo2_data.empty:
            fig.add_trace(
                go.Scatter(
                    x=vo2_data["date"], y=vo2_data["vo2_max"],
                    mode="lines+markers", name="VO2 Max",
                    line=dict(color=_palette["success"], width=2),
                    marker=dict(size=4),
                ), row=3, col=1,
            )

        fig.update_layout(
            height=650,
            template="plotly_dark" if _dark else "plotly_white",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
            margin=dict(l=60, r=20, t=40, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

        csv_download(hk_vitals, "healthkit_vitals_90d.csv", "Download Vitals CSV")

        # HR vs Readiness correlation
        corr_data = hk_vitals.dropna(subset=["resting_heart_rate_bpm", "readiness_score"])
        if len(corr_data) >= 7:
            corr = corr_data["resting_heart_rate_bpm"].corr(corr_data["readiness_score"])
            direction = "lower" if corr < 0 else "higher"
            st.markdown(
                f"**Resting HR vs Readiness correlation:** r = {corr:.2f} — "
                f"{direction} resting heart rate tends to {'improve' if corr < 0 else 'coincide with lower'} readiness scores."
            )

        # ── Body Composition Dashboard ──
        @st.cache_data(ttl=600, show_spinner="Loading body composition...")
        def load_body_comp():
            _athena = get_athena()
            return _athena.execute_query("""
                SELECT date, weight_lbs, body_fat_pct, bmi, lean_body_mass_lbs
                FROM bio_gold.daily_readiness_performance
                WHERE COALESCE(TRY(CAST(date AS date)), TRY(date_parse(date, '%Y-%m-%d %H:%i:%s')))
                      >= DATE '2026-02-20'
                  AND (weight_lbs IS NOT NULL OR body_fat_pct IS NOT NULL)
                ORDER BY date
            """)

        body_data = load_body_comp()

        if not body_data.empty and len(body_data) >= 2:
            body_data["date"] = pd.to_datetime(body_data["date"])
            for col in ["weight_lbs", "body_fat_pct", "bmi", "lean_body_mass_lbs"]:
                body_data[col] = pd.to_numeric(body_data[col], errors="coerce")

            st.divider()
            st.subheader("Apple Health — Body Composition")
            st.caption("Baseline: Hume Health pod (Feb 20, 2026). Earlier data excluded due to dual-scale calibration on 2/19.")

            # ── Summary metrics with deltas ──
            def _first_last(series):
                valid = series.dropna()
                if valid.empty:
                    return None, None, None
                return valid.iloc[-1], valid.iloc[0], valid.iloc[-1] - valid.iloc[0]

            w_latest, w_first, w_delta = _first_last(body_data["weight_lbs"])
            bf_latest, bf_first, bf_delta = _first_last(body_data["body_fat_pct"])
            lm_latest, lm_first, lm_delta = _first_last(body_data["lean_body_mass_lbs"])
            bmi_latest, bmi_first, bmi_delta = _first_last(body_data["bmi"])

            from insights_engine.config import BODY_FAT_GOAL_PCT
            bf_goal = BODY_FAT_GOAL_PCT

            mc1, mc2, mc3, mc4, mc5 = st.columns(5)
            if w_latest is not None:
                mc1.metric("Weight", f"{w_latest:.1f} lbs", delta=f"{w_delta:+.1f} lbs", delta_color="inverse")
            if bf_latest is not None:
                mc2.metric("Body Fat", f"{bf_latest:.1f}%", delta=f"{bf_delta:+.1f}%", delta_color="inverse")
                if bf_goal > 0:
                    to_goal = bf_latest - bf_goal
                    mc5.metric("To Goal", f"{to_goal:.1f}% left", delta=f"{bf_delta:+.1f}% since baseline", delta_color="inverse")
            if lm_latest is not None:
                mc3.metric("Lean Mass", f"{lm_latest:.1f} lbs", delta=f"{lm_delta:+.1f} lbs", delta_color="normal")
            if bmi_latest is not None:
                mc4.metric("BMI", f"{bmi_latest:.1f}", delta=f"{bmi_delta:+.1f}", delta_color="inverse")

            # ── Multi-metric subplot chart ──
            has_weight = body_data["weight_lbs"].notna().any()
            has_bf = body_data["body_fat_pct"].notna().any()
            has_lm = body_data["lean_body_mass_lbs"].notna().any()
            row_count = int(sum([has_weight, has_bf, has_lm]))

            if row_count > 0:
                titles = []
                if has_weight:
                    titles.append("Weight (lbs)")
                if has_bf:
                    titles.append("Body Fat %")
                if has_lm:
                    titles.append("Lean Body Mass (lbs)")

                fig_body = make_subplots(
                    rows=row_count, cols=1,
                    shared_xaxes=True,
                    vertical_spacing=0.08,
                    subplot_titles=titles,
                )

                current_row = 1
                if has_weight:
                    w_series = body_data.dropna(subset=["weight_lbs"])
                    fig_body.add_trace(go.Scatter(
                        x=w_series["date"], y=w_series["weight_lbs"],
                        mode="lines+markers", name="Weight",
                        line=dict(color=_palette["primary"], width=2),
                        marker=dict(size=4),
                    ), row=current_row, col=1)
                    current_row += 1

                if has_bf:
                    bf_series = body_data.dropna(subset=["body_fat_pct"])
                    fig_body.add_trace(go.Scatter(
                        x=bf_series["date"], y=bf_series["body_fat_pct"],
                        mode="lines+markers", name="Body Fat %",
                        line=dict(color=_palette["danger"], width=2),
                        marker=dict(size=4),
                    ), row=current_row, col=1)
                    if bf_goal > 0:
                        fig_body.add_hline(
                            y=bf_goal, line_dash="dash", line_color=_palette["success"],
                            line_width=2,
                            annotation_text=f"Goal: {bf_goal:.0f}%",
                            annotation_font_color=_palette["success"],
                            annotation_font_size=10,
                            row=current_row, col=1,
                        )
                    current_row += 1

                if has_lm:
                    lm_series = body_data.dropna(subset=["lean_body_mass_lbs"])
                    fig_body.add_trace(go.Scatter(
                        x=lm_series["date"], y=lm_series["lean_body_mass_lbs"],
                        mode="lines+markers", name="Lean Mass",
                        line=dict(color=_palette["success"], width=2),
                        marker=dict(size=4),
                    ), row=current_row, col=1)

                fig_body.update_layout(
                    height=250 * row_count,
                    template="plotly_dark" if _dark else "plotly_white",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    showlegend=False,
                    margin=dict(l=60, r=20, t=40, b=20),
                )
                st.plotly_chart(fig_body, use_container_width=True)

            # ── Body comp vs. performance correlation ──
            if has_weight or has_bf:
                merged = body_data.merge(
                    hk_vitals[["date", "readiness_score"]].dropna(subset=["readiness_score"]),
                    on="date", how="inner",
                )
                if len(merged) >= 7:
                    st.markdown("**Body Composition vs. Readiness**")
                    corr_cols = st.columns(2)
                    if has_weight:
                        w_corr_data = merged.dropna(subset=["weight_lbs", "readiness_score"])
                        if len(w_corr_data) >= 7:
                            r_w = w_corr_data["weight_lbs"].corr(w_corr_data["readiness_score"])
                            interp_w = "lower weight days tend to coincide with higher readiness" if r_w < 0 else "weight shows little negative association with readiness"
                            corr_cols[0].markdown(f"Weight vs Readiness: **r = {r_w:.2f}** — {interp_w}")
                    if has_bf:
                        bf_corr_data = merged.dropna(subset=["body_fat_pct", "readiness_score"])
                        if len(bf_corr_data) >= 7:
                            r_bf = bf_corr_data["body_fat_pct"].corr(bf_corr_data["readiness_score"])
                            interp_bf = "lower body fat tends to coincide with higher readiness" if r_bf < 0 else "body fat shows little negative association with readiness"
                            corr_cols[1].markdown(f"Body Fat vs Readiness: **r = {r_bf:.2f}** — {interp_bf}")

            # ── Weekly trend table ──
            body_data["iso_week"] = body_data["date"].dt.isocalendar().week.astype(int)
            body_data["iso_year"] = body_data["date"].dt.isocalendar().year.astype(int)
            weekly = body_data.groupby(["iso_year", "iso_week"]).agg(
                avg_weight=("weight_lbs", "mean"),
                avg_body_fat=("body_fat_pct", "mean"),
                avg_lean_mass=("lean_body_mass_lbs", "mean"),
            ).reset_index()
            weekly["week_label"] = weekly["iso_year"].astype(str) + "-W" + weekly["iso_week"].astype(str).str.zfill(2)
            weekly["weight_change"] = weekly["avg_weight"].diff()
            weekly["bf_change"] = weekly["avg_body_fat"].diff()
            base_cols = ["week_label", "avg_weight", "avg_body_fat", "bf_change", "avg_lean_mass", "weight_change"]
            base_headers = ["Week", "Avg Weight (lbs)", "Avg BF %", "BF Change (%)", "Avg Lean Mass (lbs)", "Weight Change (lbs)"]
            fmt = {
                "Avg Weight (lbs)": "{:.1f}",
                "Avg BF %": "{:.1f}",
                "BF Change (%)": "{:+.1f}",
                "Avg Lean Mass (lbs)": "{:.1f}",
                "Weight Change (lbs)": "{:+.1f}",
            }

            if bf_goal > 0:
                weekly["bf_to_goal"] = weekly["avg_body_fat"] - bf_goal
                base_cols.insert(4, "bf_to_goal")
                base_headers.insert(4, "BF to Goal")
                fmt["BF to Goal"] = "{:.1f}"

            display_weekly = weekly[base_cols].copy()
            display_weekly.columns = base_headers

            st.markdown("**Weekly Trends**")
            st.dataframe(
                display_weekly.style.format(fmt, na_rep="—"),
                use_container_width=True,
                hide_index=True,
            )
            csv_download(body_data, "body_composition.csv", "Download Body Comp CSV")

        st.divider()
    else:
        st.info("No Apple Health vitals data available yet.")

    # ── Sleep Debt Tracker ──────────────────────────────────────────────
    st.subheader("Sleep Debt Tracker")
    st.caption("Rolling sleep deficit relative to your 14-day baseline.")

    @st.cache_data(ttl=600, show_spinner="Loading sleep debt data...")
    def load_sleep_debt():
        _athena = get_athena()
        return _athena.execute_query("""
            SELECT date, sleep_score, sleep_baseline_14d, sleep_deficit_daily, sleep_debt_7d
            FROM bio_gold.feature_readiness_daily
            WHERE sleep_baseline_14d IS NOT NULL
            ORDER BY date
        """)

    try:
        sleep_debt_df = load_sleep_debt()
        if not sleep_debt_df.empty:
            import plotly.graph_objects as go
            from plotly.subplots import make_subplots

            sleep_debt_df["date"] = pd.to_datetime(sleep_debt_df["date"])
            for col in ["sleep_score", "sleep_baseline_14d", "sleep_deficit_daily", "sleep_debt_7d"]:
                sleep_debt_df[col] = pd.to_numeric(sleep_debt_df[col], errors="coerce")

            # Show last 60 days
            sleep_debt_df = sleep_debt_df.tail(60)

            fig_sd = make_subplots(
                rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                subplot_titles=("Sleep Score vs 14-Day Baseline", "7-Day Rolling Sleep Debt"),
            )

            fig_sd.add_trace(go.Scatter(
                x=sleep_debt_df["date"], y=sleep_debt_df["sleep_score"],
                name="Sleep Score", mode="lines+markers",
                line=dict(color=_palette["primary"], width=2), marker=dict(size=3),
            ), row=1, col=1)
            fig_sd.add_trace(go.Scatter(
                x=sleep_debt_df["date"], y=sleep_debt_df["sleep_baseline_14d"],
                name="14-Day Baseline", mode="lines",
                line=dict(color=_palette["text_muted"], width=2, dash="dash"),
            ), row=1, col=1)

            colors_debt = [_palette["danger"] if v < 0 else _palette["success"]
                           for v in sleep_debt_df["sleep_debt_7d"]]
            fig_sd.add_trace(go.Bar(
                x=sleep_debt_df["date"], y=sleep_debt_df["sleep_debt_7d"],
                name="7-Day Sleep Debt", marker_color=colors_debt,
            ), row=2, col=1)
            fig_sd.add_hline(y=0, row=2, col=1, line_dash="dash", line_color="gray", opacity=0.5)

            fig_sd.update_layout(
                height=500, template="plotly_dark" if _dark else "plotly_white",
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                showlegend=True, legend=dict(orientation="h", y=1.12),
                margin=dict(l=50, r=20, t=60, b=20),
            )
            st.plotly_chart(fig_sd, use_container_width=True)

            csv_download(sleep_debt_df, "sleep_debt_60d.csv", "Download Sleep Debt CSV")

            # Current status
            latest = sleep_debt_df.iloc[-1]
            debt_val = latest["sleep_debt_7d"]
            if debt_val < -20:
                st.error(f"Significant sleep debt: {debt_val:.0f} points below baseline over the past week.")
            elif debt_val < -10:
                st.warning(f"Moderate sleep debt: {debt_val:.0f} points below baseline over the past week.")
            elif debt_val >= 0:
                st.success(f"Sleep surplus: +{debt_val:.0f} points above baseline. Well recovered!")
            else:
                st.info(f"Mild sleep deficit: {debt_val:.0f} points. Within normal range.")
        else:
            st.info("No sleep debt data available yet. Run dbt to materialize the feature table.")
    except Exception as e:
        st.warning(f"Sleep debt data not available: {e}")

    st.divider()

    # ── HRV Velocity Flags ──────────────────────────────────────────────
    st.subheader("HRV Velocity")
    st.caption("2-day HRV change rate — rising/falling/stable flags for early recovery signals.")

    @st.cache_data(ttl=600, show_spinner="Loading HRV velocity data...")
    def load_hrv_velocity():
        _athena = get_athena()
        return _athena.execute_query("""
            SELECT date, hrv_ms, hrv_2day_change, hrv_velocity_flag
            FROM bio_gold.feature_readiness_daily
            WHERE hrv_2day_change IS NOT NULL
            ORDER BY date
        """)

    try:
        hrv_vel_df = load_hrv_velocity()
        if not hrv_vel_df.empty:
            import plotly.graph_objects as go

            hrv_vel_df["date"] = pd.to_datetime(hrv_vel_df["date"])
            for col in ["hrv_ms", "hrv_2day_change"]:
                hrv_vel_df[col] = pd.to_numeric(hrv_vel_df[col], errors="coerce")

            # Show last 60 days
            hrv_vel_df = hrv_vel_df.tail(60)

            flag_colors = {
                "rising": _palette["success"],
                "falling": _palette["danger"],
                "stable": _palette["text_muted"],
            }
            marker_colors = [flag_colors.get(f, _palette["text_muted"]) for f in hrv_vel_df["hrv_velocity_flag"]]

            fig_hrv = go.Figure()
            fig_hrv.add_trace(go.Bar(
                x=hrv_vel_df["date"], y=hrv_vel_df["hrv_2day_change"],
                marker_color=marker_colors, name="HRV 2-Day Change",
            ))
            fig_hrv.add_hline(y=10, line_dash="dot", line_color=_palette["success"], opacity=0.5,
                              annotation_text="Rising threshold (+10ms)")
            fig_hrv.add_hline(y=-10, line_dash="dot", line_color=_palette["danger"], opacity=0.5,
                              annotation_text="Falling threshold (-10ms)")
            fig_hrv.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.3)

            fig_hrv.update_layout(
                height=350, template="plotly_dark" if _dark else "plotly_white",
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                title="HRV 2-Day Velocity (ms change)",
                xaxis_title="Date", yaxis_title="HRV Change (ms)",
                showlegend=False,
                margin=dict(l=50, r=20, t=60, b=20),
            )
            st.plotly_chart(fig_hrv, use_container_width=True)

            csv_download(hrv_vel_df, "hrv_velocity_60d.csv", "Download HRV Velocity CSV")

            # Flag summary
            recent_flags = hrv_vel_df.tail(7)["hrv_velocity_flag"]
            rising = (recent_flags == "rising").sum()
            falling = (recent_flags == "falling").sum()
            stable = (recent_flags == "stable").sum()

            cols_hrv = st.columns(3)
            cols_hrv[0].metric("Rising (7d)", f"{rising} days", delta=None)
            cols_hrv[1].metric("Stable (7d)", f"{stable} days", delta=None)
            cols_hrv[2].metric("Falling (7d)", f"{falling} days", delta=None)

            if falling >= 3:
                st.warning("HRV has been falling frequently — consider recovery-focused activities.")
            elif rising >= 3:
                st.success("HRV trending upward — your body is adapting well.")
        else:
            st.info("No HRV velocity data available yet. Run dbt to materialize the feature table.")
    except Exception as e:
        st.warning(f"HRV velocity data not available: {e}")

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

        if st.session_state.get("latest_report_html"):
            from insights_engine.reports.delivery import generate_pdf_bytes
            pdf_bytes = generate_pdf_bytes(st.session_state["latest_report_html"])
            st.download_button(
                label="📥 Download PDF",
                data=pdf_bytes,
                file_name="weekly-report.pdf",
                mime="application/pdf",
            )

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
            st.session_state["latest_report_html"] = html
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

    from datetime import date as _date, timedelta as _timedelta

    from insights_engine.insights.what_if import DayPlan, Scenario, WhatIfSimulator
    from insights_engine.viz.what_if_charts import (
        day_card_html,
        multi_day_projection_chart,
        readiness_gauge,
        scenario_comparison_chart,
    )

    # Simulator — refresh every 10 minutes so new workout/sleep data is reflected
    import time as _time
    _sim_stale = (
        "whatif_simulator" not in st.session_state
        or _time.time() - st.session_state.get("whatif_loaded_at", 0) > 600
    )
    if _sim_stale:
        try:
            with st.spinner("Loading your historical patterns..."):
                sim = WhatIfSimulator(get_athena())
                sim.load_historical_models()
                st.session_state.whatif_simulator = sim
                st.session_state.whatif_loaded_at = _time.time()
                # Clear stale multi-day projection so it uses fresh data
                st.session_state.pop("multiday_result", None)
        except Exception as e:
            st.error(f"Failed to load historical data: {e}")
            import traceback
            st.code(traceback.format_exc())
            st.stop()

    simulator = st.session_state.whatif_simulator
    models = simulator.load_historical_models()
    baseline = models["baseline"]
    streak = models["current_streak"]

    tab_single, tab_planner = st.tabs(["Single Scenario", "Multi-Day Planner"])

    # ══════════════════════════════════════════════════════════════════
    # TAB 1: SINGLE SCENARIO (existing code, verbatim)
    # ══════════════════════════════════════════════════════════════════
    with tab_single:
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

            simulate_clicked = st.button("Simulate", type="primary", use_container_width=True)

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

    # ══════════════════════════════════════════════════════════════════
    # TAB 2: MULTI-DAY PLANNER
    # ══════════════════════════════════════════════════════════════════
    with tab_planner:
        st.subheader("Multi-Day Readiness Planner")
        st.caption(
            "Plan 3-7 days ahead. Each day cascades into the next — "
            "consecutive workouts accumulate fatigue, rest days trigger recovery."
        )

        _plan_workout_map = {
            "Rest": "rest",
            "Cycling": "cycling",
            "Strength": "strength",
            "Cycling + Strength": "cycling_and_strength",
        }
        _plan_intensity_opts = ["None", "Low", "Moderate", "High"]

        horizon = st.radio(
            "Planning horizon (days)",
            options=[3, 4, 5, 6, 7],
            index=2,
            horizontal=True,
        )

        default_sleep = int(baseline.get("mean_sleep", baseline.get("avg_readiness_7d", 75)))
        today = _date.today()

        # Day input grid — max 4 per row
        day_plans = []
        rows_needed = (horizon + 3) // 4
        day_idx = 0
        for row_i in range(rows_needed):
            cols_in_row = min(4, horizon - day_idx)
            cols = st.columns(cols_in_row)
            for ci in range(cols_in_row):
                offset = day_idx + 1
                day_date = today + _timedelta(days=offset)
                label = day_date.strftime("%a %b %-d")
                with cols[ci]:
                    st.markdown(f"**Day {offset}: {label}**")
                    sl = st.slider(
                        "Sleep",
                        0,
                        100,
                        default_sleep,
                        key=f"plan_sleep_{offset}",
                    )
                    wl = st.selectbox(
                        "Workout",
                        list(_plan_workout_map.keys()),
                        key=f"plan_wtype_{offset}",
                    )
                    il = st.select_slider(
                        "Intensity",
                        options=_plan_intensity_opts,
                        value="None" if _plan_workout_map[wl] == "rest" else "Moderate",
                        key=f"plan_inten_{offset}",
                    )
                    day_plans.append(DayPlan(
                        day_offset=offset,
                        sleep_score=sl,
                        workout_type=_plan_workout_map[wl],
                        workout_intensity=il.lower(),
                    ))
                day_idx += 1

        project_clicked = st.button("Project Readiness", type="primary", use_container_width=True)

        if project_clicked:
            with st.spinner("Running multi-day projection..."):
                multi_result = simulator.simulate_multi_day(day_plans)
            st.session_state.multiday_result = multi_result

        mdr = st.session_state.get("multiday_result")
        if mdr and mdr.projections:
            # ── Projection chart ─────────────────────────────────────
            proj_fig = multi_day_projection_chart(mdr.projections, mdr.baseline_readiness_7d)
            st.plotly_chart(proj_fig, use_container_width=True)

            # ── Summary ──────────────────────────────────────────────
            st.info(mdr.plan_summary)

            # ── Day cards ────────────────────────────────────────────
            cards_per_row = min(4, len(mdr.projections))
            card_rows = (len(mdr.projections) + cards_per_row - 1) // cards_per_row
            pi = 0
            for _ in range(card_rows):
                n_cols = min(cards_per_row, len(mdr.projections) - pi)
                card_cols = st.columns(n_cols)
                for ci in range(n_cols):
                    with card_cols[ci]:
                        st.markdown(
                            day_card_html(mdr.projections[pi], _palette),
                            unsafe_allow_html=True,
                        )
                    pi += 1

            # ── Statistical notes ────────────────────────────────────
            with st.expander("Statistical Notes"):
                st.caption(
                    f"Starting CTL: {mdr.starting_ctl:.0f} | Starting ATL: {mdr.starting_atl:.0f}"
                )
                st.caption(
                    "Confidence bands widen by 5% per day to reflect increasing uncertainty. "
                    "Readiness predictions use the same sleep→readiness regression and "
                    "workout-type adjustments as the Single Scenario tab."
                )
                st.caption(
                    "TSS estimates are approximations based on workout type and intensity. "
                    "Actual training load may vary. These are pattern-based projections, not medical advice."
                )


# ══════════════════════════════════════════════════════════════════════════
# PAGE 5: PREDICTIONS
# ══════════════════════════════════════════════════════════════════════════
elif page == "🎯 Predictions":
    st.header("Next-Day Readiness Prediction")
    st.caption(
        "ML-powered prediction of tomorrow morning's readiness score based on today's data."
    )

    from pathlib import Path
    import json

    model_dir = Path(__file__).resolve().parent.parent / "models" / "readiness_predictor"
    model_path = model_dir / "model.joblib"
    metrics_path = model_dir / "metrics.json"
    backtest_path = model_dir / "backtest.csv"

    # Load metrics for model comparison and sample size warning
    _metrics = {}
    if metrics_path.exists():
        with open(metrics_path) as _mf:
            _metrics = json.load(_mf)

    # Sample size warning badge
    if _metrics.get("sample_size_warning"):
        st.warning(
            f"Model trained on {_metrics.get('n_samples', '?')} samples (< 50) "
            "— predictions may be unreliable. Consider collecting more data."
        )

    @st.cache_data(ttl=600, show_spinner="Running prediction...")
    def _cached_prediction():
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from models.readiness_predictor.predict import predict_next_day
        return predict_next_day()

    if not model_path.exists():
        st.warning(
            "No trained model found. Run `python -m models.readiness_predictor.train` first."
        )
    else:
        # ── Run prediction ──
        pred_result = None
        try:
            pred_result = _cached_prediction()
        except Exception as e:
            st.error(f"Prediction failed: {e}")

        if pred_result:
            predicted = pred_result["predicted_readiness"]
            conf = pred_result.get("confidence", {})

            # ── Gauge + Metrics ──
            col_gauge, col_metrics = st.columns([2, 1])

            with col_gauge:
                import plotly.graph_objects as go

                gauge_fig = go.Figure(go.Indicator(
                    mode="gauge+number+delta",
                    value=predicted,
                    title={"text": f"Predicted Readiness (day after {pred_result['prediction_date']})"},
                    gauge={
                        "axis": {"range": [0, 100]},
                        "bar": {"color": _palette["primary"]},
                        "steps": [
                            {"range": [0, 50], "color": _palette["danger"]},
                            {"range": [50, 70], "color": _palette["warning"]},
                            {"range": [70, 85], "color": _palette["accent"]},
                            {"range": [85, 100], "color": _palette["success"]},
                        ],
                        "threshold": {
                            "line": {"color": "white", "width": 2},
                            "thickness": 0.8,
                            "value": predicted,
                        },
                    },
                ))
                gauge_fig.update_layout(
                    height=300,
                    template="plotly_dark" if _dark else "plotly_white",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(gauge_fig, use_container_width=True)

            with col_metrics:
                st.metric("Predicted Score", f"{predicted:.0f}")
                if conf:
                    st.metric("Confidence Range", f"{conf.get('range_low', '?')} - {conf.get('range_high', '?')}")
                    st.metric("Model MAE", f"{conf.get('cv_mae', '?')}")
                    st.metric("Model R\u00b2", f"{conf.get('cv_r2', '?')}")
                    best_model_name = conf.get("best_model", _metrics.get("best_model", "?"))
                    st.caption(f"Model: **{best_model_name}** | Trained on {conf.get('n_training_samples', '?')} days")

                # Freshness: show current input features
                inputs = pred_result.get("input_features", {})
                today_rhr = inputs.get("resting_hr")
                today_ctl = inputs.get("ctl")
                if today_rhr is not None:
                    st.caption(f"Today's resting HR: {today_rhr:.0f}" + (f" | CTL: {today_ctl:.1f}" if today_ctl else ""))

            # ── Model Comparison Table ──
            candidate_comp = _metrics.get("candidate_comparison", {})
            baseline_info = _metrics.get("naive_baseline", {})
            if candidate_comp:
                with st.expander("Model Comparison (all candidates)"):
                    comp_rows = []
                    if baseline_info:
                        comp_rows.append({
                            "Model": "Naive Baseline (7d avg)",
                            "CV MAE": baseline_info.get("cv_mae", "—"),
                            "CV RMSE": baseline_info.get("cv_rmse", "—"),
                            "CV R²": baseline_info.get("cv_r2", "—"),
                        })
                    for name, res in candidate_comp.items():
                        winner_tag = " *" if name == _metrics.get("best_model") else ""
                        comp_rows.append({
                            "Model": name + winner_tag,
                            "CV MAE": res.get("cv_mae", "—"),
                            "CV RMSE": res.get("cv_rmse", "—"),
                            "CV R²": res.get("cv_r2", "—"),
                        })
                    st.dataframe(pd.DataFrame(comp_rows), hide_index=True, use_container_width=True)

            # ── MLflow history ──
            try:
                from models.readiness_predictor.mlflow_config import TRACKING_URI, EXPERIMENT_NAME
                import mlflow
                mlflow.set_tracking_uri(TRACKING_URI)
                experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
                if experiment:
                    with st.expander("MLflow Experiment History"):
                        runs = mlflow.search_runs(
                            experiment_ids=[experiment.experiment_id],
                            order_by=["start_time DESC"],
                            max_results=10,
                        )
                        if not runs.empty:
                            display_cols = [c for c in ["run_id", "tags.mlflow.runName", "params.model_type",
                                                         "metrics.cv_mae", "metrics.cv_r2", "start_time"] if c in runs.columns]
                            st.dataframe(runs[display_cols], hide_index=True, use_container_width=True)
                            st.caption(f"View full history: `mlflow ui --backend-store-uri {TRACKING_URI}`")
            except (ImportError, Exception):
                pass

            st.divider()

            # ── Tabs: Predictions + Diagnostics ──
            pred_tab, diag_tab = st.tabs(["Prediction Details", "Model Diagnostics"])

            with pred_tab:
                # ── Feature Importance Chart ──
                st.subheader("Top Feature Drivers")
                importances = pred_result.get("feature_importances", {})
                if importances:
                    import plotly.express as px

                    top_n = dict(list(importances.items())[:10])
                    imp_df = pd.DataFrame({
                        "Feature": list(top_n.keys()),
                        "Importance": list(top_n.values()),
                    })
                    imp_df = imp_df.sort_values("Importance", ascending=True)

                    fig_imp = px.bar(
                        imp_df, x="Importance", y="Feature", orientation="h",
                        color_discrete_sequence=[_palette["primary"]],
                    )
                    fig_imp.update_layout(
                        height=350,
                        template="plotly_dark" if _dark else "plotly_white",
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        yaxis_title="",
                        xaxis_title="Feature Importance",
                        margin=dict(l=20, r=20, t=30, b=20),
                    )
                    st.plotly_chart(fig_imp, use_container_width=True)

                # ── Backtest: Over Time ──
                st.subheader("Backtest: Predicted vs Actual")
                if backtest_path.exists():
                    bt = pd.read_csv(backtest_path)
                    bt["date"] = pd.to_datetime(bt["date"])
                    bt["next_day_readiness"] = pd.to_numeric(bt["next_day_readiness"], errors="coerce")
                    bt["predicted"] = pd.to_numeric(bt["predicted"], errors="coerce")
                    bt = bt.dropna()

                    fig_time = go.Figure()
                    fig_time.add_trace(go.Scatter(
                        x=bt["date"], y=bt["next_day_readiness"],
                        mode="lines", name="Actual",
                        line=dict(color=_palette["accent"], width=2),
                    ))
                    fig_time.add_trace(go.Scatter(
                        x=bt["date"], y=bt["predicted"],
                        mode="lines", name="Predicted",
                        line=dict(color=_palette["primary"], width=2, dash="dot"),
                    ))
                    fig_time.update_layout(
                        xaxis_title="Date", yaxis_title="Readiness Score",
                        height=350,
                        template="plotly_dark" if _dark else "plotly_white",
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                    )
                    st.plotly_chart(fig_time, use_container_width=True)
                else:
                    st.info("No backtest data available. Run training to generate backtest results.")

            with diag_tab:
                st.subheader("Model Diagnostics")

                if backtest_path.exists():
                    bt = pd.read_csv(backtest_path)
                    bt["date"] = pd.to_datetime(bt["date"])
                    bt["next_day_readiness"] = pd.to_numeric(bt["next_day_readiness"], errors="coerce")
                    bt["predicted"] = pd.to_numeric(bt["predicted"], errors="coerce")
                    bt = bt.dropna()
                    residuals = bt["predicted"] - bt["next_day_readiness"]

                    diag_col1, diag_col2 = st.columns(2)

                    # Residual plot: predicted vs actual scatter + zero line
                    with diag_col1:
                        st.markdown("**Residual Plot**")
                        fig_resid = go.Figure()
                        fig_resid.add_trace(go.Scatter(
                            x=bt["predicted"], y=residuals,
                            mode="markers",
                            marker=dict(color=_palette["primary"], size=6, opacity=0.6),
                            name="Residuals",
                        ))
                        fig_resid.add_hline(y=0, line_dash="dash", line_color=_palette["danger"])
                        fig_resid.update_layout(
                            xaxis_title="Predicted", yaxis_title="Residual (Pred - Actual)",
                            height=350,
                            template="plotly_dark" if _dark else "plotly_white",
                            paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="rgba(0,0,0,0)",
                            showlegend=False,
                        )
                        st.plotly_chart(fig_resid, use_container_width=True)

                    # Learning curve: MAE vs training set size
                    with diag_col2:
                        st.markdown("**Learning Curve (Walk-Forward)**")
                        cv_details = _metrics.get("cv_details", [])
                        if cv_details:
                            lc_df = pd.DataFrame(cv_details)
                            # Handle both old format (train_end) and new format (n_train)
                            x_col = "n_train" if "n_train" in lc_df.columns else "train_end"
                            fig_lc = go.Figure()
                            fig_lc.add_trace(go.Scatter(
                                x=lc_df[x_col], y=lc_df["mae"],
                                mode="lines+markers", name="MAE",
                                line=dict(color=_palette["accent"], width=2),
                                marker=dict(size=6),
                            ))
                            fig_lc.update_layout(
                                xaxis_title="Training Set Size", yaxis_title="MAE",
                                height=350,
                                template="plotly_dark" if _dark else "plotly_white",
                                paper_bgcolor="rgba(0,0,0,0)",
                                plot_bgcolor="rgba(0,0,0,0)",
                            )
                            st.plotly_chart(fig_lc, use_container_width=True)

                    # Residual statistics
                    st.markdown("**Residual Statistics**")
                    stat_cols = st.columns(4)
                    stat_cols[0].metric("Mean Residual", f"{residuals.mean():.2f}")
                    stat_cols[1].metric("Std Residual", f"{residuals.std():.2f}")
                    stat_cols[2].metric("Max Overestimate", f"{residuals.max():.1f}")
                    stat_cols[3].metric("Max Underestimate", f"{residuals.min():.1f}")

                    # Feature importance (moved into diagnostics tab)
                    if importances:
                        st.markdown("**Feature Importances**")
                        imp_df_diag = pd.DataFrame({
                            "Feature": list(importances.keys()),
                            "Importance": [round(v, 4) for v in importances.values()],
                        })
                        st.dataframe(imp_df_diag, hide_index=True, use_container_width=True)
                else:
                    st.info("Run model training to generate diagnostics data.")

    st.divider()


# ══════════════════════════════════════════════════════════════════════════
# PAGE 6: EXPERIMENTS
# ══════════════════════════════════════════════════════════════════════════
elif page == "🧪 Experiments":
    st.header("Experiment Tracker")
    st.caption(
        "Log interventions (supplements, training changes, diet, etc.) and analyze their causal effects."
    )

    from insights_engine.experiments.tracker import ExperimentStore, Intervention, InterventionType
    from insights_engine.experiments.analyzer import (
        get_pre_post_data, bayesian_analysis, did_analysis, ANALYSIS_METRICS,
        correlation_analysis, CORRELATION_INPUT_METRICS, CORRELATION_OUTCOME_METRICS,
    )
    from insights_engine.experiments import viz as exp_viz
    import numpy as np

    store = ExperimentStore()

    exp_tab_active, exp_tab_log, exp_tab_analysis, exp_tab_correlations = st.tabs(
        ["Active", "Log New", "Analysis", "Correlations"]
    )

    # ── Active Interventions Tab ──
    with exp_tab_active:
        st.subheader("Active & Past Interventions")
        try:
            all_interventions = store.list_interventions()
        except Exception as e:
            st.error(f"Failed to load interventions: {e}")
            all_interventions = []

        if not all_interventions:
            st.info("No interventions logged yet. Use the 'Log New' tab to add one.")
        else:
            active = [i for i in all_interventions if i.is_active]
            ended = [i for i in all_interventions if not i.is_active]

            # Check for overlaps among active interventions
            if len(active) > 1:
                st.warning(
                    f"{len(active)} interventions are active simultaneously — "
                    "results may be confounded. Consider ending one before analyzing."
                )

            if active:
                st.markdown("**Active**")
                for intv in active:
                    col_info, col_action = st.columns([4, 1])
                    with col_info:
                        st.markdown(
                            f"**{intv.name}** ({intv.type.value}) — "
                            f"Started {intv.start_date}"
                            + (f" | {intv.details}" if intv.details else "")
                        )
                    with col_action:
                        if st.button("End", key=f"end_{intv.id}"):
                            store.end_intervention(intv.id)
                            st.rerun()

            if ended:
                st.markdown("**Ended**")
                summary_data = []
                for intv in ended:
                    summary_data.append({
                        "name": intv.name,
                        "type": intv.type.value,
                        "start_date": intv.start_date,
                        "end_date": intv.end_date or "—",
                        "is_active": False,
                        "verdict": "—",
                    })
                summary_df = exp_viz.experiment_summary_table(summary_data)
                if not summary_df.empty:
                    st.dataframe(summary_df, hide_index=True, use_container_width=True)

    # ── Log New Intervention Tab ──
    with exp_tab_log:
        st.subheader("Log a New Intervention")

        with st.form("new_intervention_form"):
            intv_name = st.text_input("Intervention Name", placeholder="e.g., Creatine 5g daily")
            intv_type = st.selectbox(
                "Type",
                [t.value for t in InterventionType],
                format_func=lambda x: x.replace("_", " ").title(),
            )
            intv_details = st.text_area("Details", placeholder="Dosage, protocol, etc.")
            intv_start = st.date_input("Start Date", value=date.today())
            intv_washout = st.number_input("Washout Days", min_value=0, max_value=30, value=3,
                                           help="Days after ending before analyzing (to account for lingering effects)")
            intv_notes = st.text_area("Notes", placeholder="Why you're trying this, expected outcome, etc.")

            submitted = st.form_submit_button("Log Intervention", type="primary")

            if submitted:
                if not intv_name:
                    st.error("Please enter an intervention name.")
                else:
                    # Check for overlaps
                    overlaps = store.check_overlaps(intv_start.isoformat())
                    if overlaps:
                        overlap_names = ", ".join(o.name for o in overlaps)
                        st.warning(f"Overlaps with: {overlap_names} — effects may be confounded.")

                    new_intv = Intervention(
                        id=ExperimentStore.new_id(),
                        name=intv_name,
                        type=InterventionType(intv_type),
                        details=intv_details,
                        start_date=intv_start.isoformat(),
                        washout_days=intv_washout,
                        notes=intv_notes,
                    )
                    try:
                        store.add_intervention(new_intv)
                        st.success(f"Logged: {intv_name}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to save: {e}")

    # ── Analysis Tab ──
    with exp_tab_analysis:
        st.subheader("Intervention Analysis")

        try:
            all_for_analysis = store.list_interventions()
        except Exception:
            all_for_analysis = []

        if not all_for_analysis:
            st.info("Log an intervention first, then come back here to analyze its effect.")
        else:
            sel_intv_name = st.selectbox(
                "Select Intervention",
                [i.name for i in all_for_analysis],
                key="analysis_intv_select",
            )
            sel_intv = next((i for i in all_for_analysis if i.name == sel_intv_name), None)

            sel_metric = st.selectbox(
                "Metric to Analyze",
                list(ANALYSIS_METRICS.keys()),
                format_func=lambda k: ANALYSIS_METRICS[k],
                key="analysis_metric_select",
            )

            if sel_intv and st.button("Run Analysis", type="primary"):
                athena = get_athena()
                metric_label = ANALYSIS_METRICS[sel_metric]

                with st.spinner("Fetching data and running analysis..."):
                    try:
                        pre_df, post_df = get_pre_post_data(athena, sel_intv, sel_metric)

                        if pre_df.empty or post_df.empty:
                            st.warning("Not enough data for analysis. Need both pre and post observations.")
                        else:
                            pre_vals = pre_df[sel_metric].values
                            post_vals = post_df[sel_metric].values

                            # ── Bayesian Analysis ──
                            bayes = bayesian_analysis(pre_vals, post_vals)

                            st.markdown("### Bayesian Analysis")
                            bayes_cols = st.columns(4)
                            bayes_cols[0].metric("Effect", f"{bayes.posterior_mean_effect:+.2f}")
                            bayes_cols[1].metric("P(Positive)", f"{bayes.prob_positive:.1%}")
                            bayes_cols[2].metric("Cohen's d", f"{bayes.cohens_d:.2f}")
                            bayes_cols[3].metric("Verdict", bayes.verdict)

                            st.caption(
                                f"95% CI: [{bayes.credible_interval_95[0]:.2f}, {bayes.credible_interval_95[1]:.2f}] | "
                                f"Pre: {bayes.pre_mean:.1f} (n={bayes.n_pre}) → Post: {bayes.post_mean:.1f} (n={bayes.n_post})"
                            )

                            # Charts
                            chart_col1, chart_col2 = st.columns(2)
                            with chart_col1:
                                timeline_fig = exp_viz.intervention_timeline(
                                    pre_df, post_df, sel_metric, metric_label,
                                    sel_intv.name, sel_intv.start_date, sel_intv.end_date,
                                    dark=_dark,
                                )
                                st.plotly_chart(timeline_fig, use_container_width=True)

                            with chart_col2:
                                dist_fig = exp_viz.before_after_distribution(
                                    pre_vals, post_vals, metric_label, dark=_dark,
                                )
                                st.plotly_chart(dist_fig, use_container_width=True)

                            # Posterior plot
                            post_fig = exp_viz.posterior_plot(bayes, dark=_dark)
                            st.plotly_chart(post_fig, use_container_width=True)

                            # ── DiD Analysis ──
                            st.markdown("### Difference-in-Differences")
                            did = did_analysis(pre_df, post_df, sel_metric)

                            if did.warning:
                                st.warning(did.warning)

                            did_cols = st.columns(3)
                            did_cols[0].metric("DiD Effect", f"{did.did_effect:+.2f}")
                            did_cols[1].metric("Pre-Trend R²", f"{did.pre_trend_r2:.3f}")
                            did_cols[2].metric(
                                "Parallel Trends",
                                "Valid" if did.parallel_trends_valid else "Invalid",
                            )
                            st.caption(
                                f"Counterfactual post mean: {did.counterfactual_post_mean:.1f} | "
                                f"Actual post mean: {did.actual_post_mean:.1f} | "
                                f"Pre-trend slope: {did.pre_trend_slope:.4f}/day"
                            )

                    except Exception as e:
                        st.error(f"Analysis failed: {e}")

    # ── Correlations Tab ──
    with exp_tab_correlations:
        st.subheader("Metric Correlations")
        st.caption(
            "Explore how nutrition, training, and lifestyle inputs correlate with health outcomes. "
            "Use lag to test delayed effects (e.g., today's protein → tomorrow's readiness)."
        )

        corr_col1, corr_col2 = st.columns(2)
        with corr_col1:
            corr_input = st.selectbox(
                "Input Variable",
                list(CORRELATION_INPUT_METRICS.keys()),
                format_func=lambda k: CORRELATION_INPUT_METRICS[k],
                key="corr_input_select",
            )
        with corr_col2:
            corr_outcome = st.selectbox(
                "Outcome Variable",
                list(CORRELATION_OUTCOME_METRICS.keys()),
                format_func=lambda k: CORRELATION_OUTCOME_METRICS[k],
                key="corr_outcome_select",
            )

        corr_s1, corr_s2, corr_s3 = st.columns(3)
        with corr_s1:
            corr_lookback = st.slider(
                "Lookback (days)", min_value=14, max_value=365, value=90,
                key="corr_lookback",
            )
        with corr_s2:
            corr_window = st.slider(
                "Rolling Window (days)", min_value=7, max_value=30, value=14,
                key="corr_window",
            )
        with corr_s3:
            corr_lag = st.number_input(
                "Lag (days)", min_value=0, max_value=7, value=0,
                help="Shift outcome forward by N days (today's input → future outcome)",
                key="corr_lag",
            )

        if st.button("Run Correlation Analysis", type="primary", key="run_corr"):
            athena = get_athena()
            input_label = CORRELATION_INPUT_METRICS[corr_input]
            outcome_label = CORRELATION_OUTCOME_METRICS[corr_outcome]

            end_dt = date.today()
            start_dt = end_dt - timedelta(days=corr_lookback)

            with st.spinner("Computing correlation..."):
                try:
                    result = correlation_analysis(
                        athena,
                        corr_input,
                        corr_outcome,
                        start_dt.isoformat(),
                        end_dt.isoformat(),
                        rolling_window=corr_window,
                        lag_days=corr_lag,
                    )

                    if result is None:
                        st.warning(
                            "Not enough data for correlation analysis. "
                            "Need at least 5 days where both metrics have values."
                        )
                    else:
                        # Metric cards
                        mc1, mc2, mc3, mc4 = st.columns(4)
                        mc1.metric("Pearson r", f"{result.pearson_r:.3f}")
                        mc2.metric("p-value", f"{result.p_value:.4f}")
                        mc3.metric("R²", f"{result.r_squared:.3f}")
                        mc4.metric("Observations", f"{result.n_observations}")

                        # Interpretation
                        if result.p_value < 0.05:
                            st.info(result.interpretation)
                        else:
                            st.warning(result.interpretation)

                        # Charts side by side
                        chart_c1, chart_c2 = st.columns(2)
                        with chart_c1:
                            scatter_fig = exp_viz.correlation_scatter(
                                result, input_label, outcome_label, dark=_dark,
                            )
                            st.plotly_chart(scatter_fig, use_container_width=True)
                        with chart_c2:
                            if result.rolling_r_values:
                                rolling_fig = exp_viz.rolling_correlation_chart(
                                    result, input_label, outcome_label, dark=_dark,
                                )
                                st.plotly_chart(rolling_fig, use_container_width=True)
                            else:
                                st.info("Not enough data for rolling correlation chart.")

                        # Practical interpretation
                        if corr_lag > 0:
                            lag_text = f" (with {corr_lag}-day lag: today's {input_label} → {'tomorrow' if corr_lag == 1 else f'{corr_lag} days later'}'s {outcome_label})"
                        else:
                            lag_text = " (same-day)"

                        if abs(result.pearson_r) >= 0.3 and result.p_value < 0.05:
                            direction = "higher" if result.pearson_r > 0 else "lower"
                            st.success(
                                f"Days with higher {input_label} tend to have {direction} "
                                f"{outcome_label}{lag_text}. "
                                f"Each unit increase in {input_label} is associated with a "
                                f"{result.regression_slope:+.4f} change in {outcome_label}."
                            )
                        elif result.p_value >= 0.05:
                            st.info(
                                f"No statistically significant relationship detected between "
                                f"{input_label} and {outcome_label}{lag_text} in this time period."
                            )
                        else:
                            st.info(
                                f"Weak but significant relationship between "
                                f"{input_label} and {outcome_label}{lag_text}."
                            )

                except Exception as e:
                    st.error(f"Correlation analysis failed: {e}")


# ══════════════════════════════════════════════════════════════════════════
# PAGE 7: CORRELATION DISCOVERIES
# ══════════════════════════════════════════════════════════════════════════
elif page == "🔍 Discoveries":
    st.header("Correlation Discovery Engine")
    st.caption(
        "Automated scan of all metric pairs for significant correlations and threshold effects."
    )

    from insights_engine.insights.correlation_discovery import (
        CorrelationDiscoveryEngine, DiscoveryResult, _format_metric,
    )
    from insights_engine.insights.discovery_persistence import DiscoveryStore
    import plotly.graph_objects as _pgo
    import numpy as _np

    # ── Controls Row ────────────────────────────────────────────────────
    ctrl_c1, ctrl_c2, ctrl_c3, ctrl_c4 = st.columns([2, 1, 1, 1])

    with ctrl_c1:
        # Load saved runs for dropdown
        _disc_store = DiscoveryStore()
        _saved_runs = []
        try:
            _saved_runs = _disc_store.list_runs()
        except Exception:
            pass
        _run_options = ["Run New Scan"] + list(reversed(_saved_runs))
        _selected_run = st.selectbox("Saved Runs", _run_options)

    with ctrl_c2:
        _disc_lookback = st.slider("Lookback (days)", 60, 365, 180, step=30)

    with ctrl_c3:
        _disc_min_strength = st.slider("Min |rho|", 0.15, 0.60, 0.25, step=0.05)

    with ctrl_c4:
        _run_now = st.button("Run Discovery Now", type="primary")

    # ── Load or Run ─────────────────────────────────────────────────────
    _disc_result = None

    if _run_now or _selected_run == "Run New Scan":
        if _run_now:
            with st.spinner("Scanning all metric pairs..."):
                try:
                    _disc_engine = CorrelationDiscoveryEngine(get_athena())
                    # Try loading prior for new-finding detection
                    _prior = None
                    try:
                        _prior = _disc_store.load_latest()
                    except Exception:
                        pass
                    _disc_result = _disc_engine.discover(
                        lookback_days=_disc_lookback,
                        min_rho=_disc_min_strength,
                        prior_results=_prior,
                    )
                    st.session_state["discovery_result"] = _disc_result
                    # Save locally
                    try:
                        _disc_store.save_local(_disc_result)
                    except Exception:
                        pass
                except Exception as e:
                    st.error(f"Discovery scan failed: {e}")
        elif "discovery_result" in st.session_state:
            _disc_result = st.session_state["discovery_result"]
    elif _selected_run != "Run New Scan":
        with st.spinner(f"Loading results from {_selected_run}..."):
            try:
                _disc_result = _disc_store.load_by_date(_selected_run)
            except Exception as e:
                st.error(f"Failed to load results: {e}")

    if _disc_result is None and "discovery_result" in st.session_state:
        _disc_result = st.session_state["discovery_result"]

    if _disc_result is None:
        st.info("Click **Run Discovery Now** to scan all metric pairs, or select a saved run.")
    else:
        # ── Summary Metrics ─────────────────────────────────────────────
        sm1, sm2, sm3, sm4 = st.columns(4)
        sm1.metric("Pairs Tested", f"{_disc_result.pairs_tested:,}")
        sm2.metric("Correlations", f"{len(_disc_result.correlations)}")
        sm3.metric("Threshold Effects", f"{len(_disc_result.thresholds)}")
        sm4.metric("Data Range", f"{_disc_result.total_rows} days")

        # ── Top Finding ─────────────────────────────────────────────────
        if _disc_result.top_finding_narrative:
            st.info(f"**Top Finding:** {_disc_result.top_finding_narrative}")

        # ── Tabs ────────────────────────────────────────────────────────
        disc_tab1, disc_tab2, disc_tab3 = st.tabs(
            ["Correlations", "Threshold Effects", "Correlation Heatmap"]
        )

        # ── Tab 1: Correlations ─────────────────────────────────────────
        with disc_tab1:
            if not _disc_result.correlations:
                st.warning("No significant correlations found at current thresholds.")
            else:
                for _ci, _cf in enumerate(_disc_result.correlations):
                    _direction_icon = "↑" if _cf.rho > 0 else "↓"
                    _lag_badge = f"  `lag {_cf.lag}d`" if _cf.lag > 0 else ""
                    _new_badge = "  `NEW`" if _cf.is_new else ""
                    _a_label = _format_metric(_cf.metric_a)
                    _b_label = _format_metric(_cf.metric_b)

                    with st.expander(
                        f"**{_a_label}** {_direction_icon} **{_b_label}**{_lag_badge}{_new_badge}  —  "
                        f"rho={_cf.rho:+.3f}  |  {_cf.strength}",
                        expanded=_ci < 3,
                    ):
                        _mc1, _mc2, _mc3, _mc4 = st.columns(4)
                        _mc1.metric("Spearman rho", f"{_cf.rho:+.3f}")
                        _mc2.metric("p (corrected)", f"{_cf.p_corrected:.4f}")
                        _mc3.metric("Strength", _cf.strength.replace("_", " ").title())
                        _mc4.metric("Samples", f"{_cf.n_samples}")
                        st.markdown(_cf.narrative)

        # ── Tab 2: Threshold Effects ────────────────────────────────────
        with disc_tab2:
            if not _disc_result.thresholds:
                st.warning("No significant threshold effects found.")
            else:
                for _ti, _tf in enumerate(_disc_result.thresholds):
                    _new_badge_t = "  `NEW`" if _tf.is_new else ""
                    _trigger_label = _format_metric(_tf.trigger_metric)
                    _outcome_label = _format_metric(_tf.outcome_metric)

                    with st.expander(
                        f"When **{_trigger_label}** >= {_tf.threshold:.0f} → "
                        f"**{_outcome_label}** delta={_tf.delta:+.1f}{_new_badge_t}",
                        expanded=_ti < 3,
                    ):
                        _tc1, _tc2, _tc3, _tc4 = st.columns(4)
                        _tc1.metric(f"Above (n={_tf.n_above})", f"{_tf.mean_above:.1f}")
                        _tc2.metric(f"Below (n={_tf.n_below})", f"{_tf.mean_below:.1f}")
                        _tc3.metric("Delta", f"{_tf.delta:+.1f}")
                        _tc4.metric("p-value", f"{_tf.p_value:.4f}")
                        st.markdown(_tf.narrative)

        # ── Tab 3: Correlation Heatmap ──────────────────────────────────
        with disc_tab3:
            # Build heatmap from lag-0 correlations
            _lag0 = [c for c in _disc_result.correlations if c.lag == 0]
            if not _lag0:
                st.warning("No lag-0 correlations to display in heatmap.")
            else:
                # Collect unique metrics from lag-0 findings
                _hm_metrics = sorted(set(
                    m for c in _lag0 for m in (c.metric_a, c.metric_b)
                ))
                _n_hm = len(_hm_metrics)
                _hm_matrix = _np.zeros((_n_hm, _n_hm))
                _hm_matrix[:] = _np.nan

                # Fill diagonal
                for _i in range(_n_hm):
                    _hm_matrix[_i][_i] = 1.0

                # Fill from findings
                _metric_idx = {m: i for i, m in enumerate(_hm_metrics)}
                for _c in _lag0:
                    _ia = _metric_idx.get(_c.metric_a)
                    _ib = _metric_idx.get(_c.metric_b)
                    if _ia is not None and _ib is not None:
                        _hm_matrix[_ia][_ib] = _c.rho
                        _hm_matrix[_ib][_ia] = _c.rho

                _hm_labels = [_format_metric(m) for m in _hm_metrics]

                _hm_fig = _pgo.Figure(data=_pgo.Heatmap(
                    z=_hm_matrix,
                    x=_hm_labels,
                    y=_hm_labels,
                    colorscale="RdBu_r",
                    zmid=0,
                    zmin=-1,
                    zmax=1,
                    text=_np.where(
                        _np.isnan(_hm_matrix), "",
                        _np.vectorize(lambda v: f"{v:.2f}")(_hm_matrix)
                    ),
                    texttemplate="%{text}",
                    colorbar=dict(title="Spearman rho"),
                ))
                _hm_fig.update_layout(
                    title="Significant Correlations (Lag 0)",
                    height=max(400, 50 * _n_hm),
                    xaxis=dict(tickangle=-45),
                    margin=dict(l=150, b=150),
                )
                st.plotly_chart(_hm_fig, use_container_width=True)

        # ── Save to S3 button ───────────────────────────────────────────
        if st.button("Save to S3"):
            try:
                s3_uri = _disc_store.save(_disc_result)
                st.success(f"Saved to {s3_uri}")
            except Exception as e:
                st.error(f"S3 save failed: {e}")


# ══════════════════════════════════════════════════════════════════════════
# PAGE 8: FHIR EXPORT
# ══════════════════════════════════════════════════════════════════════════
elif page == "📤 Export":
    st.header("FHIR R4 Bundle Export")
    st.caption(
        "Export your health data as an HL7 FHIR R4 Bundle (JSON) for sharing with healthcare providers."
    )

    from datetime import date, timedelta
    from insights_engine.fhir.bundle_builder import FHIRBundleBuilder

    col1, col2 = st.columns(2)
    with col1:
        export_start = st.date_input(
            "Start Date",
            value=date.today() - timedelta(days=30),
        )
    with col2:
        export_end = st.date_input(
            "End Date",
            value=date.today(),
        )

    if export_start > export_end:
        st.error("Start date must be before end date.")
    else:
        if st.button("Generate FHIR Bundle", type="primary"):
            with st.spinner("Building FHIR Bundle..."):
                try:
                    builder = FHIRBundleBuilder(get_athena())
                    bundle = builder.build(export_start, export_end)
                    bundle_json = __import__("json").dumps(bundle, indent=2)

                    # Resource count summary
                    resource_types = {}
                    for entry in bundle.get("entry", []):
                        rt = entry["resource"]["resourceType"]
                        resource_types[rt] = resource_types.get(rt, 0) + 1

                    st.success(
                        f"Bundle generated: {bundle['total']} resources "
                        f"({export_start} to {export_end})"
                    )

                    # Summary metrics
                    cols = st.columns(len(resource_types))
                    for i, (rt, count) in enumerate(sorted(resource_types.items())):
                        cols[i].metric(rt, count)

                    # JSON preview
                    with st.expander("Preview JSON", expanded=False):
                        st.code(bundle_json[:5000] + ("\n..." if len(bundle_json) > 5000 else ""), language="json")

                    # Download button
                    st.download_button(
                        label="Download FHIR Bundle (.json)",
                        data=bundle_json,
                        file_name=f"bio-lakehouse-fhir-bundle-{export_start}-to-{export_end}.json",
                        mime="application/fhir+json",
                    )

                    st.session_state["last_fhir_bundle"] = bundle_json

                except Exception as e:
                    st.error(f"Bundle generation failed: {e}")
