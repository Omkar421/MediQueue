"""
MediQueue - Hospital Waiting Time Analyzer
Step 5: Final Polish, Quality Audit & UI Integration
A hospital operations command center powered by Apache PySpark 3.5.9 (local[1]) and SQLite.
"""

import sys
import os
from datetime import datetime

# Ensure PySpark workers match active Python environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from ui_components import (
    apply_custom_css,
    render_header,
    render_kpi_card,
    get_plotly_layout,
    COLOR_NAVY_DARK,
    COLOR_NAVY_PRIMARY,
    COLOR_TEAL_PRIMARY,
    COLOR_TEAL_LIGHT,
    COLOR_CYAN_ACCENT,
    COLOR_AMBER_WARNING,
    COLOR_ROSE_CRITICAL,
    COLOR_GRAY_MUTED,
    DEPT_COLORS,
    PRIORITY_COLORS,
    CATEGORY_COLORS
)

from dashboard_helpers import (
    get_cached_analytics,
    get_cached_patient_visits,
    convert_dict_list_to_csv,
    generate_executive_pdf_report
)

# Page configuration
st.set_page_config(
    page_title="MediQueue - Hospital Waiting Time Analyzer",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inject custom healthcare command center CSS
apply_custom_css()

# Load visit records for queue operations and filter choices.
raw_visits = get_cached_patient_visits()
raw_df = pd.DataFrame(raw_visits) if raw_visits else pd.DataFrame()


# ==========================================
# 1. SIDEBAR NAVIGATION & ENGINE STATUS
# ==========================================
with st.sidebar:
    st.markdown("""
    <div class="sidebar-brand">
        <h2>🏥 MediQueue</h2>
        <p>Hospital Operations Command Center</p>
    </div>
    """, unsafe_allow_html=True)

    nav_options = [
        "Overview Dashboard",
        "Department Analytics",
        "Patient Queue",
        "Waiting Time Analysis",
        "Reports & Downloads",
        "About MediQueue"
    ]
    default_nav_idx = 0
    if "page" in st.query_params:
        requested_page = st.query_params["page"]
        if requested_page in nav_options:
            default_nav_idx = nav_options.index(requested_page)

    nav_selection = st.radio(
        "Navigation",
        nav_options,
        index=default_nav_idx,
        label_visibility="collapsed"
    )

    st.markdown("---")

    st.markdown("#### Explore the data")
    st.caption("Each selection filters the source DataFrame in Spark before the dashboard metrics are calculated.")
    if st.button("Reset analytics filters", use_container_width=True):
        st.session_state["filter_department"] = "All Departments"
        st.session_state["filter_priority"] = "All Priorities"
        st.session_state["filter_day"] = "All Days"

    departments = (
        sorted(raw_df["department"].dropna().unique().tolist())
        if not raw_df.empty else []
    )
    department_options = ["All Departments"] + departments
    selected_department = st.selectbox(
        "Department",
        department_options,
        key="filter_department"
    )
    selected_priority = st.selectbox(
        "Triage priority",
        ["All Priorities", "Critical", "High", "Medium", "Low"],
        key="filter_priority"
    )
    selected_day = st.selectbox(
        "Arrival day",
        ["All Days", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
        key="filter_day"
    )

    analytics_data = get_cached_analytics(
        department="" if selected_department == "All Departments" else selected_department,
        priority="" if selected_priority == "All Priorities" else selected_priority,
        day_of_week="" if selected_day == "All Days" else selected_day
    )
    overall_stats = analytics_data["overall_stats"]
    dept_analytics = analytics_data["department_analytics"]
    pri_analytics = analytics_data["priority_analytics"]
    wait_classes = analytics_data["waiting_time_classification"]
    peak_hours = analytics_data["peak_hour_analysis"]
    day_stats = analytics_data["day_wise_analysis"]
    insights = analytics_data["insights"]

    st.markdown(f"""
    <div class="engine-status-box">
        <div class="engine-status-title">Analytics Engine</div>
        <div class="engine-status-name">
            <span style="color:#22C55E; font-size:1.1rem;">●</span> Apache Spark 3.5.9
        </div>
        <div style="font-size:0.75rem; color:#64748B; margin-top:4px; line-height: 1.5;">
            Mode: <b>local[1]</b> (Low RAM)<br>
            Driver Memory: <b>1 GB</b><br>
            Analytics: <b>Spark DataFrames</b><br>
            Cohort: <b>{overall_stats['total_patients']} visits</b><br>
            Avg Wait: <b>{overall_stats['avg_waiting_time']} mins</b>
        </div>
    </div>
    """, unsafe_allow_html=True)

    if not raw_df.empty:
        if selected_department != "All Departments":
            raw_df = raw_df[raw_df["department"] == selected_department]
        if selected_priority != "All Priorities":
            raw_df = raw_df[raw_df["priority"] == selected_priority]
        if selected_day != "All Days":
            raw_df = raw_df[raw_df["day_of_week"] == selected_day]

    st.caption(f"Showing {overall_stats['total_patients']} matching visits from the Spark analytics pipeline.")

    # Refresh Button
    if st.button("🔄 Refresh PySpark Pipeline", use_container_width=True):
        st.cache_data.clear()
        st.toast("PySpark Analytics Pipeline Refreshed!", icon="🔄")
        st.rerun()

    st.markdown("""
    <div class="viva-box">
        <b>💡 Viva Quick Tip:</b><br>
        All aggregations and window rankings execute genuine PySpark SQL DataFrames in local mode, cached for zero rerun overhead.
    </div>
    """, unsafe_allow_html=True)

    st.markdown(
        "<div style='font-size:0.75rem; color:#94A3B8; margin-top:1rem; text-align:center;'>"
        "MediQueue Academic Engineering Project<br>TY BSc IT Capstone"
        "</div>",
        unsafe_allow_html=True
    )

if overall_stats["total_patients"] == 0:
    st.info("No visits match these filters. Change a selection or reset the analytics filters.")
    st.stop()


# Timestamp for header
current_refresh_time = datetime.now().strftime("%Y-%m-%d %H:%M")


# ==========================================
# 2. OVERVIEW DASHBOARD
# ==========================================
if nav_selection == "Overview Dashboard":
    render_header(
        title="Hospital Waiting Time Command Center",
        subtitle="Explore hospital flow interactively. All cohort metrics and department rankings are recalculated by Apache Spark.",
        refresh_time=current_refresh_time
    )
    st.caption(
        f"Active cohort: {selected_department} · {selected_priority} · {selected_day} "
        f"— {overall_stats['total_patients']} visits"
    )

    # Top KPI Metrics Row
    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

    with kpi_col1:
        render_kpi_card(
            title="Total Patient Visits",
            value=f"{overall_stats['total_patients']}",
            subtext="<span style='color:#0D9488;'>✓ Filtered and aggregated with Spark</span>",
            border_left_color=COLOR_NAVY_PRIMARY
        )

    with kpi_col2:
        render_kpi_card(
            title="Avg Hospital Wait Time",
            value=f"{overall_stats['avg_waiting_time']} <span style='font-size:1.1rem; font-weight:500;'>min</span>",
            subtext=f"Consultation avg: {overall_stats['avg_consultation_duration']} min",
            border_left_color=COLOR_TEAL_PRIMARY
        )

    with kpi_col3:
        render_kpi_card(
            title="Critical Delays (>60m)",
            value=f"{insights['critical_delay_count']}",
            subtext=f"<span style='color:#E11D48;'>▲ {insights['critical_delay_percentage']}% of total cohort</span>",
            border_left_color=COLOR_ROSE_CRITICAL
        )

    with kpi_col4:
        render_kpi_card(
            title="High / Critical Priority",
            value=f"{overall_stats['urgent_cases_count']}",
            subtext=f"<span style='color:#0284C7;'>{overall_stats['urgent_cases_percentage']}% priority caseload</span>",
            border_left_color=COLOR_AMBER_WARNING
        )

    st.markdown("<div style='margin-top: 1.5rem;'></div>", unsafe_allow_html=True)

    # Visualizations Row 1: Department Wait Times & Priority Donut
    c1, c2 = st.columns([3, 2])

    with c1:
        st.markdown("""
        <div class="content-card">
            <h3>Department Waiting Time Benchmark</h3>
            <p class="subtitle">Distributed PySpark calculation of average wait time by clinical department.</p>
        """, unsafe_allow_html=True)

        dept_df = pd.DataFrame(dept_analytics).sort_values("avg_waiting_time", ascending=True)
        fig_dept = go.Figure(go.Bar(
            x=dept_df["avg_waiting_time"],
            y=dept_df["department"],
            orientation="h",
            marker=dict(
                color=[DEPT_COLORS.get(d, COLOR_NAVY_PRIMARY) for d in dept_df["department"]],
                line=dict(width=0)
            ),
            text=dept_df["avg_waiting_time"].apply(lambda v: f" {v:.1f} min"),
            textposition="outside",
            hovertemplate="<b>%{y}</b><br>Avg Wait: %{x:.1f} mins<br>Visits: %{customdata}<extra></extra>",
            customdata=dept_df["total_visits"]
        ))
        layout_dept = get_plotly_layout(height=290)
        layout_dept["xaxis"]["title"] = "Average Waiting Time (Minutes)"
        layout_dept["margin"]["r"] = 55
        fig_dept.update_layout(layout_dept)
        st.plotly_chart(fig_dept, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with c2:
        st.markdown("""
        <div class="content-card">
            <h3>Triage Priority Breakdown</h3>
            <p class="subtitle">Distribution of patient caseload across triage urgency tiers.</p>
        """, unsafe_allow_html=True)

        pri_df = pd.DataFrame(pri_analytics)
        fig_pri = go.Figure(go.Pie(
            labels=pri_df["priority"],
            values=pri_df["total_visits"],
            hole=0.55,
            marker=dict(colors=[PRIORITY_COLORS.get(p, COLOR_NAVY_PRIMARY) for p in pri_df["priority"]]),
            textinfo="label+percent",
            hoverinfo="label+value+percent",
            showlegend=False
        ))
        layout_pri = get_plotly_layout(height=290)
        fig_pri.update_layout(layout_pri)
        st.plotly_chart(fig_pri, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # Visualizations Row 2: Waiting Time Tiers & Hourly Traffic
    c3, c4 = st.columns([1, 1])

    with c3:
        st.markdown("""
        <div class="content-card">
            <h3>Operational Waiting Time Tiers</h3>
            <p class="subtitle">Patient categorization: Short (0-15m), Moderate (16-30m), Long (31-60m), Critical Delay (>60m).</p>
        """, unsafe_allow_html=True)

        tier_df = pd.DataFrame(wait_classes)
        fig_tier = go.Figure(go.Bar(
            x=tier_df["category"],
            y=tier_df["patient_count"],
            marker=dict(color=[CATEGORY_COLORS.get(c, COLOR_TEAL_PRIMARY) for c in tier_df["category"]]),
            text=tier_df.apply(lambda r: f"{r['patient_count']} ({r['percentage']}%)", axis=1),
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Patients: %{y}<br>Tier Avg Wait: %{customdata:.1f} min<extra></extra>",
            customdata=tier_df["avg_waiting_time"]
        ))
        layout_tier = get_plotly_layout(height=280)
        layout_tier["yaxis"]["title"] = "Patient Count"
        layout_tier["margin"]["t"] = 25
        fig_tier.update_layout(layout_tier)
        st.plotly_chart(fig_tier, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with c4:
        st.markdown("""
        <div class="content-card">
            <h3>Hourly Patient Arrival Pattern</h3>
            <p class="subtitle">Hospital arrival influx highlighting peak registration windows (08:00 - 18:00).</p>
        """, unsafe_allow_html=True)

        peak_df = pd.DataFrame(peak_hours)
        fig_peak = go.Figure()
        fig_peak.add_trace(go.Scatter(
            x=peak_df["arrival_hour"],
            y=peak_df["arrival_count"],
            mode="lines+markers",
            name="Arrivals",
            line=dict(color=COLOR_TEAL_PRIMARY, width=2.5),
            fill="tozeroy",
            fillcolor="rgba(13, 148, 136, 0.08)",
            hovertemplate="Hour %{x}:00<br>Arrivals: %{y} patients<extra></extra>"
        ))
        layout_peak = get_plotly_layout(height=280)
        layout_peak["xaxis"]["title"] = "Arrival Hour of Day (00:00 - 23:00)"
        layout_peak["xaxis"]["tickmode"] = "linear"
        layout_peak["xaxis"]["tick0"] = 0
        layout_peak["xaxis"]["dtick"] = 2
        layout_peak["yaxis"]["title"] = "Patient Check-Ins"
        fig_peak.update_layout(layout_peak)
        st.plotly_chart(fig_peak, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # Recent Patient Visits Preview Table
    st.markdown("""
    <div class="content-card">
        <h3>Recent Hospital Visits Preview</h3>
        <p class="subtitle">Latest visits in the selected department, priority, and weekday cohort.</p>
    """, unsafe_allow_html=True)

    if not raw_df.empty:
        preview_cols = [
            "patient_id", "department", "priority", "patient_age",
            "arrival_time", "waiting_time", "queue_length", "consultation_duration", "doctor_id"
        ]
        sample_display = raw_df[preview_cols].tail(10).copy()
        sample_display["waiting_time"] = sample_display["waiting_time"].round(1)
        sample_display["consultation_duration"] = sample_display["consultation_duration"].round(1)
        sample_display.rename(columns={
            "patient_id": "Patient ID",
            "department": "Department",
            "priority": "Priority",
            "patient_age": "Age",
            "arrival_time": "Arrival Time",
            "waiting_time": "Wait (min)",
            "queue_length": "Queue Ahead",
            "consultation_duration": "Consult (min)",
            "doctor_id": "Assigned Doctor"
        }, inplace=True)
        st.dataframe(sample_display, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)


# ==========================================
# 3. DEPARTMENT ANALYTICS
# ==========================================
elif nav_selection == "Department Analytics":
    render_header(
        title="Department Waiting Time & Capacity Analytics",
        subtitle="Department comparative benchmarks, queue rankings, and rule-based staffing suggestions.",
        refresh_time=current_refresh_time
    )

    # Department Key Insights Summary Cards
    hw = insights.get("highest_wait_department", {})
    lw = insights.get("lowest_wait_department", {})

    d_col1, d_col2, d_col3, d_col4 = st.columns(4)
    with d_col1:
        render_kpi_card(
            title="Highest Delay Dept",
            value=hw.get("name", "N/A"),
            subtext=f"<span style='color:#E11D48;'>Avg Wait: {hw.get('avg_wait', 0):.1f} mins (Rank #1)</span>",
            border_left_color=COLOR_ROSE_CRITICAL
        )
    with d_col2:
        render_kpi_card(
            title="Fastest Triage Dept",
            value=lw.get("name", "N/A"),
            subtext=f"<span style='color:#0D9488;'>Avg Wait: {lw.get('avg_wait', 0):.1f} mins (Fast Turnaround)</span>",
            border_left_color=COLOR_TEAL_PRIMARY
        )
    with d_col3:
        busiest_dept = max(dept_analytics, key=lambda x: x["total_visits"])
        render_kpi_card(
            title="Highest Volume Dept",
            value=busiest_dept["department"],
            subtext=f"{busiest_dept['total_visits']} patient visits recorded",
            border_left_color=COLOR_CYAN_ACCENT
        )
    with d_col4:
        longest_consult = max(dept_analytics, key=lambda x: x["avg_consultation_duration"])
        render_kpi_card(
            title="Longest Service Dept",
            value=longest_consult["department"],
            subtext=f"Avg consult: {longest_consult['avg_consultation_duration']:.1f} mins",
            border_left_color=COLOR_NAVY_PRIMARY
        )

    st.markdown("<div style='margin-top: 1.5rem;'></div>", unsafe_allow_html=True)

    # Department Comparison Chart: Wait Time vs Consultation Duration
    st.markdown("""
    <div class="content-card">
        <h3>Wait Time vs Consultation Duration by Department</h3>
        <p class="subtitle">Comparing outpatient waiting friction against actual physician service durations.</p>
    """, unsafe_allow_html=True)

    comp_df = pd.DataFrame(dept_analytics).sort_values("wait_rank")
    fig_comp = go.Figure()
    fig_comp.add_trace(go.Bar(
        name="Average Waiting Time",
        x=comp_df["department"],
        y=comp_df["avg_waiting_time"],
        marker_color=COLOR_NAVY_PRIMARY,
        text=comp_df["avg_waiting_time"].apply(lambda v: f"{v:.1f} m"),
        textposition="outside"
    ))
    fig_comp.add_trace(go.Bar(
        name="Average Consultation Duration",
        x=comp_df["department"],
        y=comp_df["avg_consultation_duration"],
        marker_color=COLOR_TEAL_PRIMARY,
        text=comp_df["avg_consultation_duration"].apply(lambda v: f"{v:.1f} m"),
        textposition="outside"
    ))
    layout_comp = get_plotly_layout(height=350, show_legend=True)
    layout_comp["barmode"] = "group"
    layout_comp["yaxis"]["title"] = "Duration (Minutes)"
    fig_comp.update_layout(layout_comp)
    st.plotly_chart(fig_comp, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

    # Department Ranking Table & Staffing Recommendations
    col_t1, col_t2 = st.columns([3, 2])

    with col_t1:
        st.markdown("""
        <div class="content-card">
            <h3>Department Performance League Table</h3>
            <p class="subtitle">Ranked by PySpark Window dense_rank() over average waiting time.</p>
        """, unsafe_allow_html=True)

        rank_table = comp_df[[
            "wait_rank", "department", "total_visits",
            "avg_waiting_time", "max_waiting_time", "avg_consultation_duration"
        ]].copy()
        rank_table["avg_waiting_time"] = rank_table["avg_waiting_time"].round(1)
        rank_table["max_waiting_time"] = rank_table["max_waiting_time"].round(1)
        rank_table["avg_consultation_duration"] = rank_table["avg_consultation_duration"].round(1)
        rank_table.rename(columns={
            "wait_rank": "Rank",
            "department": "Department",
            "total_visits": "Total Visits",
            "avg_waiting_time": "Avg Wait (m)",
            "max_waiting_time": "Max Wait (m)",
            "avg_consultation_duration": "Avg Consult (m)"
        }, inplace=True)
        st.dataframe(rank_table, use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with col_t2:
        st.markdown("""
        <div class="content-card">
            <h3>Rule-Based Staffing Recommendations</h3>
            <p class="subtitle">Operational actions generated from PySpark threshold evaluations.</p>
        """, unsafe_allow_html=True)

        staff_recs = insights.get("staffing_recommendations", [])
        if staff_recs:
            for r in staff_recs:
                is_crit = r["department"] == "Emergency"
                crit_class = "critical" if is_crit else ""
                st.markdown(f"""
                <div class="rec-card {crit_class}">
                    <div class="rec-title">{r['department']} (Avg Wait: {r['avg_wait']:.1f}m)</div>
                    <div class="rec-desc">{r['reason']}</div>
                    <div class="rec-action">➔ Action: {r['action']}</div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("All departments currently operating within acceptable benchmark thresholds.")

        st.caption(f"Note: {insights['disclaimer']}")
        st.markdown("</div>", unsafe_allow_html=True)


# ==========================================
# 4. PATIENT QUEUE (INTERACTIVE FILTERING)
# ==========================================
elif nav_selection == "Patient Queue":
    render_header(
        title="Live Patient Queue Management",
        subtitle="Search and inspect visit telemetry alongside the Spark-filtered analytics cohort.",
        refresh_time=current_refresh_time
    )

    if raw_df.empty:
        st.warning("No hospital visit records are available for this cohort.")
    else:
        st.markdown("""
        <div class="content-card">
            <h3>Queue Telemetry Filter Console</h3>
            <p class="subtitle">Narrow the active cohort by wait category or search patient and doctor IDs.</p>
        """, unsafe_allow_html=True)

        f_c1, f_c2 = st.columns(2)
        with f_c1:
            wait_tier_opts = ["All Wait Tiers", "Short Wait (0-15m)", "Moderate Wait (16-30m)", "Long Wait (31-60m)", "Critical Delay (>60m)"]
            selected_tier = st.selectbox("Wait Time Category", wait_tier_opts)

        with f_c2:
            search_query = st.text_input("🔍 Search by Patient ID (e.g. P1002) or Doctor ID (e.g. DOC-EM-01):", "")

        # Apply the page-specific filters on top of the Spark-selected cohort.
        filtered = raw_df.copy()

        if selected_tier == "Short Wait (0-15m)":
            filtered = filtered[filtered["waiting_time"] <= 15]
        elif selected_tier == "Moderate Wait (16-30m)":
            filtered = filtered[(filtered["waiting_time"] > 15) & (filtered["waiting_time"] <= 30)]
        elif selected_tier == "Long Wait (31-60m)":
            filtered = filtered[(filtered["waiting_time"] > 30) & (filtered["waiting_time"] <= 60)]
        elif selected_tier == "Critical Delay (>60m)":
            filtered = filtered[filtered["waiting_time"] > 60]

        if search_query.strip():
            sq = search_query.strip().lower()
            filtered = filtered[
                filtered["patient_id"].str.lower().str.contains(sq) |
                filtered["doctor_id"].str.lower().str.contains(sq)
            ]

        # Filtered Count Indicator
        st.markdown(f"**Displaying {len(filtered)} of {len(raw_df)} visits in the active Spark cohort.**")

        if filtered.empty:
            st.info("ℹ️ No patient visits match the selected filter criteria. Try selecting 'All Departments' or resetting the search filter.")
        else:
            # Table Display
            display_cols = [
                "patient_id", "department", "priority", "patient_age",
                "arrival_time", "waiting_time", "queue_length",
                "consultation_duration", "doctor_id", "day_of_week"
            ]
            out_table = filtered[display_cols].copy()
            out_table["waiting_time"] = out_table["waiting_time"].round(1)
            out_table["consultation_duration"] = out_table["consultation_duration"].round(1)
            out_table.rename(columns={
                "patient_id": "Patient ID",
                "department": "Department",
                "priority": "Priority",
                "patient_age": "Age",
                "arrival_time": "Arrival Time",
                "waiting_time": "Wait (min)",
                "queue_length": "Queue Length",
                "consultation_duration": "Consult Time (min)",
                "doctor_id": "Doctor ID",
                "day_of_week": "Day"
            }, inplace=True)

            st.dataframe(
                out_table,
                use_container_width=True,
                hide_index=True,
                height=420
            )
        st.markdown("</div>", unsafe_allow_html=True)


# ==========================================
# 5. WAITING TIME ANALYSIS
# ==========================================
elif nav_selection == "Waiting Time Analysis":
    render_header(
        title="In-Depth Waiting Time & Queue Dynamics",
        subtitle="Operational delay breakdown, peak influx windows, and weekly traffic trends.",
        refresh_time=current_refresh_time
    )

    # 4 Waiting Time Tier Metric Cards
    tier_cols = st.columns(4)
    tier_colors = [COLOR_TEAL_PRIMARY, COLOR_CYAN_ACCENT, COLOR_AMBER_WARNING, COLOR_ROSE_CRITICAL]

    for idx, t in enumerate(wait_classes):
        with tier_cols[idx]:
            render_kpi_card(
                title=t["category"],
                value=f"{t['patient_count']} <span style='font-size:1.1rem; font-weight:500;'>({t['percentage']}%)</span>",
                subtext=f"Tier Avg wait: {t['avg_waiting_time']:.1f} mins",
                border_left_color=tier_colors[idx]
            )

    st.markdown("<div style='margin-top: 1.5rem;'></div>", unsafe_allow_html=True)

    # Department-wise wait distribution
    col_w1, col_w2 = st.columns([1, 1])

    with col_w1:
        st.markdown("""
        <div class="content-card">
            <h3>Department Delay Range Comparison</h3>
            <p class="subtitle">Comparing minimum, average, and peak wait times across departments.</p>
        """, unsafe_allow_html=True)

        d_df = pd.DataFrame(dept_analytics)
        fig_d_range = go.Figure()
        fig_d_range.add_trace(go.Bar(
            name="Minimum Wait",
            x=d_df["department"],
            y=d_df["min_waiting_time"],
            marker_color="#CBD5E1"
        ))
        fig_d_range.add_trace(go.Bar(
            name="Average Wait",
            x=d_df["department"],
            y=d_df["avg_waiting_time"],
            marker_color=COLOR_NAVY_PRIMARY
        ))
        fig_d_range.add_trace(go.Bar(
            name="Maximum Wait",
            x=d_df["department"],
            y=d_df["max_waiting_time"],
            marker_color=COLOR_ROSE_CRITICAL
        ))
        layout_d_range = get_plotly_layout(height=320, show_legend=True)
        layout_d_range["barmode"] = "group"
        layout_d_range["yaxis"]["title"] = "Wait Time (Minutes)"
        fig_d_range.update_layout(layout_d_range)
        st.plotly_chart(fig_d_range, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with col_w2:
        st.markdown("""
        <div class="content-card">
            <h3>Weekly Traffic & Waiting Profile</h3>
            <p class="subtitle">Daily outpatient influx and average wait fluctuations (Monday to Sunday).</p>
        """, unsafe_allow_html=True)

        day_df = pd.DataFrame(day_stats)
        fig_day = go.Figure()
        fig_day.add_trace(go.Bar(
            name="Patient Visits",
            x=day_df["day_of_week"],
            y=day_df["total_visits"],
            marker_color=COLOR_TEAL_PRIMARY,
            yaxis="y"
        ))
        fig_day.add_trace(go.Scatter(
            name="Avg Wait (min)",
            x=day_df["day_of_week"],
            y=day_df["avg_waiting_time"],
            mode="lines+markers",
            line=dict(color=COLOR_ROSE_CRITICAL, width=3),
            yaxis="y2"
        ))
        layout_day = get_plotly_layout(height=320, show_legend=True)
        layout_day["yaxis"]["title"] = "Total Visits"
        layout_day["yaxis2"] = dict(
            title="Avg Wait (min)",
            overlaying="y",
            side="right",
            showgrid=False
        )
        fig_day.update_layout(layout_day)
        st.plotly_chart(fig_day, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # Peak Hour Queue Density Chart
    st.markdown("""
    <div class="content-card">
        <h3>Hourly Queue Density and Average Queue Length</h3>
        <p class="subtitle">Correlation between check-in arrival hour, waiting duration, and queue depth ahead.</p>
    """, unsafe_allow_html=True)

    p_df = pd.DataFrame(peak_hours)
    fig_p_dense = go.Figure()
    fig_p_dense.add_trace(go.Bar(
        name="Arrivals",
        x=p_df["hour_formatted"],
        y=p_df["arrival_count"],
        marker_color="#94A3B8"
    ))
    fig_p_dense.add_trace(go.Scatter(
        name="Avg Wait Time (m)",
        x=p_df["hour_formatted"],
        y=p_df["avg_waiting_time"],
        mode="lines+markers",
        line=dict(color=COLOR_NAVY_PRIMARY, width=2.5),
        yaxis="y2"
    ))
    layout_p_dense = get_plotly_layout(height=340, show_legend=True)
    layout_p_dense["xaxis"]["tickangle"] = -45
    layout_p_dense["yaxis"]["title"] = "Patient Arrivals"
    layout_p_dense["yaxis2"] = dict(
        title="Avg Wait (Minutes)",
        overlaying="y",
        side="right",
        showgrid=False
    )
    fig_p_dense.update_layout(layout_p_dense)
    st.plotly_chart(fig_p_dense, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)


# ==========================================
# 6. REPORTS & DOWNLOADS
# ==========================================
elif nav_selection == "Reports & Downloads":
    render_header(
        title="Hospital Reports & Analytics Export",
        subtitle="One-click CSV exports of PySpark analytics and downloadable executive PDF operational report.",
        refresh_time=current_refresh_time
    )

    st.markdown("""
    <div class="content-card">
        <h3>Operational Report Downloads</h3>
        <p class="subtitle">Export complete raw dataset or aggregated analytical metrics for hospital administrative reporting.</p>
    """, unsafe_allow_html=True)

    r_c1, r_c2 = st.columns([1, 1])

    with r_c1:
        st.markdown("#### 📄 Executive PDF Operational Report")
        st.write("A publication-ready summary report containing executive KPIs, PySpark department rankings, waiting-time tiers, and rule-based staffing recommendations.")
        
        pdf_bytes = generate_executive_pdf_report(analytics_data)
        st.download_button(
            label="📥 Download Executive Summary PDF Report",
            data=pdf_bytes,
            file_name=f"MediQueue_Executive_Report_{datetime.now().strftime('%Y%m%d')}.pdf",
            mime="application/pdf",
            use_container_width=True
        )

    with r_c2:
        st.markdown("#### 📊 Raw Patient Telemetry CSV")
        st.write(f"Complete visit dataset ({len(raw_visits)} outpatient visits), including arrival times, consultation durations, queue depths, and priorities.")

        raw_csv = convert_dict_list_to_csv(raw_visits)
        st.download_button(
            label="📥 Download Full Visits Dataset (CSV)",
            data=raw_csv,
            file_name="mediqueue_hospital_visits_200.csv",
            mime="text/csv",
            use_container_width=True
        )

    st.markdown("---")
    st.markdown("#### ⚡ PySpark Aggregated Metrics Exports")
    st.write("Specialized CSV exports computed directly by the PySpark DataFrame analytics pipeline:")

    m_c1, m_c2, m_c3 = st.columns(3)

    with m_c1:
        dept_csv = convert_dict_list_to_csv(dept_analytics)
        st.download_button(
            label="📥 Department Rankings (CSV)",
            data=dept_csv,
            file_name="pyspark_department_rankings.csv",
            mime="text/csv",
            use_container_width=True
        )

    with m_c2:
        pri_csv = convert_dict_list_to_csv(pri_analytics)
        st.download_button(
            label="📥 Priority Tiers (CSV)",
            data=pri_csv,
            file_name="pyspark_priority_analytics.csv",
            mime="text/csv",
            use_container_width=True
        )

    with m_c3:
        tier_csv = convert_dict_list_to_csv(wait_classes)
        st.download_button(
            label="📥 Wait Time Tiers (CSV)",
            data=tier_csv,
            file_name="pyspark_wait_categories.csv",
            mime="text/csv",
            use_container_width=True
        )

    st.markdown("</div>", unsafe_allow_html=True)


# ==========================================
# 7. ABOUT MEDIQUEUE (COLLEGE VIVA HIGHLIGHTS)
# ==========================================
elif nav_selection == "About MediQueue":
    render_header(
        title="About MediQueue Architecture & Viva Highlights",
        subtitle="System design, Big Data motivation, and college demonstration presentation notes.",
        refresh_time=current_refresh_time
    )

    st.markdown("""
    <div class="content-card">
        <h3>🎓 Project Architecture & College Demonstration Notes</h3>
        <p class="subtitle">Designed specifically for a TY BSc IT final year engineering project.</p>

        <h4>1. Why PySpark for Hospital Waiting Time Analysis?</h4>
        <p>In municipal and private hospital systems, thousands of patients arrive daily across emergency, outpatient, and specialty clinics.
        Demonstrating <b>Apache PySpark</b> illustrates how distributed computing frameworks process, aggregate, and rank large-scale queue telemetry
        with windowed ranking operations that scale to millions of records across multi-hospital networks.</p>

        <h4>2. Architecture & Separation of Concerns</h4>
        <ul>
            <li><b>Transactional Layer (SQLite3):</b> Manages atomic visit insertions, patient queue tracking, status updates, and constraint validations.</li>
            <li><b>Analytical Layer (PySpark 3.5.9 local[1]):</b> Performs batch aggregations, windowed department rankings, waiting-time classifications, and peak-hour profiling.</li>
            <li><b>Presentation Layer (Streamlit + Plotly):</b> Delivers a reactive, hospital command center interface with responsive Plotly visualizations.</li>
        </ul>

        <h4>3. Why local[1] on an 8 GB RAM Laptop?</h4>
        <p><code>local[1]</code> restricts PySpark to a single local thread, avoiding multi-process CPU thrashing, limiting memory allocation to 1 GB,
        and setting shuffle partitions to 1. This provides authentic distributed DataFrame execution without crashing standard student laptops.</p>

        <h4>4. Strict Academic Boundary Note</h4>
        <p>MediQueue is strictly an <b>operational logistics and queue optimization system</b>. It does not perform clinical diagnosis, triage diagnosis, or automated medical prescribing.
        All 200 visit records are 100% synthetic and contain zero real patient data.</p>

        <hr style="margin: 1.5rem 0; border: none; border-top: 1px solid #E2E8F0;">

        <h4>🎯 Viva Examiner Q&A Cheat Sheet</h4>
        <div style="background:#F8FAFC; padding:1.2rem; border-radius:8px; border:1px solid #E2E8F0; margin-top:0.75rem;">
            <p><b>Q1: How does MediQueue identify hospital bottlenecks?</b><br>
            <i>Answer:</i> The PySpark engine computes department average waiting times and applies a Window dense_rank() function. General Medicine (51.5 min) and Cardiology (51.4 min) rank highest in wait times, triggering rule-based staffing suggestions.</p>
            
            <p><b>Q2: How does patient triage priority affect waiting time?</b><br>
            <i>Answer:</i> Critical patients wait an average of 5.0 minutes, while Low priority patients wait an average of 77.8 minutes. This proves fast-tracking for urgent care operates accurately in the data model.</p>
            
            <p><b>Q3: What happens during peak arrival windows?</b><br>
            <i>Answer:</i> Between 10:00 and 13:00, arrival volume peaks at 27 check-ins per hour, driving queue depths up to 11.8 patients ahead and increasing average wait times above 50 minutes.</p>
            
            <p><b>Q4: How does the system avoid excessive memory consumption?</b><br>
            <i>Answer:</i> By combining PySpark <code>local[1]</code> with Streamlit's <code>@st.cache_data</code>, analytical aggregations execute once and cache in memory, eliminating redundant Spark session spin-ups on user interactions.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
