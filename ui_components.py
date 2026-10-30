"""
MediQueue - UI Components and Styling
Provides custom CSS, visual styling constants, KPI card renderers,
and consistent Plotly chart theme templates for the Hospital Command Center layout.
"""

import streamlit as st
import plotly.graph_objects as go

# Color Palette Constants (Navy & Teal Hospital Operations Command Theme)
COLOR_NAVY_DARK = "#0F172A"       # Primary text & deep navy
COLOR_NAVY_PRIMARY = "#1E3A8A"    # Primary brand & bar charts
COLOR_TEAL_PRIMARY = "#0D9488"    # Accent teal
COLOR_TEAL_LIGHT = "#14B8A6"      # Secondary teal
COLOR_CYAN_ACCENT = "#0284C7"     # Informational cyan
COLOR_AMBER_WARNING = "#D97706"   # Medium priority / warning
COLOR_ROSE_CRITICAL = "#E11D48"   # Critical priority / delay
COLOR_GRAY_MUTED = "#64748B"      # Subtitles and axis labels
COLOR_CARD_BG = "#FFFFFF"         # Card background
COLOR_BORDER = "#E2E8F0"          # Card border

# Department palette
DEPT_COLORS = {
    "Emergency": COLOR_ROSE_CRITICAL,
    "General Medicine": COLOR_NAVY_PRIMARY,
    "Cardiology": COLOR_CYAN_ACCENT,
    "Orthopedics": COLOR_AMBER_WARNING,
    "Pediatrics": COLOR_TEAL_PRIMARY
}

# Priority palette
PRIORITY_COLORS = {
    "Critical": COLOR_ROSE_CRITICAL,
    "High": COLOR_AMBER_WARNING,
    "Medium": COLOR_CYAN_ACCENT,
    "Low": COLOR_TEAL_PRIMARY
}

# Wait Category palette
CATEGORY_COLORS = {
    "Short Wait (0-15m)": COLOR_TEAL_PRIMARY,
    "Moderate Wait (16-30m)": COLOR_CYAN_ACCENT,
    "Long Wait (31-60m)": COLOR_AMBER_WARNING,
    "Critical Delay (>60m)": COLOR_ROSE_CRITICAL
}


def apply_custom_css():
    """Inject modern, clean healthcare command center CSS into Streamlit."""
    st.markdown("""
    <style>
        /* Base typography & page resets */
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');
        
        html, body, [class*="css"] {
            font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif;
        }

        .main .block-container {
            padding-top: 1.5rem;
            padding-bottom: 2.5rem;
            padding-left: 2rem;
            padding-right: 2rem;
            max-width: 1440px;
        }

        /* Top Header Banner */
        .dashboard-header {
            background: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 12px;
            padding: 1.25rem 1.75rem;
            margin-bottom: 1.5rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.03);
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 1rem;
        }
        .header-title-box h1 {
            font-size: 1.65rem;
            font-weight: 700;
            color: #0F172A;
            margin: 0;
            line-height: 1.2;
        }
        .header-title-box p {
            font-size: 0.90rem;
            color: #64748B;
            margin: 0.25rem 0 0 0;
        }
        .header-badge-box {
            display: flex;
            gap: 0.6rem;
            align-items: center;
            flex-wrap: wrap;
        }
        .status-pill {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 5px 12px;
            border-radius: 20px;
            font-size: 0.78rem;
            font-weight: 600;
            background: #F0FDF4;
            color: #166534;
            border: 1px solid #BBF7D0;
        }
        .spark-pill {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 5px 12px;
            border-radius: 20px;
            font-size: 0.78rem;
            font-weight: 600;
            background: #EFF6FF;
            color: #1E40AF;
            border: 1px solid #BFDBFE;
        }
        .time-pill {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 5px 12px;
            border-radius: 20px;
            font-size: 0.78rem;
            font-weight: 600;
            background: #F8FAFC;
            color: #475569;
            border: 1px solid #CBD5E1;
        }
        .live-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background-color: #22C55E;
        }
        .spark-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background-color: #3B82F6;
        }

        /* KPI Metric Cards */
        .kpi-card {
            background: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 12px;
            padding: 1.25rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.02);
            position: relative;
            min-height: 125px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            transition: transform 0.15s ease, box-shadow 0.15s ease;
        }
        .kpi-card:hover {
            box-shadow: 0 4px 6px -1px rgba(0,0,0,0.07), 0 2px 4px -1px rgba(0,0,0,0.04);
        }
        .kpi-title {
            font-size: 0.78rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: #64748B;
            margin-bottom: 0.35rem;
        }
        .kpi-value {
            font-size: 1.85rem;
            font-weight: 700;
            color: #0F172A;
            line-height: 1.15;
            margin-bottom: 0.35rem;
        }
        .kpi-subtext {
            font-size: 0.80rem;
            color: #64748B;
            display: flex;
            align-items: center;
            gap: 4px;
        }

        /* Section Container Box */
        .content-card {
            background: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 12px;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.03);
        }
        .content-card h3 {
            font-size: 1.1rem;
            font-weight: 600;
            color: #0F172A;
            margin-top: 0;
            margin-bottom: 0.25rem;
        }
        .content-card p.subtitle {
            font-size: 0.85rem;
            color: #64748B;
            margin-bottom: 1.25rem;
        }

        /* Recommendation cards */
        .rec-card {
            background: #F8FAFC;
            border: 1px solid #E2E8F0;
            border-left: 4px solid #0D9488;
            border-radius: 8px;
            padding: 1rem 1.25rem;
            margin-bottom: 0.75rem;
        }
        .rec-card.critical {
            border-left-color: #E11D48;
            background: #FFF1F2;
        }
        .rec-title {
            font-weight: 600;
            font-size: 0.92rem;
            color: #0F172A;
            margin-bottom: 0.25rem;
        }
        .rec-desc {
            font-size: 0.85rem;
            color: #475569;
            margin-bottom: 0.4rem;
        }
        .rec-action {
            font-size: 0.82rem;
            font-weight: 600;
            color: #0D9488;
        }
        .rec-card.critical .rec-action {
            color: #BE123C;
        }

        /* Sidebar Styling */
        section[data-testid="stSidebar"] {
            background-color: #F8FAFC;
            border-right: 1px solid #E2E8F0;
        }
        .sidebar-brand {
            padding: 0.5rem 0 1rem 0;
            border-bottom: 1px solid #E2E8F0;
            margin-bottom: 1rem;
        }
        .sidebar-brand h2 {
            font-size: 1.35rem;
            font-weight: 700;
            color: #1E3A8A;
            margin: 0;
        }
        .sidebar-brand p {
            font-size: 0.82rem;
            color: #64748B;
            margin: 2px 0 0 0;
        }
        .engine-status-box {
            background: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 8px;
            padding: 0.85rem;
            margin-top: 1rem;
            margin-bottom: 1rem;
        }
        .engine-status-title {
            font-size: 0.72rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: #64748B;
            margin-bottom: 0.25rem;
        }
        .engine-status-name {
            font-size: 0.85rem;
            font-weight: 600;
            color: #0F172A;
            display: flex;
            align-items: center;
            gap: 6px;
        }

    </style>
    """, unsafe_allow_html=True)


def render_header(title: str, subtitle: str, refresh_time: str, total_visits: int):
    """Render the top dashboard banner with status, engine, and refresh timestamp."""
    st.markdown(f"""
    <div class="dashboard-header">
        <div class="header-title-box">
            <h1>{title}</h1>
            <p>{subtitle}</p>
        </div>
        <div class="header-badge-box">
            <div class="status-pill">
                <span class="live-dot"></span> {total_visits} Visits
            </div>
            <div class="spark-pill">
                <span class="spark-dot"></span> PySpark 3.5.9 local[1]
            </div>
            <div class="time-pill">
                🕒 Synced: {refresh_time}
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_kpi_card(title: str, value: str, subtext: str, border_left_color: str = COLOR_NAVY_PRIMARY):
    """Render a styled KPI metric card with consistent layout and sizing."""
    st.markdown(f"""
    <div class="kpi-card" style="border-top: 3px solid {border_left_color};">
        <div>
            <div class="kpi-title">{title}</div>
            <div class="kpi-value">{value}</div>
        </div>
        <div class="kpi-subtext">{subtext}</div>
    </div>
    """, unsafe_allow_html=True)


def get_plotly_layout(height: int = 340, show_legend: bool = False) -> dict:
    """Return consistent, clean Plotly layout parameters."""
    return dict(
        height=height,
        margin=dict(l=20, r=20, t=30, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Plus Jakarta Sans, sans-serif", color="#334155", size=12),
        showlegend=show_legend,
        xaxis=dict(
            showgrid=True,
            gridcolor="#F1F5F9",
            zeroline=False,
            showline=True,
            linecolor="#E2E8F0"
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="#F1F5F9",
            zeroline=False,
            showline=True,
            linecolor="#E2E8F0"
        )
    )
