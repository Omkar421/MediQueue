"""
MediQueue - Dashboard Helper Functions
Provides cached data access to the PySpark analytics engine, raw visit records from SQLite,
CSV export transformations, and PDF executive summary generation via ReportLab.
"""

import io
import csv
import pandas as pd
import streamlit as st
from datetime import datetime
from typing import Dict, List, Any

from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors

import database
from analytics import run_full_analytics


@st.cache_data(show_spinner="Executing PySpark Analytics Engine (local[1])...")
def get_cached_analytics(
    csv_path: str = database.DEFAULT_DB_PATH,
    department: str = "",
    priority: str = "",
    day_of_week: str = ""
) -> Dict[str, Any]:
    """
    Run and cache Spark aggregations over SQLite visits for the selected filters.
    """
    return run_full_analytics(
        source_path=csv_path,
        department=department or None,
        priority=priority or None,
        day_of_week=day_of_week or None
    )


@st.cache_data(show_spinner="Fetching patient queue records from SQLite...")
def get_cached_patient_visits(
    csv_path: str = database.DEFAULT_CSV_PATH,
    db_path: str = database.DEFAULT_DB_PATH
) -> List[Dict[str, Any]]:
    """
    Fetch patient visits from SQLite, importing the bundled CSV if the database is empty.
    """
    visits = database.get_all_visits(db_path=db_path)
    if not visits:
        database.import_csv(csv_path=csv_path, db_path=db_path)
        visits = database.get_all_visits(db_path=db_path)
    return visits


def convert_dict_list_to_csv(data: List[Dict[str, Any]]) -> str:
    """Convert a list of dictionaries into a CSV formatted string."""
    if not data:
        return ""
    df = pd.DataFrame(data)
    return df.to_csv(index=False)


def generate_executive_pdf_report(analytics: Dict[str, Any]) -> bytes:
    """
    Generate an executive PDF operational report of hospital wait times
    using ReportLab (Python standard dependencies).
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=40,
        leftMargin=40,
        topMargin=40,
        bottomMargin=40
    )
    
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'DocTitle',
        parent=styles['Heading1'],
        fontName='Helvetica-Bold',
        fontSize=20,
        leading=24,
        textColor=colors.HexColor('#0F172A'),
        spaceAfter=6
    )
    subtitle_style = ParagraphStyle(
        'DocSubtitle',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=10,
        leading=14,
        textColor=colors.HexColor('#64748B'),
        spaceAfter=15
    )
    section_style = ParagraphStyle(
        'SectionHeader',
        parent=styles['Heading2'],
        fontName='Helvetica-Bold',
        fontSize=13,
        leading=16,
        textColor=colors.HexColor('#1E3A8A'),
        spaceBefore=12,
        spaceAfter=6
    )
    body_style = ParagraphStyle(
        'DocBody',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=9,
        leading=13,
        textColor=colors.HexColor('#334155')
    )

    story = []
    overall = analytics.get("overall_stats", {})
    
    # Header
    story.append(Paragraph("MediQueue - Hospital Waiting Time Executive Report", title_style))
    report_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    story.append(Paragraph(
        f"Generated on: {report_date} | Engine: Apache Spark 3.5.9 (local[1]) | "
        f"Dataset: {overall.get('total_patients', 0)} visits",
        subtitle_style
    ))
    story.append(Spacer(1, 10))
    # Overall KPIs
    # Overall KPIs
    story.append(Paragraph("1. Overall Hospital Performance KPIs", section_style))
    kpi_data = [
        ["Metric", "Value", "Operational Significance"],
        ["Total Outpatient Visits", str(overall.get("total_patients", 0)), "Full dataset coverage"],
        ["Average Waiting Time", f"{overall.get('avg_waiting_time', 0.0)} mins", "Hospital benchmark across all departments"],
        ["Minimum Waiting Time", f"{overall.get('min_waiting_time', 0.0)} mins", "Fastest triage turnaround (Emergency/Critical)"],
        ["Maximum Waiting Time", f"{overall.get('max_waiting_time', 0.0)} mins", "Peak queue delay recorded"],
        ["Average Consultation Time", f"{overall.get('avg_consultation_duration', 0.0)} mins", "Doctor service duration average"],
        ["Urgent Cases (High/Critical)", f"{overall.get('urgent_cases_count', 0)} ({overall.get('urgent_cases_percentage', 0.0)}%)", "Priority caseload requiring immediate beds"]
    ]
    t_kpi = Table(kpi_data, colWidths=[160, 110, 260])
    t_kpi.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1E3A8A')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F8FAFC')),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#CBD5E1')),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 8.5),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F8FAFC')]),
    ]))
    story.append(t_kpi)
    story.append(Spacer(1, 12))

    # Department Rankings
    dept_list = analytics.get("department_analytics", [])
    story.append(Paragraph("2. Department Waiting Time Rankings (PySpark Window Function)", section_style))
    dept_table_data = [["Rank", "Department", "Visits", "Avg Wait (m)", "Max Wait (m)", "Avg Consult (m)"]]
    for d in dept_list:
        dept_table_data.append([
            str(d["wait_rank"]),
            d["department"],
            str(d["total_visits"]),
            f"{d['avg_waiting_time']:.1f}",
            f"{d['max_waiting_time']:.1f}",
            f"{d['avg_consultation_duration']:.1f}"
        ])
    t_dept = Table(dept_table_data, colWidths=[40, 140, 70, 90, 90, 100])
    t_dept.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0D9488')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#CBD5E1')),
        ('FONTSIZE', (0, 1), (-1, -1), 8.5),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F0FDFA')]),
    ]))
    story.append(t_dept)
    story.append(Spacer(1, 12))

    # Waiting Time Tiers
    tiers = analytics.get("waiting_time_classification", [])
    story.append(Paragraph("3. Operational Waiting Time Classification Tiers", section_style))
    tier_data = [["Category", "Patients", "Percentage", "Avg Wait in Tier (m)"]]
    for t in tiers:
        tier_data.append([
            t["category"],
            str(t["patient_count"]),
            f"{t['percentage']}%",
            f"{t['avg_waiting_time']:.1f}"
        ])
    t_tier = Table(tier_data, colWidths=[180, 100, 100, 150])
    t_tier.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0F172A')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#CBD5E1')),
        ('FONTSIZE', (0, 1), (-1, -1), 8.5),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F8FAFC')]),
    ]))
    story.append(t_tier)
    story.append(Spacer(1, 12))

    # Insights & Recommendations
    insights = analytics.get("insights", {})
    story.append(Paragraph("4. Rule-Based Operational Staffing Recommendations", section_style))
    recs = insights.get("staffing_recommendations", [])
    if recs:
        for idx, r in enumerate(recs, start=1):
            story.append(Paragraph(f"<b>[{idx}] {r['department']}:</b> {r['reason']}", body_style))
            story.append(Paragraph(f"<i>Action:</i> {r['action']}", body_style))
            story.append(Spacer(1, 4))
    else:
        story.append(Paragraph("All departments operating within standard thresholds.", body_style))
        
    story.append(Spacer(1, 10))
    story.append(Paragraph("<b>Note:</b> These insights are descriptive analytics and rule-based operational recommendations derived from PySpark aggregations, not machine-learning predictions.", subtitle_style))

    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes
