"""
MediQueue - PySpark Analytics Demo Runner
Executes the distributed analytics engine and presents a structured terminal report
of hospital waiting times, department rankings, priority profiles, peak hours, and operational insights.
"""

import sys

# Ensure UTF-8 output encoding on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from analytics import (
    get_spark_session,
    stop_spark_session,
    load_hospital_data,
    MediQueueAnalytics,
    run_full_analytics
)

def print_separator(title: str = "", width: int = 75, char: str = "="):
    if title:
        padding = max(0, (width - len(title) - 2) // 2)
        print(f"\n{char * padding} {title} {char * max(0, width - len(title) - 2 - padding)}")
    else:
        print(char * width)

def run_demo():
    print_separator("[ MediQueue - PySpark Analytics Engine (local[1]) ]")
    print("Initializing Spark session and loading hospital visit dataset...")
    
    spark = get_spark_session()
    df = load_hospital_data(spark)
    analytics = MediQueueAnalytics(df)
    
    # 1. Overall Statistics
    print_separator("1. OVERALL HOSPITAL OPERATIONAL STATISTICS", char="-")
    stats = analytics.compute_overall_stats()
    print(f"  * Total Patient Visits        : {stats['total_patients']}")
    print(f"  * Average Waiting Time        : {stats['avg_waiting_time']} minutes")
    print(f"  * Minimum Waiting Time        : {stats['min_waiting_time']} minutes")
    print(f"  * Maximum Waiting Time        : {stats['max_waiting_time']} minutes")
    print(f"  * Average Consultation Time   : {stats['avg_consultation_duration']} minutes")
    print(f"  * High / Critical Cases       : {stats['urgent_cases_count']} ({stats['urgent_cases_percentage']}%)")

    # 2. Department Analytics & Ranking
    print_separator("2. DEPARTMENT WAITING-TIME RANKING", char="-")
    dept_stats = analytics.compute_department_analytics()
    header_dept = f"{'Rank':<6}{'Department':<20}{'Visits':<10}{'Avg Wait (m)':<15}{'Max Wait (m)':<15}{'Avg Consult (m)':<16}"
    print(header_dept)
    print("-" * len(header_dept))
    for d in dept_stats:
        print(f"{d['wait_rank']:<6}{d['department']:<20}{d['total_visits']:<10}{d['avg_waiting_time']:<15.2f}{d['max_waiting_time']:<15.2f}{d['avg_consultation_duration']:<16.2f}")

    # 3. Priority Breakdown
    print_separator("3. CLINICAL PRIORITY BREAKDOWN", char="-")
    pri_stats = analytics.compute_priority_analytics()
    header_pri = f"{'Priority':<12}{'Visits':<10}{'Avg Wait (m)':<15}{'Min Wait (m)':<15}{'Max Wait (m)':<15}{'Avg Consult (m)':<16}"
    print(header_pri)
    print("-" * len(header_pri))
    for p in pri_stats:
        print(f"{p['priority']:<12}{p['total_visits']:<10}{p['avg_waiting_time']:<15.2f}{p['min_waiting_time']:<15.2f}{p['max_waiting_time']:<15.2f}{p['avg_consultation_duration']:<16.2f}")

    # 4. Waiting Time Categories
    print_separator("4. WAITING TIME CLASSIFICATION TIERS", char="-")
    classes = analytics.compute_waiting_time_classification()
    header_cls = f"{'Category':<26}{'Patients':<12}{'Percentage':<14}{'Avg Wait in Tier (m)':<20}"
    print(header_cls)
    print("-" * len(header_cls))
    for c in classes:
        print(f"{c['category']:<26}{c['patient_count']:<12}{str(c['percentage']) + '%':<14}{c['avg_waiting_time']:<20.2f}")

    # 5. Peak Hours
    print_separator("5. PATIENT ARRIVALS & PEAK HOUR ANALYSIS", char="-")
    peak_hours = analytics.compute_peak_hour_analysis()
    header_peak = f"{'Arrival Hour':<18}{'Patient Arrivals':<18}{'Avg Wait (m)':<15}{'Avg Queue Ahead':<15}"
    print(header_peak)
    print("-" * len(header_peak))
    for h in peak_hours:
        print(f"{h['hour_formatted']:<18}{h['arrival_count']:<18}{h['avg_waiting_time']:<15.2f}{h['avg_queue_depth']:<15.1f}")

    # 6. Day-wise Traffic
    print_separator("6. DAY-WISE TRAFFIC & WAIT PROFILE", char="-")
    days = analytics.compute_day_wise_analysis()
    header_day = f"{'Day of Week':<15}{'Total Visits':<15}{'Avg Wait (m)':<15}{'Avg Consult (m)':<15}"
    print(header_day)
    print("-" * len(header_day))
    for d in days:
        print(f"{d['day_of_week']:<15}{d['total_visits']:<15}{d['avg_waiting_time']:<15.2f}{d['avg_consultation_duration']:<15.2f}")

    # 7. Operational Rule-Based Insights
    print_separator("7. OPERATIONAL RULE-BASED INSIGHTS", char="-")
    insights = analytics.generate_insights()
    hw = insights["highest_wait_department"]
    lw = insights["lowest_wait_department"]
    bh = insights["busiest_arrival_hour"]
    tp = insights["top_priority_category"]

    if hw:
        print(f"  [!] Highest Delay Department  : {hw['name']} (Rank #{hw['rank']}, Avg Wait: {hw['avg_wait']} mins)")
    if lw:
        print(f"  [*] Fastest Service Department : {lw['name']} (Rank #{lw['rank']}, Avg Wait: {lw['avg_wait']} mins)")
    if bh:
        print(f"  [>] Peak Arrival Window        : {bh['hour']} ({bh['patient_arrivals']} arrivals, Avg Wait: {bh['avg_waiting_time']} mins)")
    print(f"  [!] Critical Delays (> 60m)    : {insights['critical_delay_count']} patients ({insights['critical_delay_percentage']}%)")
    if tp:
        print(f"  [*] Highest Volume Priority    : {tp['priority']} ({tp['patient_count']} patients, Avg Wait: {tp['avg_wait']} mins)")

    print("\n  [+] Rule-Based Staffing Recommendations:")
    for idx, rec in enumerate(insights.get("staffing_recommendations", []), start=1):
        print(f"     [{idx}] {rec['department']}: {rec['reason']}")
        print(f"         -> Recommended Action: {rec['action']}")

    print(f"\n  [i] Academic Boundary Note: {insights['disclaimer']}")
    print_separator(char="=")

    # Clean shutdown of Spark session
    stop_spark_session()
    print("Spark session stopped cleanly.")

if __name__ == "__main__":
    run_demo()
