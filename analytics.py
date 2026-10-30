"""
MediQueue - PySpark Analytics Engine
Hospital Waiting Time Analyzer.
Provides distributed DataFrame-based operational metrics, department rankings,
priority comparisons, wait-time classifications, peak-hour profiling, and rule-based insights.
Configured for lightweight local execution on student laptops (local[1], 1 GB RAM).
"""

import os
import sys
from typing import Dict, List, Optional, Any, Tuple

# Ensure PySpark workers match the exact Python executable of the active runtime
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# Constants & Defaults
DEFAULT_CSV_PATH = os.path.join("data", "hospital_visits.csv")
DEFAULT_DB_PATH = os.path.join("data", "mediqueue.db")

HOSPITAL_VISITS_SCHEMA = StructType([
    StructField("patient_id", StringType(), False),
    StructField("department", StringType(), False),
    StructField("arrival_time", StringType(), False),
    StructField("consultation_start", StringType(), False),
    StructField("consultation_end", StringType(), False),
    StructField("doctor_id", StringType(), False),
    StructField("priority", StringType(), False),
    StructField("patient_age", IntegerType(), False),
    StructField("queue_length", IntegerType(), False),
    StructField("waiting_time", DoubleType(), False),
    StructField("consultation_duration", DoubleType(), False),
    StructField("day_of_week", StringType(), False)
])

# Singleton SparkSession reference
_spark_session: Optional[SparkSession] = None


def get_spark_session() -> SparkSession:
    """
    Get or initialize a lightweight, singleton SparkSession configured for local execution.
    - Master: local[1] (single worker thread, minimal memory footprint)
    - App Name: MediQueue Analytics
    - Driver Memory: 1g
    - UI disabled to conserve system ports and RAM
    - Shuffle partitions: 1 (optimized for small, local analytical workloads)
    """
    global _spark_session
    if _spark_session is None or _spark_session._jsc is None:
        _spark_session = (
            SparkSession.builder
            .master("local[1]")
            .appName("MediQueue Analytics")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
        # Suppress excessive Spark INFO/DEBUG logs
        _spark_session.sparkContext.setLogLevel("ERROR")
    return _spark_session


def stop_spark_session() -> None:
    """
    Safely stop and clean up the active SparkSession.
    """
    global _spark_session
    if _spark_session is not None:
        try:
            _spark_session.stop()
        except Exception:
            pass
        _spark_session = None


def load_hospital_data(
    spark: Optional[SparkSession] = None,
    source_path: Optional[str] = None
) -> DataFrame:
    """
    Load hospital visit data into a PySpark DataFrame.
    Supports loading directly from CSV or SQLite fallback.
    
    Args:
        spark: Active SparkSession (defaults to get_spark_session()).
        source_path: Path to CSV or SQLite DB (defaults to data/hospital_visits.csv).
        
    Returns:
        PySpark DataFrame adhering to HOSPITAL_VISITS_SCHEMA.
    """
    session = spark or get_spark_session()
    path = source_path or DEFAULT_CSV_PATH
    
    if path.endswith(".csv") and os.path.exists(path):
        df = (
            session.read
            .format("csv")
            .option("header", "true")
            .schema(HOSPITAL_VISITS_SCHEMA)
            .load(path)
        )
        return df
    elif path.endswith(".db") or os.path.exists(DEFAULT_DB_PATH):
        # Fallback to reading from SQLite database layer
        import database
        db_file = path if path.endswith(".db") else DEFAULT_DB_PATH
        rows = database.get_all_visits(db_path=db_file)
        if not rows:
            return session.createDataFrame([], HOSPITAL_VISITS_SCHEMA)

        def sql_literal(value: Any, data_type: Any) -> str:
            if isinstance(data_type, StringType):
                encoded = str(value).encode("utf-8").hex()
                return f"CAST(unhex('{encoded}') AS STRING)"
            if isinstance(data_type, IntegerType):
                return f"CAST({int(value)} AS INT)"
            if isinstance(data_type, DoubleType):
                return f"CAST({float(value)} AS DOUBLE)"
            raise TypeError(f"Unsupported SQLite analytics field type: {data_type}")

        fields = HOSPITAL_VISITS_SCHEMA.fields
        values = ", ".join(
            "(" + ", ".join(
                sql_literal(row[field.name], field.dataType)
                for field in fields
            ) + ")"
            for row in rows
        )
        columns = ", ".join(field.name for field in fields)
        return session.sql(f"SELECT * FROM VALUES {values} AS visits({columns})")
    else:
        raise FileNotFoundError(f"Hospital data source not found at: {path}")


class MediQueueAnalytics:
    """
    PySpark-based Analytics Engine for hospital outpatient and emergency waiting times.
    Implements aggregations, rankings, wait-time categorizations, and operational insights.
    """

    def __init__(self, df: DataFrame):
        """
        Initialize the analytics engine with a validated Spark DataFrame.
        """
        self.df = df

    # A. Overall Hospital Statistics
    def compute_overall_stats(self) -> Dict[str, Any]:
        """
        Compute overall facility metrics:
        - Total patient visits
        - Average waiting time
        - Minimum waiting time
        - Maximum waiting time
        - Average consultation duration
        - Count of High and Critical priority patients
        """
        total_records = self.df.count()
        if total_records == 0:
            return {
                "total_patients": 0,
                "avg_waiting_time": 0.0,
                "min_waiting_time": 0.0,
                "max_waiting_time": 0.0,
                "avg_consultation_duration": 0.0,
                "urgent_cases_count": 0,
                "urgent_cases_percentage": 0.0
            }

        agg_row = self.df.select(
            F.count("*").alias("total"),
            F.avg("waiting_time").alias("avg_wait"),
            F.min("waiting_time").alias("min_wait"),
            F.max("waiting_time").alias("max_wait"),
            F.avg("consultation_duration").alias("avg_consult"),
            F.sum(
                F.when(F.col("priority").isin(["High", "Critical"]), 1).otherwise(0)
            ).alias("urgent_count")
        ).first()

        urgent_count = int(agg_row["urgent_count"] or 0)
        return {
            "total_patients": int(agg_row["total"]),
            "avg_waiting_time": round(float(agg_row["avg_wait"] or 0.0), 2),
            "min_waiting_time": round(float(agg_row["min_wait"] or 0.0), 2),
            "max_waiting_time": round(float(agg_row["max_wait"] or 0.0), 2),
            "avg_consultation_duration": round(float(agg_row["avg_consult"] or 0.0), 2),
            "urgent_cases_count": urgent_count,
            "urgent_cases_percentage": round((urgent_count / total_records) * 100, 2)
        }

    # B. Department Analytics
    def compute_department_analytics(self) -> List[Dict[str, Any]]:
        """
        Compute department-level performance metrics:
        - Total visits
        - Average waiting time
        - Maximum waiting time
        - Average consultation duration
        - Rank departments by average waiting time (highest delay = rank 1)
        """
        if self.df.count() == 0:
            return []

        dept_summary = (
            self.df.groupBy("department")
            .agg(
                F.count("*").alias("total_visits"),
                F.round(F.avg("waiting_time"), 2).alias("avg_waiting_time"),
                F.round(F.min("waiting_time"), 2).alias("min_waiting_time"),
                F.round(F.max("waiting_time"), 2).alias("max_waiting_time"),
                F.round(F.avg("consultation_duration"), 2).alias("avg_consultation_duration")
            )
        )

        # Window ranking based on average waiting time descending
        window_spec = Window.orderBy(F.desc("avg_waiting_time"))
        ranked_df = dept_summary.withColumn("wait_rank", F.dense_rank().over(window_spec))

        rows = ranked_df.collect()
        return [
            {
                "department": r["department"],
                "total_visits": int(r["total_visits"]),
                "avg_waiting_time": float(r["avg_waiting_time"]),
                "min_waiting_time": float(r["min_waiting_time"]),
                "max_waiting_time": float(r["max_waiting_time"]),
                "avg_consultation_duration": float(r["avg_consultation_duration"]),
                "wait_rank": int(r["wait_rank"])
            }
            for r in rows
        ]

    # C. Priority Analytics
    def compute_priority_analytics(self) -> List[Dict[str, Any]]:
        """
        Compare waiting times across clinical priorities:
        - Patient count by priority
        - Average, min, and max waiting time
        - Average consultation duration
        """
        if self.df.count() == 0:
            return []

        pri_df = (
            self.df.groupBy("priority")
            .agg(
                F.count("*").alias("total_visits"),
                F.round(F.avg("waiting_time"), 2).alias("avg_waiting_time"),
                F.round(F.min("waiting_time"), 2).alias("min_waiting_time"),
                F.round(F.max("waiting_time"), 2).alias("max_waiting_time"),
                F.round(F.avg("consultation_duration"), 2).alias("avg_consultation_duration")
            )
        )

        # Sort order by medical urgency
        urgency_order = {"Critical": 1, "High": 2, "Medium": 3, "Low": 4}
        rows = pri_df.collect()
        result = [
            {
                "priority": r["priority"],
                "total_visits": int(r["total_visits"]),
                "avg_waiting_time": float(r["avg_waiting_time"]),
                "min_waiting_time": float(r["min_waiting_time"]),
                "max_waiting_time": float(r["max_waiting_time"]),
                "avg_consultation_duration": float(r["avg_consultation_duration"])
            }
            for r in rows
        ]
        result.sort(key=lambda x: urgency_order.get(x["priority"], 99))
        return result

    # D. Waiting Time Classification
    def compute_waiting_time_classification(self) -> List[Dict[str, Any]]:
        """
        Classify patient visits into 4 operational waiting-time tiers:
        - Short Wait: 0–15 minutes
        - Moderate Wait: 16–30 minutes
        - Long Wait: 31–60 minutes
        - Critical Delay: Above 60 minutes
        """
        total_records = self.df.count()
        if total_records == 0:
            return []

        categorized_df = self.df.withColumn(
            "wait_category",
            F.when(F.col("waiting_time") <= 15, "Short Wait (0-15m)")
            .when((F.col("waiting_time") > 15) & (F.col("waiting_time") <= 30), "Moderate Wait (16-30m)")
            .when((F.col("waiting_time") > 30) & (F.col("waiting_time") <= 60), "Long Wait (31-60m)")
            .otherwise("Critical Delay (>60m)")
        )

        counts = (
            categorized_df.groupBy("wait_category")
            .agg(
                F.count("*").alias("patient_count"),
                F.round(F.avg("waiting_time"), 2).alias("category_avg_wait")
            )
        ).collect()

        category_order = [
            "Short Wait (0-15m)",
            "Moderate Wait (16-30m)",
            "Long Wait (31-60m)",
            "Critical Delay (>60m)"
        ]

        category_map = {r["wait_category"]: (int(r["patient_count"]), float(r["category_avg_wait"])) for r in counts}
        
        result = []
        for cat in category_order:
            count, avg_wait = category_map.get(cat, (0, 0.0))
            percentage = round((count / total_records) * 100, 2) if total_records > 0 else 0.0
            result.append({
                "category": cat,
                "patient_count": count,
                "percentage": percentage,
                "avg_waiting_time": avg_wait
            })
        return result

    # E. Peak Hour Analysis
    def compute_peak_hour_analysis(self) -> List[Dict[str, Any]]:
        """
        Extract arrival hour from timestamps and analyze patient arrival distribution:
        - Arrival hour (0-23)
        - Patient arrival count
        - Average waiting time for arrivals in that hour
        """
        if self.df.count() == 0:
            return []

        # Extract hour directly from string 'YYYY-MM-DD HH:MM:SS' using substring
        hour_df = self.df.withColumn(
            "arrival_hour",
            F.substring(F.col("arrival_time"), 12, 2).cast(IntegerType())
        )

        hour_summary = (
            hour_df.groupBy("arrival_hour")
            .agg(
                F.count("*").alias("arrival_count"),
                F.round(F.avg("waiting_time"), 2).alias("avg_waiting_time"),
                F.round(F.avg("queue_length"), 1).alias("avg_queue_depth")
            )
            .orderBy("arrival_hour")
        )

        rows = hour_summary.collect()
        return [
            {
                "arrival_hour": int(r["arrival_hour"]),
                "hour_formatted": f"{int(r['arrival_hour']):02d}:00 - {int(r['arrival_hour']):02d}:59",
                "arrival_count": int(r["arrival_count"]),
                "avg_waiting_time": float(r["avg_waiting_time"] or 0.0),
                "avg_queue_depth": float(r["avg_queue_depth"] or 0.0)
            }
            for r in rows
        ]

    # F. Day-wise Analysis
    def compute_day_wise_analysis(self) -> List[Dict[str, Any]]:
        """
        Analyze traffic and wait times by day of the week:
        - Total visits per day
        - Average waiting time
        - Minimum / Maximum wait times
        """
        if self.df.count() == 0:
            return []

        day_summary = (
            self.df.groupBy("day_of_week")
            .agg(
                F.count("*").alias("total_visits"),
                F.round(F.avg("waiting_time"), 2).alias("avg_waiting_time"),
                F.round(F.avg("consultation_duration"), 2).alias("avg_consultation_duration")
            )
        )

        day_order = {
            "Monday": 1, "Tuesday": 2, "Wednesday": 3,
            "Thursday": 4, "Friday": 5, "Saturday": 6, "Sunday": 7
        }

        rows = day_summary.collect()
        result = [
            {
                "day_of_week": r["day_of_week"],
                "total_visits": int(r["total_visits"]),
                "avg_waiting_time": float(r["avg_waiting_time"] or 0.0),
                "avg_consultation_duration": float(r["avg_consultation_duration"] or 0.0)
            }
            for r in rows
        ]
        result.sort(key=lambda x: day_order.get(x["day_of_week"], 99))
        return result

    # 2. Rule-Based Waiting Time Insights
    def generate_insights(self) -> Dict[str, Any]:
        """
        Generate operational, rule-based insights from PySpark aggregations.
        Note: These are descriptive analytics and deterministic rule-based insights,
        not machine learning predictions.
        """
        total_records = self.df.count()
        if total_records == 0:
            return {
                "highest_wait_department": None,
                "lowest_wait_department": None,
                "busiest_hour": None,
                "critical_delay_percentage": 0.0,
                "critical_delay_count": 0,
                "top_priority_category": None,
                "staffing_recommendations": [],
                "disclaimer": "Descriptive analytics and rule-based insights; not machine-learning predictions."
            }

        overall_stats = self.compute_overall_stats()
        dept_analytics = self.compute_department_analytics()
        pri_analytics = self.compute_priority_analytics()
        wait_classes = self.compute_waiting_time_classification()
        peak_hours = self.compute_peak_hour_analysis()

        # Highest and lowest wait departments
        highest_dept = dept_analytics[0] if dept_analytics else None
        lowest_dept = dept_analytics[-1] if dept_analytics else None

        # Busiest hour by arrivals
        busiest_hour_record = max(peak_hours, key=lambda x: x["arrival_count"]) if peak_hours else None

        # Critical delay patients (> 60 mins)
        critical_delay_info = next(
            (c for c in wait_classes if "Critical Delay" in c["category"]),
            {"patient_count": 0, "percentage": 0.0}
        )

        # Largest priority group
        top_priority = max(pri_analytics, key=lambda x: x["total_visits"]) if pri_analytics else None

        # Staffing recommendations based on rule thresholds:
        # 1. Departments where average wait time exceeds the hospital average by > 10%
        # 2. Departments with significant proportion of long waits
        staffing_recommendations = []
        hospital_avg = overall_stats["avg_waiting_time"]

        for dept in dept_analytics:
            dept_name = dept["department"]
            dept_avg = dept["avg_waiting_time"]
            
            if dept_avg > hospital_avg * 1.15:
                staffing_recommendations.append({
                    "department": dept_name,
                    "avg_wait": dept_avg,
                    "reason": f"Average wait time ({dept_avg} min) is {round(((dept_avg - hospital_avg) / hospital_avg) * 100, 1)}% higher than hospital benchmark ({hospital_avg} min).",
                    "action": "Consider adding an auxiliary consulting doctor or staggering OPD appointment slots."
                })
            elif dept_name == "Emergency" and dept["max_waiting_time"] > 30:
                staffing_recommendations.append({
                    "department": dept_name,
                    "avg_wait": dept_avg,
                    "reason": f"Emergency max wait reached {dept['max_waiting_time']} min.",
                    "action": "Review triage flow to ensure immediate bed allocation for critical arrivals."
                })

        return {
            "highest_wait_department": {
                "name": highest_dept["department"],
                "avg_wait": highest_dept["avg_waiting_time"],
                "rank": highest_dept["wait_rank"]
            } if highest_dept else None,
            "lowest_wait_department": {
                "name": lowest_dept["department"],
                "avg_wait": lowest_dept["avg_waiting_time"],
                "rank": lowest_dept["wait_rank"]
            } if lowest_dept else None,
            "busiest_arrival_hour": {
                "hour": busiest_hour_record["hour_formatted"],
                "patient_arrivals": busiest_hour_record["arrival_count"],
                "avg_waiting_time": busiest_hour_record["avg_waiting_time"]
            } if busiest_hour_record else None,
            "critical_delay_count": critical_delay_info["patient_count"],
            "critical_delay_percentage": critical_delay_info["percentage"],
            "top_priority_category": {
                "priority": top_priority["priority"],
                "patient_count": top_priority["total_visits"],
                "avg_wait": top_priority["avg_waiting_time"]
            } if top_priority else None,
            "staffing_recommendations": staffing_recommendations,
            "disclaimer": "Descriptive analytics and rule-based insights derived from PySpark aggregations; not machine-learning predictions."
        }


def run_full_analytics(
    source_path: Optional[str] = None,
    department: Optional[str] = None,
    priority: Optional[str] = None,
    day_of_week: Optional[str] = None
) -> Dict[str, Any]:
    """
    Load the dataset, apply optional filters in Spark, and compute all analytics.
    """
    spark = get_spark_session()
    df = load_hospital_data(spark=spark, source_path=source_path)

    if department:
        df = df.filter(F.col("department") == department)
    if priority:
        df = df.filter(F.col("priority") == priority)
    if day_of_week:
        df = df.filter(F.col("day_of_week") == day_of_week)

    engine = MediQueueAnalytics(df)
    
    return {
        "overall_stats": engine.compute_overall_stats(),
        "department_analytics": engine.compute_department_analytics(),
        "priority_analytics": engine.compute_priority_analytics(),
        "waiting_time_classification": engine.compute_waiting_time_classification(),
        "peak_hour_analysis": engine.compute_peak_hour_analysis(),
        "day_wise_analysis": engine.compute_day_wise_analysis(),
        "insights": engine.generate_insights()
    }


if __name__ == "__main__":
    print("Running MediQueue PySpark Analytics Engine demo...")
    results = run_full_analytics()
    print("Overall Stats:", results["overall_stats"])
    print("\nDepartment Ranking:")
    for d in results["department_analytics"]:
        print(f"  Rank {d['wait_rank']}: {d['department']} - Avg Wait: {d['avg_waiting_time']} min (Visits: {d['total_visits']})")
    print("\nKey Insights:")
    for k, v in results["insights"].items():
        print(f"  {k}: {v}")
    stop_spark_session()
