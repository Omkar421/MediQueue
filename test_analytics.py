"""
MediQueue - PySpark Analytics Unit Tests
Validates SparkSession lifecycle, DataFrame transformations, statistical aggregations,
wait-time classification, peak-hour profiling, rule-based insights, and empty dataset handling.
"""

import os
import sys
import unittest

# Ensure PySpark workers match the active runtime
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from analytics import (
    get_spark_session,
    stop_spark_session,
    load_hospital_data,
    MediQueueAnalytics,
    HOSPITAL_VISITS_SCHEMA,
    DEFAULT_CSV_PATH
)


class TestMediQueueAnalytics(unittest.TestCase):
    """Test suite for PySpark analytics transformations and operational metrics."""

    @classmethod
    def setUpClass(cls):
        """Initialize a single lightweight local[1] SparkSession for all test cases."""
        cls.spark = get_spark_session()
        
        # Small deterministic test dataset (6 visits)
        cls.sample_data = [
            ("T001", "Emergency", "2026-10-19 02:15:00", "2026-10-19 02:20:00", "2026-10-19 02:45:00", "DOC-1", "Critical", 25, 2, 5.0, 25.0, "Monday"),
            ("T002", "Emergency", "2026-10-19 02:30:00", "2026-10-19 02:40:00", "2026-10-19 03:00:00", "DOC-1", "High", 40, 3, 10.0, 20.0, "Monday"),
            ("T003", "Cardiology", "2026-10-19 10:00:00", "2026-10-19 10:25:00", "2026-10-19 10:55:00", "DOC-2", "Medium", 65, 5, 25.0, 30.0, "Monday"),
            ("T004", "Cardiology", "2026-10-19 10:15:00", "2026-10-19 11:00:00", "2026-10-19 11:30:00", "DOC-2", "Low", 70, 7, 45.0, 30.0, "Monday"),
            ("T005", "General Medicine", "2026-10-20 11:00:00", "2026-10-20 12:15:00", "2026-10-20 12:35:00", "DOC-3", "Low", 50, 10, 75.0, 20.0, "Tuesday"),
            ("T006", "Pediatrics", "2026-10-20 11:30:00", "2026-10-20 11:45:00", "2026-10-20 12:00:00", "DOC-4", "High", 8, 1, 15.0, 15.0, "Tuesday")
        ]
        cls.test_df = cls.spark.createDataFrame(cls.sample_data, HOSPITAL_VISITS_SCHEMA)
        cls.analytics = MediQueueAnalytics(cls.test_df)

    @classmethod
    def tearDownClass(cls):
        """Stop SparkSession after all tests complete."""
        stop_spark_session()

    # 1. Spark Session Initialization
    def test_spark_session_initialization(self):
        """Verify Spark session configuration, master, and app name."""
        self.assertIsNotNone(self.spark)
        self.assertEqual(self.spark.conf.get("spark.master"), "local[1]")
        self.assertEqual(self.spark.conf.get("spark.app.name"), "MediQueue Analytics")

    # 2. Overall Statistics
    def test_overall_statistics(self):
        """Verify overall hospital statistics aggregation."""
        stats = self.analytics.compute_overall_stats()
        # Total records: 6
        self.assertEqual(stats["total_patients"], 6)
        # Waiting times: 5, 10, 25, 45, 75, 15 -> sum = 175 -> avg = 29.17
        self.assertEqual(stats["min_waiting_time"], 5.0)
        self.assertEqual(stats["max_waiting_time"], 75.0)
        self.assertAlmostEqual(stats["avg_waiting_time"], 29.17, places=2)
        # Consultation: 25, 20, 30, 30, 20, 15 -> sum = 140 -> avg = 23.33
        self.assertAlmostEqual(stats["avg_consultation_duration"], 23.33, places=2)
        # Urgent cases (Critical=1, High=2 -> total=3)
        self.assertEqual(stats["urgent_cases_count"], 3)
        self.assertEqual(stats["urgent_cases_percentage"], 50.0)

    # 3. Department Analytics & Ranking
    def test_department_aggregation_and_ranking(self):
        """Verify department aggregation and waiting time rank assignment."""
        dept_metrics = self.analytics.compute_department_analytics()
        self.assertEqual(len(dept_metrics), 4)

        # General Medicine has highest avg wait (75.0) -> Rank 1
        gm = next(d for d in dept_metrics if d["department"] == "General Medicine")
        self.assertEqual(gm["wait_rank"], 1)
        self.assertEqual(gm["total_visits"], 1)
        self.assertEqual(gm["avg_waiting_time"], 75.0)

        # Cardiology has avg wait (25 + 45)/2 = 35.0 -> Rank 2
        cardio = next(d for d in dept_metrics if d["department"] == "Cardiology")
        self.assertEqual(cardio["wait_rank"], 2)
        self.assertEqual(cardio["total_visits"], 2)
        self.assertEqual(cardio["avg_waiting_time"], 35.0)

        # Emergency has avg wait (5 + 10)/2 = 7.5 -> Rank 4 (lowest wait)
        em = next(d for d in dept_metrics if d["department"] == "Emergency")
        self.assertEqual(em["wait_rank"], 4)
        self.assertEqual(em["avg_waiting_time"], 7.5)

    # 4. Priority Aggregation
    def test_priority_aggregation(self):
        """Verify waiting times across clinical priority tiers."""
        pri_metrics = self.analytics.compute_priority_analytics()
        self.assertEqual(len(pri_metrics), 4)

        # Order must follow Critical -> High -> Medium -> Low
        priorities = [p["priority"] for p in pri_metrics]
        self.assertEqual(priorities, ["Critical", "High", "Medium", "Low"])

        critical_p = next(p for p in pri_metrics if p["priority"] == "Critical")
        self.assertEqual(critical_p["avg_waiting_time"], 5.0)

        low_p = next(p for p in pri_metrics if p["priority"] == "Low")
        # Low waits: 45 and 75 -> avg = 60.0
        self.assertEqual(low_p["avg_waiting_time"], 60.0)

    # 5. Waiting Time Classification
    def test_waiting_time_classification(self):
        """Verify patient classification into operational tiers."""
        categories = self.analytics.compute_waiting_time_classification()
        self.assertEqual(len(categories), 4)

        cat_dict = {c["category"]: c["patient_count"] for c in categories}
        # T001 (5), T002 (10), T006 (15) -> 3 patients in Short Wait (0-15m)
        self.assertEqual(cat_dict["Short Wait (0-15m)"], 3)
        # T003 (25) -> 1 patient in Moderate Wait (16-30m)
        self.assertEqual(cat_dict["Moderate Wait (16-30m)"], 1)
        # T004 (45) -> 1 patient in Long Wait (31-60m)
        self.assertEqual(cat_dict["Long Wait (31-60m)"], 1)
        # T005 (75) -> 1 patient in Critical Delay (>60m)
        self.assertEqual(cat_dict["Critical Delay (>60m)"], 1)

    # 6. Peak Hour Aggregation
    def test_peak_hour_aggregation(self):
        """Verify extraction of arrival hour and traffic counting."""
        peak_hours = self.analytics.compute_peak_hour_analysis()
        # Hours present in sample: 02 (2 arrivals), 10 (2 arrivals), 11 (2 arrivals)
        self.assertEqual(len(peak_hours), 3)

        hour_map = {h["arrival_hour"]: h["arrival_count"] for h in peak_hours}
        self.assertEqual(hour_map[2], 2)
        self.assertEqual(hour_map[10], 2)
        self.assertEqual(hour_map[11], 2)

    # 7. Day-wise Analysis
    def test_day_wise_analysis(self):
        """Verify patient visit volume and waiting time by day of the week."""
        day_metrics = self.analytics.compute_day_wise_analysis()
        self.assertEqual(len(day_metrics), 2)  # Monday and Tuesday in sample

        mon = next(d for d in day_metrics if d["day_of_week"] == "Monday")
        self.assertEqual(mon["total_visits"], 4)

        tue = next(d for d in day_metrics if d["day_of_week"] == "Tuesday")
        self.assertEqual(tue["total_visits"], 2)

    # 8. Rule-Based Insights
    def test_rule_based_insights(self):
        """Verify extraction of operational insights and recommendations."""
        insights = self.analytics.generate_insights()
        
        self.assertEqual(insights["highest_wait_department"]["name"], "General Medicine")
        self.assertEqual(insights["lowest_wait_department"]["name"], "Emergency")
        self.assertEqual(insights["critical_delay_count"], 1)
        self.assertAlmostEqual(insights["critical_delay_percentage"], 16.67, places=1)
        self.assertIn("disclaimer", insights)
        self.assertIn("staffing_recommendations", insights)

    # 9. Empty Dataset Handling
    def test_empty_dataset_handling(self):
        """Verify all analytics methods handle empty DataFrames safely without crashing."""
        empty_df = self.spark.createDataFrame([], HOSPITAL_VISITS_SCHEMA)
        empty_analytics = MediQueueAnalytics(empty_df)

        stats = empty_analytics.compute_overall_stats()
        self.assertEqual(stats["total_patients"], 0)
        self.assertEqual(stats["avg_waiting_time"], 0.0)

        self.assertEqual(empty_analytics.compute_department_analytics(), [])
        self.assertEqual(empty_analytics.compute_priority_analytics(), [])
        self.assertEqual(empty_analytics.compute_waiting_time_classification(), [])
        self.assertEqual(empty_analytics.compute_peak_hour_analysis(), [])
        self.assertEqual(empty_analytics.compute_day_wise_analysis(), [])

        insights = empty_analytics.generate_insights()
        self.assertIsNone(insights["highest_wait_department"])
        self.assertEqual(insights["critical_delay_count"], 0)

    # 10. Full Dataset Load
    def test_load_actual_hospital_dataset(self):
        """Verify loading the actual data/hospital_visits.csv file into PySpark."""
        self.assertTrue(os.path.exists(DEFAULT_CSV_PATH))
        df = load_hospital_data(self.spark, DEFAULT_CSV_PATH)
        self.assertEqual(df.count(), 200)

        # Validate schema field names
        fields = df.schema.fieldNames()
        expected_fields = [
            "patient_id", "department", "arrival_time", "consultation_start",
            "consultation_end", "doctor_id", "priority", "patient_age",
            "queue_length", "waiting_time", "consultation_duration", "day_of_week"
        ]
        for field in expected_fields:
            self.assertIn(field, fields)


if __name__ == "__main__":
    unittest.main()

