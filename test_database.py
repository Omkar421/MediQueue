"""
MediQueue - Database Layer Unit Tests
Tests initialization, CSV import, CRUD operations, data validation,
and statistical reporting for the SQLite storage engine.
"""

import os
import tempfile
import unittest
import sqlite3
import csv
from datetime import datetime

import database
from database import (
    DEFAULT_DB_PATH,
    DEFAULT_CSV_PATH,
    VALID_DEPARTMENTS,
    VALID_PRIORITIES,
    ValidationError,
    init_db,
    validate_visit,
    insert_visit,
    update_visit,
    get_all_visits,
    get_visits_by_department,
    get_visit_by_id,
    import_csv,
    get_database_stats
)


class TestMediQueueDatabase(unittest.TestCase):
    """Test suite for MediQueue SQLite database operations and validations."""

    def setUp(self):
        """Create a temporary database for isolated test execution."""
        self.temp_db_fd, self.temp_db_path = tempfile.mkstemp(suffix=".db")
        os.close(self.temp_db_fd)
        init_db(self.temp_db_path)

        self.sample_visit = {
            "patient_id": "TEST-1001",
            "department": "Emergency",
            "arrival_time": "2026-10-25 10:15:00",
            "consultation_start": "2026-10-25 10:20:00",
            "consultation_end": "2026-10-25 10:45:00",
            "doctor_id": "DOC-EM-01",
            "priority": "Critical",
            "patient_age": 45,
            "queue_length": 2,
            "waiting_time": 5.0,
            "consultation_duration": 25.0,
            "day_of_week": "Sunday"
        }

    def tearDown(self):
        """Clean up temporary test database."""
        if os.path.exists(self.temp_db_path):
            try:
                os.remove(self.temp_db_path)
            except PermissionError:
                pass

    # 1. Initialization Tests
    def test_init_db_creates_table_and_indexes(self):
        """Verify that init_db creates hospital_visits table and indexes."""
        conn = sqlite3.connect(self.temp_db_path)
        cursor = conn.cursor()
        
        # Check table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='hospital_visits'")
        self.assertIsNotNone(cursor.fetchone(), "Table 'hospital_visits' should exist.")
        
        # Check column names
        cursor.execute("PRAGMA table_info(hospital_visits)")
        columns = [row[1] for row in cursor.fetchall()]
        expected_cols = [
            "patient_id", "department", "arrival_time", "consultation_start",
            "consultation_end", "doctor_id", "priority", "patient_age",
            "queue_length", "waiting_time", "consultation_duration",
            "day_of_week", "created_at"
        ]
        for col in expected_cols:
            self.assertIn(col, columns, f"Column '{col}' must be present in schema.")
            
        # Check indexes
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = [row[0] for row in cursor.fetchall()]
        self.assertTrue(any("department" in idx for idx in indexes))
        self.assertTrue(any("priority" in idx for idx in indexes))
        conn.close()

    # 2. Insert Operations
    def test_insert_visit_success(self):
        """Verify insertion of a valid visit record."""
        result = insert_visit(self.sample_visit, db_path=self.temp_db_path)
        self.assertTrue(result)
        
        fetched = get_visit_by_id("TEST-1001", db_path=self.temp_db_path)
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched["patient_id"], "TEST-1001")
        self.assertEqual(fetched["department"], "Emergency")
        self.assertEqual(fetched["priority"], "Critical")
        self.assertEqual(fetched["waiting_time"], 5.0)

    def test_insert_duplicate_patient_id_fails(self):
        """Verify that duplicate patient_id raises ValidationError."""
        insert_visit(self.sample_visit, db_path=self.temp_db_path)
        with self.assertRaises(ValidationError):
            insert_visit(self.sample_visit, db_path=self.temp_db_path)

    # 3. Data Validation Tests
    def test_validation_invalid_department(self):
        """Verify rejection of invalid department."""
        invalid_visit = self.sample_visit.copy()
        invalid_visit["department"] = "Radiology"
        with self.assertRaises(ValidationError):
            insert_visit(invalid_visit, db_path=self.temp_db_path)

    def test_validation_invalid_priority(self):
        """Verify rejection of invalid priority."""
        invalid_visit = self.sample_visit.copy()
        invalid_visit["priority"] = "Extreme"
        with self.assertRaises(ValidationError):
            insert_visit(invalid_visit, db_path=self.temp_db_path)

    def test_validation_negative_waiting_time(self):
        """Verify rejection of negative waiting time."""
        invalid_visit = self.sample_visit.copy()
        invalid_visit["waiting_time"] = -10.0
        with self.assertRaises(ValidationError):
            insert_visit(invalid_visit, db_path=self.temp_db_path)

    def test_validation_negative_consultation_duration(self):
        """Verify rejection of negative consultation duration."""
        invalid_visit = self.sample_visit.copy()
        invalid_visit["consultation_duration"] = -5.0
        with self.assertRaises(ValidationError):
            insert_visit(invalid_visit, db_path=self.temp_db_path)

    def test_validation_missing_required_field(self):
        """Verify rejection when a required field is missing."""
        invalid_visit = self.sample_visit.copy()
        del invalid_visit["doctor_id"]
        with self.assertRaises(ValidationError):
            insert_visit(invalid_visit, db_path=self.temp_db_path)

    def test_validation_empty_string_field(self):
        """Verify rejection when a required field is an empty string."""
        invalid_visit = self.sample_visit.copy()
        invalid_visit["patient_id"] = "   "
        with self.assertRaises(ValidationError):
            insert_visit(invalid_visit, db_path=self.temp_db_path)

    def test_validation_invalid_datetime_format(self):
        """Verify rejection when datetime format is invalid."""
        invalid_visit = self.sample_visit.copy()
        invalid_visit["arrival_time"] = "25/10/2026 10:15"
        with self.assertRaises(ValidationError):
            insert_visit(invalid_visit, db_path=self.temp_db_path)

    # 4. Fetch Operations
    def test_fetch_operations(self):
        """Verify fetching all visits and fetching by department."""
        v1 = self.sample_visit.copy()
        v1["patient_id"] = "P-TEST-1"
        v1["department"] = "Cardiology"
        v1["arrival_time"] = "2026-10-25 09:00:00"

        v2 = self.sample_visit.copy()
        v2["patient_id"] = "P-TEST-2"
        v2["department"] = "Pediatrics"
        v2["arrival_time"] = "2026-10-25 10:00:00"

        insert_visit(v1, db_path=self.temp_db_path)
        insert_visit(v2, db_path=self.temp_db_path)

        # Fetch all
        all_visits = get_all_visits(db_path=self.temp_db_path)
        self.assertEqual(len(all_visits), 2)
        self.assertEqual(all_visits[0]["patient_id"], "P-TEST-1")

        # Fetch by department
        cardio_visits = get_visits_by_department("Cardiology", db_path=self.temp_db_path)
        self.assertEqual(len(cardio_visits), 1)
        self.assertEqual(cardio_visits[0]["patient_id"], "P-TEST-1")

        peds_visits = get_visits_by_department("Pediatrics", db_path=self.temp_db_path)
        self.assertEqual(len(peds_visits), 1)
        self.assertEqual(peds_visits[0]["patient_id"], "P-TEST-2")

        ortho_visits = get_visits_by_department("Orthopedics", db_path=self.temp_db_path)
        self.assertEqual(len(ortho_visits), 0)

    def test_fetch_by_invalid_department_raises_validation_error(self):
        """Verify querying an unsupported department raises ValidationError."""
        with self.assertRaises(ValidationError):
            get_visits_by_department("Oncology", db_path=self.temp_db_path)

    # 5. Update Operations
    def test_update_visit_success(self):
        """Verify updating an existing record."""
        insert_visit(self.sample_visit, db_path=self.temp_db_path)
        
        updated = update_visit(
            "TEST-1001",
            {"priority": "High", "waiting_time": 12.0},
            db_path=self.temp_db_path
        )
        self.assertTrue(updated)

        fetched = get_visit_by_id("TEST-1001", db_path=self.temp_db_path)
        self.assertEqual(fetched["priority"], "High")
        self.assertEqual(fetched["waiting_time"], 12.0)

    def test_update_visit_invalid_fields_fail(self):
        """Verify that updating with invalid values raises ValidationError."""
        insert_visit(self.sample_visit, db_path=self.temp_db_path)
        with self.assertRaises(ValidationError):
            update_visit("TEST-1001", {"waiting_time": -5.0}, db_path=self.temp_db_path)

    def test_update_nonexistent_patient(self):
        """Verify update returns False when patient_id does not exist."""
        result = update_visit("NON-EXISTENT", {"priority": "Low"}, db_path=self.temp_db_path)
        self.assertFalse(result)

    # 6. Database Statistics Tests
    def test_database_stats(self):
        """Verify statistical metrics calculation."""
        # Empty DB stats
        stats_empty = get_database_stats(db_path=self.temp_db_path)
        self.assertEqual(stats_empty["total_visits"], 0)

        # Populate with 2 records
        v1 = self.sample_visit.copy()
        v1["patient_id"] = "S1"
        v1["department"] = "Emergency"
        v1["waiting_time"] = 10.0
        v1["consultation_duration"] = 20.0

        v2 = self.sample_visit.copy()
        v2["patient_id"] = "S2"
        v2["department"] = "Emergency"
        v2["waiting_time"] = 20.0
        v2["consultation_duration"] = 30.0

        insert_visit(v1, db_path=self.temp_db_path)
        insert_visit(v2, db_path=self.temp_db_path)

        stats = get_database_stats(db_path=self.temp_db_path)
        self.assertEqual(stats["total_visits"], 2)
        self.assertEqual(stats["department_counts"]["Emergency"], 2)
        self.assertEqual(stats["avg_waiting_time"], 15.0)
        self.assertEqual(stats["min_waiting_time"], 10.0)
        self.assertEqual(stats["max_waiting_time"], 20.0)
        self.assertEqual(stats["avg_consultation_duration"], 25.0)
        self.assertEqual(stats["busiest_department"], "Emergency")

    # 7. CSV Import Tests
    def test_import_actual_csv_dataset(self):
        """Verify importing the generated data/hospital_visits.csv."""
        self.assertTrue(os.path.exists(DEFAULT_CSV_PATH), f"CSV not found at {DEFAULT_CSV_PATH}")
        summary = import_csv(csv_path=DEFAULT_CSV_PATH, db_path=self.temp_db_path)
        
        self.assertEqual(summary["total_rows"], 200)
        self.assertEqual(summary["imported"], 200)
        self.assertEqual(summary["skipped"], 0)
        
        stats = get_database_stats(db_path=self.temp_db_path)
        self.assertEqual(stats["total_visits"], 200)
        
        # Verify all 5 departments are present
        for dept in VALID_DEPARTMENTS:
            self.assertIn(dept, stats["department_counts"])
            self.assertGreater(stats["department_counts"][dept], 0)
            
        # Verify all 4 priority levels are present
        for pri in VALID_PRIORITIES:
            self.assertIn(pri, stats["priority_counts"])
            self.assertGreater(stats["priority_counts"][pri], 0)

    def test_import_csv_with_malformed_records(self):
        """Verify that malformed records in CSV are safely handled and skipped."""
        temp_csv_fd, temp_csv_path = tempfile.mkstemp(suffix=".csv")
        os.close(temp_csv_fd)
        
        headers = [
            "patient_id", "department", "arrival_time", "consultation_start",
            "consultation_end", "doctor_id", "priority", "patient_age",
            "queue_length", "waiting_time", "consultation_duration", "day_of_week"
        ]
        
        rows = [
            # Valid row
            ["M001", "Emergency", "2026-10-25 10:00:00", "2026-10-25 10:05:00", "2026-10-25 10:25:00", "DOC-1", "High", 30, 2, 5, 20, "Sunday"],
            # Invalid department
            ["M002", "UnknownDept", "2026-10-25 10:00:00", "2026-10-25 10:05:00", "2026-10-25 10:25:00", "DOC-1", "High", 30, 2, 5, 20, "Sunday"],
            # Negative waiting time
            ["M003", "Emergency", "2026-10-25 10:00:00", "2026-10-25 10:05:00", "2026-10-25 10:25:00", "DOC-1", "High", 30, 2, -10, 20, "Sunday"],
            # Missing fields
            ["M004", "Emergency", "2026-10-25 10:00:00", "", "", "DOC-1", "High", 30, 2, 5, 20, "Sunday"],
            # Another valid row
            ["M005", "Pediatrics", "2026-10-25 11:00:00", "2026-10-25 11:15:00", "2026-10-25 11:35:00", "DOC-2", "Medium", 8, 3, 15, 20, "Sunday"]
        ]
        
        with open(temp_csv_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
            
        summary = import_csv(csv_path=temp_csv_path, db_path=self.temp_db_path)
        self.assertEqual(summary["total_rows"], 5)
        self.assertEqual(summary["imported"], 2)
        self.assertEqual(summary["skipped"], 3)
        self.assertEqual(len(summary["errors"]), 3)
        
        # Clean up temp file
        if os.path.exists(temp_csv_path):
            os.remove(temp_csv_path)


if __name__ == "__main__":
    unittest.main()

