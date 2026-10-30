"""Tests for cached dashboard data helpers."""

import csv
import os
import tempfile
import unittest

import database
from dashboard_helpers import get_cached_patient_visits


class TestDashboardPatientVisits(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "mediqueue.db")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_empty_database_is_seeded_from_csv(self):
        visits = get_cached_patient_visits(db_path=self.db_path)

        self.assertEqual(len(visits), 200)
        self.assertEqual(len(database.get_all_visits(db_path=self.db_path)), 200)

    def test_existing_database_records_are_preserved(self):
        with open(database.DEFAULT_CSV_PATH, encoding="utf-8", newline="") as csv_file:
            sample_visit = next(csv.DictReader(csv_file))
        database.insert_visit(sample_visit, db_path=self.db_path)

        visits = get_cached_patient_visits(db_path=self.db_path)

        self.assertEqual(len(visits), 1)
        self.assertEqual(visits[0]["patient_id"], sample_visit["patient_id"])


if __name__ == "__main__":
    unittest.main()
