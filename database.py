"""
MediQueue - Database Module
SQLite Data Layer for Hospital Waiting Time Analyzer.
Provides database initialization, CRUD operations, CSV dataset import, 
validation, and statistical aggregations.
"""

import os
import csv
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, Iterator, List, Optional, Tuple, Any, Union

# Configuration & Constants
DEFAULT_DB_PATH = os.path.join("data", "mediqueue.db")
DEFAULT_CSV_PATH = os.path.join("data", "hospital_visits.csv")

VALID_DEPARTMENTS = [
    "Emergency",
    "General Medicine",
    "Cardiology",
    "Orthopedics",
    "Pediatrics"
]

VALID_PRIORITIES = [
    "Low",
    "Medium",
    "High",
    "Critical"
]

REQUIRED_FIELDS = [
    "patient_id",
    "department",
    "arrival_time",
    "consultation_start",
    "consultation_end",
    "doctor_id",
    "priority",
    "patient_age",
    "queue_length",
    "waiting_time",
    "consultation_duration",
    "day_of_week"
]

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class ValidationError(ValueError):
    """Custom exception raised when visit data fails validation."""
    pass


def get_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    """
    Get a configured SQLite database connection.
    Enforces foreign keys and returns rows as dictionary-like Row objects.
    """
    path = db_path or DEFAULT_DB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


@contextmanager
def _managed_connection(db_path: Optional[str] = None) -> Iterator[sqlite3.Connection]:
    """Commit or roll back database work, then always close the connection."""
    conn = get_connection(db_path)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def init_db(db_path: Optional[str] = None) -> None:
    """
    Initialize SQLite database and create required tables and indexes.
    """
    path = db_path or DEFAULT_DB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    
    with _managed_connection(path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hospital_visits (
                patient_id TEXT PRIMARY KEY,
                department TEXT NOT NULL,
                arrival_time TEXT NOT NULL,
                consultation_start TEXT NOT NULL,
                consultation_end TEXT NOT NULL,
                doctor_id TEXT NOT NULL,
                priority TEXT NOT NULL,
                patient_age INTEGER NOT NULL,
                queue_length INTEGER NOT NULL,
                waiting_time REAL NOT NULL,
                consultation_duration REAL NOT NULL,
                day_of_week TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Indexes for fast querying & analytical filtering
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_visits_department ON hospital_visits (department);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_visits_priority ON hospital_visits (priority);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_visits_arrival_time ON hospital_visits (arrival_time);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_visits_doctor ON hospital_visits (doctor_id);")
        
        conn.commit()


def validate_visit(visit_data: Dict[str, Any], partial: bool = False) -> Tuple[bool, Optional[str]]:
    """
    Validate a visit record dictionary.
    
    Args:
        visit_data: Dictionary containing visit fields.
        partial: If True, only validates fields present in visit_data (for updates).
        
    Returns:
        (is_valid: bool, error_message: Optional[str])
    """
    if not isinstance(visit_data, dict):
        return False, "Visit data must be a dictionary."
    
    # Check required fields when not partial
    if not partial:
        for field in REQUIRED_FIELDS:
            if field not in visit_data or visit_data[field] is None:
                return False, f"Missing required field: '{field}'"
            if isinstance(visit_data[field], str) and not str(visit_data[field]).strip():
                return False, f"Field '{field}' cannot be empty."

    # Validate patient_id
    if "patient_id" in visit_data:
        pid = str(visit_data["patient_id"]).strip()
        if not pid:
            return False, "Field 'patient_id' cannot be empty."

    # Validate department
    if "department" in visit_data:
        dept = visit_data["department"]
        if dept not in VALID_DEPARTMENTS:
            return False, f"Invalid department '{dept}'. Allowed: {VALID_DEPARTMENTS}"

    # Validate priority
    if "priority" in visit_data:
        pri = visit_data["priority"]
        if pri not in VALID_PRIORITIES:
            return False, f"Invalid priority '{pri}'. Allowed: {VALID_PRIORITIES}"

    # Validate numeric fields
    numeric_checks = [
        ("patient_age", int, 0, 130, "Patient age must be an integer between 0 and 130."),
        ("queue_length", int, 0, 1000, "Queue length must be a non-negative integer."),
        ("waiting_time", (int, float), 0, None, "Waiting time cannot be negative."),
        ("consultation_duration", (int, float), 0, None, "Consultation duration cannot be negative.")
    ]
    
    for field, expected_type, min_val, max_val, err_msg in numeric_checks:
        if field in visit_data:
            val = visit_data[field]
            try:
                # Handle numeric strings from CSV
                if expected_type is int:
                    num_val = int(val)
                else:
                    num_val = float(val)
            except (ValueError, TypeError):
                return False, f"Invalid numeric value for '{field}': {val}"
                
            if min_val is not None and num_val < min_val:
                return False, err_msg
            if max_val is not None and num_val > max_val:
                return False, err_msg

    # Validate datetime formats if present
    for dt_field in ["arrival_time", "consultation_start", "consultation_end"]:
        if dt_field in visit_data:
            dt_str = str(visit_data[dt_field]).strip()
            try:
                datetime.strptime(dt_str, DATETIME_FORMAT)
            except ValueError:
                return False, f"Invalid datetime format for '{dt_field}'. Expected '{DATETIME_FORMAT}', got '{dt_str}'."

    return True, None


def clean_visit_record(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize and type-cast a raw dictionary into properly typed DB record.
    """
    return {
        "patient_id": str(raw_record["patient_id"]).strip(),
        "department": str(raw_record["department"]).strip(),
        "arrival_time": str(raw_record["arrival_time"]).strip(),
        "consultation_start": str(raw_record["consultation_start"]).strip(),
        "consultation_end": str(raw_record["consultation_end"]).strip(),
        "doctor_id": str(raw_record["doctor_id"]).strip(),
        "priority": str(raw_record["priority"]).strip(),
        "patient_age": int(raw_record["patient_age"]),
        "queue_length": int(raw_record["queue_length"]),
        "waiting_time": float(raw_record["waiting_time"]),
        "consultation_duration": float(raw_record["consultation_duration"]),
        "day_of_week": str(raw_record["day_of_week"]).strip()
    }


def insert_visit(visit_data: Dict[str, Any], db_path: Optional[str] = None) -> bool:
    """
    Validate and insert a single visit record into the database.
    
    Args:
        visit_data: Dict containing record fields.
        db_path: Optional database file path.
        
    Returns:
        True if inserted successfully.
        
    Raises:
        ValidationError: If input validation fails or duplicate patient_id is provided.
    """
    is_valid, err = validate_visit(visit_data, partial=False)
    if not is_valid:
        raise ValidationError(f"Validation failed: {err}")
    
    clean_data = clean_visit_record(visit_data)
    
    path = db_path or DEFAULT_DB_PATH
    init_db(path)
    
    try:
        with _managed_connection(path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO hospital_visits (
                    patient_id, department, arrival_time, consultation_start,
                    consultation_end, doctor_id, priority, patient_age,
                    queue_length, waiting_time, consultation_duration, day_of_week
                ) VALUES (
                    :patient_id, :department, :arrival_time, :consultation_start,
                    :consultation_end, :doctor_id, :priority, :patient_age,
                    :queue_length, :waiting_time, :consultation_duration, :day_of_week
                )
            """, clean_data)
            conn.commit()
            return True
    except sqlite3.IntegrityError as e:
        raise ValidationError(f"Duplicate or constraint violation for patient '{clean_data['patient_id']}': {e}")


def update_visit(patient_id: str, update_fields: Dict[str, Any], db_path: Optional[str] = None) -> bool:
    """
    Update fields of an existing hospital visit record.
    
    Args:
        patient_id: Patient ID to update.
        update_fields: Dictionary of fields to update.
        db_path: Optional database file path.
        
    Returns:
        True if the record was updated, False if patient_id was not found.
        
    Raises:
        ValidationError: If update_fields contain invalid values.
    """
    if not patient_id or not str(patient_id).strip():
        raise ValidationError("patient_id cannot be empty for update.")
    if not update_fields:
        return False
        
    is_valid, err = validate_visit(update_fields, partial=True)
    if not is_valid:
        raise ValidationError(f"Validation failed for update: {err}")
        
    path = db_path or DEFAULT_DB_PATH
    init_db(path)
    
    # Cast known numeric types if present in update_fields
    sanitized_fields = {}
    for k, v in update_fields.items():
        if k in ("patient_age", "queue_length"):
            sanitized_fields[k] = int(v)
        elif k in ("waiting_time", "consultation_duration"):
            sanitized_fields[k] = float(v)
        else:
            sanitized_fields[k] = str(v).strip()
            
    set_clause = ", ".join([f"{k} = :{k}" for k in sanitized_fields.keys()])
    params = {**sanitized_fields, "patient_id": str(patient_id).strip()}
    
    with _managed_connection(path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            UPDATE hospital_visits
            SET {set_clause}
            WHERE patient_id = :patient_id
        """, params)
        conn.commit()
        return cursor.rowcount > 0


def get_all_visits(db_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fetch all visit records from the database ordered by arrival_time.
    
    Returns:
        List of visit records as dictionaries.
    """
    path = db_path or DEFAULT_DB_PATH
    init_db(path)
    
    with _managed_connection(path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM hospital_visits ORDER BY arrival_time ASC")
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


def get_visits_by_department(department: str, db_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fetch visit records filtered by department.
    
    Args:
        department: Hospital department name.
        db_path: Optional database file path.
        
    Returns:
        List of matching visit records as dictionaries.
    """
    if department not in VALID_DEPARTMENTS:
        raise ValidationError(f"Invalid department '{department}'. Allowed: {VALID_DEPARTMENTS}")
        
    path = db_path or DEFAULT_DB_PATH
    init_db(path)
    
    with _managed_connection(path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM hospital_visits WHERE department = ? ORDER BY arrival_time ASC",
            (department,)
        )
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


def get_visit_by_id(patient_id: str, db_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Fetch a single visit record by patient_id.
    """
    path = db_path or DEFAULT_DB_PATH
    init_db(path)
    
    with _managed_connection(path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM hospital_visits WHERE patient_id = ?", (str(patient_id).strip(),))
        row = cursor.fetchone()
        return dict(row) if row else None


def import_csv(
    csv_path: Optional[str] = None,
    db_path: Optional[str] = None,
    ignore_existing: bool = True
) -> Dict[str, Any]:
    """
    Import hospital visit records from a CSV file into SQLite.
    Safely validates rows and catches invalid/duplicate records.
    
    Args:
        csv_path: Path to CSV file (defaults to data/hospital_visits.csv).
        db_path: Path to SQLite DB (defaults to data/mediqueue.db).
        ignore_existing: If True, duplicate records are skipped without halting import.
        
    Returns:
        Dictionary with import summary:
        {
            "total_rows": int,
            "imported": int,
            "skipped": int,
            "errors": List[str]
        }
    """
    source_file = csv_path or DEFAULT_CSV_PATH
    target_db = db_path or DEFAULT_DB_PATH
    
    if not os.path.exists(source_file):
        raise FileNotFoundError(f"CSV dataset not found at: {source_file}")
        
    init_db(target_db)
    
    summary = {
        "total_rows": 0,
        "imported": 0,
        "skipped": 0,
        "errors": []
    }
    
    records_to_insert = []
    
    with open(source_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_idx, row in enumerate(reader, start=2): # header is row 1
            summary["total_rows"] += 1
            
            # Validation
            is_valid, err = validate_visit(row, partial=False)
            if not is_valid:
                summary["skipped"] += 1
                summary["errors"].append(f"Row {row_idx}: {err}")
                continue
                
            try:
                clean_data = clean_visit_record(row)
                records_to_insert.append(clean_data)
            except Exception as e:
                summary["skipped"] += 1
                summary["errors"].append(f"Row {row_idx}: Parsing failed - {e}")
                
    # Bulk insert with integrity handling
    with _managed_connection(target_db) as conn:
        cursor = conn.cursor()
        for rec in records_to_insert:
            try:
                cursor.execute("""
                    INSERT INTO hospital_visits (
                        patient_id, department, arrival_time, consultation_start,
                        consultation_end, doctor_id, priority, patient_age,
                        queue_length, waiting_time, consultation_duration, day_of_week
                    ) VALUES (
                        :patient_id, :department, :arrival_time, :consultation_start,
                        :consultation_end, :doctor_id, :priority, :patient_age,
                        :queue_length, :waiting_time, :consultation_duration, :day_of_week
                    )
                """, rec)
                summary["imported"] += 1
            except sqlite3.IntegrityError as e:
                summary["skipped"] += 1
                if not ignore_existing:
                    summary["errors"].append(f"Integrity error for {rec.get('patient_id')}: {e}")
                else:
                    summary["errors"].append(f"Duplicate skipped for patient_id: {rec.get('patient_id')}")
        conn.commit()
        
    return summary


def get_database_stats(db_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Compute basic database metrics and distributions from SQLite.
    
    Returns:
        Dictionary containing counts, department distributions, priority breakdowns,
        and wait-time summaries.
    """
    path = db_path or DEFAULT_DB_PATH
    init_db(path)
    
    with _managed_connection(path) as conn:
        cursor = conn.cursor()
        
        # Total records
        cursor.execute("SELECT COUNT(*) AS total FROM hospital_visits")
        total = cursor.fetchone()["total"]
        
        if total == 0:
            return {
                "total_visits": 0,
                "department_counts": {},
                "priority_counts": {},
                "avg_waiting_time": 0.0,
                "avg_consultation_duration": 0.0,
                "min_waiting_time": 0.0,
                "max_waiting_time": 0.0,
                "busiest_department": None,
                "day_distribution": {}
            }
            
        # Department counts
        cursor.execute("""
            SELECT department, COUNT(*) AS count
            FROM hospital_visits
            GROUP BY department
            ORDER BY count DESC
        """)
        department_counts = {row["department"]: row["count"] for row in cursor.fetchall()}
        
        # Priority counts
        cursor.execute("""
            SELECT priority, COUNT(*) AS count
            FROM hospital_visits
            GROUP BY priority
            ORDER BY count DESC
        """)
        priority_counts = {row["priority"]: row["count"] for row in cursor.fetchall()}
        
        # Aggregate statistics
        cursor.execute("""
            SELECT 
                AVG(waiting_time) AS avg_wait,
                MIN(waiting_time) AS min_wait,
                MAX(waiting_time) AS max_wait,
                AVG(consultation_duration) AS avg_consult
            FROM hospital_visits
        """)
        agg = cursor.fetchone()
        
        # Day of week distribution
        cursor.execute("""
            SELECT day_of_week, COUNT(*) AS count
            FROM hospital_visits
            GROUP BY day_of_week
            ORDER BY count DESC
        """)
        day_distribution = {row["day_of_week"]: row["count"] for row in cursor.fetchall()}
        
        busiest_dept = max(department_counts, key=department_counts.get) if department_counts else None
        
        return {
            "total_visits": total,
            "department_counts": department_counts,
            "priority_counts": priority_counts,
            "avg_waiting_time": round(agg["avg_wait"] or 0.0, 2),
            "avg_consultation_duration": round(agg["avg_consult"] or 0.0, 2),
            "min_waiting_time": round(agg["min_wait"] or 0.0, 2),
            "max_waiting_time": round(agg["max_wait"] or 0.0, 2),
            "busiest_department": busiest_dept,
            "day_distribution": day_distribution
        }


if __name__ == "__main__":
    # Test execution when run directly
    print("Initializing MediQueue SQLite Database...")
    init_db()
    print("Importing sample dataset...")
    import_result = import_csv()
    print("Import Summary:", import_result)
    print("\nDatabase Statistics:")
    stats = get_database_stats()
    for k, v in stats.items():
        print(f"  {k}: {v}")
