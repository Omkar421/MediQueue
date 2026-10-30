"""
MediQueue - Synthetic Dataset Generator
Generates realistic hospital outpatient and emergency visit records for Step 2.
"""

import os
import csv
import random
from datetime import datetime, timedelta

def generate_hospital_dataset(output_path="data/hospital_visits.csv", total_records=200, seed=42):
    random.seed(seed)
    
    # Department configurations
    departments = {
        "Emergency": {
            "weight": 0.20,
            "doctors": ["DOC-EM-01", "DOC-EM-02", "DOC-EM-03"],
            "age_range": (3, 82),
            "priority_weights": {"Critical": 0.35, "High": 0.40, "Medium": 0.20, "Low": 0.05},
            "consult_range": (15, 35),
            "base_wait_factor": 0.4  # Emergency gets fastest response
        },
        "General Medicine": {
            "weight": 0.26,
            "doctors": ["DOC-GM-01", "DOC-GM-02", "DOC-GM-03", "DOC-GM-04"],
            "age_range": (18, 79),
            "priority_weights": {"Critical": 0.02, "High": 0.15, "Medium": 0.55, "Low": 0.28},
            "consult_range": (10, 20),
            "base_wait_factor": 1.0
        },
        "Cardiology": {
            "weight": 0.18,
            "doctors": ["DOC-CARD-01", "DOC-CARD-02", "DOC-CARD-03"],
            "age_range": (36, 85),
            "priority_weights": {"Critical": 0.15, "High": 0.40, "Medium": 0.35, "Low": 0.10},
            "consult_range": (20, 40),
            "base_wait_factor": 1.1
        },
        "Orthopedics": {
            "weight": 0.17,
            "doctors": ["DOC-ORTHO-01", "DOC-ORTHO-02", "DOC-ORTHO-03"],
            "age_range": (14, 75),
            "priority_weights": {"Critical": 0.03, "High": 0.20, "Medium": 0.52, "Low": 0.25},
            "consult_range": (15, 30),
            "base_wait_factor": 0.95
        },
        "Pediatrics": {
            "weight": 0.19,
            "doctors": ["DOC-PED-01", "DOC-PED-02", "DOC-PED-03"],
            "age_range": (0, 16),
            "priority_weights": {"Critical": 0.05, "High": 0.25, "Medium": 0.50, "Low": 0.20},
            "consult_range": (12, 25),
            "base_wait_factor": 0.8
        }
    }
    
    # Priority wait time base ranges (in minutes)
    priority_base_wait = {
        "Critical": (2, 8),
        "High": (8, 20),
        "Medium": (18, 42),
        "Low": (30, 70)
    }
    
    # Generate dates across a 7-day period (Monday to Sunday)
    start_date = datetime(2026, 10, 19, 8, 0, 0) # Monday 8:00 AM
    
    # Allocate records per department
    dept_names = list(departments.keys())
    # Deterministic allocation to sum exactly to total_records
    dept_counts = {
        "Emergency": 40,
        "General Medicine": 52,
        "Cardiology": 36,
        "Orthopedics": 34,
        "Pediatrics": 38
    }
    assert sum(dept_counts.values()) == total_records
    
    records = []
    patient_seq = 1001
    
    for dept_name, count in dept_counts.items():
        dept_cfg = departments[dept_name]
        
        for _ in range(count):
            patient_id = f"P{patient_seq}"
            patient_seq += 1
            
            # Select priority based on department profile
            priorities = list(dept_cfg["priority_weights"].keys())
            weights = list(dept_cfg["priority_weights"].values())
            priority = random.choices(priorities, weights=weights, k=1)[0]
            
            # Doctor
            doctor_id = random.choice(dept_cfg["doctors"])
            
            # Patient Age
            age = random.randint(dept_cfg["age_range"][0], dept_cfg["age_range"][1])
            
            # Arrival Time: distributed across 7 days
            day_offset = random.randint(0, 6)
            if dept_name == "Emergency":
                # Emergency arrivals happen 24 hours
                hour = random.randint(0, 23)
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
            else:
                # OPD visits usually 08:00 to 18:30 with peak 09:30-13:30
                if random.random() < 0.65:
                    hour = random.randint(9, 13)
                else:
                    hour = random.choice([8, 14, 15, 16, 17, 18])
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                
            arrival_dt = start_date + timedelta(days=day_offset, hours=hour - 8, minutes=minute, seconds=second)
            day_of_week = arrival_dt.strftime("%A")
            
            # Queue length at arrival (0 to 16, higher during peak hours)
            if 9 <= hour <= 13:
                queue_length = random.randint(4, 16)
            elif 14 <= hour <= 17:
                queue_length = random.randint(2, 10)
            else:
                queue_length = random.randint(0, 5)
                
            # Waiting time calculation
            base_min, base_max = priority_base_wait[priority]
            raw_wait = random.uniform(base_min, base_max)
            # Adjust by department factor and queue length
            queue_multiplier = 1.0 + (queue_length * 0.08)
            waiting_time = int(round(raw_wait * dept_cfg["base_wait_factor"] * queue_multiplier))
            
            # Safety clamp: critical must remain <= 15 min, minimum wait >= 1 min
            if priority == "Critical":
                waiting_time = max(1, min(waiting_time, 15))
            else:
                waiting_time = max(2, waiting_time)
                
            # Consultation duration
            c_min, c_max = dept_cfg["consult_range"]
            consultation_duration = random.randint(c_min, c_max)
            
            # Calculate timestamps
            consult_start_dt = arrival_dt + timedelta(minutes=waiting_time)
            consult_end_dt = consult_start_dt + timedelta(minutes=consultation_duration)
            
            records.append({
                "patient_id": patient_id,
                "department": dept_name,
                "arrival_time": arrival_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "consultation_start": consult_start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "consultation_end": consult_end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "doctor_id": doctor_id,
                "priority": priority,
                "patient_age": age,
                "queue_length": queue_length,
                "waiting_time": waiting_time,
                "consultation_duration": consultation_duration,
                "day_of_week": day_of_week
            })
            
    # Sort chronologically by arrival_time
    records.sort(key=lambda x: x["arrival_time"])
    
    # Ensure destination directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Write CSV
    headers = [
        "patient_id", "department", "arrival_time", "consultation_start",
        "consultation_end", "doctor_id", "priority", "patient_age",
        "queue_length", "waiting_time", "consultation_duration", "day_of_week"
    ]
    
    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(records)
        
    print(f"Generated {len(records)} records in {output_path}")
    return records

if __name__ == "__main__":
    generate_hospital_dataset()

