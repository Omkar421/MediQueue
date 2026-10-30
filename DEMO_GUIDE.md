# 🎯 MediQueue – College Demonstration Guide
### Final Year TY BSc IT Capstone Project Presentation & Live Demo Walkthrough

---

## ⏱️ Quick Elevator Pitch (30-Second Intro for External Examiners)

> *"Good morning/afternoon, Respected Examiners. Our project is **MediQueue: Hospital Waiting Time Analyzer and Queue Optimization System**.*  
> *In modern outpatient healthcare, unmanaged patient queues lead to overcrowding, prolonged delays, and physician workload imbalance.*  
> *MediQueue implements a real-world Big Data analytics pipeline using **Apache PySpark** and an interactive **Streamlit** command center. It processes transactional outpatient records, aggregates queue dynamics across departments, classifies waiting delays into operational severity tiers, and delivers automated staffing recommendations and executive PDF reports—all without requiring cloud dependencies or expensive server hardware."*

---

## 🚀 Step 1: Launching the Application

1. Open PowerShell or Terminal in the project root:
   ```powershell
   cd "C:\Users\JAGRUTI\.gemini\antigravity\scratch\MediQueue"
   ```

2. Start the Streamlit application:
   ```powershell
   streamlit run app.py
   ```

3. The application will launch automatically in your default browser at:
   `http://localhost:8501`

*(Notice: The entire application operates 100% offline. No internet connection or external cloud API is needed.)*

---

## 🧭 Step 2: Six-Page Live Demo Walkthrough

### Page 1: Overview Dashboard (Command Center)
- **What to show:**
  - **Header Telemetry:** Point out the `200 Visits Synced`, `PySpark 3.5.9 local[1]`, and dynamic `🕒 Synced` timestamp.
  - **4 Top KPI Cards:**
    - **Total Patient Visits:** `200`
    - **Average Hospital Wait Time:** `38.9 min`
    - **Critical Delays (>60m):** `41 patients (20.5%)`
    - **Urgent Triage Cases:** `73 patients (High + Critical)`
  - **Visualizations:**
    - *Department Wait Time Bar Chart:* Highlight that Emergency has the lowest wait (~8.9 min) while General Medicine and Cardiology experience higher queues.
    - *Triage Urgency Donut Chart:* Show the distribution across Critical (26), High (47), Medium (95), and Low (32).
    - *Waiting Time Severity Breakdown:* Explain the 4 operational categories.
    - *Hourly Influx Area Chart:* Show arrival surges occurring between 10:00 AM and 12:00 PM.
  - **What to say to the examiner:**
    > *"All metrics displayed on this dashboard originate directly from our Apache PySpark aggregation engine, not static hardcoded values. The dashboard caches the Spark DataFrame output to keep reruns instantaneous."*

---

### Page 2: Department Analytics (Bottleneck Detection)
- **What to show:**
  - **Wait Time vs Consultation Duration Chart:** Compare waiting friction against actual physician consultation length.
  - **Department League Table:** Show the ranking generated using PySpark Window function `dense_rank()`.
    - Rank 1 (Longest Wait): General Medicine (~51.5 min)
    - Rank 2: Cardiology (~51.4 min)
    - Rank 3: Orthopedics (~45.9 min)
    - Rank 4: Pediatrics (~34.9 min)
    - Rank 5 (Fastest): Emergency (~8.9 min)
  - **Rule-Based Staffing Recommendations:** Explain how the system flags departments whose average wait exceeds the hospital benchmark (38.9 min) by >15% and advises administrators to deploy auxiliary consulting doctors.
  - **What to say to the examiner:**
    > *"Notice how Emergency has the lowest wait time despite handling high patient volume. This verifies our clinical priority weighting logic, where Critical and High priority patients bypass standard OPD queues."*

---

### Page 3: Live Patient Queue (Transactional Telemetry)
- **What to show:**
  - **Filter Console:** 
    - Filter by **Department** (e.g. select `Emergency` only ➔ table instantly updates to 40 emergency patients).
    - Filter by **Priority** (e.g. select `Critical` ➔ shows rapid turnaround records).
    - Filter by **Wait Category** (e.g. `Critical Delay (>60m)`).
    - Click **"🔄 Reset Filters"** to restore all 200 visits with a single click.
  - **Live Search Bar:** Type `DOC-EM-01` or `P1005` to demonstrate instant record lookup.
  - **What to say to the examiner:**
    > *"This view connects directly to our SQLite transactional database (`mediqueue.db`). It demonstrates operational queue surveillance where hospital desk staff can monitor patient wait status in real time."*

---

### Page 4: In-Depth Waiting Time Analysis
- **What to show:**
  - **4 Severity Cards:** 
    - Short Wait (0–15m): 47 patients (23.5%)
    - Moderate Wait (16–30m): 40 patients (20.0%)
    - Long Wait (31–60m): 72 patients (36.0%)
    - Critical Delay (>60m): 41 patients (20.5%)
  - **Department Delay Range Chart:** Displays Minimum, Average, and Peak wait times per department.
  - **Weekly Traffic & Waiting Profile:** Dual-axis chart highlighting patient arrival influx across Monday through Sunday.
  - **Hourly Queue Density:** Shows correlation between check-in arrival hour and queue depth ahead.

---

### Page 5: Reports & Analytics Export
- **What to show:**
  - Click **"📥 Download Executive Summary PDF Report"**: Open the generated PDF to show examiners a publication-ready management brief with executive KPIs, department rankings, and staffing actions.
  - Click **"📥 Download Full Visits Dataset (CSV)"**: Show the 200 raw transactional visit records.
  - Click **"📥 Department Rankings (CSV)"**: Show the PySpark aggregated metric export.
  - **What to say to the examiner:**
    > *"Hospital administrators can generate and download executive summary reports on-demand for operational audits without requiring third-party cloud reporting software."*

---

### Page 6: About MediQueue & Architecture
- **What to show:**
  - Core architectural overview separating SQLite (OLTP) and Apache PySpark (OLAP).

---

## ⚡ Step 3: Explaining the PySpark Big Data Component

When the examiner asks: *"Why do you need Apache Spark for 200 records?"*
- **Answer:**
  1. **Scalability Demonstration:** In real-world enterprise hospital chains, telemetry generates millions of check-in records daily. PySpark provides the exact distributed DataFrame API, Window functions, and SQL transformations that run on multi-node clusters in production.
  2. **Architectural Separation (OLTP vs OLAP):** SQLite manages the fast transactional operations (adding and updating patients), while PySpark executes the heavy analytical aggregations (grouping, ranking, peak-hour distributions).
  3. **Low-Resource Engineering (`local[1]`):** We specifically tuned the SparkSession to `local[1]` with a 1 GB driver memory limit and disabled the web UI. This guarantees zero out-of-memory crashes on standard 8 GB student laptops while executing genuine Apache Spark bytecode.

---

## 🧪 Step 4: Demonstrating the Automated Test Suites

Show the examiner that the system is backed by empirical unit tests:

1. **Database Layer Tests (18 tests):**
   ```powershell
   python -m unittest test_database.py -v
   ```
   *Verifies schema creation, CRUD operations, duplicate detection, and boundary validation.*

2. **PySpark Analytics Tests (10 tests):**
   ```powershell
   python -m unittest test_analytics.py -v
   ```
   *Verifies Spark aggregations, Window ranking, priority analysis, and crash-safety on empty datasets.*

---

## 🛡️ Demonstration Safety Checklist

- [x] Python 3.11 with OpenJDK 17 configured.
- [x] PySpark worker environment variables explicitly set to prevent version mismatches.
- [x] SQLite database contains all 200 synthetic records across all 5 departments and 4 priority levels.
- [x] `@st.cache_data` enabled so filter clicks render instantly without waiting for Spark recalculation.
- [x] Executive PDF and CSV export buttons tested and fully functional.
- [x] 100% offline execution ready.
