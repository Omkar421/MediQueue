"""
Automated genuine screenshot capture for MediQueue.
Captures high-resolution, presentation-ready screenshots of all 6 dashboard views.
"""

import os
import time
import urllib.parse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

BASE_URL = "http://localhost:8505"
OUTPUT_DIR = os.path.abspath("screenshots")
os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGES = [
    ("Overview Dashboard", "dashboard.png"),
    ("Department Analytics", "department-analytics.png"),
    ("Patient Queue", "patient-queue.png"),
    ("Waiting Time Analysis", "waiting-time-analysis.png"),
    ("Reports & Downloads", "reports-downloads.png"),
    ("About MediQueue", "about-mediqueue.png")
]

def capture_all():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1600,1100")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--force-device-scale-factor=1")

    driver = webdriver.Chrome(options=chrome_options)

    try:
        for page_name, filename in PAGES:
            target_url = f"{BASE_URL}/?page={urllib.parse.quote(page_name)}"
            print(f"[*] Navigating to: {page_name} -> {target_url}")
            driver.get(target_url)

            # Wait for dashboard-header to appear
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "dashboard-header"))
                )
            except Exception as e:
                print(f"[!] Warning waiting for header on {page_name}: {e}")

            # Additional pause to allow Plotly SVGs, tables, and metric cards to render smoothly
            time.sleep(3.0)

            out_path = os.path.join(OUTPUT_DIR, filename)
            driver.save_screenshot(out_path)
            file_size = os.path.getsize(out_path)
            print(f"[+] Saved: {filename} ({file_size:,} bytes)")

    finally:
        driver.quit()
        print("[*] All screenshots captured and driver closed cleanly.")

if __name__ == "__main__":
    capture_all()
