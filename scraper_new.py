import asyncio
from aiohttp import ClientSession, ClientTimeout
from bs4 import BeautifulSoup
from datetime import datetime
from tqdm import tqdm
import json
import os
import re
import csv

VERGIL_ROOT = "https://vergil.registrar.columbia.edu"
UWB_ROOT = "http://www.columbia.edu/cu/bulletin/uwb/sel"
TERM_WORDS = [None, "Spring", "Summer", "Fall"]

# ------------------------------------------------------
# Save JSON locally
# ------------------------------------------------------
def save_results_locally(data, folder="output"):
    os.makedirs(folder, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    json_path = os.path.join(folder, f"vergil_data_{timestamp}.json")
    with open(json_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\nSaved {len(data)} courses → {json_path}")
    return json_path

# ------------------------------------------------------
# Save CSV
# ------------------------------------------------------
def save_results_csv(data, folder="output"):
    if not data:
        print("No courses to save as CSV.")
        return None

    os.makedirs(folder, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    csv_path = os.path.join(folder, f"vergil_data_{timestamp}.csv")

    # Collect all possible keys
    all_keys = set()
    for course in data:
        all_keys.update(course.keys())
    all_keys = sorted(all_keys)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys)
        writer.writeheader()
        writer.writerows(data)

    print(f"Saved {len(data)} courses → {csv_path}")
    return csv_path

# ------------------------------------------------------
# Generic fetch helper
# ------------------------------------------------------
async def fetch(session, url, params=None, retries=3):
    for _ in range(retries):
        try:
            async with session.get(url, params=params) as r:
                return await r.read()
        except:
            await asyncio.sleep(0.4)
    return None

# ------------------------------------------------------
# Parse department codes from UWB A–Z page
# ------------------------------------------------------
def parse_department_page(html, semester):
    soup = BeautifulSoup(html, "lxml")

    # Accept 2–6 letter department codes (fix for UNHS, UNHI, GUHIS, EAAS, etc.)
    regex = re.compile(r"/(\w{2,6})_" + re.escape(semester) + r"\.html")

    depts = []
    rows = soup.select("table tr")[3:-1]  # matches your original working logic

    for row in rows:
        for a in row.find_all("a"):
            href = a.get("href", "")
            m = regex.search(href)
            if m:
                depts.append(m.group(1))

    return depts

# ------------------------------------------------------
# Main scraper
# ------------------------------------------------------
def scrape_courses(term_code):
    semester = TERM_WORDS[int(term_code[-1])] + term_code[:-1]
    print(f"\n=== Scraping {semester} ===")
    print("> Fetching departments…")

    # -----------------------------
    # Get department list
    # -----------------------------
    async def get_departments():
        timeout = ClientTimeout(total=30)
        async with ClientSession(timeout=timeout) as session:
            tasks = [
                fetch(session, f"{UWB_ROOT}/dept-{chr(c)}.html")
                for c in range(65, 91)
            ]
            pages = await asyncio.gather(*tasks)

            all_depts = []
            for page in pages:
                if not page:
                    continue
                all_depts += parse_department_page(page, semester)

            return sorted(set(all_depts))

    departments = asyncio.run(get_departments())
    print(f"> Found {len(departments)} departments from UWB")

    # -----------------------------
    # Add hardcoded Columbia History fallback departments
    # -----------------------------
    MANUAL_DEPTS = ["HIST", "UNHS", "UNHI", "GUHIS"]
    departments = sorted(set(departments + MANUAL_DEPTS))
    print(f"> Total departments (including manual): {len(departments)}")
    print("> Added manual fallbacks:", MANUAL_DEPTS)

    # -----------------------------
    # Scrape courses
    # -----------------------------
    async def get_courses():
        timeout = ClientTimeout(total=50)
        async with ClientSession(timeout=timeout) as session:
            all_courses = []

            for dept in tqdm(departments, desc="Scraping departments"):
                params = {
                    "dept": dept,
                    "key": "*",
                    "moreresults": "2",
                    "term": term_code,
                }

                raw = await fetch(session, f"{VERGIL_ROOT}/doc-adv-queries.php", params=params)
                if not raw:
                    print(f"!! No response for {dept}")
                    continue

                # Fix JSON prefix (Columbia adds junk before '[')
                text = raw.decode("utf-8", errors="ignore")
                json_start = text.find("[")
                if json_start == -1:
                    print(f"!! No JSON array found for {dept}")
                    continue

                try:
                    data = json.loads(text[json_start:])
                    all_courses.extend(data)
                except Exception as e:
                    print(f"!! JSON error for {dept}: {e}")

            # Deduplicate by course ID if exists
            seen = set()
            deduped = []
            for c in all_courses:
                cid = c.get("id") or c.get("CourseNumber") or str(c)
                if cid not in seen:
                    deduped.append(c)
                    seen.add(cid)

            return deduped

    # Execute async scraping
    all_courses = asyncio.run(get_courses())

    # -----------------------------
    # Save results
    # -----------------------------
    save_results_locally(all_courses)
    save_results_csv(all_courses)

# ------------------------------------------------------
# Run
# ------------------------------------------------------
if __name__ == "__main__":
    scrape_courses("20253")  #  # CHANGE THIS — <yearsemester> (spring - 1, summer - 2, fall 3)
