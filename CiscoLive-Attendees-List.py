import csv
import json
import os
import glob
from collections import defaultdict
from datetime import datetime

# ── Source file handling: pick the most recent CSV if multiple exist ──────────
csv_files = glob.glob("*.csv")

if not csv_files:
    print("Error: No CSV files found in the current directory.")
    exit(1)

# Sort by last modified time and pick the most recent one
input_file = max(csv_files, key=os.path.getmtime)
print(f"Using most recent CSV file: {input_file} "
      f"(modified: {datetime.fromtimestamp(os.path.getmtime(input_file)).strftime('%Y-%m-%d %H:%M:%S')})")

# ── Parse CSV ─────────────────────────────────────────────────────────────────
job_title_data = defaultdict(lambda: {"companies": defaultdict(int)})
session_codes  = set()
total_attendees = 0

try:
    with open(input_file, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)

        # Validate required columns exist
        required_columns = {"SESSION CODE", "FIRST NAME", "LAST NAME", "COMPANY NAME", "JOB TITLE"}
        missing_columns  = required_columns - set(reader.fieldnames)
        if missing_columns:
            print(f"Error: Missing columns in CSV: {missing_columns}")
            exit(1)

        for row in reader:
            job_title = row["JOB TITLE"].strip()
            company   = row["COMPANY NAME"].strip()
            session_codes.add(row["SESSION CODE"].strip())

            job_title_data[job_title]["companies"][company] += 1
            total_attendees += 1  # Count every row as one attendee

except FileNotFoundError:
    print(f"Error: File '{input_file}' not found.")
    exit(1)
except Exception as e:
    print(f"Error reading CSV: {e}")
    exit(1)

# ── Output file named after session code(s) ───────────────────────────────────
# If multiple session codes exist, join them with underscore e.g. BRKENT-3115_BRKENT-3116.json
session_code_str = "_".join(sorted(session_codes))
output_file      = f"{session_code_str}.json"

if os.path.exists(output_file):
    print(f"Warning: '{output_file}' already exists — it will be overwritten.")

# ── Build JSON structure ──────────────────────────────────────────────────────
output = {
    "generated_at":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "source_file":      input_file,
    "session_codes":    sorted(list(session_codes)),
    "total_attendees":  total_attendees,
    "total_job_titles": len(job_title_data),
    "job_titles": [
        {
            "job_title":       job_title,
            "total_companies": len(data["companies"]),
            "total_members":   sum(data["companies"].values()),  # sum across all companies
            "companies": [
                {
                    "company_name":  company,
                    "total_members": count
                }
                for company, count in sorted(data["companies"].items())
            ]
        }
        for job_title, data in sorted(job_title_data.items())
    ]
}

# ── Write JSON (overwrite if exists) ─────────────────────────────────────────
try:
    with open(output_file, "w", encoding="utf-8") as jsonfile:
        json.dump(output, jsonfile, indent=4)
    print(f"Done! JSON file created/overwritten: {output_file}")
except Exception as e:
    print(f"Error writing JSON: {e}")
    exit(1)