import csv
import json
import os
from collections import defaultdict

input_file = "BRKENT-3115.csv"  # Change this to your CSV file path
output_file = "output.json"  # Single output JSON file

job_title_data = defaultdict(lambda: {"job_title": "", "companies": defaultdict(list)})

with open(input_file, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        job_title = row["JOB TITLE"].strip()
        company = row["COMPANY NAME"].strip()

        job_title_data[job_title]["job_title"] = job_title
        job_title_data[job_title]["companies"][company].append({
            "session_code": row["SESSION CODE"].strip(),
            "first_name": row["FIRST NAME"].strip(),
            "last_name": row["LAST NAME"].strip()
        })

# Build the single unified JSON structure
output = {
    "total_job_titles": len(job_title_data),
    "job_titles": [
        {
            "job_title": job_title,
            "total_companies": len(data["companies"]),
            "companies": [
                {
                    "company_name": company,
                    "total_members": len(members),
                    "members": members
                }
                for company, members in sorted(data["companies"].items())
            ]
        }
        for job_title, data in sorted(job_title_data.items())
    ]
}

with open(output_file, "w", encoding="utf-8") as jsonfile:
    json.dump(output, jsonfile, indent=4)

print(f"Done! JSON file created: {output_file}")