import csv
import json
import io
import os
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

# ── Thread-safe stats ─────────────────────────────────────────────────────────
stats_lock = threading.Lock()
STATS_FILE = "usage_stats.json"

def load_stats():
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "total_page_visits":         0,
        "total_files_processed":     0,
        "total_attendees_processed": 0,
        "first_visit":               None,
        "last_visit":                None,
        "last_upload":               None,
        "daily_visits":              {},
        "daily_uploads":             {},
        "recent_uploads":            [],
        "hourly_visits":             {},
        "unique_ips":                [],
    }

def save_stats(s):
    try:
        with open(STATS_FILE, "w") as f:
            json.dump(s, f, indent=2)
    except Exception:
        pass

STATS = load_stats()

def record_visit(ip):
    with stats_lock:
        now      = datetime.now()
        date_key = now.strftime("%Y-%m-%d")
        hour_key = now.strftime("%H")
        STATS["total_page_visits"] += 1
        STATS["last_visit"]         = now.strftime("%Y-%m-%d %H:%M:%S")
        if not STATS["first_visit"]:
            STATS["first_visit"]    = now.strftime("%Y-%m-%d %H:%M:%S")
        STATS["daily_visits"][date_key]  = STATS["daily_visits"].get(date_key, 0) + 1
        STATS["hourly_visits"][hour_key] = STATS["hourly_visits"].get(hour_key, 0) + 1
        if ip and ip not in STATS["unique_ips"]:
            STATS["unique_ips"].append(ip)
            if len(STATS["unique_ips"]) > 500:
                STATS["unique_ips"] = STATS["unique_ips"][-500:]
        save_stats(STATS)

def record_upload(filename, attendees, job_titles, session_codes):
    with stats_lock:
        now      = datetime.now()
        date_key = now.strftime("%Y-%m-%d")
        STATS["total_files_processed"]       += 1
        STATS["total_attendees_processed"]   += attendees
        STATS["last_upload"]                  = now.strftime("%Y-%m-%d %H:%M:%S")
        STATS["daily_uploads"][date_key]      = STATS["daily_uploads"].get(date_key, 0) + 1
        STATS["recent_uploads"].insert(0, {
            "time":          now.strftime("%Y-%m-%d %H:%M:%S"),
            "filename":      filename,
            "attendees":     attendees,
            "job_titles":    job_titles,
            "session_codes": session_codes,
        })
        STATS["recent_uploads"] = STATS["recent_uploads"][:20]
        save_stats(STATS)

# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    ip = request.headers.get("X-Forwarded-For", request.remote_addr)
    record_visit(ip.split(",")[0].strip() if ip else "unknown")
    return render_template("index.html")

@app.route("/process", methods=["POST"])
def process():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded."}), 400
    file = request.files["file"]
    if not file.filename.endswith(".csv"):
        return jsonify({"error": "Only CSV files are accepted."}), 400
    try:
        stream  = io.StringIO(file.stream.read().decode("utf-8-sig"))
        reader  = csv.DictReader(stream)
        required = {"SESSION CODE", "FIRST NAME", "LAST NAME", "COMPANY NAME", "JOB TITLE"}
        missing  = required - set(reader.fieldnames)
        if missing:
            return jsonify({"error": f"Missing columns: {', '.join(missing)}"}), 400

        job_title_data  = defaultdict(lambda: {"companies": defaultdict(int)})
        session_codes   = set()
        total_attendees = 0

        for row in reader:
            job_title = row["JOB TITLE"].strip()
            company   = row["COMPANY NAME"].strip()
            session_codes.add(row["SESSION CODE"].strip())
            job_title_data[job_title]["companies"][company] += 1
            total_attendees += 1

        output = {
            "generated_at":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source_file":      file.filename,
            "session_codes":    sorted(list(session_codes)),
            "total_attendees":  total_attendees,
            "total_job_titles": len(job_title_data),
            "job_titles": [
                {
                    "job_title":       jt,
                    "total_companies": len(d["companies"]),
                    "total_members":   sum(d["companies"].values()),
                    "companies": [
                        {"company_name": c, "total_members": n}
                        for c, n in sorted(d["companies"].items())
                    ]
                }
                for jt, d in sorted(job_title_data.items())
            ]
        }

        record_upload(
            filename      = file.filename,
            attendees     = total_attendees,
            job_titles    = len(job_title_data),
            session_codes = sorted(list(session_codes))
        )

        session_code_str = "_".join(sorted(session_codes))
        return jsonify({"filename": f"{session_code_str}.json", "data": output})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/stats")
def stats_api():
    with stats_lock:
        s = json.loads(json.dumps(STATS))  # deep copy

    today = datetime.now()
    trend = []
    for i in range(6, -1, -1):
        day = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        trend.append({
            "date":    day,
            "label":   (today - timedelta(days=i)).strftime("%b %d"),
            "visits":  s["daily_visits"].get(day, 0),
            "uploads": s["daily_uploads"].get(day, 0),
        })

    hourly = [
        {"hour": f"{h:02d}:00", "visits": s["hourly_visits"].get(f"{h:02d}", 0)}
        for h in range(24)
    ]

    return jsonify({
        "total_page_visits":         s["total_page_visits"],
        "total_files_processed":     s["total_files_processed"],
        "total_attendees_processed": s["total_attendees_processed"],
        "unique_visitors":           len(s["unique_ips"]),
        "first_visit":               s["first_visit"]  or "N/A",
        "last_visit":                s["last_visit"]   or "N/A",
        "last_upload":               s["last_upload"]  or "N/A",
        "recent_uploads":            s["recent_uploads"],
        "trend_7days":               trend,
        "hourly_distribution":       hourly,
    })

@app.route("/stats/reset", methods=["POST"])
def reset_stats():
    global STATS
    with stats_lock:
        STATS = {
            "total_page_visits": 0, "total_files_processed": 0,
            "total_attendees_processed": 0, "first_visit": None,
            "last_visit": None, "last_upload": None,
            "daily_visits": {}, "daily_uploads": {},
            "recent_uploads": [], "hourly_visits": {}, "unique_ips": [],
        }
        if os.path.exists(STATS_FILE):
            os.remove(STATS_FILE)
    return jsonify({"message": "Stats reset successfully."})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80, debug=False)
