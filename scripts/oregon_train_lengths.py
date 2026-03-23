#!/usr/bin/env python3
"""
Oregon Train Length Supplement
===============================
Fetches estimatedDimensions.lengthFeet for every Oregon train sighting.
This is a lightweight pass — we only need the sighting ID and length,
not full car-level data.

Patches the train-level CSV with a new length_feet column, and outputs
a standalone JSON for the dashboard distribution chart.

Outputs:
  ../data/oregon_train_lengths.json
"""

import csv
import json
import os
import sys
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin

API_BASE_URL = "https://api.railstate.com"
HARDCODED_API_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJhdWQiOiJhcGkucmFpbHN0YXRlLmNvbSIsImlzcyI6InJhaWxzdGF0ZS5jb20iLCJ1aWQiOiJ5"
    "RWE5aTFSbjljTmhtbEFEQkNYd3BFR3h1eEczIiwidGlkIjoiRGFuIERldm9lIFRlc3RpbmcifQ."
    "8YQJPJE3X2xomrgUCXo41tx0Vh0gCRWRZ2JbQKYpAXE"
)

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
PULL_LOG = DATA_DIR / "oregon_pull_log.json"
OUTPUT = DATA_DIR / "oregon_train_lengths.json"

RESPONSE_SIZE = 500
RATE_LIMIT_DELAY = 0.1
MONTH_DELAY = 1.0
SENSOR_DELAY = 2.0

SENSOR_SUBDIVISION = {
    "Bend, OR": "BNSF Oregon Trunk Sub", "Cold Springs, OR": "UP La Grande Sub",
    "Echo, OR": "UP La Grande Sub", "Eugene, OR": "UP Brooklyn Sub",
    "Haig, OR": "UP Brooklyn Sub", "Irving, OR": "UP Brooklyn Sub",
    "Jefferson, OR": "UP Brooklyn Sub", "Modoc Point, OR": "UP Cascade Sub",
    "N. Portland E, OR": "BNSF Fallbridge Sub", "N. Portland W, OR": "BNSF Fallbridge Sub",
    "Ontario, OR": "UP Nampa Sub", "Salem, OR": "UP Brooklyn Sub",
    "Springfield Jct, OR": "UP Brooklyn Sub", "Troutdale, OR": "UP Graham Line",
    "Worden, OR": "UP Cascade Sub",
}


def fetch_page(url, params, headers):
    for attempt in range(4):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=120)
            if resp.status_code == 429:
                time.sleep(5 * (2 ** attempt))
                continue
            if resp.status_code != 200:
                return None, f"HTTP {resp.status_code}"
            return resp.json(), None
        except Exception as e:
            time.sleep(2)
    return None, "Max retries"


def main():
    api_key = os.environ.get("RAILSTATE_API_KEY", HARDCODED_API_KEY)
    headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}

    with open(PULL_LOG) as f:
        log = json.load(f)

    sensors = log["sensor_summaries"]
    print(f"Oregon Train Length Pull — {len(sensors)} sensors, 12 months")
    print("=" * 55)

    # Build monthly date ranges
    months = []
    for m in range(1, 13):
        m_start = datetime(2025, m, 1)
        m_end = datetime(2025, m + 1, 1) - timedelta(seconds=1) if m < 12 else datetime(2025, 12, 31, 23, 59, 59)
        months.append((m_start, m_end))

    records = []  # {sighting_id, sensor, train_type, length_feet, total_cars, date}
    total_api = 0

    for si, sensor in enumerate(sensors):
        s_name = sensor["name"]
        s_id = sensor["sensor_id"]
        print(f"  [{si+1}/{len(sensors)}] {s_name}", flush=True)

        for m_start, m_end in months:
            m_label = m_start.strftime("%b")
            print(f"    {m_label}...", end=" ", flush=True)

            url = urljoin(API_BASE_URL, "/api/v3/trains/full_sightings")
            params = {
                "sensors": str(s_id),
                "detection_time_from": m_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "detection_time_to": m_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "response_size": RESPONSE_SIZE,
            }

            month_count = 0
            while True:
                total_api += 1
                data, err = fetch_page(url, params, headers)
                if err or not data:
                    break
                sightings = data.get("sightings", [])
                if not sightings:
                    break

                for s in sightings:
                    ed = s.get("estimatedDimensions") or {}
                    length = ed.get("lengthFeet")
                    dt_str = s.get("detectionTimeUTC", "")
                    date = dt_str[:10] if len(dt_str) >= 10 else ""
                    records.append({
                        "sighting_id": s.get("sightingId"),
                        "sensor": s_name,
                        "subdivision": SENSOR_SUBDIVISION.get(s_name, ""),
                        "train_type": s.get("trainType", ""),
                        "train_operator": s.get("trainOperator", ""),
                        "direction": s.get("direction", ""),
                        "date": date,
                        "total_cars": len(s.get("cars", [])),
                        "length_feet": round(length, 1) if length else None,
                        "speed_mph": round(s.get("speedMph", 0), 1) if s.get("speedMph") else None,
                    })
                    month_count += 1

                next_link = data.get("nextRequestLink")
                if not next_link:
                    break
                url, params = next_link, None
                time.sleep(RATE_LIMIT_DELAY)

            print(f"{month_count} trains")
            time.sleep(MONTH_DELAY)

        time.sleep(SENSOR_DELAY)

    print(f"\nTotal records: {len(records):,}")
    print(f"API requests: {total_api}")

    # Count how many have length
    with_length = sum(1 for r in records if r["length_feet"] is not None)
    print(f"With length_feet: {with_length:,} ({100*with_length/len(records):.1f}%)")

    with open(OUTPUT, "w") as f:
        json.dump({"records": records, "total": len(records)}, f)
    print(f"Saved to {OUTPUT.name} ({OUTPUT.stat().st_size/1024/1024:.1f} MB)")


if __name__ == "__main__":
    main()
