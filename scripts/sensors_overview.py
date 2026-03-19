#!/usr/bin/env python3
"""
Fetch RailState sensor overview for the dashboard overview tab.

Outputs:
    ../data/sensors_overview.json
"""

import json
import os
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests

API_BASE_URL = "https://api.railstate.com"

HARDCODED_API_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJhdWQiOiJhcGkucmFpbHN0YXRlLmNvbSIsImlzcyI6InJhaWxzdGF0ZS5jb20iLCJ1aWQiOiJ5"
    "RWE5aTFSbjljTmhtbEFEQkNYd3BFR3h1eEczIiwidGlkIjoiRGFuIERldm9lIFRlc3RpbmcifQ."
    "8YQJPJE3X2xomrgUCXo41tx0Vh0gCRWRZ2JbQKYpAXE"
)

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
OUTPUT_PATH = DATA_DIR / "sensors_overview.json"


def main():
    api_key = os.environ.get("RAILSTATE_API_KEY", HARDCODED_API_KEY)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    url = urljoin(API_BASE_URL, "/api/v3/sensors/overview")
    headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    sensors = resp.json().get("sensors", [])

    active = [s for s in sensors if s.get("isActive")]
    us_count = sum(1 for s in active if s.get("country") == "United States")
    ca_count = sum(1 for s in active if s.get("country") == "Canada")

    # Build GeoJSON features for active sensors
    features = []
    for s in active:
        lat, lng = s.get("lat"), s.get("lng")
        if lat and lng:
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lng, lat]},
                "properties": {
                    "name": s.get("name", ""),
                    "country": s.get("country", ""),
                    "region": s.get("region", ""),
                    "railways": s.get("railways", []),
                },
            })

    output = {
        "last_updated": datetime.utcnow().strftime("%Y-%m-%d"),
        "total_active": len(active),
        "us_active": us_count,
        "ca_active": ca_count,
        "sensors": {
            "type": "FeatureCollection",
            "features": features,
        },
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Saved {len(active)} active sensors ({us_count} US, {ca_count} CA) to {OUTPUT_PATH.name}")


if __name__ == "__main__":
    main()
