#!/usr/bin/env python3
"""
TEMPLATE — Script Contract for Daily Updaters

Your scripts (update_lpg.py, update_ethanol.py, etc.) should
follow this pattern. Each script:

  1. Calls the RailState API however you need to
  2. Writes results into the matching JSON file in data/
  3. Follows the JSON schema below

This template handles the file reading/writing part.
You just fill in the API call.
"""

import json
import os
from datetime import datetime
from pathlib import Path

# ── Path to this commodity's data file ──
DATA_FILE = Path(__file__).parent.parent / "data" / "lpg_cross_border.json"


def load_data() -> dict:
    with open(DATA_FILE, "r") as f:
        return json.load(f)


def save_data(data: dict):
    data["last_updated"] = datetime.now().strftime("%Y-%m-%d")
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)


def get_existing_daily_dates(data: dict) -> set:
    return {row[0] for row in data["daily"]["rows"]}


def main():
    # ── Your API key from GitHub Secrets / environment ──
    api_key = os.environ.get("RAILSTATE_API_KEY", "")

    # ══════════════════════════════════════════════════
    # YOUR CODE: call the RailState API
    # Return a list of dicts like:
    #   [{"date": "2026-03-11", "value": 148}, ...]
    # ══════════════════════════════════════════════════

    new_rows = your_api_call_here(api_key)  # Replace this

    # ── Merge into data file ──
    data = load_data()
    existing = get_existing_daily_dates(data)
    added = 0

    for row in new_rows:
        if row["date"] not in existing:
            data["daily"]["rows"].append([row["date"], row["value"]])
            added += 1

    if added > 0:
        # Sort by date
        data["daily"]["rows"].sort(key=lambda r: r[0])

        # Keep last 180 days max
        data["daily"]["rows"] = data["daily"]["rows"][-180:]

        # Recompute monthly RS totals from daily
        monthly_sums = {}
        for r in data["daily"]["rows"]:
            month = r[0][:7]
            monthly_sums[month] = monthly_sums.get(month, 0) + r[1]

        existing_months = {r[0]: i for i, r in enumerate(data["monthly"]["rows"])}
        for month, total in monthly_sums.items():
            if month in existing_months:
                data["monthly"]["rows"][existing_months[month]][1] = total
            else:
                data["monthly"]["rows"].append([month, total, None])
        data["monthly"]["rows"].sort(key=lambda r: r[0])

        save_data(data)
        print(f"✓ Added {added} new daily rows to {DATA_FILE.name}")
    else:
        print(f"– No new data for {DATA_FILE.name}")


if __name__ == "__main__":
    main()
