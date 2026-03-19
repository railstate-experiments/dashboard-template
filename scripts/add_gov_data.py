#!/usr/bin/env python3
"""
Add government/public filing data to a commodity's monthly comparison.

Usage:
    python add_gov_data.py lpg_cross_border 2026-01 3920
    python add_gov_data.py cp 2025-12 108400
    python add_gov_data.py glencore 2025-H2 67200
"""

import json
import sys
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"


def main():
    if len(sys.argv) != 4:
        print("Usage: python add_gov_data.py <commodity_id> <period> <value>")
        print("\nAvailable commodity IDs:")
        for f in sorted(DATA_DIR.glob("*.json")):
            with open(f) as fh:
                d = json.load(fh)
            print(f"  {f.stem:<28} ({d.get('gov_source_label', '?')})")
        sys.exit(1)

    cid, period, value = sys.argv[1], sys.argv[2], int(sys.argv[3].replace(",", ""))
    filepath = DATA_DIR / f"{cid}.json"

    with open(filepath, "r") as f:
        data = json.load(f)

    # Find existing month or add new
    found = False
    for row in data["monthly"]["rows"]:
        if row[0] == period:
            old = row[2]
            row[2] = value
            found = True
            print(f"Updated: {period} gov = {value:,} (was {old})")
            break

    if not found:
        data["monthly"]["rows"].append([period, None, value])
        data["monthly"]["rows"].sort(key=lambda r: r[0])
        print(f"Added: {period} gov = {value:,}")

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    print(f"✓ Saved {filepath.name}")


if __name__ == "__main__":
    main()
