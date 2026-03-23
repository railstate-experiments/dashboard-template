#!/usr/bin/env python3
"""
Generate Oregon corridor GeoJSON for the planning map.
Draws line segments between sensors along each corridor,
with properties for volume and train type mix.

Outputs:
  ../data/oregon_corridors.json
"""

import json
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
PLANNING_JSON = DATA_DIR / "oregon_planning_analysis.json"
PULL_LOG = DATA_DIR / "oregon_pull_log.json"
OUTPUT = DATA_DIR / "oregon_corridors.json"

# Corridor definitions: ordered sensor sequences along each subdivision
# Trains flow between consecutive sensors in this list
CORRIDORS = {
    "UP Brooklyn Sub": {
        "sensors": ["Haig, OR", "Salem, OR", "Jefferson, OR", "Irving, OR", "Eugene, OR", "Springfield Jct, OR"],
        "color": "#fbcb0a",  # UP yellow
    },
    "UP Cascade Sub": {
        "sensors": ["Springfield Jct, OR", "Modoc Point, OR", "Worden, OR"],
        "color": "#f59e0b",  # amber
    },
    "UP La Grande Sub": {
        "sensors": ["Troutdale, OR", "Echo, OR", "Cold Springs, OR"],
        "color": "#e05d00",  # orange
    },
    "UP Nampa Sub": {
        "sensors": ["Cold Springs, OR", "Ontario, OR"],
        "color": "#dc2626",  # red
    },
    "UP Graham Line": {
        "sensors": ["Haig, OR", "Troutdale, OR"],
        "color": "#0284c7",  # sky blue
    },
    "BNSF Fallbridge Sub": {
        "sensors": ["N. Portland W, OR", "N. Portland E, OR"],
        "color": "#c75b12",  # BNSF orange
    },
    "BNSF Oregon Trunk Sub": {
        "sensors": ["N. Portland E, OR", "Bend, OR"],
        "color": "#7c3aed",  # purple
    },
}


def main():
    # Load sensor coordinates
    with open(PULL_LOG) as f:
        log = json.load(f)
    coords = {}
    for s in log["sensor_summaries"]:
        coords[s["name"]] = [s["lng"], s["lat"]]

    # Load planning analysis for volumes
    with open(PLANNING_JSON) as f:
        pl = json.load(f)

    # Build sensor volume lookup
    sensor_vol = {}
    for sv in pl["sensor_volumes"]:
        sensor_vol[sv["name"]] = sv

    # Build corridor volume lookup
    corridor_vol = {}
    for cv in pl["corridors_volume"]:
        corridor_vol[cv["subdivision"]] = cv

    months = pl.get("months", [])
    month_labels = pl.get("month_labels", [])

    features = []

    for sub_name, cfg in CORRIDORS.items():
        sensors = cfg["sensors"]
        color = cfg["color"]

        # Get corridor-level stats
        cv = corridor_vol.get(sub_name, {})
        total_trains = cv.get("total_trains", 0)
        by_type = cv.get("by_type", {})
        corridor_monthly = cv.get("monthly", [0] * len(months))

        # Draw line segments between consecutive sensors
        for i in range(len(sensors) - 1):
            s1, s2 = sensors[i], sensors[i + 1]
            c1, c2 = coords.get(s1), coords.get(s2)
            if not c1 or not c2:
                continue

            # Segment volume = average of both endpoint sensor volumes
            sv1 = sensor_vol.get(s1, {})
            sv2 = sensor_vol.get(s2, {})
            v1 = sv1.get("total_trains", 0)
            v2 = sv2.get("total_trains", 0)
            seg_volume = (v1 + v2) // 2

            # Monthly volumes per segment (avg of both endpoints)
            m1 = sv1.get("monthly", [0] * len(months))
            m2 = sv2.get("monthly", [0] * len(months))
            seg_monthly = [(m1[j] + m2[j]) // 2 for j in range(len(months))]

            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [c1, c2],
                },
                "properties": {
                    "subdivision": sub_name,
                    "color": color,
                    "segment": f"{s1} → {s2}",
                    "sensor_a": s1,
                    "sensor_b": s2,
                    "volume": seg_volume,
                    "monthly": json.dumps(seg_monthly),
                    "corridor_total": total_trains,
                    "corridor_by_type": json.dumps(by_type),
                },
            })

    # Also add sensor point features
    for sname, sv in sensor_vol.items():
        c = coords.get(sname)
        if not c:
            continue
        sub = sv.get("subdivision", "")
        by_type = sv.get("by_type", {})
        top3 = sorted(by_type.items(), key=lambda x: -x[1])[:3]
        monthly = sv.get("monthly", [0] * len(months))
        monthly_by_type = sv.get("monthly_by_type", {})

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": c},
            "properties": {
                "type": "sensor",
                "name": sname,
                "subdivision": sub,
                "total_trains": sv["total_trains"],
                "top_types": json.dumps(dict(top3)),
                "monthly": json.dumps(monthly),
                "monthly_by_type": json.dumps(monthly_by_type),
            },
        })

    output = {"type": "FeatureCollection", "features": features,
              "months": months, "month_labels": month_labels}
    with open(OUTPUT, "w") as f:
        json.dump(output, f)

    line_count = sum(1 for f in features if f["geometry"]["type"] == "LineString")
    point_count = sum(1 for f in features if f["geometry"]["type"] == "Point")
    print(f"Generated {line_count} corridor segments + {point_count} sensor points → {OUTPUT.name}")


if __name__ == "__main__":
    main()
