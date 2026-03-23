#!/usr/bin/env python3
"""
Oregon Hazmat Flow Analysis
============================
Reads the raw Oregon car-level CSV and produces analysis JSON files
for the CRISI dashboard Hazardous Material Flows tab.

Outputs:
  ../data/oregon_hazmat_analysis.json  - All hazmat analysis in one file

Sections:
  1. Summary stats
  2. Monthly volume by UN hazard class (stacked bar)
  3. Top UN placards table with commodity names and hazard classes
  4. Hazmat volume by sensor (for bubble map)
  5. Daily hazmat trend (line chart with MAs)
  6. Hazmat by direction (flow analysis)
  7. Hazmat by railroad
  8. Monthly volume by top placard (time series)
  9. Sensor x placard heatmap data
"""

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
CARS_CSV = DATA_DIR / "oregon_cars_raw.csv"
TRAINS_CSV = DATA_DIR / "oregon_trains_raw.csv"
OUTPUT_PATH = DATA_DIR / "oregon_hazmat_analysis.json"
LOG_PATH = DATA_DIR / "oregon_pull_log.json"

# ── UN Placard → Commodity Name & Hazard Class lookup ──
UN_LOOKUP = {
    "UN1075": ("Liquefied Petroleum Gas (LPG)", "2.1 — Flammable Gas"),
    "UN1202": ("Diesel Fuel / Fuel Oil", "3 — Flammable Liquid"),
    "UN3257": ("Elevated Temperature Liquid", "9 — Miscellaneous"),
    "UN1267": ("Crude Petroleum", "3 — Flammable Liquid"),
    "UN1017": ("Chlorine", "2.3 — Toxic Gas"),
    "UN1005": ("Anhydrous Ammonia", "2.3 — Toxic Gas"),
    "UN1824": ("Sodium Hydroxide Solution", "8 — Corrosive"),
    "UN1789": ("Hydrochloric Acid", "8 — Corrosive"),
    "UN1987": ("Denatured Alcohol / Ethanol", "3 — Flammable Liquid"),
    "UN1830": ("Sulfuric Acid", "8 — Corrosive"),
    "UN3295": ("Hydrocarbons, Liquid", "3 — Flammable Liquid"),
    "UN3082": ("Environmentally Hazardous Substance, Liquid", "9 — Miscellaneous"),
    "UN1230": ("Methanol", "3 — Flammable Liquid"),
    "UN1993": ("Flammable Liquid, n.o.s.", "3 — Flammable Liquid"),
    "UN2015": ("Hydrogen Peroxide", "5.1 — Oxidizer"),
    "UN1863": ("Jet Fuel", "3 — Flammable Liquid"),
    "UN1805": ("Phosphoric Acid", "8 — Corrosive"),
    "UN1951": ("Argon, Refrigerated Liquid", "2.2 — Non-Flammable Gas"),
    "UN1262": ("Octanes", "3 — Flammable Liquid"),
    "UN1832": ("Sulfuric Acid, Spent", "8 — Corrosive"),
    "UN3077": ("Environmentally Hazardous Substance, Solid", "9 — Miscellaneous"),
    "UN3475": ("Ethanol and Gasoline Mixture", "3 — Flammable Liquid"),
    "UN1942": ("Ammonium Nitrate", "5.1 — Oxidizer"),
    "UN1268": ("Petroleum Distillates", "3 — Flammable Liquid"),
    "UN2014": ("Hydrogen Peroxide, Aqueous", "5.1 — Oxidizer"),
    "UN1170": ("Ethanol", "3 — Flammable Liquid"),
    "UN2031": ("Nitric Acid", "8 — Corrosive"),
    "UN1791": ("Sodium Hypochlorite Solution", "8 — Corrosive"),
    "UN1866": ("Resin Solution", "3 — Flammable Liquid"),
    "UN2055": ("Styrene Monomer", "3 — Flammable Liquid"),
    "UN1760": ("Corrosive Liquid, n.o.s.", "8 — Corrosive"),
    "UN1823": ("Sodium Hydroxide, Solid", "8 — Corrosive"),
    "UN3264": ("Corrosive Liquid, Acidic, Inorganic", "8 — Corrosive"),
    "UN1203": ("Gasoline", "3 — Flammable Liquid"),
    "UN2672": ("Ammonia Solution", "8 — Corrosive"),
    "UN1977": ("Nitrogen, Refrigerated Liquid", "2.2 — Non-Flammable Gas"),
    "UN1076": ("Phosgene", "2.3 — Toxic Gas"),
    "UN1040": ("Ethylene Oxide", "2.3 — Toxic Gas"),
    "UN2448": ("Sulfur, Molten", "4.1 — Flammable Solid"),
    "UN1831": ("Oleum (Fuming Sulfuric Acid)", "8 — Corrosive"),
    "UN3266": ("Corrosive Liquid, Basic, Inorganic", "8 — Corrosive"),
    "UN2209": ("Formaldehyde Solution", "8 — Corrosive"),
    "UN1972": ("Methane, Refrigerated Liquid (LNG)", "2.1 — Flammable Gas"),
    "UN1294": ("Toluene", "3 — Flammable Liquid"),
    "UN1307": ("Xylenes", "3 — Flammable Liquid"),
    "UN2187": ("Carbon Dioxide, Refrigerated Liquid", "2.2 — Non-Flammable Gas"),
    "UN1090": ("Acetone", "3 — Flammable Liquid"),
    "UN1547": ("Aniline", "6.1 — Toxic"),
    "UN1710": ("Trichloroethylene", "6.1 — Toxic"),
    "UN2312": ("Phenol, Molten", "6.1 — Toxic"),
}

# Hazard class groupings for the stacked bar chart
HAZARD_CLASS_GROUPS = {
    "2 — Gases": ["2.1", "2.2", "2.3"],
    "3 — Flammable Liquids": ["3"],
    "4 — Flammable Solids": ["4.1", "4.2", "4.3"],
    "5 — Oxidizers": ["5.1", "5.2"],
    "6 — Toxic": ["6.1", "6.2"],
    "8 — Corrosives": ["8"],
    "9 — Miscellaneous": ["9"],
}

# ── Oregon sensor → railroad subdivision mapping ──
# Based on sensor coordinates and known UP/BNSF subdivision geography
SENSOR_SUBDIVISION = {
    "Bend, OR":             ("BNSF Oregon Trunk Sub", "BNSF"),
    "Cold Springs, OR":     ("UP La Grande Sub", "UP"),
    "Echo, OR":             ("UP La Grande Sub", "UP"),
    "Eugene, OR":           ("UP Brooklyn Sub", "UP"),
    "Haig, OR":             ("UP Brooklyn Sub", "UP"),
    "Irving, OR":           ("UP Brooklyn Sub", "UP"),
    "Jefferson, OR":        ("UP Brooklyn Sub", "UP"),
    "Modoc Point, OR":      ("UP Cascade Sub", "UP"),
    "N. Portland E, OR":    ("BNSF Fallbridge Sub", "BNSF"),
    "N. Portland W, OR":    ("BNSF Fallbridge Sub", "BNSF"),
    "Ontario, OR":          ("UP Nampa Sub", "UP"),
    "Salem, OR":            ("UP Brooklyn Sub", "UP"),
    "Springfield Jct, OR":  ("UP Brooklyn Sub", "UP"),
    "Troutdale, OR":        ("UP Graham Line", "UP"),
    "Worden, OR":           ("UP Cascade Sub", "UP"),
}

HAZARD_CLASS_COLORS = {
    "2 — Gases": "#e05d00",
    "3 — Flammable Liquids": "#dc2626",
    "4 — Flammable Solids": "#f59e0b",
    "5 — Oxidizers": "#8b5cf6",
    "6 — Toxic": "#0284c7",
    "8 — Corrosives": "#7c3aed",
    "9 — Miscellaneous": "#737d8e",
}


def get_hazard_class(placard):
    """Get the hazard class string for a UN placard."""
    if placard in UN_LOOKUP:
        return UN_LOOKUP[placard][1]
    return "Unknown"


def get_class_group(hazard_class):
    """Map a specific hazard class (e.g. '2.3 — Toxic Gas') to its group."""
    class_num = hazard_class.split(" ")[0] if hazard_class else ""
    for group, prefixes in HAZARD_CLASS_GROUPS.items():
        if class_num in prefixes:
            return group
    return "Other"


def moving_avg(values, window):
    """Compute moving average, returning None for insufficient data."""
    result = []
    for i in range(len(values)):
        if i < window - 1:
            result.append(None)
        else:
            avg = sum(values[i - window + 1:i + 1]) / window
            result.append(round(avg, 1))
    return result


def main():
    print("Oregon Hazmat Flow Analysis")
    print("=" * 50)

    # ── Load sensor metadata from pull log ──
    sensor_meta = {}
    if LOG_PATH.exists():
        with open(LOG_PATH) as f:
            log = json.load(f)
        for s in log.get("sensor_summaries", []):
            sensor_meta[s["name"]] = {
                "lat": s["lat"],
                "lng": s["lng"],
                "railways": s["railways"],
            }

    # ── Read all hazmat car records ──
    print("Reading car-level data...")
    placards = Counter()
    by_sensor = Counter()
    by_sensor_direction = defaultdict(lambda: Counter())
    by_direction = Counter()
    by_operator = Counter()
    by_month = Counter()
    by_date = Counter()
    by_placard_month = defaultdict(lambda: Counter())
    by_placard_sensor = defaultdict(lambda: Counter())
    by_class_group_month = defaultdict(lambda: Counter())
    by_sensor_class_group = defaultdict(lambda: Counter())
    by_sensor_month = defaultdict(lambda: Counter())
    by_placard_direction = defaultdict(lambda: Counter())

    total_hazmat = 0
    with open(CARS_CSV) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["is_hazmat"] != "1":
                continue
            total_hazmat += 1

            p = row["hazmat_placard"]
            s = row["sensor_name"]
            d = row["direction"]
            o = row["train_operator"]
            date = row["date"]
            month = date[:7]

            hc = get_hazard_class(p)
            cg = get_class_group(hc)

            placards[p] += 1
            by_sensor[s] += 1
            by_sensor_direction[s][d] += 1
            by_direction[d] += 1
            by_operator[o] += 1
            by_month[month] += 1
            by_date[date] += 1
            by_placard_month[p][month] += 1
            by_placard_sensor[p][s] += 1
            by_class_group_month[cg][month] += 1
            by_sensor_class_group[s][cg] += 1
            by_sensor_month[s][month] += 1
            by_placard_direction[p][d] += 1

    print(f"  Total hazmat car records: {total_hazmat:,}")
    print(f"  Unique UN placards: {len(placards)}")

    # ── Also read train-level for hazmat train counts ──
    print("Reading train-level data...")
    trains_with_hazmat = 0
    trains_total = 0
    with open(TRAINS_CSV) as f:
        reader = csv.DictReader(f)
        for row in reader:
            trains_total += 1
            if int(row.get("hazmat_car_count", 0)) > 0:
                trains_with_hazmat += 1
    print(f"  Total trains: {trains_total:,}")
    print(f"  Trains carrying hazmat: {trains_with_hazmat:,} ({100*trains_with_hazmat/trains_total:.1f}%)")

    # ══════════════════════════════════════════════════
    # BUILD OUTPUT JSON
    # ══════════════════════════════════════════════════
    months_sorted = sorted(by_month.keys())
    month_labels = []
    for m in months_sorted:
        dt = datetime.strptime(m, "%Y-%m")
        month_labels.append(dt.strftime("%b '%y"))

    # 1. Summary stats
    summary = {
        "total_hazmat_cars": total_hazmat,
        "total_trains": trains_total,
        "trains_with_hazmat": trains_with_hazmat,
        "pct_trains_with_hazmat": round(100 * trains_with_hazmat / trains_total, 1),
        "unique_un_placards": len(placards),
        "sensors_reporting": len(by_sensor),
        "date_range": {"start": "2025-01-01", "end": "2025-12-31"},
        "avg_hazmat_cars_per_day": round(total_hazmat / 365, 0),
    }

    # 2. Monthly volume by hazard class group (stacked bar)
    class_groups_sorted = [
        "3 — Flammable Liquids",
        "2 — Gases",
        "8 — Corrosives",
        "9 — Miscellaneous",
        "5 — Oxidizers",
        "6 — Toxic",
        "4 — Flammable Solids",
    ]
    monthly_by_class = {
        "labels": month_labels,
        "months": months_sorted,
        "datasets": [],
    }
    for cg in class_groups_sorted:
        vals = [by_class_group_month[cg].get(m, 0) for m in months_sorted]
        if sum(vals) > 0:
            monthly_by_class["datasets"].append({
                "label": cg,
                "data": vals,
                "color": HAZARD_CLASS_COLORS.get(cg, "#999"),
                "total": sum(vals),
            })

    # 3. Top UN placards table
    top_placards = []
    for rank, (p, count) in enumerate(placards.most_common(30), 1):
        commodity, hazard_class = UN_LOOKUP.get(p, ("Unknown", "Unknown"))
        # Find primary corridor (subdivision with most of this placard)
        top_sensor = by_placard_sensor[p].most_common(1)
        top_sensor_name = top_sensor[0][0] if top_sensor else "—"
        primary_corridor = SENSOR_SUBDIVISION.get(top_sensor_name, ("Unknown",))[0]
        # Direction breakdown
        dir_counts = dict(by_placard_direction[p])
        top_placards.append({
            "rank": rank,
            "un_number": p,
            "commodity": commodity,
            "hazard_class": hazard_class,
            "total_cars": count,
            "pct_of_hazmat": round(100 * count / total_hazmat, 1),
            "primary_corridor": primary_corridor,
            "primary_sensor": top_sensor_name,
            "direction_breakdown": dir_counts,
            "monthly": [by_placard_month[p].get(m, 0) for m in months_sorted],
        })

    # 4. Hazmat volume by sensor (for map)
    sensor_volumes = []
    for s, count in sorted(by_sensor.items(), key=lambda x: -x[1]):
        meta = sensor_meta.get(s, {})
        dir_breakdown = dict(by_sensor_direction[s])
        class_breakdown = dict(by_sensor_class_group[s])
        monthly = [by_sensor_month[s].get(m, 0) for m in months_sorted]
        sub_info = SENSOR_SUBDIVISION.get(s, ("Unknown", "Unknown"))
        sensor_volumes.append({
            "name": s,
            "lat": meta.get("lat", 0),
            "lng": meta.get("lng", 0),
            "railways": meta.get("railways", []),
            "subdivision": sub_info[0],
            "primary_railroad": sub_info[1],
            "total_hazmat_cars": count,
            "direction_breakdown": dir_breakdown,
            "class_breakdown": class_breakdown,
            "monthly": monthly,
        })

    # 4b. Corridor-level aggregation (by subdivision)
    corridor_totals = defaultdict(lambda: {
        "total": 0, "sensors": [], "class_breakdown": defaultdict(int),
        "direction_breakdown": defaultdict(int), "monthly": [0]*len(months_sorted)
    })
    for sv in sensor_volumes:
        sub = sv["subdivision"]
        corridor_totals[sub]["total"] += sv["total_hazmat_cars"]
        corridor_totals[sub]["sensors"].append(sv["name"])
        for cg, val in sv["class_breakdown"].items():
            corridor_totals[sub]["class_breakdown"][cg] += val
        for d, val in sv["direction_breakdown"].items():
            corridor_totals[sub]["direction_breakdown"][d] += val
        for i, val in enumerate(sv["monthly"]):
            corridor_totals[sub]["monthly"][i] += val

    corridors = []
    for sub, data in sorted(corridor_totals.items(), key=lambda x: -x[1]["total"]):
        corridors.append({
            "subdivision": sub,
            "total_hazmat_cars": data["total"],
            "sensors": data["sensors"],
            "sensor_count": len(data["sensors"]),
            "class_breakdown": dict(data["class_breakdown"]),
            "direction_breakdown": dict(data["direction_breakdown"]),
            "monthly": data["monthly"],
        })

    # 5. Daily hazmat trend
    dates_sorted = sorted(by_date.keys())
    # Fill gaps (days with 0)
    if dates_sorted:
        start_dt = datetime.strptime(dates_sorted[0], "%Y-%m-%d")
        end_dt = datetime.strptime(dates_sorted[-1], "%Y-%m-%d")
        all_dates = []
        d = start_dt
        while d <= end_dt:
            all_dates.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)
    else:
        all_dates = []

    daily_values = [by_date.get(d, 0) for d in all_dates]
    daily_labels = []
    for d in all_dates:
        dt = datetime.strptime(d, "%Y-%m-%d")
        daily_labels.append(dt.strftime("%b %d"))

    daily_trend = {
        "dates": all_dates,
        "labels": daily_labels,
        "values": daily_values,
        "ma7": moving_avg(daily_values, 7),
        "ma30": moving_avg(daily_values, 30),
        "total_days": len(all_dates),
        "avg_per_day": round(sum(daily_values) / max(len(daily_values), 1), 1),
        "max_day": max(daily_values) if daily_values else 0,
        "min_day": min(daily_values) if daily_values else 0,
    }

    # 6. Hazmat by direction
    direction_analysis = {
        "totals": dict(by_direction),
        "by_sensor": {},
    }
    for s in by_sensor:
        direction_analysis["by_sensor"][s] = dict(by_sensor_direction[s])

    # 7. Hazmat by railroad
    railroad_analysis = {
        "totals": dict(by_operator),
        "pct": {o: round(100 * c / total_hazmat, 1) for o, c in by_operator.items()},
    }

    # 8. Sensor x hazard class heatmap
    heatmap = {
        "sensors": [s["name"] for s in sensor_volumes],
        "classes": [cg for cg in class_groups_sorted if any(
            by_sensor_class_group[s].get(cg, 0) > 0 for s in by_sensor
        )],
        "values": [],
    }
    for s_data in sensor_volumes:
        row = [by_sensor_class_group[s_data["name"]].get(cg, 0) for cg in heatmap["classes"]]
        heatmap["values"].append(row)

    # ── Assemble final output ──
    output = {
        "generated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "summary": summary,
        "monthly_by_class": monthly_by_class,
        "top_placards": top_placards,
        "sensor_volumes": sensor_volumes,
        "corridors": corridors,
        "daily_trend": daily_trend,
        "direction_analysis": direction_analysis,
        "railroad_analysis": railroad_analysis,
        "heatmap": heatmap,
        "month_labels": month_labels,
        "months": months_sorted,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nSaved analysis to {OUTPUT_PATH.name}")
    print(f"  File size: {OUTPUT_PATH.stat().st_size / 1024:.0f} KB")

    # ── Print summary ──
    print(f"\n{'='*50}")
    print("HAZMAT ANALYSIS SUMMARY")
    print(f"{'='*50}")
    print(f"  Total hazmat cars:      {total_hazmat:>10,}")
    print(f"  Trains with hazmat:     {trains_with_hazmat:>10,} / {trains_total:,} ({summary['pct_trains_with_hazmat']}%)")
    print(f"  Unique UN placards:     {len(placards):>10,}")
    print(f"  Avg hazmat cars/day:    {summary['avg_hazmat_cars_per_day']:>10,.0f}")
    print(f"\n  Top 10 hazardous materials:")
    for item in top_placards[:10]:
        print(f"    {item['un_number']:8s}  {item['total_cars']:>8,} cars  ({item['pct_of_hazmat']:>4.1f}%)  {item['commodity']}")
    print(f"\n  Hazmat by hazard class group:")
    for ds in monthly_by_class["datasets"]:
        print(f"    {ds['label']:30s}  {ds['total']:>8,} cars")


if __name__ == "__main__":
    main()
