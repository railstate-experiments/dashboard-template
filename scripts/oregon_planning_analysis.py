#!/usr/bin/env python3
"""
Oregon State Rail Planning Analysis
=====================================
Reads the raw Oregon train-level and car-level CSVs and produces analysis
JSON for the CRISI dashboard State Rail Planning tab.

Sections:
  1. Summary stats
  2. Train volume by corridor & train type
  3. Car type mix by corridor
  4. Travel times between sensor pairs (from locomotive matching)
  5. Train spacing / headway analysis
  6. Train length (car count) by type and corridor, trends over time
  7. Time-of-day distribution (for grade crossing planning)
  8. Weekday vs weekend patterns
  9. Directional balance by corridor
 10. Monthly traffic trends

Outputs:
  ../data/oregon_planning_analysis.json
"""

import csv
import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
TRAINS_CSV = DATA_DIR / "oregon_trains_raw.csv"
CARS_CSV = DATA_DIR / "oregon_cars_raw.csv"
OUTPUT_PATH = DATA_DIR / "oregon_planning_analysis.json"

# ── Subdivision mapping (same as hazmat analysis) ──
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

# Logical sensor ordering along corridors for travel time pairs
CORRIDOR_SENSORS = {
    "UP Brooklyn Sub": [
        "Haig, OR", "Salem, OR", "Jefferson, OR", "Irving, OR",
        "Eugene, OR", "Springfield Jct, OR"
    ],
    "UP Cascade Sub": ["Springfield Jct, OR", "Modoc Point, OR", "Worden, OR"],
    "UP La Grande Sub": ["Troutdale, OR", "Echo, OR", "Cold Springs, OR"],
    "UP Nampa Sub": ["Cold Springs, OR", "Ontario, OR"],
    "BNSF Fallbridge Sub": ["N. Portland E, OR", "N. Portland W, OR", "Troutdale, OR"],
}


def parse_dt(s):
    """Parse ISO datetime string (with or without milliseconds)."""
    if not s:
        return None
    try:
        # Strip milliseconds if present: 2025-01-01T07:03:03.535Z → 2025-01-01T07:03:03Z
        clean = s.split(".")[0] + "Z" if "." in s else s
        return datetime.strptime(clean, "%Y-%m-%dT%H:%M:%SZ")
    except:
        return None


def percentiles(vals, pcts=[10, 25, 50, 75, 90]):
    """Compute percentiles for a list of values."""
    if not vals:
        return {}
    sv = sorted(vals)
    n = len(sv)
    result = {}
    for p in pcts:
        k = (n - 1) * p / 100
        f = int(k)
        c = f + 1 if f + 1 < n else f
        result[f"p{p}"] = round(sv[f] + (k - f) * (sv[c] - sv[f]), 1)
    return result


def moving_avg(values, window):
    result = []
    for i in range(len(values)):
        if i < window - 1:
            result.append(None)
        else:
            result.append(round(sum(values[i - window + 1:i + 1]) / window, 1))
    return result


def main():
    print("Oregon State Rail Planning Analysis")
    print("=" * 55)

    # ════════════════════════════════════════
    # PASS 1: Read train-level data
    # ════════════════════════════════════════
    print("Reading train-level data...")

    trains = []
    with open(TRAINS_CSV) as f:
        for row in csv.DictReader(f):
            dt = parse_dt(row["detection_time"])
            if not dt:
                continue
            row["_dt"] = dt
            row["_hour"] = dt.hour
            row["_weekday"] = dt.weekday()  # 0=Mon, 6=Sun
            row["_month"] = row["date"][:7]
            row["_subdivision"] = SENSOR_SUBDIVISION.get(row["sensor_name"], ("Unknown", "Unknown"))[0]
            row["_total_cars"] = int(row["total_cars"])
            trains.append(row)

    print(f"  {len(trains):,} train records loaded")

    # ════════════════════════════════════════
    # ANALYSIS 1: Summary stats
    # ════════════════════════════════════════
    total_trains = len(trains)
    total_cars_sum = sum(t["_total_cars"] for t in trains)
    train_type_counts = Counter(t["train_type"] for t in trains)
    operator_counts = Counter(t["train_operator"] for t in trains)
    months_sorted = sorted(set(t["_month"] for t in trains))
    month_labels = []
    for m in months_sorted:
        dt = datetime.strptime(m, "%Y-%m")
        month_labels.append(dt.strftime("%b '%y"))

    summary = {
        "total_trains": total_trains,
        "total_car_sightings": total_cars_sum,
        "sensors": len(set(t["sensor_name"] for t in trains)),
        "train_types": len(train_type_counts),
        "operators": dict(operator_counts),
        "date_range": {"start": "2025-01-01", "end": "2025-12-31"},
        "avg_trains_per_day": round(total_trains / 365),
        "avg_cars_per_train": round(total_cars_sum / total_trains, 1),
    }

    # ════════════════════════════════════════
    # ANALYSIS 2: Train volume by corridor & type
    # ════════════════════════════════════════
    print("Computing corridor volumes...")
    corridor_type = defaultdict(lambda: defaultdict(int))
    corridor_monthly = defaultdict(lambda: defaultdict(int))
    sensor_type = defaultdict(lambda: defaultdict(int))
    sensor_monthly = defaultdict(lambda: defaultdict(int))
    sensor_monthly_type = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    for t in trains:
        corridor_type[t["_subdivision"]][t["train_type"]] += 1
        corridor_monthly[t["_subdivision"]][t["_month"]] += 1
        sensor_type[t["sensor_name"]][t["train_type"]] += 1
        sensor_monthly[t["sensor_name"]][t["_month"]] += 1
        sensor_monthly_type[t["sensor_name"]][t["_month"]][t["train_type"]] += 1

    corridors_volume = []
    for sub in sorted(corridor_type, key=lambda s: -sum(corridor_type[s].values())):
        total = sum(corridor_type[sub].values())
        corridors_volume.append({
            "subdivision": sub,
            "total_trains": total,
            "by_type": dict(corridor_type[sub]),
            "monthly": [corridor_monthly[sub].get(m, 0) for m in months_sorted],
        })

    sensor_volumes = []
    for sn in sorted(sensor_type, key=lambda s: -sum(sensor_type[s].values())):
        total = sum(sensor_type[sn].values())
        sub = SENSOR_SUBDIVISION.get(sn, ("Unknown",))[0]
        monthly = [sensor_monthly[sn].get(m, 0) for m in months_sorted]
        monthly_by_type = {}
        for m in months_sorted:
            monthly_by_type[m] = dict(sensor_monthly_type[sn][m])
        sensor_volumes.append({
            "name": sn,
            "subdivision": sub,
            "total_trains": total,
            "by_type": dict(sensor_type[sn]),
            "monthly": monthly,
            "monthly_by_type": monthly_by_type,
        })

    # ════════════════════════════════════════
    # ANALYSIS 3: Car type mix by corridor
    # ════════════════════════════════════════
    print("Computing car type mix...")
    corridor_cars = defaultdict(lambda: defaultdict(int))
    for t in trains:
        sub = t["_subdivision"]
        corridor_cars[sub]["Tank Car"] += int(t["tank_car_count"])
        corridor_cars[sub]["Covered Hopper"] += int(t["hopper_count"])
        corridor_cars[sub]["Gondola"] += int(t["gondola_count"])
        corridor_cars[sub]["Box Car"] += int(t["boxcar_count"])
        corridor_cars[sub]["Flat/Well Car"] += int(t["flatcar_count"])
        corridor_cars[sub]["Intermodal"] += int(t["container_car_count"])
        corridor_cars[sub]["Locomotive"] += int(t["locomotive_count"])
        corridor_cars[sub]["Other"] += int(t["other_car_count"])

    car_mix = []
    for sub in sorted(corridor_cars, key=lambda s: -sum(corridor_cars[s].values())):
        car_mix.append({
            "subdivision": sub,
            "total_cars": sum(corridor_cars[sub].values()),
            "by_type": dict(corridor_cars[sub]),
        })

    # ════════════════════════════════════════
    # ANALYSIS 4: Travel times (locomotive matching)
    # ════════════════════════════════════════
    print("Reading locomotive sightings for travel time analysis...")
    loco_sightings = defaultdict(list)  # loco_id → [(sensor, datetime), ...]

    with open(CARS_CSV) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if "Locomotive" not in row["car_type"]:
                continue
            car_id = row["car_id"]
            if not car_id:
                continue
            dt = parse_dt(row["detection_time"])
            if dt:
                loco_sightings[car_id].append((row["sensor_name"], dt))

    print(f"  {len(loco_sightings):,} unique locomotives tracked")

    # Sort each locomotive's sightings by time
    for lid in loco_sightings:
        loco_sightings[lid].sort(key=lambda x: x[1])

    # Compute travel times between sensor pairs
    pair_times = defaultdict(list)  # (sensorA, sensorB) → [minutes, ...]
    pair_times_monthly = defaultdict(lambda: defaultdict(list))

    for lid, sightings in loco_sightings.items():
        for i in range(len(sightings) - 1):
            s1, t1 = sightings[i]
            s2, t2 = sightings[i + 1]
            if s1 == s2:
                continue
            delta_min = (t2 - t1).total_seconds() / 60
            # Only keep reasonable travel times (5 min to 24 hours)
            if 5 <= delta_min <= 1440:
                pair_key = (s1, s2)
                pair_times[pair_key].append(delta_min)
                month = t1.strftime("%Y-%m")
                pair_times_monthly[pair_key][month].append(delta_min)

    print(f"  {len(pair_times):,} sensor pairs with travel time data")

    # Build travel time output — top pairs by sample count
    travel_times = []
    for (s1, s2), times in sorted(pair_times.items(), key=lambda x: -len(x[1])):
        if len(times) < 20:
            continue
        sub1 = SENSOR_SUBDIVISION.get(s1, ("Unknown",))[0]
        sub2 = SENSOR_SUBDIVISION.get(s2, ("Unknown",))[0]
        pcts = percentiles(times)
        # Monthly median trend
        monthly_medians = []
        for m in months_sorted:
            mt = pair_times_monthly[(s1, s2)].get(m, [])
            monthly_medians.append(round(statistics.median(mt), 1) if len(mt) >= 3 else None)

        travel_times.append({
            "origin": s1,
            "destination": s2,
            "origin_sub": sub1,
            "dest_sub": sub2,
            "sample_count": len(times),
            "median_minutes": round(statistics.median(times), 1),
            "mean_minutes": round(statistics.mean(times), 1),
            "std_minutes": round(statistics.stdev(times), 1) if len(times) > 1 else 0,
            "min_minutes": round(min(times), 1),
            "max_minutes": round(max(times), 1),
            "percentiles": pcts,
            "monthly_median": monthly_medians,
        })

    travel_times.sort(key=lambda x: -x["sample_count"])
    print(f"  {len(travel_times)} pairs with 20+ observations")

    # ════════════════════════════════════════
    # ANALYSIS 5: Train spacing / headway
    # ════════════════════════════════════════
    print("Computing train spacing (headway)...")
    sensor_trains_sorted = defaultdict(list)
    for t in trains:
        sensor_trains_sorted[t["sensor_name"]].append(t["_dt"])

    headway_by_sensor = {}
    for sensor, times in sensor_trains_sorted.items():
        times.sort()
        gaps = []
        for i in range(1, len(times)):
            gap_min = (times[i] - times[i - 1]).total_seconds() / 60
            if 1 <= gap_min <= 1440:  # 1 min to 24 hours
                gaps.append(gap_min)
        if gaps:
            sub = SENSOR_SUBDIVISION.get(sensor, ("Unknown",))[0]
            headway_by_sensor[sensor] = {
                "sensor": sensor,
                "subdivision": sub,
                "sample_count": len(gaps),
                "median_minutes": round(statistics.median(gaps), 1),
                "mean_minutes": round(statistics.mean(gaps), 1),
                "percentiles": percentiles(gaps),
                "pct_under_30min": round(100 * sum(1 for g in gaps if g < 30) / len(gaps), 1),
                "pct_under_60min": round(100 * sum(1 for g in gaps if g < 60) / len(gaps), 1),
            }

    # Also compute headway by hour of day for peak analysis
    headway_by_hour = defaultdict(list)
    for sensor, times in sensor_trains_sorted.items():
        times.sort()
        for i in range(1, len(times)):
            gap_min = (times[i] - times[i - 1]).total_seconds() / 60
            if 1 <= gap_min <= 1440:
                headway_by_hour[times[i].hour].append(gap_min)

    hourly_headway = {}
    for h in range(24):
        gaps = headway_by_hour.get(h, [])
        if gaps:
            hourly_headway[h] = {
                "median": round(statistics.median(gaps), 1),
                "mean": round(statistics.mean(gaps), 1),
                "count": len(gaps),
            }

    # ════════════════════════════════════════
    # ANALYSIS 6: Train length by type & corridor
    # ════════════════════════════════════════
    print("Computing train length distributions...")

    # Load estimated length in feet from supplemental pull
    LENGTHS_JSON = DATA_DIR / "oregon_train_lengths.json"
    feet_by_sighting = {}
    if LENGTHS_JSON.exists():
        print("  Loading train length (feet) data...")
        with open(LENGTHS_JSON) as f:
            len_data = json.load(f)
        for rec in len_data.get("records", []):
            if rec.get("length_feet") is not None:
                feet_by_sighting[rec["sighting_id"]] = rec["length_feet"]
        print(f"  {len(feet_by_sighting):,} trains with length_feet")
    else:
        print("  WARNING: oregon_train_lengths.json not found — using car counts only")

    length_by_type_cars = defaultdict(list)
    length_by_type_feet = defaultdict(list)
    length_by_sensor = defaultdict(list)
    length_by_type_monthly_feet = defaultdict(lambda: defaultdict(list))

    for t in trains:
        cars = t["_total_cars"]
        tt = t["train_type"]
        sid = int(t.get("sighting_id", 0)) if t.get("sighting_id", "").isdigit() else t.get("sighting_id")
        feet = feet_by_sighting.get(sid)

        if cars > 0:
            length_by_type_cars[tt].append(cars)
            length_by_sensor[t["sensor_name"]].append(cars)

        if feet and feet > 0:
            length_by_type_feet[tt].append(feet)
            length_by_type_monthly_feet[tt][t["_month"]].append(feet)

    has_feet = len(feet_by_sighting) > 0
    print(f"  Using {'estimated feet' if has_feet else 'car counts'} for distributions")

    # Histogram bins for distribution curves (feet)
    BIN_WIDTH_FT = 500
    BIN_MAX_FT = 15000
    bin_edges_ft = list(range(0, BIN_MAX_FT + BIN_WIDTH_FT, BIN_WIDTH_FT))
    bin_labels_ft = [f"{b:,}-{b+BIN_WIDTH_FT-1:,}" for b in bin_edges_ft[:-1]]

    # Minimum length to include (filters partial sightings / light engines)
    MIN_FEET_FOR_DIST = 1000

    def make_histogram_ft(vals, min_val=MIN_FEET_FOR_DIST):
        filtered = [v for v in vals if v >= min_val]
        counts = [0] * (len(bin_edges_ft) - 1)
        for v in filtered:
            idx = min(int(v // BIN_WIDTH_FT), len(counts) - 1)
            if idx >= 0:
                counts[idx] += 1
        total = len(filtered)
        pcts = [round(100 * c / total, 2) if total > 0 else 0 for c in counts]
        return {"counts": counts, "pcts": pcts, "filtered_count": total,
                "excluded": len(vals) - total}

    train_lengths = []
    for tt in sorted(length_by_type_cars, key=lambda x: -len(length_by_type_cars[x])):
        car_vals = length_by_type_cars[tt]
        feet_vals = length_by_type_feet.get(tt, [])

        # Monthly median trend (feet)
        monthly_median_ft = []
        for m in months_sorted:
            mv = length_by_type_monthly_feet[tt].get(m, [])
            monthly_median_ft.append(round(statistics.median(mv), 0) if mv else None)

        hist_ft = make_histogram_ft(feet_vals) if feet_vals else None

        entry = {
            "train_type": tt,
            "sample_count": len(car_vals),
            "median_cars": round(statistics.median(car_vals), 1),
            "mean_cars": round(statistics.mean(car_vals), 1),
            "min_cars": min(car_vals),
            "max_cars": max(car_vals),
            "percentiles_cars": percentiles(car_vals),
        }
        if feet_vals:
            entry.update({
                "feet_count": len(feet_vals),
                "median_feet": round(statistics.median(feet_vals), 0),
                "mean_feet": round(statistics.mean(feet_vals), 0),
                "min_feet": round(min(feet_vals), 0),
                "max_feet": round(max(feet_vals), 0),
                "percentiles_feet": percentiles(feet_vals),
                "monthly_median_feet": monthly_median_ft,
                "histogram_feet": hist_ft,
            })

        train_lengths.append(entry)

    sensor_lengths = []
    for sn in sorted(length_by_sensor, key=lambda s: -len(length_by_sensor[s])):
        vals = length_by_sensor[sn]
        sub = SENSOR_SUBDIVISION.get(sn, ("Unknown",))[0]
        sensor_lengths.append({
            "sensor": sn,
            "subdivision": sub,
            "sample_count": len(vals),
            "median_cars": round(statistics.median(vals), 1),
            "mean_cars": round(statistics.mean(vals), 1),
            "percentiles": percentiles(vals),
        })

    # ════════════════════════════════════════
    # ANALYSIS 7: Time-of-day distribution
    # ════════════════════════════════════════
    print("Computing time-of-day distribution...")
    hourly_total = Counter()
    hourly_by_sensor = defaultdict(lambda: Counter())
    for t in trains:
        hourly_total[t["_hour"]] += 1
        hourly_by_sensor[t["sensor_name"]][t["_hour"]] += 1

    time_of_day = {
        "total": {str(h): hourly_total.get(h, 0) for h in range(24)},
        "by_sensor": {},
    }
    for sn in hourly_by_sensor:
        time_of_day["by_sensor"][sn] = {str(h): hourly_by_sensor[sn].get(h, 0) for h in range(24)}

    # Peak hour analysis
    peak_hours = sorted(range(24), key=lambda h: -hourly_total.get(h, 0))[:6]
    off_peak = sorted(range(24), key=lambda h: hourly_total.get(h, 0))[:6]

    # ════════════════════════════════════════
    # ANALYSIS 8: Weekday vs weekend
    # ════════════════════════════════════════
    print("Computing weekday vs weekend patterns...")
    weekday_trains = sum(1 for t in trains if t["_weekday"] < 5)
    weekend_trains = sum(1 for t in trains if t["_weekday"] >= 5)
    weekday_days = sum(1 for d in range(365) if (datetime(2025,1,1)+timedelta(days=d)).weekday() < 5)
    weekend_days = 365 - weekday_days

    day_of_week = Counter()
    for t in trains:
        day_of_week[t["_weekday"]] += 1

    weekday_weekend = {
        "weekday_total": weekday_trains,
        "weekend_total": weekend_trains,
        "weekday_avg_per_day": round(weekday_trains / weekday_days, 1),
        "weekend_avg_per_day": round(weekend_trains / weekend_days, 1),
        "by_day": {["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][d]: day_of_week.get(d,0) for d in range(7)},
    }

    # ════════════════════════════════════════
    # ANALYSIS 9: Directional balance
    # ════════════════════════════════════════
    print("Computing directional balance...")
    dir_by_sensor = defaultdict(lambda: Counter())
    dir_by_corridor = defaultdict(lambda: Counter())
    for t in trains:
        dir_by_sensor[t["sensor_name"]][t["direction"]] += 1
        dir_by_corridor[t["_subdivision"]][t["direction"]] += 1

    directional = {
        "by_corridor": {sub: dict(counts) for sub, counts in dir_by_corridor.items()},
        "by_sensor": {sn: dict(counts) for sn, counts in dir_by_sensor.items()},
    }

    # ════════════════════════════════════════
    # ANALYSIS 10: Monthly traffic trends
    # ════════════════════════════════════════
    print("Computing monthly trends...")
    monthly_trains = Counter()
    monthly_by_type = defaultdict(lambda: Counter())
    for t in trains:
        monthly_trains[t["_month"]] += 1
        monthly_by_type[t["train_type"]][t["_month"]] += 1

    monthly_trends = {
        "labels": month_labels,
        "months": months_sorted,
        "total": [monthly_trains.get(m, 0) for m in months_sorted],
    }
    # Top train types as monthly series
    top_types = [tt for tt, _ in train_type_counts.most_common(6)]
    monthly_trends["by_type"] = {}
    for tt in top_types:
        monthly_trends["by_type"][tt] = [monthly_by_type[tt].get(m, 0) for m in months_sorted]

    # ════════════════════════════════════════
    # ASSEMBLE OUTPUT
    # ════════════════════════════════════════
    output = {
        "generated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "month_labels": month_labels,
        "months": months_sorted,
        "summary": summary,
        "train_type_counts": dict(train_type_counts),
        "corridors_volume": corridors_volume,
        "sensor_volumes": sensor_volumes,
        "car_mix": car_mix,
        "travel_times": travel_times[:60],
        "headway": {
            "by_sensor": headway_by_sensor,
            "by_hour": hourly_headway,
        },
        "train_lengths": train_lengths,
        "length_bin_labels_ft": bin_labels_ft,
        "length_bin_width_ft": BIN_WIDTH_FT,
        "sensor_lengths": sensor_lengths,
        "time_of_day": time_of_day,
        "peak_hours": peak_hours,
        "weekday_weekend": weekday_weekend,
        "directional": directional,
        "monthly_trends": monthly_trends,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nSaved to {OUTPUT_PATH.name}")
    print(f"  File size: {OUTPUT_PATH.stat().st_size / 1024:.0f} KB")

    # ── Summary ──
    print(f"\n{'='*55}")
    print("PLANNING ANALYSIS SUMMARY")
    print(f"{'='*55}")
    print(f"  Total trains:           {total_trains:>10,}")
    print(f"  Total car sightings:    {total_cars_sum:>10,}")
    print(f"  Avg trains/day:         {summary['avg_trains_per_day']:>10,}")
    print(f"  Avg cars/train:         {summary['avg_cars_per_train']:>10}")
    print(f"\n  Train types:")
    for tt, c in train_type_counts.most_common():
        print(f"    {tt:25s} {c:>7,}  ({100*c/total_trains:4.1f}%)")
    print(f"\n  Travel time pairs:      {len(travel_times)}")
    print(f"  Top 5 travel time pairs:")
    for tp in travel_times[:5]:
        print(f"    {tp['origin']:25s} → {tp['destination']:25s}  median={tp['median_minutes']:.0f}min  n={tp['sample_count']}")
    print(f"\n  Train lengths by type:")
    for tl in train_lengths[:6]:
        print(f"    {tl['train_type']:25s}  median={tl['median_cars']:.0f} cars  range={tl['min_cars']}-{tl['max_cars']}")
    print(f"\n  Weekday avg: {weekday_weekend['weekday_avg_per_day']:.0f} trains/day")
    print(f"  Weekend avg: {weekday_weekend['weekend_avg_per_day']:.0f} trains/day")


if __name__ == "__main__":
    main()
