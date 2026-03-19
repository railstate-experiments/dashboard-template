#!/usr/bin/env python3
"""
Western Canada Ports - Weekly Container & Export Volumes
========================================================
Tracks container imports (intermodal) and bulk export commodities through
Western Canadian ports: Vancouver (CN & CPKC) and Prince Rupert (CN).

Supports incremental daily updates:
  - First run: fetches Jan 1 2024 to most recent Sunday
  - Subsequent runs: fetches from (last_date - 2 days), deduplicates
  - --rebuild: full re-fetch from Jan 1 2024
  - --days N: fetch last N days only

Environment:
  RAILSTATE_API_KEY - API key (optional, falls back to hardcoded key)

Outputs:
  ../data/wcan_raw_sightings.csv  - raw sighting-level data for incremental updates
  ../data/wcan_ports.json         - aggregated JSON for dashboards
"""

import argparse
import csv
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests

# ============================================================================
# CONFIGURATION
# ============================================================================

API_BASE_URL = "https://api.railstate.com"

HARDCODED_API_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJhdWQiOiJhcGkucmFpbHN0YXRlLmNvbSIsImlzcyI6InJhaWxzdGF0ZS5jb20iLCJ1aWQiOiJ5"
    "RWE5aTFSbjljTmhtbEFEQkNYd3BFR3h1eEczIiwidGlkIjoiRGFuIERldm9lIFRlc3RpbmcifQ."
    "8YQJPJE3X2xomrgUCXo41tx0Vh0gCRWRZ2JbQKYpAXE"
)

DEFAULT_START_DATE = datetime(2024, 1, 1)

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
RAW_CSV_PATH = DATA_DIR / "wcan_raw_sightings.csv"
OUTPUT_JSON_PATH = DATA_DIR / "wcan_ports.json"

RAW_CSV_COLUMNS = [
    "date", "sensor_key", "sighting_id", "train_type",
    "commodity", "car_count", "teu",
]

# Train types we exclude from export counts (but intermodal counted for containers)
EXCLUDED_TRAIN_TYPES = {"intermodal", "passenger", "mow", "light_locomotive", "automotive"}

# ============================================================================
# SENSOR CONFIGURATION
# ============================================================================

# Container imports (intermodal trains)
CONTAINER_SENSORS = {
    "vancouver_cn": {"name": "Heffley W, BC", "direction": "northbound"},
    "vancouver_cpkc": {"name": "Chase, BC", "direction": "eastbound"},
    "prince_rupert_cn": {
        "primary": {"name": "Phelan, BC", "direction": "eastbound"},
        "fallback": {"name": "Terrace, BC", "direction": "eastbound"},
    },
}

# Export bulk (non-intermodal trains)
EXPORT_SENSORS = {
    "vancouver_cn": {"name": "Heffley W, BC", "direction": "southbound"},
    "vancouver_cpkc": {"name": "Chase, BC", "direction": "westbound"},
    "prince_rupert_cn": {
        "primary": {"name": "Phelan, BC", "direction": "westbound"},
        "fallback": {"name": "Terrace, BC", "direction": "westbound"},
    },
    "blaine_bnsf": {"name": "Blaine, WA", "direction": "northbound"},
}

# ============================================================================
# API CLIENT (same pattern as ethanol scripts)
# ============================================================================

class RailStateFetcher:
    def __init__(self, api_key: str, base_url: str = API_BASE_URL):
        self.api_key = api_key
        self.base_url = base_url
        self._sensor_cache = {}

    def _request(self, url: str, params: dict = None) -> Tuple[dict, Optional[str]]:
        try:
            headers = {"Authorization": f"Bearer {self.api_key}", "Accept": "application/json"}
            response = requests.get(url, params=params, headers=headers, timeout=120)
            if response.status_code != 200:
                return {}, f"HTTP {response.status_code}"
            return response.json(), None
        except requests.exceptions.Timeout:
            return {}, "Timeout"
        except Exception as e:
            return {}, str(e)

    def load_sensors(self) -> Dict[str, int]:
        if self._sensor_cache:
            return self._sensor_cache
        url = urljoin(self.base_url, "/api/v3/sensors/overview")
        data, error = self._request(url)
        if error:
            print(f"  Warning: Could not load sensors: {error}", flush=True)
            return {}
        for sensor in data.get("sensors", []):
            name, sid = sensor.get("name"), sensor.get("sensorId")
            if name and sid:
                self._sensor_cache[name] = sid
        return self._sensor_cache

    def get_sensor_id(self, name: str) -> Optional[int]:
        if not self._sensor_cache:
            self.load_sensors()
        if name in self._sensor_cache:
            return self._sensor_cache[name]
        for n, sid in self._sensor_cache.items():
            if n.lower() == name.lower():
                return sid
        return None

    def fetch_sightings(self, sensor_id: int, start: datetime, end: datetime,
                        direction: str = None) -> List[Dict]:
        url = urljoin(self.base_url, "/api/v3/trains/full_sightings")
        all_sightings = []
        params = {
            "sensors": str(sensor_id),
            "detection_time_from": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "detection_time_to": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "response_size": 500,
        }

        while True:
            data, error = self._request(url, params)
            if error:
                print(f"    API error: {error}", flush=True)
                break
            sightings = data.get("sightings", [])
            if not sightings:
                break
            if direction:
                sightings = [s for s in sightings if s.get("direction", "").lower() == direction.lower()]
            all_sightings.extend(sightings)
            next_link = data.get("nextRequestLink")
            if not next_link:
                break
            url, params = next_link, None

        return all_sightings


# ============================================================================
# TRAIN CLASSIFICATION
# ============================================================================

def get_aar_type(car: dict) -> str:
    """Get AAR car type from top-level or nested equipmentParameters."""
    aar = car.get("aarType")
    if aar:
        return aar
    ep = car.get("equipmentParameters") or {}
    tc = ep.get("typeCode") or ep.get("aarCarType") or ""
    return tc


def classify_train(sighting: dict) -> str:
    """Classify a train using the API's trainType field, with fallback to car composition."""
    # Use API-provided trainType if available
    api_type = (sighting.get("trainType") or "").strip()
    if api_type:
        t = api_type.lower()
        TYPE_MAP = {
            "intermodal": "intermodal",
            "coal unit": "coal_unit",
            "grain unit": "grain_unit",
            "potash unit": "potash_unit",
            "sulphur unit": "sulphur_unit",
            "manifest": "manifest",
            "automotive": "automotive",
            "passenger": "passenger",
            "m-o-w": "mow",
            "mow": "mow",
        }
        for key, val in TYPE_MAP.items():
            if key in t:
                return val
        return t.replace(" ", "_")

    # Fallback: derive from car composition
    cars = sighting.get("cars", [])
    non_loco = [c for c in cars if c.get("type", "").lower() != "locomotive"]
    if not non_loco:
        return "light_locomotive"

    total = len(non_loco)
    type_counts: Counter = Counter()
    for c in non_loco:
        ct = c.get("type", "").lower()
        type_counts[ct] += 1

    stack_count = sum(v for k, v in type_counts.items() if "stack" in k)
    if stack_count / total >= 0.5:
        return "intermodal"

    gondola_count = sum(v for k, v in type_counts.items() if "gondola" in k)
    if gondola_count / total >= 0.70:
        return "coal_unit"

    covered_hopper_count = sum(v for k, v in type_counts.items() if "covered hopper" in k)
    if covered_hopper_count / total >= 0.70:
        return "potash_unit"  # conservative default

    return "manifest"


# ============================================================================
# CONTAINER COUNTING (intermodal trains)
# ============================================================================

def count_teu(cars: List[dict]) -> int:
    """Count TEU from containers nested inside Stack Cars.

    Container types:
      "Container 20 Feet" = 1 TEU
      "Container 40 Feet" = 2 TEU
      Other sizes (e.g. 53 Feet) are excluded.
    """
    teu = 0
    for c in cars:
        t = c.get("type", "").lower()
        if t != "stack car":
            continue
        containers = c.get("containers") or []
        for container in containers:
            ct = (container.get("type") or "").lower()
            if "20" in ct:
                teu += 1
            elif "40" in ct:
                teu += 2
            # Other sizes (53 ft, etc.) excluded
    return teu


# ============================================================================
# EXPORT CAR COUNTING (non-excluded, non-intermodal trains)
# ============================================================================

def count_export_commodities(cars: List[dict], train_type: str, coal_only: bool = False) -> Dict[str, int]:
    """
    Count export commodity cars on a non-excluded train.

    Returns dict with keys: coal, potash, grain, forest_products, lpg
    For blaine_bnsf (coal_only=True), only coal is counted.
    """
    counts = {"coal": 0, "potash": 0, "grain": 0, "forest_products": 0, "lpg": 0}
    non_loco = [c for c in cars if c.get("type", "").lower() != "locomotive"]

    if not non_loco:
        return counts

    # Coal unit trains: ALL non-loco cars are coal. Nothing else counted.
    if train_type == "coal_unit":
        counts["coal"] = len(non_loco)
        return counts

    # If coal_only (Blaine), we only care about coal from coal_unit trains
    if coal_only:
        return counts

    # Potash unit trains: ALL non-loco cars are potash. Nothing else counted.
    if train_type == "potash_unit":
        counts["potash"] = len(non_loco)
        return counts

    # Grain unit trains: count C113/C114 non-loco cars
    if train_type == "grain_unit":
        for c in non_loco:
            if get_aar_type(c) in ("C114", "C113"):
                counts["grain"] += 1
        return counts

    # Manifest and other trains: count specific commodities
    for c in non_loco:
        t = c.get("type", "").lower()

        # Grain: C114/C113 covered hoppers
        if "covered hopper" in t and get_aar_type(c) in ("C114", "C113"):
            counts["grain"] += 1
            continue

        # Forest products: Box Car, Centerbeam
        if t == "box car" or "centerbeam" in t or "center beam" in t:
            counts["forest_products"] += 1
            continue

        # LPG: tank cars with UN1075 hazmat
        if "tank" in t:
            hazmats = c.get("hazmats") or []
            for h in hazmats:
                if h.get("placardType") == "UN1075":
                    counts["lpg"] += 1
                    break

    return counts


# ============================================================================
# SIGHTING PROCESSING
# ============================================================================

def process_sightings(sightings: List[dict], sensor_key: str,
                      mode: str, coal_only: bool = False) -> List[dict]:
    """
    Process sightings into raw record rows.

    mode: 'containers' or 'exports'
    Returns list of dicts matching RAW_CSV_COLUMNS.
    """
    records = []
    for s in sightings:
        sid = s.get("sightingId", "")
        dt_str = s.get("detectionTimeUTC", "")
        if not dt_str:
            continue
        try:
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except ValueError:
            try:
                dt = datetime.strptime(dt_str[:19], "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                continue
        date_str = dt.strftime("%Y-%m-%d")

        cars = s.get("cars", [])
        train_type = classify_train(s)

        if mode == "containers":
            # Only count intermodal trains
            if train_type != "intermodal":
                continue
            teu = count_teu(cars)
            if teu > 0:
                records.append({
                    "date": date_str,
                    "sensor_key": sensor_key,
                    "sighting_id": sid,
                    "train_type": train_type,
                    "commodity": "containers",
                    "car_count": 0,
                    "teu": teu,
                })

        elif mode == "exports":
            # Total car movements: ALL non-loco cars on revenue trains
            if train_type not in {"passenger", "mow", "light_locomotive"}:
                non_loco = [c for c in cars if c.get("type", "").lower() != "locomotive"]
                if non_loco:
                    records.append({
                        "date": date_str,
                        "sensor_key": sensor_key,
                        "sighting_id": sid,
                        "train_type": train_type,
                        "commodity": "total_cars",
                        "car_count": len(non_loco),
                        "teu": 0,
                    })

            # Commodity-specific counts (excludes intermodal/automotive)
            if train_type in EXCLUDED_TRAIN_TYPES:
                continue
            commodities = count_export_commodities(cars, train_type, coal_only=coal_only)
            for commodity, count in commodities.items():
                if count > 0:
                    records.append({
                        "date": date_str,
                        "sensor_key": sensor_key,
                        "sighting_id": sid,
                        "train_type": train_type,
                        "commodity": commodity,
                        "car_count": count,
                        "teu": 0,
                    })

    return records


# ============================================================================
# PHELAN / TERRACE FALLBACK
# ============================================================================

def merge_phelan_terrace(phelan_sightings: List[dict],
                         terrace_sightings: List[dict]) -> Tuple[List[dict], List[str]]:
    """
    Per-day fallback: use Phelan data unless Phelan has zero sightings for
    that day, in which case use Terrace.

    Returns (merged sightings, list of fallback dates).
    """
    def sighting_date(s):
        dt_str = s.get("detectionTimeUTC", "")
        try:
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            return None

    # Group by date
    phelan_by_date: Dict[str, List[dict]] = defaultdict(list)
    for s in phelan_sightings:
        d = sighting_date(s)
        if d:
            phelan_by_date[d].append(s)

    terrace_by_date: Dict[str, List[dict]] = defaultdict(list)
    for s in terrace_sightings:
        d = sighting_date(s)
        if d:
            terrace_by_date[d].append(s)

    all_dates = sorted(set(list(phelan_by_date.keys()) + list(terrace_by_date.keys())))
    merged = []
    fallback_dates = []

    for d in all_dates:
        if phelan_by_date[d]:
            merged.extend(phelan_by_date[d])
        elif terrace_by_date[d]:
            merged.extend(terrace_by_date[d])
            fallback_dates.append(d)

    return merged, fallback_dates


# ============================================================================
# DATA FETCHING
# ============================================================================

def fetch_sensor_sightings(fetcher: RailStateFetcher, sensor_cfg: dict,
                           start: datetime, end: datetime) -> List[dict]:
    """Fetch sightings for a single sensor/direction config."""
    name = sensor_cfg["name"]
    direction = sensor_cfg["direction"]
    sensor_id = fetcher.get_sensor_id(name)
    if not sensor_id:
        print(f"    Warning: Sensor not found: {name}", flush=True)
        return []
    print(f"    Fetching {name} ({direction})...", flush=True)
    sightings = fetcher.fetch_sightings(sensor_id, start, end, direction)
    print(f"      {len(sightings):,} sightings", flush=True)
    return sightings


def fetch_all_data(fetcher: RailStateFetcher, start: datetime, end: datetime) -> List[dict]:
    """
    Fetch container and export sightings for all sensors.
    Returns list of raw record dicts.
    """
    all_records = []

    # --- CONTAINER IMPORTS ---
    print("\n--- CONTAINER IMPORTS (intermodal) ---", flush=True)

    for key, cfg in CONTAINER_SENSORS.items():
        print(f"\n  [{key}]", flush=True)

        if "primary" in cfg:
            # Prince Rupert with Phelan/Terrace fallback
            phelan = fetch_sensor_sightings(fetcher, cfg["primary"], start, end)
            terrace = fetch_sensor_sightings(fetcher, cfg["fallback"], start, end)
            merged, fb_dates = merge_phelan_terrace(phelan, terrace)
            if fb_dates:
                print(f"    Terrace fallback used for {len(fb_dates)} days", flush=True)
            records = process_sightings(merged, key, mode="containers")
        else:
            sightings = fetch_sensor_sightings(fetcher, cfg, start, end)
            records = process_sightings(sightings, key, mode="containers")

        print(f"    -> {len(records)} container records", flush=True)
        all_records.extend(records)

    # --- EXPORT BULK ---
    print("\n--- EXPORT BULK (non-intermodal) ---", flush=True)

    for key, cfg in EXPORT_SENSORS.items():
        print(f"\n  [{key}]", flush=True)
        coal_only = (key == "blaine_bnsf")

        if "primary" in cfg:
            phelan = fetch_sensor_sightings(fetcher, cfg["primary"], start, end)
            terrace = fetch_sensor_sightings(fetcher, cfg["fallback"], start, end)
            merged, fb_dates = merge_phelan_terrace(phelan, terrace)
            if fb_dates:
                print(f"    Terrace fallback used for {len(fb_dates)} days", flush=True)
            records = process_sightings(merged, key, mode="exports", coal_only=coal_only)
        else:
            sightings = fetch_sensor_sightings(fetcher, cfg, start, end)
            records = process_sightings(sightings, key, mode="exports", coal_only=coal_only)

        print(f"    -> {len(records)} export records", flush=True)
        all_records.extend(records)

    return all_records


# ============================================================================
# RAW CSV I/O
# ============================================================================

def load_raw_csv(path: Path) -> List[dict]:
    """Load existing raw CSV, return list of dicts."""
    if not path.exists():
        return []
    try:
        with open(path, "r", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        print(f"Loaded {len(rows):,} existing raw records from {path.name}", flush=True)
        return rows
    except Exception as e:
        print(f"Warning: Could not load {path.name}: {e}", flush=True)
        return []


def save_raw_csv(records: List[dict], path: Path):
    """Save raw records to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RAW_CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(records)
    print(f"Saved {len(records):,} raw records to {path.name}", flush=True)


def merge_raw_records(existing: List[dict], new: List[dict]) -> List[dict]:
    """Merge and deduplicate raw records by (sighting_id, commodity)."""
    seen = set()
    merged = []

    # New records take precedence (add them last, but use set for dedup)
    # Cast sighting_id to str to ensure CSV-loaded (str) and API-loaded (int) match
    for r in existing + new:
        key = (str(r.get("sighting_id", "")), r.get("commodity", ""), r.get("sensor_key", ""))
        if key not in seen:
            seen.add(key)
            merged.append(r)

    merged.sort(key=lambda r: r.get("date", ""))
    return merged


# ============================================================================
# DATE UTILITIES
# ============================================================================

def last_sunday(ref_date: datetime = None) -> datetime:
    """Find the most recent Sunday before ref_date (or today)."""
    if ref_date is None:
        ref_date = datetime.utcnow()
    d = ref_date.date()
    # weekday(): Monday=0 ... Sunday=6
    days_since_sunday = (d.weekday() + 1) % 7
    if days_since_sunday == 0:
        days_since_sunday = 7  # If today is Sunday, go back to previous Sunday
    sunday = d - timedelta(days=days_since_sunday)
    return datetime(sunday.year, sunday.month, sunday.day, 23, 59, 59)


def iso_week_monday(date_str: str) -> str:
    """Return the Monday of the ISO week that date_str belongs to."""
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    monday = d - timedelta(days=d.weekday())
    return monday.strftime("%Y-%m-%d")


def generate_weeks(start_date: str, end_date: str) -> List[str]:
    """Generate all Monday dates between start and end (inclusive of week)."""
    s = datetime.strptime(start_date, "%Y-%m-%d").date()
    e = datetime.strptime(end_date, "%Y-%m-%d").date()
    # Start from the Monday of the week containing start_date
    monday = s - timedelta(days=s.weekday())
    weeks = []
    while monday <= e:
        weeks.append(monday.strftime("%Y-%m-%d"))
        monday += timedelta(days=7)
    return weeks


def generate_months(start_month: str, end_month: str) -> List[str]:
    """Generate all YYYY-MM strings from start to end inclusive."""
    months = []
    y, m = int(start_month[:4]), int(start_month[5:7])
    ey, em = int(end_month[:4]), int(end_month[5:7])
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def generate_daily_dates(start: str, end: str) -> List[str]:
    """Generate all YYYY-MM-DD strings from start to end inclusive."""
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    dates = []
    d = s
    while d <= e:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return dates


# ============================================================================
# AGGREGATION
# ============================================================================

def aggregate_containers(records: List[dict], end_date: datetime) -> dict:
    """
    Aggregate container records into weekly TEU by sensor_key.
    Returns dict: {sensor_key: {"weeks": [...], "teu": [...]}}
    """
    container_recs = [r for r in records if r.get("commodity") == "containers"]

    # Group by sensor_key and week
    weekly: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in container_recs:
        key = r["sensor_key"]
        week = iso_week_monday(r["date"])
        weekly[key][week] += int(r.get("teu", 0))

    start_str = DEFAULT_START_DATE.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    all_weeks = generate_weeks(start_str, end_str)

    result = {}
    for sensor_key in ["vancouver_cn", "vancouver_cpkc", "prince_rupert_cn"]:
        teu_by_week = weekly.get(sensor_key, {})
        result[sensor_key] = {
            "weeks": all_weeks,
            "teu": [teu_by_week.get(w, 0) for w in all_weeks],
        }

    return result


def aggregate_exports(records: List[dict], end_date: datetime) -> dict:
    """
    Aggregate export records into monthly and daily counts by sensor_key.
    """
    export_recs = [r for r in records if r.get("commodity") != "containers"]
    commodities = ["coal", "potash", "grain", "forest_products", "lpg", "total_cars"]

    # Group by sensor_key, month/date, commodity
    monthly: Dict[str, Dict[str, Dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )
    daily: Dict[str, Dict[str, Dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )

    for r in export_recs:
        key = r["sensor_key"]
        date_str = r["date"]
        month_str = date_str[:7]
        commodity = r["commodity"]
        count = int(r.get("car_count", 0))
        monthly[key][month_str][commodity] += count
        daily[key][date_str][commodity] += count

    # Date ranges
    start_month = DEFAULT_START_DATE.strftime("%Y-%m")
    end_month = end_date.strftime("%Y-%m")
    all_months = generate_months(start_month, end_month)

    # Daily: most recent 90 days
    end_d = end_date.date()
    start_120 = end_d - timedelta(days=119)
    daily_dates = generate_daily_dates(start_120.strftime("%Y-%m-%d"), end_d.strftime("%Y-%m-%d"))

    result = {}

    # Standard export sensors (all commodities)
    for sensor_key in ["vancouver_cn", "vancouver_cpkc", "prince_rupert_cn"]:
        m_data = monthly.get(sensor_key, {})
        d_data = daily.get(sensor_key, {})

        entry = {"months": all_months}
        for c in commodities:
            entry[c] = [m_data.get(m, {}).get(c, 0) for m in all_months]

        entry["daily_dates"] = daily_dates
        for c in commodities:
            entry[f"daily_{c}"] = [d_data.get(d, {}).get(c, 0) for d in daily_dates]

        result[sensor_key] = entry

    # Blaine BNSF: coal + total_cars
    blaine_m = monthly.get("blaine_bnsf", {})
    result["blaine_bnsf_coal"] = {
        "months": all_months,
        "coal": [blaine_m.get(m, {}).get("coal", 0) for m in all_months],
        "total_cars": [blaine_m.get(m, {}).get("total_cars", 0) for m in all_months],
    }

    return result


def aggregate_totals_by_railroad(exports: dict) -> dict:
    """
    Sum exports by railroad:
      CN = vancouver_cn + prince_rupert_cn
      CPKC = vancouver_cpkc
      BNSF Coal = blaine_bnsf_coal
    """
    months = exports["vancouver_cn"]["months"]
    commodities = ["coal", "potash", "grain", "forest_products", "lpg"]

    cn = {}
    for c in commodities:
        van = exports["vancouver_cn"][c]
        pr = exports["prince_rupert_cn"][c]
        cn[c] = [van[i] + pr[i] for i in range(len(months))]

    cpkc = {}
    for c in commodities:
        cpkc[c] = exports["vancouver_cpkc"][c]

    bnsf_coal = exports["blaine_bnsf_coal"]["coal"]

    # Total car movements (all non-loco on revenue trains, including intermodal/automotive)
    cn_total = exports["vancouver_cn"].get("total_cars", [0] * len(months))
    pr_total = exports["prince_rupert_cn"].get("total_cars", [0] * len(months))
    cn["total_cars"] = [cn_total[i] + pr_total[i] for i in range(len(months))]
    cpkc["total_cars"] = exports["vancouver_cpkc"].get("total_cars", [0] * len(months))
    bnsf_total = exports["blaine_bnsf_coal"].get("total_cars", [0] * len(months))

    return {
        "months": months,
        "cn": cn,
        "cpkc": cpkc,
        "bnsf_coal": bnsf_coal,
        "bnsf_total_cars": bnsf_total,
    }


# ============================================================================
# INCREMENTAL UPDATE LOGIC
# ============================================================================

def determine_fetch_window(existing: List[dict], args) -> Tuple[datetime, datetime]:
    """Determine start/end dates for the API fetch."""
    # Use yesterday as end date to include partial current week
    end_date = datetime.utcnow().replace(hour=23, minute=59, second=59) - timedelta(days=1)

    if args.rebuild or not existing:
        start_date = DEFAULT_START_DATE
        mode = "FULL REBUILD" if args.rebuild else "INITIAL FETCH"
    elif args.days:
        start_date = datetime.utcnow() - timedelta(days=args.days)
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        mode = f"LAST {args.days} DAYS"
    else:
        # Incremental: from last date - 2 days overlap
        dates = [r["date"] for r in existing if r.get("date")]
        if dates:
            last = max(dates)
            last_dt = datetime.strptime(last, "%Y-%m-%d")
            start_date = last_dt - timedelta(days=2)
        else:
            start_date = DEFAULT_START_DATE
        mode = "INCREMENTAL UPDATE"

    print(f"Mode: {mode}", flush=True)
    print(f"Fetch window: {start_date.date()} to {end_date.date()}", flush=True)
    return start_date, end_date


# ============================================================================
# OUTPUT
# ============================================================================

def build_output_json(records: List[dict], end_date: datetime) -> dict:
    """Build the final output JSON structure."""
    containers = aggregate_containers(records, end_date)
    exports = aggregate_exports(records, end_date)
    totals = aggregate_totals_by_railroad(exports)

    return {
        "last_updated": datetime.utcnow().strftime("%Y-%m-%d"),
        "containers": containers,
        "exports": exports,
        "total_by_railroad": totals,
    }


def save_output_json(data: dict, path: Path):
    """Write JSON output file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved output JSON to {path.name}", flush=True)


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Western Canada Ports Weekly Volumes")
    parser.add_argument("--days", type=int, default=None,
                        help="Override fetch window to last N days")
    parser.add_argument("--rebuild", action="store_true",
                        help="Force full re-fetch from Jan 1 2024")
    args = parser.parse_args()

    api_key = os.environ.get("RAILSTATE_API_KEY", HARDCODED_API_KEY)

    print("=" * 70, flush=True)
    print("WESTERN CANADA PORTS - WEEKLY VOLUMES", flush=True)
    print(f"Run Time: {datetime.utcnow().isoformat()}", flush=True)
    print("=" * 70, flush=True)

    # Load existing raw data
    existing = [] if args.rebuild else load_raw_csv(RAW_CSV_PATH)

    # Determine fetch window
    start_date, end_date = determine_fetch_window(existing, args)

    # Initialize API client
    fetcher = RailStateFetcher(api_key)
    fetcher.load_sensors()
    print(f"Loaded {len(fetcher._sensor_cache)} sensors", flush=True)

    # Fetch data
    print("\n" + "=" * 70, flush=True)
    print("FETCHING DATA", flush=True)
    print("=" * 70, flush=True)

    new_records = fetch_all_data(fetcher, start_date, end_date)
    print(f"\nNew records fetched: {len(new_records):,}", flush=True)

    # Merge with existing
    combined = merge_raw_records(existing, new_records)
    print(f"Total records after merge: {len(combined):,}", flush=True)

    if not combined:
        print("\nNo data found!", flush=True)
        sys.exit(1)

    # Save raw CSV
    print("\n" + "=" * 70, flush=True)
    print("SAVING RAW DATA", flush=True)
    print("=" * 70, flush=True)
    save_raw_csv(combined, RAW_CSV_PATH)

    # Build and save output JSON
    print("\n" + "=" * 70, flush=True)
    print("AGGREGATING & SAVING OUTPUT", flush=True)
    print("=" * 70, flush=True)

    output = build_output_json(combined, end_date)
    save_output_json(output, OUTPUT_JSON_PATH)

    # Print summary
    print("\n" + "=" * 70, flush=True)
    print("SUMMARY", flush=True)
    print("=" * 70, flush=True)

    container_recs = [r for r in combined if r.get("commodity") == "containers"]
    export_recs = [r for r in combined if r.get("commodity") != "containers"]
    total_teu = sum(int(r.get("teu", 0)) for r in container_recs)
    total_export_cars = sum(int(r.get("car_count", 0)) for r in export_recs)

    print(f"Date range: {DEFAULT_START_DATE.date()} to {end_date.date()}", flush=True)
    print(f"Container records: {len(container_recs):,} ({total_teu:,} TEU)", flush=True)
    print(f"Export records: {len(export_recs):,} ({total_export_cars:,} cars)", flush=True)

    # Container weekly totals (last 4 weeks)
    weeks = output["containers"]["vancouver_cn"]["weeks"]
    if len(weeks) >= 4:
        print(f"\nLast 4 weeks container TEU:", flush=True)
        print(f"  {'Week':<12} {'Van CN':>8} {'Van CPKC':>10} {'Pr Rupert':>10}", flush=True)
        for i in range(-4, 0):
            w = weeks[i]
            vcn = output["containers"]["vancouver_cn"]["teu"][i]
            vcp = output["containers"]["vancouver_cpkc"]["teu"][i]
            pr = output["containers"]["prince_rupert_cn"]["teu"][i]
            print(f"  {w:<12} {vcn:>8,} {vcp:>10,} {pr:>10,}", flush=True)

    # Export monthly totals (last 3 months)
    months = output["exports"]["vancouver_cn"]["months"]
    if len(months) >= 3:
        print(f"\nLast 3 months export cars (Vancouver CN):", flush=True)
        print(f"  {'Month':<10} {'Coal':>6} {'Potash':>8} {'Grain':>7} {'Forest':>8} {'LPG':>5}", flush=True)
        for i in range(-3, 0):
            m = months[i]
            e = output["exports"]["vancouver_cn"]
            print(f"  {m:<10} {e['coal'][i]:>6} {e['potash'][i]:>8} {e['grain'][i]:>7} "
                  f"{e['forest_products'][i]:>8} {e['lpg'][i]:>5}", flush=True)

    print("\n" + "=" * 70, flush=True)
    print("DONE", flush=True)
    print("=" * 70, flush=True)


if __name__ == "__main__":
    main()
