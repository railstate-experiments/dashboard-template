#!/usr/bin/env python3
"""
Oregon Full Data Pull — All Sensors, All Data, Full Year 2025
=============================================================
Fetches ALL sightings (all trains, all cars, all containers, all hazmat
placards) from every RailState sensor in the state of Oregon for
January 1, 2025 through December 31, 2025.

This script pulls raw sighting data at the car level and saves it to CSV
for downstream analysis (tables, charts, map visualizations).

Usage:
  python oregon_full_pull.py                    # full pull, all sensors
  python oregon_full_pull.py --sensor "Salem, OR"  # single sensor test
  python oregon_full_pull.py --month 1          # January only (for testing)

Environment:
  RAILSTATE_API_KEY - API key (optional, falls back to hardcoded key)

Outputs:
  ../data/oregon_trains_raw.csv      - train-level records
  ../data/oregon_cars_raw.csv        - car-level records (with hazmat)
  ../data/oregon_containers_raw.csv  - container-level records
  ../data/oregon_pull_log.json       - pull metadata and sensor summary
"""

import argparse
import csv
import json
import os
import sys
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin

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

# Date range: full year 2025
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2025, 12, 31, 23, 59, 59)

# API pagination
RESPONSE_SIZE = 500

# Rate limiting
RATE_LIMIT_DELAY = 0.1          # seconds between pagination requests
SENSOR_DELAY = 2.0              # seconds between sensors
MONTH_DELAY = 1.0               # seconds between monthly chunks
RATE_LIMIT_MAX_RETRIES = 3
RATE_LIMIT_RETRY_DELAY = 5.0    # base delay for 429 backoff

# Output paths
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
TRAINS_CSV = DATA_DIR / "oregon_trains_raw.csv"
CARS_CSV = DATA_DIR / "oregon_cars_raw.csv"
CONTAINERS_CSV = DATA_DIR / "oregon_containers_raw.csv"
LOG_PATH = DATA_DIR / "oregon_pull_log.json"

# ============================================================================
# API CLIENT
# ============================================================================

class RailStateFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._sensor_cache = {}
        self._request_count = 0

    def _request(self, url: str, params: dict = None) -> Tuple[dict, Optional[str]]:
        headers = {"Authorization": f"Bearer {self.api_key}", "Accept": "application/json"}
        for attempt in range(RATE_LIMIT_MAX_RETRIES + 1):
            try:
                self._request_count += 1
                response = requests.get(url, params=params, headers=headers, timeout=120)
                if response.status_code == 429:
                    if attempt < RATE_LIMIT_MAX_RETRIES:
                        wait = RATE_LIMIT_RETRY_DELAY * (2 ** attempt)
                        print(f"    Rate limited (429). Waiting {wait:.0f}s...")
                        time.sleep(wait)
                        continue
                    return {}, "Rate limited (429) after retries"
                if response.status_code != 200:
                    return {}, f"HTTP {response.status_code}"
                return response.json(), None
            except requests.exceptions.Timeout:
                if attempt < RATE_LIMIT_MAX_RETRIES:
                    time.sleep(RATE_LIMIT_RETRY_DELAY)
                    continue
                return {}, "Timeout after retries"
            except Exception as e:
                return {}, str(e)
        return {}, "Max retries exceeded"

    def load_sensors(self) -> Dict[str, dict]:
        """Load all sensors, return {name: {sensorId, region, country, ...}}"""
        if self._sensor_cache:
            return self._sensor_cache
        url = urljoin(API_BASE_URL, "/api/v3/sensors/overview")
        data, error = self._request(url)
        if error:
            print(f"ERROR: Could not load sensors: {error}")
            sys.exit(1)
        for sensor in data.get("sensors", []):
            name = sensor.get("name")
            if name:
                self._sensor_cache[name] = sensor
        print(f"  Loaded {len(self._sensor_cache)} sensors from API")
        return self._sensor_cache

    def get_oregon_sensors(self) -> List[dict]:
        """Return all active Oregon sensors."""
        sensors = self.load_sensors()
        oregon = []
        for name, info in sensors.items():
            region = (info.get("region") or "").strip()
            is_active = info.get("isActive", False)
            if region.lower() in ("oregon", "or") and is_active:
                oregon.append(info)
        oregon.sort(key=lambda s: s.get("name", ""))
        return oregon

    def fetch_sightings(self, sensor_id: int, start: datetime, end: datetime) -> List[dict]:
        """Fetch ALL sightings for a sensor in a time range (no direction filter)."""
        url = urljoin(API_BASE_URL, "/api/v3/trains/full_sightings")
        all_sightings = []
        params = {
            "sensors": str(sensor_id),
            "detection_time_from": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "detection_time_to": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "response_size": RESPONSE_SIZE,
        }
        page = 0
        while True:
            page += 1
            data, error = self._request(url, params)
            if error:
                print(f"      Page {page}: ERROR - {error}")
                break
            sightings = data.get("sightings", [])
            if not sightings:
                break
            all_sightings.extend(sightings)
            if page % 10 == 0:
                print(f"      Page {page}: {len(all_sightings)} trains so far...")
            next_link = data.get("nextRequestLink")
            if not next_link:
                break
            url, params = next_link, None
            time.sleep(RATE_LIMIT_DELAY)

        return all_sightings


# ============================================================================
# DATA EXTRACTION
# ============================================================================

def extract_placard(car: dict) -> Optional[str]:
    """Get the first non-EMPTY hazmat placard from a car."""
    hazmats = car.get("hazmats") or []
    for hazmat in hazmats:
        placard = hazmat.get("placardType")
        if placard and placard != "EMPTY":
            return placard
    return None

def extract_all_placards(car: dict) -> List[str]:
    """Get ALL non-EMPTY hazmat placards from a car."""
    placards = []
    hazmats = car.get("hazmats") or []
    for hazmat in hazmats:
        placard = hazmat.get("placardType")
        if placard and placard != "EMPTY":
            placards.append(placard)
    return placards

def get_car_capacity(car: dict) -> dict:
    """Extract capacity fields from a car."""
    params = car.get("equipmentParameters") or {}
    dims = params.get("dimensions") or {}
    return {
        "gallonage_capacity": dims.get("gallonageCapacity"),
        "cubic_feet_capacity": dims.get("cubicFeetCapacity"),
        "cubic_capacity": dims.get("cubicCapacity"),
        "type_code": params.get("typeCode"),
    }

def process_sightings(sightings: List[dict], sensor_name: str, sensor_region: str,
                       sensor_lat: float, sensor_lng: float,
                       sensor_railways: List[str]) -> Tuple[list, list, list]:
    """
    Process raw sightings into train-level, car-level, and container-level rows.
    Returns (train_rows, car_rows, container_rows).
    """
    train_rows = []
    car_rows = []
    container_rows = []

    for sighting in sightings:
        sighting_id = sighting.get("sightingId", "")
        detection_time = sighting.get("detectionTimeUTC", "")
        direction = sighting.get("direction", "")
        train_type = sighting.get("trainType", "")
        train_operator = sighting.get("trainOperator", "")

        cars = sighting.get("cars", [])

        # Parse date from detection time
        try:
            dt = datetime.strptime(detection_time, "%Y-%m-%dT%H:%M:%SZ")
            date_str = dt.strftime("%Y-%m-%d")
        except:
            date_str = detection_time[:10] if len(detection_time) >= 10 else ""

        # Count cars by type
        total_cars = len(cars)
        locomotive_count = 0
        tank_car_count = 0
        hopper_count = 0
        gondola_count = 0
        boxcar_count = 0
        flatcar_count = 0
        container_car_count = 0
        other_car_count = 0
        hazmat_car_count = 0
        total_containers = 0

        for car in cars:
            car_type = car.get("type", "")
            if "Locomotive" in car_type:
                locomotive_count += 1
            elif car_type == "Tank Car":
                tank_car_count += 1
            elif "Hopper" in car_type:
                hopper_count += 1
            elif "Gondola" in car_type:
                gondola_count += 1
            elif "Box" in car_type:
                boxcar_count += 1
            elif "Flat" in car_type or "Well" in car_type:
                flatcar_count += 1
            elif "Container" in car_type or "Intermodal" in car_type:
                container_car_count += 1
            else:
                other_car_count += 1

            if extract_placard(car):
                hazmat_car_count += 1

            # Count containers on this car
            car_containers = car.get("containers") or []
            total_containers += len(car_containers)

        # Train-level row
        train_rows.append({
            "sighting_id": sighting_id,
            "sensor_name": sensor_name,
            "sensor_region": sensor_region,
            "sensor_lat": sensor_lat,
            "sensor_lng": sensor_lng,
            "sensor_railways": "|".join(sensor_railways),
            "detection_time": detection_time,
            "date": date_str,
            "direction": direction,
            "train_type": train_type,
            "train_operator": train_operator,
            "total_cars": total_cars,
            "locomotive_count": locomotive_count,
            "tank_car_count": tank_car_count,
            "hopper_count": hopper_count,
            "gondola_count": gondola_count,
            "boxcar_count": boxcar_count,
            "flatcar_count": flatcar_count,
            "container_car_count": container_car_count,
            "other_car_count": other_car_count,
            "hazmat_car_count": hazmat_car_count,
            "total_containers": total_containers,
        })

        # Car-level rows
        for car_idx, car in enumerate(cars):
            car_type = car.get("type", "")
            car_id = car.get("carId", "")
            cap = get_car_capacity(car)
            placard = extract_placard(car)
            all_placards = extract_all_placards(car)

            car_rows.append({
                "sighting_id": sighting_id,
                "sensor_name": sensor_name,
                "sensor_region": sensor_region,
                "sensor_lat": sensor_lat,
                "sensor_lng": sensor_lng,
                "detection_time": detection_time,
                "date": date_str,
                "direction": direction,
                "train_type": train_type,
                "train_operator": train_operator,
                "car_position": car_idx + 1,
                "car_id": car_id,
                "car_type": car_type,
                "type_code": cap["type_code"] or "",
                "gallonage_capacity": cap["gallonage_capacity"] or "",
                "cubic_feet_capacity": cap["cubic_feet_capacity"] or "",
                "cubic_capacity": cap["cubic_capacity"] or "",
                "hazmat_placard": placard or "",
                "all_hazmat_placards": "|".join(all_placards) if all_placards else "",
                "is_hazmat": 1 if placard else 0,
            })

            # Container-level rows
            car_containers = car.get("containers") or []
            for cont_idx, container in enumerate(car_containers):
                container_rows.append({
                    "sighting_id": sighting_id,
                    "sensor_name": sensor_name,
                    "detection_time": detection_time,
                    "date": date_str,
                    "direction": direction,
                    "train_operator": train_operator,
                    "car_id": car_id,
                    "car_position": car_idx + 1,
                    "container_position": cont_idx + 1,
                    "container_id": container.get("containerId", ""),
                    "container_type": container.get("containerType", ""),
                    "container_size": container.get("containerSize", ""),
                    "container_owner": container.get("containerOwner", ""),
                })

    return train_rows, car_rows, container_rows


# ============================================================================
# CSV I/O
# ============================================================================

TRAIN_FIELDS = [
    "sighting_id", "sensor_name", "sensor_region", "sensor_lat", "sensor_lng",
    "sensor_railways", "detection_time", "date", "direction", "train_type",
    "train_operator", "total_cars", "locomotive_count", "tank_car_count",
    "hopper_count", "gondola_count", "boxcar_count", "flatcar_count",
    "container_car_count", "other_car_count", "hazmat_car_count", "total_containers",
]

CAR_FIELDS = [
    "sighting_id", "sensor_name", "sensor_region", "sensor_lat", "sensor_lng",
    "detection_time", "date", "direction", "train_type", "train_operator",
    "car_position", "car_id", "car_type", "type_code",
    "gallonage_capacity", "cubic_feet_capacity", "cubic_capacity",
    "hazmat_placard", "all_hazmat_placards", "is_hazmat",
]

CONTAINER_FIELDS = [
    "sighting_id", "sensor_name", "detection_time", "date", "direction",
    "train_operator", "car_id", "car_position", "container_position",
    "container_id", "container_type", "container_size", "container_owner",
]

def write_csv(path: Path, rows: list, fields: list, append: bool = False):
    """Write rows to CSV. If append=True, skip header if file exists."""
    mode = "a" if append and path.exists() else "w"
    write_header = not (append and path.exists())
    with open(path, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Oregon Full Data Pull")
    parser.add_argument("--sensor", type=str, help="Pull a single sensor by name (for testing)")
    parser.add_argument("--month", type=int, help="Pull a single month (1-12) for testing")
    args = parser.parse_args()

    api_key = os.environ.get("RAILSTATE_API_KEY", HARDCODED_API_KEY)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    fetcher = RailStateFetcher(api_key)

    # ── Step 1: Get Oregon sensors ──
    print("=" * 70)
    print("OREGON FULL DATA PULL — January 1 to December 31, 2025")
    print("=" * 70)
    print("\nStep 1: Loading sensor inventory...")
    oregon_sensors = fetcher.get_oregon_sensors()

    if args.sensor:
        oregon_sensors = [s for s in oregon_sensors if s["name"].lower() == args.sensor.lower()]
        if not oregon_sensors:
            print(f"ERROR: Sensor '{args.sensor}' not found in Oregon")
            all_names = [s["name"] for s in fetcher.get_oregon_sensors()]
            print(f"  Available: {', '.join(all_names)}")
            sys.exit(1)

    print(f"\n  Found {len(oregon_sensors)} Oregon sensors:")
    for s in oregon_sensors:
        rr = ", ".join(s.get("railways", []))
        print(f"    {s['name']:30s}  sensorId={s['sensorId']}  railways=[{rr}]")

    # ── Step 2: Build monthly date ranges ──
    months = []
    if args.month:
        m = args.month
        m_start = datetime(2025, m, 1)
        if m == 12:
            m_end = datetime(2025, 12, 31, 23, 59, 59)
        else:
            m_end = datetime(2025, m + 1, 1) - timedelta(seconds=1)
        months.append((m_start, m_end))
    else:
        for m in range(1, 13):
            m_start = datetime(2025, m, 1)
            if m == 12:
                m_end = datetime(2025, 12, 31, 23, 59, 59)
            else:
                m_end = datetime(2025, m + 1, 1) - timedelta(seconds=1)
            months.append((m_start, m_end))

    print(f"\n  Date range: {months[0][0].strftime('%Y-%m-%d')} to {months[-1][1].strftime('%Y-%m-%d')}")
    print(f"  Monthly chunks: {len(months)}")

    # ── Step 3: Clear output files (write headers) ──
    write_csv(TRAINS_CSV, [], TRAIN_FIELDS, append=False)
    write_csv(CARS_CSV, [], CAR_FIELDS, append=False)
    write_csv(CONTAINERS_CSV, [], CONTAINER_FIELDS, append=False)

    # ── Step 4: Fetch data sensor by sensor, month by month ──
    print("\nStep 2: Fetching sightings...\n")

    grand_totals = {
        "trains": 0, "cars": 0, "containers": 0, "hazmat_cars": 0,
        "api_requests": 0,
    }
    sensor_summaries = []

    for s_idx, sensor in enumerate(oregon_sensors):
        s_name = sensor["name"]
        s_id = sensor["sensorId"]
        s_lat = sensor.get("lat", 0)
        s_lng = sensor.get("lng", 0)
        s_rr = sensor.get("railways", [])
        s_region = sensor.get("region", "Oregon")

        print(f"  [{s_idx+1}/{len(oregon_sensors)}] {s_name} (id={s_id})")

        sensor_trains = 0
        sensor_cars = 0
        sensor_containers = 0
        sensor_hazmat = 0

        for m_start, m_end in months:
            m_label = m_start.strftime("%b %Y")
            print(f"    {m_label}...", end=" ", flush=True)

            sightings = fetcher.fetch_sightings(s_id, m_start, m_end)
            print(f"{len(sightings)} trains", end="", flush=True)

            if sightings:
                train_rows, car_rows, container_rows = process_sightings(
                    sightings, s_name, s_region, s_lat, s_lng, s_rr
                )

                # Append to CSVs
                if train_rows:
                    write_csv(TRAINS_CSV, train_rows, TRAIN_FIELDS, append=True)
                if car_rows:
                    write_csv(CARS_CSV, car_rows, CAR_FIELDS, append=True)
                if container_rows:
                    write_csv(CONTAINERS_CSV, container_rows, CONTAINER_FIELDS, append=True)

                hazmat_count = sum(1 for c in car_rows if c["is_hazmat"])
                print(f", {len(car_rows)} cars, {len(container_rows)} containers, {hazmat_count} hazmat")

                sensor_trains += len(train_rows)
                sensor_cars += len(car_rows)
                sensor_containers += len(container_rows)
                sensor_hazmat += hazmat_count
            else:
                print()

            time.sleep(MONTH_DELAY)

        sensor_summaries.append({
            "name": s_name,
            "sensor_id": s_id,
            "lat": s_lat,
            "lng": s_lng,
            "railways": s_rr,
            "trains": sensor_trains,
            "cars": sensor_cars,
            "containers": sensor_containers,
            "hazmat_cars": sensor_hazmat,
        })

        grand_totals["trains"] += sensor_trains
        grand_totals["cars"] += sensor_cars
        grand_totals["containers"] += sensor_containers
        grand_totals["hazmat_cars"] += sensor_hazmat

        print(f"    Subtotal: {sensor_trains:,} trains, {sensor_cars:,} cars, "
              f"{sensor_containers:,} containers, {sensor_hazmat:,} hazmat cars\n")

        if s_idx < len(oregon_sensors) - 1:
            time.sleep(SENSOR_DELAY)

    grand_totals["api_requests"] = fetcher._request_count

    # ── Step 5: Summary ──
    print("=" * 70)
    print("PULL COMPLETE")
    print("=" * 70)
    print(f"  Total trains:     {grand_totals['trains']:>10,}")
    print(f"  Total cars:       {grand_totals['cars']:>10,}")
    print(f"  Total containers: {grand_totals['containers']:>10,}")
    print(f"  Total hazmat:     {grand_totals['hazmat_cars']:>10,}")
    print(f"  API requests:     {grand_totals['api_requests']:>10,}")
    print(f"\n  Outputs:")
    print(f"    {TRAINS_CSV}")
    print(f"    {CARS_CSV}")
    print(f"    {CONTAINERS_CSV}")

    # ── Step 6: Save pull log ──
    log = {
        "pull_date": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "date_range": {
            "start": months[0][0].strftime("%Y-%m-%d"),
            "end": months[-1][1].strftime("%Y-%m-%d"),
        },
        "sensors_queried": len(oregon_sensors),
        "totals": grand_totals,
        "sensor_summaries": sensor_summaries,
    }
    with open(LOG_PATH, "w") as f:
        json.dump(log, f, indent=2)
    print(f"    {LOG_PATH}")


if __name__ == "__main__":
    main()
