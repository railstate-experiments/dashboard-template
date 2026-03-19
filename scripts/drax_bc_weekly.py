#!/usr/bin/env python3
"""
Drax BC Operations - Wood Pellet Volume
========================================
Counts large covered hoppers (≥6,000 cu ft est. capacity) heading westbound
at Phelan, BC (with Terrace, BC as fallback for days with no Phelan data).

These are covered hoppers carrying wood pellets for Drax export via Prince Rupert.
Converts to tonnes at 105 tonnes/car.

Usage:
    python drax_bc_weekly.py              # incremental update
    python drax_bc_weekly.py --rebuild    # full re-fetch from Jan 2024
    python drax_bc_weekly.py --days 60    # fetch last 60 days

Outputs:
    ../data/drax_bc_raw.csv   - raw car-level data
    ../data/drax_bc.json      - JSON for dashboard
"""

import argparse
import json
import os
import pandas as pd
import requests
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin

# ============================================================================
# CONFIGURATION
# ============================================================================

API_BASE_URL = "https://api.railstate.com"

HARDCODED_API_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJhcGkucmFpbHN0YXRlLmNvbSIsImlzcyI6InJhaWxzdGF0ZS5jb20iLCJ1aWQiOiJ5RWE5aTFSbjljTmhtbEFEQkNYd3BFR3h1eEczIiwidGlkIjoiRGFuIERldm9lIFRlc3RpbmcifQ.8YQJPJE3X2xomrgUCXo41tx0Vh0gCRWRZ2JbQKYpAXE'

DEFAULT_START_DATE = datetime(2024, 1, 1)

# Minimum cubic capacity (cu ft) for wood pellet covered hoppers
MIN_CUBIC_CAPACITY = 6000

# Tonnes per car
TONNES_PER_CAR = 105

# Sensor config
PRIMARY_SENSOR = {'name': 'Phelan, BC', 'direction': 'westbound'}
FALLBACK_SENSOR = {'name': 'Terrace, BC', 'direction': 'westbound'}

# Output paths
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / 'data'
RAW_CSV_PATH = DATA_DIR / 'drax_bc_raw.csv'
JSON_PATH = DATA_DIR / 'drax_bc.json'


# ============================================================================
# API
# ============================================================================

class RailStateFetcher:
    def __init__(self, api_key: str, base_url: str = API_BASE_URL):
        self.api_key = api_key
        self.base_url = base_url
        self._sensor_cache = {}

    def _request(self, url: str, params: dict = None) -> Tuple[dict, Optional[str]]:
        try:
            headers = {'Authorization': f'Bearer {self.api_key}', 'Accept': 'application/json'}
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
        url = urljoin(self.base_url, '/api/v3/sensors/overview')
        data, error = self._request(url)
        if error:
            print(f"  Warning: Could not load sensors: {error}", flush=True)
            return {}
        for sensor in data.get('sensors', []):
            name, sid = sensor.get('name'), sensor.get('sensorId')
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
        url = urljoin(self.base_url, '/api/v3/trains/full_sightings')
        all_sightings = []
        params = {
            'sensors': str(sensor_id),
            'detection_time_from': start.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'detection_time_to': end.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'response_size': 500
        }
        while True:
            data, error = self._request(url, params)
            if error:
                print(f"      API error: {error}", flush=True)
                break
            sightings = data.get('sightings', [])
            if not sightings:
                break
            if direction:
                sightings = [s for s in sightings
                             if s.get('direction', '').lower() == direction.lower()]
            all_sightings.extend(sightings)
            next_link = data.get('nextRequestLink')
            if not next_link:
                break
            url, params = next_link, None
        return all_sightings


# ============================================================================
# CAR EXTRACTION
# ============================================================================

def get_cubic_capacity(car: dict) -> float:
    """Get estimated cubic capacity in cubic feet from car data."""
    try:
        params = car.get('equipmentParameters') or {}
        dims = params.get('dimensions') or {}
        # Try cubicCapacity first
        cap = dims.get('cubicCapacity') or dims.get('cubicFeetCapacity')
        if cap and float(cap) > 0:
            return float(cap)
        # Try volume field
        vol = dims.get('volume')
        if vol and float(vol) > 0:
            return float(vol)
    except (TypeError, ValueError):
        pass
    return 0.0


def is_large_covered_hopper(car: dict) -> bool:
    """Check if car is a covered hopper with cubic capacity >= 6000 cu ft."""
    car_type = car.get('type', '').lower()
    if 'covered hopper' not in car_type:
        return False
    capacity = get_cubic_capacity(car)
    return capacity >= MIN_CUBIC_CAPACITY


def extract_drax_cars(sightings: List[dict], sensor_name: str) -> List[dict]:
    """Extract large covered hopper cars from sightings."""
    records = []
    for sighting in sightings:
        detection_time = sighting.get('detectionTimeUTC')
        cars = sighting.get('cars', [])
        for car in cars:
            if is_large_covered_hopper(car):
                records.append({
                    'car_id': car.get('carId', ''),
                    'detection_time': detection_time,
                    'sensor': sensor_name,
                    'cubic_capacity': get_cubic_capacity(car),
                    'sighting_id': sighting.get('sightingId'),
                })
    return records


# ============================================================================
# FETCH WITH FALLBACK
# ============================================================================

def fetch_with_fallback(fetcher: RailStateFetcher, start: datetime, end: datetime) -> List[dict]:
    """Fetch from Phelan, use Terrace as fallback for days with no Phelan data."""
    # Fetch Phelan
    phelan_id = fetcher.get_sensor_id(PRIMARY_SENSOR['name'])
    terrace_id = fetcher.get_sensor_id(FALLBACK_SENSOR['name'])

    all_records = []

    if phelan_id:
        print(f"  Fetching {PRIMARY_SENSOR['name']} ({PRIMARY_SENSOR['direction']})...", flush=True)
        sightings = fetcher.fetch_sightings(phelan_id, start, end, PRIMARY_SENSOR['direction'])
        phelan_records = extract_drax_cars(sightings, PRIMARY_SENSOR['name'])
        print(f"    {len(sightings)} sightings -> {len(phelan_records)} Drax cars", flush=True)
    else:
        print(f"  Warning: Sensor not found: {PRIMARY_SENSOR['name']}", flush=True)
        phelan_records = []

    if terrace_id:
        print(f"  Fetching {FALLBACK_SENSOR['name']} ({FALLBACK_SENSOR['direction']})...", flush=True)
        sightings = fetcher.fetch_sightings(terrace_id, start, end, FALLBACK_SENSOR['direction'])
        terrace_records = extract_drax_cars(sightings, FALLBACK_SENSOR['name'])
        print(f"    {len(sightings)} sightings -> {len(terrace_records)} Drax cars", flush=True)
    else:
        print(f"  Warning: Sensor not found: {FALLBACK_SENSOR['name']}", flush=True)
        terrace_records = []

    # Group by date
    phelan_by_date = defaultdict(list)
    for r in phelan_records:
        dt = pd.to_datetime(r['detection_time'], format='ISO8601', utc=True)
        phelan_by_date[dt.date()].append(r)

    terrace_by_date = defaultdict(list)
    for r in terrace_records:
        dt = pd.to_datetime(r['detection_time'], format='ISO8601', utc=True)
        terrace_by_date[dt.date()].append(r)

    # Merge: use Phelan if available, Terrace as fallback
    all_dates = sorted(set(list(phelan_by_date.keys()) + list(terrace_by_date.keys())))
    fallback_days = 0
    for d in all_dates:
        if phelan_by_date[d]:
            all_records.extend(phelan_by_date[d])
        elif terrace_by_date[d]:
            all_records.extend(terrace_by_date[d])
            fallback_days += 1

    print(f"  Total: {len(all_records)} records ({fallback_days} days used Terrace fallback)", flush=True)
    return all_records


# ============================================================================
# AGGREGATION & OUTPUT
# ============================================================================

def build_json(raw_csv_path: Path, json_path: Path):
    """Build dashboard JSON from raw CSV."""
    df = pd.read_csv(raw_csv_path)
    df['detection_time'] = pd.to_datetime(df['detection_time'], format='ISO8601', utc=True)
    df['date'] = df['detection_time'].dt.date

    # Deduplicate by car_id and date
    df = df.drop_duplicates(subset=['car_id', 'date'], keep='first')
    df['date'] = pd.to_datetime(df['date'])

    # Daily counts
    daily = df.groupby('date').agg(car_count=('car_id', 'count')).reset_index()
    daily = daily.sort_values('date')

    # Fill missing dates
    if len(daily) > 0:
        date_range = pd.date_range(start=daily['date'].min(), end=daily['date'].max(), freq='D')
        daily = daily.set_index('date').reindex(date_range, fill_value=0).reset_index()
        daily = daily.rename(columns={'index': 'date'})

    # Weekly aggregation (Mon-Sun)
    daily['week_start'] = daily['date'] - pd.to_timedelta(daily['date'].dt.dayofweek, unit='D')
    weekly = daily.groupby('week_start').agg(car_count=('car_count', 'sum')).reset_index()
    # Drop the last incomplete week
    now = datetime.utcnow()
    last_sunday = pd.Timestamp(now.date() - timedelta(days=now.weekday() + 1))
    weekly = weekly[weekly['week_start'] <= last_sunday]

    # Monthly aggregation
    daily['month'] = daily['date'].dt.to_period('M')
    monthly = daily.groupby('month').agg(car_count=('car_count', 'sum')).reset_index()
    monthly['month'] = monthly['month'].astype(str)

    # Daily: last 120 days (extra 30 for MA warm-up, display last 90)
    daily_120 = daily.tail(120)

    result = {
        'last_updated': datetime.utcnow().strftime('%Y-%m-%d'),
        'tonnes_per_car': TONNES_PER_CAR,
        'min_cubic_capacity': MIN_CUBIC_CAPACITY,
        'weeks': [d.strftime('%Y-%m-%d') for d in weekly['week_start']],
        'weekly_cars': weekly['car_count'].tolist(),
        'months': monthly['month'].tolist(),
        'monthly_cars': monthly['car_count'].tolist(),
        'daily_dates': [d.strftime('%Y-%m-%d') for d in daily_120['date']],
        'daily_cars': daily_120['car_count'].tolist(),
    }

    with open(json_path, 'w') as f:
        json.dump(result, f, indent=2)

    total_cars = sum(result['monthly_cars'])
    print(f"\nJSON written: {json_path.name}", flush=True)
    print(f"  Weeks: {len(result['weeks'])}", flush=True)
    print(f"  Months: {len(result['months'])}", flush=True)
    print(f"  Total carloads: {total_cars:,}", flush=True)
    print(f"  Est. total tonnes: {total_cars * TONNES_PER_CAR:,}", flush=True)


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Drax BC Wood Pellet Volume')
    parser.add_argument('--days', type=int, default=None)
    parser.add_argument('--rebuild', action='store_true')
    args = parser.parse_args()

    api_key = os.environ.get('RAILSTATE_API_KEY', HARDCODED_API_KEY)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    yesterday = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    end_date = yesterday.replace(hour=23, minute=59, second=59)

    # Determine date range
    if args.rebuild:
        start_date = DEFAULT_START_DATE
        mode = "FULL REBUILD"
    elif args.days:
        start_date = (yesterday - timedelta(days=args.days - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
        mode = f"LAST {args.days} DAYS"
    elif RAW_CSV_PATH.exists():
        existing = pd.read_csv(RAW_CSV_PATH)
        existing['detection_time'] = pd.to_datetime(existing['detection_time'], format='ISO8601', utc=True)
        last_date = existing['detection_time'].max()
        start_date = (last_date - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        mode = "INCREMENTAL"
    else:
        start_date = DEFAULT_START_DATE
        mode = "INITIAL FETCH"

    print("=" * 70, flush=True)
    print("DRAX BC OPERATIONS — WOOD PELLET VOLUME", flush=True)
    print(f"Run Time: {datetime.utcnow().isoformat()}", flush=True)
    print(f"Mode: {mode}", flush=True)
    print(f"Date Range: {start_date.date()} to {end_date.date()}", flush=True)
    print(f"Min cubic capacity: {MIN_CUBIC_CAPACITY} cu ft", flush=True)
    print(f"Tonnes/car: {TONNES_PER_CAR}", flush=True)
    print("=" * 70, flush=True)

    fetcher = RailStateFetcher(api_key)
    fetcher.load_sensors()
    print(f"Loaded {len(fetcher._sensor_cache)} sensors", flush=True)

    new_records = fetch_with_fallback(fetcher, start_date, end_date)

    if not new_records:
        print("\nNo new data found.", flush=True)
        if RAW_CSV_PATH.exists():
            print("Rebuilding JSON from existing data...", flush=True)
            build_json(RAW_CSV_PATH, JSON_PATH)
        return

    new_df = pd.DataFrame(new_records)

    # Merge with existing
    if RAW_CSV_PATH.exists() and not args.rebuild:
        existing_df = pd.read_csv(RAW_CSV_PATH)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        combined['detection_time'] = pd.to_datetime(combined['detection_time'], format='ISO8601', utc=True)
        before = len(combined)
        combined = combined.drop_duplicates(subset=['car_id', 'detection_time'], keep='last')
        after = len(combined)
        if before != after:
            print(f"Deduplicated: {before:,} -> {after:,}", flush=True)
        combined = combined.sort_values('detection_time').reset_index(drop=True)
    else:
        combined = new_df

    combined.to_csv(RAW_CSV_PATH, index=False)
    print(f"Saved {len(combined):,} raw records to {RAW_CSV_PATH.name}", flush=True)

    build_json(RAW_CSV_PATH, JSON_PATH)

    print("\n" + "=" * 70, flush=True)
    print("DONE", flush=True)
    print("=" * 70, flush=True)


if __name__ == '__main__':
    main()
