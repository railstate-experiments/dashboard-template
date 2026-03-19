#!/usr/bin/env python3
"""
Methanol Medicine Hat Daily Volumes
====================================
Pulls daily UN1230 (methanol) tank car counts from RailState API at 4 sensors
surrounding the Methanex facility in Medicine Hat, Alberta.

Sensors and directions (capturing outbound from Medicine Hat):
  - Waldeck, SK    — eastbound
  - Brooks, AB     — westbound
  - Coalhurst, AB  — westbound
  - Kevin, MT      — southbound

Supports incremental daily updates:
  - First run: fetches Jan 1 2024 to yesterday
  - Subsequent runs: fetches from (last_date - 2 days) to yesterday, deduplicates

Usage:
  python methanol_medicine_hat.py              # incremental update
  python methanol_medicine_hat.py --rebuild    # full re-fetch from Jan 1 2024
  python methanol_medicine_hat.py --days 30    # fetch last 30 days

Outputs:
  ../data/methanol_cars_raw.csv      - raw car-level data
  ../data/methanol_daily.json        - daily + monthly JSON for dashboard
  ../data/methanol_sensors.json      - sensor monthly volumes for map
"""

import argparse
import json
import os
import sys
import time
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

# Start date for full fetch
DEFAULT_START_DATE = datetime(2024, 1, 1)

# Methanol UN placard
METHANOL_PLACARD = 'UN1230'

# Volume conversion: 30,000 gallon cars, methanol density 0.791 kg/L, adjusted for headspace
TONNES_PER_CAR = 89

# Rate limiting
RATE_LIMIT_SLEEP = 2.0

# Output paths
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / 'data'
RAW_CSV_PATH = DATA_DIR / 'methanol_cars_raw.csv'
DAILY_JSON_PATH = DATA_DIR / 'methanol_daily.json'
SENSORS_JSON_PATH = DATA_DIR / 'methanol_sensors.json'

# ============================================================================
# SENSOR CONFIGURATION (4 sensors surrounding Medicine Hat)
# ============================================================================

SENSOR_CONFIG = [
    {'name': 'Waldeck, SK',    'direction': 'eastbound',  'lat': 50.359, 'lng': -107.596},
    {'name': 'Brooks, AB',     'direction': 'westbound',  'lat': 50.564, 'lng': -111.907},
    {'name': 'Coalhurst, AB',  'direction': 'westbound',  'lat': 49.733, 'lng': -112.924},
    {'name': 'Kevin, MT',      'direction': 'southbound', 'lat': 48.750, 'lng': -111.980},
]

# ============================================================================
# API CLIENT
# ============================================================================

class RailStateFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = API_BASE_URL
        self._sensor_cache = {}
        self._request_count = 0

    def _request(self, url: str, params: dict = None) -> Tuple[dict, Optional[str]]:
        if self._request_count > 0:
            time.sleep(RATE_LIMIT_SLEEP)
        self._request_count += 1
        try:
            headers = {'Authorization': f'Bearer {self.api_key}', 'Accept': 'application/json'}
            response = requests.get(url, params=params, headers=headers, timeout=120)
            if response.status_code != 200:
                return {}, f"HTTP {response.status_code}: {response.text[:200]}"
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
            print(f"  Warning: Could not load sensors: {error}")
            return {}
        for sensor in data.get('sensors', []):
            name, sid = sensor.get('name'), sensor.get('sensorId')
            if name and sid:
                self._sensor_cache[name] = sid
        print(f"  Loaded {len(self._sensor_cache)} sensors from API")
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
            'detection_time_from': start.strftime('%Y-%m-%dT00:00:00Z'),
            'detection_time_to': end.strftime('%Y-%m-%dT23:59:59Z'),
            'response_size': 500
        }
        page = 0
        while True:
            page += 1
            data, error = self._request(url, params)
            if error:
                print(f"      API error (page {page}): {error}")
                break
            sightings = data.get('sightings', [])
            if not sightings:
                break
            if direction:
                sightings = [s for s in sightings if s.get('direction', '').lower() == direction.lower()]
            all_sightings.extend(sightings)
            next_link = data.get('nextRequestLink')
            if not next_link:
                break
            url = next_link
            params = None
        return all_sightings


# ============================================================================
# EXTRACT UN1230 CARS
# ============================================================================

def extract_methanol_cars(sightings: List[dict], sensor_name: str) -> List[dict]:
    """Extract individual UN1230 car records from sightings."""
    records = []
    for sighting in sightings:
        det_time = sighting.get('detectionTimeUTC', '')
        direction = sighting.get('direction', '')
        for car in sighting.get('cars', []):
            hazmats = car.get('hazmats') or []
            is_methanol = any(h.get('placardType') == METHANOL_PLACARD for h in hazmats)
            if is_methanol:
                car_id = str(car.get('carId', '')).strip().upper()
                records.append({
                    'car_id': car_id,
                    'detection_time': det_time,
                    'date': det_time[:10] if len(det_time) >= 10 else '',
                    'direction': direction,
                    'sensor': sensor_name,
                })
    return records


# ============================================================================
# MAIN FETCH LOGIC
# ============================================================================

def determine_fetch_window(args) -> Tuple[datetime, datetime]:
    """Determine start/end dates for the fetch."""
    yesterday = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

    if args.rebuild:
        return DEFAULT_START_DATE, yesterday

    if args.days:
        start = yesterday - timedelta(days=args.days)
        return start, yesterday

    # Incremental: check existing raw CSV
    if RAW_CSV_PATH.exists():
        try:
            df = pd.read_csv(RAW_CSV_PATH)
            if 'date' in df.columns and len(df) > 0:
                last_date = pd.to_datetime(df['date']).max()
                start = last_date - timedelta(days=2)  # 2-day overlap for dedup
                print(f"  Incremental update from {start.date()} (last data: {last_date.date()})")
                return start, yesterday
        except Exception as e:
            print(f"  Warning reading existing CSV: {e}")

    # First run
    print(f"  First run — fetching from {DEFAULT_START_DATE.date()}")
    return DEFAULT_START_DATE, yesterday


def fetch_all_sensors(fetcher: RailStateFetcher, start: datetime, end: datetime) -> pd.DataFrame:
    """Fetch methanol cars from all 4 sensors."""
    all_records = []

    for sensor_cfg in SENSOR_CONFIG:
        name = sensor_cfg['name']
        direction = sensor_cfg['direction']
        print(f"\n  {name} ({direction}):")

        sid = fetcher.get_sensor_id(name)
        if sid is None:
            print(f"    SKIP: Sensor not found")
            continue

        sightings = fetcher.fetch_sightings(sid, start, end, direction=direction)
        print(f"    Sightings: {len(sightings)}")

        records = extract_methanol_cars(sightings, name)
        print(f"    UN1230 cars: {len(records)}")
        all_records.extend(records)

    if not all_records:
        return pd.DataFrame(columns=['car_id', 'detection_time', 'date', 'direction', 'sensor'])

    df = pd.DataFrame(all_records)
    return df


def merge_raw_data(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    """Merge existing and new raw data, deduplicating."""
    if existing_df is None or len(existing_df) == 0:
        combined = new_df
    else:
        combined = pd.concat([existing_df, new_df], ignore_index=True)

    # Deduplicate by car_id + detection_time
    before = len(combined)
    combined = combined.drop_duplicates(subset=['car_id', 'detection_time'], keep='last')
    after = len(combined)
    if before != after:
        print(f"  Deduplication: {before} -> {after} records ({before - after} removed)")

    combined = combined.sort_values('detection_time').reset_index(drop=True)
    return combined


def build_daily_json(df: pd.DataFrame) -> dict:
    """Build the daily JSON structure for the dashboard."""
    daily = df.groupby('date').agg(cars=('car_id', 'count')).reset_index()
    daily = daily.sort_values('date')

    days = {}
    for _, row in daily.iterrows():
        days[row['date']] = {
            'cars': int(row['cars']),
            'tonnes': int(row['cars']) * TONNES_PER_CAR,
        }

    return {
        'last_updated': datetime.utcnow().strftime('%Y-%m-%d'),
        'tonnes_per_car': TONNES_PER_CAR,
        'days': days,
    }


def build_sensors_json(df: pd.DataFrame) -> dict:
    """Build sensor monthly volumes for the map."""
    sensors = []
    for sensor_cfg in SENSOR_CONFIG:
        name = sensor_cfg['name']
        sensor_df = df[df['sensor'] == name]
        sensor_df = sensor_df.copy()
        sensor_df['month'] = sensor_df['date'].str[:7]
        monthly = sensor_df.groupby('month')['car_id'].count().to_dict()

        sensors.append({
            'name': name,
            'lat': sensor_cfg['lat'],
            'lng': sensor_cfg['lng'],
            'direction': sensor_cfg['direction'],
            'monthly': dict(sorted(monthly.items())),
        })

    return {
        'description': 'Methanol sensor volumes surrounding Methanex Medicine Hat facility',
        'last_updated': datetime.utcnow().strftime('%Y-%m-%d'),
        'sensors': sensors,
    }


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Methanol Medicine Hat Daily Volumes')
    parser.add_argument('--rebuild', action='store_true', help='Full re-fetch from Jan 1 2024')
    parser.add_argument('--days', type=int, help='Fetch last N days')
    args = parser.parse_args()

    print("=" * 70)
    print("METHANOL MEDICINE HAT — DAILY VOLUMES")
    print(f"Run time: {datetime.utcnow().isoformat()}Z")
    print("=" * 70)

    # Load API key
    api_key = os.environ.get('RAILSTATE_API_KEY', HARDCODED_API_KEY)
    fetcher = RailStateFetcher(api_key)

    # Determine fetch window
    start, end = determine_fetch_window(args)
    print(f"\n  Fetch window: {start.date()} to {end.date()}")

    # Fetch from API
    print("\n--- Fetching API Data ---")
    new_df = fetch_all_sensors(fetcher, start, end)
    print(f"\n  Total new records: {len(new_df)}")

    # Load existing raw data and merge
    existing_df = None
    if RAW_CSV_PATH.exists() and not args.rebuild:
        try:
            existing_df = pd.read_csv(RAW_CSV_PATH)
            print(f"  Existing raw records: {len(existing_df)}")
        except Exception as e:
            print(f"  Warning reading existing CSV: {e}")

    combined_df = merge_raw_data(existing_df, new_df)
    print(f"  Combined records: {len(combined_df)}")

    # Save raw CSV
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    combined_df.to_csv(RAW_CSV_PATH, index=False)
    print(f"  Saved raw CSV: {RAW_CSV_PATH.name}")

    # Build and save daily JSON
    daily_json = build_daily_json(combined_df)
    with open(DAILY_JSON_PATH, 'w') as f:
        json.dump(daily_json, f, indent=2)
    print(f"  Saved daily JSON: {DAILY_JSON_PATH.name}")

    # Build and save sensors JSON
    sensors_json = build_sensors_json(combined_df)
    with open(SENSORS_JSON_PATH, 'w') as f:
        json.dump(sensors_json, f, indent=2)
    print(f"  Saved sensors JSON: {SENSORS_JSON_PATH.name}")

    # Summary
    dates = sorted(daily_json['days'].keys())
    if dates:
        print(f"\n  Date range: {dates[0]} to {dates[-1]}")
        print(f"  Total days: {len(dates)}")
        total_cars = sum(d['cars'] for d in daily_json['days'].values())
        print(f"  Total cars: {total_cars:,}")
        print(f"  Total tonnes: {total_cars * TONNES_PER_CAR:,}")

        # Last 7 days
        print(f"\n  Last 7 days:")
        print(f"  {'Date':<12} {'Cars':>6} {'Tonnes':>8}")
        for d in dates[-7:]:
            v = daily_json['days'][d]
            print(f"  {d:<12} {v['cars']:>6} {v['tonnes']:>8,}")

    # Monthly summary
    monthly = defaultdict(int)
    for d, v in daily_json['days'].items():
        monthly[d[:7]] += v['cars']
    print(f"\n  Monthly totals:")
    print(f"  {'Month':<10} {'Cars':>6} {'Tonnes':>10}")
    for m in sorted(monthly)[-6:]:
        print(f"  {m:<10} {monthly[m]:>6} {monthly[m]*TONNES_PER_CAR:>10,}")

    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)


if __name__ == '__main__':
    main()
