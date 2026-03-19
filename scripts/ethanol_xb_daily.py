#!/usr/bin/env python3
"""
Ethanol Cross-Border Daily Volumes - Standalone Script
======================================================
Pulls daily totals: car count, gallons, and barrels across 9 border crossings.
Includes 7-day, 30-day, and 90-day moving averages.

Supports incremental daily updates:
  - First run: fetches Jan 1 2025 to yesterday
  - Subsequent runs: fetches from (last_date - 2 days) to yesterday, deduplicates

Usage:
  python ethanol_xb_daily.py              # incremental update
  python ethanol_xb_daily.py --rebuild    # full re-fetch from Jan 1 2025
  python ethanol_xb_daily.py --days 10    # fetch last 10 days

Environment:
  RAILSTATE_API_KEY - API key (optional, falls back to hardcoded key)

Outputs:
  ../data/ethanol_xb_cars_raw.csv   - raw car-level data
  ../data/ethanol_xb_daily.csv      - daily aggregates with moving averages
  ../data/ethanol_xb_daily.json     - JSON for dashboard

NOTES:
- Excludes ALL UN1987 from Windsor Region
- Removed Devlin, ON (not a border crossing)
- Added Ste Anne, MB WESTBOUND (captures US exports heading west into Canada)
"""

import argparse
import json
import os
import sys
import pandas as pd
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin

# ============================================================================
# CONFIGURATION
# ============================================================================

API_BASE_URL = "https://api.railstate.com"

# Fallback API key (overridden by RAILSTATE_API_KEY env var)
HARDCODED_API_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJhcGkucmFpbHN0YXRlLmNvbSIsImlzcyI6InJhaWxzdGF0ZS5jb20iLCJ1aWQiOiJ5RWE5aTFSbjljTmhtbEFEQkNYd3BFR3h1eEczIiwidGlkIjoiRGFuIERldm9lIFRlc3RpbmcifQ.8YQJPJE3X2xomrgUCXo41tx0Vh0gCRWRZ2JbQKYpAXE'

# Date configuration - January 1, 2025 start
DEFAULT_START_DATE = datetime(2025, 1, 1)

# Ethanol UN placards
ETHANOL_PLACARDS = {'UN1170', 'UN1987', 'UN3475'}

# Volume calculations
DEFAULT_GALLONS_PER_CAR = 30000
GALLONS_PER_BARREL = 42

# Unit train detection threshold
UNIT_TRAIN_THRESHOLD = 0.70
MIN_BLOCK_SIZE = 6

# Output paths (relative to this script's directory)
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / 'data'
RAW_CSV_PATH = DATA_DIR / 'ethanol_xb_cars_raw.csv'
DAILY_CSV_PATH = DATA_DIR / 'ethanol_xb_daily.csv'
DAILY_JSON_PATH = DATA_DIR / 'ethanol_xb_daily.json'

# ============================================================================
# CROSSING CONFIGURATION (9 border crossing points)
# ============================================================================

CROSSING_CONFIG = {
    'Blaine_WA': {
        'display_name': 'Blaine, WA',
        'sensors': [{'name': 'Blaine, WA', 'direction': 'northbound'}],
        'extra_days': 0,
        'exclude_un1987': False,
    },
    'Ste_Anne_MB': {
        'display_name': 'Ste Anne, MB',
        'sensors': [{'name': 'Ste Anne, MB', 'direction': 'westbound'}],  # Westbound = into Canada
        'extra_days': 0,
        'exclude_un1987': False,
    },
    'Mcara_SK': {
        'display_name': 'Mcara, SK',
        'sensors': [{'name': 'Mcara, SK', 'direction': 'northbound'}],
        'extra_days': 0,
        'exclude_un1987': False,
    },
    'Kevin_Coalhurst': {
        'display_name': 'Kevin, MT / Coalhurst, AB',
        'sensors': [
            {'name': 'Kevin, MT', 'direction': 'northbound'},
            {'name': 'Coalhurst, AB', 'direction': 'westbound'},
        ],
        'extra_days': 1,
        'exclude_un1987': False,
    },
    'Moyie_Springs_ID': {
        'display_name': 'Moyie Springs, ID',
        'sensors': [{'name': 'Moyie Springs, ID', 'direction': 'northbound'}],
        'extra_days': 0,
        'exclude_un1987': False,
    },
    'Letellier_MB': {
        'display_name': 'Letellier, MB',
        'sensors': [{'name': 'Letellier, MB', 'direction': 'northbound'}],
        'extra_days': 0,
        'exclude_un1987': False,
    },
    'Grande_Pointe_MB': {
        'display_name': 'Grande Pointe, MB',
        'sensors': [{'name': 'Grande Pointe, MB', 'direction': 'northbound'}],
        'extra_days': 0,
        'exclude_un1987': False,
    },
    'Port_Huron_London': {
        'display_name': 'Port Huron, MI / London West, ON',
        'sensors': [
            {'name': 'Port Huron, MI', 'direction': 'eastbound'},
            {'name': 'London West, ON', 'direction': 'eastbound'},
        ],
        'extra_days': 0,
        'exclude_un1987': False,
    },
    'Windsor_Region': {
        'display_name': 'Windsor / Komoka / Galt',
        'sensors': [
            {'name': 'Windsor TFR, ON', 'direction': 'eastbound'},
            {'name': 'Komoka, ON', 'direction': 'eastbound'},
            {'name': 'Galt, ON', 'direction': 'eastbound'},
        ],
        'extra_days': 0,
        'exclude_un1987': True,  # Exclude ALL UN1987 from this crossing
    },
}

# ============================================================================
# API FUNCTIONS
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
            print(f"  Warning: Could not load sensors: {error}")
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
            url, params = next_link, None

        return all_sightings


# ============================================================================
# CAR PROCESSING FUNCTIONS
# ============================================================================

def is_tank_car(car: dict) -> bool:
    return car.get('type', '') == 'Tank Car'

def get_placard(car: dict) -> Optional[str]:
    hazmats = car.get('hazmats') or []
    for hazmat in hazmats:
        placard = hazmat.get('placardType')
        if placard and placard != 'EMPTY':
            return placard
    return None

def is_ethanol_car(car: dict) -> bool:
    placard = get_placard(car)
    return placard in ETHANOL_PLACARDS if placard else False

def get_car_capacity(car: dict) -> float:
    try:
        params = car.get('equipmentParameters') or {}
        dims = params.get('dimensions') or {}
        capacity = dims.get('gallonageCapacity')
        if capacity and capacity > 0:
            return float(capacity)
    except:
        pass
    return DEFAULT_GALLONS_PER_CAR

def classify_train(cars: List[dict]) -> Tuple[str, Optional[str]]:
    if not cars:
        return 'manifest', None
    tank_cars = [c for c in cars if is_tank_car(c)]
    if not tank_cars:
        return 'manifest', None

    ethanol_cars = [c for c in tank_cars if is_ethanol_car(c)]
    has_other_placards = any(get_placard(c) and get_placard(c) not in ETHANOL_PLACARDS for c in tank_cars)

    if has_other_placards:
        return 'manifest', None

    if len(tank_cars) > 0:
        ethanol_ratio = len(ethanol_cars) / len(tank_cars)
        if ethanol_ratio >= UNIT_TRAIN_THRESHOLD:
            placard_counts = {}
            for car in ethanol_cars:
                p = get_placard(car)
                if p:
                    placard_counts[p] = placard_counts.get(p, 0) + 1
            dominant_placard = max(placard_counts, key=placard_counts.get) if placard_counts else 'UN1170'
            return 'unit', dominant_placard

    return 'manifest', None

def fill_unit_train_placards(cars: List[dict], dominant_placard: str) -> List[dict]:
    filled_cars = []
    for car in cars:
        car_copy = car.copy()
        if is_tank_car(car) and not get_placard(car):
            car_copy['_filled_placard'] = dominant_placard
        filled_cars.append(car_copy)
    return filled_cars

def fill_manifest_train_placards(cars: List[dict]) -> List[dict]:
    filled_cars = [c.copy() for c in cars]
    i = 0
    while i < len(filled_cars):
        if not is_tank_car(filled_cars[i]):
            i += 1
            continue
        block_start = i
        block_end = i
        while block_end < len(filled_cars) and is_tank_car(filled_cars[block_end]):
            block_end += 1
        block_size = block_end - block_start

        if block_size >= MIN_BLOCK_SIZE:
            first_placard = get_placard(filled_cars[block_start])
            last_placard = get_placard(filled_cars[block_end - 1])
            if first_placard in ETHANOL_PLACARDS and last_placard in ETHANOL_PLACARDS:
                has_other = any(get_placard(filled_cars[j]) and get_placard(filled_cars[j]) not in ETHANOL_PLACARDS
                               for j in range(block_start, block_end))
                if not has_other:
                    ethanol_count = sum(1 for j in range(block_start, block_end) if is_ethanol_car(filled_cars[j]))
                    if ethanol_count / block_size >= 0.5:
                        for j in range(block_start, block_end):
                            if is_tank_car(filled_cars[j]) and not get_placard(filled_cars[j]):
                                filled_cars[j]['_filled_placard'] = first_placard
        i = block_end
    return filled_cars

def process_train_cars(sighting: dict) -> Tuple[List[dict], str, Optional[str]]:
    cars = sighting.get('cars', [])
    if not cars:
        return [], 'manifest', None
    train_type, dominant_placard = classify_train(cars)
    if train_type == 'unit':
        filled_cars = fill_unit_train_placards(cars, dominant_placard or 'UN1170')
    else:
        filled_cars = fill_manifest_train_placards(cars)
    for car in filled_cars:
        car['_train_type'] = train_type
        car['_dominant_placard'] = dominant_placard
        car['_sighting_id'] = sighting.get('sightingId')
        car['_detection_time'] = sighting.get('detectionTimeUTC')
    return filled_cars, train_type, dominant_placard


# ============================================================================
# DATA EXTRACTION
# ============================================================================

def extract_ethanol_cars(sightings: List[dict], sensor_name: str, crossing: str, exclude_un1987: bool) -> List[dict]:
    ethanol_records = []
    for sighting in sightings:
        cars, train_type, dominant_placard = process_train_cars(sighting)
        for car in cars:
            original_placard = get_placard(car)
            filled_placard = car.get('_filled_placard')
            is_original_ethanol = original_placard in ETHANOL_PLACARDS if original_placard else False
            is_filled_ethanol = filled_placard in ETHANOL_PLACARDS if filled_placard else False

            if is_original_ethanol or is_filled_ethanol:
                placard = original_placard if is_original_ethanol else filled_placard
                un_code = placard[2:] if placard.startswith('UN') else placard

                # Skip UN1987 from Windsor Region
                if exclude_un1987 and un_code == '1987':
                    continue

                ethanol_records.append({
                    'car_id': car.get('carId', ''),
                    'un_code': un_code,
                    'placard': placard,
                    'detection_time': car.get('_detection_time'),
                    'sensor_name': sensor_name,
                    'crossing': crossing,
                    'capacity_gallons': get_car_capacity(car),
                })
    return ethanol_records

def fetch_crossing_data(fetcher: RailStateFetcher, crossing_key: str,
                        config: dict, start: datetime, end: datetime) -> pd.DataFrame:
    all_records = []
    extra_days = config.get('extra_days', 0)
    exclude_un1987 = config.get('exclude_un1987', False)
    extended_end = end + timedelta(days=extra_days)

    for sensor_cfg in config.get('sensors', []):
        sensor_name = sensor_cfg['name']
        direction = sensor_cfg.get('direction')
        sensor_id = fetcher.get_sensor_id(sensor_name)

        if not sensor_id:
            print(f"    Warning: Sensor not found: {sensor_name}")
            continue

        print(f"    Fetching {sensor_name} ({direction})...")
        sightings = fetcher.fetch_sightings(sensor_id, start, extended_end, direction)

        if sightings:
            records = extract_ethanol_cars(sightings, sensor_name, crossing_key, exclude_un1987)
            all_records.extend(records)
            print(f"      {len(sightings):,} sightings -> {len(records):,} ethanol cars")
        else:
            print(f"      No sightings found")

    return pd.DataFrame(all_records) if all_records else pd.DataFrame()


# ============================================================================
# AGGREGATION
# ============================================================================

def calculate_daily_totals(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate daily totals with car count, gallons, barrels."""
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df['detection_time'] = pd.to_datetime(df['detection_time'], format='ISO8601', utc=True)
    df['date'] = df['detection_time'].dt.date

    # Deduplicate by car_id and date
    df = df.drop_duplicates(subset=['car_id', 'date'], keep='first')

    df['date'] = pd.to_datetime(df['date'])

    daily = df.groupby(['date']).agg(
        car_count=('car_id', 'count'),
        total_gallons=('capacity_gallons', 'sum'),
    ).reset_index()

    # Calculate barrels
    daily['total_barrels'] = (daily['total_gallons'] / GALLONS_PER_BARREL).round(2)

    return daily


def add_moving_averages(df: pd.DataFrame) -> pd.DataFrame:
    """Add 7-day, 30-day, and 90-day moving averages."""
    if df.empty:
        return df

    df = df.copy()
    df = df.sort_values('date')

    # Fill in missing dates with zeros for proper moving averages
    date_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')
    df = df.set_index('date').reindex(date_range, fill_value=0).reset_index()
    df = df.rename(columns={'index': 'date'})

    # Calculate moving averages
    for col in ['car_count', 'total_gallons', 'total_barrels']:
        df[f'{col}_7d_ma'] = df[col].rolling(window=7, min_periods=1).mean().round(2)
        df[f'{col}_30d_ma'] = df[col].rolling(window=30, min_periods=1).mean().round(2)
        df[f'{col}_90d_ma'] = df[col].rolling(window=90, min_periods=1).mean().round(2)

    # Reorder columns for clarity
    df = df[[
        'date',
        'car_count', 'car_count_7d_ma', 'car_count_30d_ma', 'car_count_90d_ma',
        'total_gallons', 'total_gallons_7d_ma', 'total_gallons_30d_ma', 'total_gallons_90d_ma',
        'total_barrels', 'total_barrels_7d_ma', 'total_barrels_30d_ma', 'total_barrels_90d_ma',
    ]]

    return df


# ============================================================================
# INCREMENTAL UPDATE LOGIC
# ============================================================================

def load_existing_raw(path: Path) -> Optional[pd.DataFrame]:
    """Load existing raw car CSV if it exists."""
    if path.exists():
        try:
            df = pd.read_csv(path)
            if not df.empty:
                print(f"Loaded {len(df):,} existing raw records from {path.name}")
                return df
        except Exception as e:
            print(f"Warning: Could not load {path.name}: {e}")
    return None


def determine_fetch_window(existing_df: Optional[pd.DataFrame], args) -> Tuple[datetime, datetime]:
    """Determine the start/end dates for the API fetch."""
    yesterday = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    end_date = yesterday.replace(hour=23, minute=59, second=59)

    if args.rebuild or existing_df is None:
        # Full fetch from default start
        start_date = DEFAULT_START_DATE
        mode = "FULL REBUILD" if args.rebuild else "INITIAL FETCH"
    elif args.days:
        # Override: fetch last N days
        start_date = yesterday - timedelta(days=args.days - 1)
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        mode = f"LAST {args.days} DAYS"
    else:
        # Incremental: last_date - 2 days overlap for safety
        existing_df['detection_time'] = pd.to_datetime(existing_df['detection_time'], format='ISO8601', utc=True)
        last_date = existing_df['detection_time'].max()
        start_date = (last_date - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        # Convert to naive datetime for consistency
        start_date = start_date.replace(tzinfo=None)
        mode = "INCREMENTAL UPDATE"

    print(f"Mode: {mode}")
    print(f"Fetch window: {start_date.date()} to {end_date.date()}")

    return start_date, end_date


def merge_raw_data(existing_df: Optional[pd.DataFrame], new_df: pd.DataFrame) -> pd.DataFrame:
    """Merge new data into existing, deduplicating by car_id + detection_time."""
    if existing_df is None or existing_df.empty:
        return new_df
    if new_df.empty:
        return existing_df

    combined = pd.concat([existing_df, new_df], ignore_index=True)

    # Normalize detection_time for dedup
    combined['detection_time'] = pd.to_datetime(combined['detection_time'], format='ISO8601', utc=True)

    # Deduplicate: same car at same time is the same record
    before = len(combined)
    combined = combined.drop_duplicates(subset=['car_id', 'detection_time'], keep='last')
    after = len(combined)

    if before != after:
        print(f"Deduplicated: {before:,} -> {after:,} records ({before - after:,} duplicates removed)")

    # Sort by detection_time
    combined = combined.sort_values('detection_time').reset_index(drop=True)

    return combined


def save_raw_csv(df: pd.DataFrame, path: Path):
    """Save raw car data to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Saved {len(df):,} raw records to {path.name}")


def save_daily_csv(df: pd.DataFrame, path: Path):
    """Save daily aggregated data to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    save_df = df.copy()
    save_df['date'] = save_df['date'].dt.strftime('%Y-%m-%d')
    save_df.to_csv(path, index=False)
    print(f"Saved {len(save_df):,} daily records to {path.name}")


def save_daily_json(df: pd.DataFrame, path: Path):
    """Save daily data as JSON for dashboard consumption."""
    path.parent.mkdir(parents=True, exist_ok=True)

    if df.empty:
        output = {
            "description": "Ethanol cross-border daily volumes",
            "last_updated": datetime.utcnow().strftime('%Y-%m-%d'),
            "dates": [],
            "total_barrels": [],
            "car_count": [],
            "total_gallons": [],
        }
    else:
        output = {
            "description": "Ethanol cross-border daily volumes",
            "last_updated": datetime.utcnow().strftime('%Y-%m-%d'),
            "dates": [d.strftime('%Y-%m-%d') for d in df['date']],
            "total_barrels": [round(v, 2) for v in df['total_barrels']],
            "car_count": [int(v) for v in df['car_count']],
            "total_gallons": [round(v, 2) for v in df['total_gallons']],
        }

    with open(path, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"Saved JSON to {path.name} ({len(output['dates'])} dates)")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Ethanol Cross-Border Daily Volumes')
    parser.add_argument('--days', type=int, default=None,
                        help='Override fetch window to last N days')
    parser.add_argument('--rebuild', action='store_true',
                        help='Force full re-fetch from Jan 1 2025')
    args = parser.parse_args()

    # API key: env var takes precedence
    api_key = os.environ.get('RAILSTATE_API_KEY', HARDCODED_API_KEY)

    print("=" * 70)
    print("ETHANOL CROSS-BORDER DAILY VOLUMES")
    print(f"Run Time: {datetime.utcnow().isoformat()}")
    print("=" * 70)

    # Load existing raw data (if any)
    existing_df = None if args.rebuild else load_existing_raw(RAW_CSV_PATH)

    # Determine fetch window
    start_date, end_date = determine_fetch_window(existing_df, args)

    print(f"\nUN1987 excluded from Windsor Region")

    # Initialize fetcher
    fetcher = RailStateFetcher(api_key)
    fetcher.load_sensors()
    print(f"Loaded {len(fetcher._sensor_cache)} sensors")

    # Fetch data from API
    print("\n" + "=" * 70)
    print("FETCHING DATA")
    print("=" * 70)

    all_data = []
    for crossing_key, config in CROSSING_CONFIG.items():
        display_name = config.get('display_name', crossing_key)
        print(f"\n{display_name}:")
        df = fetch_crossing_data(fetcher, crossing_key, config, start_date, end_date)
        if not df.empty:
            all_data.append(df)
            print(f"  Total: {len(df):,} records")

    # Merge with existing data
    new_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    print(f"\nNew records fetched: {len(new_df):,}")

    combined_df = merge_raw_data(existing_df, new_df)

    if combined_df.empty:
        print("\nNo data found!")
        sys.exit(1)

    # Save raw data
    print("\n" + "=" * 70)
    print("SAVING RAW DATA")
    print("=" * 70)
    save_raw_csv(combined_df, RAW_CSV_PATH)

    # Calculate daily totals (always re-aggregate from full raw data)
    print("\n" + "=" * 70)
    print("CALCULATING DAILY TOTALS")
    print("=" * 70)

    ethanol_daily = calculate_daily_totals(combined_df)
    print(f"Daily totals calculated: {len(ethanol_daily)} days")

    # Add moving averages
    print("\nAdding moving averages (7-day, 30-day, 90-day)...")
    ethanol_daily = add_moving_averages(ethanol_daily)
    print("Moving averages calculated")

    # Save outputs
    print("\n" + "=" * 70)
    print("SAVING OUTPUTS")
    print("=" * 70)

    save_daily_csv(ethanol_daily, DAILY_CSV_PATH)
    save_daily_json(ethanol_daily, DAILY_JSON_PATH)

    # Print summary
    print("\n" + "=" * 70)
    print("DAILY SUMMARY")
    print("=" * 70)

    if not ethanol_daily.empty:
        print(f"\nDate range: {ethanol_daily['date'].min().date()} to {ethanol_daily['date'].max().date()}")
        print(f"Total days: {len(ethanol_daily)}")

        # Show last 14 days
        print("\n" + "-" * 90)
        print(f"{'Date':<12} {'Cars':>6} {'7d MA':>8} {'30d MA':>8} {'90d MA':>8} {'Barrels':>10} {'7d MA':>10} {'30d MA':>10}")
        print("-" * 90)

        for _, row in ethanol_daily.tail(14).iterrows():
            date_str = row['date'].strftime('%Y-%m-%d')
            print(f"{date_str:<12} {row['car_count']:>6,.0f} {row['car_count_7d_ma']:>8,.1f} {row['car_count_30d_ma']:>8,.1f} {row['car_count_90d_ma']:>8,.1f} {row['total_barrels']:>10,.0f} {row['total_barrels_7d_ma']:>10,.0f} {row['total_barrels_30d_ma']:>10,.0f}")

        print("-" * 90)

        # Summary statistics
        print("\n" + "-" * 70)
        print("SUMMARY STATISTICS (Full Period)")
        print("-" * 70)
        print(f"Total days: {len(ethanol_daily)}")
        print(f"Total cars: {ethanol_daily['car_count'].sum():,.0f}")
        print(f"Total barrels: {ethanol_daily['total_barrels'].sum():,.0f}")
        print(f"Average cars/day: {ethanol_daily['car_count'].mean():,.1f}")
        print(f"Average barrels/day: {ethanol_daily['total_barrels'].mean():,.0f}")

        print("\nMost Recent 30-Day Moving Averages:")
        last_row = ethanol_daily.iloc[-1]
        print(f"  Cars/day (30d MA): {last_row['car_count_30d_ma']:,.1f}")
        print(f"  Gallons/day (30d MA): {last_row['total_gallons_30d_ma']:,.0f}")
        print(f"  Barrels/day (30d MA): {last_row['total_barrels_30d_ma']:,.0f}")

        print("\nMost Recent 90-Day Moving Averages:")
        print(f"  Cars/day (90d MA): {last_row['car_count_90d_ma']:,.1f}")
        print(f"  Gallons/day (90d MA): {last_row['total_gallons_90d_ma']:,.0f}")
        print(f"  Barrels/day (90d MA): {last_row['total_barrels_90d_ma']:,.0f}")

    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)


if __name__ == '__main__':
    main()
