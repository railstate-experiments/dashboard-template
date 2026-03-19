#!/usr/bin/env python3
"""
Ethanol Texas Corridor — Daily Data Pull
=========================================
Tracks ethanol tank cars (UN1170 and UN1987, reported separately) heading
southbound through three Texas sensors: Crabb, Gish, and League City.

Volume assumption: 714 barrels per tank car.

Includes unit-train gap-filling logic: if ≥70% of tank cars carry an ethanol
placard, empty tank cars are filled with the dominant placard.

Supports incremental daily updates:
  - First run: fetches last 180 days
  - Subsequent runs: fetches from (last_date - 2 days) to yesterday, deduplicates

Usage:
  python ethanol_texas_daily.py              # incremental update
  python ethanol_texas_daily.py --rebuild    # full re-fetch (180 days)
  python ethanol_texas_daily.py --days 30    # fetch last 30 days

Environment:
  RAILSTATE_API_KEY - API key (optional, falls back to hardcoded key)

Outputs:
  ../data/ethanol_texas_cars_raw.csv  - raw car-level data
  ../data/ethanol_texas_daily.csv     - daily aggregates with moving averages
  ../data/ethanol_texas.json          - JSON for dashboard (monthly + daily)
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
from zoneinfo import ZoneInfo

# ============================================================================
# CONFIGURATION
# ============================================================================

API_BASE_URL = "https://api.railstate.com"

HARDCODED_API_KEY = (
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.'
    'eyJhdWQiOiJhcGkucmFpbHN0YXRlLmNvbSIsImlzcyI6InJhaWxzdGF0ZS5jb20iLCJ1aWQiOiJ5RWE5aTFSbjljTmhtbEFEQkNYd3BFR3h1eEczIiwidGlkIjoiRGFuIERldm9lIFRlc3RpbmcifQ.'
    '8YQJPJE3X2xomrgUCXo41tx0Vh0gCRWRZ2JbQKYpAXE'
)

# Default fetch window (first run)
DEFAULT_INITIAL_DAYS = 180

# Overlap days for incremental updates
OVERLAP_DAYS = 2

# Ethanol UN placards
ETHANOL_PLACARDS = {'UN1170', 'UN1987'}

# Volume: flat 714 barrels per tank car
BARRELS_PER_CAR = 714

# Unit train detection threshold
UNIT_TRAIN_THRESHOLD = 0.70

# Block detection for manifest trains
MIN_BLOCK_SIZE = 6

# Timezone for daily aggregation (Texas = US Central)
LOCAL_TZ = ZoneInfo('America/Chicago')

# Monthly output window
MONTHLY_WINDOW_MONTHS = 6

# Daily output window
DAILY_WINDOW_DAYS = 90

# ============================================================================
# SENSOR CONFIGURATION
# ============================================================================

SENSOR_CONFIG = [
    {'name': 'Crabb, TX', 'direction': 'southbound'},
    {'name': 'Gish, TX', 'direction': 'southbound'},
    {'name': 'North Spring, TX', 'direction': 'eastbound'},
]

# ============================================================================
# OUTPUT PATHS
# ============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / 'data'
RAW_CSV_PATH = DATA_DIR / 'ethanol_texas_cars_raw.csv'
DAILY_CSV_PATH = DATA_DIR / 'ethanol_texas_daily.csv'
DASHBOARD_JSON_PATH = DATA_DIR / 'ethanol_texas.json'


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
            'response_size': 500,
        }

        while True:
            data, error = self._request(url, params)
            if error:
                if error == "Timeout":
                    print(f"      Timeout, continuing...")
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
# CAR PROCESSING
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


def classify_train(cars: List[dict]) -> Tuple[str, Optional[str]]:
    """Classify as unit or manifest. Returns (type, dominant_placard)."""
    if not cars:
        return 'manifest', None
    tank_cars = [c for c in cars if is_tank_car(c)]
    if not tank_cars:
        return 'manifest', None

    ethanol_cars = [c for c in tank_cars if is_ethanol_car(c)]

    # If non-ethanol hazmat placards present, it's manifest
    has_other_placards = any(
        get_placard(c) and get_placard(c) not in ETHANOL_PLACARDS
        for c in tank_cars
    )
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
    """Fill empty tank car placards on unit trains with the dominant placard."""
    filled_cars = []
    for car in cars:
        car_copy = car.copy()
        if is_tank_car(car) and not get_placard(car):
            car_copy['_filled_placard'] = dominant_placard
        filled_cars.append(car_copy)
    return filled_cars


def fill_manifest_train_placards(cars: List[dict]) -> List[dict]:
    """Fill empty placards in ethanol blocks within manifest trains."""
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
                has_other = any(
                    get_placard(filled_cars[j]) and get_placard(filled_cars[j]) not in ETHANOL_PLACARDS
                    for j in range(block_start, block_end)
                )
                if not has_other:
                    ethanol_count = sum(
                        1 for j in range(block_start, block_end) if is_ethanol_car(filled_cars[j])
                    )
                    if ethanol_count / block_size >= 0.5:
                        for j in range(block_start, block_end):
                            if is_tank_car(filled_cars[j]) and not get_placard(filled_cars[j]):
                                filled_cars[j]['_filled_placard'] = first_placard
        i = block_end
    return filled_cars


def process_train_cars(sighting: dict) -> Tuple[List[dict], str, Optional[str]]:
    """Process all cars in a sighting, applying gap-filling logic."""
    cars = sighting.get('cars', [])
    if not cars:
        return [], 'manifest', None
    train_type, dominant_placard = classify_train(cars)
    if train_type == 'unit':
        filled_cars = fill_unit_train_placards(cars, dominant_placard or 'UN1170')
    else:
        filled_cars = fill_manifest_train_placards(cars)
    sighting_id = sighting.get('sightingId')
    detection_time = sighting.get('detectionTimeUTC')
    for pos, car in enumerate(filled_cars):
        car['_train_type'] = train_type
        car['_dominant_placard'] = dominant_placard
        car['_sighting_id'] = sighting_id
        car['_detection_time'] = detection_time
        car['_position'] = pos
    return filled_cars, train_type, dominant_placard


# ============================================================================
# DATA EXTRACTION
# ============================================================================

def extract_ethanol_cars(sightings: List[dict], sensor_name: str) -> List[dict]:
    """Extract ethanol car records from sightings for a given sensor.

    Cars without a carId get a synthetic ID based on sighting ID and
    position in the train so they are not collapsed during deduplication.
    """
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

                # Assign synthetic ID for cars with no carId
                car_id = car.get('carId') or ''
                if not car_id:
                    car_id = f"NOID_{car.get('_sighting_id')}_{car.get('_position')}"

                ethanol_records.append({
                    'car_id': car_id,
                    'un_code': un_code,
                    'placard': placard,
                    'is_filled': is_filled_ethanol and not is_original_ethanol,
                    'train_type': car.get('_train_type', ''),
                    'detection_time': car.get('_detection_time'),
                    'sensor_name': sensor_name,
                    'sighting_id': car.get('_sighting_id'),
                    'barrels': BARRELS_PER_CAR,
                })
    return ethanol_records


# ============================================================================
# INCREMENTAL UPDATE LOGIC
# ============================================================================

def load_existing_raw(path: Path) -> Optional[pd.DataFrame]:
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
    yesterday = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    end_date = yesterday.replace(hour=23, minute=59, second=59)

    if args.rebuild or existing_df is None:
        start_date = end_date - timedelta(days=DEFAULT_INITIAL_DAYS)
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        mode = "FULL REBUILD" if args.rebuild else "INITIAL FETCH"
    elif args.days:
        start_date = yesterday - timedelta(days=args.days - 1)
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        mode = f"LAST {args.days} DAYS"
    else:
        existing_df['detection_time'] = pd.to_datetime(existing_df['detection_time'], format='ISO8601', utc=True)
        last_date = existing_df['detection_time'].max()
        start_date = (last_date - timedelta(days=OVERLAP_DAYS)).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )
        mode = "INCREMENTAL UPDATE"

    print(f"Mode: {mode}")
    print(f"Fetch window: {start_date.date()} to {end_date.date()}")
    return start_date, end_date


def merge_raw_data(existing_df: Optional[pd.DataFrame], new_df: pd.DataFrame) -> pd.DataFrame:
    if existing_df is None or existing_df.empty:
        return new_df
    if new_df.empty:
        return existing_df

    combined = pd.concat([existing_df, new_df], ignore_index=True)
    combined['detection_time'] = pd.to_datetime(combined['detection_time'], format='ISO8601', utc=True)

    before = len(combined)
    combined = combined.drop_duplicates(subset=['car_id', 'detection_time'], keep='last')
    after = len(combined)

    if before != after:
        print(f"Deduplicated: {before:,} -> {after:,} records ({before - after:,} duplicates removed)")

    combined = combined.sort_values('detection_time').reset_index(drop=True)
    return combined


# ============================================================================
# AGGREGATION
# ============================================================================

def calculate_daily_totals(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate daily totals with separate UN1170/UN1987 breakdowns in barrels."""
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df['detection_time'] = pd.to_datetime(df['detection_time'], format='ISO8601', utc=True)
    # Convert to US Central time before extracting date
    df['date'] = df['detection_time'].dt.tz_convert(LOCAL_TZ).dt.date

    # Deduplicate by car_id, date, and sensor — same car at different
    # sensors on the same day counts as separate trips
    df = df.drop_duplicates(subset=['car_id', 'date', 'sensor_name'], keep='first')
    df['date'] = pd.to_datetime(df['date'])

    # Compute per-UN-code flags
    df['is_1170'] = (df['un_code'] == '1170').astype(int)
    df['is_1987'] = (df['un_code'] == '1987').astype(int)
    df['barrels_1170'] = df['is_1170'] * BARRELS_PER_CAR
    df['barrels_1987'] = df['is_1987'] * BARRELS_PER_CAR

    daily = df.groupby('date').agg(
        total_cars=('car_id', 'count'),
        un1170_cars=('is_1170', 'sum'),
        un1987_cars=('is_1987', 'sum'),
        total_barrels=('barrels', 'sum'),
        un1170_barrels=('barrels_1170', 'sum'),
        un1987_barrels=('barrels_1987', 'sum'),
    ).reset_index()

    return daily


def add_moving_averages(df: pd.DataFrame) -> pd.DataFrame:
    """Add 7-day and 30-day moving averages. Fill missing dates with zeros."""
    if df.empty:
        return df

    df = df.copy()
    df = df.sort_values('date')

    # Fill missing dates with zeros for proper moving averages
    date_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')
    df = df.set_index('date').reindex(date_range, fill_value=0).reset_index()
    df = df.rename(columns={'index': 'date'})

    for col in ['total_barrels', 'un1170_barrels', 'un1987_barrels',
                'total_cars', 'un1170_cars', 'un1987_cars']:
        df[f'{col}_7d_ma'] = df[col].rolling(window=7, min_periods=1).mean().round(2)
        df[f'{col}_30d_ma'] = df[col].rolling(window=30, min_periods=1).mean().round(2)

    return df


def calculate_monthly_totals(df: pd.DataFrame, n_months: int) -> pd.DataFrame:
    """Aggregate daily data into monthly totals for the last n_months."""
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df['month'] = df['date'].dt.to_period('M')

    monthly = df.groupby('month').agg(
        total_cars=('total_cars', 'sum'),
        un1170_cars=('un1170_cars', 'sum'),
        un1987_cars=('un1987_cars', 'sum'),
        total_barrels=('total_barrels', 'sum'),
        un1170_barrels=('un1170_barrels', 'sum'),
        un1987_barrels=('un1987_barrels', 'sum'),
    ).reset_index()

    monthly = monthly.sort_values('month')
    monthly = monthly.tail(n_months)
    monthly['month'] = monthly['month'].astype(str)

    return monthly


# ============================================================================
# OUTPUT
# ============================================================================

def save_raw_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Saved {len(df):,} raw records to {path.name}")


def save_daily_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    save_df = df.copy()
    save_df['date'] = save_df['date'].dt.strftime('%Y-%m-%d')
    save_df.to_csv(path, index=False)
    print(f"Saved {len(save_df):,} daily records to {path.name}")


def save_dashboard_json(daily_df: pd.DataFrame, monthly_df: pd.DataFrame, path: Path):
    """Build and save the dashboard JSON with monthly and daily sections."""
    path.parent.mkdir(parents=True, exist_ok=True)

    # Monthly section
    monthly_rows = []
    if not monthly_df.empty:
        for _, row in monthly_df.iterrows():
            monthly_rows.append([
                row['month'],
                int(row['un1170_cars']),
                int(row['un1170_barrels']),
                int(row['un1987_cars']),
                int(row['un1987_barrels']),
                int(row['total_cars']),
                int(row['total_barrels']),
            ])

    # Daily section — last 90 days
    daily_section = {}
    if not daily_df.empty:
        recent = daily_df.tail(DAILY_WINDOW_DAYS).copy()
        daily_section = {
            'dates': [d.strftime('%Y-%m-%d') for d in recent['date']],
            'un1170_cars': [int(v) for v in recent['un1170_cars']],
            'un1170_barrels': [int(v) for v in recent['un1170_barrels']],
            'un1987_cars': [int(v) for v in recent['un1987_cars']],
            'un1987_barrels': [int(v) for v in recent['un1987_barrels']],
            'total_cars': [int(v) for v in recent['total_cars']],
            'total_barrels': [int(v) for v in recent['total_barrels']],
            'total_barrels_7d_ma': [round(v, 2) for v in recent['total_barrels_7d_ma']],
            'total_barrels_30d_ma': [round(v, 2) for v in recent['total_barrels_30d_ma']],
            'un1170_barrels_7d_ma': [round(v, 2) for v in recent['un1170_barrels_7d_ma']],
            'un1170_barrels_30d_ma': [round(v, 2) for v in recent['un1170_barrels_30d_ma']],
            'un1987_barrels_7d_ma': [round(v, 2) for v in recent['un1987_barrels_7d_ma']],
            'un1987_barrels_30d_ma': [round(v, 2) for v in recent['un1987_barrels_30d_ma']],
        }

    output = {
        'commodity': 'ethanol_texas',
        'display_name': 'Ethanol — Texas',
        'subtitle': 'Crabb · Gish · League City · Southbound',
        'unit': 'barrels',
        'barrels_per_car': BARRELS_PER_CAR,
        'last_updated': datetime.utcnow().strftime('%Y-%m-%d'),
        'monthly': {
            'columns': [
                'month',
                'un1170_cars', 'un1170_barrels',
                'un1987_cars', 'un1987_barrels',
                'total_cars', 'total_barrels',
            ],
            'rows': monthly_rows,
        },
        'daily': daily_section,
    }

    with open(path, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"Saved dashboard JSON to {path.name}")
    print(f"  Monthly: {len(monthly_rows)} months")
    print(f"  Daily: {len(daily_section.get('dates', []))} days")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Ethanol Texas Corridor — Daily Data Pull')
    parser.add_argument('--days', type=int, default=None,
                        help='Override fetch window to last N days')
    parser.add_argument('--rebuild', action='store_true',
                        help='Force full re-fetch (last 180 days)')
    args = parser.parse_args()

    api_key = os.environ.get('RAILSTATE_API_KEY', HARDCODED_API_KEY)

    print("=" * 70)
    print("ETHANOL TEXAS CORRIDOR — DAILY DATA PULL")
    print(f"Run Time: {datetime.utcnow().isoformat()}")
    print(f"Volume assumption: {BARRELS_PER_CAR} barrels per tank car")
    print("=" * 70)

    # Load existing raw data
    existing_df = None if args.rebuild else load_existing_raw(RAW_CSV_PATH)

    # Determine fetch window
    start_date, end_date = determine_fetch_window(existing_df, args)

    # Initialize fetcher
    fetcher = RailStateFetcher(api_key)
    fetcher.load_sensors()
    print(f"Loaded {len(fetcher._sensor_cache)} sensors")

    # ── Fetch data from each sensor ──
    print("\n" + "=" * 70)
    print("FETCHING DATA")
    print("=" * 70)

    all_data = []
    for sensor_cfg in SENSOR_CONFIG:
        sensor_name = sensor_cfg['name']
        direction = sensor_cfg['direction']
        sensor_id = fetcher.get_sensor_id(sensor_name)

        if not sensor_id:
            print(f"\n  Warning: Sensor not found: {sensor_name}")
            continue

        print(f"\n  {sensor_name} ({direction}):")
        sightings = fetcher.fetch_sightings(sensor_id, start_date, end_date, direction)

        if sightings:
            records = extract_ethanol_cars(sightings, sensor_name)
            all_data.extend(records)
            original = sum(1 for r in records if not r['is_filled'])
            filled = sum(1 for r in records if r['is_filled'])
            un1170 = sum(1 for r in records if r['un_code'] == '1170')
            un1987 = sum(1 for r in records if r['un_code'] == '1987')
            print(f"    {len(sightings):,} sightings -> {len(records):,} ethanol cars")
            print(f"    UN1170: {un1170:,}  UN1987: {un1987:,}  (detected: {original:,}, filled: {filled:,})")
        else:
            print(f"    No sightings found")

    # Merge with existing data
    new_df = pd.DataFrame(all_data) if all_data else pd.DataFrame()
    print(f"\nNew records fetched: {len(new_df):,}")

    combined_df = merge_raw_data(existing_df, new_df)

    if combined_df.empty:
        print("\nNo data found!")
        sys.exit(1)

    # ── Save raw data ──
    print("\n" + "=" * 70)
    print("SAVING RAW DATA")
    print("=" * 70)
    save_raw_csv(combined_df, RAW_CSV_PATH)

    # ── Calculate daily totals ──
    print("\n" + "=" * 70)
    print("CALCULATING DAILY TOTALS")
    print("=" * 70)

    daily_totals = calculate_daily_totals(combined_df)
    print(f"Daily totals calculated: {len(daily_totals)} days")

    daily_with_ma = add_moving_averages(daily_totals)
    print("Moving averages calculated (7-day, 30-day)")

    # ── Calculate monthly totals ──
    monthly_totals = calculate_monthly_totals(daily_with_ma, MONTHLY_WINDOW_MONTHS)
    print(f"Monthly totals calculated: {len(monthly_totals)} months")

    # ── Save outputs ──
    print("\n" + "=" * 70)
    print("SAVING OUTPUTS")
    print("=" * 70)

    save_daily_csv(daily_with_ma, DAILY_CSV_PATH)
    save_dashboard_json(daily_with_ma, monthly_totals, DASHBOARD_JSON_PATH)

    # ── Summary ──
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    if not daily_with_ma.empty:
        print(f"\nDate range: {daily_with_ma['date'].min().date()} to {daily_with_ma['date'].max().date()}")
        print(f"Total days: {len(daily_with_ma)}")

        # Monthly table
        if not monthly_totals.empty:
            print("\n" + "-" * 80)
            print(f"{'Month':<10} {'UN1170 Cars':>12} {'UN1170 Bbl':>12} {'UN1987 Cars':>12} {'UN1987 Bbl':>12} {'Total Bbl':>12}")
            print("-" * 80)
            for _, row in monthly_totals.iterrows():
                print(f"{row['month']:<10} {row['un1170_cars']:>12,.0f} {row['un1170_barrels']:>12,.0f} {row['un1987_cars']:>12,.0f} {row['un1987_barrels']:>12,.0f} {row['total_barrels']:>12,.0f}")
            print("-" * 80)

        # Last 14 days
        print("\n" + "-" * 100)
        print(f"{'Date':<12} {'1170':>6} {'1987':>6} {'Total':>6} {'Bbl':>10} {'7d MA':>10} {'30d MA':>10} {'1170 Bbl':>10} {'1987 Bbl':>10}")
        print("-" * 100)
        for _, row in daily_with_ma.tail(14).iterrows():
            print(f"{row['date'].strftime('%Y-%m-%d'):<12} "
                  f"{row['un1170_cars']:>6,.0f} {row['un1987_cars']:>6,.0f} {row['total_cars']:>6,.0f} "
                  f"{row['total_barrels']:>10,.0f} {row['total_barrels_7d_ma']:>10,.0f} {row['total_barrels_30d_ma']:>10,.0f} "
                  f"{row['un1170_barrels']:>10,.0f} {row['un1987_barrels']:>10,.0f}")
        print("-" * 100)

        # Averages
        last_row = daily_with_ma.iloc[-1]
        print(f"\nMost Recent Moving Averages:")
        print(f"  Total barrels/day (7d):  {last_row['total_barrels_7d_ma']:,.0f}")
        print(f"  Total barrels/day (30d): {last_row['total_barrels_30d_ma']:,.0f}")
        print(f"  UN1170 barrels/day (7d): {last_row['un1170_barrels_7d_ma']:,.0f}")
        print(f"  UN1987 barrels/day (7d): {last_row['un1987_barrels_7d_ma']:,.0f}")

    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)


if __name__ == '__main__':
    main()
