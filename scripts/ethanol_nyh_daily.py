#!/usr/bin/env python3
"""
RailState Ethanol NY Harbor Pipeline - Daily Data Pull
======================================================
Standalone script for daily incremental updates of ethanol pipeline data.

Usage:
    python ethanol_nyh_daily.py              # Daily incremental update
    python ethanol_nyh_daily.py --days 60    # Fetch last 60 days
    python ethanol_nyh_daily.py --rebuild    # Force full re-aggregation from raw data

Output files (relative to script directory):
    ../data/ethanol_cars_raw.csv          - Persistent raw car-level data
    ../data/ethanol_daily_by_region.csv   - Daily totals by sub-region
    ../data/ethanol_daily_combined.csv    - Daily totals combined across regions
    ../data/ethanol_nyh_pipeline.json     - Pipeline stage JSON for the dashboard
"""

import argparse
import json
import os
import sys
import pandas as pd
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin

# ============================================================================
# CONFIGURATION
# ============================================================================

API_BASE_URL = "https://api.railstate.com"

HARDCODED_API_KEY = (
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.'
    'eyJhdWQiOiJhcGkucmFpbHN0YXRlLmNvbSIsImlzcyI6InJhaWxzdGF0ZS5jb20iLCJ1aWQiOiJ5RWE5aTFSbjljTmhtbEFEQkNYd3BFR3h1eEczIiwidGlkIjoiRGFuIERldm9lIFRlc3RpbmcifQ.'
    '8YQJPJE3X2xomrgUCXo41tx0Vh0gCRWRZ2JbQKYpAXE'
)

# Fetch window: days of overlap to catch any late-arriving data
OVERLAP_DAYS = 2

# Default days to fetch on first run (no existing CSV)
DEFAULT_INITIAL_DAYS = 30

# Ethanol UN placards
ETHANOL_PLACARDS = {'UN1170', 'UN1987'}

# Volume calculations
DEFAULT_GALLONS_PER_CAR = 30000
GALLONS_PER_BARREL = 42

# Unit train detection threshold
UNIT_TRAIN_THRESHOLD = 0.70

# Block detection settings
MIN_BLOCK_SIZE = 6

# ============================================================================
# REGION CONFIGURATION
# ============================================================================

REGION_CONFIG = {
    'Three_Days_Out_NS': {
        'display_name': 'Three Days Out NS',
        'sensors': [
            {'name': 'Mishawaka, IN', 'direction': 'eastbound'},
            {'name': 'Osceola, IN', 'direction': 'eastbound'},
        ]
    },
    'Three_Days_Out_CSX': {
        'display_name': 'Three Days Out CSX',
        'sensors': [
            {'name': 'Cromwell, IN', 'direction': 'eastbound'},
            {'name': 'Kimmell, IN', 'direction': 'eastbound'},
        ]
    },
    'Two_Days_Out_NS': {
        'display_name': 'Two Days Out NS',
        'sensors': [
            {'name': 'East Palestine N, OH', 'direction': 'eastbound'},
            {'name': 'East Palestine S, OH', 'direction': 'eastbound'},
            {'name': 'New Galilee S, PA', 'direction': 'eastbound'},
        ]
    },
    'Two_Days_Out_CSX': {
        'display_name': 'Two Days Out CSX',
        'sensors': [
            {'name': 'Ravena, NY', 'direction': 'southbound'},
            {'name': 'Coxsackie, NY', 'direction': 'southbound'},
        ]
    },
    'One_Day_Out_NS': {
        'display_name': 'One Day Out NS',
        'sensors': [
            {'name': 'Hummelstown, PA', 'direction': 'eastbound'},
        ]
    },
    'Arrived': {
        'display_name': 'Arrived',
        'sensors': [
            {'name': 'Ceramics, NJ', 'direction': 'eastbound'},
            {'name': 'Carteret W, NJ', 'direction': 'southbound'},
        ]
    },
}

# Pipeline stage mapping: stage_name -> list of sub-regions to sum
STAGE_MAPPING = {
    '3_days_out': ['Three_Days_Out_NS', 'Three_Days_Out_CSX'],
    '2_days_out': ['Two_Days_Out_NS', 'Two_Days_Out_CSX'],
    '1_day_out': ['One_Day_Out_NS'],
    'arrived': ['Arrived'],
}

# ============================================================================
# PATH CONFIGURATION (relative to script directory)
# ============================================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, '..', 'data')

RAW_CSV_PATH = os.path.join(DATA_DIR, 'ethanol_cars_raw.csv')
BY_REGION_CSV_PATH = os.path.join(DATA_DIR, 'ethanol_daily_by_region.csv')
COMBINED_CSV_PATH = os.path.join(DATA_DIR, 'ethanol_daily_combined.csv')
PIPELINE_JSON_PATH = os.path.join(DATA_DIR, 'ethanol_nyh_pipeline.json')


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

        page = 0
        while True:
            data, error = self._request(url, params)
            if error:
                if error == "Timeout":
                    print(f"      Timeout on page {page}, continuing...")
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
            page += 1

        return all_sightings


# ============================================================================
# CAR CLASSIFICATION AND PLACARD FILLING
# ============================================================================

def is_tank_car(car: dict) -> bool:
    car_type = car.get('type', '')
    return car_type == 'Tank Car'


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


def classify_train(cars: List[dict]) -> str:
    if not cars:
        return 'manifest'
    tank_cars = [c for c in cars if is_tank_car(c)]
    if not tank_cars:
        return 'manifest'
    ethanol_cars = [c for c in tank_cars if is_ethanol_car(c)]
    if len(tank_cars) > 0:
        ethanol_ratio = len(ethanol_cars) / len(tank_cars)
        if ethanol_ratio >= UNIT_TRAIN_THRESHOLD:
            return 'unit'
    return 'manifest'


def fill_unit_train_placards(cars: List[dict]) -> List[dict]:
    filled_cars = []
    for car in cars:
        car_copy = car.copy()
        if is_tank_car(car) and not get_placard(car):
            car_copy['_filled_placard'] = 'UN1170'
            car_copy['_fill_reason'] = 'unit_train'
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
            first_car = filled_cars[block_start]
            last_car = filled_cars[block_end - 1]
            first_is_ethanol = is_ethanol_car(first_car) or first_car.get('_filled_placard') in ETHANOL_PLACARDS
            last_is_ethanol = is_ethanol_car(last_car) or last_car.get('_filled_placard') in ETHANOL_PLACARDS
            if first_is_ethanol and last_is_ethanol:
                ethanol_count = sum(1 for j in range(block_start, block_end)
                                   if is_ethanol_car(filled_cars[j]) or
                                   filled_cars[j].get('_filled_placard') in ETHANOL_PLACARDS)
                if ethanol_count / block_size >= 0.5:
                    for j in range(block_start, block_end):
                        car = filled_cars[j]
                        if is_tank_car(car) and not get_placard(car) and not car.get('_filled_placard'):
                            car['_filled_placard'] = 'UN1170'
                            car['_fill_reason'] = 'block_fill'
        i = block_end
    return filled_cars


def process_train_cars(sighting: dict) -> List[dict]:
    cars = sighting.get('cars', [])
    if not cars:
        return []
    train_type = classify_train(cars)
    if train_type == 'unit':
        filled_cars = fill_unit_train_placards(cars)
    else:
        filled_cars = fill_manifest_train_placards(cars)
    for car in filled_cars:
        car['_train_type'] = train_type
        car['_sighting_id'] = sighting.get('sightingId')
        car['_detection_time'] = sighting.get('detectionTimeUTC')
        car['_direction'] = sighting.get('direction')
    return filled_cars


# ============================================================================
# DATA EXTRACTION
# ============================================================================

def extract_ethanol_cars(sightings: List[dict], sensor_name: str, region: str) -> List[dict]:
    ethanol_records = []
    for sighting in sightings:
        cars = process_train_cars(sighting)
        for car in cars:
            original_placard = get_placard(car)
            filled_placard = car.get('_filled_placard')
            is_original_ethanol = original_placard in ETHANOL_PLACARDS if original_placard else False
            is_filled_ethanol = filled_placard in ETHANOL_PLACARDS if filled_placard else False
            if is_original_ethanol or is_filled_ethanol:
                placard = original_placard if is_original_ethanol else filled_placard
                un_code = placard[2:] if placard.startswith('UN') else placard
                ethanol_records.append({
                    'car_id': car.get('carId', ''),
                    'un_code': un_code,
                    'placard': placard,
                    'is_filled': is_filled_ethanol and not is_original_ethanol,
                    'fill_reason': car.get('_fill_reason', ''),
                    'train_type': car.get('_train_type', ''),
                    'detection_time': car.get('_detection_time'),
                    'direction': car.get('_direction'),
                    'sensor_name': sensor_name,
                    'region': region,
                    'sighting_id': car.get('_sighting_id'),
                    'capacity_gallons': get_car_capacity(car),
                })
    return ethanol_records


def fetch_region_data(fetcher: RailStateFetcher, region_key: str,
                      config: dict, start: datetime, end: datetime) -> pd.DataFrame:
    all_records = []
    display_name = config.get('display_name', region_key)
    for sensor_cfg in config.get('sensors', []):
        sensor_name = sensor_cfg['name']
        direction = sensor_cfg.get('direction')
        sensor_id = fetcher.get_sensor_id(sensor_name)
        if not sensor_id:
            print(f"    Warning: Sensor not found: {sensor_name}")
            continue
        print(f"    Fetching {sensor_name} ({direction})...")
        sightings = fetcher.fetch_sightings(sensor_id, start, end, direction)
        if not sightings:
            print(f"      No sightings found")
            continue
        records = extract_ethanol_cars(sightings, sensor_name, region_key)
        all_records.extend(records)
        original_count = sum(1 for r in records if not r['is_filled'])
        filled_count = sum(1 for r in records if r['is_filled'])
        print(f"      {len(sightings):,} sightings -> {len(records):,} ethanol cars ({original_count:,} detected, {filled_count:,} filled)")
    return pd.DataFrame(all_records) if all_records else pd.DataFrame()


# ============================================================================
# DEDUPLICATION AND AGGREGATION
# ============================================================================

def deduplicate_by_region(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df['detection_time'] = pd.to_datetime(df['detection_time'], format='ISO8601', utc=True)
    df['date'] = pd.to_datetime(df['detection_time'].dt.date)
    df = df.sort_values('detection_time')
    deduped = df.drop_duplicates(subset=['car_id', 'region', 'date'], keep='first')
    return deduped


def calculate_region_daily_totals(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df['un_code'] = df['un_code'].astype(str).str.replace('UN', '', regex=False)

    # Calculate gallons by UN code
    df['un1170_gallons'] = df.apply(lambda x: x['capacity_gallons'] if x['un_code'] == '1170' else 0, axis=1)
    df['un1987_gallons'] = df.apply(lambda x: x['capacity_gallons'] if x['un_code'] == '1987' else 0, axis=1)

    daily = df.groupby(['date', 'region']).agg(
        car_count=('car_id', 'count'),
        filled_count=('is_filled', 'sum'),
        un1170_count=('un_code', lambda x: (x == '1170').sum()),
        un1987_count=('un_code', lambda x: (x == '1987').sum()),
        total_gallons=('capacity_gallons', 'sum'),
        un1170_gallons=('un1170_gallons', 'sum'),
        un1987_gallons=('un1987_gallons', 'sum'),
    ).reset_index()

    # Calculate barrels (total and by UN code)
    daily['total_barrels'] = (daily['total_gallons'] / GALLONS_PER_BARREL).round(2)
    daily['un1170_barrels'] = (daily['un1170_gallons'] / GALLONS_PER_BARREL).round(2)
    daily['un1987_barrels'] = (daily['un1987_gallons'] / GALLONS_PER_BARREL).round(2)

    return daily


def calculate_combined_daily_totals(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df['un_code'] = df['un_code'].astype(str).str.replace('UN', '', regex=False)

    # Calculate gallons by UN code
    df['un1170_gallons'] = df.apply(lambda x: x['capacity_gallons'] if x['un_code'] == '1170' else 0, axis=1)
    df['un1987_gallons'] = df.apply(lambda x: x['capacity_gallons'] if x['un_code'] == '1987' else 0, axis=1)

    daily = df.groupby(['date']).agg(
        car_count=('car_id', 'count'),
        filled_count=('is_filled', 'sum'),
        un1170_count=('un_code', lambda x: (x == '1170').sum()),
        un1987_count=('un_code', lambda x: (x == '1987').sum()),
        total_gallons=('capacity_gallons', 'sum'),
        un1170_gallons=('un1170_gallons', 'sum'),
        un1987_gallons=('un1987_gallons', 'sum'),
    ).reset_index()

    # Calculate barrels (total and by UN code)
    daily['total_barrels'] = (daily['total_gallons'] / GALLONS_PER_BARREL).round(2)
    daily['un1170_barrels'] = (daily['un1170_gallons'] / GALLONS_PER_BARREL).round(2)
    daily['un1987_barrels'] = (daily['un1987_gallons'] / GALLONS_PER_BARREL).round(2)

    return daily


def add_rolling_averages(df: pd.DataFrame, value_columns: List[str]) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df = df.sort_values('date')
    for col in value_columns:
        if col in df.columns:
            df[f'{col}_7d_avg'] = df[col].rolling(window=7, min_periods=1).mean().round(2)
            df[f'{col}_14d_avg'] = df[col].rolling(window=14, min_periods=1).mean().round(2)
            df[f'{col}_30d_avg'] = df[col].rolling(window=30, min_periods=1).mean().round(2)
    return df


def add_rolling_averages_by_group(df: pd.DataFrame, group_col: str,
                                   value_columns: List[str]) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df = df.sort_values(['date', group_col])
    result_dfs = []
    for group_val in df[group_col].unique():
        group_df = df[df[group_col] == group_val].copy()
        group_df = group_df.sort_values('date')
        for col in value_columns:
            if col in group_df.columns:
                group_df[f'{col}_7d_avg'] = group_df[col].rolling(window=7, min_periods=1).mean().round(2)
                group_df[f'{col}_14d_avg'] = group_df[col].rolling(window=14, min_periods=1).mean().round(2)
                group_df[f'{col}_30d_avg'] = group_df[col].rolling(window=30, min_periods=1).mean().round(2)
        result_dfs.append(group_df)
    return pd.concat(result_dfs, ignore_index=True)


# ============================================================================
# PIPELINE JSON GENERATION
# ============================================================================

def build_pipeline_json(daily_by_region: pd.DataFrame) -> dict:
    """
    Aggregate the 6 sub-regions into 4 pipeline stages and produce the
    dashboard JSON structure.
    """
    if daily_by_region.empty:
        return {
            "description": "Ethanol NY Harbor pipeline daily data by stage",
            "last_updated": datetime.utcnow().strftime('%Y-%m-%d'),
            "stages": {},
        }

    df = daily_by_region.copy()
    df['date'] = pd.to_datetime(df['date'])

    stages = {}
    for stage_name, sub_regions in STAGE_MAPPING.items():
        stage_df = df[df['region'].isin(sub_regions)].copy()
        if stage_df.empty:
            stages[stage_name] = {
                'dates': [],
                'car_count': [],
                'un1170_count': [],
                'total_gallons': [],
                'total_barrels': [],
                'un1170_gallons': [],
                'un1170_barrels': [],
            }
            continue

        # Sum across sub-regions per date
        agg = stage_df.groupby('date').agg(
            car_count=('car_count', 'sum'),
            un1170_count=('un1170_count', 'sum'),
            total_gallons=('total_gallons', 'sum'),
            un1170_gallons=('un1170_gallons', 'sum'),
        ).reset_index()

        agg = agg.sort_values('date').reset_index(drop=True)
        agg['total_barrels'] = (agg['total_gallons'] / GALLONS_PER_BARREL).round(2)
        agg['un1170_barrels'] = (agg['un1170_gallons'] / GALLONS_PER_BARREL).round(2)

        stages[stage_name] = {
            'dates': agg['date'].dt.strftime('%Y-%m-%d').tolist(),
            'car_count': agg['car_count'].tolist(),
            'un1170_count': agg['un1170_count'].tolist(),
            'total_gallons': agg['total_gallons'].tolist(),
            'total_barrels': agg['total_barrels'].tolist(),
            'un1170_gallons': agg['un1170_gallons'].tolist(),
            'un1170_barrels': agg['un1170_barrels'].tolist(),
        }

    return {
        "description": "Ethanol NY Harbor pipeline daily data by stage",
        "last_updated": datetime.utcnow().strftime('%Y-%m-%d'),
        "stages": stages,
    }


# ============================================================================
# MAIN
# ============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description='RailState Ethanol NY Harbor Pipeline - Daily Data Pull'
    )
    parser.add_argument(
        '--days', type=int, default=None,
        help='Override fetch window: number of days to fetch (from today minus N days to today)'
    )
    parser.add_argument(
        '--rebuild', action='store_true',
        help='Force full re-aggregation from raw data (does not re-fetch from API)'
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print("=" * 70)
    print("RAILSTATE ETHANOL NY HARBOR PIPELINE - DAILY DATA PULL")
    print(f"Run Time: {datetime.utcnow().isoformat()}")
    print("=" * 70)

    # Resolve API key
    api_key = os.environ.get('RAILSTATE_API_KEY', HARDCODED_API_KEY)
    if not api_key:
        print("ERROR: No API key found. Set RAILSTATE_API_KEY environment variable.")
        sys.exit(1)

    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    # Initialize fetcher
    fetcher = RailStateFetcher(api_key)
    fetcher.load_sensors()
    print(f"Loaded {len(fetcher._sensor_cache)} sensors")

    # Determine date range
    today = datetime.utcnow().replace(hour=23, minute=59, second=59)
    existing_df = pd.DataFrame()
    skip_fetch = False

    if args.days is not None:
        # Explicit days override
        start_date = today - timedelta(days=args.days)
        print(f"\n*** EXPLICIT FETCH: last {args.days} days ***")
        print(f"Fetching from: {start_date.date()} to {today.date()}")
        # Still load existing data for merging
        if os.path.exists(RAW_CSV_PATH):
            existing_df = pd.read_csv(RAW_CSV_PATH)
            existing_df['detection_time'] = pd.to_datetime(existing_df['detection_time'], format='ISO8601', utc=True)
            existing_df['date'] = pd.to_datetime(existing_df['date'])
            print(f"Loaded {len(existing_df):,} existing records for merging")
    elif args.rebuild:
        # Rebuild mode: just re-aggregate, no fetch
        print(f"\n*** REBUILD MODE ***")
        if os.path.exists(RAW_CSV_PATH):
            existing_df = pd.read_csv(RAW_CSV_PATH)
            existing_df['detection_time'] = pd.to_datetime(existing_df['detection_time'], format='ISO8601', utc=True)
            existing_df['date'] = pd.to_datetime(existing_df['date'])
            print(f"Loaded {len(existing_df):,} existing records")
            skip_fetch = True
            start_date = today  # Not used when skip_fetch=True
        else:
            print("No existing raw data found. Cannot rebuild. Fetching last 30 days instead.")
            start_date = today - timedelta(days=DEFAULT_INITIAL_DAYS)
    else:
        # Daily update mode
        print(f"\n*** DAILY UPDATE MODE ***")
        if os.path.exists(RAW_CSV_PATH):
            existing_df = pd.read_csv(RAW_CSV_PATH)
            existing_df['detection_time'] = pd.to_datetime(existing_df['detection_time'], format='ISO8601', utc=True)
            existing_df['date'] = pd.to_datetime(existing_df['date'])
            print(f"Loaded {len(existing_df):,} existing records")

            last_date = existing_df['date'].max().date() if hasattr(existing_df['date'].max(), 'date') else existing_df['date'].max()
            start_date = datetime.combine(last_date, datetime.min.time()) - timedelta(days=OVERLAP_DAYS)
            print(f"Last data date: {last_date}")
            print(f"Fetching from: {start_date.date()} to {today.date()}")
        else:
            print(f"No existing data found - fetching last {DEFAULT_INITIAL_DAYS} days")
            start_date = today - timedelta(days=DEFAULT_INITIAL_DAYS)

    # ========================================================================
    # FETCH NEW DATA
    # ========================================================================

    new_df = pd.DataFrame()

    if not skip_fetch:
        print("\n" + "=" * 70)
        print("FETCHING NEW DATA BY REGION")
        print("=" * 70)

        all_new_data = []
        for region_key, config in REGION_CONFIG.items():
            display_name = config.get('display_name', region_key)
            print(f"\n{display_name}:")
            df = fetch_region_data(fetcher, region_key, config, start_date, today)
            if not df.empty:
                all_new_data.append(df)
                print(f"  Total: {len(df):,} ethanol car records")
            else:
                print(f"  No new data")

        if all_new_data:
            new_df = pd.concat(all_new_data, ignore_index=True)
            print(f"\nNew records fetched: {len(new_df):,}")
        else:
            print("\nNo new data fetched")

    # ========================================================================
    # COMBINE AND DEDUPLICATE
    # ========================================================================

    if not existing_df.empty and not new_df.empty:
        print("\nCombining with existing data...")
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    elif not existing_df.empty:
        combined_df = existing_df
    elif not new_df.empty:
        combined_df = new_df
    else:
        combined_df = pd.DataFrame()

    if not combined_df.empty:
        print("Deduplicating...")
        ethanol_raw_df = deduplicate_by_region(combined_df)
        print(f"Total unique car-days: {len(ethanol_raw_df):,}")
    else:
        ethanol_raw_df = pd.DataFrame()

    # ========================================================================
    # CALCULATE DAILY TOTALS
    # ========================================================================

    print("\n" + "=" * 70)
    print("CALCULATING DAILY TOTALS")
    print("=" * 70)

    if args.rebuild or skip_fetch:
        # Full recalculation from raw data
        print("Recalculating all aggregations from raw data...")
        ethanol_daily_by_region = calculate_region_daily_totals(ethanol_raw_df)
        ethanol_daily_combined = calculate_combined_daily_totals(ethanol_raw_df)
        print(f"Rebuilt aggregations: {len(ethanol_daily_by_region)} region rows, {len(ethanol_daily_combined)} combined rows")
    else:
        # Incremental: load existing aggregated data
        hist_daily_by_region = pd.DataFrame()
        hist_daily_combined = pd.DataFrame()

        try:
            if os.path.exists(BY_REGION_CSV_PATH):
                hist_daily_by_region = pd.read_csv(BY_REGION_CSV_PATH)
                hist_daily_by_region['date'] = pd.to_datetime(hist_daily_by_region['date'])
            if os.path.exists(COMBINED_CSV_PATH):
                hist_daily_combined = pd.read_csv(COMBINED_CSV_PATH)
                hist_daily_combined['date'] = pd.to_datetime(hist_daily_combined['date'])
            if not hist_daily_by_region.empty:
                print(f"Loaded historical aggregations: {len(hist_daily_by_region)} region rows, {len(hist_daily_combined)} combined rows")
        except Exception as e:
            print(f"Could not load historical aggregations ({e}) - calculating from scratch")
            hist_daily_by_region = pd.DataFrame()
            hist_daily_combined = pd.DataFrame()

        if hist_daily_by_region.empty and hist_daily_combined.empty:
            print("No historical aggregations found - calculating from scratch")

        # Calculate aggregations for NEW data only
        if not new_df.empty:
            new_df_deduped = deduplicate_by_region(new_df)
            new_daily_by_region = calculate_region_daily_totals(new_df_deduped)
            new_daily_combined = calculate_combined_daily_totals(new_df_deduped)
            print(f"New aggregations: {len(new_daily_by_region)} region rows, {len(new_daily_combined)} combined rows")
        else:
            new_daily_by_region = pd.DataFrame()
            new_daily_combined = pd.DataFrame()

        # Combine historical + new, keeping new data for overlapping dates
        if not hist_daily_by_region.empty and not new_daily_by_region.empty:
            new_dates = set(new_daily_by_region['date'].dt.date)
            hist_daily_by_region = hist_daily_by_region[~hist_daily_by_region['date'].dt.date.isin(new_dates)]
            ethanol_daily_by_region = pd.concat([hist_daily_by_region, new_daily_by_region], ignore_index=True)
        elif not hist_daily_by_region.empty:
            ethanol_daily_by_region = hist_daily_by_region
        else:
            ethanol_daily_by_region = new_daily_by_region

        if not hist_daily_combined.empty and not new_daily_combined.empty:
            new_dates = set(new_daily_combined['date'].dt.date)
            hist_daily_combined = hist_daily_combined[~hist_daily_combined['date'].dt.date.isin(new_dates)]
            ethanol_daily_combined = pd.concat([hist_daily_combined, new_daily_combined], ignore_index=True)
        elif not hist_daily_combined.empty:
            ethanol_daily_combined = hist_daily_combined
        else:
            ethanol_daily_combined = new_daily_combined

    # Sort by date
    if not ethanol_daily_by_region.empty:
        ethanol_daily_by_region = ethanol_daily_by_region.sort_values(['date', 'region']).reset_index(drop=True)
    if not ethanol_daily_combined.empty:
        ethanol_daily_combined = ethanol_daily_combined.sort_values('date').reset_index(drop=True)

    # Recalculate rolling averages on the full combined data
    rolling_cols = ['car_count', 'un1170_count', 'un1987_count',
                    'total_gallons', 'un1170_gallons', 'un1987_gallons',
                    'total_barrels', 'un1170_barrels', 'un1987_barrels']

    # Remove old rolling average columns before recalculating
    if not ethanol_daily_by_region.empty:
        for col in list(ethanol_daily_by_region.columns):
            if '_avg' in col:
                ethanol_daily_by_region = ethanol_daily_by_region.drop(columns=[col])
        ethanol_daily_by_region = add_rolling_averages_by_group(ethanol_daily_by_region, 'region', rolling_cols)

    if not ethanol_daily_combined.empty:
        for col in list(ethanol_daily_combined.columns):
            if '_avg' in col:
                ethanol_daily_combined = ethanol_daily_combined.drop(columns=[col])
        ethanol_daily_combined = add_rolling_averages(ethanol_daily_combined, rolling_cols)

    # Ensure region_display is present
    region_display = {k: v['display_name'] for k, v in REGION_CONFIG.items()}
    if not ethanol_daily_by_region.empty and 'region_display' not in ethanol_daily_by_region.columns:
        ethanol_daily_by_region['region_display'] = ethanol_daily_by_region['region'].map(region_display)

    print(f"Final daily by region rows: {len(ethanol_daily_by_region):,}")
    print(f"Final daily combined rows: {len(ethanol_daily_combined):,}")

    # ========================================================================
    # SUMMARY
    # ========================================================================

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    if not ethanol_raw_df.empty:
        print(f"\nTotal unique ethanol car-days: {len(ethanol_raw_df):,}")
        print(f"  Original placards: {(~ethanol_raw_df['is_filled']).sum():,}")
        print(f"  Filled placards: {ethanol_raw_df['is_filled'].sum():,}")

        print(f"\nBy UN Code:")
        for un_code in ['1170', '1987']:
            count = (ethanol_raw_df['un_code'] == un_code).sum()
            print(f"  UN{un_code}: {count:,} cars")

        print(f"\nBy Region:")
        for region_key, config in REGION_CONFIG.items():
            display_name = config.get('display_name', region_key)
            count = (ethanol_raw_df['region'] == region_key).sum()
            print(f"  {display_name}: {count:,} car-days")

        if not ethanol_daily_combined.empty:
            recent = ethanol_daily_combined.tail(30)
            avg_cars = recent['car_count'].mean()
            avg_gallons = recent['total_gallons'].mean()
            avg_barrels = recent['total_barrels'].mean()
            avg_1170_barrels = recent['un1170_barrels'].mean()
            avg_1987_barrels = recent['un1987_barrels'].mean()
            print(f"\nLast 30-Day Average:")
            print(f"  Total Cars/day: {avg_cars:,.1f}")
            print(f"  Total Gallons/day: {avg_gallons:,.0f}")
            print(f"  Total Barrels/day: {avg_barrels:,.0f}")
            print(f"    UN1170 Barrels/day: {avg_1170_barrels:,.0f}")
            print(f"    UN1987 Barrels/day: {avg_1987_barrels:,.0f}")

    # ========================================================================
    # SAVE OUTPUT FILES
    # ========================================================================

    print("\n" + "=" * 70)
    print("SAVING OUTPUT FILES")
    print("=" * 70)

    # Save raw CSV
    if not ethanol_raw_df.empty:
        ethanol_raw_df.to_csv(RAW_CSV_PATH, index=False)
        print(f"Saved {len(ethanol_raw_df):,} rows to {RAW_CSV_PATH}")
    else:
        print("No raw data to save")

    # Save by-region CSV
    if not ethanol_daily_by_region.empty:
        ethanol_daily_by_region.to_csv(BY_REGION_CSV_PATH, index=False)
        print(f"Saved {len(ethanol_daily_by_region):,} rows to {BY_REGION_CSV_PATH}")

    # Save combined CSV
    if not ethanol_daily_combined.empty:
        ethanol_daily_combined.to_csv(COMBINED_CSV_PATH, index=False)
        print(f"Saved {len(ethanol_daily_combined):,} rows to {COMBINED_CSV_PATH}")

    # Build and save pipeline JSON
    if not ethanol_daily_by_region.empty:
        pipeline_json = build_pipeline_json(ethanol_daily_by_region)
        with open(PIPELINE_JSON_PATH, 'w') as f:
            json.dump(pipeline_json, f)
        stage_summary = ", ".join(
            f"{k}: {len(v['dates'])} days" for k, v in pipeline_json['stages'].items()
        )
        print(f"Saved pipeline JSON to {PIPELINE_JSON_PATH}")
        print(f"  Stages: {stage_summary}")
    else:
        print("No aggregated data to write pipeline JSON")

    print("\n" + "=" * 70)
    print("ETHANOL NY HARBOR PIPELINE UPDATE COMPLETE")
    print("=" * 70)


if __name__ == '__main__':
    main()
