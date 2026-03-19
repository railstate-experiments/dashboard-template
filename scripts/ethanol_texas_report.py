#!/usr/bin/env python3
"""
Ethanol Texas Export Corridor — Comparison Report
===================================================
Compares RailState sensor data (Crabb, TX + League City, TX southbound)
against US Census export data from Texas City + Galveston ports.

Monthly comparison:
  - Census 2207.10 (undenatured) vs RailState UN1170
  - Census full 2207 (2207.10 + 2207.20) vs RailState UN1170 + UN1987

Daily (last 90 days):
  - UN1170 and UN1987 car counts and barrels
  - 7-day and 30-day moving averages

Usage:
  python ethanol_texas_report.py              # incremental update
  python ethanol_texas_report.py --rebuild    # full re-fetch (180 days)
  python ethanol_texas_report.py --days 30    # fetch last 30 days

Outputs:
  ../data/ethanol_texas_report_raw.csv   - raw car-level data
  ../data/ethanol_texas_report.json      - dashboard JSON (monthly comparison + daily)
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

# Fetch window
DEFAULT_INITIAL_DAYS = 180
OVERLAP_DAYS = 2

# Ethanol placards
ETHANOL_PLACARDS = {'UN1170', 'UN1987'}

# Volume: flat 714 barrels per tank car
BARRELS_PER_CAR = 714

# Unit train / block detection
UNIT_TRAIN_THRESHOLD = 0.70
MIN_BLOCK_SIZE = 6

# Timezone for daily aggregation
LOCAL_TZ = ZoneInfo('America/Chicago')

# Daily output window
DAILY_WINDOW_DAYS = 90

# Census API
CENSUS_API_BASE = "https://api.census.gov/data/timeseries/intltrade/exports/porths"
CENSUS_PORTS = {'5306': 'Texas City, TX', '5310': 'Galveston, TX'}
CENSUS_CODES = {'220710': 'Undenatured', '220720': 'Denatured'}

# Ethanol density for kg -> barrel conversion
ETHANOL_KG_PER_LITER = 0.789
LITERS_PER_GALLON = 3.78541
GALLONS_PER_BARREL = 42

# ============================================================================
# SENSOR CONFIGURATION
# ============================================================================

SENSOR_CONFIG = [
    {'name': 'Crabb, TX', 'direction': 'southbound'},
    {'name': 'League City, TX', 'direction': 'southbound'},
]

# ============================================================================
# OUTPUT PATHS
# ============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / 'data'
RAW_CSV_PATH = DATA_DIR / 'ethanol_texas_report_raw.csv'
DASHBOARD_JSON_PATH = DATA_DIR / 'ethanol_texas_report.json'


# ============================================================================
# RAILSTATE API
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
    for h in (car.get('hazmats') or []):
        p = h.get('placardType')
        if p and p != 'EMPTY':
            return p
    return None

def is_ethanol_car(car: dict) -> bool:
    p = get_placard(car)
    return p in ETHANOL_PLACARDS if p else False

def classify_train(cars: List[dict]) -> Tuple[str, Optional[str]]:
    if not cars:
        return 'manifest', None
    tank_cars = [c for c in cars if is_tank_car(c)]
    if not tank_cars:
        return 'manifest', None
    ethanol_cars = [c for c in tank_cars if is_ethanol_car(c)]
    if any(get_placard(c) and get_placard(c) not in ETHANOL_PLACARDS for c in tank_cars):
        return 'manifest', None
    if len(tank_cars) > 0 and len(ethanol_cars) / len(tank_cars) >= UNIT_TRAIN_THRESHOLD:
        counts = {}
        for c in ethanol_cars:
            p = get_placard(c)
            if p:
                counts[p] = counts.get(p, 0) + 1
        dominant = max(counts, key=counts.get) if counts else 'UN1170'
        return 'unit', dominant
    return 'manifest', None

def fill_unit_train_placards(cars: List[dict], dominant: str) -> List[dict]:
    out = []
    for c in cars:
        cc = c.copy()
        if is_tank_car(c) and not get_placard(c):
            cc['_filled_placard'] = dominant
        out.append(cc)
    return out

def fill_manifest_train_placards(cars: List[dict]) -> List[dict]:
    out = [c.copy() for c in cars]
    i = 0
    while i < len(out):
        if not is_tank_car(out[i]):
            i += 1
            continue
        bs = i
        be = i
        while be < len(out) and is_tank_car(out[be]):
            be += 1
        sz = be - bs
        if sz >= MIN_BLOCK_SIZE:
            fp = get_placard(out[bs])
            lp = get_placard(out[be - 1])
            if fp in ETHANOL_PLACARDS and lp in ETHANOL_PLACARDS:
                if not any(get_placard(out[j]) and get_placard(out[j]) not in ETHANOL_PLACARDS
                           for j in range(bs, be)):
                    if sum(1 for j in range(bs, be) if is_ethanol_car(out[j])) / sz >= 0.5:
                        for j in range(bs, be):
                            if is_tank_car(out[j]) and not get_placard(out[j]):
                                out[j]['_filled_placard'] = fp
        i = be
    return out

def process_train_cars(sighting: dict) -> Tuple[List[dict], str, Optional[str]]:
    cars = sighting.get('cars', [])
    if not cars:
        return [], 'manifest', None
    train_type, dominant = classify_train(cars)
    if train_type == 'unit':
        filled = fill_unit_train_placards(cars, dominant or 'UN1170')
    else:
        filled = fill_manifest_train_placards(cars)
    sid = sighting.get('sightingId')
    det = sighting.get('detectionTimeUTC')
    for pos, car in enumerate(filled):
        car['_train_type'] = train_type
        car['_sighting_id'] = sid
        car['_detection_time'] = det
        car['_position'] = pos
    return filled, train_type, dominant

def extract_ethanol_cars(sightings: List[dict], sensor_name: str) -> List[dict]:
    records = []
    for sighting in sightings:
        cars, train_type, dominant = process_train_cars(sighting)
        for car in cars:
            orig = get_placard(car)
            filled = car.get('_filled_placard')
            is_orig = orig in ETHANOL_PLACARDS if orig else False
            is_fill = filled in ETHANOL_PLACARDS if filled else False
            if is_orig or is_fill:
                placard = orig if is_orig else filled
                un_code = placard[2:] if placard.startswith('UN') else placard
                car_id = car.get('carId') or ''
                if not car_id:
                    car_id = f"NOID_{car.get('_sighting_id')}_{car.get('_position')}"
                records.append({
                    'car_id': car_id,
                    'un_code': un_code,
                    'placard': placard,
                    'is_filled': is_fill and not is_orig,
                    'train_type': car.get('_train_type', ''),
                    'detection_time': car.get('_detection_time'),
                    'sensor_name': sensor_name,
                    'sighting_id': car.get('_sighting_id'),
                    'barrels': BARRELS_PER_CAR,
                })
    return records


# ============================================================================
# RAILSTATE INCREMENTAL UPDATE
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
        print(f"Deduplicated: {before:,} -> {after:,} records ({before - after:,} removed)")
    combined = combined.sort_values('detection_time').reset_index(drop=True)
    return combined


# ============================================================================
# RAILSTATE AGGREGATION
# ============================================================================

def calculate_daily_totals(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df['detection_time'] = pd.to_datetime(df['detection_time'], format='ISO8601', utc=True)
    df['date'] = df['detection_time'].dt.tz_convert(LOCAL_TZ).dt.date
    df = df.drop_duplicates(subset=['car_id', 'date', 'sensor_name'], keep='first')
    df['date'] = pd.to_datetime(df['date'])
    df['un_code'] = df['un_code'].astype(str)
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
    if df.empty:
        return df
    df = df.copy()
    df = df.sort_values('date')
    date_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')
    df = df.set_index('date').reindex(date_range, fill_value=0).reset_index()
    df = df.rename(columns={'index': 'date'})
    for col in ['total_barrels', 'un1170_barrels', 'un1987_barrels',
                'total_cars', 'un1170_cars', 'un1987_cars']:
        df[f'{col}_7d_ma'] = df[col].rolling(window=7, min_periods=1).mean().round(2)
        df[f'{col}_30d_ma'] = df[col].rolling(window=30, min_periods=1).mean().round(2)
    return df

def calculate_monthly_totals(df: pd.DataFrame) -> pd.DataFrame:
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
    monthly['month'] = monthly['month'].astype(str)
    return monthly


# ============================================================================
# CENSUS DATA
# ============================================================================

def kg_to_barrels(kg: float) -> float:
    return kg / ETHANOL_KG_PER_LITER / LITERS_PER_GALLON / GALLONS_PER_BARREL

def fetch_census_data() -> dict:
    """Fetch Census port-level export data for 2207.10 and 2207.20
    from Texas City and Galveston. Returns dict keyed by month."""

    print("\n  Fetching Census export data...")

    # Determine date range: try from 2025-01 through current month
    now = datetime.utcnow()
    end_time = f"{now.year}-{now.month:02d}"

    results = {}  # month -> {census_2207_10_bbl, census_2207_20_bbl, census_total_bbl}

    for hs_code, hs_label in CENSUS_CODES.items():
        for port_id, port_name in CENSUS_PORTS.items():
            url = (f"{CENSUS_API_BASE}?get=PORT_NAME,ALL_VAL_MO,VES_WGT_MO"
                   f"&E_COMMODITY={hs_code}&SUMMARY_LVL=DET&PORT={port_id}"
                   f"&time=from+2025-01+to+{end_time}")
            try:
                resp = requests.get(url, timeout=60)
                if resp.status_code == 204 or not resp.text.strip():
                    continue
                if resp.status_code != 200:
                    print(f"    Warning: Census API returned {resp.status_code} for {hs_code}/{port_name}")
                    continue
                rows = resp.json()[1:]  # skip header
                for row in rows:
                    month = row[6]  # time column
                    kg = int(row[2])  # VES_WGT_MO
                    bbl = kg_to_barrels(kg)

                    if month not in results:
                        results[month] = {
                            'census_2207_10_bbl': 0.0,
                            'census_2207_20_bbl': 0.0,
                        }

                    if hs_code == '220710':
                        results[month]['census_2207_10_bbl'] += bbl
                    else:
                        results[month]['census_2207_20_bbl'] += bbl

                print(f"    {hs_label} / {port_name}: {len(rows)} months")
            except Exception as e:
                print(f"    Warning: Census fetch failed for {hs_code}/{port_name}: {e}")

    # Calculate totals
    for month in results:
        results[month]['census_total_bbl'] = (
            results[month]['census_2207_10_bbl'] +
            results[month]['census_2207_20_bbl']
        )

    print(f"  Census data: {len(results)} months loaded")
    return results


# ============================================================================
# OUTPUT
# ============================================================================

def save_dashboard_json(daily_df: pd.DataFrame, monthly_df: pd.DataFrame,
                        census: dict, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

    # Monthly comparison rows
    monthly_rows = []
    all_months = set()
    if not monthly_df.empty:
        all_months.update(monthly_df['month'].tolist())
    all_months.update(census.keys())

    for m in sorted(all_months):
        rs_row = monthly_df[monthly_df['month'] == m] if not monthly_df.empty else pd.DataFrame()
        c = census.get(m, {})

        rs_1170_cars = int(rs_row['un1170_cars'].iloc[0]) if not rs_row.empty else None
        rs_1170_bbl = int(rs_row['un1170_barrels'].iloc[0]) if not rs_row.empty else None
        rs_1987_cars = int(rs_row['un1987_cars'].iloc[0]) if not rs_row.empty else None
        rs_1987_bbl = int(rs_row['un1987_barrels'].iloc[0]) if not rs_row.empty else None
        rs_total_cars = int(rs_row['total_cars'].iloc[0]) if not rs_row.empty else None
        rs_total_bbl = int(rs_row['total_barrels'].iloc[0]) if not rs_row.empty else None

        census_2207_10 = round(c.get('census_2207_10_bbl', 0)) if c else None
        census_2207_20 = round(c.get('census_2207_20_bbl', 0)) if c else None
        census_total = round(c.get('census_total_bbl', 0)) if c else None

        monthly_rows.append([
            m,
            census_2207_10, census_2207_20, census_total,
            rs_1170_cars, rs_1170_bbl,
            rs_1987_cars, rs_1987_bbl,
            rs_total_cars, rs_total_bbl,
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
        'commodity': 'ethanol_texas_report',
        'display_name': 'Ethanol — Texas Export Corridor',
        'subtitle': 'RailState (Crabb + League City SB) vs Census (Texas City + Galveston)',
        'unit': 'barrels',
        'barrels_per_car': BARRELS_PER_CAR,
        'last_updated': datetime.utcnow().strftime('%Y-%m-%d'),
        'railstate_sensors': [s['name'] + ' ' + s['direction'] for s in SENSOR_CONFIG],
        'census_ports': list(CENSUS_PORTS.values()),
        'monthly_comparison': {
            'columns': [
                'month',
                'census_2207_10_bbl', 'census_2207_20_bbl', 'census_total_bbl',
                'rs_un1170_cars', 'rs_un1170_bbl',
                'rs_un1987_cars', 'rs_un1987_bbl',
                'rs_total_cars', 'rs_total_bbl',
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
    parser = argparse.ArgumentParser(description='Ethanol Texas Export Corridor — Comparison Report')
    parser.add_argument('--days', type=int, default=None,
                        help='Override fetch window to last N days')
    parser.add_argument('--rebuild', action='store_true',
                        help='Force full re-fetch (last 180 days)')
    args = parser.parse_args()

    api_key = os.environ.get('RAILSTATE_API_KEY', HARDCODED_API_KEY)

    print("=" * 70)
    print("ETHANOL TEXAS EXPORT CORRIDOR — COMPARISON REPORT")
    print(f"Run Time: {datetime.utcnow().isoformat()}")
    print(f"RailState sensors: {', '.join(s['name'] + ' (' + s['direction'] + ')' for s in SENSOR_CONFIG)}")
    print(f"Census ports: {', '.join(CENSUS_PORTS.values())}")
    print(f"Volume: {BARRELS_PER_CAR} barrels per car")
    print("=" * 70)

    # ── RailState fetch ──
    existing_df = None if args.rebuild else load_existing_raw(RAW_CSV_PATH)
    start_date, end_date = determine_fetch_window(existing_df, args)

    fetcher = RailStateFetcher(api_key)
    fetcher.load_sensors()
    print(f"Loaded {len(fetcher._sensor_cache)} sensors")

    print("\n" + "=" * 70)
    print("FETCHING RAILSTATE DATA")
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
            un1170 = sum(1 for r in records if r['un_code'] == '1170')
            un1987 = sum(1 for r in records if r['un_code'] == '1987')
            print(f"    {len(sightings):,} sightings -> {len(records):,} ethanol cars")
            print(f"    UN1170: {un1170:,}  UN1987: {un1987:,}")
        else:
            print(f"    No sightings found")

    new_df = pd.DataFrame(all_data) if all_data else pd.DataFrame()
    print(f"\nNew records fetched: {len(new_df):,}")
    combined_df = merge_raw_data(existing_df, new_df)

    if combined_df.empty:
        print("\nNo RailState data found!")
        sys.exit(1)

    # Save raw
    print("\n" + "=" * 70)
    print("SAVING RAW DATA")
    print("=" * 70)
    combined_df.to_csv(RAW_CSV_PATH, index=False)
    print(f"Saved {len(combined_df):,} raw records to {RAW_CSV_PATH.name}")

    # ── RailState aggregation ──
    print("\n" + "=" * 70)
    print("CALCULATING RAILSTATE TOTALS")
    print("=" * 70)

    daily_totals = calculate_daily_totals(combined_df)
    daily_with_ma = add_moving_averages(daily_totals)
    monthly_totals = calculate_monthly_totals(daily_with_ma)
    print(f"Daily: {len(daily_with_ma)} days")
    print(f"Monthly: {len(monthly_totals)} months")

    # ── Census fetch ──
    print("\n" + "=" * 70)
    print("FETCHING CENSUS DATA")
    print("=" * 70)

    census = fetch_census_data()

    # ── Save output ──
    print("\n" + "=" * 70)
    print("SAVING REPORT")
    print("=" * 70)

    save_dashboard_json(daily_with_ma, monthly_totals, census, DASHBOARD_JSON_PATH)

    # ── Print comparison ──
    print("\n" + "=" * 70)
    print("MONTHLY COMPARISON")
    print("=" * 70)

    print(f"\n--- 2207.10 (Undenatured) vs UN1170 ---")
    print(f"{'Month':<10} {'Census':>14} {'RailState':>14} {'RS/Census':>10}")
    print("-" * 52)
    for m in sorted(set(monthly_totals['month'].tolist()) & set(census.keys())):
        rs_row = monthly_totals[monthly_totals['month'] == m]
        c = census[m]
        census_bbl = c['census_2207_10_bbl']
        rs_bbl = int(rs_row['un1170_barrels'].iloc[0])
        ratio = rs_bbl / census_bbl if census_bbl > 0 else 0
        print(f"{m:<10} {census_bbl:>14,.0f} {rs_bbl:>14,} {ratio:>9.1%}")

    print(f"\n--- Full 2207 (Undent + Dent) vs UN1170 + UN1987 ---")
    print(f"{'Month':<10} {'Census':>14} {'RailState':>14} {'RS/Census':>10}")
    print("-" * 52)
    for m in sorted(set(monthly_totals['month'].tolist()) & set(census.keys())):
        rs_row = monthly_totals[monthly_totals['month'] == m]
        c = census[m]
        census_bbl = c['census_total_bbl']
        rs_bbl = int(rs_row['total_barrels'].iloc[0])
        ratio = rs_bbl / census_bbl if census_bbl > 0 else 0
        print(f"{m:<10} {census_bbl:>14,.0f} {rs_bbl:>14,} {ratio:>9.1%}")

    # Daily summary
    if not daily_with_ma.empty:
        last = daily_with_ma.iloc[-1]
        print(f"\nMost Recent Moving Averages:")
        print(f"  Total barrels/day (7d):  {last['total_barrels_7d_ma']:,.0f}")
        print(f"  Total barrels/day (30d): {last['total_barrels_30d_ma']:,.0f}")
        print(f"  UN1170 barrels/day (7d): {last['un1170_barrels_7d_ma']:,.0f}")
        print(f"  UN1987 barrels/day (7d): {last['un1987_barrels_7d_ma']:,.0f}")

    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)


if __name__ == '__main__':
    main()
