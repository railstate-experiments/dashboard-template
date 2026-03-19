#!/usr/bin/env python3
"""
Ethanol Cross-Border (US→Canada) — Comparison Report
=====================================================
Compares RailState sensor data at 9 US-Canada border crossings against
US Census export data (HS 2207) to Canada.

Placard-to-Census mapping:
  UN1170           → Census 2207.10 (undenatured)
  UN3475 + UN1987  → Census 2207.20 (denatured)

Fetches both directions at each crossing:
  - Export direction (US→Canada): the primary measurement
  - Return direction (Canada→US): to confirm empty returns

Each crossing uses its local timezone for daily date assignment.

Usage:
  python ethanol_xb_report.py              # incremental update
  python ethanol_xb_report.py --rebuild    # full re-fetch from Jan 2024
  python ethanol_xb_report.py --days 30    # fetch last 30 days

Outputs:
  ../data/ethanol_xb_report_raw.csv    - raw car-level data (both directions)
  ../data/ethanol_xb_report.json       - dashboard JSON (monthly comparison + daily)
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

# Default start date for full rebuild
DEFAULT_START_DATE = datetime(2024, 1, 1)
OVERLAP_DAYS = 2

# Ethanol UN placards
ETHANOL_PLACARDS = {'UN1170', 'UN1987', 'UN3475'}

# Placard grouping for Census comparison
UNDENATURED_PLACARDS = {'UN1170'}
DENATURED_PLACARDS = {'UN1987', 'UN3475'}

# Volume: 714 barrels per tank car
BARRELS_PER_CAR = 714

# Unit train / block detection
UNIT_TRAIN_THRESHOLD = 0.70
MIN_BLOCK_SIZE = 6

# Daily output window
DAILY_WINDOW_DAYS = 90

# Census API
CENSUS_EXPORTS_BASE = "https://api.census.gov/data/timeseries/intltrade/exports/hs"
CANADA_CTY_CODE = "1220"

# Census 10-digit codes with volume in liters
CENSUS_EXPORT_CODES = {
    'undent': ['2207106010', '2207106090'],
    'dent': ['2207200010', '2207200090'],
}

LITERS_PER_GALLON = 3.78541
GALLONS_PER_BARREL = 42

# ============================================================================
# CROSSING CONFIGURATION
# ============================================================================

CROSSING_CONFIG = {
    'Blaine_WA': {
        'display_name': 'Blaine, WA',
        'timezone': 'America/Los_Angeles',
        'sensors': [{'name': 'Blaine, WA', 'export_dir': 'northbound', 'return_dir': 'southbound'}],
        'exclude_un1987': False,
    },
    'Ste_Anne_MB': {
        'display_name': 'Ste Anne, MB',
        'timezone': 'America/Winnipeg',
        'sensors': [{'name': 'Ste Anne, MB', 'export_dir': 'westbound', 'return_dir': 'eastbound'}],
        'exclude_un1987': False,
    },
    'Mcara_SK': {
        'display_name': 'Mcara, SK',
        'timezone': 'America/Regina',
        'sensors': [{'name': 'Mcara, SK', 'export_dir': 'northbound', 'return_dir': 'southbound'}],
        'exclude_un1987': False,
    },
    'Kevin_Coalhurst': {
        'display_name': 'Kevin, MT / Coalhurst, AB',
        'timezone': 'America/Denver',
        'sensors': [
            {'name': 'Kevin, MT', 'export_dir': 'northbound', 'return_dir': 'southbound'},
            {'name': 'Coalhurst, AB', 'export_dir': 'westbound', 'return_dir': 'eastbound'},
        ],
        'extra_days': 1,
        'exclude_un1987': False,
    },
    'Moyie_Springs_ID': {
        'display_name': 'Moyie Springs, ID',
        'timezone': 'America/Boise',
        'sensors': [{'name': 'Moyie Springs, ID', 'export_dir': 'northbound', 'return_dir': 'southbound'}],
        'exclude_un1987': False,
    },
    'Letellier_MB': {
        'display_name': 'Letellier, MB',
        'timezone': 'America/Winnipeg',
        'sensors': [{'name': 'Letellier, MB', 'export_dir': 'northbound', 'return_dir': 'southbound'}],
        'exclude_un1987': False,
    },
    'Grande_Pointe_MB': {
        'display_name': 'Grande Pointe, MB',
        'timezone': 'America/Winnipeg',
        'sensors': [{'name': 'Grande Pointe, MB', 'export_dir': 'northbound', 'return_dir': 'southbound'}],
        'exclude_un1987': False,
    },
    'Port_Huron_London': {
        'display_name': 'Port Huron, MI / London West, ON',
        'timezone': 'America/Detroit',
        'sensors': [
            {'name': 'Port Huron, MI', 'export_dir': 'eastbound', 'return_dir': 'westbound'},
            {'name': 'London West, ON', 'export_dir': 'eastbound', 'return_dir': 'westbound'},
        ],
        'exclude_un1987': False,
    },
    'Windsor_Region': {
        'display_name': 'Windsor / Komoka / Galt',
        'timezone': 'America/Toronto',
        'sensors': [
            {'name': 'Windsor TFR, ON', 'export_dir': 'eastbound', 'return_dir': 'westbound'},
            {'name': 'Komoka, ON', 'export_dir': 'eastbound', 'return_dir': 'westbound'},
            {'name': 'Galt, ON', 'export_dir': 'eastbound', 'return_dir': 'westbound'},
        ],
        'exclude_un1987': True,
    },
}

# ============================================================================
# OUTPUT PATHS
# ============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / 'data'
RAW_CSV_PATH = DATA_DIR / 'ethanol_xb_report_raw.csv'
DASHBOARD_JSON_PATH = DATA_DIR / 'ethanol_xb_report.json'


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
                if error == "Timeout":
                    print(f"        Timeout, continuing...")
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
        dominant = max(counts, key=counts.get) if counts else 'UN3475'
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
        filled = fill_unit_train_placards(cars, dominant or 'UN3475')
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

def extract_ethanol_cars(sightings: List[dict], sensor_name: str,
                         crossing: str, direction_label: str,
                         exclude_un1987: bool) -> List[dict]:
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
                if exclude_un1987 and un_code == '1987':
                    continue
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
                    'crossing': crossing,
                    'direction': direction_label,
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
        start_date = DEFAULT_START_DATE
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
    combined = combined.drop_duplicates(subset=['car_id', 'detection_time', 'direction'], keep='last')
    after = len(combined)
    if before != after:
        print(f"Deduplicated: {before:,} -> {after:,} records ({before - after:,} removed)")
    combined = combined.sort_values('detection_time').reset_index(drop=True)
    return combined


# ============================================================================
# RAILSTATE AGGREGATION
# ============================================================================

def calculate_daily_totals(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate daily totals for EXPORT direction only, using local timezone per crossing."""
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df = df[df['direction'] == 'export'].copy()
    if df.empty:
        return pd.DataFrame()

    df['detection_time'] = pd.to_datetime(df['detection_time'], format='ISO8601', utc=True)

    # Apply local timezone per crossing
    tz_map = {k: ZoneInfo(v['timezone']) for k, v in CROSSING_CONFIG.items()}
    df['date'] = df.apply(
        lambda r: r['detection_time'].astimezone(tz_map.get(r['crossing'], ZoneInfo('UTC'))).date(),
        axis=1
    )

    # Dedup at crossing level — multiple sensors at the same crossing watch the
    # same corridor, so a car seen at both Port Huron and London West on the
    # same day is one car, not two. A car at different crossings on the same
    # day counts separately.
    df = df.drop_duplicates(subset=['car_id', 'date', 'crossing'], keep='first')
    df['date'] = pd.to_datetime(df['date'])
    df['un_code'] = df['un_code'].astype(str)

    df['is_undent'] = df['un_code'].isin({'1170'}).astype(int)
    df['is_dent'] = df['un_code'].isin({'1987', '3475'}).astype(int)
    df['bbl_undent'] = df['is_undent'] * BARRELS_PER_CAR
    df['bbl_dent'] = df['is_dent'] * BARRELS_PER_CAR

    daily = df.groupby('date').agg(
        total_cars=('car_id', 'count'),
        undent_cars=('is_undent', 'sum'),
        dent_cars=('is_dent', 'sum'),
        total_barrels=('barrels', 'sum'),
        undent_barrels=('bbl_undent', 'sum'),
        dent_barrels=('bbl_dent', 'sum'),
    ).reset_index()
    return daily

def add_moving_averages(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy().sort_values('date')
    date_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')
    df = df.set_index('date').reindex(date_range, fill_value=0).reset_index()
    df = df.rename(columns={'index': 'date'})
    for col in ['total_barrels', 'undent_barrels', 'dent_barrels',
                'total_cars', 'undent_cars', 'dent_cars']:
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
        undent_cars=('undent_cars', 'sum'),
        dent_cars=('dent_cars', 'sum'),
        total_barrels=('total_barrels', 'sum'),
        undent_barrels=('undent_barrels', 'sum'),
        dent_barrels=('dent_barrels', 'sum'),
    ).reset_index()
    monthly = monthly.sort_values('month')
    monthly['month'] = monthly['month'].astype(str)
    return monthly


# ============================================================================
# CENSUS DATA
# ============================================================================

def fetch_census_exports_to_canada() -> dict:
    """Fetch Census 10-digit export data to Canada for 2207.10 and 2207.20."""
    print("\n  Fetching Census export data to Canada...")
    now = datetime.utcnow()
    end_time = f"{now.year}-{now.month:02d}"
    results = {}

    for group, codes in CENSUS_EXPORT_CODES.items():
        for code in codes:
            url = (f"{CENSUS_EXPORTS_BASE}?get=ALL_VAL_MO,QTY_1_MO,UNIT_QY1"
                   f"&E_COMMODITY={code}&CTY_CODE={CANADA_CTY_CODE}"
                   f"&time=from+2024-01+to+{end_time}")
            try:
                resp = requests.get(url, timeout=60)
                if resp.status_code == 204 or not resp.text.strip():
                    continue
                if resp.status_code != 200:
                    print(f"    Warning: Census API {resp.status_code} for {code}")
                    continue
                rows = resp.json()[1:]
                for row in rows:
                    month = row[5]
                    liters = int(row[1])
                    val = int(row[0])
                    if month not in results:
                        results[month] = {'undent_l': 0, 'dent_l': 0, 'undent_val': 0, 'dent_val': 0}
                    if group == 'undent':
                        results[month]['undent_l'] += liters
                        results[month]['undent_val'] += val
                    else:
                        results[month]['dent_l'] += liters
                        results[month]['dent_val'] += val
                print(f"    {code}: {len(rows)} months")
            except Exception as e:
                print(f"    Warning: Census fetch failed for {code}: {e}")

    # Convert to barrels
    for m in results:
        r = results[m]
        r['undent_bbl'] = r['undent_l'] / LITERS_PER_GALLON / GALLONS_PER_BARREL
        r['dent_bbl'] = r['dent_l'] / LITERS_PER_GALLON / GALLONS_PER_BARREL
        r['total_bbl'] = r['undent_bbl'] + r['dent_bbl']

    print(f"  Census data: {len(results)} months loaded")
    return results


# ============================================================================
# OUTPUT
# ============================================================================

def save_dashboard_json(daily_df: pd.DataFrame, monthly_df: pd.DataFrame,
                        census: dict, return_summary: dict, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

    # Monthly comparison
    all_months = set()
    if not monthly_df.empty:
        all_months.update(monthly_df['month'].tolist())
    all_months.update(census.keys())

    monthly_rows = []
    for m in sorted(all_months):
        rs = monthly_df[monthly_df['month'] == m] if not monthly_df.empty else pd.DataFrame()
        c = census.get(m, {})

        monthly_rows.append([
            m,
            round(c.get('undent_bbl', 0)) if c else None,
            round(c.get('dent_bbl', 0)) if c else None,
            round(c.get('total_bbl', 0)) if c else None,
            int(rs['undent_cars'].iloc[0]) if not rs.empty else None,
            int(rs['undent_barrels'].iloc[0]) if not rs.empty else None,
            int(rs['dent_cars'].iloc[0]) if not rs.empty else None,
            int(rs['dent_barrels'].iloc[0]) if not rs.empty else None,
            int(rs['total_cars'].iloc[0]) if not rs.empty else None,
            int(rs['total_barrels'].iloc[0]) if not rs.empty else None,
        ])

    # Daily section
    daily_section = {}
    if not daily_df.empty:
        recent = daily_df.tail(DAILY_WINDOW_DAYS).copy()
        daily_section = {
            'dates': [d.strftime('%Y-%m-%d') for d in recent['date']],
            'undent_cars': [int(v) for v in recent['undent_cars']],
            'undent_barrels': [int(v) for v in recent['undent_barrels']],
            'dent_cars': [int(v) for v in recent['dent_cars']],
            'dent_barrels': [int(v) for v in recent['dent_barrels']],
            'total_cars': [int(v) for v in recent['total_cars']],
            'total_barrels': [int(v) for v in recent['total_barrels']],
            'total_barrels_7d_ma': [round(v, 2) for v in recent['total_barrels_7d_ma']],
            'total_barrels_30d_ma': [round(v, 2) for v in recent['total_barrels_30d_ma']],
            'undent_barrels_7d_ma': [round(v, 2) for v in recent['undent_barrels_7d_ma']],
            'undent_barrels_30d_ma': [round(v, 2) for v in recent['undent_barrels_30d_ma']],
            'dent_barrels_7d_ma': [round(v, 2) for v in recent['dent_barrels_7d_ma']],
            'dent_barrels_30d_ma': [round(v, 2) for v in recent['dent_barrels_30d_ma']],
        }

    crossings_list = [v['display_name'] for v in CROSSING_CONFIG.values()]

    output = {
        'commodity': 'ethanol_xb_report',
        'display_name': 'Ethanol — US to Canada Cross-Border',
        'subtitle': f'{len(CROSSING_CONFIG)} border crossings vs Census exports to Canada',
        'unit': 'barrels',
        'barrels_per_car': BARRELS_PER_CAR,
        'last_updated': datetime.utcnow().strftime('%Y-%m-%d'),
        'crossings': crossings_list,
        'placard_mapping': {
            'undenatured': 'UN1170 → Census 2207.10',
            'denatured': 'UN3475 + UN1987 → Census 2207.20',
        },
        'return_traffic_summary': return_summary,
        'monthly_comparison': {
            'columns': [
                'month',
                'census_undent_bbl', 'census_dent_bbl', 'census_total_bbl',
                'rs_undent_cars', 'rs_undent_bbl',
                'rs_dent_cars', 'rs_dent_bbl',
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
    parser = argparse.ArgumentParser(description='Ethanol Cross-Border — Comparison Report')
    parser.add_argument('--days', type=int, default=None)
    parser.add_argument('--rebuild', action='store_true')
    args = parser.parse_args()

    api_key = os.environ.get('RAILSTATE_API_KEY', HARDCODED_API_KEY)

    print("=" * 70)
    print("ETHANOL CROSS-BORDER (US→CANADA) — COMPARISON REPORT")
    print(f"Run Time: {datetime.utcnow().isoformat()}")
    print(f"Crossings: {len(CROSSING_CONFIG)}")
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
    for crossing_key, config in CROSSING_CONFIG.items():
        display = config['display_name']
        extra_days = config.get('extra_days', 0)
        exclude_un1987 = config.get('exclude_un1987', False)
        extended_end = end_date + timedelta(days=extra_days)

        print(f"\n  {display}:")

        for sensor_cfg in config['sensors']:
            sensor_name = sensor_cfg['name']
            sensor_id = fetcher.get_sensor_id(sensor_name)
            if not sensor_id:
                print(f"    Warning: Sensor not found: {sensor_name}")
                continue

            # Export direction
            export_dir = sensor_cfg['export_dir']
            print(f"    {sensor_name} ({export_dir} = export)...")
            sightings = fetcher.fetch_sightings(sensor_id, start_date, extended_end, export_dir)
            if sightings:
                records = extract_ethanol_cars(sightings, sensor_name, crossing_key,
                                               'export', exclude_un1987)
                all_data.extend(records)
                print(f"      {len(sightings):,} sightings -> {len(records):,} ethanol cars")
            else:
                print(f"      No sightings")

            # Return direction
            return_dir = sensor_cfg['return_dir']
            print(f"    {sensor_name} ({return_dir} = return)...")
            sightings = fetcher.fetch_sightings(sensor_id, start_date, extended_end, return_dir)
            if sightings:
                records = extract_ethanol_cars(sightings, sensor_name, crossing_key,
                                               'return', exclude_un1987)
                all_data.extend(records)
                print(f"      {len(sightings):,} sightings -> {len(records):,} ethanol cars")
            else:
                print(f"      No sightings")

    new_df = pd.DataFrame(all_data) if all_data else pd.DataFrame()
    print(f"\nNew records fetched: {len(new_df):,}")

    if not new_df.empty:
        export_count = len(new_df[new_df['direction'] == 'export'])
        return_count = len(new_df[new_df['direction'] == 'return'])
        print(f"  Export direction: {export_count:,}")
        print(f"  Return direction: {return_count:,}")

    combined_df = merge_raw_data(existing_df, new_df)

    if combined_df.empty:
        print("\nNo data found!")
        sys.exit(1)

    # Save raw
    print("\n" + "=" * 70)
    print("SAVING RAW DATA")
    print("=" * 70)
    combined_df.to_csv(RAW_CSV_PATH, index=False)
    print(f"Saved {len(combined_df):,} raw records to {RAW_CSV_PATH.name}")

    # ── Return traffic summary ──
    print("\n" + "=" * 70)
    print("RETURN TRAFFIC ANALYSIS")
    print("=" * 70)

    return_df = combined_df[combined_df['direction'] == 'return'].copy() if 'direction' in combined_df.columns else pd.DataFrame()
    return_summary = {}
    if not return_df.empty:
        return_summary['total_cars'] = len(return_df)
        return_summary['note'] = 'Cars seen in return direction — confirms empty returns if ethanol placard but returning toward US'
        by_placard = return_df['un_code'].astype(str).value_counts().to_dict()
        return_summary['by_placard'] = {f'UN{k}': int(v) for k, v in by_placard.items()}
        print(f"Return direction ethanol cars: {len(return_df):,}")
        for p, c in by_placard.items():
            print(f"  UN{p}: {c:,}")
    else:
        print("No return direction data")

    # ── RailState aggregation (export direction only) ──
    print("\n" + "=" * 70)
    print("CALCULATING RAILSTATE TOTALS (export direction)")
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

    census = fetch_census_exports_to_canada()

    # ── Save output ──
    print("\n" + "=" * 70)
    print("SAVING REPORT")
    print("=" * 70)

    save_dashboard_json(daily_with_ma, monthly_totals, census, return_summary,
                        DASHBOARD_JSON_PATH)

    # ── Print comparison ──
    print("\n" + "=" * 70)
    print("MONTHLY COMPARISON")
    print("=" * 70)

    overlap_months = sorted(set(monthly_totals['month'].tolist()) & set(census.keys()))

    print(f"\n--- Undenatured: Census 2207.10 vs RailState UN1170 ---")
    print(f"{'Month':<10} {'Census':>14} {'RailState':>14} {'RS/Census':>10}")
    print("-" * 52)
    for m in overlap_months:
        rs = monthly_totals[monthly_totals['month'] == m]
        c = census[m]
        c_bbl = c['undent_bbl']
        rs_bbl = int(rs['undent_barrels'].iloc[0])
        ratio = rs_bbl / c_bbl if c_bbl > 0 else 0
        print(f"{m:<10} {c_bbl:>14,.0f} {rs_bbl:>14,} {ratio:>9.1%}")

    print(f"\n--- Denatured: Census 2207.20 vs RailState UN3475+UN1987 ---")
    print(f"{'Month':<10} {'Census':>14} {'RailState':>14} {'RS/Census':>10}")
    print("-" * 52)
    for m in overlap_months:
        rs = monthly_totals[monthly_totals['month'] == m]
        c = census[m]
        c_bbl = c['dent_bbl']
        rs_bbl = int(rs['dent_barrels'].iloc[0])
        ratio = rs_bbl / c_bbl if c_bbl > 0 else 0
        print(f"{m:<10} {c_bbl:>14,.0f} {rs_bbl:>14,} {ratio:>9.1%}")

    print(f"\n--- Total: Census full 2207 vs RailState all placards ---")
    print(f"{'Month':<10} {'Census':>14} {'RailState':>14} {'RS/Census':>10}")
    print("-" * 52)
    for m in overlap_months:
        rs = monthly_totals[monthly_totals['month'] == m]
        c = census[m]
        c_bbl = c['total_bbl']
        rs_bbl = int(rs['total_barrels'].iloc[0])
        ratio = rs_bbl / c_bbl if c_bbl > 0 else 0
        print(f"{m:<10} {c_bbl:>14,.0f} {rs_bbl:>14,} {ratio:>9.1%}")

    # Daily summary
    if not daily_with_ma.empty:
        last = daily_with_ma.iloc[-1]
        print(f"\nMost Recent Moving Averages:")
        print(f"  Total barrels/day (7d):  {last['total_barrels_7d_ma']:,.0f}")
        print(f"  Total barrels/day (30d): {last['total_barrels_30d_ma']:,.0f}")
        print(f"  Undent barrels/day (7d): {last['undent_barrels_7d_ma']:,.0f}")
        print(f"  Dent barrels/day (7d):   {last['dent_barrels_7d_ma']:,.0f}")

    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)


if __name__ == '__main__':
    main()
