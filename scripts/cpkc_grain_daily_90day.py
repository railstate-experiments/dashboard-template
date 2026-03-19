"""
CPKC Canadian Grain — Daily 90-Day Rolling Monitor
====================================================
Standalone script that:
  1. First run: fetches 90 days of data, applies V9.4 exclusion/rescue logic
  2. Subsequent runs: fetches only new days, merges into existing data
  3. Always generates an HTML dashboard with monthly history + daily 90-day charts

Uses the EXACT same V9.4 methodology as the production analysis:
  - Same API, same 20 sensors, same exclusion logic, same rescue mechanism
  - Same grain equipment codes (C113, C114)
  - Same trip gap (7 days), same 96 tonnes/carload
  - Same operator filtering and multi-destination dedup

Output files (in same directory as this script):
  - daily_90day_data.json   (daily carload data store)
  - cpkc_grain_daily_90day.html  (dashboard)
"""

import requests
import json
import os
import sys
import time
import re
import calendar
import urllib.parse
import threading
from datetime import datetime, timedelta, date
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import pytz
except ImportError:
    print("ERROR: pytz is required. Install with: pip install pytz")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas is required. Install with: pip install pandas")
    sys.exit(1)

sys.stdout.reconfigure(line_buffering=True)

# ============================================================
# CONFIGURATION — identical to V9.4 production
# ============================================================

RAILSTATE_API_TOKEN = os.environ.get("RAILSTATE_API_KEY", (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJhdWQiOiJhcGkucmFpbHN0YXRlLmNvbSIsImlzcyI6InJhaWxzdGF0ZS5jb20iLCJ1aWQiOiJ5RWE5aTFSbjljTmhtbEFEQkNYd3BFR3h1eEczIiwidGlkIjoiRGFuIERldm9lIFRlc3RpbmcifQ."
    "8YQJPJE3X2xomrgUCXo41tx0Vh0gCRWRZ2JbQKYpAXE"
))

API_BASE_URL = "https://api.railstate.com"
SIGHTINGS_ENDPOINT = "/api/v3/trains/full_sightings"
FULL_API_URL = API_BASE_URL + SIGHTINGS_ENDPOINT

GRAIN_EQUIPMENT_CODES = ['C113', 'C114']
TRIP_GAP_DAYS = 7
AVG_TONNES_PER_CAR = 96.0
MAX_API_WORKERS = 5
RATE_LIMIT_RETRY_DELAY = 2.0
RATE_LIMIT_MAX_RETRIES = 3
MAX_RESULTS_PER_PAGE = 200
ROLLING_DAYS = 120

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DAILY_DATA_FILE = os.path.join(SCRIPT_DIR, "daily_90day_data.json")
HTML_OUTPUT_FILE = os.path.join(SCRIPT_DIR, "cpkc_grain_daily_90day.html")
PRODUCTION_RESULTS_FILE = os.path.join(SCRIPT_DIR, "v9_4_production_results.json")

# --- Sensor Configuration (identical to production) ---
CPKC_SENSORS = {
    "carneys_spur_wb": {"name": "Carney's Spur, BC", "sensor_id": "128", "direction": "Westbound", "destination": "Vancouver", "is_export": True, "cpkc_only": True},
    "kevin_sb": {"name": "Kevin, MT (SB)", "sensor_id": "140", "direction": "Southbound", "destination": "US_Kevin", "is_export": True, "cpkc_only": False},
    "kevin_nb": {"name": "Kevin, MT (NB)", "sensor_id": "140", "direction": "Northbound", "destination": "_kevin_nb", "is_export": False, "cpkc_only": False},
    "keewatin_eb": {"name": "Keewatin, ON", "sensor_id": "83", "direction": "Eastbound", "destination": "Thunder_Bay", "is_export": True, "cpkc_only": True},
    "moyie_sb": {"name": "Moyie Springs, ID (SB)", "sensor_id": "40", "direction": "Southbound", "destination": "Portland", "is_export": True, "cpkc_only": False},
    "mcara_sb": {"name": "Mcara, SK (SB)", "sensor_id": "123", "direction": "Southbound", "destination": "US_Mcara", "is_export": True, "cpkc_only": True},
    "moyie_nb": {"name": "Moyie Springs, ID (NB)", "sensor_id": "40", "direction": "Northbound", "destination": "_moyie_nb", "is_export": False, "cpkc_only": False},
    "mcara_nb": {"name": "Mcara, SK (NB)", "sensor_id": "123", "direction": "Northbound", "destination": "_mcara_nb", "is_export": False, "cpkc_only": False},
    "rufus_eb": {"name": "Rufus, SK (EB)", "sensor_id": "132", "direction": "Eastbound", "destination": "_rufus_eb", "is_export": False, "cpkc_only": False},
    "rufus_wb": {"name": "Rufus, SK (WB)", "sensor_id": "132", "direction": "Westbound", "destination": "_rufus_wb", "is_export": False, "cpkc_only": False},
    "milaty_eb": {"name": "Milaty, SK (EB)", "sensor_id": "61", "direction": "Eastbound", "destination": "_milaty_eb", "is_export": False, "cpkc_only": False},
    "milaty_wb": {"name": "Milaty, SK (WB)", "sensor_id": "61", "direction": "Westbound", "destination": "_milaty_wb", "is_export": False, "cpkc_only": False},
    "craven_nb": {"name": "Craven, SK (NB)", "sensor_id": "64", "direction": "Northbound", "destination": "_craven_nb", "is_export": False, "cpkc_only": False},
    "craven_sb": {"name": "Craven, SK (SB)", "sensor_id": "64", "direction": "Southbound", "destination": "_craven_sb", "is_export": False, "cpkc_only": False},
    "zola_wb": {"name": "Zola, SK (WB)", "sensor_id": "323", "direction": "Westbound", "destination": "_zola_wb", "is_export": False, "cpkc_only": False},
    "zola_eb": {"name": "Zola, SK (EB)", "sensor_id": "323", "direction": "Eastbound", "destination": "_zola_eb", "is_export": False, "cpkc_only": False},
    "mortlach_wb": {"name": "Mortlach, SK (WB)", "sensor_id": "319", "direction": "Westbound", "destination": "_mortlach_wb", "is_export": False, "cpkc_only": False},
    "mortlach_eb": {"name": "Mortlach, SK (EB)", "sensor_id": "319", "direction": "Eastbound", "destination": "_mortlach_eb", "is_export": False, "cpkc_only": False},
    "waldeck_wb": {"name": "Waldeck, SK (WB)", "sensor_id": "179", "direction": "Westbound", "destination": "_waldeck_wb", "is_export": False, "cpkc_only": False},
    "waldeck_eb": {"name": "Waldeck, SK (EB)", "sensor_id": "179", "direction": "Eastbound", "destination": "_waldeck_eb", "is_export": False, "cpkc_only": False},
}

CANADIAN_LOADING_INDICATORS = {
    '_rufus_eb', '_milaty_eb', '_craven_nb',
    '_zola_eb', '_mortlach_eb', '_waldeck_eb',
}
US_BORDER_ENTRY = {'_mcara_nb', '_moyie_nb', '_kevin_nb'}
EXPORT_DESTINATIONS = {'Vancouver', 'Portland', 'Thunder_Bay', 'US_Mcara', 'US_Kevin'}
DEST_PRIORITY = {'Vancouver': 5, 'Portland': 4, 'Thunder_Bay': 3, 'US_Mcara': 2, 'US_Kevin': 1}

print_lock = threading.Lock()


def safe_print(msg):
    with print_lock:
        print(msg, flush=True)


# ============================================================
# API FETCHING (identical to production)
# ============================================================

def fetch_data_for_url(url, headers, params=None):
    for attempt in range(RATE_LIMIT_MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 429:
                if attempt < RATE_LIMIT_MAX_RETRIES:
                    time.sleep(RATE_LIMIT_RETRY_DELAY * (2 ** attempt))
                    continue
                return None
            response.raise_for_status()
            return response.json()
        except Exception:
            if attempt < RATE_LIMIT_MAX_RETRIES:
                time.sleep(RATE_LIMIT_RETRY_DELAY)
                continue
            return None
    return None


def fetch_grain_cars_for_day(query_date, sensor_key, sensor_config):
    """Fetch grain car sightings for one day at one sensor."""
    all_cars = []
    sensor_id = sensor_config["sensor_id"]
    direction = sensor_config["direction"]

    pt_tz = pytz.timezone('America/Los_Angeles')
    utc_tz = pytz.utc

    local_day_start = pt_tz.localize(datetime.combine(query_date, datetime.min.time()))
    utc_start = local_day_start.astimezone(utc_tz).isoformat().replace('+00:00', 'Z')
    local_day_end = pt_tz.localize(datetime.combine(query_date + timedelta(days=1), datetime.min.time()))
    utc_end = local_day_end.astimezone(utc_tz).isoformat().replace('+00:00', 'Z')

    params = {
        'sensors': sensor_id,
        "detection_time_from": utc_start,
        "detection_time_to": utc_end,
        'response_size': MAX_RESULTS_PER_PAGE,
    }
    if direction:
        params['direction'] = direction

    headers = {'Authorization': f'Bearer {RAILSTATE_API_TOKEN}', 'Accept': 'application/json'}
    current_url = FULL_API_URL
    current_params = params
    page = 0

    while current_url and page < 100:
        page += 1
        content = fetch_data_for_url(current_url, headers, params=(current_params if page == 1 else None))
        if content is None:
            break

        if isinstance(content, dict):
            sightings = content.get('sightings', [])

            for item in sightings:
                if not isinstance(item, dict):
                    continue

                train_type = item.get('trainType', '')
                train_operator = item.get('trainOperator', '')
                detection_time = item.get('detectionTimeUTC', '')

                if train_type not in ['Grain Unit', 'Manifest']:
                    continue

                cars = item.get('cars', [])
                for car in cars:
                    if not isinstance(car, dict):
                        continue

                    car_id = car.get('carId', '')
                    car_type = car.get('type', '')

                    if car_type == 'Locomotive' or not car_id:
                        continue

                    equip_params = car.get('equipmentParameters', {}) or {}
                    type_code = equip_params.get('typeCode', '') if isinstance(equip_params, dict) else ''

                    cubic_capacity = 0
                    if isinstance(equip_params, dict):
                        dims = equip_params.get('dimensions', {}) or {}
                        if isinstance(dims, dict):
                            cubic_capacity = dims.get('cubicFeetCapacity', 0) or 0
                        if not cubic_capacity:
                            cubic_capacity = equip_params.get('cubicCapacity', 0) or 0
                        if not cubic_capacity:
                            cubic_capacity = equip_params.get('cubic_capacity', 0) or 0

                    if train_type == 'Manifest':
                        if not any(code in type_code.upper() for code in GRAIN_EQUIPMENT_CODES):
                            continue

                    is_c114 = 'C114' in type_code.upper()
                    is_c113 = 'C113' in type_code.upper()
                    is_grain_car = is_c114 or is_c113 or train_type == 'Grain Unit'

                    if not is_grain_car:
                        continue

                    all_cars.append({
                        'car_id': car_id,
                        'is_c114': is_c114,
                        'is_c113': is_c113,
                        'type_code': type_code.upper().strip(),
                        'cubic_capacity': float(cubic_capacity) if cubic_capacity else 0.0,
                        'train_type': train_type,
                        'train_operator': train_operator,
                        'destination': sensor_config['destination'],
                        'is_export': sensor_config['is_export'],
                        'cpkc_only': sensor_config['cpkc_only'],
                        'detection_time': detection_time,
                    })

            next_link = content.get('nextRequestLink')
            if next_link:
                current_url = urllib.parse.unquote(next_link)
                current_params = None
                time.sleep(0.05)
            else:
                current_url = None
        else:
            current_url = None

    return all_cars


def fetch_date_range_data(start_date, end_date):
    """Fetch all sensor data for a date range. Returns a list of car dicts."""
    all_cars = []
    total_days = (end_date - start_date).days + 1
    dates = [start_date + timedelta(days=i) for i in range(total_days)]

    safe_print(f"  Fetching {total_days} days x {len(CPKC_SENSORS)} sensors = {total_days * len(CPKC_SENSORS)} day-sensor combinations...")

    sensor_count = 0
    for sensor_key, sensor_config in CPKC_SENSORS.items():
        sensor_count += 1
        with ThreadPoolExecutor(max_workers=MAX_API_WORKERS) as executor:
            futures = {
                executor.submit(fetch_grain_cars_for_day, d, sensor_key, sensor_config): d
                for d in dates
            }
            for future in as_completed(futures):
                try:
                    cars = future.result()
                    if cars:
                        all_cars.extend(cars)
                except Exception:
                    pass
        time.sleep(0.3)
        if sensor_count % 5 == 0:
            safe_print(f"    ... {sensor_count}/{len(CPKC_SENSORS)} sensors ({len(all_cars):,} sightings)")

    safe_print(f"  Total sightings fetched: {len(all_cars):,}")
    return all_cars


# ============================================================
# V9.4 EXCLUSION LOGIC (identical to production)
# ============================================================

def identify_all_car_level_exclusions(df):
    """
    Full V8-style car-level exclusions for all destinations.
    Returns dict of sets of car_ids to exclude.
    """
    exclusions = {
        'us_transit_portland': set(),
        'portland_empty_roundtrips': set(),
        'portland_via_kevin_transit': set(),
        'mcara_us_origin': set(),
        'mcara_empty_returns': set(),
        'kevin_us_origin': set(),
        'kevin_empty_returns': set(),
    }

    if df.empty:
        return exclusions

    df = df.copy()
    df['detection_dt'] = pd.to_datetime(df['detection_time'], format='ISO8601')

    moyie_sb_cars = set(df[df['destination'] == 'Portland']['car_id'].unique())
    moyie_nb_cars = set(df[df['destination'] == '_moyie_nb']['car_id'].unique())
    mcara_sb_cars = set(df[df['destination'] == 'US_Mcara']['car_id'].unique())
    mcara_nb_cars = set(df[df['destination'] == '_mcara_nb']['car_id'].unique())
    kevin_sb_cars = set(df[df['destination'] == 'US_Kevin']['car_id'].unique())
    kevin_nb_cars = set(df[df['destination'] == '_kevin_nb']['car_id'].unique())

    # Portland Empty Round-trips
    for car_id in (moyie_nb_cars & moyie_sb_cars):
        car_df = df[df['car_id'] == car_id].sort_values('detection_dt')
        journey = list(car_df['destination'])
        nb_pos = [i for i, d in enumerate(journey) if d == '_moyie_nb']
        sb_pos = [i for i, d in enumerate(journey) if d == 'Portland']
        for np_ in nb_pos:
            next_sb = [p for p in sb_pos if p > np_]
            if next_sb:
                between = journey[np_:min(next_sb)]
                if not any(d in CANADIAN_LOADING_INDICATORS for d in between):
                    exclusions['portland_empty_roundtrips'].add(car_id)
                    break

    # US Transit to Portland
    for car_id in moyie_sb_cars:
        if car_id in exclusions['portland_empty_roundtrips']:
            continue
        car_df = df[df['car_id'] == car_id].sort_values('detection_dt')
        journey = list(car_df['destination'])
        for i, d in enumerate(journey):
            if d == 'Portland':
                pre = journey[:i]
                if any(d2 in US_BORDER_ENTRY for d2 in pre) and not any(d2 in CANADIAN_LOADING_INDICATORS for d2 in pre):
                    exclusions['us_transit_portland'].add(car_id)
                    break

    # Portland via Kevin Transit
    for car_id in (kevin_nb_cars & moyie_sb_cars):
        if car_id in exclusions['portland_empty_roundtrips'] or car_id in exclusions['us_transit_portland']:
            continue
        car_df = df[df['car_id'] == car_id].sort_values('detection_dt')
        journey = list(car_df['destination'])
        knb_pos = [i for i, d in enumerate(journey) if d == '_kevin_nb']
        msb_pos = [i for i, d in enumerate(journey) if d == 'Portland']
        for kp in knb_pos:
            next_msb = [p for p in msb_pos if p > kp]
            if next_msb:
                between = journey[kp:min(next_msb)]
                if not any(d in CANADIAN_LOADING_INDICATORS for d in between):
                    exclusions['portland_via_kevin_transit'].add(car_id)
                    break

    # Mcara US Origin
    for car_id in (mcara_nb_cars & mcara_sb_cars):
        car_df = df[df['car_id'] == car_id].sort_values('detection_dt')
        journey = list(car_df['destination'])
        nb_pos = [i for i, d in enumerate(journey) if d == '_mcara_nb']
        sb_pos = [i for i, d in enumerate(journey) if d == 'US_Mcara']
        for np_ in nb_pos:
            next_sb = [p for p in sb_pos if p > np_]
            if next_sb:
                between = journey[np_:min(next_sb)]
                if not any(d in CANADIAN_LOADING_INDICATORS for d in between):
                    exclusions['mcara_us_origin'].add(car_id)
                    break

    # Mcara Empty Returns
    for car_id in (moyie_nb_cars & mcara_sb_cars):
        if car_id in exclusions['mcara_us_origin']:
            continue
        car_df = df[df['car_id'] == car_id].sort_values('detection_dt')
        journey = list(car_df['destination'])
        mnb_pos = [i for i, d in enumerate(journey) if d == '_moyie_nb']
        msb_pos = [i for i, d in enumerate(journey) if d == 'US_Mcara']
        for mp in mnb_pos:
            next_sb = [p for p in msb_pos if p > mp]
            if next_sb:
                between = journey[mp:min(next_sb)]
                if not any(d in CANADIAN_LOADING_INDICATORS for d in between):
                    exclusions['mcara_empty_returns'].add(car_id)
                    break

    # Kevin US Origin
    for car_id in (kevin_nb_cars & kevin_sb_cars):
        car_df = df[df['car_id'] == car_id].sort_values('detection_dt')
        journey = list(car_df['destination'])
        nb_pos = [i for i, d in enumerate(journey) if d == '_kevin_nb']
        sb_pos = [i for i, d in enumerate(journey) if d == 'US_Kevin']
        for np_ in nb_pos:
            next_sb = [p for p in sb_pos if p > np_]
            if next_sb:
                between = journey[np_:min(next_sb)]
                if not any(d in CANADIAN_LOADING_INDICATORS for d in between):
                    exclusions['kevin_us_origin'].add(car_id)
                    break

    # Kevin Empty Returns
    for car_id in (moyie_nb_cars & kevin_sb_cars):
        if car_id in exclusions['kevin_us_origin']:
            continue
        car_df = df[df['car_id'] == car_id].sort_values('detection_dt')
        journey = list(car_df['destination'])
        mnb_pos = [i for i, d in enumerate(journey) if d == '_moyie_nb']
        ksb_pos = [i for i, d in enumerate(journey) if d == 'US_Kevin']
        for mp in mnb_pos:
            next_sb = [p for p in ksb_pos if p > mp]
            if next_sb:
                between = journey[mp:min(next_sb)]
                if not any(d in CANADIAN_LOADING_INDICATORS for d in between):
                    exclusions['kevin_empty_returns'].add(car_id)
                    break

    return exclusions


def count_trips_v9_4_daily(df, car_exclusions):
    """
    V9.4 counting with daily assignment.
    Returns dict: { "YYYY-MM-DD": {"carloads": N, "tonnes": N, "by_destination": {...}}, ... }
    """
    if df.empty:
        return {}

    df = df.copy()
    df['detection_dt'] = pd.to_datetime(df['detection_time'], format='ISO8601')

    # Step 1: Export sightings only
    export_df = df[df['is_export'] == True].copy()
    if export_df.empty:
        return {}

    # Step 2: Operator filtering
    cpkc_only_dests = ['Vancouver', 'Thunder_Bay', 'US_Mcara']
    interchange_dests = ['Portland', 'US_Kevin']

    cpkc_mask = (
        (export_df['destination'].isin(cpkc_only_dests)) &
        (export_df['train_operator'].str.upper().str.contains('CP|CPKC', na=False, regex=True))
    )
    interchange_mask = export_df['destination'].isin(interchange_dests)
    filtered_df = export_df[cpkc_mask | interchange_mask].copy()

    if filtered_df.empty:
        return {}

    # Collect excluded car_ids by destination
    portland_excluded = (
        car_exclusions['us_transit_portland'] |
        car_exclusions['portland_empty_roundtrips'] |
        car_exclusions['portland_via_kevin_transit']
    )
    mcara_excluded = car_exclusions['mcara_us_origin'] | car_exclusions['mcara_empty_returns']
    kevin_excluded = car_exclusions['kevin_us_origin'] | car_exclusions['kevin_empty_returns']

    # Step 3: Apply V8 car-level exclusions
    portland_excl_mask = (filtered_df['destination'] == 'Portland') & (filtered_df['car_id'].isin(portland_excluded))
    mcara_excl_mask = (filtered_df['destination'] == 'US_Mcara') & (filtered_df['car_id'].isin(mcara_excluded))
    kevin_excl_mask = (filtered_df['destination'] == 'US_Kevin') & (filtered_df['car_id'].isin(kevin_excluded))

    excluded_sightings = filtered_df[portland_excl_mask | mcara_excl_mask | kevin_excl_mask].copy()
    clean_df = filtered_df[~portland_excl_mask & ~mcara_excl_mask & ~kevin_excl_mask].copy()

    # Build full journey lookup for rescue
    all_sightings_by_car = {}
    for car_id, car_df in df.groupby('car_id'):
        all_sightings_by_car[car_id] = car_df.sort_values('detection_dt')

    # Daily results accumulator
    daily = defaultdict(lambda: {"carloads": 0, "tonnes": 0.0, "by_destination": defaultdict(int)})

    def process_trips(trip_df):
        """Build trips from export sightings. Returns list of (car_id, trip, primary_dest, first_sighting)."""
        trip_list = []
        for car_id, car_export in trip_df.groupby('car_id'):
            car_export = car_export.sort_values('detection_dt')
            trips = []
            current_trip = []
            for _, row in car_export.iterrows():
                if not current_trip:
                    current_trip = [row]
                else:
                    days_gap = (row['detection_dt'] - current_trip[-1]['detection_dt']).days
                    if days_gap >= TRIP_GAP_DAYS:
                        trips.append(current_trip)
                        current_trip = [row]
                    else:
                        current_trip.append(row)
            if current_trip:
                trips.append(current_trip)

            for trip in trips:
                trip_dests = set(s['destination'] for s in trip)
                if len(trip_dests) > 1:
                    primary_dest = max(trip_dests, key=lambda d: DEST_PRIORITY.get(d, 0))
                    primary_sightings = [s for s in trip if s['destination'] == primary_dest]
                    first_sighting = primary_sightings[0]
                else:
                    primary_dest = list(trip_dests)[0]
                    first_sighting = trip[0]
                trip_list.append((car_id, trip, primary_dest, first_sighting))
        return trip_list

    def assign_trip_to_day(first_sighting, primary_dest):
        """Assign a counted trip to the date of its first export sighting."""
        dt = first_sighting.get('detection_time', '')
        if dt:
            try:
                trip_date = pd.to_datetime(dt).date().isoformat()
                daily[trip_date]["carloads"] += 1
                daily[trip_date]["tonnes"] += AVG_TONNES_PER_CAR
                daily[trip_date]["by_destination"][primary_dest] += 1
            except Exception:
                pass

    # Phase 1: Count clean (non-excluded) trips
    clean_trips = process_trips(clean_df)
    for car_id, trip, primary_dest, first_sighting in clean_trips:
        assign_trip_to_day(first_sighting, primary_dest)

    # Phase 2: Rescue eligible trips from excluded cars
    if not excluded_sightings.empty:
        excluded_trips = process_trips(excluded_sightings)

        for car_id, trip, primary_dest, first_sighting in excluded_trips:
            trip_start = trip[0]['detection_dt']

            full_df = all_sightings_by_car.get(car_id)
            if full_df is None:
                continue

            full_journey = list(full_df['destination'])
            full_times = list(full_df['detection_dt'])

            # Find most recent export before this trip
            prev_export_time = None
            for d, t in zip(full_journey, full_times):
                if t >= trip_start:
                    break
                if d in EXPORT_DESTINATIONS:
                    prev_export_time = t

            # Check for Canadian loading between previous export and this trip
            if prev_export_time is not None:
                has_loading = any(
                    d in CANADIAN_LOADING_INDICATORS
                    for d, t in zip(full_journey, full_times)
                    if prev_export_time < t < trip_start
                )
            else:
                has_loading = any(
                    d in CANADIAN_LOADING_INDICATORS
                    for d, t in zip(full_journey, full_times)
                    if t < trip_start
                )

            if has_loading:
                assign_trip_to_day(first_sighting, primary_dest)

    # Convert defaultdicts to regular dicts
    result = {}
    for day_str, day_data in daily.items():
        result[day_str] = {
            "carloads": day_data["carloads"],
            "tonnes": day_data["tonnes"],
            "by_destination": dict(day_data["by_destination"]),
        }

    return result


# ============================================================
# DATA PERSISTENCE
# ============================================================

def load_daily_data():
    """Load existing daily data file, or return None if not found."""
    if os.path.exists(DAILY_DATA_FILE):
        try:
            with open(DAILY_DATA_FILE, 'r') as f:
                data = json.load(f)
            safe_print(f"  Loaded existing data: {len(data.get('days', {}))} days, last updated {data.get('last_updated', 'unknown')}")
            return data
        except Exception as e:
            safe_print(f"  Warning: Could not load existing data ({e}), starting fresh")
            return None
    return None


def save_daily_data(data):
    """Save daily data to JSON file (scripts/ and data/ directories)."""
    with open(DAILY_DATA_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    # Also write to data/ directory for the dashboard to fetch
    data_dir_copy = os.path.join(SCRIPT_DIR, '..', 'data', 'daily_90day_data.json')
    with open(data_dir_copy, 'w') as f:
        json.dump(data, f, indent=2)
    safe_print(f"  Saved data: {len(data.get('days', {}))} days")


def trim_to_rolling_window(data, today):
    """Remove days older than ROLLING_DAYS from the data."""
    cutoff = (today - timedelta(days=ROLLING_DAYS)).isoformat()
    days = data.get('days', {})
    trimmed = {k: v for k, v in days.items() if k >= cutoff}
    removed = len(days) - len(trimmed)
    if removed > 0:
        safe_print(f"  Trimmed {removed} days older than {cutoff}")
    data['days'] = trimmed
    return data


# ============================================================
# MAIN DATA PIPELINE
# ============================================================

def run_full_fetch(today):
    """First run: fetch 90 days of data."""
    start_date = today - timedelta(days=ROLLING_DAYS - 1)
    end_date = today - timedelta(days=1)  # Through yesterday

    safe_print(f"\n  FIRST RUN: Fetching {ROLLING_DAYS} days ({start_date} to {end_date})")
    safe_print(f"  This will take several minutes...\n")

    raw_cars = fetch_date_range_data(start_date, end_date)

    if not raw_cars:
        safe_print("  WARNING: No data fetched. Check API token and connectivity.")
        return {"last_updated": today.isoformat(), "days": {}}

    df = pd.DataFrame(raw_cars)
    safe_print(f"\n  Running V9.4 exclusion logic on {len(df):,} sightings...")

    car_exclusions = identify_all_car_level_exclusions(df)

    total_excluded_cars = sum(len(v) for v in car_exclusions.values())
    safe_print(f"  Car-level exclusions: {total_excluded_cars} cars flagged")

    daily_results = count_trips_v9_4_daily(df, car_exclusions)

    total_carloads = sum(d['carloads'] for d in daily_results.values())
    safe_print(f"  V9.4 results: {total_carloads:,} carloads across {len(daily_results)} days")

    data = {
        "last_updated": today.isoformat(),
        "days": daily_results,
    }
    return data


def run_incremental_fetch(existing_data, today):
    """Subsequent run: fetch only new days since last update."""
    last_updated = existing_data.get('last_updated', '')
    if not last_updated:
        safe_print("  No last_updated found, doing full fetch")
        return run_full_fetch(today)

    last_date = date.fromisoformat(last_updated)
    yesterday = today - timedelta(days=1)

    if last_date >= today:
        safe_print(f"  Already up to date (last updated: {last_updated})")
        return existing_data

    # Fetch from last_updated through yesterday
    # We need some overlap for trip building context, so fetch a wider window
    # but only record days from last_updated onwards
    context_start = last_date - timedelta(days=TRIP_GAP_DAYS + 1)
    fetch_start = last_date
    fetch_end = yesterday

    new_days_count = (fetch_end - fetch_start).days + 1
    safe_print(f"\n  INCREMENTAL: Fetching {new_days_count} new day(s) ({fetch_start} to {fetch_end})")
    safe_print(f"  (With context window from {context_start} for trip building)\n")

    raw_cars = fetch_date_range_data(context_start, fetch_end)

    if not raw_cars:
        safe_print("  WARNING: No data fetched for incremental update")
        existing_data['last_updated'] = today.isoformat()
        return existing_data

    df = pd.DataFrame(raw_cars)
    safe_print(f"\n  Running V9.4 exclusion logic on {len(df):,} sightings...")

    car_exclusions = identify_all_car_level_exclusions(df)
    daily_results = count_trips_v9_4_daily(df, car_exclusions)

    # Merge: only update days from fetch_start onwards
    days = existing_data.get('days', {})
    new_count = 0
    for day_str, day_data in daily_results.items():
        if day_str >= fetch_start.isoformat():
            days[day_str] = day_data
            new_count += 1

    safe_print(f"  Updated {new_count} day(s) in data store")

    existing_data['days'] = days
    existing_data['last_updated'] = today.isoformat()
    return existing_data


# ============================================================
# MONTHLY HISTORY (from production results file)
# ============================================================

def load_monthly_history():
    """Load monthly history from the production results JSON if available."""
    if not os.path.exists(PRODUCTION_RESULTS_FILE):
        safe_print(f"  Note: No production results file found at {PRODUCTION_RESULTS_FILE}")
        return []

    try:
        with open(PRODUCTION_RESULTS_FILE, 'r') as f:
            prod = json.load(f)
        months = []
        for m in prod.get('monthly_results', []):
            months.append({
                'label': f"{calendar.month_abbr[m['month']]} {m['year']}",
                'carloads': m['total_trips'],
                'tonnes': m.get('tonnes', m['total_trips'] * AVG_TONNES_PER_CAR),
                'year': m['year'],
                'month': m['month'],
            })
        safe_print(f"  Loaded {len(months)} months of history from production results")
        return months
    except Exception as e:
        safe_print(f"  Warning: Could not load production results ({e})")
        return []


# ============================================================
# HTML DASHBOARD GENERATION
# ============================================================

def generate_dashboard(daily_data, monthly_history):
    """Generate the self-contained HTML dashboard."""
    days = daily_data.get('days', {})
    sorted_days = sorted(days.keys())

    if not sorted_days:
        safe_print("  No daily data to chart")
        return

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')

    # Compute summary stats
    carloads_list = [days[d]['carloads'] for d in sorted_days]
    total_90 = sum(carloads_list)

    # 7-day and 30-day averages
    last_7 = carloads_list[-7:] if len(carloads_list) >= 7 else carloads_list
    last_30 = carloads_list[-30:] if len(carloads_list) >= 30 else carloads_list
    avg_7 = sum(last_7) / len(last_7) if last_7 else 0
    avg_30 = sum(last_30) / len(last_30) if last_30 else 0
    yesterday_count = carloads_list[-1] if carloads_list else 0
    yesterday_date = sorted_days[-1] if sorted_days else "N/A"

    # Moving averages for chart
    ma7 = []
    ma30 = []
    for i in range(len(carloads_list)):
        # 7-day MA
        window7 = carloads_list[max(0, i - 6):i + 1]
        ma7.append(sum(window7) / len(window7))
        # 30-day MA
        window30 = carloads_list[max(0, i - 29):i + 1]
        ma30.append(sum(window30) / len(window30))

    max_daily = max(carloads_list) if carloads_list else 1
    max_ma = max(max(ma7), max(ma30)) if ma7 else max_daily
    chart_max = max(max_daily, max_ma) * 1.1

    # Monthly history chart
    max_monthly = max(m['carloads'] for m in monthly_history) if monthly_history else 1

    # ---- Build HTML ----
    html = []
    html.append(f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CPKC Canadian Grain - Daily Monitoring Dashboard</title>
<style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        color: #2c3e50;
        background: #fff;
        line-height: 1.5;
        font-size: 13px;
    }}
    .container {{ max-width: 1200px; margin: 0 auto; padding: 20px 30px; }}
    .header {{
        background: #1a2744;
        color: #fff;
        padding: 28px 40px;
        margin-bottom: 0;
    }}
    .header h1 {{ font-size: 22px; font-weight: 600; margin-bottom: 4px; }}
    .header .subtitle {{ font-size: 13px; color: #8fa4c4; margin-bottom: 2px; }}
    .header .date {{ font-size: 12px; color: #6b83a8; }}
    .section {{
        margin-bottom: 28px;
        page-break-inside: avoid;
    }}
    .section h2 {{
        font-size: 16px;
        color: #1a2744;
        border-bottom: 2px solid #1a2744;
        padding-bottom: 5px;
        margin-bottom: 12px;
        font-weight: 600;
    }}
    .summary-cards {{
        display: flex;
        gap: 15px;
        flex-wrap: wrap;
        margin-bottom: 18px;
    }}
    .card {{
        background: #f7f9fc;
        border: 1px solid #d5dde8;
        border-radius: 6px;
        padding: 14px 18px;
        flex: 1;
        min-width: 160px;
    }}
    .card .label {{ font-size: 11px; color: #6b83a8; text-transform: uppercase; letter-spacing: 0.5px; }}
    .card .value {{ font-size: 22px; font-weight: 700; color: #1a2744; margin-top: 2px; }}
    .card .sub {{ font-size: 11px; color: #7f8c8d; margin-top: 2px; }}
    .chart-container {{
        background: #fafbfd;
        border: 1px solid #e0e5ec;
        border-radius: 6px;
        padding: 20px;
        margin-bottom: 18px;
        overflow-x: auto;
    }}
    .chart-title {{
        font-size: 14px;
        font-weight: 600;
        color: #1a2744;
        margin-bottom: 12px;
    }}
    table {{
        width: 100%;
        border-collapse: collapse;
        margin-bottom: 14px;
        font-size: 12px;
    }}
    th {{
        background: #1a2744;
        color: #fff;
        padding: 7px 10px;
        text-align: left;
        font-weight: 500;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.3px;
    }}
    th.num, td.num {{ text-align: right; }}
    td {{
        padding: 6px 10px;
        border-bottom: 1px solid #e8ecf1;
    }}
    tr:nth-child(even) {{ background: #f9fafb; }}
    tr:hover {{ background: #eef2f7; }}
    .legend {{
        display: flex;
        gap: 20px;
        margin-bottom: 8px;
        font-size: 12px;
    }}
    .legend-item {{
        display: flex;
        align-items: center;
        gap: 5px;
    }}
    .legend-swatch {{
        width: 14px;
        height: 3px;
        border-radius: 1px;
    }}
    .footer {{
        margin-top: 30px;
        padding-top: 14px;
        border-top: 1px solid #d5dde8;
        font-size: 11px;
        color: #95a5a6;
    }}
    @media print {{
        .container {{ padding: 10px; }}
        .header {{ padding: 20px; }}
        body {{ font-size: 11px; }}
    }}
</style>
</head>
<body>

<div class="header">
    <h1>CPKC Canadian Grain &mdash; Daily Monitoring Dashboard</h1>
    <div class="subtitle">V9.4 Methodology &mdash; V8 Car-Level Exclusions + Trip-Level Rescue</div>
    <div class="date">Last updated: {now_str}</div>
</div>

<div class="container">
""")

    # Summary cards
    html.append(f"""
<div class="section">
    <h2>Summary Statistics</h2>
    <div class="summary-cards">
        <div class="card">
            <div class="label">90-Day Total</div>
            <div class="value">{total_90:,}</div>
            <div class="sub">{total_90 * AVG_TONNES_PER_CAR:,.0f} tonnes</div>
        </div>
        <div class="card">
            <div class="label">7-Day Average</div>
            <div class="value">{avg_7:,.0f}</div>
            <div class="sub">carloads/day</div>
        </div>
        <div class="card">
            <div class="label">30-Day Average</div>
            <div class="value">{avg_30:,.0f}</div>
            <div class="sub">carloads/day</div>
        </div>
        <div class="card">
            <div class="label">Yesterday ({yesterday_date})</div>
            <div class="value">{yesterday_count:,}</div>
            <div class="sub">carloads</div>
        </div>
    </div>
</div>
""")

    # ---- Chart 1: Monthly Historical ----
    if monthly_history:
        bar_height = 22
        gap = 4
        label_width = 80
        value_width = 60
        bar_area_width = 600
        svg_width = label_width + bar_area_width + value_width + 20
        svg_height = len(monthly_history) * (bar_height + gap) + 30

        html.append(f"""
<div class="section">
    <h2>Monthly Historical</h2>
    <div class="chart-container">
        <div class="chart-title">Monthly Carloads (V9.4) &mdash; Jan 2025 to Present</div>
        <svg width="{svg_width}" height="{svg_height}" xmlns="http://www.w3.org/2000/svg">
""")
        for i, m in enumerate(monthly_history):
            y = i * (bar_height + gap) + 10
            bar_w = (m['carloads'] / max_monthly) * bar_area_width if max_monthly > 0 else 0
            # Color: blue for 2025, darker for 2026
            color = "#3b6cb5" if m['year'] == 2025 else "#1a2744"
            html.append(f'            <text x="{label_width - 5}" y="{y + bar_height * 0.7}" text-anchor="end" font-size="11" fill="#2c3e50">{m["label"]}</text>\n')
            html.append(f'            <rect x="{label_width}" y="{y}" width="{bar_w:.1f}" height="{bar_height}" fill="{color}" rx="2"/>\n')
            html.append(f'            <text x="{label_width + bar_w + 5}" y="{y + bar_height * 0.7}" font-size="11" fill="#555">{m["carloads"]:,}</text>\n')

        html.append("""        </svg>
    </div>
</div>
""")

    # ---- Chart 2: Daily 90-Day Rolling ----
    chart_w = 900
    chart_h = 300
    pad_left = 55
    pad_right = 20
    pad_top = 20
    pad_bottom = 60
    plot_w = chart_w - pad_left - pad_right
    plot_h = chart_h - pad_top - pad_bottom
    n_days = len(sorted_days)
    bar_w_raw = plot_w / max(n_days, 1)
    bar_w = max(bar_w_raw * 0.7, 1)
    bar_gap = bar_w_raw * 0.3

    svg_h = chart_h + 10

    html.append(f"""
<div class="section">
    <h2>Daily 90-Day Rolling</h2>
    <div class="chart-container">
        <div class="chart-title">Daily Carloads with Moving Averages</div>
        <div class="legend">
            <div class="legend-item"><div class="legend-swatch" style="background:#a8c4e0;height:12px;width:12px;"></div> Daily carloads</div>
            <div class="legend-item"><div class="legend-swatch" style="background:#2166ac;"></div> 7-day moving average</div>
            <div class="legend-item"><div class="legend-swatch" style="background:#c0392b;"></div> 30-day moving average</div>
        </div>
        <svg width="{chart_w}" height="{svg_h}" xmlns="http://www.w3.org/2000/svg">
""")

    # Y-axis gridlines and labels
    y_ticks = 5
    for i in range(y_ticks + 1):
        y_val = chart_max * i / y_ticks
        y_px = pad_top + plot_h - (plot_h * i / y_ticks)
        html.append(f'            <line x1="{pad_left}" y1="{y_px:.1f}" x2="{chart_w - pad_right}" y2="{y_px:.1f}" stroke="#e0e5ec" stroke-width="0.5"/>\n')
        html.append(f'            <text x="{pad_left - 5}" y="{y_px + 4:.1f}" text-anchor="end" font-size="10" fill="#888">{int(y_val):,}</text>\n')

    # Bars
    for i, day_str in enumerate(sorted_days):
        val = carloads_list[i]
        x = pad_left + i * bar_w_raw + bar_gap / 2
        bar_h = (val / chart_max) * plot_h if chart_max > 0 else 0
        y = pad_top + plot_h - bar_h
        html.append(f'            <rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{bar_h:.1f}" fill="#a8c4e0" rx="1"/>\n')

    # 7-day MA line
    points_7 = []
    for i in range(n_days):
        x = pad_left + i * bar_w_raw + bar_w_raw / 2
        y = pad_top + plot_h - (ma7[i] / chart_max) * plot_h if chart_max > 0 else pad_top + plot_h
        points_7.append(f"{x:.1f},{y:.1f}")
    if points_7:
        html.append(f'            <polyline points="{" ".join(points_7)}" fill="none" stroke="#2166ac" stroke-width="2" stroke-linejoin="round"/>\n')

    # 30-day MA line
    points_30 = []
    for i in range(n_days):
        x = pad_left + i * bar_w_raw + bar_w_raw / 2
        y = pad_top + plot_h - (ma30[i] / chart_max) * plot_h if chart_max > 0 else pad_top + plot_h
        points_30.append(f"{x:.1f},{y:.1f}")
    if points_30:
        html.append(f'            <polyline points="{" ".join(points_30)}" fill="none" stroke="#c0392b" stroke-width="2" stroke-linejoin="round" stroke-dasharray="6,3"/>\n')

    # X-axis date labels (every ~7 days)
    label_interval = max(1, n_days // 12)
    for i in range(0, n_days, label_interval):
        x = pad_left + i * bar_w_raw + bar_w_raw / 2
        y_label = pad_top + plot_h + 15
        # Format date as MM/DD
        try:
            d = date.fromisoformat(sorted_days[i])
            label = d.strftime('%m/%d')
        except Exception:
            label = sorted_days[i][-5:]
        html.append(f'            <text x="{x:.1f}" y="{y_label:.1f}" text-anchor="middle" font-size="9" fill="#888" transform="rotate(-45 {x:.1f} {y_label:.1f})">{label}</text>\n')

    # Axes
    html.append(f'            <line x1="{pad_left}" y1="{pad_top}" x2="{pad_left}" y2="{pad_top + plot_h}" stroke="#2c3e50" stroke-width="1"/>\n')
    html.append(f'            <line x1="{pad_left}" y1="{pad_top + plot_h}" x2="{chart_w - pad_right}" y2="{pad_top + plot_h}" stroke="#2c3e50" stroke-width="1"/>\n')

    html.append("""        </svg>
    </div>
</div>
""")

    # ---- Data Table ----
    html.append("""
<div class="section">
    <h2>Daily Data Table</h2>
    <table>
        <thead>
            <tr>
                <th>Date</th>
                <th class="num">Carloads</th>
                <th class="num">Tonnes</th>
                <th class="num">7-Day MA</th>
                <th class="num">30-Day MA</th>
                <th class="num">Vancouver</th>
                <th class="num">Portland</th>
                <th class="num">Thunder Bay</th>
                <th class="num">US Mcara</th>
                <th class="num">US Kevin</th>
            </tr>
        </thead>
        <tbody>
""")

    # Show most recent first
    for i in range(len(sorted_days) - 1, -1, -1):
        day_str = sorted_days[i]
        d = days[day_str]
        by_dest = d.get('by_destination', {})
        html.append(f"""            <tr>
                <td>{day_str}</td>
                <td class="num">{d['carloads']:,}</td>
                <td class="num">{d['tonnes']:,.0f}</td>
                <td class="num">{ma7[i]:,.0f}</td>
                <td class="num">{ma30[i]:,.0f}</td>
                <td class="num">{by_dest.get('Vancouver', 0):,}</td>
                <td class="num">{by_dest.get('Portland', 0):,}</td>
                <td class="num">{by_dest.get('Thunder_Bay', 0):,}</td>
                <td class="num">{by_dest.get('US_Mcara', 0):,}</td>
                <td class="num">{by_dest.get('US_Kevin', 0):,}</td>
            </tr>
""")

    html.append("""        </tbody>
    </table>
</div>
""")

    # Footer
    html.append(f"""
<div class="footer">
    <p>Generated by CPKC Canadian Grain Daily Monitor (V9.4) on {now_str}.</p>
    <p>Data source: RailState API. Methodology: V9.4 Hybrid (V8 car-level exclusions + trip-level rescue).</p>
    <p>Rolling window: {ROLLING_DAYS} days. Trip gap threshold: {TRIP_GAP_DAYS} days. Tonnage: {AVG_TONNES_PER_CAR:.0f} tonnes/carload.</p>
</div>

</div>
</body>
</html>
""")

    html_content = "".join(html)
    with open(HTML_OUTPUT_FILE, 'w') as f:
        f.write(html_content)
    safe_print(f"  Dashboard written to: {HTML_OUTPUT_FILE}")


# ============================================================
# MAIN
# ============================================================

def main():
    today = date.today()

    print("=" * 70)
    print("  CPKC Canadian Grain - Daily 90-Day Rolling Monitor")
    print(f"  V9.4 Methodology | {today.isoformat()}")
    print("=" * 70)

    # Step 1: Check for existing data
    existing = load_daily_data()

    if existing is None:
        # First run
        print("\n  No existing data found. Running FULL 90-day fetch...")
        data = run_full_fetch(today)
    else:
        # Incremental
        print("\n  Existing data found. Running INCREMENTAL update...")
        data = run_incremental_fetch(existing, today)

    # Step 2: Trim to rolling window
    data = trim_to_rolling_window(data, today)

    # Step 3: Save
    save_daily_data(data)

    # Step 4: Load monthly history
    print("\n  Loading monthly history...")
    monthly_history = load_monthly_history()

    # Step 5: Generate dashboard
    print("\n  Generating HTML dashboard...")
    generate_dashboard(data, monthly_history)

    # Summary
    days = data.get('days', {})
    total = sum(d['carloads'] for d in days.values())
    print("\n" + "=" * 70)
    print(f"  COMPLETE")
    print(f"  Days in rolling window: {len(days)}")
    print(f"  Total carloads (90-day): {total:,}")
    print(f"  Total tonnes (90-day): {total * AVG_TONNES_PER_CAR:,.0f}")
    print(f"  Data file: {DAILY_DATA_FILE}")
    print(f"  Dashboard: {HTML_OUTPUT_FILE}")
    print("=" * 70)


if __name__ == '__main__':
    main()
