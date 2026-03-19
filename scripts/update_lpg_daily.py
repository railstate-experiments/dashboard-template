#!/usr/bin/env python3
"""Pull daily LPG (UN1075) car counts from RailState sensors for last 180 days.

Outputs data/lpg_xb_daily.json with daily car counts and BBL estimates.
Applies transit removal for Ste Anne/Devlin ↔ Port Huron corridor.
"""

import requests
import time
import os
import re
import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

BENCH_DIR = Path(__file__).parent.parent
DATA_DIR = BENCH_DIR / 'data'

API_BASE_URL = os.environ.get('RAILSTATE_API_BASE', 'https://api.railstate.com')
API_KEY = os.environ.get('RAILSTATE_API_KEY', '')
if not API_KEY:
    config_path = Path(__file__).parent.parent.parent / 'config.env'
    if config_path.exists():
        with open(config_path) as f:
            for line in f:
                if line.startswith('RAILSTATE_API_KEY='):
                    API_KEY = line.split('=', 1)[1].strip()

HEADERS = {'Authorization': f'Bearer {API_KEY}', 'Accept': 'application/json'}
AVG_BBL_PER_CAR = 700.8  # Fleet weighted average at 87.5% fill

# Sensors and counted directions
BORDER_SENSORS = [
    ('Blaine, WA', 'southbound'),
    ('Port Huron, MI', 'westbound'),
    ('Ste Anne, MB', 'eastbound'),
    ('Moyie Springs, ID', 'southbound'),
    ('Mcara, SK', 'southbound'),
    ('Kevin, MT', 'southbound'),
    ('Letellier, MB', 'southbound'),
    ('Rouses Point, NY', 'southbound'),
    ('Massena, NY', 'westbound'),
    ('Island Pond, VT', 'eastbound'),
    ('Windsor TFR, ON', 'westbound'),
]

# Transit detection sensors — need all directions
TRANSIT_SENSORS = [
    ('Ste Anne, MB', ['eastbound', 'westbound']),
    ('Devlin, ON', ['eastbound', 'westbound']),
    ('Port Huron, MI', ['westbound', 'eastbound']),
]

TRANSIT_WINDOW_DAYS = 7

def normalize_car_id(cid):
    return re.sub(r'\s+', ' ', str(cid).strip().upper())

def get_placard(car):
    for h in (car.get('hazmats') or []):
        p = h.get('placardType')
        if p and p not in ['None', 'EMPTY']:
            return p
    return None

def parse_time(t):
    try:
        return datetime.strptime(t[:19], '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        return datetime.strptime(t[:10], '%Y-%m-%d')

def fetch_sensor_data(sensor_name, directions, sensor_ids, start, end):
    """Fetch UN1075 car sightings for a sensor. Returns list of dicts."""
    sid = sensor_ids.get(sensor_name)
    if not sid:
        return []

    results = []
    url = f'{API_BASE_URL}/api/v3/trains/full_sightings'
    params = {
        'sensors': str(sid),
        'detection_time_from': start.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'detection_time_to': end.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'response_size': 500,
    }

    while True:
        time.sleep(2.0)
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=120)
            if r.status_code != 200:
                break
        except requests.RequestException:
            break
        data = r.json()
        sightings = data.get('sightings', [])
        if not sightings:
            break

        for s in sightings:
            direction = (s.get('direction', '') or '').lower()
            if direction not in [d.lower() for d in directions]:
                continue

            det_time = s.get('detectionTimeUTC', '')
            if not det_time:
                continue
            date = det_time[:10]

            for car in s.get('cars', []):
                if 'tank' not in (car.get('type', '') or '').lower():
                    continue
                if get_placard(car) != 'UN1075':
                    continue

                car_id = car.get('carId', '')
                ep = car.get('equipmentParameters') or {}
                dims = ep.get('dimensions') or {}
                gallons = dims.get('gallonageCapacity')
                bbl = (gallons * 0.875 / 42) if gallons and gallons > 0 else AVG_BBL_PER_CAR

                results.append({
                    'car_id': normalize_car_id(car_id) if car_id else '',
                    'direction': direction,
                    'time': det_time,
                    'date': date,
                    'sensor': sensor_name,
                    'bbl': bbl,
                })

        next_link = data.get('nextRequestLink')
        if not next_link:
            break
        url = next_link
        params = None

    return results


def detect_transits(all_sightings):
    """Detect Canada-to-Canada transit car-date pairs to remove."""
    # Group by car_id
    by_car = defaultdict(list)
    for rec in all_sightings:
        if rec['car_id']:
            by_car[rec['car_id']].append(rec)

    sa_devlin_names = {'Ste Anne, MB', 'Devlin, ON'}
    transit_dates = defaultdict(int)  # date → count to remove
    transit_bbl = defaultdict(float)

    for car_id, sightings in by_car.items():
        sightings.sort(key=lambda x: x['time'])
        sa_eb = [s for s in sightings if s['sensor'] in sa_devlin_names and s['direction'] == 'eastbound']
        sa_wb = [s for s in sightings if s['sensor'] in sa_devlin_names and s['direction'] == 'westbound']
        ph_eb = [s for s in sightings if s['sensor'] == 'Port Huron, MI' and s['direction'] == 'eastbound']
        ph_wb = [s for s in sightings if s['sensor'] == 'Port Huron, MI' and s['direction'] == 'westbound']

        # EB transit: SA/Devlin EB → PH EB within 7 days (remove from SA EB date)
        for s1 in sa_eb:
            t1 = parse_time(s1['time'])
            for s2 in ph_eb:
                t2 = parse_time(s2['time'])
                delta = (t2 - t1).total_seconds() / 86400
                if 0 < delta <= TRANSIT_WINDOW_DAYS:
                    transit_dates[s1['date']] += 1
                    transit_bbl[s1['date']] += s1['bbl']
                    break

        # WB transit: PH WB → SA/Devlin WB within 7 days (remove from PH WB date)
        for s1 in ph_wb:
            t1 = parse_time(s1['time'])
            for s2 in sa_wb:
                t2 = parse_time(s2['time'])
                delta = (t2 - t1).total_seconds() / 86400
                if 0 < delta <= TRANSIT_WINDOW_DAYS:
                    transit_dates[s1['date']] += 1
                    transit_bbl[s1['date']] += s1['bbl']
                    break

    return dict(transit_dates), dict(transit_bbl)


def main():
    print('LPG Cross-Border Daily Update')
    print('=' * 50)

    # Get sensor IDs
    print('Fetching sensor IDs...', flush=True)
    resp = requests.get(f'{API_BASE_URL}/api/v3/sensors/overview', headers=HEADERS, timeout=60)
    sensor_ids = {s['name']: s['sensorId'] for s in resp.json().get('sensors', [])}

    # Date range: last 180 days + 7 day buffer for transit matching
    end = datetime.utcnow()
    start = end - timedelta(days=187)  # Extra 7 days for transit window

    # Fetch border sensor data (counted directions only)
    print(f'\nFetching {len(BORDER_SENSORS)} border sensors ({start.date()} to {end.date()})...')
    daily_cars = defaultdict(int)
    daily_bbl = defaultdict(float)
    all_transit_sightings = []

    for sensor_name, direction in BORDER_SENSORS:
        print(f'  {sensor_name} ({direction})...', end=' ', flush=True)
        data = fetch_sensor_data(sensor_name, [direction], sensor_ids, start, end)
        for rec in data:
            daily_cars[rec['date']] += 1
            daily_bbl[rec['date']] += rec['bbl']
        print(f'{len(data)} cars')

        # Collect for transit detection
        if sensor_name in ('Ste Anne, MB', 'Port Huron, MI'):
            all_transit_sightings.extend(data)

    # Fetch transit detection sensors (all directions)
    print('\nFetching transit detection sensors...')
    for sensor_name, directions in TRANSIT_SENSORS:
        # Skip directions we already fetched for border counting
        already_fetched = set()
        for bn, bd in BORDER_SENSORS:
            if bn == sensor_name:
                already_fetched.add(bd.lower())

        extra_dirs = [d for d in directions if d.lower() not in already_fetched]
        if extra_dirs:
            print(f'  {sensor_name} ({"/".join(extra_dirs)})...', end=' ', flush=True)
            data = fetch_sensor_data(sensor_name, extra_dirs, sensor_ids, start, end)
            all_transit_sightings.extend(data)
            print(f'{len(data)} cars')
        elif sensor_name == 'Devlin, ON':
            print(f'  {sensor_name} ({"/".join(directions)})...', end=' ', flush=True)
            data = fetch_sensor_data(sensor_name, directions, sensor_ids, start, end)
            all_transit_sightings.extend(data)
            print(f'{len(data)} cars')

    # Detect transits
    print('\nDetecting Canada-to-Canada transits...', end=' ', flush=True)
    transit_dates, transit_bbl_remove = detect_transits(all_transit_sightings)
    total_transit = sum(transit_dates.values())
    print(f'{total_transit} transit cars detected')

    # Apply transit removal and build output
    output_start = (end - timedelta(days=180)).strftime('%Y-%m-%d')
    dates = sorted(d for d in daily_cars.keys() if d >= output_start)

    out_dates = []
    out_cars = []
    out_bbl = []

    for d in dates:
        adj_cars = max(0, daily_cars[d] - transit_dates.get(d, 0))
        adj_bbl = max(0, daily_bbl[d] - transit_bbl_remove.get(d, 0))
        out_dates.append(d)
        out_cars.append(adj_cars)
        out_bbl.append(round(adj_bbl))

    # Write output
    output = {
        'description': 'LPG cross-border daily volumes (Canada → US rail exports)',
        'last_updated': datetime.now().strftime('%Y-%m-%d'),
        'dates': out_dates,
        'total_cars': out_cars,
        'total_barrels': out_bbl,
    }

    out_path = DATA_DIR / 'lpg_xb_daily.json'
    with open(out_path, 'w') as f:
        json.dump(output, f, indent=2)

    print(f'\nWrote {out_path} ({len(out_dates)} days)')
    print(f'Date range: {out_dates[0]} to {out_dates[-1]}')
    print(f'Total cars: {sum(out_cars):,} (after transit removal)')
    print(f'Total BBL: {sum(out_bbl):,}')
    avg_daily_cars = sum(out_cars) / len(out_cars) if out_cars else 0
    print(f'Avg daily: {avg_daily_cars:.0f} cars / {avg_daily_cars * AVG_BBL_PER_CAR:,.0f} BBL')


if __name__ == '__main__':
    main()
