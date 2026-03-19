#!/usr/bin/env python3
"""Generate LPG cross-border monthly JSON from existing Border Crossing analysis outputs.

Reads:
  - lpg_railstate_monthly.csv (RS car counts)
  - lpg_car_sizes.json (weighted BBL)
  - lpg_transit_removal.json (transit cars to subtract)
  - cer_lpg_by_mode.csv (CER government data)
  - lpg_railstate_detail.json (sensor-level detail)

Writes:
  - data/lpg_cross_border.json (monthly comparison)
  - data/lpg_sensor_volumes.json (sensor-level for map)
"""

import csv
import json
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BENCH_DIR = Path(__file__).parent.parent
DATA_DIR = BENCH_DIR / 'data'
BC_OUTPUT = Path(__file__).parent.parent.parent / 'Border Crossing' / 'output'

def load_csv(path):
    with open(path) as f:
        return list(csv.DictReader(f))

# --- Load all inputs ---
rs_data = load_csv(BC_OUTPUT / 'lpg_railstate_monthly.csv')
cer_data = load_csv(BC_OUTPUT / 'cer_lpg_by_mode.csv')

with open(BC_OUTPUT / 'lpg_car_sizes.json') as f:
    car_sizes = json.load(f)

with open(BC_OUTPUT / 'lpg_transit_removal.json') as f:
    transit = json.load(f)

with open(BC_OUTPUT / 'lpg_railstate_detail.json') as f:
    rs_detail = json.load(f)

AVG_BBL = car_sizes['avg_bbl_per_car']  # 700.8
weighted_monthly_bbl = car_sizes['monthly_weighted_bbl']
transit_monthly = transit['monthly_transit_remove']

# --- CER rail by month ---
cer_rail = defaultdict(float)
for r in cer_data:
    if r['mode'] == 'Railway':
        cer_rail[r['period']] += float(r['volume_bbl'])

# --- RS by month ---
rs_by_month = {}
for r in rs_data:
    rs_by_month[r['month']] = int(r['total'])

# --- Build monthly comparison (from March 2024) ---
all_months = sorted(set(list(rs_by_month.keys()) + list(cer_rail.keys())))
all_months = [m for m in all_months if m >= '2024-03']

monthly_rows = []
for m in all_months:
    rs_raw = rs_by_month.get(m, 0)
    transit_remove = transit_monthly.get(m, 0)
    rs_adj = rs_raw - transit_remove

    # Weighted BBL adjusted proportionally
    raw_bbl = weighted_monthly_bbl.get(m, rs_raw * AVG_BBL)
    adj_ratio = rs_adj / rs_raw if rs_raw > 0 else 1
    rs_bbl_k = round(raw_bbl * adj_ratio / 1000)

    cer_bbl = cer_rail.get(m, 0)
    cer_bbl_k = round(cer_bbl / 1000) if cer_bbl > 0 else None

    monthly_rows.append([m, rs_bbl_k, cer_bbl_k])

# --- Build sensor volumes for map ---
sensor_configs = [
    ('Blaine, WA', 'southbound', 48.99, -122.75),
    ('Port Huron, MI', 'westbound', 42.97, -82.43),
    ('Ste Anne, MB', 'eastbound', 49.64, -96.57),
    ('Moyie Springs, ID', 'southbound', 48.73, -116.18),
    ('Mcara, SK', 'southbound', 49.00, -102.14),
    ('Kevin, MT', 'southbound', 48.75, -111.98),
    ('Letellier, MB', 'southbound', 49.07, -97.25),
    ('Rouses Point, NY', 'southbound', 44.99, -73.37),
    ('Massena, NY', 'westbound', 44.93, -74.89),
    ('Island Pond, VT', 'eastbound', 44.81, -71.88),
    ('Windsor TFR, ON', 'westbound', 42.31, -83.04),
]

sensors_out = []
for name, direction, lat, lng in sensor_configs:
    sensor_detail = rs_detail.get(name, {})
    monthly = {}
    for m in all_months:
        m_data = sensor_detail.get(m, {})
        cars = m_data.get('total_un1075', 0)
        if cars > 0:
            monthly[m] = cars
    sensors_out.append({
        'name': name,
        'lat': lat,
        'lng': lng,
        'direction': direction,
        'monthly': monthly,
    })

# --- Write outputs ---
cross_border = {
    'commodity': 'lpg_cross_border',
    'display_name': 'LPG Cross-Border',
    'subtitle': 'Canada → US · Propane & Butane · Rail Exports',
    'unit': 'barrels (k)',
    'gov_source_label': 'CER',
    'gov_lag_label': '8–10 wks',
    'last_updated': datetime.now().strftime('%Y-%m-%d'),
    'monthly': {
        'description': 'Monthly totals in thousands of BBL. railstate = weighted BBL from sensor data after transit removal. gov = CER Railway export volumes.',
        'columns': ['month', 'railstate', 'gov'],
        'rows': monthly_rows,
    },
}

with open(DATA_DIR / 'lpg_cross_border.json', 'w') as f:
    json.dump(cross_border, f, indent=2)
print(f'Wrote {DATA_DIR / "lpg_cross_border.json"} ({len(monthly_rows)} months)')

sensor_volumes = {
    'description': 'LPG cross-border volumes by sensor location for map visualization',
    'last_updated': datetime.now().strftime('%Y-%m-%d'),
    'sensors': sensors_out,
}

with open(DATA_DIR / 'lpg_sensor_volumes.json', 'w') as f:
    json.dump(sensor_volumes, f, indent=2)
print(f'Wrote {DATA_DIR / "lpg_sensor_volumes.json"} ({len(sensors_out)} sensors)')

# Summary
rs_total = sum(r[1] for r in monthly_rows if r[1])
cer_total = sum(r[2] for r in monthly_rows if r[2])
cer_months = sum(1 for r in monthly_rows if r[2])
print(f'\nMonthly data: {all_months[0]} – {all_months[-1]}')
print(f'RS total: {rs_total:,}k BBL')
print(f'CER total: {cer_total:,}k BBL ({cer_months} months with data)')
if cer_total > 0:
    # Only ratio over overlap
    rs_overlap = sum(r[1] for r in monthly_rows if r[1] and r[2])
    print(f'RS/CER ratio (overlap): {rs_overlap/cer_total*100:.1f}%')
