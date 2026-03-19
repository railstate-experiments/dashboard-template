# Ethanol Texas Export Corridor — Methodology & Reference

## Overview

This module tracks ethanol railcar volumes heading toward the Texas Gulf Coast export terminals and compares them against US Census Bureau export data from the Texas City and Galveston ports. It produces two outputs:

1. **Monthly comparison** — RailState sensor volumes vs Census export volumes, broken out by undenatured (2207.10 vs UN1170) and total ethanol (full 2207 vs UN1170 + UN1987)
2. **Daily chart** — 90-day rolling view of daily ethanol volumes by UN placard, with 7-day and 30-day moving averages

---

## File Locations

```
benchmarking/
├── scripts/
│   ├── ethanol_texas_report.py          ← comparison report script (Crabb + League City vs Census)
│   └── ethanol_texas_daily.py           ← standalone sensor script (Crabb + Gish + North Spring)
├── data/
│   ├── ethanol_texas_report.json        ← dashboard JSON: monthly comparison + 90-day daily
│   ├── ethanol_texas_report_raw.csv     ← raw car-level data (Crabb + League City)
│   ├── ethanol_texas.json               ← dashboard JSON from ethanol_texas_daily.py
│   ├── ethanol_texas_cars_raw.csv       ← raw car-level data (Crabb + Gish + North Spring)
│   └── ethanol_texas_daily.csv          ← daily aggregates from ethanol_texas_daily.py
└── .github/
    └── workflows/
        └── daily-update.yml             ← runs both scripts daily at 6 AM ET
```

### Primary report: `ethanol_texas_report.py`

This is the comparison script. It fetches RailState data from two sensors and Census data from two ports, then produces the combined JSON.

### Supporting script: `ethanol_texas_daily.py`

This is the standalone RailState-only script with three sensors (Crabb, Gish, North Spring). It was used during development to explore sensor coverage and is retained as a separate data feed.

---

## RailState Configuration

### Sensors

| Sensor | Direction | Role |
|---|---|---|
| **Crabb, TX** | Southbound | Captures ethanol moving south toward Texas City/Galveston |
| **League City, TX** | Southbound | Captures ethanol moving south toward Galveston — highest volume sensor |

### API Details

- **Base URL:** `https://api.railstate.com`
- **Endpoints used:**
  - `GET /api/v3/sensors/overview` — loads sensor name-to-ID mapping
  - `GET /api/v3/trains/full_sightings` — paginated train/car sighting data
- **Authentication:** Bearer token via `RAILSTATE_API_KEY` env var (falls back to hardcoded key in script)
- **Pagination:** follows `nextRequestLink` in response until exhausted; `response_size=500` per page

### Ethanol Car Identification

**UN Placards tracked:**

| Placard | Description | Census Comparison |
|---|---|---|
| **UN1170** | Ethanol (≥80% concentration) | Compared against Census 2207.10 (undenatured) |
| **UN1987** | Denatured alcohol / ethanol solution | Combined with UN1170 to compare against full Census 2207 |

**How ethanol cars are identified:**

1. Only `Tank Car` type cars are considered
2. Each car's `hazmats[].placardType` is checked; values of `EMPTY` are ignored
3. Cars with placard `UN1170` or `UN1987` are counted as ethanol

### Gap-Filling Logic

Some tank cars pass sensors without a readable placard. The script applies gap-filling in two scenarios:

**Unit trains** (≥70% of tank cars carry an ethanol placard):
- All empty-placard tank cars are assigned the dominant placard of the train
- The dominant placard is whichever of UN1170/UN1987 appears most in that train

**Manifest trains** — block detection:
- Consecutive blocks of ≥6 tank cars are evaluated
- If the first and last car in the block have ethanol placards, and no non-ethanol hazmat placards are present, and ≥50% of the block is already ethanol, empty cars are filled with the first car's placard

### Cars Without a Car ID

The RailState API returns `carId: null` for approximately 15% of tank cars (unreadable markings). These are assigned a synthetic ID of `NOID_{sightingId}_{position}` so they are not collapsed during deduplication.

### Volume Assumption

**714 barrels per tank car** (flat rate, all cars).

This is conservative. The API's `equipmentParameters.dimensions.gallonageCapacity` field (available for ~85% of cars) shows an average of 30,295 gallons = 721 barrels. The 714 assumption underestimates by ~1%.

### Deduplication

- **Raw merge:** `car_id + detection_time` — removes pagination overlaps where the same sighting appears on multiple API pages
- **Daily aggregation:** `car_id + date + sensor_name` — same car at different sensors on the same day counts as separate trips (different routes to different terminals)

### Timezone

All daily date assignments use **US Central time** (`America/Chicago`). The API returns `detectionTimeUTC`; this is converted to Central before extracting the calendar date. This aligns with how the Census Bureau reports monthly trade data and avoids shifting late-evening sightings into the next calendar day.

### Fetch Strategy

| Mode | Command | Behavior |
|---|---|---|
| Daily incremental | `python ethanol_texas_report.py` | Fetches from `last_date - 2 days` to yesterday |
| Explicit window | `python ethanol_texas_report.py --days 30` | Fetches last N days |
| Full rebuild | `python ethanol_texas_report.py --rebuild` | Fetches last 180 days from scratch |

The 2-day overlap on incremental updates ensures late-arriving sightings are captured. New data is merged with existing raw CSV and deduplicated.

---

## Census Configuration

### Data Source

**US Census Bureau International Trade API** — port-level exports by HS code.

- **Endpoint:** `https://api.census.gov/data/timeseries/intltrade/exports/porths`
- **No API key required** (public endpoint)

### Ports

| Port Code | Port Name | Census District |
|---|---|---|
| **5306** | Texas City, TX | Houston-Galveston (53) |
| **5310** | Galveston, TX | Houston-Galveston (53) |

Houston port (5301) is excluded — its ethanol volume is minimal (~2% of district total).

### HS Codes

| HS Code | Description | Maps to |
|---|---|---|
| **2207.10** | Undenatured ethyl alcohol, ≥80% vol | RailState UN1170 |
| **2207.20** | Denatured ethyl alcohol | RailState UN1987 |
| **Full 2207** | 2207.10 + 2207.20 combined | RailState UN1170 + UN1987 |

### Unit Conversion

The Census port-level endpoint provides `VES_WGT_MO` (vessel shipping weight in kilograms) but not commodity-specific volume units. Conversion to barrels:

```
barrels = kg / 0.789 (ethanol density, kg/L) / 3.78541 (L/gal) / 42 (gal/bbl)
```

This conversion was validated against the user's known December 2025 figures:
- Galveston: script = 2,164,203 bbl vs known = 2,163,693 bbl (diff: +510, 0.02%)
- Texas City: script = 78,568 bbl vs known = 78,549 bbl (diff: +19, 0.02%)

### Census Data Lag

Census trade data is published approximately 6–8 weeks after the reporting month. The script fetches all available months from January 2025 through the current month; months not yet published return HTTP 204 and are skipped. New months are automatically included on the next run.

---

## Output JSON Schema

**File:** `data/ethanol_texas_report.json`

```json
{
  "commodity": "ethanol_texas_report",
  "display_name": "Ethanol — Texas Export Corridor",
  "subtitle": "RailState (Crabb + League City SB) vs Census (Texas City + Galveston)",
  "unit": "barrels",
  "barrels_per_car": 714,
  "last_updated": "2026-03-15",
  "railstate_sensors": ["Crabb, TX southbound", "League City, TX southbound"],
  "census_ports": ["Texas City, TX", "Galveston, TX"],

  "monthly_comparison": {
    "columns": [
      "month",
      "census_2207_10_bbl",       // Census undenatured exports (barrels)
      "census_2207_20_bbl",       // Census denatured exports (barrels)
      "census_total_bbl",         // Census 2207.10 + 2207.20 combined
      "rs_un1170_cars",           // RailState UN1170 car count
      "rs_un1170_bbl",            // RailState UN1170 barrels (cars × 714)
      "rs_un1987_cars",           // RailState UN1987 car count
      "rs_un1987_bbl",            // RailState UN1987 barrels (cars × 714)
      "rs_total_cars",            // RailState total car count
      "rs_total_bbl"              // RailState total barrels
    ],
    "rows": [
      ["2025-01", 2744822, 347895, 3092717, null, null, null, null, null, null],
      ["2025-11", 1948401, 894527, 2842928, 3620, 2584680, 222, 158508, 3842, 2743188],
      ...
    ]
  },

  "daily": {
    "dates":                  ["2025-12-15", ...],   // last 90 days
    "un1170_cars":            [116, ...],
    "un1170_barrels":         [82824, ...],
    "un1987_cars":            [3, ...],
    "un1987_barrels":         [2142, ...],
    "total_cars":             [119, ...],
    "total_barrels":          [84966, ...],
    "total_barrels_7d_ma":    [94656.0, ...],
    "total_barrels_30d_ma":   [78873.2, ...],
    "un1170_barrels_7d_ma":   [89964.0, ...],
    "un1170_barrels_30d_ma":  [75779.2, ...],
    "un1987_barrels_7d_ma":   [4692.0, ...],
    "un1987_barrels_30d_ma":  [3094.0, ...]
  }
}
```

**Notes on monthly rows:**
- Months with Census data but no RailState data have `null` for RS fields (pre-September 2025)
- Months with RailState data but no Census data yet have `null` for Census fields (recent months pending Census publication)
- The comparison is meaningful starting **November 2025** — earlier months have partial RailState sensor coverage

---

## Interpretation Notes

### Why RailState can exceed Census

RailState measures **all rail ethanol arriving** at the Texas Gulf Coast corridor. Census measures **vessel exports** from Texas City and Galveston. RailState will exceed Census when:

- Ethanol enters **terminal storage** and hasn't shipped yet (timing lag)
- Ethanol is consumed **domestically** (blending, refining) rather than exported
- Rail arrivals in month N load onto vessels in month N+1

### Why Census can exceed RailState

Census will exceed RailState when:

- Ethanol arrives by **pipeline or truck** (not captured by rail sensors)
- Storage **draws down** — previously stored ethanol ships out
- Ethanol arrives on **rail routes not covered** by Crabb and League City sensors

### Expected RS/Census Ratio

Based on Nov 2025–Jan 2026 data:
- **2207.10 vs UN1170:** ~125–145% (RailState sees more because not all UN1170 gets exported as 2207.10)
- **Full 2207 vs UN1170+UN1987:** ~90–115% (tighter match; December 2025 was 101.8%)

The full 2207 comparison is the better apples-to-apples metric.

---

## GitHub Actions

The script runs daily as part of `daily-update.yml`:

```yaml
- name: Update Ethanol Texas Report
  env:
    RAILSTATE_API_KEY: ${{ secrets.RAILSTATE_API_KEY }}
  run: python scripts/ethanol_texas_report.py
  continue-on-error: true
```

Dependencies: `requests`, `pandas` (both installed in the workflow's pip step).

---

## Adding to the Dashboard

To add this as a new tab in `index.html`:

1. The JSON file is `data/ethanol_texas_report.json`
2. The monthly comparison data is in `monthly_comparison.rows`
3. The daily chart data is in `daily` with separate UN1170/UN1987 series
4. Moving averages are pre-calculated: `*_7d_ma` and `*_30d_ma` fields
5. Use `null` checks on monthly rows to handle months where one data source is missing
