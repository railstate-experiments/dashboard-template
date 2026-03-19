# RailState Dashboard — Project Architecture

This document describes how the benchmarking dashboard is structured so it can be replicated for new dashboards showing different data.

---

## Overview

The dashboard is a **single-file HTML application** (`index.html`) that loads data from JSON files via `fetch()`, renders charts with Chart.js, and displays maps with Mapbox GL JS. Data is updated daily by Python scripts orchestrated through GitHub Actions, deployed via GitHub Pages.

---

## 1. index.html Structure

### CSS Design System

All styling is inline in `<style>` tags at the top of `index.html`.

**CSS Variables (`:root`):**
| Variable | Value | Purpose |
|----------|-------|---------|
| `--rs` | `#80BC2C` | RailState green (primary brand) |
| `--rs-blue` | `#0452BE` | RailState blue (sensors, accents) |
| `--gov` | `#6366f1` | Government/public data (indigo) |
| `--gap` | `#c42b2b` | Data gap indicator (red) |
| `--bg` | `#f4f5f7` | Page background |
| `--surface` | `#ffffff` | Card/panel background |
| `--font-head` | `Poppins` | Headings (600-800 weight) |
| `--font-body` | `Inter` | Body text (300-500 weight) |

### Tab System

Tabs are defined as buttons in `.tab-bar` with `data-tab="tab-id"` attributes. Each tab has a corresponding `.tab-panel#tab-id` div.

```html
<button class="tab-btn active" data-tab="tab-overview">Overview</button>
<div class="tab-panel active" id="tab-overview">...</div>
```

**Lazy initialization:** Charts only render when their tab is first clicked, tracked by a `chartInited` object:

```javascript
const chartInited = {};
const initCharts = {
  'tab-wcan': function() { /* fetch data, build charts */ },
  'tab-lpg': function() { /* ... */ },
};

function switchTab(id) {
  // Toggle active classes...
  if (!chartInited[id] && initCharts[id]) {
    initCharts[id]();
    chartInited[id] = true;
  }
}
```

### Data Loading Pattern

All data fetches use a cache-buster to prevent stale CDN/browser caches:

```javascript
const _cb = '?_=' + Date.now();
fetch('data/lpg_cross_border.json' + _cb)
  .then(r => r.ok ? r.json() : null)
  .then(data => { /* build chart */ });
```

### Historical Data Priority

For comparison charts (LPG, CPKC, Glencore), curated historical JSON is loaded first. Daily API data only supplements months not in the curated file:

```javascript
fetch('data/cp.json' + _cb).then(r => r.json()).then(cpData => {
  const monthlyRS = {};
  cpData.monthly.rows.forEach(row => {
    if (row[1] !== null) monthlyRS[row[0]] = row[1];
  });
  return fetch('data/daily_90day_data.json' + _cb).then(r => r.json()).then(dailyData => {
    // Only add months NOT already in curated data
    Object.entries(dailyMonthly).forEach(([m, v]) => {
      if (!(m in monthlyRS)) monthlyRS[m] = v;
    });
  });
});
```

### Responsive Breakpoints

| Breakpoint | Target | Key Changes |
|------------|--------|-------------|
| `≤768px` | Tablet | Maps 350px, scrollable tab bar, reduced padding |
| `≤480px` | Phone | Maps 280px, 2-column stat pills, smaller fonts |

---

## 2. Chart.js Patterns

### Monthly Comparison Bar Chart (RS vs Government)

```javascript
new Chart(canvas, {
  type: 'bar',
  data: {
    labels: monthLabels,
    datasets: [
      { label: 'RailState', data: rsVals, backgroundColor: 'rgba(128,188,44,0.65)', borderRadius: 3, order: 1 },
      { label: 'Government', data: govVals, backgroundColor: 'rgba(99,102,241,0.5)', order: 2 },
      { label: 'Gap', data: gapVals, backgroundColor: 'rgba(226,84,84,0.25)', borderDash: [4,3], order: 0 }
    ]
  },
  options: {
    responsive: true, maintainAspectRatio: true,
    interaction: { mode: 'index', intersect: false },
    plugins: { legend: { display: false }, tooltip: { ... } },
    scales: {
      x: { grid: { color: 'rgba(0,0,0,0.09)' } },
      y: { ticks: { callback: v => v.toLocaleString() } }
    }
  }
});
```

### Daily Line Chart with Moving Averages

```javascript
function movingAvg(arr, window) {
  return arr.map((_, i) => {
    if (i < window - 1) return null;
    const slice = arr.slice(i - window + 1, i + 1);
    return Math.round(slice.reduce((a, b) => a + b, 0) / window);
  });
}

const ma7 = movingAvg(dailyData, 7);
const ma30 = movingAvg(dailyData, 30);

// datasets:
{ label: 'Daily', data: dailyData, borderColor: 'rgba(100,116,139,0.4)', fill: true, order: 3 },
{ label: '7-day avg', data: ma7, borderColor: '#ef4444', borderWidth: 2, order: 2 },
{ label: '30-day avg', data: ma30, borderColor: '#083473', borderWidth: 2.5, order: 1 }
```

### Weekly Bar Chart with Moving Averages

Same pattern but uses `weeklyMA(arr, window)` with partial windows at start (no nulls).

### Custom Legend

Legends are rendered as HTML (not Chart.js built-in) for styling control:

```html
<div class="chart-legend">
  <div class="legend-item"><div class="legend-swatch" style="background:#80BC2C"></div> RailState</div>
  <div class="legend-item"><div class="legend-swatch" style="background:#6366f1"></div> Government</div>
</div>
```

---

## 3. Mapbox Map Patterns

### Map Initialization

```javascript
const map = new mapboxgl.Map({
  container: 'map-container-id',
  style: 'mapbox://styles/mapbox/light-v11',  // or satellite-streets-v12
  center: [lng, lat],
  zoom: 5,
  scrollZoom: false,
  cooperativeGestures: true
});
map.addControl(new mapboxgl.NavigationControl({ showCompass: false }));
```

### Rail Network Overlay (from GeoJSON)

```javascript
map.addSource('rail-network', { type: 'geojson', data: 'data/na_rail_network.json' });
map.addLayer({
  id: 'rail-lines', type: 'line', source: 'rail-network',
  paint: {
    'line-color': ['match', ['get', 'rr'],
      'CPRS', '#2d8c3c', 'CN', '#c0392b', 'BNSF', '#c75b12',
      'UP', '#fbcb0a', 'NS', '#555555', 'CSXT', '#2b6cb0', '#b0b8c4'],
    'line-width': ['interpolate', ['linear'], ['zoom'], 3, 0.6, 6, 1.3, 10, 2.2]
  }
});
```

### Sensor Bubble Map (Proportional Circles)

```javascript
map.addSource('sensors', { type: 'geojson', data: featureCollection });
map.addLayer({
  id: 'sensor-circles', type: 'circle', source: 'sensors',
  paint: {
    'circle-radius': ['interpolate', ['linear'], ['get', 'value'], 0, 6, maxVal, 40],
    'circle-color': 'rgba(52,132,235,0.5)',
    'circle-stroke-color': 'rgba(52,132,235,0.9)',
    'circle-stroke-width': 2
  }
});
```

### Satellite Map (Facility View)

Uses `satellite-streets-v12` style, higher zoom (12-14), with rail line overlay in a specific color.

---

## 4. Python Data Pipeline

### API Client Pattern

Every script uses the same base class:

```python
class RailStateFetcher:
    def __init__(self, api_key):
        self.api_key = api_key
        self._sensor_cache = {}

    def _request(self, url, params=None):
        headers = {'Authorization': f'Bearer {self.api_key}', 'Accept': 'application/json'}
        response = requests.get(url, params=params, headers=headers, timeout=120)
        return response.json(), None  # (data, error)

    def load_sensors(self):
        # GET /api/v3/sensors/overview → cache {name: sensorId}

    def fetch_sightings(self, sensor_id, start, end, direction=None):
        # GET /api/v3/trains/full_sightings with pagination
        # Params: sensors, detection_time_from, detection_time_to, response_size=500
        # Follow nextRequestLink for pagination
```

### Incremental Update Pattern

```python
# 1. Load existing raw CSV
existing_df = pd.read_csv(RAW_CSV_PATH)
last_date = existing_df['date'].max()

# 2. Fetch new data (2-day overlap for dedup)
start = last_date - timedelta(days=2)
new_records = fetch_from_api(start, yesterday)

# 3. Merge and deduplicate
combined = pd.concat([existing_df, new_df])
combined = combined.drop_duplicates(subset=['car_id', 'detection_time'], keep='last')

# 4. Save raw CSV + aggregate to JSON
combined.to_csv(RAW_CSV_PATH, index=False)
daily_json = aggregate_daily(combined)
json.dump(daily_json, open(DAILY_JSON_PATH, 'w'), indent=2)
```

### Existing Output Preservation (CI Pattern)

When running in GitHub Actions, local historical source files don't exist. Scripts load the existing output JSON to preserve curated data:

```python
def load_existing_output():
    if OUTPUT_PATH.exists():
        with open(OUTPUT_PATH) as f:
            data = json.load(f)
        return {row[0]: row for row in data['monthly']['rows']}
    return {}

# In build function:
if month in existing_monthly and existing_monthly[month][1] is not None:
    rows.append(list(existing_monthly[month]))  # Preserve curated value
elif api_data_for_month is not None:
    rows.append([month, api_value, None])  # Only use API for new months
```

### Script Inventory

| Script | Commodity | Output Files | Args |
|--------|-----------|-------------|------|
| `ethanol_xb_daily.py` | Ethanol cross-border | `ethanol_xb_daily.json`, `ethanol_xb_cars_raw.csv` | `--rebuild`, `--days N` |
| `ethanol_xb_report.py` | Ethanol monthly report | `ethanol_xb_report.json` | `--rebuild`, `--days N` |
| `ethanol_nyh_daily.py` | Ethanol NY Harbor | `ethanol_nyh_pipeline.json` | `--rebuild`, `--days N` |
| `ethanol_texas_daily.py` | Ethanol Texas | `ethanol_texas.json`, `ethanol_texas_daily.csv` | `--rebuild`, `--days N` |
| `ethanol_texas_report.py` | Ethanol Texas report | `ethanol_texas_report.json` | `--rebuild`, `--days N` |
| `update_lpg_cross_border.py` | LPG cross-border | `lpg_cross_border.json`, `lpg_xb_daily.json`, `lpg_sensor_volumes.json` | `--days N` |
| `cpkc_grain_daily_90day.py` | CPKC grain | `daily_90day_data.json` | None |
| `lalb_containers.py` | LA/LB containers | `lalb_containers.json`, `lalb_raw_weekly.csv` | None |
| `wcan_ports_weekly.py` | Western Canada ports | `wcan_ports.json`, `wcan_raw_sightings.csv` | `--rebuild`, `--days N` |
| `drax_bc_weekly.py` | Drax BC pellets | `drax_bc.json`, `drax_bc_raw.csv` | None |
| `drax_stock.py` | Drax stock price | `drax_stock.json` | None |
| `methanol_medicine_hat.py` | Methanol | `methanol_daily.json`, `methanol_sensors.json`, `methanol_cars_raw.csv` | `--rebuild`, `--days N` |
| `sensors_overview.py` | Sensor network | `sensors_overview.json` | None |

---

## 5. Data File Types

### Curated Historical (never overwritten by daily scripts)
- `cp.json` — CPKC grain monthly with government comparison
- `glencore.json` — Glencore coal production vs public reports
- `methanol_monthly.json` — Methanol monthly car counts from historical analysis

### Daily-Updated JSON
- All files listed in script inventory above
- Structure varies by commodity but follows patterns:
  - **Monthly comparison:** `{monthly: {columns: [...], rows: [[month, rs_val, gov_val], ...]}}`
  - **Daily volumes:** `{dates: [...], values: [...], moving_avg_7: [...]}`
  - **Sensor volumes:** `{sensors: [{name, lat, lng, monthly: {month: count}}]}`

### Raw CSV (intermediate, for incremental dedup)
- `*_cars_raw.csv` — Car-level sighting records
- `*_raw_weekly.csv` — Weekly aggregated records

### GeoJSON
- `na_rail_network.json` — North American rail lines by operator (~10MB)
- `sensors_overview.json` — Sensor locations as GeoJSON FeatureCollection

---

## 6. GitHub Actions Workflow

**File:** `.github/workflows/daily-update.yml`

```yaml
on:
  schedule:
    - cron: '0 11 * * *'  # 6:00 AM ET
  workflow_dispatch:        # Manual trigger

permissions:
  contents: write

steps:
  - Checkout repo
  - Setup Python 3.12
  - Install: requests pandas pytz openpyxl
  - Run each script with continue-on-error: true
  - Auto-commit: git add data/ → commit → pull --rebase → push
```

**Key details:**
- Each script step has `RAILSTATE_API_KEY: ${{ secrets.RAILSTATE_API_KEY }}`
- `continue-on-error: true` so one script failure doesn't block others
- Commit step stashes unstaged changes before rebase to handle concurrent pushes
- Workflow can be disabled/enabled via `gh api .../workflows/{id}/disable`

---

## 7. Static Assets

| Directory | Contents |
|-----------|----------|
| `images/how-it-works/` | 3 process step images (capture, extraction, delivery) |
| `images/Rail Car Images/` | 7 annotated rail car photos |
| `images/logos/` | 10 agency/company logos (PNG) |
| `images/customer logos/` | Customer company logos |
| `*.svg` (root) | RailState logos (white + color) |

---

## 8. Creating a New Dashboard

To create a new dashboard from this template:

1. **Clone this repo** as your starting point
2. **Strip existing tabs** from `index.html` — keep the CSS, tab system, and helper functions
3. **Create new data scripts** following the `RailStateFetcher` pattern
4. **Add new tabs** with chart containers and initialization functions
5. **Create a GitHub Actions workflow** for your scripts
6. **Set `RAILSTATE_API_KEY`** in repo Settings → Secrets → Actions
7. **Enable GitHub Pages** from Settings → Pages → Deploy from branch: main
