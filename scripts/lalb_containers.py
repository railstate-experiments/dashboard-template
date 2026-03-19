#!/usr/bin/env python3
"""
LA/LB Container Volume
======================
Processes historical Excel data and fetches new weekly data from RailState API
for container volumes through Los Angeles / Long Beach ports.

Combines BNSF (Helendale, CA) and UP (historical: Loma Linda, CA; current: Mecca, CA)
eastbound intermodal trains.

TEU = 20ft containers (×1 TEU) + 40ft containers (×2 TEU)
Domestic = 53ft containers + 53ft trailers

Usage:
    python lalb_containers.py              # process Excel + incremental API fetch
    python lalb_containers.py --rebuild    # full rebuild from Excel + API
    python lalb_containers.py --excel-only # only process Excel, no API fetch

Outputs:
    ../data/lalb_containers.json  - weekly JSON for dashboard
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests

# ============================================================================
# CONFIGURATION
# ============================================================================

API_BASE_URL = "https://api.railstate.com"

HARDCODED_API_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJhdWQiOiJhcGkucmFpbHN0YXRlLmNvbSIsImlzcyI6InJhaWxzdGF0ZS5jb20iLCJ1aWQiOiJ5"
    "RWE5aTFSbjljTmhtbEFEQkNYd3BFR3h1eEczIiwidGlkIjoiRGFuIERldm9lIFRlc3RpbmcifQ."
    "8YQJPJE3X2xomrgUCXo41tx0Vh0gCRWRZ2JbQKYpAXE"
)

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
EXCEL_PATH = DATA_DIR / "RailState_SoCal_Containers_through March 8 2026.xlsx"
PARQUET_PATH = DATA_DIR / "containers_historical.parquet"
OUTPUT_JSON_PATH = DATA_DIR / "lalb_containers.json"
RAW_CSV_PATH = DATA_DIR / "lalb_raw_weekly.csv"

# Display starts from this Monday (but we compute MAs from earlier data)
DISPLAY_START_WEEK = "2024-09-02"  # Mon of 9/2/2024 - 9/8/2024

# Company charts start from this week
COMPANY_START_WEEK = "2025-07-28"

# Sensors for new data fetches
API_SENSORS = {
    "bnsf": {"name": "Helendale, CA", "direction": "eastbound"},
    "up": {"name": "Mecca, CA", "direction": "eastbound"},
}


# ============================================================================
# EXCEL PARSING
# ============================================================================

def parse_week_range(week_str: str) -> Optional[str]:
    """Parse week string like '9/2/2024 - 9/8/2024' and return the Monday date as YYYY-MM-DD."""
    if not isinstance(week_str, str):
        return None
    # Extract first date
    m = re.match(r'(\d{1,2}/\d{1,2}/\d{4})', week_str.strip())
    if not m:
        return None
    try:
        dt = datetime.strptime(m.group(1), "%m/%d/%Y")
        # Find Monday of that week
        monday = dt - timedelta(days=dt.weekday())
        return monday.strftime("%Y-%m-%d")
    except ValueError:
        return None


def process_excel(excel_path: Path) -> pd.DataFrame:
    """Read the Excel file and return combined weekly data."""
    df = pd.read_excel(excel_path, sheet_name="DETAILS", header=None)

    # The first row has sensor names, second row (index 1) has column headers
    # Data starts at row index 2
    # BNSF (Helendale) columns: 0-13
    # UP (Loma Linda) columns: 14-26

    records = []
    for idx in range(2, len(df)):
        row = df.iloc[idx]

        # BNSF data
        bnsf_week = parse_week_range(str(row.iloc[0]))
        if not bnsf_week:
            continue

        def safe_int(val):
            try:
                return int(float(val)) if pd.notna(val) else 0
            except (ValueError, TypeError):
                return 0

        bnsf_20 = safe_int(row.iloc[1])
        bnsf_40 = safe_int(row.iloc[2])
        bnsf_53_container = safe_int(row.iloc[5])
        bnsf_53_trailer = safe_int(row.iloc[9])

        # UP data
        up_20 = safe_int(row.iloc[15])
        up_40 = safe_int(row.iloc[16])
        up_53_container = safe_int(row.iloc[19])
        up_53_trailer = safe_int(row.iloc[23])

        # Combine
        total_20 = bnsf_20 + up_20
        total_40 = bnsf_40 + up_40
        total_teu = total_20 * 1 + total_40 * 2
        total_domestic = (bnsf_53_container + up_53_container +
                          bnsf_53_trailer + up_53_trailer)

        records.append({
            "week": bnsf_week,
            "containers_20": total_20,
            "containers_40": total_40,
            "teu": total_teu,
            "domestic": total_domestic,
            "source": "excel",
        })

    result = pd.DataFrame(records)
    print(f"Parsed {len(result)} weeks from Excel", flush=True)
    return result


# ============================================================================
# API CLIENT
# ============================================================================

class RailStateFetcher:
    def __init__(self, api_key: str, base_url: str = API_BASE_URL):
        self.api_key = api_key
        self.base_url = base_url
        self._sensor_cache = {}

    def _request(self, url: str, params: dict = None) -> Tuple[dict, Optional[str]]:
        try:
            headers = {"Authorization": f"Bearer {self.api_key}", "Accept": "application/json"}
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
        url = urljoin(self.base_url, "/api/v3/sensors/overview")
        data, error = self._request(url)
        if error:
            print(f"  Warning: Could not load sensors: {error}", flush=True)
            return {}
        for sensor in data.get("sensors", []):
            name, sid = sensor.get("name"), sensor.get("sensorId")
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
        url = urljoin(self.base_url, "/api/v3/trains/full_sightings")
        all_sightings = []
        params = {
            "sensors": str(sensor_id),
            "detection_time_from": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "detection_time_to": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "response_size": 500,
        }
        while True:
            data, error = self._request(url, params)
            if error:
                print(f"    API error: {error}", flush=True)
                break
            sightings = data.get("sightings", [])
            if not sightings:
                break
            if direction:
                sightings = [s for s in sightings
                             if s.get("direction", "").lower() == direction.lower()]
            all_sightings.extend(sightings)
            next_link = data.get("nextRequestLink")
            if not next_link:
                break
            url, params = next_link, None
        return all_sightings


# ============================================================================
# API DATA PROCESSING
# ============================================================================

def count_containers_from_sighting(sighting: dict) -> dict:
    """Count 20ft, 40ft containers and 53ft containers/trailers from a sighting."""
    # Only process intermodal trains
    train_type = (sighting.get("trainType") or "").lower()
    if "intermodal" not in train_type:
        # Fallback: check car composition
        cars = sighting.get("cars", [])
        non_loco = [c for c in cars if c.get("type", "").lower() != "locomotive"]
        if not non_loco:
            return {"containers_20": 0, "containers_40": 0, "domestic": 0}
        stack_count = sum(1 for c in non_loco if "stack" in c.get("type", "").lower())
        if stack_count / len(non_loco) < 0.5:
            return {"containers_20": 0, "containers_40": 0, "domestic": 0}

    cars = sighting.get("cars", [])
    c20 = 0
    c40 = 0
    domestic = 0

    for car in cars:
        if car.get("type", "").lower() != "stack car":
            continue
        containers = car.get("containers") or []
        for container in containers:
            ct = (container.get("type") or "").lower()
            if "20" in ct and "container" in ct:
                c20 += 1
            elif "40" in ct and "container" in ct:
                c40 += 1
            elif "53" in ct:
                # 53ft containers or 53ft trailers = domestic
                domestic += 1

    return {"containers_20": c20, "containers_40": c40, "domestic": domestic}


def fetch_api_data(fetcher: RailStateFetcher, start: datetime, end: datetime) -> pd.DataFrame:
    """Fetch container data from API sensors and aggregate by week."""
    daily_totals = defaultdict(lambda: {"containers_20": 0, "containers_40": 0, "domestic": 0})

    for key, cfg in API_SENSORS.items():
        sensor_id = fetcher.get_sensor_id(cfg["name"])
        if not sensor_id:
            print(f"  Warning: Sensor not found: {cfg['name']}", flush=True)
            continue
        print(f"  Fetching {cfg['name']} ({cfg['direction']})...", flush=True)
        sightings = fetcher.fetch_sightings(sensor_id, start, end, cfg["direction"])
        print(f"    {len(sightings):,} sightings", flush=True)

        for s in sightings:
            dt_str = s.get("detectionTimeUTC", "")
            if not dt_str:
                continue
            try:
                dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            except ValueError:
                continue
            date_str = dt.strftime("%Y-%m-%d")
            counts = count_containers_from_sighting(s)
            daily_totals[date_str]["containers_20"] += counts["containers_20"]
            daily_totals[date_str]["containers_40"] += counts["containers_40"]
            daily_totals[date_str]["domestic"] += counts["domestic"]

    # Aggregate to weekly (Mon-Sun)
    weekly_totals = defaultdict(lambda: {"containers_20": 0, "containers_40": 0, "domestic": 0})
    for date_str, counts in daily_totals.items():
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        monday = dt - timedelta(days=dt.weekday())
        week_str = monday.strftime("%Y-%m-%d")
        weekly_totals[week_str]["containers_20"] += counts["containers_20"]
        weekly_totals[week_str]["containers_40"] += counts["containers_40"]
        weekly_totals[week_str]["domestic"] += counts["domestic"]

    records = []
    for week, counts in sorted(weekly_totals.items()):
        teu = counts["containers_20"] * 1 + counts["containers_40"] * 2
        records.append({
            "week": week,
            "containers_20": counts["containers_20"],
            "containers_40": counts["containers_40"],
            "teu": teu,
            "domestic": counts["domestic"],
            "source": "api",
        })

    print(f"  API: {len(records)} weeks of data", flush=True)
    return pd.DataFrame(records) if records else pd.DataFrame(
        columns=["week", "containers_20", "containers_40", "teu", "domestic", "source"]
    )


# ============================================================================
# MERGE & OUTPUT
# ============================================================================

def merge_data(excel_df: pd.DataFrame, api_df: pd.DataFrame) -> pd.DataFrame:
    """Merge Excel and API data, preferring API data for overlapping weeks."""
    if api_df.empty:
        return excel_df
    if excel_df.empty:
        return api_df

    # For overlapping weeks, API data takes precedence
    combined = pd.concat([excel_df, api_df], ignore_index=True)
    # Keep last occurrence (API) for duplicate weeks
    combined = combined.drop_duplicates(subset=["week"], keep="last")
    combined = combined.sort_values("week").reset_index(drop=True)
    return combined


def get_last_complete_week() -> str:
    """Return the Monday of the last complete Mon-Sun week."""
    today = datetime.utcnow().date()
    days_since_monday = today.weekday()
    if days_since_monday == 6:
        last_sunday = today - timedelta(days=7)
    else:
        last_sunday = today - timedelta(days=days_since_monday + 1)
    last_complete_monday = last_sunday - timedelta(days=6)
    return last_complete_monday.strftime("%Y-%m-%d")


def process_parquet(parquet_path: Path, last_complete_week: str) -> dict:
    """Process parquet file for company-level weekly breakdowns.

    Top 10 companies per week (dynamic). Excludes 'Unknown' and 'Other'.
    Combines 'XPO Logistics' into 'STG Logistics' for domestic.

    Returns dict with 'company_domestic' and 'company_international' keys.
    """
    print("\n--- COMPANY BREAKDOWN (Parquet) ---", flush=True)
    if not parquet_path.exists():
        print(f"  Parquet not found: {parquet_path}", flush=True)
        return {}

    df = pd.read_parquet(parquet_path)

    # Filter to SoCal eastbound sensors
    socal_sensors = ["Helendale, CA", "Loma Linda, CA", "Mecca, CA"]
    df = df[df["sensor_name"].isin(socal_sensors) & (df["direction"] == "Eastbound")]
    print(f"  SoCal eastbound rows: {len(df):,}", flush=True)

    # Add week column (Monday of week)
    df["week"] = df["detection_time"].dt.tz_localize(None).dt.to_period("W-SUN").apply(
        lambda p: p.start_time.strftime("%Y-%m-%d")
    )

    # Filter to COMPANY_START_WEEK through last complete week
    df = df[(df["week"] >= COMPANY_START_WEEK) & (df["week"] <= last_complete_week)]
    print(f"  Rows in date range: {len(df):,}", flush=True)

    all_weeks = sorted(df["week"].unique())

    def build_top10_per_week(by_company_week, value_col, exclude_companies):
        """Build per-week top 10 data, excluding specified companies.

        Returns (all_company_names_sorted, {company: [values_per_week]})
        """
        # Exclude unwanted companies
        filtered = by_company_week[~by_company_week["company"].isin(exclude_companies)]

        # For each week, find top 10
        all_companies_in_top10 = set()
        weekly_top10 = {}
        for week in all_weeks:
            week_data = filtered[filtered["week"] == week].sort_values(value_col, ascending=False)
            top10 = week_data.head(10)
            weekly_top10[week] = dict(zip(top10["company"], top10[value_col]))
            all_companies_in_top10.update(top10["company"].tolist())

        # Sort companies by total across all weeks (for consistent legend order)
        company_totals = {}
        for co in all_companies_in_top10:
            company_totals[co] = sum(
                weekly_top10[w].get(co, 0) for w in all_weeks
            )
        sorted_companies = sorted(all_companies_in_top10, key=lambda c: -company_totals[c])

        # Build data arrays: only include value if company is in that week's top 10
        company_data = {}
        for co in sorted_companies:
            company_data[co] = [
                int(weekly_top10[w].get(co, 0)) for w in all_weeks
            ]

        return sorted_companies, company_data

    # --- Domestic: top 10 per week, exclude Unknown/Other, merge XPO→STG ---
    dom = df[df["is_domestic"]].copy()
    # Combine XPO Logistics and STG Logistics
    dom.loc[dom["company"] == "XPO Logistics", "company"] = "STG Logistics"
    dom_by_company_week = dom.groupby(["company", "week"], observed=True).size().reset_index(name="count")

    dom_companies, dom_company_data = build_top10_per_week(
        dom_by_company_week, "count", {"Unknown", "Other"}
    )
    print(f"  Domestic companies (ever in top 10): {dom_companies}", flush=True)

    # --- International: top 10 per week, exclude Unknown/Other ---
    intl = df[df["is_international"]].copy()
    intl["teu"] = intl["size_feet"].apply(lambda s: 1 if s == 20 else (2 if s == 40 else 0))
    intl = intl[intl["teu"] > 0]
    intl_by_company_week = intl.groupby(["company", "week"], observed=True)["teu"].sum().reset_index()

    intl_companies, intl_company_data = build_top10_per_week(
        intl_by_company_week, "teu", {"Unknown", "Other"}
    )
    print(f"  International companies (ever in top 10): {intl_companies}", flush=True)

    return {
        "company_domestic": {
            "weeks": all_weeks,
            "companies": dom_companies,
            "data": dom_company_data,
        },
        "company_international": {
            "weeks": all_weeks,
            "companies": intl_companies,
            "data": intl_company_data,
        },
    }


def build_json(df: pd.DataFrame, company_data: dict = None) -> dict:
    """Build output JSON from weekly dataframe.

    Computes 4-week MAs on full data (including pre-September weeks),
    then trims to display range starting at DISPLAY_START_WEEK.
    """
    df = df.sort_values("week").reset_index(drop=True)

    last_complete_week = get_last_complete_week()
    df = df[df["week"] <= last_complete_week]

    all_weeks = df["week"].tolist()
    all_teu = df["teu"].astype(int).tolist()
    all_domestic = df["domestic"].astype(int).tolist()

    # Compute 4-week MAs on full data
    all_teu_ma4 = []
    all_dom_ma4 = []
    for i in range(len(all_teu)):
        if i < 3:
            all_teu_ma4.append(None)
            all_dom_ma4.append(None)
        else:
            all_teu_ma4.append(round(sum(all_teu[i-3:i+1]) / 4))
            all_dom_ma4.append(round(sum(all_domestic[i-3:i+1]) / 4))

    # Trim to display range
    try:
        display_start_idx = all_weeks.index(DISPLAY_START_WEEK)
    except ValueError:
        # Find first week >= DISPLAY_START_WEEK
        display_start_idx = 0
        for i, w in enumerate(all_weeks):
            if w >= DISPLAY_START_WEEK:
                display_start_idx = i
                break

    weeks = all_weeks[display_start_idx:]
    teu = all_teu[display_start_idx:]
    domestic = all_domestic[display_start_idx:]
    teu_ma4 = all_teu_ma4[display_start_idx:]
    dom_ma4 = all_dom_ma4[display_start_idx:]

    result = {
        "last_updated": datetime.utcnow().strftime("%Y-%m-%d"),
        "weeks": weeks,
        "teu": teu,
        "domestic": domestic,
        "teu_ma4": teu_ma4,
        "domestic_ma4": dom_ma4,
    }

    if company_data:
        result.update(company_data)

    return result


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="LA/LB Container Volume")
    parser.add_argument("--rebuild", action="store_true", help="Full rebuild")
    parser.add_argument("--excel-only", action="store_true", help="Only process Excel, no API")
    parser.add_argument("--days", type=int, default=None, help="Fetch last N days from API")
    args = parser.parse_args()

    api_key = os.environ.get("RAILSTATE_API_KEY", HARDCODED_API_KEY)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 70, flush=True)
    print("LA/LB CONTAINER VOLUME", flush=True)
    print(f"Run Time: {datetime.utcnow().isoformat()}", flush=True)
    print("=" * 70, flush=True)

    # Step 1: Process Excel
    print("\n--- HISTORICAL DATA (Excel) ---", flush=True)
    if EXCEL_PATH.exists():
        excel_df = process_excel(EXCEL_PATH)
    else:
        print(f"  Excel file not found: {EXCEL_PATH}", flush=True)
        excel_df = pd.DataFrame(
            columns=["week", "containers_20", "containers_40", "teu", "domestic", "source"]
        )

    # Step 2: Fetch API data (for weeks after Excel coverage)
    api_df = pd.DataFrame(
        columns=["week", "containers_20", "containers_40", "teu", "domestic", "source"]
    )

    if not args.excel_only:
        print("\n--- API DATA ---", flush=True)
        # Find the last week in Excel data to start API fetch after
        if not excel_df.empty:
            last_excel_week = excel_df["week"].max()
            # Start API fetch from the Monday after the last Excel week
            api_start = datetime.strptime(last_excel_week, "%Y-%m-%d") + timedelta(days=7)
        else:
            api_start = datetime.strptime(START_WEEK, "%Y-%m-%d")

        # End date: yesterday
        api_end = datetime.utcnow().replace(hour=23, minute=59, second=59) - timedelta(days=1)

        if args.days:
            api_start = datetime.utcnow() - timedelta(days=args.days)
            api_start = api_start.replace(hour=0, minute=0, second=0, microsecond=0)

        if api_start < api_end:
            print(f"  Fetch window: {api_start.date()} to {api_end.date()}", flush=True)
            fetcher = RailStateFetcher(api_key)
            fetcher.load_sensors()
            print(f"  Loaded {len(fetcher._sensor_cache)} sensors", flush=True)
            api_df = fetch_api_data(fetcher, api_start, api_end)
        else:
            print("  Excel data is up to date, no API fetch needed.", flush=True)

    # Step 3: Merge
    combined = merge_data(excel_df, api_df)
    print(f"\nTotal weeks: {len(combined)}", flush=True)

    # Save raw CSV
    combined.to_csv(RAW_CSV_PATH, index=False)
    print(f"Saved raw data to {RAW_CSV_PATH.name}", flush=True)

    # Step 4: Process parquet for company breakdowns
    last_complete_week = get_last_complete_week()
    company_data = process_parquet(PARQUET_PATH, last_complete_week)

    # Step 5: Build and save JSON
    output = build_json(combined, company_data)
    with open(OUTPUT_JSON_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nJSON written: {OUTPUT_JSON_PATH.name}", flush=True)
    print(f"  Weeks: {len(output['weeks'])}", flush=True)
    if output["weeks"]:
        print(f"  Range: {output['weeks'][0]} to {output['weeks'][-1]}", flush=True)
        total_teu = sum(output["teu"])
        total_dom = sum(output["domestic"])
        print(f"  Total TEU: {total_teu:,}", flush=True)
        print(f"  Total Domestic: {total_dom:,}", flush=True)

    print("\n" + "=" * 70, flush=True)
    print("DONE", flush=True)
    print("=" * 70, flush=True)


if __name__ == "__main__":
    main()
