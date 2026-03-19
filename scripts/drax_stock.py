#!/usr/bin/env python3
"""
Fetch Drax Group (DRX.L) daily stock prices from Yahoo Finance.
Covers the same period as the Drax BC wood pellet data (Jan 2024+).

Outputs:
    ../data/drax_stock.json
"""

import json
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen, Request

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
OUTPUT_PATH = DATA_DIR / "drax_stock.json"

SYMBOL = "DRX.L"
START_DATE = datetime(2024, 1, 1)


def fetch_yahoo_chart(symbol: str, start: datetime, end: datetime) -> dict:
    """Fetch daily OHLCV data from Yahoo Finance v8 chart API."""
    period1 = int(start.timestamp())
    period2 = int(end.timestamp())

    params = urlencode({
        "period1": period1,
        "period2": period2,
        "interval": "1d",
        "includePrePost": "false",
    })

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?{params}"
    req = Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "Accept": "application/json",
    })

    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    end = datetime.utcnow()

    print(f"Fetching {SYMBOL} from {START_DATE.date()} to {end.date()}...", flush=True)
    raw = fetch_yahoo_chart(SYMBOL, START_DATE, end)

    result = raw.get("chart", {}).get("result", [])
    if not result:
        print("No data returned from Yahoo Finance!", flush=True)
        return

    r = result[0]
    timestamps = r.get("timestamp", [])
    quote = r.get("indicators", {}).get("quote", [{}])[0]
    closes = quote.get("close", [])
    highs = quote.get("high", [])
    lows = quote.get("low", [])
    volumes = quote.get("volume", [])

    # Convert to daily data
    dates = []
    close_prices = []
    high_prices = []
    low_prices = []
    vol_data = []

    for i, ts in enumerate(timestamps):
        dt = datetime.utcfromtimestamp(ts)
        date_str = dt.strftime("%Y-%m-%d")
        c = closes[i] if i < len(closes) else None
        h = highs[i] if i < len(highs) else None
        lo = lows[i] if i < len(lows) else None
        v = volumes[i] if i < len(volumes) else None

        if c is not None:
            dates.append(date_str)
            close_prices.append(round(c, 2))
            high_prices.append(round(h, 2) if h else None)
            low_prices.append(round(lo, 2) if lo else None)
            vol_data.append(v)

    # Get currency
    meta = r.get("meta", {})
    currency = meta.get("currency", "GBP")

    output = {
        "symbol": SYMBOL,
        "currency": currency,
        "last_updated": datetime.utcnow().strftime("%Y-%m-%d"),
        "dates": dates,
        "close": close_prices,
        "high": high_prices,
        "low": low_prices,
        "volume": vol_data,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Saved {len(dates)} trading days to {OUTPUT_PATH.name}", flush=True)
    if dates:
        print(f"  Range: {dates[0]} to {dates[-1]}", flush=True)
        print(f"  Latest close: {close_prices[-1]} {currency}", flush=True)


if __name__ == "__main__":
    main()
