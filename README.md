# RailState Commodity Intelligence Dashboard

## Repo Structure

```
railstate-dashboard/
├── index.html                      ← the dashboard
├── data/
│   ├── lpg_cross_border.json       ← one file per commodity tab
│   ├── ethanol_cross_border.json
│   ├── ethanol_ny_harbor.json
│   ├── diesel_cross_border.json
│   ├── cp.json
│   └── glencore.json
├── scripts/
│   ├── _template_updater.py        ← copy this to start a new script
│   ├── update_lpg.py               ← YOUR script: API call + write JSON
│   ├── update_ethanol.py
│   ├── update_diesel.py
│   ├── update_cp.py
│   ├── update_glencore.py
│   └── add_gov_data.py             ← manual entry for gov/public data
└── .github/
    └── workflows/
        └── daily-update.yml        ← GitHub Actions cron job
```

## How It Works

```
┌─────────────┐   6 AM ET daily   ┌──────────────────┐
│ GitHub       │ ─────────────────→│ Your Python      │
│ Actions cron │                   │ scripts run       │
└─────────────┘                   └────────┬─────────┘
                                           │ writes JSON
                                           ▼
                                  ┌──────────────────┐
                                  │ data/*.json       │
                                  │ (committed back   │
                                  │  to the repo)     │
                                  └────────┬─────────┘
                                           │ GitHub Pages serves
                                           ▼
                                  ┌──────────────────┐
                                  │ index.html        │
                                  │ reads JSON at     │
                                  │ page load         │
                                  └──────────────────┘
```

## Setup: Step by Step

### 1. Create the GitHub repo

```bash
cd railstate-dashboard
git init
git add .
git commit -m "Initial commit"
gh repo create railstate-dashboard --private --push
```

Or create it through the GitHub web UI and push.

### 2. Add your API scripts

Copy `scripts/_template_updater.py` for each commodity. Each script should:
- Call the RailState API however you need
- Append new daily rows to the matching `data/*.json` file
- Follow the JSON schema (see below)

Name them to match the workflow:
- `scripts/update_lpg.py`
- `scripts/update_ethanol.py`
- `scripts/update_diesel.py`
- `scripts/update_cp.py`
- `scripts/update_glencore.py`

### 3. Add secrets in GitHub

Go to **Settings → Secrets and variables → Actions** and add:

| Secret               | Value                        |
|----------------------|------------------------------|
| `RAILSTATE_API_KEY`  | Your API key                 |
| `RAILSTATE_API_BASE` | e.g. `https://api.railstate.com/v1` |

Add any others your scripts reference via `os.environ`.

### 4. Enable write permissions for Actions

Go to **Settings → Actions → General → Workflow permissions**:
- Select **"Read and write permissions"**
- Check **"Allow GitHub Actions to create and approve pull requests"**

This lets the bot commit the updated JSON files back to the repo.

### 5. Enable GitHub Pages

Go to **Settings → Pages**:
- Source: **Deploy from a branch**
- Branch: **main** / **/ (root)**

Your dashboard will be live at:
`https://<your-username>.github.io/railstate-dashboard/`

### 6. Test it

Trigger a manual run from the **Actions** tab → **Daily Data Update** → **Run workflow**.

Check the run log to confirm your scripts executed and data files were committed.

## JSON Data Schema

Each file in `data/` follows this structure:

```json
{
  "commodity": "lpg_cross_border",
  "display_name": "LPG Cross-Border",
  "unit": "barrels (k)",
  "gov_source_label": "EIA / STB",
  "gov_lag_label": "6–8 wks",
  "last_updated": "2026-03-11",

  "monthly": {
    "columns": ["month", "railstate", "gov"],
    "rows": [
      ["2025-04", 4180, 3870],
      ["2026-01", 4310, null]
    ]
  },

  "daily": {
    "columns": ["date", "value"],
    "rows": [
      ["2025-12-12", 148],
      ["2025-12-13", 132]
    ]
  }
}
```

**Key rules:**
- `monthly.rows[n][2] = null` renders as the "gap" (not yet reported) in the dashboard
- `daily.rows` should be the most recent ~90–180 days
- Keep rows sorted by date (the template helper does this for you)
- `unit` drives the Y-axis label and tooltip text in charts

## Adding Government Data

When a new EIA report, CPKC filing, or Glencore production report drops:

```bash
python scripts/add_gov_data.py lpg_cross_border 2026-01 3920
python scripts/add_gov_data.py cp 2025-12 108400
```

Then commit and push:

```bash
git add data/
git commit -m "Add EIA Jan 2026 data"
git push
```

GitHub Pages redeploys automatically.

## Local Development

Open `index.html` in a browser. If running from the filesystem (file://),
you may need a local server for the JSON fetches:

```bash
python -m http.server 8000
# then visit http://localhost:8000
```
