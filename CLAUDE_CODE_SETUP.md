# Benchmarking Repo Setup — Instructions for Claude Code

These are exact terminal instructions to set up the RailState benchmarking dashboard as a private GitHub repo with GitHub Pages hosting and GitHub Actions automation pre-configured.

The API update scripts are NOT included yet — they will be added later. This setup gets the repo, hosting, secrets, and permissions ready so that when the scripts are added, everything works immediately.

## Prerequisites

- Git installed and authenticated
- GitHub CLI (`gh`) installed and authenticated (`gh auth login`)
- GitHub Pro account (for private Pages — $4/month at github.com/settings/billing/plans)
- The project files (see File Structure below)

---

## File Structure

Before starting, confirm the `benchmarking` directory has this structure. The `index.html` is the main dashboard file (renamed from `railstate-dashboard.html`).

```
benchmarking/
├── .gitignore
├── .github/
│   └── workflows/
│       └── daily-update.yml
├── README.md
├── index.html                      ← the dashboard (renamed from railstate-dashboard.html)
├── data/
│   ├── lpg_cross_border.json
│   ├── ethanol_cross_border.json
│   ├── ethanol_ny_harbor.json
│   ├── diesel_cross_border.json
│   ├── cp.json
│   └── glencore.json
└── scripts/
    ├── _template_updater.py        ← reference template for writing update scripts later
    └── add_gov_data.py             ← manual entry for government/public filing data
```

The `scripts/` directory does not yet contain the API update scripts (update_lpg.py, etc.). Those will be added in a separate step later. The GitHub Actions workflow will simply skip those steps gracefully (each step has `continue-on-error: true`) until the scripts exist.

---

## Step 1: Initialize and push to GitHub

```bash
cd benchmarking
git init
git add .
git commit -m "Initial commit — dashboard, data schema, automation scaffold"
gh repo create benchmarking --private --source=. --push
```

Verify it worked:

```bash
gh repo view benchmarking --web
```

This opens the repo in your browser. Confirm it says "Private" next to the repo name.

---

## Step 2: Add secrets for the API scripts

These are encrypted environment variables that your Python scripts will access during GitHub Actions runs. Set them now so they're ready when the scripts are added. Nobody can read them after they're saved — not even you. They only get injected at runtime.

```bash
gh secret set RAILSTATE_API_KEY --repo benchmarking
```

You will be prompted to paste the value. Paste your API key and press Enter.

```bash
gh secret set RAILSTATE_API_BASE --repo benchmarking
```

Paste your API base URL (e.g., `https://api.railstate.com/v1`) and press Enter.

Add any additional secrets your scripts will need the same way:

```bash
gh secret set ANY_OTHER_SECRET_NAME --repo benchmarking
```

Verify secrets are set:

```bash
gh secret list --repo benchmarking
```

You should see the secret names listed (values are not shown).

---

## Step 3: Enable Actions write permissions

The GitHub Actions bot needs permission to commit updated data files back to the repo.

```bash
OWNER=$(gh api user --jq '.login')
gh api "repos/${OWNER}/benchmarking/actions/permissions/workflow" \
  --method PUT \
  --field default_workflow_permissions=write \
  --field can_approve_pull_request_reviews=true
```

---

## Step 4: Enable GitHub Pages

```bash
OWNER=$(gh api user --jq '.login')
gh api "repos/${OWNER}/benchmarking/pages" \
  --method POST \
  --field source='{"branch":"main","path":"/"}' \
  --field build_type=legacy
```

If Pages is already enabled and this errors, update instead:

```bash
gh api "repos/${OWNER}/benchmarking/pages" \
  --method PUT \
  --field source='{"branch":"main","path":"/"}' \
  --field build_type=legacy
```

Get your Pages URL:

```bash
gh api "repos/${OWNER}/benchmarking/pages" --jq '.html_url'
```

The dashboard will be live at that URL within a couple of minutes. Because the repo is private, only people logged into GitHub with access to the repo can view the page.

---

## Step 5: Verify the setup

Confirm Pages is live:

```bash
OWNER=$(gh api user --jq '.login')
echo "https://${OWNER}.github.io/benchmarking/"
```

Visit that URL in a browser (you must be logged into GitHub). You should see the dashboard with placeholder data.

Confirm secrets are stored:

```bash
gh secret list --repo benchmarking
```

Confirm write permissions:

```bash
OWNER=$(gh api user --jq '.login')
gh api "repos/${OWNER}/benchmarking/actions/permissions/workflow" --jq '.default_workflow_permissions'
```

Should return `write`.

---

## Done — what's ready and what's next

**Ready now:**
- Private repo at github.com/YOUR-USERNAME/benchmarking
- Dashboard live at private GitHub Pages URL
- API secrets stored and encrypted
- Actions workflow configured with write permissions
- Government data entry script (`scripts/add_gov_data.py`) works now

**Still needed:**
- API update scripts (update_lpg.py, update_ethanol.py, update_diesel.py, update_cp.py, update_glencore.py) — add to `scripts/` when ready
- Wire `index.html` to read from the JSON data files instead of placeholder random data

**When the scripts are ready, add them like this:**

```bash
cd benchmarking
# Add your scripts to scripts/
git add scripts/
git commit -m "Add API update scripts"
git push
```

**Then test the full workflow:**

```bash
gh workflow run "Daily Data Update" --repo benchmarking
gh run watch --repo benchmarking
```

If a run fails, view the logs:

```bash
gh run view --repo benchmarking --log-failed
```

---

## Reference: Adding government data (works now)

When a new public report comes out, run locally:

```bash
python scripts/add_gov_data.py lpg_cross_border 2026-01 3920
python scripts/add_gov_data.py cp 2025-12 108400

git add data/
git commit -m "Add EIA Jan 2026 LPG data"
git push
```

GitHub Pages will redeploy automatically after the push.

---

## Reference: Adding a new commodity tab later

1. Create a new JSON data file in `data/` following the schema
2. Write a new update script in `scripts/`
3. Add a new step in `.github/workflows/daily-update.yml`
4. Update `index.html` to add the new tab
5. Commit and push

---

## Troubleshooting

**Workflow not running on schedule:**
GitHub may delay or skip scheduled runs on repos with no recent activity. Push any commit to wake it up, or trigger manually with `gh workflow run`.

**Pages returns 404:**
Wait 2-3 minutes after enabling. If still 404, confirm the file is named `index.html` (not `railstate-dashboard.html`) and is in the repo root.

**Secrets not available in scripts:**
Make sure the secret names in `daily-update.yml` env blocks match exactly what you set with `gh secret set`. Names are case-sensitive.

**Permission denied on push from Actions:**
Re-run Step 3 to enable write permissions. The `default_workflow_permissions` must be set to `write`.

**Workflow steps fail because scripts don't exist yet:**
This is expected. Each step has `continue-on-error: true`, so the workflow will complete and the commit step will run. Once you add the actual scripts, those steps will start succeeding.
