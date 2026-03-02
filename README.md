BIT Capital Polymarket Signal Scanner
====================================

Reviewer-friendly setup guide for a fresh ZIP extraction.

What this app does
------------------
- Pulls active markets from Polymarket
- Classifies markets as `IGNORE / MONITOR / ACTIONABLE`
- Routes signals to BIT fund/holding exposure
- Generates analyst-style reports
- Runs with:
  - `SQLite + rules` (fast, reproducible, no keys)
  - `Postgres + LLM` (full mode, richer reasoning)

Requirements
------------
- Python 3.10+ recommended (3.9 works with provider warnings)
- Internet access (Polymarket API)
- Optional for full mode: at least one key
  - `GROQ_API_KEY` or `GEMINI_API_KEY` or `ANTHROPIC_API_KEY`

1) Start From ZIP (All Platforms)
---------------------------------
1. Extract ZIP
2. Open terminal in project folder
3. Create env file:

```bash
cp .env.example .env
```

Windows PowerShell equivalent:

```powershell
Copy-Item .env.example .env
```

2) macOS / Linux Setup
----------------------
```bash
cd /path/to/polymarket-signal-scanner-v2
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 app.py --port 5001
```

3) Windows Setup (PowerShell)
-----------------------------
```powershell
cd C:\path\to\polymarket-signal-scanner-v2
py -3 -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python app.py --port 5001
```

Note: You can run without activating the venv (above). This avoids PowerShell execution-policy issues.

4) Run Modes
------------

A) Fast Reviewer Mode (recommended)
- Keep `.env` defaults (`DB_BACKEND=sqlite`, no API keys)
- Start app using commands above
- Expected banner:

```text
DB Backend: sqlite | data/polymarket_scanner.db
LLM: ✗ Rule-based fallback (set GROQ_API_KEY or GEMINI_API_KEY or ANTHROPIC_API_KEY)
URL: http://localhost:5001
```

B) Full Mode (Postgres + LLM)
Edit `.env`:

```env
DB_BACKEND=postgres
DATABASE_URL=postgres://USER:PASSWORD@HOST:PORT/DBNAME
GROQ_API_KEY=your_key_here
# GEMINI_API_KEY=your_key_here
# ANTHROPIC_API_KEY=your_key_here
```

Then start app (same commands as your platform section).

Expected banner:

```text
DB Backend: postgres | postgres://...
LLM: ✓ Groq API
URL: http://localhost:5001
Actions: Postgres write paths enabled (manual + API-tested)
```

5) How To Provide API Keys
--------------------------
Recommended: put keys into `.env` and restart app.

macOS/Linux (temporary shell key):
```bash
export GROQ_API_KEY="gsk_..."
python3 app.py --port 5001
```

Windows PowerShell (temporary session key):
```powershell
$env:GROQ_API_KEY = "gsk_..."
.venv\Scripts\python app.py --port 5001
```

Key check:

macOS/Linux:
```bash
echo "$GROQ_API_KEY"
```

PowerShell:
```powershell
echo $env:GROQ_API_KEY
```

If key is set, output is non-empty.

6) Expected Runtime Output
--------------------------
After startup, expected recurring logs:
- Scheduler starts:
  - `[SCHEDULER] Started: ingestion=... analysis=... report=...`
- Ingestion runs:
  - `[INGESTION] Batch: fetched=... stored=...`
  - `[INGESTION] Complete: ...`
- Analysis runs:
  - `[ANALYSIS] Analyzing ... markets ...`
  - `[ANALYSIS] Complete: {'analyzed': ..., 'actionable': ..., ...}`
- Report runs:
  - `[REPORT] Generating signal report...`
  - `[REPORT] Report #... saved to database.`

7) Open App + Smoke Test
------------------------
Open:
- [http://localhost:5001](http://localhost:5001)

In UI:
1. Click `Ingest`
2. Click `Analyze`
3. Click `Generate Report`
4. Validate:
   - Dashboard has `Top Actionable Signals`
   - Live Markets table is populated
   - Reports tab shows newest report on top

8) Optional API Smoke Test
--------------------------
```bash
curl -s http://localhost:5001/api/stats
curl -s "http://localhost:5001/api/signals?label=ACTIONABLE&limit=5"
curl -s http://localhost:5001/api/reports
curl -s http://localhost:5001/api/job/status
```

9) Common Issues
----------------
A) Port in use
- Run with another port:
  - `python3 app.py --port 5050`
  - or `.\.venv\Scripts\python app.py --port 5050`

B) PowerShell `Activate.ps1` blocked
- Use direct venv Python commands (no activation), as shown above.

C) `open .env` does not work on Windows
- Use `notepad .env` or edit in VS Code.

D) LLM still shows rule-based
- Ensure key exists in `.env` or session env
- Restart process after setting keys
- Confirm with:
  - `echo "$GROQ_API_KEY"` (mac/Linux)
  - `echo $env:GROQ_API_KEY` (PowerShell)

E) Provider quota errors (`429`)
- Expected on free tiers.
- App continues with fallback logic where possible.

10) Project Layout
------------------
```text
app.py
backend/
  ingestion.py
  analysis.py
  report_generator.py
  scheduler.py
database/
  db.py
  db_postgres.py
  schema.sql
  schema_postgres.sql
frontend/
  index.html
scripts/
  migrate_sqlite_to_postgres.py
REPORT.md
```

11) Core Endpoints
------------------
- `GET /api/stats`
- `GET /api/markets`
- `GET /api/markets/live`
- `GET /api/signals`
- `GET /api/reports`
- `POST /api/actions/ingest`
- `POST /api/actions/analyze`
- `POST /api/actions/report`
- `GET /api/job/status`


11) Case-Study Mapping (What to look for)
-----------------------------------------
- BIT focus understanding: fund/holding trigger ontology integrated
- Intelligent filtering: strict ACTIONABLE/MONITOR/IGNORE routing
- LLM signal extraction: provider-aware causal reasoning with fallback
- Scheduled ingestion: recurring background pipeline
- Database: normalized Postgres schema available
- Web interface: analyst/PM workflow pages

Final Notes for Reviewers
-------------------------
- For a strict reproducible run without external dependencies, use SQLite mode.
- For the best scoring intent of the case study, use Postgres + at least one LLM key.
- See REPORT.md for implementation decisions and learnings.
