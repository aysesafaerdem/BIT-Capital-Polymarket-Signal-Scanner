BIT Capital Polymarket Signal Scanner
====================================

This repo is packaged for reviewer execution from a fresh ZIP download (no prior setup assumed).

What this app does (in 30 seconds)
----------------------------------
- Pulls live markets from Polymarket
- Classifies markets into IGNORE / MONITOR / ACTIONABLE
- Routes signals to BIT Capital funds & holdings using a trigger ontology
- Generates a concise analyst-style report
- Works in two modes:
  A) Fast (SQLite + rule-based): reproducible run, no external DB, no API keys
  B) Full (Postgres + LLM): richer reasoning + persistent storage

Requirements
------------
- macOS/Linux terminal (bash/zsh)
- Python 3.10+ recommended (3.9 works with minor provider warnings)
- Internet access (Polymarket API)
- Optional (for Full mode): at least one LLM API key
  - GROQ_API_KEY and/or GEMINI_API_KEY and/or ANTHROPIC_API_KEY

1) Start From ZIP (Reviewer Path)
--------------------------------
1. Download and extract the ZIP
2. Open Terminal
3. cd into the project folder:

  cd /path/to/polymarket-signal-scanner-v2

4. Create + activate a virtual environment:

  python3 -m venv .venv
  source .venv/bin/activate

5. Install dependencies:

  pip install -r requirements.txt

6. Create your local env file:

  cp .env.example .env

2) Choose Run Mode
------------------

A) Fast Reviewer Mode (Recommended for a quick, reproducible run)
- Uses SQLite
- Uses rule-based fallback (no API keys required)
- No external services needed

Start:

  python3 app.py --port 5001

Expected startup banner includes:
- DB Backend: sqlite
- LLM: Rule-based fallback ...

Visit:
  http://localhost:5001

B) Full Case-Study Mode (Best demo / recommended if you have keys)
- Uses Postgres (including Supabase)
- Uses LLM-based reasoning when configured
- Falls back gracefully to rules if the LLM is unavailable

1) Edit .env and set:

  DB_BACKEND=postgres
  DATABASE_URL=postgres://USER:PASSWORD@HOST:PORT/DBNAME

  # Choose at least one:
  GROQ_API_KEY=...
  # GEMINI_API_KEY=...
  # ANTHROPIC_API_KEY=...

2) Load env vars into your shell:

  set -a
  source .env
  set +a

3) Start:

  python3 app.py --port 5001

Expected startup banner includes:
- DB Backend: postgres | postgres://...
- LLM: ✓ Groq API (or your configured provider)

3) Open the App
---------------
Visit:
  http://localhost:5001

4) Reviewer Smoke Test (UI)
---------------------------
In the UI, run the standard workflow:

1. Click Ingest (fetch Polymarket markets)
2. Click Analyze (classify + route + score)
3. Click Generate Report
4. Verify pages:
   - Dashboard: top signals + PM queue summary
   - Live Markets: full live market feed
   - Reports: newly generated report appears at the top

Notes:
- If no LLM key is configured, results are still produced via rules.
- If provider quota is hit, the pipeline continues via fallback.

5) API Smoke Test (Optional)
----------------------------
From another terminal:

  curl -s http://localhost:5001/api/stats
  curl -s "http://localhost:5001/api/signals?label=ACTIONABLE&limit=5"
  curl -s http://localhost:5001/api/reports
  curl -s http://localhost:5001/api/job/status

6) Known / Expected Behavior During Review
------------------------------------------
- If LLM quota is exhausted or a provider errors:
  - analysis/report generation falls back gracefully
  - the pipeline does not stop
- Free-tier limits may reduce reasoning depth, but the app remains operational.

7) What Good Output Looks Like (Quick sanity checks)
----------------------------------------------------
After Analyze, you should see signals with:
- A label: IGNORE, MONITOR, or ACTIONABLE
- A relevance score (0–100)
- Routed funds (e.g., Technology Leaders, Crypto Leaders, Multi Asset)
- Clear explainability:
  - matched keywords / tags (driver + channel)
  - a short cause→effect chain (event → channel → holdings)
  - impacted holdings with direction (UP/DOWN/MIXED) and confidence

Example (illustrative):
- Market: "Will US impose new export controls on AI chips by <date>?"
- Channel: AI/Semis + Regulation
- Funds: Technology Leaders / Global Leaders
- Holdings impacted: NVDA, TSM, MU (direction depends on policy severity + exposure)

8) Common Issues (and fixes)
----------------------------

A) Port already in use (5001)
- Run on a different port:
    python3 app.py --port 5050
  Then open:
    http://localhost:5050

B) Virtual environment not activated
- If you see "ModuleNotFoundError" after installing:
    source .venv/bin/activate
  Then retry.

C) Polymarket API request errors
- Ensure you have internet access.
- Retry Ingest after a minute (rate limits/network hiccups happen).

D) Postgres mode fails to connect
- Double-check DATABASE_URL format and credentials.
- Make sure the DB allows inbound connections from your IP (Supabase settings).
- If unsure, use Fast Reviewer Mode (SQLite) for a deterministic run.

E) LLM not detected / provider errors
- Confirm you set at least one key in .env and exported it:
    set -a && source .env && set +a
- If quota is exhausted, the app will still run via rule-based fallback.

9) Project Layout
-----------------
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

10) Core Endpoints
------------------
- GET  /api/stats
- GET  /api/markets
- GET  /api/markets/live
- GET  /api/signals
- GET  /api/reports
- POST /api/actions/ingest
- POST /api/actions/analyze
- POST /api/actions/report
- GET  /api/job/status

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
