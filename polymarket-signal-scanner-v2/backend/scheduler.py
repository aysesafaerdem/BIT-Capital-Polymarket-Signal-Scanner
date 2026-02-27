"""
Scheduler for periodic data ingestion and analysis jobs.
Uses Python threading for simple scheduling without external dependencies.
"""
import threading
import time
import os
import sys
from datetime import datetime, timezone
from database.db import DB_BACKEND
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Job intervals (in seconds)
INGESTION_INTERVAL = int(os.environ.get("INGESTION_INTERVAL_SECS", 3600))    # 1 hour
ANALYSIS_INTERVAL = int(os.environ.get("ANALYSIS_INTERVAL_SECS", 1800))      # 30 min
REPORT_INTERVAL = int(os.environ.get("REPORT_INTERVAL_SECS", 43200))         # 12 hours
SCHEDULER_ANALYSIS_BATCH_SIZE = int(os.environ.get("SCHEDULER_ANALYSIS_BATCH_SIZE", 10))

_scheduler_running = False
_threads = []

def run_ingestion_job():
    """Wrapper for ingestion job with error handling."""
    try:
        from backend.ingestion import run_ingestion, inject_sample_markets
        print(f"[SCHEDULER] Running ingestion job at {datetime.now().isoformat()}")
        result = run_ingestion(max_markets=300)
        
        # Check if we got real markets; if not, use samples
        from database.db import get_connection
        conn = get_connection()
        count = conn.execute("SELECT COUNT(*) as c FROM markets").fetchone()["c"]
        conn.close()
        
        if count < 5:
            print("[SCHEDULER] No markets from API, injecting sample data...")
            inject_sample_markets()
        
        return result
    except Exception as e:
        print(f"[SCHEDULER] Ingestion job error: {e}")
        return {"error": str(e)}


def run_analysis_job():
    """Wrapper for analysis job with error handling."""
    try:
        from backend.analysis import analyze_markets
        print(f"[SCHEDULER] Running analysis job at {datetime.now().isoformat()}")
        return analyze_markets(batch_size=SCHEDULER_ANALYSIS_BATCH_SIZE)
    except Exception as e:
        print(f"[SCHEDULER] Analysis job error: {e}")
        return {"error": str(e)}


def run_report_job():
    """Wrapper for report generation with error handling."""
    try:
        from backend.report_generator import generate_full_report
        print(f"[SCHEDULER] Running report generation at {datetime.now().isoformat()}")
        return generate_full_report()
    except Exception as e:
        print(f"[SCHEDULER] Report job error: {e}")
        return {"error": str(e)}


def schedule_job(job_fn, interval_secs: int, initial_delay: int = 0):
    """Run a job periodically in a background thread."""
    def _loop():
        if initial_delay > 0:
            time.sleep(initial_delay)
        while _scheduler_running:
            try:
                job_fn()
            except Exception as e:
                print(f"[SCHEDULER] Error in job {job_fn.__name__}: {e}")
            # Wait for next interval (checking running flag frequently)
            for _ in range(interval_secs):
                if not _scheduler_running:
                    break
                time.sleep(1)
    
    t = threading.Thread(target=_loop, daemon=True, name=f"scheduler-{job_fn.__name__}")
    return t


def start_scheduler():
    """Start all scheduled jobs."""
    global _scheduler_running, _threads
    if _scheduler_running and any(t.is_alive() for t in _threads):
        print("[SCHEDULER] Already running; skipping duplicate start.")
        return _threads
    _scheduler_running = True
    
    # Ingestion: start immediately, then every INGESTION_INTERVAL
    t1 = schedule_job(run_ingestion_job, INGESTION_INTERVAL, initial_delay=2)
    
    # Analysis: start after ingestion has had time to run
    t2 = schedule_job(run_analysis_job, ANALYSIS_INTERVAL, initial_delay=30)
    
    # Reports: start after analysis
    t3 = schedule_job(run_report_job, REPORT_INTERVAL, initial_delay=120)
    
    _threads = [t1, t2, t3]
    for t in _threads:
        t.start()
    
    print(f"[SCHEDULER] Started: ingestion={INGESTION_INTERVAL}s, analysis={ANALYSIS_INTERVAL}s, report={REPORT_INTERVAL}s")
    return _threads


def stop_scheduler():
    """Stop all scheduled jobs."""
    global _scheduler_running
    _scheduler_running = False
    print("[SCHEDULER] Stopping...")


def get_scheduler_status() -> dict:
    """Get status of scheduler jobs."""
    from database.db import get_connection
    conn = get_connection()
    try:
        if DB_BACKEND == "postgres":
            logs = conn.execute("""
                SELECT job_name, status, message, started_at, completed_at
                FROM job_runs
                ORDER BY started_at DESC
                LIMIT 20
            """).fetchall()
        else:
            logs = conn.execute("""
                SELECT job_name, status, message, started_at, completed_at
                FROM scheduler_log
                ORDER BY started_at DESC
                LIMIT 20
            """).fetchall()
        
        status = {
            "running": _scheduler_running,
            "active_threads": sum(1 for t in _threads if t.is_alive()),
            "recent_jobs": [dict(r) for r in logs]
        }
        return status
    finally:
        conn.close()


if __name__ == "__main__":
    # Run a single cycle for testing
    print("[SCHEDULER] Running single job cycle for testing...")
    run_ingestion_job()
    run_analysis_job()
    run_report_job()
    print("[SCHEDULER] Test cycle complete.")
