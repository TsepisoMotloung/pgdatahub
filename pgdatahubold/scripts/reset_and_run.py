#!/usr/bin/env python3
"""Reset DB tables and optionally re-run ETL.

Usage examples:
  # backup + drop all tables + run ETL with sectional commits and pauses
  python scripts/reset_and_run.py --backup --run --sectional-commit --pause-every 3 --pause-seconds 30

  # drop only (confirm required)
  python scripts/reset_and_run.py

  # non-interactive (skip confirm)
  python scripts/reset_and_run.py --yes --run

Behavior:
- Loads DB config from DATABASE_URL env var or config.json (same as the app).
- If --backup is given and pg_dump is available, writes a dump file
  named backup_pgdatahub_YYYYmmdd_HHMMSS.sql in the current directory.
- Drops all tables in the public schema (or the tables specified with --tables).
- If --run is provided, executes `python main.py --data-root data` with
  the given ETL env options.
"""

import argparse
import json
import os
import shlex
import subprocess
import sys
from datetime import datetime

try:
    from src.config import load_db_config
    from src.db import get_engine
except Exception as e:
    print("Could not import project modules. Run this script from repository root.")
    raise


def run_pg_dump(cfg, out_path):
    # Construct pg_dump command from config dict (prefer DATABASE_URL)
    # Support cfg either as {'url': '...'} or dict with keys (user,password,host,port,database)
    cmd = None
    if cfg.get("url"):
        url = cfg["url"]
        # pg_dump accepts a libpq connection string via PGPASSWORD env or connection string
        # We'll pass it as the PG* env vars when possible.
        cmd = ["pg_dump", "--format=plain", "--file", out_path, url]
    else:
        user = cfg.get("user")
        password = cfg.get("password")
        host = cfg.get("host", "localhost")
        port = str(cfg.get("port", 5432))
        database = cfg.get("database")
        cmd = ["pg_dump", "--host", host, "--port", port, "--username", user, "--format=plain", "--file", out_path, database]
    try:
        env = os.environ.copy()
        if cfg.get("password"):
            env["PGPASSWORD"] = cfg.get("password")
        print("Running pg_dump ->", out_path)
        subprocess.run(cmd, check=True, env=env)
        print("pg_dump completed")
        return True
    except FileNotFoundError:
        print("pg_dump not found in PATH; skipping pg_dump backup")
        return False
    except subprocess.CalledProcessError as e:
        print("pg_dump failed:", e)
        return False


def list_tables(engine):
    from sqlalchemy import inspect

    ins = inspect(engine)
    return ins.get_table_names()


def drop_tables(engine, tables):
    from sqlalchemy import text

    if not tables:
        print("No tables to drop")
        return
    # Use a transaction
    with engine.begin() as conn:
        for t in tables:
            print("Dropping table:", t)
            conn.execute(text(f'DROP TABLE IF EXISTS "{t}" CASCADE'))
    print("Dropped tables:", ", ".join(tables))


def run_main(etl_sectional_commit, pause_every, pause_seconds, data_root="data"):
    # Build env for subprocess
    env = os.environ.copy()
    if etl_sectional_commit:
        env["ETL_SECTIONAL_COMMIT"] = "1"
    if pause_every is not None:
        env["ETL_PAUSE_EVERY"] = str(pause_every)
    if pause_seconds is not None:
        env["ETL_PAUSE_SECONDS"] = str(pause_seconds)
    cmd = [sys.executable, "main.py", "--data-root", data_root]
    print("Running:", " ".join(shlex.quote(x) for x in cmd))
    subprocess.run(cmd, check=True, env=env)


def main():
    parser = argparse.ArgumentParser(description="Backup, drop tables and optionally rerun ETL (main.py)")
    parser.add_argument("--backup", action="store_true", help="Run pg_dump to back up the database before dropping tables")
    parser.add_argument("--yes", action="store_true", help="Skip interactive confirmation")
    parser.add_argument("--run", action="store_true", help="Run main.py after dropping tables")
    parser.add_argument("--sectional-commit", action="store_true", help="Set ETL_SECTIONAL_COMMIT when running main.py")
    parser.add_argument("--pause-every", type=int, default=None, help="If running, set ETL_PAUSE_EVERY")
    parser.add_argument("--pause-seconds", type=int, default=0, help="If running, set ETL_PAUSE_SECONDS")
    parser.add_argument("--tables", nargs="*", help="Specific table names to drop (default: all tables in public schema)")
    parser.add_argument("--data-root", default="data", help="Data root to pass to main.py when --run is used")
    args = parser.parse_args()

    cfg = load_db_config()
    if not cfg:
        print("No database configuration found (DATABASE_URL or config.json). Aborting.")
        sys.exit(1)

    engine = get_engine(cfg)

    # Determine tables to drop
    if args.tables:
        tables = args.tables
    else:
        tables = list_tables(engine)

    if not tables:
        print("No tables found to drop. Exiting.")
        return

    print("About to drop tables:")
    for t in tables:
        print("  -", t)

    proceed = args.yes
    if not proceed:
        ans = input("Proceed and drop these tables? Type 'yes' to continue: ")
        proceed = ans.strip().lower() == "yes"

    if not proceed:
        print("Aborted by user")
        return

    # Optional backup
    if args.backup:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        out = f"backup_pgdatahub_{ts}.sql"
        ok = run_pg_dump(cfg, out)
        if not ok:
            print("Backup not completed or failed. Continue with drop? (type 'yes' to continue)")
            ans = input().strip().lower()
            if ans != "yes":
                print("Aborted")
                return

    # Drop tables
    drop_tables(engine, tables)

    # Optionally run ETL
    if args.run:
        try:
            run_main(args.sectional_commit, args.pause_every, args.pause_seconds, data_root=args.data_root)
        except subprocess.CalledProcessError as e:
            print("main.py failed with return code", e.returncode)
            raise


if __name__ == "__main__":
    main()
