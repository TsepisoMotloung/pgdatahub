import logging
from sqlalchemy import create_engine, MetaData, Table, Column, text
from sqlalchemy import Integer, BigInteger, Float, Numeric, String, DateTime, Date, Boolean, LargeBinary, Text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import column
from datetime import datetime
logger = logging.getLogger(__name__)


def get_engine(db_config: dict):
    # Accept either a URL string in db_config['url'] or a dict with keys
    if not db_config:
        raise ValueError("Database configuration not provided")
    if "url" in db_config:
        return create_engine(db_config["url"])
    user = db_config.get("user")
    password = db_config.get("password")
    host = db_config.get("host", "localhost")
    port = db_config.get("port", 5432)
    database = db_config.get("database")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)


def ensure_schema_table(engine):
    # Create the etl_schema_changes table in a database-agnostic way
    from sqlalchemy import Table, Column, Integer, Text, MetaData

    md = MetaData()
    table = Table(
        "etl_schema_changes",
        md,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("table_name", Text, nullable=False),
        Column("column_name", Text),
        Column("old_type", Text),
        Column("new_type", Text),
        Column("change_type", Text, nullable=False),
        Column("change_timestamp", DateTime),
        Column("migration_query", Text),
        Column("source_file", Text),
    )
    md.create_all(engine, tables=[table])


def ensure_imports_table(engine):
    """Create the etl_imports table to track processed source files.

    Columns:
    - id
    - table_name
    - source_file
    - file_sha256
    - row_count
    - imported_at
    """
    from sqlalchemy import Table, Column, Integer, Text, MetaData, TIMESTAMP, UniqueConstraint

    md = MetaData()
    table = Table(
        "etl_imports",
        md,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("table_name", Text, nullable=False),
        Column("source_file", Text, nullable=False),
        Column("file_sha256", Text, nullable=False),
        Column("row_count", Integer),
        Column("imported_at", DateTime),
        UniqueConstraint("table_name", "source_file", "file_sha256", name="u_etl_imports_file"),
    )
    md.create_all(engine, tables=[table])


def is_imported(engine, table_name, source_file, file_sha256):
    from sqlalchemy import text

    sql = text(
        "SELECT COUNT(1) as c FROM etl_imports WHERE table_name = :t AND source_file = :s AND file_sha256 = :h"
    )
    with engine.begin() as conn:
        r = conn.execute(sql, {"t": table_name, "s": source_file, "h": file_sha256}).fetchone()
        return bool(r and r[0])


def log_import(engine, table_name, source_file, file_sha256, row_count):
    from sqlalchemy import text

    sql = text(
        "INSERT INTO etl_imports (table_name, source_file, file_sha256, row_count, imported_at) VALUES (:t, :s, :h, :r, :i)"
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "t": table_name,
                "s": source_file,
                "h": file_sha256,
                "r": row_count,
                "i": datetime.utcnow(),
            },
        )


def reflect_table_columns(engine, table_name):
    md = MetaData()
    try:
        table = Table(table_name, md, autoload_with=engine)
    except Exception:
        return {}
    cols = {}
    for c in table.columns:
        cols[c.name] = str(c.type)
    return cols


def pandas_dtype_to_sqlalchemy(dtype):
    # dtype is a numpy/pandas dtype name
    t = str(dtype)
    if "int" in t:
        return Integer
    if "float" in t:
        return Float
    if "datetime" in t:
        return DateTime
    if "bool" in t:
        return Boolean
    if "object" in t or "str" in t:
        return String
    return String


def is_numeric_sql_type(sql_type: str) -> bool:
    """Return True if sql_type string represents a numeric SQL type."""
    if not sql_type:
        return False
    t = sql_type.upper()
    return any(x in t for x in ("DOUBLE", "INT", "REAL", "NUMERIC", "DECIMAL"))


def create_table_from_df(engine, table_name, df):
    # Create an empty table skeleton using pandas without inserting rows
    try:
        df.head(0).to_sql(table_name, engine, if_exists="fail", index=False)
        logger.info("Table %s created", table_name)
        return True
    except Exception as e:
        logger.error("Failed to create table %s: %s", table_name, e)
        raise


def add_column(engine, table_name, column_name, sqlalchemy_type):
    q = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {sqlalchemy_type}"
    with engine.begin() as conn:
        conn.execute(text(q))
    return q


# Very small set of safe widenings allowed
SAFE_WIDENINGS = [
    ("SMALLINT", "INTEGER"),
    ("INTEGER", "BIGINT"),
    ("INTEGER", "DOUBLE PRECISION"),
    ("REAL", "DOUBLE PRECISION"),
    ("VARCHAR", "TEXT"),
]


def is_safe_widening(old_type, new_type):
    o = old_type.upper() if old_type else ""
    n = new_type.upper() if new_type else ""
    return (o, n) in SAFE_WIDENINGS


def alter_column_type(engine, table_name, column_name, new_type):
    q = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {new_type} USING {column_name}::{new_type}"
    with engine.begin() as conn:
        conn.execute(text(q))
    return q


def log_schema_change(engine, table_name, column_name, old_type, new_type, change_type, migration_query, source_file=None):
    sql = text(
        "INSERT INTO etl_schema_changes (table_name, column_name, old_type, new_type, change_type, change_timestamp, migration_query, source_file)"
        " VALUES (:table_name, :column_name, :old_type, :new_type, :change_type, :change_timestamp, :migration_query, :source_file)"
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "table_name": table_name,
                "column_name": column_name,
                "old_type": old_type,
                "new_type": new_type,
                "change_type": change_type,
                "change_timestamp": datetime.utcnow(),
                "migration_query": migration_query,
                "source_file": source_file,
            },
        )


def _validate_table_name(table_name: str) -> bool:
    """Allow only simple table names (alphanumeric and underscores) to avoid SQL injection."""
    import re

    return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", table_name))


def find_imports_by_sha(engine, table_name, file_sha256):
    from sqlalchemy import text

    sql = text(
        "SELECT id, table_name, source_file, file_sha256, row_count, imported_at FROM etl_imports WHERE table_name = :t AND file_sha256 = :h"
    )
    with engine.begin() as conn:
        res = conn.execute(sql, {"t": table_name, "h": file_sha256}).fetchall()
        out = []
        for r in res:
            # SQLAlchemy Row supports ._mapping for dict-like access
            try:
                out.append(dict(r._mapping))
            except Exception:
                # fallback for older versions
                out.append({k: r[i] for i, k in enumerate(r.keys())})
        return out


def find_imports_by_source(engine, table_name, source_file):
    """Return all etl_imports rows matching a given table and source_file."""
    from sqlalchemy import text

    sql = text(
        "SELECT id, table_name, source_file, file_sha256, row_count, imported_at FROM etl_imports WHERE table_name = :t AND source_file = :s"
    )
    with engine.begin() as conn:
        res = conn.execute(sql, {"t": table_name, "s": source_file}).fetchall()
        out = []
        for r in res:
            try:
                out.append(dict(r._mapping))
            except Exception:
                out.append({k: r[i] for i, k in enumerate(r.keys())})
        return out


def get_backups_root() -> str:
    """Return backups root directory from env or default."""
    import os

    return os.environ.get("ETL_BACKUPS_DIR", "backups")


def record_backup_metadata(table_name: str, backup_path: str, row_count: int, reason: str = "import_revert", extra: dict | None = None):
    """Append metadata about backups into backups/<table>/manifest.json"""
    import json
    from pathlib import Path

    backups_dir = Path(get_backups_root()) / table_name
    backups_dir.mkdir(parents=True, exist_ok=True)
    manifest = backups_dir / "manifest.json"
    entry = {
        "backup_path": str(backup_path),
        "row_count": int(row_count),
        "reason": reason,
        "extra": extra or {},
    }
    if manifest.exists():
        try:
            with manifest.open("r", encoding="utf-8") as fh:
                arr = json.load(fh) or []
        except Exception:
            arr = []
    else:
        arr = []
    arr.append(entry)
    with manifest.open("w", encoding="utf-8") as fh:
        json.dump(arr, fh, indent=2)


def cleanup_old_backups(table_name: str, backups_root: str | None = None, retention_days: int | None = None, max_files: int | None = None):
    """Delete backups older than retention_days and trim to last max_files if set."""
    import os
    from pathlib import Path
    import time

    if backups_root is None:
        backups_root = get_backups_root()

    base = Path(backups_root) / table_name
    if not base.exists():
        return

    files = sorted([p for p in base.iterdir() if p.is_file() and p.suffix == ".csv"], key=lambda p: p.stat().st_mtime)

    # Remove by retention_days
    if retention_days is None:
        import os

        rd_env = os.environ.get("ETL_BACKUP_RETENTION_DAYS")
        try:
            retention_days = int(rd_env) if rd_env is not None else None
        except Exception:
            retention_days = None

    if retention_days is not None:
        cutoff = time.time() - (int(retention_days) * 86400)
        for p in list(files):
            if p.stat().st_mtime < cutoff:
                try:
                    p.unlink()
                    files.remove(p)
                    logger.info("Removed old backup %s due to retention policy", p)
                except Exception:
                    logger.exception("Failed to remove old backup %s", p)

    # Trim to max_files
    if max_files is None:
        import os

        mf_env = os.environ.get("ETL_BACKUP_MAX_FILES")
        try:
            max_files = int(mf_env) if mf_env is not None else None
        except Exception:
            max_files = None

    if max_files is not None and len(files) > int(max_files):
        # remove oldest until len(files) == max_files
        to_remove = files[: len(files) - int(max_files)]
        for p in to_remove:
            try:
                p.unlink()
                files.remove(p)
                logger.info("Removed old backup %s to enforce max_files=%s", p, max_files)
            except Exception:
                logger.exception("Failed to remove old backup %s", p)


def backup_table_rows(engine, table_name, where_clause=None, params=None, backups_root: str | None = None):
    """Backup rows from `table_name` matching optional where_clause to a timestamped CSV.

    Uses env var `ETL_BACKUPS_DIR` to decide root unless `backups_root` provided.
    Returns the path to the created CSV file and the number of rows backed up.
    """
    import pandas as pd
    from pathlib import Path
    import time
    import uuid

    if not _validate_table_name(table_name):
        raise ValueError("Invalid table name")

    if backups_root is None:
        backups_root = get_backups_root()

    q = f"SELECT * FROM {table_name}"
    if where_clause:
        q = q + " WHERE " + where_clause

    # Read into DataFrame
    try:
        df = pd.read_sql_query(q, engine, params=params)
    except Exception as e:
        logger.exception("Failed to read rows for backup from %s: %s", table_name, e)
        raise

    backups_dir = Path(backups_root) / table_name
    backups_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%dT%H%M%S")
    uniq = uuid.uuid4().hex[:8]
    fname = f"{table_name}_{ts}_{uniq}.csv"
    path = backups_dir / fname
    df.to_csv(path, index=False)
    logger.info("Backed up %s rows from %s to %s", len(df), table_name, path)

    # record metadata
    try:
        record_backup_metadata(table_name, path, len(df), reason="import_revert")
    except Exception:
        logger.exception("Failed to record backup metadata for %s", path)

    # run cleanup policy
    try:
        cleanup_old_backups(table_name, backups_root=backups_root)
    except Exception:
        logger.exception("Failed to run backup cleanup for %s", table_name)

    return str(path), int(len(df))


def revert_import_by_source(engine, table_name, source_file, dry_run=False, backups_root: str | None = None):
    """Revert an import identified by its table and source_file name.

    This is a conservative revert: it only deletes rows WHERE source_file = :s and removes matching etl_imports rows.
    It creates a backup of the affected rows first (unless dry_run).
    Dry-run returns a description of planned backups and counts; non-dry-run performs backup and deletion and returns total deleted rows.
    """
    from sqlalchemy import text
    from pathlib import Path

    if backups_root is None:
        backups_root = get_backups_root()

    if not _validate_table_name(table_name):
        raise ValueError("Invalid table name")

    imports = find_imports_by_source(engine, table_name, source_file)
    if not imports:
        raise ValueError("No matching import found for given table and source_file")

    results = []
    total_deleted = 0
    for imp in imports:
        src = imp.get("source_file")
        imp_id = imp.get("id")
        # Count rows in target table matching source_file
        count_sql = text(f"SELECT COUNT(1) as c FROM {table_name} WHERE source_file = :s")
        with engine.begin() as conn:
            cnt = conn.execute(count_sql, {"s": src}).scalar() or 0

        planned_backup_name = f"{table_name}_{src}.csv"
        planned_backup_path = str(Path(backups_root) / table_name / planned_backup_name)

        if dry_run:
            results.append({
                "table": table_name,
                "source_file": src,
                "count": int(cnt),
                "import_id": imp_id,
                "planned_backup": planned_backup_path,
            })
            continue

        # create backup then delete rows
        where = "source_file = :s"
        params = {"s": src}
        backup_path, backed_up = backup_table_rows(engine, table_name, where_clause=where, params=params, backups_root=backups_root)

        del_sql = text(f"DELETE FROM {table_name} WHERE source_file = :s")
        with engine.begin() as conn:
            res = conn.execute(del_sql, {"s": src})
            deleted = getattr(res, "rowcount", None)
            if deleted is None:
                deleted = cnt
            total_deleted += int(deleted)

        # remove import log rows matching this import id
        del_imp_sql = text("DELETE FROM etl_imports WHERE id = :id")
        with engine.begin() as conn:
            conn.execute(del_imp_sql, {"id": imp_id})

        results.append({"table": table_name, "source_file": src, "deleted": int(deleted), "backup": backup_path, "backed_up": backed_up})

    return results if dry_run else total_deleted


# Keep the sha-based revert for backward compatibility (delegates to source-based behavior)
def revert_import(engine, table_name, file_sha256=None, dry_run=False, backups_root: str | None = None):
    """Backward-compatible wrapper: prefer source-file based revert when a source_file is provided.

    If `file_sha256` looks like a 64-char hex string, will use the sha lookup. If a filename is passed, treat it as source_file.
    """
    import re

    if file_sha256 is None:
        raise ValueError("file_sha256 or source_file required")

    # if looks like a sha: 64 hex chars -> use sha mode
    if re.fullmatch(r"[0-9a-fA-F]{64}", str(file_sha256)):
        # existing behaviour
        return _revert_import_by_sha(engine, table_name, file_sha256, dry_run=dry_run, backups_root=backups_root)
    # else treat as source filename
    return revert_import_by_source(engine, table_name, str(file_sha256), dry_run=dry_run, backups_root=backups_root)


# Extracted original logic for sha-based revert to keep code clear
def _revert_import_by_sha(engine, table_name, file_sha256, dry_run=False, backups_root: str | None = None):
    from sqlalchemy import text
    from pathlib import Path

    if backups_root is None:
        backups_root = get_backups_root()

    if not _validate_table_name(table_name):
        raise ValueError("Invalid table name")

    imports = find_imports_by_sha(engine, table_name, file_sha256)
    if not imports:
        raise ValueError("No matching import found for given table and file_sha256")

    results = []
    total_deleted = 0
    for imp in imports:
        src = imp.get("source_file")
        imp_id = imp.get("id")
        # Count rows in target table matching source_file
        count_sql = text(f"SELECT COUNT(1) as c FROM {table_name} WHERE source_file = :s")
        with engine.begin() as conn:
            cnt = conn.execute(count_sql, {"s": src}).scalar() or 0
        # Determine planned backup path (do not create on dry-run)
        planned_backup_name = f"{table_name}_{src}_{file_sha256[:8]}.csv"
        planned_backup_path = str(Path(backups_root) / table_name / planned_backup_name)
        # For dry-run, report planned backup and counts
        if dry_run:
            results.append({
                "table": table_name,
                "source_file": src,
                "count": int(cnt),
                "import_id": imp_id,
                "planned_backup": planned_backup_path,
            })
            continue

        # Check if backup already exists for this import (by SHA fragment)
        backups_dir = Path(backups_root) / table_name
        existing = []
        if backups_dir.exists():
            for p in backups_dir.iterdir():
                if p.is_file() and p.name.startswith(f"{table_name}_") and file_sha256[:8] in p.name:
                    existing.append(p)

        if existing:
            backup_path = str(existing[-1])
            backed_up = sum(1 for _ in existing)  # count of candidate backups
            logger.info("Found existing backup for %s import at %s", src, backup_path)
        else:
            # Create backup of rows to be deleted
            where = "source_file = :s"
            params = {"s": src}
            backup_path, backed_up = backup_table_rows(engine, table_name, where_clause=where, params=params, backups_root=backups_root)

        # delete rows
        del_sql = text(f"DELETE FROM {table_name} WHERE source_file = :s")
        with engine.begin() as conn:
            res = conn.execute(del_sql, {"s": src})
            # Some drivers expose rowcount differently
            deleted = getattr(res, "rowcount", None)
            if deleted is None:
                # count again
                deleted = cnt
            total_deleted += int(deleted)
        # remove import log
        del_imp_sql = text("DELETE FROM etl_imports WHERE id = :id")
        with engine.begin() as conn:
            conn.execute(del_imp_sql, {"id": imp_id})
        # attach backup info to results for records
        results.append({"table": table_name, "source_file": src, "deleted": int(deleted), "backup": backup_path, "backed_up": backed_up})
    return results if dry_run else total_deleted


# -------------------- Schema revert helpers --------------------

def _validate_column_name(column_name: str) -> bool:
    import re

    return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", column_name))


def find_schema_changes_by_source(engine, table_name, source_file):
    """Return list of schema change rows matching table+source_file ordered by id asc."""
    from sqlalchemy import text

    if not _validate_table_name(table_name):
        raise ValueError("Invalid table name")
    sql = text(
        "SELECT id, table_name, column_name, old_type, new_type, change_type, migration_query, source_file FROM etl_schema_changes WHERE table_name = :t AND source_file = :s ORDER BY id ASC"
    )
    with engine.begin() as conn:
        res = conn.execute(sql, {"t": table_name, "s": source_file}).fetchall()
        out = []
        for r in res:
            try:
                out.append(dict(r._mapping))
            except Exception:
                out.append({k: r[i] for i, k in enumerate(r.keys())})
        return out


def revert_schema_changes(engine, table_name, source_file, dry_run=False, backups_root: str | None = None, skip_backup: bool = False):
    """Revert schema changes for a particular import (source_file).

    Dry-run returns list of actions; non-dry-run applies them in reverse order and removes
    the corresponding etl_schema_changes rows on success, returning the number of actions performed.

    If skip_backup is True, a pre-revert backup will not be created here (useful if caller created it).
    """
    from sqlalchemy import text

    if backups_root is None:
        backups_root = get_backups_root()

    changes = find_schema_changes_by_source(engine, table_name, source_file)
    if not changes:
        raise ValueError("No schema changes found for given table and source_file")

    # apply in reverse order
    actions = []
    performed = 0
    for ch in reversed(changes):
        ch_id = ch.get("id")
        c = ch.get("column_name")
        old = ch.get("old_type")
        new = ch.get("new_type")
        typ = ch.get("change_type")

        if typ == "add_column":
            action_sql = f"ALTER TABLE {table_name} DROP COLUMN {c}"
            actions.append({"action": "drop_column", "sql": action_sql, "change_id": ch_id, "column": c})
        elif typ == "alter_type":
            if not old:
                raise ValueError("Cannot revert alter_type without old_type recorded")
            action_sql = f"ALTER TABLE {table_name} ALTER COLUMN {c} TYPE {old} USING {c}::{old}"
            actions.append({"action": "alter_type", "sql": action_sql, "change_id": ch_id, "column": c, "old_type": old})
        elif typ == "create_table":
            action_sql = f"DROP TABLE IF EXISTS {table_name}"
            actions.append({"action": "drop_table", "sql": action_sql, "change_id": ch_id})
        else:
            # Unknown change type: include it in dry-run report but skip applying
            actions.append({"action": "unknown", "change_type": typ, "change_id": ch_id})

    if dry_run:
        # include planned backup info for schema reverts (backup whole table)
        for a in actions:
            a["planned_backup"] = f"{backups_root}/{table_name}/{table_name}_<timestamp>_<uniq>.csv"
        return actions

    # Execute actions
    # Optionally create a backup of the table state (full table) before applying schema reverts
    backup_path = None
    backed_up = 0
    if not skip_backup:
        try:
            backup_path, backed_up = backup_table_rows(engine, table_name, backups_root=backups_root)
        except Exception as e:
            logger.exception("Failed to create pre-revert backup for %s: %s", table_name, e)
            raise

    for act in actions:
        sql_text = text(act["sql"])
        try:
            with engine.begin() as conn:
                conn.execute(sql_text)
            performed += 1
            # remove schema change record
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM etl_schema_changes WHERE id = :id"), {"id": act.get("change_id")})
        except Exception as e:
            logger.exception("Failed to apply revert action %s: %s", act, e)
            raise

    # return number of actions performed
    return performed


def insert_dataframe(engine, table_name, df):
    try:
        df.to_sql(table_name, engine, if_exists="append", index=False, method="multi")
    except Exception as e:
        logger.error("Failed to insert rows into %s: %s", table_name, e)
        raise
