import os
import glob
import logging
from datetime import datetime
import pandas as pd
from .config import load_etl_config, get_sheet_name_for_folder
from .utils import normalize_dataframe_columns, utcnow_iso
from .db import (
    get_engine,
    ensure_schema_table,
    reflect_table_columns,
    create_table_from_df,
    add_column,
    alter_column_type,
    log_schema_change,
    pandas_dtype_to_sqlalchemy,
    insert_dataframe,
    is_numeric_sql_type,
    is_safe_widening,
)

logger = logging.getLogger(__name__)

EXCEL_EXTS = [".xlsx", ".xls", ".xlsm", ".xlsb"]


def find_table_folders(data_root="Data"):
    if not os.path.isdir(data_root):
        return []
    # Walk one level deep and gather folders (including nested)
    folders = []
    for root, dirs, files in os.walk(data_root):
        for d in dirs:
            folders.append(os.path.join(root, d))
    return folders


class FolderPaused(Exception):
    """Raised when processing of a folder is paused due to an error and a pause file is written."""


def _write_pause_file(pause_file, payload):
    try:
        import json

        with open(pause_file, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
    except Exception:
        logger.exception("Failed to write pause file %s", pause_file)


def _read_pause_file(pause_file):
    import json

    try:
        with open(pause_file, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def process_folder(engine, cfg, folder_path, sectional_commit=False, pause_file=".etl_pause.json", cool_down_seconds=0, pause_every=None, pause_seconds=0, file_whitelist=None):
    # Determine the config sheet name for the folder
    # compute parts relative to data root 'Data'
    parts = [p for p in folder_path.split(os.sep) if p]
    # Match data root case-insensitively (supports both 'Data' and 'data')
    data_idx = next((i for i, p in enumerate(parts) if p.lower() == "data"), None)
    if data_idx is not None:
        relative_parts = parts[data_idx + 1 :]
    else:
        # fallback to last part only
        relative_parts = [os.path.basename(folder_path)]
    logger.debug("Resolved relative parts %s for folder %s", relative_parts, folder_path)
    sheet_name = get_sheet_name_for_folder(cfg, relative_parts)
    table_name = os.path.basename(folder_path)

    logger.debug("Processing folder %s -> table %s, sheet %s", folder_path, table_name, sheet_name)

    if not sheet_name:
        logger.info("No sheet configured for folder %s — skipping", folder_path)
        return

    # find excel files in this folder
    files = []
    for ext in EXCEL_EXTS:
        pattern = os.path.join(folder_path, f"*{ext}")
        files.extend(glob.glob(pattern))

    # apply whitelist filter if provided (match by basename)
    if file_whitelist:
        filtered = [f for f in files if os.path.basename(f) in set(file_whitelist)]
        logger.debug("Filtering files with whitelist %s: %d -> %d", file_whitelist, len(files), len(filtered))
        files = filtered
        if not files:
            logger.info("No excel files match whitelist in %s — skipping", folder_path)
            return

    if not files:
        logger.info("No excel files found in %s", folder_path)
        return

    # Ensure schema log table and import log exist
    ensure_schema_table(engine)
    try:
        from .db import ensure_imports_table, is_imported, log_import

        ensure_imports_table(engine)
    except Exception:
        # if imports table cannot be created for some reason, continue but warn
        logger.warning("Could not ensure imports table exists")

    # If sectional_commit is requested, use a single connection/transaction for the whole folder
    if sectional_commit:
        import time

        with engine.connect() as conn:
            trans = conn.begin()
            try:
                file_count = 0
                for f in files:
                    # Isolate each file in its own try-except to prevent cascading failures
                    try:
                        # Read only the configured sheet; skip if missing
                        try:
                            df = pd.read_excel(f, sheet_name=sheet_name)
                        except ValueError as e:
                            # sheet missing or cannot be found
                            logger.warning("Skipping file %s — sheet '%s' not found", f, sheet_name)
                            continue

                        # compute file sha to detect duplicates
                        import hashlib

                        with open(f, "rb") as fh:
                            file_bytes = fh.read()
                        file_sha = hashlib.sha256(file_bytes).hexdigest()

                        # skip if already imported exactly
                        try:
                            if is_imported(engine, table_name, os.path.basename(f), file_sha):
                                logger.info("Skipping %s — already imported (same file hash)", f)
                                # count this file as processed for pause logic
                                if pause_every and pause_every > 0:
                                    file_count += 1
                                    if file_count % pause_every == 0:
                                        trans.commit()
                                        logger.info("Processed %d files in folder %s; pausing for %s seconds", file_count, folder_path, pause_seconds)
                                        if pause_seconds and pause_seconds > 0:
                                            time.sleep(int(pause_seconds))
                                        trans = conn.begin()
                                continue
                        except Exception:
                            logger.warning("Could not check import log for %s", f)

                        df = normalize_dataframe_columns(df)
                        # add metadata
                        df["source_file"] = os.path.basename(f)
                        df["load_timestamp"] = utcnow_iso()

                        # get existing table columns (use same conn so uncommitted DDL is visible)
                        existing_cols = reflect_table_columns(engine, table_name, conn=conn)

                        if not existing_cols:
                            # create table inside transaction
                            create_table_from_df(engine, table_name, df, conn=conn)
                            log_schema_change(
                                engine,
                                table_name,
                                None,
                                None,
                                None,
                                "create_table",
                                f"CREATE TABLE from dataframe: {table_name}",
                                os.path.basename(f),
                                conn=conn,
                            )
                        else:
                            # compare columns
                            df_cols = list(df.columns)
                            new_cols = [c for c in df_cols if c not in existing_cols]
                            for c in new_cols:
                                # Determine dtype robustly even if df[c] is a DataFrame due to duplicate column names
                                col = df[c]
                                if isinstance(col, pd.DataFrame):
                                    dtypes = set(str(x) for x in col.dtypes)
                                    dtype = dtypes.pop() if len(dtypes) == 1 else "object"
                                else:
                                    dtype = str(col.dtype)
                                # map to SQL (use default SQL strings for simplicity)
                                sql_type = "TEXT"
                                if "int" in dtype:
                                    sql_type = "INTEGER"
                                if "float" in dtype:
                                    sql_type = "DOUBLE PRECISION"
                                if "datetime" in dtype:
                                    sql_type = "TIMESTAMP"
                                if "bool" in dtype:
                                    sql_type = "BOOLEAN"
                                q = add_column(engine, table_name, c, sql_type, conn=conn)
                                log_schema_change(
                                    engine,
                                    table_name,
                                    c,
                                    None,
                                    sql_type,
                                    "add_column",
                                    q,
                                    os.path.basename(f),
                                    conn=conn,
                                )

                            # check for safe type widenings
                            for c in df.columns:
                                if c in existing_cols:
                                    old_type = existing_cols[c]
                                    col = df[c]
                                    if isinstance(col, pd.DataFrame):
                                        dtypes = set(str(x) for x in col.dtypes)
                                        new_type = dtypes.pop() if len(dtypes) == 1 else "object"
                                    else:
                                        new_type = str(col.dtype)
                                    # we map types to simple SQL strings similar to above
                                    target_type = "TEXT"
                                    if "int" in new_type:
                                        target_type = "INTEGER"
                                    if "float" in new_type:
                                        target_type = "DOUBLE PRECISION"
                                    if "datetime" in new_type:
                                        target_type = "TIMESTAMP"
                                    # if old_type differs and is safe widening -> alter
                                    if old_type and old_type.upper() != target_type.upper():
                                        if is_safe_widening(old_type, target_type):
                                            q = alter_column_type(engine, table_name, c, target_type, conn=conn)
                                            log_schema_change(
                                                engine,
                                                table_name,
                                                c,
                                                old_type,
                                                target_type,
                                                "alter_type",
                                                q,
                                                os.path.basename(f),
                                                conn=conn,
                                            )

                            # If the existing table has numeric types but incoming data contains
                            # non-numeric values, alter the column to TEXT so the insert won't fail
                            for c in df.columns:
                                if c in existing_cols:
                                    old_type = existing_cols[c]
                                    if is_numeric_sql_type(old_type):
                                        # check whether there are any non-numeric values in the column
                                        s = df[c].dropna()
                                        if not s.empty:
                                            coerced = pd.to_numeric(s, errors="coerce")
                                            num_non_numeric = int(coerced.isna().sum())
                                            if num_non_numeric > 0:
                                                try:
                                                    q = alter_column_type(engine, table_name, c, "TEXT", conn=conn)
                                                    log_schema_change(
                                                        engine,
                                                        table_name,
                                                        c,
                                                        old_type,
                                                        "TEXT",
                                                        "alter_type",
                                                        q,
                                                        os.path.basename(f),
                                                        conn=conn,
                                                    )
                                                except Exception as e:
                                                    # If we cannot alter the column (eg sqlite), coerce values to
                                                    # string in the dataframe so they can be inserted without
                                                    # failing parameter type checks.
                                                    logger.warning(
                                                        "Could not alter column %s to TEXT: %s. Coercing values to str in dataframe.",
                                                        c,
                                                        e,
                                                    )
                                                    df[c] = df[c].astype(str)

                        # Finally insert rows
                        insert_dataframe(engine, table_name, df, conn=conn)
                        logger.info("Inserted data from %s into table %s", f, table_name)

                        # log import to avoid re-importing identical files later
                        try:
                            from .db import log_import

                            row_count = len(df.index)
                            log_import(engine, table_name, os.path.basename(f), file_sha, row_count, conn=conn)
                        except Exception:
                            logger.warning("Failed to log import for %s", f)

                        # handle pause points if requested
                        if pause_every and pause_every > 0:
                            file_count += 1
                            if file_count % pause_every == 0:
                                # commit current transaction so the DB is in a consistent state
                                trans.commit()
                                logger.info("Processed %d files in folder %s; pausing for %s seconds", file_count, folder_path, pause_seconds)
                                if pause_seconds and pause_seconds > 0:
                                    time.sleep(int(pause_seconds))
                                # begin a new transaction for remaining files
                                trans = conn.begin()

                    except Exception as e:
                        logger.exception("Failed processing file %s: %s", f, e)
                        # Rollback the folder transaction and write pause file
                        trans.rollback()
                        payload = {
                            "folder": folder_path,
                            "table": table_name,
                            "error": str(e),
                            "file": os.path.basename(f),
                            "timestamp": datetime.utcnow().isoformat(sep=" ", timespec="seconds"),
                        }
                        _write_pause_file(pause_file, payload)
                        raise FolderPaused(f"Folder {folder_path} paused due to error: {e}")

                # commit folder-level transaction
                trans.commit()
                logger.info("Committed folder %s; cooling down for %s seconds", folder_path, cool_down_seconds)
                if cool_down_seconds and cool_down_seconds > 0:
                    time.sleep(int(cool_down_seconds))

            except FolderPaused:
                # propagate folder pause upward
                raise
            except Exception as e:
                trans.rollback()
                logger.exception("Folder %s failed and transaction rolled back: %s", folder_path, e)
                payload = {
                    "folder": folder_path,
                    "table": table_name,
                    "error": str(e),
                    "file": None,
                    "timestamp": datetime.utcnow().isoformat(sep=" ", timespec="seconds"),
                }
                _write_pause_file(pause_file, payload)
                raise FolderPaused(f"Folder {folder_path} paused due to error: {e}")

        # done
        return


def resume_from_pause(engine, cfg, pause_file=".etl_pause.json"):
    info = _read_pause_file(pause_file)
    if not info:
        raise ValueError("No pause file found or file invalid")
    folder = info.get("folder")
    if not folder:
        raise ValueError("Pause file missing folder information")
    # Attempt to reprocess the paused folder in sectional mode
    try:
        process_folder(engine, cfg, folder, sectional_commit=True, pause_file=pause_file)
        # on success, remove pause file
        try:
            os.remove(pause_file)
        except Exception:
            logger.warning("Could not remove pause file %s after successful resume", pause_file)
        return True
    except FolderPaused as e:
        logger.info("Folder %s still paused after resume attempt: %s", folder, e)
        return False


def run(data_root="Data", etl_config_path=None, db_config=None, sectional_commit=False, cool_down_seconds=0, resume=False, pause_file=".etl_pause.json", pause_every=None, pause_seconds=0):
    cfg = load_etl_config(etl_config_path)
    engine = get_engine(db_config)

    # If resume requested, attempt it first and then continue (if it succeeds)
    if resume:
        try:
            resumed = resume_from_pause(engine, cfg, pause_file=pause_file)
            if not resumed:
                logger.info("Resume attempt found the folder still paused; aborting run")
                return
        except Exception as e:
            logger.exception("Resume failed to start: %s", e)
            return

    folders = find_table_folders(data_root)
    if not folders:
        logger.info("No folders found under %s", data_root)
        return

    for folder in folders:
        try:
            process_folder(
                engine,
                cfg,
                folder,
                sectional_commit=sectional_commit,
                pause_file=pause_file,
                cool_down_seconds=cool_down_seconds,
                pause_every=pause_every,
                pause_seconds=pause_seconds,
            )
        except FolderPaused:
            # If a folder paused, stop the run so operator can inspect and resume later
            logger.warning("Processing paused at folder %s. See %s for details. Halting run.", folder, pause_file)
            return
        except Exception:
            logger.exception("Folder %s failed. Continuing with others.", folder)
            # ADDED: Dispose engine on folder-level failure too
            try:
                engine.dispose()
            except:
                pass
            continue