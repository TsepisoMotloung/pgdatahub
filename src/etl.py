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


def process_folder(engine, cfg, folder_path):
    # Determine the config sheet name for the folder
    # compute parts relative to data root 'Data'
    parts = [p for p in folder_path.split(os.sep) if p]
    if "Data" in parts:
        idx = len(parts) - 1 - parts[::-1].index("Data")
        relative_parts = parts[idx + 1 :]
    else:
        # fallback to last part only
        relative_parts = [os.path.basename(folder_path)]
    sheet_name = get_sheet_name_for_folder(cfg, relative_parts)
    table_name = os.path.basename(folder_path)

    if not sheet_name:
        logger.info("No sheet configured for folder %s — skipping", folder_path)
        return

    # find excel files in this folder
    files = []
    for ext in EXCEL_EXTS:
        pattern = os.path.join(folder_path, f"*{ext}")
        files.extend(glob.glob(pattern))

    if not files:
        logger.info("No excel files found in %s", folder_path)
        return

    # Ensure schema log table exists
    ensure_schema_table(engine)

    for f in files:
        try:
            # Read only the configured sheet; skip if missing
            try:
                df = pd.read_excel(f, sheet_name=sheet_name)
            except ValueError as e:
                # sheet missing or cannot be found
                logger.warning("Skipping file %s — sheet '%s' not found", f, sheet_name)
                continue

            df = normalize_dataframe_columns(df)
            # add metadata
            df["source_file"] = os.path.basename(f)
            df["load_timestamp"] = utcnow_iso()

            # get existing table columns
            existing_cols = reflect_table_columns(engine, table_name)

            if not existing_cols:
                # create table
                create_table_from_df(engine, table_name, df)
                log_schema_change(
                    engine,
                    table_name,
                    None,
                    None,
                    None,
                    "create_table",
                    f"CREATE TABLE from dataframe: {table_name}",
                    os.path.basename(f),
                )
            else:
                # compare columns
                df_cols = list(df.columns)
                new_cols = [c for c in df_cols if c not in existing_cols]
                for c in new_cols:
                    dtype = str(df[c].dtype)
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
                    q = add_column(engine, table_name, c, sql_type)
                    log_schema_change(
                        engine,
                        table_name,
                        c,
                        None,
                        sql_type,
                        "add_column",
                        q,
                        os.path.basename(f),
                    )

                # check for safe type widenings
                for c in df.columns:
                    if c in existing_cols:
                        old_type = existing_cols[c]
                        new_type = str(df[c].dtype)
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
                                q = alter_column_type(engine, table_name, c, target_type)
                                log_schema_change(
                                    engine,
                                    table_name,
                                    c,
                                    old_type,
                                    target_type,
                                    "alter_type",
                                    q,
                                    os.path.basename(f),
                                )

            # Finally insert rows
            insert_dataframe(engine, table_name, df)
            logger.info("Inserted data from %s into table %s", f, table_name)

        except Exception as e:
            logger.exception("Failed processing file %s: %s", f, e)
            # continue with next file
            continue


# provide a small helper used above
from .db import is_safe_widening


def run(data_root="Data", etl_config_path=None, db_config=None):
    cfg = load_etl_config(etl_config_path)
    engine = get_engine(db_config)
    folders = find_table_folders(data_root)
    if not folders:
        logger.info("No folders found under %s", data_root)
        return

    for folder in folders:
        try:
            process_folder(engine, cfg, folder)
        except Exception:
            logger.exception("Folder %s failed. Continuing with others.", folder)
            continue
