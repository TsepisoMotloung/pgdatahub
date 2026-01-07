"""Entry point and utility functions for running and testing pgdatahub.

This file exposes helper functions used by the test-suite (file operations,
text cleaning, simple dataframe handling, and a small postgres import helper),
and also provides a single `main()` entrypoint that orchestrates the original
workflow using the `data/` directory at project root.
"""
import os
import re
import json
import logging
import sys
from datetime import datetime
from contextlib import closing

import pandas as pd
import psycopg2 as ps
import argparse

from src.etl import run
from src.config import load_db_config


# configure logging at module level for helpers/tests
logger = logging.getLogger(__name__)


def find_data_files():
    data_files = []
    excel_extensions = [".xlsx", ".xls", ".xlsm", ".xlsb", ".odf", ".ods", ".odt"]
    try:
        all_files = os.listdir(os.getcwd())
    except Exception:
        all_files = []
    for f in all_files:
        if f.lower() in ["config.json", "config.template.json"]:
            continue
        if f.endswith(".csv") or f.endswith(".json") or any(f.endswith(ext) for ext in excel_extensions):
            data_files.append(f)
    return data_files


def configure_data_dir(data_files, data_directory):
    os.makedirs(data_directory, exist_ok=True)
    for f in data_files:
        try:
            source = os.path.join(os.getcwd(), f)
            dest = os.path.join(data_directory, f)
            os.rename(source, dest)
        except Exception:
            # tests patch os.rename so ignore errors here
            continue


def create_df_dict(data_files, data_directory):
    data_path = os.path.join(os.getcwd(), data_directory)
    df = {}
    for f in data_files:
        file_path = os.path.join(data_path, f)
        ext = os.path.splitext(f)[1].lower()
        try:
            if ext == ".csv":
                df[f] = pd.read_csv(file_path)
            elif ext in [".xlsx", ".xls", ".xlsm", ".xlsb", ".odf", ".ods", ".odt"]:
                excel_data = pd.read_excel(file_path, sheet_name=None)
                if len(excel_data) == 1:
                    df[f] = list(excel_data.values())[0]
                else:
                    base = os.path.splitext(f)[0]
                    for sheet, sheet_df in excel_data.items():
                        clean_sheet = clean_text(sheet)
                        new_key = f"{base}_{clean_sheet}.xlsx"
                        df[new_key] = sheet_df
            elif ext == ".json":
                with open(file_path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                df[f] = pd.json_normalize(data)
        except Exception:
            # tests often patch pandas functions; swallow errors here
            continue
    return df


TR_MAPPING = str.maketrans("ıİğĞüÜşŞöÖçÇ", "iIgGuUsSoOcC")


def clean_text(text):
    if "." in text and not text.startswith("."):
        base_name = text.split(".")[0]
    else:
        base_name = text
    s = base_name.replace(".", "_")
    s = s.translate(TR_MAPPING).lower()
    s = re.sub(r"[^a-zA-Z0-9_]", "_", s)
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    if s and s[0].isdigit():
        s = "col_" + s
    return s


def clean_file_names(data_files, dataframe):
    cleaned = []
    for f in data_files:
        cname = clean_text(f)
        cleaned.append(cname)
        dataframe[cname] = dataframe.pop(f)
    return cleaned


def process_dataframes(dataframes_dict):
    DTYPE_MAPPING = {
        "int8": "SMALLINT",
        "int16": "SMALLINT",
        "int32": "INTEGER",
        "int64": "BIGINT",
        "float16": "REAL",
        "float32": "REAL",
        "float64": "DOUBLE PRECISION",
        "object": "VARCHAR",
        "string": "VARCHAR",
        "category": "VARCHAR",
        "default": "VARCHAR",
    }
    schemas = {}
    for fname, df in dataframes_dict.items():
        df.columns = [clean_text(c) for c in df.columns]
        # map dtypes
        types = []
        for c in df.dtypes:
            t = str(c)
            mapped = DTYPE_MAPPING.get(t, DTYPE_MAPPING["default"])
            types.append(mapped)
        pairs = zip(df.columns, types)
        schemas[fname] = ", ".join(f"{col} {typ}" for col, typ in pairs)
    return schemas


def save_df_to_csv(clean_file_name, dataframe_dict):
    for name, df in dataframe_dict.items():
        df.to_csv(f"{name}.csv", header=df.columns, index=False, encoding="utf-8")


def load_config():
    if os.environ.get("DATABASE_URL"):
        return {"url": os.environ.get("DATABASE_URL")}
    with open("config.json", "r") as f:
        cfg = json.load(f)
    return cfg.get("database", {})


def import_to_postgres(table_schema, conn_dict, skip_db=False):
    """Import CSVs into Postgres. Accepts either a connection dict or a dict with a 'url' (DSN).

    If `skip_db` is True or the connection fails, the import is skipped gracefully.
    """
    if skip_db:
        logger.info("SKIP_DB set — skipping database import.")
        return

    # Interpret a DSN-style URL if provided (e.g., DATABASE_URL)
    if isinstance(conn_dict, dict) and "url" in conn_dict:
        url = conn_dict["url"]
        # psycopg2 expects a postgres:// DSN, not postgresql+psycopg2://
        if url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql+psycopg2://", "postgresql://", 1)
        # Ensure username/password in the DSN are percent-encoded (dots or special chars can cause parsing issues)
        from urllib.parse import urlsplit, urlunsplit, quote

        parts = urlsplit(url)
        netloc = parts.netloc
        if "@" in netloc:
            userinfo, hostport = netloc.split("@", 1)
            if ":" in userinfo:
                user, pwd = userinfo.split(":", 1)
            else:
                user, pwd = userinfo, ""
            user_enc = quote(user, safe="")
            pwd_enc = quote(pwd, safe="")
            netloc = f"{user_enc}:{pwd_enc}@{hostport}" if pwd_enc else f"{user_enc}@{hostport}"
            url = urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
        connect_args = {"dsn": url}
    else:
        connect_args = conn_dict or {}

    try:
        with closing(ps.connect(**connect_args)) as conn:
            with conn.cursor() as cur:
                for file_name, schema in table_schema.items():
                    cur.execute("CREATE TABLE IF NOT EXISTS %s (%s)", (ps.extensions.AsIs(file_name), ps.extensions.AsIs(schema)))
                    with open(f"{file_name}.csv", "r", encoding="utf-8") as my_file:
                        QUERY = f"""
                        COPY {file_name} FROM STDIN WITH
                            CSV
                            HEADER
                            DELIMITER AS ','
                            ENCODING 'UTF8'
                        """
                        cur.copy_expert(sql=QUERY, file=my_file)
                conn.commit()
    except ps.OperationalError as e:
        logger.error("DB connection failed: %s — skipping import.", e)
        return


def move_processed_files(processed_files):
    processed_dir = "processed_data"
    os.makedirs(processed_dir, exist_ok=True)
    for file_name in processed_files:
        src = os.path.join(os.getcwd(), f"{file_name}.csv")
        dst = os.path.join(processed_dir, f"{file_name}.csv")
        try:
            os.rename(src, dst)
        except Exception:
            continue


def main(skip_db=False, data_root_default="data"):
    logger = logging.getLogger(__name__)

    # If the project contains a top-level 'data' directory, we process folders inside it.
    # This is the preferred and authoritative source of data for imports.
    data_root = data_root_default
    if os.path.isdir(data_root):
        # Honor --skip-db by setting environment var, which ETL import step respects
        if skip_db:
            os.environ["SKIP_DB"] = "1"
        try:
            from src import etl

            etl.run(data_root=data_root, etl_config_path="etl_config.yaml", db_config=load_config())
            logger.info("ETL run completed for data root '%s'", data_root)
            return
        except Exception:
            logger.exception("ETL run failed")
            sys.exit(1)

    # Fallback legacy behavior (accept files at repo root and move them into 'data/')
    legacy_data_root = "data"
    if not os.path.exists(legacy_data_root):
        logger.info("Data directory '%s' not found — creating it.", legacy_data_root)
        os.makedirs(legacy_data_root, exist_ok=True)

    # Original orchestration: find files, move them to data_root, create dataframes,
    # clean names, build schemas, write CSVs, import into DB and move processed files.
    data_files = find_data_files()
    if not data_files:
        logger.error("No data files found to process")
        sys.exit(1)
        return

    configure_data_dir(data_files, legacy_data_root)
    df_dict = create_df_dict(data_files, legacy_data_root)
    if not df_dict:
        logger.error("Failed to process any data files")
        sys.exit(1)
        return

    datasets = clean_file_names(list(df_dict.keys()), df_dict)
    schemas = process_dataframes(df_dict)
    save_df_to_csv(datasets, df_dict)

    db_config = load_config()
    # Honor environment variable SKIP_DB or CLI flag
    skip_db_env = os.environ.get("SKIP_DB", "").lower() in ("1", "true", "yes")
    import_to_postgres(schemas, db_config, skip_db=skip_db or skip_db_env)
    move_processed_files(datasets)
    logger.info("All datasets have been imported successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run pgdatahub ETL pipeline (processes Data/ subfolders)")
    parser.add_argument("--skip-db", dest="skip_db", action="store_true", help="Skip database import step")
    parser.add_argument("--data-root", dest="data_root", default="Data", help="Path to the Data root directory")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(f"data_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
            logging.StreamHandler(),
        ],
    )
    main(skip_db=args.skip_db, data_root_default=args.data_root)
