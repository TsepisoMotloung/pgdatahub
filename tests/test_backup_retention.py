import os
from pathlib import Path
import pandas as pd
import yaml
import time
import json

import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.etl import run
from src.db import backup_table_rows, get_backups_root


def test_backup_retention_max_files(tmp_path, monkeypatch):
    # ensure backups root is in tmp_path
    monkeypatch.setenv("ETL_BACKUPS_DIR", str(tmp_path / "backups"))
    monkeypatch.setenv("ETL_BACKUP_MAX_FILES", "2")

    data_root = tmp_path / "Data"
    table_folder = data_root / "pol"
    table_folder.mkdir(parents=True)
    df1 = pd.DataFrame({"col1": [1], "col2": ["a"]})
    xlsx = table_folder / "file1.xlsx"
    with pd.ExcelWriter(xlsx, engine="openpyxl") as writer:
        df1.to_excel(writer, sheet_name="Sheet1", index=False)

    cfg = {"pol": {"sheet_name": "Sheet1"}}
    cfg_path = tmp_path / "etl_config.yaml"
    with open(cfg_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh)

    db_file = tmp_path / "test_retention.db"
    db_url = f"sqlite:///{db_file}"
    db_conf = {"url": db_url}

    # run ETL to create table
    run(data_root=str(data_root), etl_config_path=str(cfg_path), db_config=db_conf)

    # create three backups manually
    p1, _ = backup_table_rows(__import__('sqlalchemy').create_engine(db_url), "pol")
    time.sleep(0.1)
    p2, _ = backup_table_rows(__import__('sqlalchemy').create_engine(db_url), "pol")
    time.sleep(0.1)
    p3, _ = backup_table_rows(__import__('sqlalchemy').create_engine(db_url), "pol")

    # list backups
    bs = sorted((tmp_path / "backups" / "pol").glob("pol_*.csv"))
    assert len(bs) == 2  # trimmed to 2 by ETL_BACKUP_MAX_FILES

    # manifest should contain entries for backups
    m = tmp_path / "backups" / "pol" / "manifest.json"
    assert m.exists()
    with open(m, "r", encoding="utf-8") as fh:
        arr = json.load(fh)
    assert len(arr) == 3  # manifest still retains records for each backup