import os
import pandas as pd
from sqlalchemy import create_engine, text
import yaml
import sys
import os

# ensure repo root on path so `src` package imports work in tests
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.etl import run
from src.db import backup_table_rows


def test_revert_import_creates_backup(tmp_path, monkeypatch):
    data_root = tmp_path / "Data"
    table_folder = data_root / "pol"
    table_folder.mkdir(parents=True)
    df1 = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    xlsx = table_folder / "file1.xlsx"
    with pd.ExcelWriter(xlsx, engine="openpyxl") as writer:
        df1.to_excel(writer, sheet_name="Sheet1", index=False)

    cfg = {"pol": {"sheet_name": "Sheet1"}}
    cfg_path = tmp_path / "etl_config.yaml"
    with open(cfg_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh)

    db_file = tmp_path / "test_revert_backup.db"
    db_url = f"sqlite:///{db_file}"
    db_conf = {"url": db_url}

    # run ETL to import
    run(data_root=str(data_root), etl_config_path=str(cfg_path), db_config=db_conf)

    engine = create_engine(db_url)
    with engine.connect() as conn:
        row = conn.execute(text("SELECT file_sha256, source_file FROM etl_imports WHERE table_name='pol'"))
        file_sha, source = row.fetchone()

    # Dry run should report planned_backup
    from src.db import revert_import
    dry = revert_import(engine, "pol", file_sha, dry_run=True)
    assert len(dry) == 1
    assert "planned_backup" in dry[0]

    # Apply revert should create actual backup file (sha mode)
    res = revert_import(engine, "pol", file_sha, dry_run=False)
    assert res >= 0

    # Re-import and apply revert by source filename (conservative)
    run(data_root=str(data_root), etl_config_path=str(cfg_path), db_config=db_conf)
    dry2 = revert_import(engine, "pol", source, dry_run=True)
    assert len(dry2) == 1
    assert "planned_backup" in dry2[0]
    res2 = revert_import(engine, "pol", source, dry_run=False)
    assert res2 >= 0

    # Find backup file in backups/pol
    found = False
    manifest = None
    for root, dirs, files in os.walk("backups"):
        if root.endswith("/pol") or root.endswith("\\pol"):
            for f in files:
                if f.startswith("pol_") and f.endswith(".csv"):
                    found = True
                    break
            if found:
                # manifest should exist
                import json

                m = os.path.join(root, "manifest.json")
                if os.path.exists(m):
                    with open(m, "r", encoding="utf-8") as fh:
                        manifest = json.load(fh)
                break
    assert found
    assert manifest is not None and len(manifest) >= 1


def test_revert_schema_creates_backup(tmp_path):
    # Setup as above
    data_root = tmp_path / "Data"
    table_folder = data_root / "pol"
    table_folder.mkdir(parents=True)
    df1 = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    xlsx = table_folder / "file1.xlsx"
    with pd.ExcelWriter(xlsx, engine="openpyxl") as writer:
        df1.to_excel(writer, sheet_name="Sheet1", index=False)

    cfg = {"pol": {"sheet_name": "Sheet1"}}
    cfg_path = tmp_path / "etl_config.yaml"
    with open(cfg_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh)

    db_file = tmp_path / "test_revert_schema.db"
    db_url = f"sqlite:///{db_file}"
    db_conf = {"url": db_url}

    # run ETL to create table and add any columns
    run(data_root=str(data_root), etl_config_path=str(cfg_path), db_config=db_conf)

    engine = create_engine(db_url)
    # Make a fake schema change log entry to revert
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO etl_schema_changes (table_name, column_name, old_type, new_type, change_type, change_timestamp, migration_query, source_file) VALUES ('pol', 'col2', 'VARCHAR', 'TEXT', 'add_column', CURRENT_TIMESTAMP, 'ALTER...', 'file1.xlsx')"))

    from src.db import revert_schema_changes
    # Dry run planned actions include planned_backup
    actions = revert_schema_changes(engine, "pol", "file1.xlsx", dry_run=True)
    assert any("planned_backup" in a for a in actions)

    res = revert_schema_changes(engine, "pol", "file1.xlsx", dry_run=False)
    assert isinstance(res, int)
    assert res >= 1

    # Find backup file in backups/pol
    found = False
    for root, dirs, files in os.walk("backups"):
        for f in files:
            if f.startswith("pol_") and f.endswith(".csv"):
                found = True
                break
        if found:
            break
    assert found