import os
import sys
import pandas as pd
from sqlalchemy import create_engine, inspect, text
import yaml

# ensure repo root on path so `src` package imports work in tests
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.etl import run
from src.db import find_schema_changes_by_source, revert_schema_changes


def write_excel(path, sheet_name, df):
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)


def test_revert_schema_changes_for_added_column(tmp_path):
    data_root = tmp_path / "Data"
    table_folder = data_root / "pol"
    table_folder.mkdir(parents=True)

    # first file creates table
    df1 = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    write_excel(table_folder / "file1.xlsx", "Sheet1", df1)

    # config
    cfg = {"pol": {"sheet_name": "Sheet1"}}
    cfg_path = tmp_path / "etl_config.yaml"
    with open(cfg_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh)

    db_file = tmp_path / "test_revert_schema.db"
    db_url = f"sqlite:///{db_file}"
    db_conf = {"url": db_url}

    # run ETL once to create initial table from file1
    run(data_root=str(data_root), etl_config_path=str(cfg_path), db_config=db_conf)

    # now add file2 and run again so add_column is triggered
    df2 = pd.DataFrame({"col1": [3], "col2": ["c"], "col_new": [9]})
    write_excel(table_folder / "file2.xlsx", "Sheet1", df2)
    run(data_root=str(data_root), etl_config_path=str(cfg_path), db_config=db_conf)

    engine = create_engine(db_url)
    inspector = inspect(engine)

    # verify column exists
    cols = [c["name"] for c in inspector.get_columns("pol")]
    assert "col_new" in cols

    # find schema changes for file2.xlsx
    ch = find_schema_changes_by_source(engine, "pol", "file2.xlsx")
    assert any(c["change_type"] == "add_column" and c["column_name"] == "col_new" for c in ch)

    # dry run revert
    dry = revert_schema_changes(engine, "pol", "file2.xlsx", dry_run=True)
    assert any(a["action"] == "drop_column" and a["column"] == "col_new" for a in dry)

    # perform revert
    count = revert_schema_changes(engine, "pol", "file2.xlsx", dry_run=False)
    assert count >= 1

    # verify column removed
    inspector = inspect(engine)
    cols2 = [c["name"] for c in inspector.get_columns("pol")]
    assert "col_new" not in cols2

    # ensure schema change entry removed
    with engine.connect() as conn:
        res = conn.execute(text("SELECT COUNT(*) FROM etl_schema_changes WHERE source_file = 'file2.xlsx'"))
        c = res.scalar()
    assert c == 0
