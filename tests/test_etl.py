import os
import sys
import pandas as pd
from sqlalchemy import text
import yaml
from sqlalchemy import create_engine, inspect
from pathlib import Path

# ensure repo root on path so `src` package imports work in tests
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.etl import run


def write_excel(path, sheet_name, df):
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)


def test_folder_creates_table_and_logs_schema_changes(tmp_path):
    # Setup data folder
    data_root = tmp_path / "Data"
    table_folder = data_root / "mytable"
    table_folder.mkdir(parents=True)

    # create an excel file with configured sheet
    df1 = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    excel_path = table_folder / "file1.xlsx"
    write_excel(excel_path, "Sheet1", df1)

    # write config
    cfg = {"mytable": {"sheet_name": "Sheet1"}}
    cfg_path = tmp_path / "etl_config.yaml"
    with open(cfg_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh)

    # use sqlite file DB so we can inspect from another connection
    db_file = tmp_path / "test.db"
    db_url = f"sqlite:///{db_file}"
    db_conf = {"url": db_url}

    # run etl
    run(data_root=str(data_root), etl_config_path=str(cfg_path), db_config=db_conf)

    # inspect DB
    engine = create_engine(db_url)
    inspector = inspect(engine)
    assert "mytable" in inspector.get_table_names()
    assert "etl_schema_changes" in inspector.get_table_names()

    cols = [c["name"] for c in inspector.get_columns("mytable")]
    # metadata columns exist
    assert "source_file" in cols
    assert "load_timestamp" in cols

    # now add another file with new column
    df2 = pd.DataFrame({"col1": [3], "col2": ["c"], "col_new": [9]})
    excel_path2 = table_folder / "file2.xlsx"
    write_excel(excel_path2, "Sheet1", df2)

    # re-run
    run(data_root=str(data_root), etl_config_path=str(cfg_path), db_config=db_conf)

    # check that column added
    inspector = inspect(engine)
    cols = [c["name"] for c in inspector.get_columns("mytable")]
    assert "col_new" in cols

    # check that schema changes logged
    with engine.connect() as conn:
        res = conn.execute(text("SELECT count(*) FROM etl_schema_changes WHERE change_type = 'add_column'"))
        count = res.scalar()
    assert count >= 1


def test_missing_sheet_skips_file(tmp_path):
    data_root = tmp_path / "Data"
    table_folder = data_root / "skiptable"
    table_folder.mkdir(parents=True)

    # create an excel file with a different sheet
    df = pd.DataFrame({"a": [1]})
    excel_path = table_folder / "file.xlsx"
    write_excel(excel_path, "OtherSheet", df)

    cfg = {"skiptable": {"sheet_name": "ExpectedSheet"}}
    cfg_path = tmp_path / "etl_config.yaml"
    with open(cfg_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh)

    db_file = tmp_path / "test2.db"
    db_url = f"sqlite:///{db_file}"
    db_conf = {"url": db_url}

    # run etl — should not create table
    run(data_root=str(data_root), etl_config_path=str(cfg_path), db_config=db_conf)

    engine = create_engine(db_url)
    inspector = inspect(engine)
    assert "skiptable" not in inspector.get_table_names()


def test_numeric_column_with_non_numeric_values_coerces_to_text(tmp_path):
    # Setup data folder
    data_root = tmp_path / "Data"
    table_folder = data_root / "pol"
    table_folder.mkdir(parents=True)

    # create initial excel file with numeric column which will create table
    df1 = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    excel_path = table_folder / "file1.xlsx"
    write_excel(excel_path, "Sheet1", df1)

    # write config
    cfg = {"pol": {"sheet_name": "Sheet1"}}
    cfg_path = tmp_path / "etl_config.yaml"
    with open(cfg_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh)

    # use sqlite file DB so we can inspect from another connection
    db_file = tmp_path / "test_numeric.db"
    db_url = f"sqlite:///{db_file}"
    db_conf = {"url": db_url}

    # run etl first time to create table
    run(data_root=str(data_root), etl_config_path=str(cfg_path), db_config=db_conf)

    # now add another file where col1 has non-numeric values (eg 'No')
    df2 = pd.DataFrame({"col1": ["No"], "col2": ["c"]})
    excel_path2 = table_folder / "file2.xlsx"
    write_excel(excel_path2, "Sheet1", df2)

    # re-run - should not raise and should insert the string value into the table
    run(data_root=str(data_root), etl_config_path=str(cfg_path), db_config=db_conf)

    # verify row inserted
    engine = create_engine(db_url)
    with engine.connect() as conn:
        res = conn.execute(text("SELECT col1 FROM pol WHERE col2 = 'c'"))
        val = res.scalar()
    # SQLite is permissive, but we expect to have inserted the string 'No'
    assert str(val) == 'No'
