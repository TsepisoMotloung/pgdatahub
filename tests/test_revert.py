import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
import yaml

# ensure repo root on path so `src` package imports work in tests
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.etl import run
from src.db import find_imports_by_sha, revert_import, find_imports_by_source, revert_import_by_source


def write_excel(path, sheet_name, df):
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)


def test_revert_import_by_file_sha(tmp_path):
    # Setup data folder
    data_root = tmp_path / "Data"
    table_folder = data_root / "pol"
    table_folder.mkdir(parents=True)

    # create an excel file with configured sheet
    df1 = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    excel_path = table_folder / "file1.xlsx"
    write_excel(excel_path, "Sheet1", df1)

    # write config
    cfg = {"pol": {"sheet_name": "Sheet1"}}
    cfg_path = tmp_path / "etl_config.yaml"
    with open(cfg_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh)

    # use sqlite file DB so we can inspect from another connection
    db_file = tmp_path / "test_revert.db"
    db_url = f"sqlite:///{db_file}"
    db_conf = {"url": db_url}

    # run etl to create table and import
    run(data_root=str(data_root), etl_config_path=str(cfg_path), db_config=db_conf)

    engine = create_engine(db_url)
    # ensure import logged
    with engine.connect() as conn:
        res = conn.execute(text("SELECT file_sha256, source_file FROM etl_imports WHERE table_name='pol'"))
        row = res.fetchone()
    assert row is not None
    file_sha, src = row[0], row[1]

    # dry run should report counts
    dry = revert_import(engine, "pol", file_sha, dry_run=True)
    assert isinstance(dry, list)
    assert dry[0]["count"] == 2

    # perform revert
    deleted = revert_import(engine, "pol", file_sha, dry_run=False)
    assert deleted == 2

    # check etl_imports row removed
    with engine.connect() as conn:
        res = conn.execute(text("SELECT COUNT(*) FROM etl_imports WHERE file_sha256 = :h"), {"h": file_sha})
        c = res.scalar()
    assert c == 0

    # check table empty
    with engine.connect() as conn:
        res = conn.execute(text("SELECT COUNT(*) FROM pol"))
        c2 = res.scalar()
    assert c2 == 0

    # Re-import file to create rows again
    run(data_root=str(data_root), etl_config_path=str(cfg_path), db_config=db_conf)

    # Now revert by source file name (conservative delete)
    dry2 = revert_import_by_source(engine, "pol", src, dry_run=True)
    assert len(dry2) == 1
    assert "planned_backup" in dry2[0]

    deleted2 = revert_import_by_source(engine, "pol", src, dry_run=False)
    assert deleted2 >= 1

    # Verify etl_imports entry removed and table rows removed
    with engine.connect() as conn:
        c1 = conn.execute(text("SELECT COUNT(*) FROM etl_imports WHERE source_file = :s"), {"s": src}).scalar()
        c2 = conn.execute(text("SELECT COUNT(*) FROM pol")).scalar()
    assert c1 == 0
    assert c2 == 0
