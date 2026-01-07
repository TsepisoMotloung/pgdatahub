import os
import pandas as pd
import shutil
import tempfile
from src.etl import process_folder
from src.db import get_engine


def test_process_folder_skips_already_imported(tmp_path):
    # Setup a temporary data root and folder
    data_root = tmp_path / "Data"
    folder = data_root / "test_table"
    folder.mkdir(parents=True)

    # Create a simple DataFrame and write to Excel with sheet 'mysheet'
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    file_path = folder / "file1.xlsx"
    df.to_excel(file_path, sheet_name="mysheet", index=False)

    # config: map folder to sheet
    cfg = {"test_table": {"sheet_name": "mysheet"}}

    # Use a persistent sqlite engine for testing
    engine = get_engine({"url": "sqlite:///:memory:"})

    # First run: should import
    process_folder(engine, cfg, str(folder))

    # Second run: should skip the same file (idempotent)
    process_folder(engine, cfg, str(folder))

    # Check imports table has only one entry
    from sqlalchemy import text
    with engine.begin() as conn:
        res = conn.execute(text("SELECT count(1) FROM etl_imports WHERE table_name = 'test_table'"))
        cnt = res.fetchone()[0]
        assert cnt == 1
