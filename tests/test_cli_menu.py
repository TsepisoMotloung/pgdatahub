import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
import yaml

# ensure repo root on path so `src` package imports work in tests
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.main import interactive_menu
from src.etl import run


def write_excel(path, sheet_name, df):
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)


def test_interactive_menu_revert_import_dry_run(tmp_path, monkeypatch, capsys):
    # Prepare data and run ETL to create an import
    data_root = tmp_path / "Data"
    table_folder = data_root / "pol"
    table_folder.mkdir(parents=True)
    df1 = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    write_excel(table_folder / "file1.xlsx", "Sheet1", df1)
    cfg = {"pol": {"sheet_name": "Sheet1"}}
    cfg_path = tmp_path / "etl_config.yaml"
    with open(cfg_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh)

    db_file = tmp_path / "test_menu.db"
    db_url = f"sqlite:///{db_file}"
    db_conf = {"url": db_url}

    # run ETL twice so file1 is imported
    run(data_root=str(data_root), etl_config_path=str(cfg_path), db_config=db_conf)

    # find sha and source file
    engine = create_engine(db_url)
    with engine.connect() as conn:
        row = conn.execute(text("SELECT file_sha256, source_file FROM etl_imports WHERE table_name='pol'"))
        file_sha, source = row.fetchone()

    # Simulate interactive inputs: choose Revert -> Revert import -> table -> sha -> Dry run -> Exit
    # choose revert -> revert import -> table -> identifier (provide filename) -> detect mode -> dry run -> (blank backups dir) -> exit
    inputs = iter(["2", "1", "pol", source, "detect", "y", "", "3"])
    monkeypatch.setattr("builtins.input", lambda prompt="": next(inputs))

    # Ensure interactive_menu uses the same DB
    monkeypatch.setenv("DATABASE_URL", db_url)

    # Run interactive menu
    interactive_menu()

    captured = capsys.readouterr()
    assert "DRY RUN" in captured.out

    # Ensure import still exists
    with engine.connect() as conn:
        cnt = conn.execute(text("SELECT COUNT(*) FROM etl_imports WHERE file_sha256 = :h"), {"h": file_sha}).scalar()
    assert cnt == 1


def test_interactive_menu_revert_import_apply(tmp_path, monkeypatch):
    # Prepare data and run ETL to create an import
    data_root = tmp_path / "Data"
    table_folder = data_root / "pol"
    table_folder.mkdir(parents=True)
    df1 = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    write_excel(table_folder / "file1.xlsx", "Sheet1", df1)
    cfg = {"pol": {"sheet_name": "Sheet1"}}
    cfg_path = tmp_path / "etl_config.yaml"
    with open(cfg_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh)

    db_file = tmp_path / "test_menu_apply.db"
    db_url = f"sqlite:///{db_file}"
    db_conf = {"url": db_url}

    run(data_root=str(data_root), etl_config_path=str(cfg_path), db_config=db_conf)

    engine = create_engine(db_url)
    with engine.connect() as conn:
        row = conn.execute(text("SELECT file_sha256, source_file FROM etl_imports WHERE table_name='pol'"))
        file_sha, source = row.fetchone()
    # choose revert -> revert import -> table -> identifier (provide filename) -> detect mode -> apply revert -> blank backups dir -> confirm -> exit
    inputs = iter(["2", "1", "pol", source, "detect", "n", "", "y", "3"])
    monkeypatch.setattr("builtins.input", lambda prompt="": next(inputs))

    # Ensure interactive_menu uses the same DB
    monkeypatch.setenv("DATABASE_URL", db_url)

    # Run interactive menu
    interactive_menu()

    # After applying revert, import entry should be removed and table empty
    with engine.connect() as conn:
        c1 = conn.execute(text("SELECT COUNT(*) FROM etl_imports WHERE file_sha256 = :h"), {"h": file_sha}).scalar()
        c2 = conn.execute(text("SELECT COUNT(*) FROM pol")).scalar()
    assert c1 == 0
    assert c2 == 0
