import json
import os
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
import pytest

# Ensure project root is on sys.path so 'src' imports work when running tests directly
sys.path.insert(0, os.getcwd())

from src import etl
from src.config import load_etl_config


def _make_folder(tmp_path: Path, name: str = "sectional"):
    data_dir = tmp_path / "data" / name
    data_dir.mkdir(parents=True)
    # create a placeholder excel file; actual contents are mocked by pandas.read_excel
    f = data_dir / "report.xlsx"
    f.write_text("placeholder")
    return str(data_dir)


def test_sectional_commit_pauses_on_error(tmp_path, monkeypatch):
    folder = _make_folder(tmp_path, "test_folder")
    pause_file = str(tmp_path / "pause.json")

    # Build an engine (sqlite in-memory)
    engine = create_engine("sqlite:///:memory:")
    cfg = load_etl_config()

    # Monkeypatch read_excel to return a small DataFrame
    monkeypatch.setattr(pd, "read_excel", lambda *args, **kwargs: pd.DataFrame({"a": [1, 2]}))

    # Ensure is_imported returns False so processing proceeds
    monkeypatch.setattr("src.db.is_imported", lambda *args, **kwargs: False)

    # Ensure sheet name is found (cfg may not include our ad-hoc folder)
    monkeypatch.setattr("src.etl.get_sheet_name_for_folder", lambda cfg, parts: "Sheet 1")

    # Make insert_dataframe raise an exception to simulate failure
    def fail_insert(engine_arg, table_name, df, conn=None):
        raise RuntimeError("simulated insert failure")

    # Patch the function reference used inside src.etl (it was imported at module import time)
    monkeypatch.setattr("src.etl.insert_dataframe", fail_insert)

    with pytest.raises(etl.FolderPaused):
        etl.process_folder(engine, cfg, folder, sectional_commit=True, pause_file=pause_file)

    # Pause file should be written and contain folder information
    assert os.path.exists(pause_file)
    with open(pause_file, "r", encoding="utf-8") as fh:
        obj = json.load(fh)
    assert obj.get("folder") == folder
    assert "simulated insert failure" in obj.get("error")


def test_resume_from_pause_removes_pause_and_retries(tmp_path, monkeypatch):
    folder = _make_folder(tmp_path, "resume_folder")
    pause_file = str(tmp_path / "pause_resume.json")

    # Write a pause file
    payload = {"folder": folder, "table": os.path.basename(folder), "error": "x", "file": None}
    with open(pause_file, "w", encoding="utf-8") as fh:
        json.dump(payload, fh)

    engine = create_engine("sqlite:///:memory:")
    cfg = load_etl_config()

    # When process_folder is called during resume, allow it to succeed (no exception)
    called = {}

    def fake_process_folder(engine_arg, cfg_arg, folder_arg, sectional_commit=False, pause_file=None, cool_down_seconds=0):
        called["ok"] = True
        return

    monkeypatch.setattr(etl, "process_folder", fake_process_folder)

    # resume_from_pause should call process_folder and remove pause file
    ok = etl.resume_from_pause(engine, cfg, pause_file=pause_file)
    assert ok is True
    assert not os.path.exists(pause_file)
    assert called.get("ok") is True
