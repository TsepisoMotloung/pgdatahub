import os
import shutil
import sys
from pathlib import Path
import pandas as pd
import time
import pytest
from sqlalchemy import create_engine, text

# Ensure project root is on sys.path when running tests directly
sys.path.insert(0, os.getcwd())

from src import etl


pytestmark = pytest.mark.integration


def _write_xlsx(path: Path, df: pd.DataFrame, sheet_name: str = "Sheet 1"):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(path, sheet_name=sheet_name, index=False)


@pytest.mark.skipif("DATABASE_URL" not in os.environ, reason="DATABASE_URL not set")
def test_etl_runs_against_postgres(tmp_path: Path, monkeypatch):
    db_url = os.environ["DATABASE_URL"]
    engine = create_engine(db_url)

    # Create a clean data folder for this test
    data_root = tmp_path / "data"
    folder = data_root / "new_business"
    folder.mkdir(parents=True)

    # Create a small sample dataframe and write as xlsx in the expected sheet name
    df = pd.DataFrame({"Client": ["A", "B"], "Policy No": [100, 101], "suspended_premium": [0.0, 10.0]})
    xlsx_path = folder / "test_report.xlsx"
    _write_xlsx(xlsx_path, df, sheet_name="Sheet 1")

    # Clean possible leftover state: drop target table and remove import/schema log entries
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS new_business"))
        # Remove import log entries for this table and filename
        try:
            conn.execute(text("DELETE FROM etl_imports WHERE table_name = 'new_business'"))
        except Exception:
            # table might not exist yet; ignore
            pass
        try:
            conn.execute(text("DELETE FROM etl_schema_changes WHERE table_name = 'new_business'"))
        except Exception:
            pass

    # Sanity hooks: wrap some functions to assert behavior during the run
    called = {"create_table": 0, "insert": 0}

    orig_create_table = etl.create_table_from_df
    orig_insert = etl.insert_dataframe

    def wrap_create_table(engine_arg, table_name, df_arg, conn=None):
        called["create_table"] += 1
        return orig_create_table(engine_arg, table_name, df_arg)

    def wrap_insert(engine_arg, table_name, df_arg, conn=None):
        called["insert"] += 1
        return orig_insert(engine_arg, table_name, df_arg)

    import importlib
    importlib.reload(etl)
    # patch the functions used inside etl module
    etl.create_table_from_df = wrap_create_table
    etl.insert_dataframe = wrap_insert
    # ensure file is treated as not already imported
    etl.is_imported = lambda *args, **kwargs: False

    # Run the ETL against the test data directory
    etl.run(data_root=str(data_root), etl_config_path="etl_config.yaml", db_config={"url": db_url})

    # Check that create_table and insert were called
    assert called["create_table"] > 0, "create_table was not invoked"
    assert called["insert"] > 0, "insert was not invoked"

    # Validate that table exists and rows were inserted (poll for up to 10s)
    found = False
    for _ in range(20):
        with engine.begin() as conn:
            r = conn.execute(
                text("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name='new_business'")
            ).fetchone()
            if r:
                found = True
                break
        time.sleep(0.5)

    assert found, "new_business table was not created"

    with engine.begin() as conn:
        r = conn.execute(text("SELECT COUNT(1) FROM new_business")).scalar()
        assert int(r) == 2

    # Clean up
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS new_business"))

    shutil.rmtree(data_root)
