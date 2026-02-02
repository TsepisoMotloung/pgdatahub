"""Integration tests for PGDataHub ETL."""

import unittest
import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from sqlalchemy import create_engine, text

# Skip integration tests if no database available
SKIP_INTEGRATION = os.getenv('SKIP_INTEGRATION_TESTS', '1') == '1'


@unittest.skipIf(SKIP_INTEGRATION, "Integration tests disabled")
class TestETLIntegration(unittest.TestCase):
    """Integration tests for full ETL workflow."""

    @classmethod
    def setUpClass(cls):
        """Set up test database and data."""
        cls.database_url = os.getenv(
            'TEST_DATABASE_URL',
            'postgresql+psycopg://postgres:postgres@localhost:5432/pgdh_test'
        )

        # Create test data directory
        cls.test_data_dir = Path(tempfile.mkdtemp())

        # Create test folder structure
        cls.folder_a = cls.test_data_dir / 'folder_a'
        cls.folder_a.mkdir()

        # Create test Excel files
        cls._create_test_excel_files()

        # Create database connection
        cls.engine = create_engine(cls.database_url)

        # Clean up any existing test tables
        cls._cleanup_database()

    @classmethod
    def tearDownClass(cls):
        """Clean up test resources."""
        # Remove test data directory
        shutil.rmtree(cls.test_data_dir, ignore_errors=True)

        # Clean up database
        cls._cleanup_database()

    @classmethod
    def _create_test_excel_files(cls):
        """Create test Excel files."""
        # File 1: Simple data
        df1 = pd.DataFrame({
            'Name': ['Alice', 'Bob', 'Charlie'],
            'Age': [30, 25, 35],
            'City': ['New York', 'Los Angeles', 'Chicago']
        })
        df1.to_excel(cls.folder_a / 'file1.xlsx', sheet_name='Policies', index=False)

        # File 2: Different columns (schema evolution test)
        df2 = pd.DataFrame({
            'Name': ['David', 'Eve'],
            'Age': [40, 28],
            'Department': ['Sales', 'Engineering'],
            'Salary': [50000, 75000]
        })
        df2.to_excel(cls.folder_a / 'file2.xlsx', sheet_name='Policies', index=False)

    @classmethod
    def _cleanup_database(cls):
        """Clean up test tables from database."""
        try:
            with cls.engine.connect() as conn:
                # Drop tables
                tables = ['etl_imports', 'etl_schema_changes', 'folder_a']
                for table in tables:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))
                conn.commit()
        except Exception as e:
            print(f"Cleanup warning: {e}")

    def test_01_database_connection(self):
        """Test database connection."""
        from src.database import DatabaseManager

        db = DatabaseManager(self.database_url)
        db.connect()

        self.assertIsNotNone(db.engine)
        db.close()

    def test_02_create_tracking_tables(self):
        """Test creation of tracking tables."""
        from src.database import DatabaseManager

        db = DatabaseManager(self.database_url)
        db.connect()
        db.create_tracking_tables()

        # Verify tables exist
        self.assertTrue(db.table_exists('etl_imports'))
        self.assertTrue(db.table_exists('etl_schema_changes'))

        db.close()

    def test_03_full_etl_run(self):
        """Test full ETL run."""
        from src.etl import ETLOrchestrator

        orchestrator = ETLOrchestrator(self.test_data_dir)
        orchestrator.db = DatabaseManager(self.database_url)
        orchestrator.db.connect()
        orchestrator.db.create_tracking_tables()

        # Create schema manager with connected db
        from src.schema_manager import SchemaManager
        orchestrator.schema_manager = SchemaManager(orchestrator.db)

        success = orchestrator.run()

        self.assertTrue(success)

        # Verify data was inserted
        with self.engine.connect() as conn:
            result = conn.execute(text('SELECT COUNT(*) FROM folder_a'))
            count = result.scalar()
            self.assertEqual(count, 5)  # 3 + 2 rows

        orchestrator.close()

    def test_04_deduplication(self):
        """Test that duplicate files are skipped."""
        from src.database import DatabaseManager

        db = DatabaseManager(self.database_url)
        db.connect()

        # Check if file is marked as imported
        from src.utils import compute_file_hash
        file_hash = compute_file_hash(self.folder_a / 'file1.xlsx')

        is_imported = db.is_file_imported('folder_a', self.folder_a / 'file1.xlsx', file_hash)
        self.assertTrue(is_imported)

        db.close()

    def test_05_schema_changes_logged(self):
        """Test that schema changes are logged."""
        from src.database import DatabaseManager

        db = DatabaseManager(self.database_url)
        db.connect()

        # Check for schema change records
        with db.transaction() as conn:
            if conn:
                result = conn.execute(text(
                    'SELECT COUNT(*) FROM etl_schema_changes WHERE table_name = :table'
                ), {'table': 'folder_a'})
                count = result.scalar()
                self.assertGreater(count, 0)

        db.close()


@unittest.skipIf(SKIP_INTEGRATION, "Integration tests disabled")
class TestPauseResume(unittest.TestCase):
    """Tests for pause and resume functionality."""

    @classmethod
    def setUpClass(cls):
        """Set up test resources."""
        cls.test_data_dir = Path(tempfile.mkdtemp())
        cls.database_url = os.getenv(
            'TEST_DATABASE_URL',
            'postgresql+psycopg://postgres:postgres@localhost:5432/pgdh_test'
        )

    @classmethod
    def tearDownClass(cls):
        """Clean up test resources."""
        shutil.rmtree(cls.test_data_dir, ignore_errors=True)

    def setUp(self):
        """Set up each test."""
        # Clean up pause file
        pause_file = self.test_data_dir / '.etl_pause.json'
        if pause_file.exists():
            pause_file.unlink()

    def test_pause_state_write_read(self):
        """Test writing and reading pause state."""
        from src.pause_manager import PauseManager

        pm = PauseManager(self.test_data_dir)

        # Write pause state
        test_folder = self.test_data_dir / 'test_folder'
        test_file = test_folder / 'test.xlsx'
        pm.write_pause_state(test_folder, 'test_table', test_file, 'Test error')

        # Read pause state
        state = pm.read_pause_state()

        self.assertIsNotNone(state)
        self.assertEqual(state['table'], 'test_table')
        self.assertEqual(state['error'], 'Test error')

    def test_pause_state_clear(self):
        """Test clearing pause state."""
        from src.pause_manager import PauseManager

        pm = PauseManager(self.test_data_dir)

        # Write and then clear
        test_folder = self.test_data_dir / 'test_folder'
        pm.write_pause_state(test_folder, 'test_table', test_folder / 'test.xlsx', 'Error')
        pm.clear_pause_state()

        self.assertFalse(pm.has_pause_state())


if __name__ == '__main__':
    # Run with: SKIP_INTEGRATION_TESTS=0 python -m pytest tests/test_integration.py
    unittest.main()
