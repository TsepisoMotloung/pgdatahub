"""Unit tests for configuration management."""

import unittest
import os
import tempfile
from pathlib import Path
from src.config import Config, get_config


class TestConfig(unittest.TestCase):
    """Tests for Config class."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_path = Path(self.temp_dir) / 'config.json'
        self.etl_config_path = Path(self.temp_dir) / 'etl_config.yaml'

        # Create test config.json
        config_json = '''
        {
            "database": {
                "host": "testhost",
                "port": 5432,
                "database": "testdb",
                "user": "testuser",
                "password": "testpass"
            },
            "etl": {
                "chunk_size": 5000,
                "supported_extensions": [".xlsx", ".xls"],
                "case_sensitive_folders": false
            }
        }
        '''
        with open(self.config_path, 'w') as f:
            f.write(config_json)

        # Create test etl_config.yaml
        config_yaml = '''
default_sheet: Sheet1
folder_a:
  sheet: Policies
folder_b:
  nested:
    sheet: Claims
'''
        with open(self.etl_config_path, 'w') as f:
            f.write(config_yaml)

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir)

        # Clear environment variables
        for key in ['DATABASE_URL', 'ETL_CHUNK_SIZE', 'ETL_SECTIONAL_COMMIT',
                    'ETL_PAUSE_EVERY', 'ETL_PAUSE_SECONDS', 'SKIP_DB']:
            if key in os.environ:
                del os.environ[key]

    def test_database_url_from_env(self):
        """Test DATABASE_URL from environment variable."""
        os.environ['DATABASE_URL'] = 'postgresql+psycopg://envuser:envpass@envhost:5432/envdb'

        config = Config(str(self.config_path))
        config._load_json_config()  # Manually load without env override

        # When env var is set, it should take priority
        self.assertIn('envhost', config.database_url)

    def test_chunk_size_from_env(self):
        """Test ETL_CHUNK_SIZE from environment variable."""
        os.environ['ETL_CHUNK_SIZE'] = '2000'

        config = Config(str(self.config_path))
        self.assertEqual(config.chunk_size, 2000)

    def test_chunk_size_from_config(self):
        """Test chunk_size from config file."""
        config = Config(str(self.config_path))
        self.assertEqual(config.chunk_size, 5000)

    def test_sectional_commit_default(self):
        """Test default sectional commit setting."""
        config = Config(str(self.config_path))
        self.assertFalse(config.sectional_commit)

    def test_sectional_commit_from_env(self):
        """Test ETL_SECTIONAL_COMMIT from environment variable."""
        os.environ['ETL_SECTIONAL_COMMIT'] = '1'

        config = Config(str(self.config_path))
        self.assertTrue(config.sectional_commit)

    def test_get_sheet_name_simple(self):
        """Test sheet name resolution for simple folder."""
        # Temporarily change working directory to load yaml
        import yaml
        original_yaml = '''
default_sheet: Sheet1
folder_a:
  sheet: Policies
folder_b:
  nested:
    sheet: Claims
'''
        config = Config(str(self.config_path))
        config._etl_config = yaml.safe_load(original_yaml)

        self.assertEqual(config.get_sheet_name(['folder_a']), 'Policies')

    def test_get_sheet_name_nested(self):
        """Test sheet name resolution for nested folder."""
        import yaml
        config = Config(str(self.config_path))
        config._etl_config = yaml.safe_load('''
default_sheet: Sheet1
folder_a:
  sheet: Policies
folder_b:
  nested:
    sheet: Claims
''')

        self.assertEqual(config.get_sheet_name(['folder_b', 'nested']), 'Claims')

    def test_get_sheet_name_default(self):
        """Test default sheet name fallback."""
        import yaml
        config = Config(str(self.config_path))
        config._etl_config = yaml.safe_load('''
default_sheet: Sheet1
folder_a:
  sheet: Policies
''')

        self.assertEqual(config.get_sheet_name(['unknown_folder']), 'Sheet1')

    def test_supported_extensions(self):
        """Test supported extensions retrieval."""
        config = Config(str(self.config_path))
        extensions = config.supported_extensions
        self.assertIn('.xlsx', extensions)
        self.assertIn('.xls', extensions)


if __name__ == '__main__':
    unittest.main()
