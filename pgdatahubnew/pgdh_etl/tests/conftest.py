"""Pytest configuration and fixtures."""

import pytest
import os
import tempfile
import shutil
from pathlib import Path


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def mock_database_url():
    """Provide a mock database URL."""
    return 'postgresql+psycopg://test:test@localhost:5432/test'


@pytest.fixture
def sample_config_json():
    """Provide sample config.json content."""
    return '''
    {
        "database": {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "testuser",
            "password": "testpass"
        },
        "etl": {
            "chunk_size": 10000,
            "supported_extensions": [".xlsx", ".xls", ".xlsm", ".xlsb"],
            "case_sensitive_folders": false
        }
    }
    '''


@pytest.fixture
def sample_etl_config_yaml():
    """Provide sample etl_config.yaml content."""
    return '''
default_sheet: Sheet1
folder_a:
  sheet: Policies
folder_b:
  nested:
    sheet: Claims
'''


@pytest.fixture
def sample_dataframe():
    """Provide a sample pandas DataFrame."""
    import pandas as pd
    return pd.DataFrame({
        'Name': ['Alice', 'Bob', 'Charlie'],
        'Age': [30, 25, 35],
        'Salary': [50000.0, 60000.0, 75000.0]
    })


def pytest_configure(config):
    """Configure pytest."""
    # Add custom markers
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection."""
    # Skip integration tests by default
    skip_integration = pytest.mark.skip(
        reason="Integration test - run with SKIP_INTEGRATION_TESTS=0"
    )

    for item in items:
        if "integration" in item.keywords:
            if os.getenv('SKIP_INTEGRATION_TESTS', '1') == '1':
                item.add_marker(skip_integration)
