import pytest
import os
import sys
import pandas as pd
from unittest.mock import patch, MagicMock

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import main


class TestIntegration:
    @patch("src.etl.run")
    def test_main_function_success(self, mock_etl_run, tmp_path, monkeypatch):
        """When the Data directory exists, main should call src.etl.run with the Data root and db config."""
        # Create a data directory
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        monkeypatch.chdir(tmp_path)

        # Provide a DATABASE_URL so load_config doesn't fail
        monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")

        # Call main
        main()

        # Ensure etl.run was called
        assert mock_etl_run.called
        called_args, called_kwargs = mock_etl_run.call_args
        assert called_kwargs.get("data_root") == "data"

    @patch("main.os.path.isdir", return_value=False)
    @patch("main.find_data_files")
    @patch("sys.exit")
    def test_main_no_files_found(self, mock_exit, mock_find_files, mock_isdir):
        """Test main function when no data files are found."""
        # Setup - return empty list of files
        mock_find_files.return_value = []

        # Call main function - it will call sys.exit, but we've patched it
        main()

        # Assert that sys.exit was called with error code 1
        mock_exit.assert_called_once_with(1)

    @patch("main.os.path.isdir", return_value=False)
    @patch("main.find_data_files")
    @patch("main.configure_data_dir")
    @patch("main.create_df_dict")
    @patch("sys.exit")
    def test_main_no_dataframes_created(
        self, mock_exit, mock_create_df, mock_config_dir, mock_find_files, mock_isdir
    ):
        """Test main function when no dataframes are created."""
        # Setup
        mock_find_files.return_value = ["test.csv"]
        mock_create_df.return_value = {}  # Empty dictionary returned

        # Call main function - it will call sys.exit, but we've patched it
        main()

        # Assert that sys.exit was called with error code 1
        mock_exit.assert_called_once_with(1)
