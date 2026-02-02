"""Unit tests for Excel processor module."""

import unittest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import pandas as pd
import numpy as np
from src.excel_processor import ExcelProcessor


class TestExcelProcessor(unittest.TestCase):
    """Tests for ExcelProcessor class."""

    def setUp(self):
        """Set up test fixtures."""
        self.processor = ExcelProcessor()

    def test_coalesce_duplicate_columns_no_duplicates(self):
        """Test coalesce with no duplicate columns."""
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        result = self.processor.coalesce_duplicate_columns(df)
        self.assertEqual(list(result.columns), ['col1', 'col2'])

    def test_coalesce_duplicate_columns_with_duplicates(self):
        """Test coalesce with duplicate columns."""
        # Create DataFrame with duplicate column names
        df = pd.DataFrame(
            [[1, 'a'], [2, 'b'], [3, 'c']],
            columns=['col', 'col']
        )
        result = self.processor.coalesce_duplicate_columns(df)
        # Should have only one 'col' column
        self.assertEqual(list(result.columns), ['col'])

    def test_infer_types_numeric(self):
        """Test type inference for numeric columns."""
        df = pd.DataFrame({
            'numbers': ['1', '2', '3', '4', '5']
        })
        result = self.processor._infer_types(df)
        self.assertTrue(pd.api.types.is_numeric_dtype(result['numbers']))

    def test_infer_types_mixed(self):
        """Test type inference for mixed columns."""
        df = pd.DataFrame({
            'mixed': ['1', '2', 'abc', 'def', 'ghi']  # Less than 50% numeric
        })
        result = self.processor._infer_types(df)
        # Should remain as text due to low numeric ratio
        self.assertFalse(pd.api.types.is_numeric_dtype(result['mixed']))

    def test_rows_to_dataframe(self):
        """Test conversion of rows to DataFrame."""
        rows = [
            ('value1', 'value2', 'value3'),
            ('value4', 'value5', 'value6')
        ]
        columns = ['col_a', 'col_b', 'col_c']
        result = self.processor._rows_to_dataframe(rows, columns)

        self.assertEqual(len(result), 2)
        self.assertEqual(list(result.columns), columns)

    def test_rows_to_dataframe_extra_columns(self):
        """Test handling of rows with more columns than headers."""
        rows = [
            ('value1', 'value2', 'value3', 'extra1'),
            ('value4', 'value5', 'value6', 'extra2')
        ]
        columns = ['col_a', 'col_b', 'col_c']
        result = self.processor._rows_to_dataframe(rows, columns)

        self.assertEqual(len(result.columns), 4)
        self.assertIn('extra_col_3', result.columns)


class TestExcelProcessorValidation(unittest.TestCase):
    """Tests for Excel file validation."""

    def setUp(self):
        """Set up test fixtures."""
        self.processor = ExcelProcessor()

    @patch('pathlib.Path.exists')
    def test_validate_file_not_exists(self, mock_exists):
        """Test validation when file doesn't exist."""
        mock_exists.return_value = False

        file_path = Path('/nonexistent/file.xlsx')
        is_valid, errors = self.processor.validate_file(file_path)

        self.assertFalse(is_valid)
        self.assertTrue(any('does not exist' in e for e in errors))

    @patch('pathlib.Path.exists')
    @patch('pathlib.Path.stat')
    def test_validate_file_large_file(self, mock_stat, mock_exists):
        """Test validation warning for large files."""
        mock_exists.return_value = True

        # Mock file stat for large file (150 MB)
        mock_stat_result = Mock()
        mock_stat_result.st_size = 150 * 1024 * 1024
        mock_stat.return_value = mock_stat_result

        # Mock openpyxl to succeed
        with patch('openpyxl.load_workbook') as mock_load:
            mock_wb = Mock()
            mock_load.return_value = mock_wb

            file_path = Path('/test/file.xlsx')
            is_valid, errors = self.processor.validate_file(file_path)

            self.assertTrue(is_valid)


if __name__ == '__main__':
    unittest.main()
