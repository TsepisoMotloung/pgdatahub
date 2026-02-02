"""Unit tests for utility functions."""

import unittest
from pathlib import Path
from src.utils import (
    clean_text, normalize_column_names, is_valid_sql_identifier,
    sanitize_table_name, get_folder_path_parts
)


class TestCleanText(unittest.TestCase):
    """Tests for clean_text function."""

    def test_basic_cleaning(self):
        """Test basic text cleaning."""
        self.assertEqual(clean_text('Hello World'), 'hello_world')

    def test_special_characters(self):
        """Test removal of special characters."""
        self.assertEqual(clean_text('Hello@#$%World'), 'hello_world')

    def test_unicode_normalization(self):
        """Test Unicode normalization."""
        self.assertEqual(clean_text('Café'), 'caf')

    def test_multiple_underscores(self):
        """Test collapsing multiple underscores."""
        self.assertEqual(clean_text('Hello___World'), 'hello_world')

    def test_leading_trailing_underscores(self):
        """Test removal of leading/trailing underscores."""
        self.assertEqual(clean_text('_HelloWorld_'), 'helloworld')

    def test_empty_string(self):
        """Test empty string handling."""
        self.assertEqual(clean_text(''), '')

    def test_none_input(self):
        """Test None input handling."""
        self.assertEqual(clean_text(None), '')

    def test_numeric_input(self):
        """Test numeric input handling."""
        self.assertEqual(clean_text(123), '123')


class TestNormalizeColumnNames(unittest.TestCase):
    """Tests for normalize_column_names function."""

    def test_basic_normalization(self):
        """Test basic column name normalization."""
        columns = ['First Name', 'Last Name', 'Age']
        expected = ['first_name', 'last_name', 'age']
        self.assertEqual(normalize_column_names(columns), expected)

    def test_duplicate_columns(self):
        """Test handling of duplicate column names."""
        columns = ['Name', 'Name', 'Name']
        expected = ['name', 'name_1', 'name_2']
        self.assertEqual(normalize_column_names(columns), expected)

    def test_empty_column_names(self):
        """Test handling of empty column names."""
        columns = ['', 'Name', '']
        expected = ['column', 'name', 'column_1']
        self.assertEqual(normalize_column_names(columns), expected)

    def test_special_characters(self):
        """Test handling of special characters."""
        columns = ['Column@1', 'Column#2', 'Column$3']
        expected = ['column_1', 'column_2', 'column_3']
        self.assertEqual(normalize_column_names(columns), expected)

    def test_case_preservation(self):
        """Test that all columns are lowercased."""
        columns = ['UPPER', 'Mixed', 'lower']
        expected = ['upper', 'mixed', 'lower']
        self.assertEqual(normalize_column_names(columns), expected)


class TestIsValidSQLIdentifier(unittest.TestCase):
    """Tests for is_valid_sql_identifier function."""

    def test_valid_identifiers(self):
        """Test valid SQL identifiers."""
        self.assertTrue(is_valid_sql_identifier('valid_name'))
        self.assertTrue(is_valid_sql_identifier('_underscore_start'))
        self.assertTrue(is_valid_sql_identifier('name123'))

    def test_invalid_starting_char(self):
        """Test identifiers starting with invalid characters."""
        self.assertFalse(is_valid_sql_identifier('123name'))
        self.assertFalse(is_valid_sql_identifier('@name'))

    def test_reserved_words(self):
        """Test SQL reserved words."""
        self.assertFalse(is_valid_sql_identifier('select'))
        self.assertFalse(is_valid_sql_identifier('INSERT'))
        self.assertFalse(is_valid_sql_identifier('table'))

    def test_empty_string(self):
        """Test empty string."""
        self.assertFalse(is_valid_sql_identifier(''))


class TestSanitizeTableName(unittest.TestCase):
    """Tests for sanitize_table_name function."""

    def test_basic_sanitization(self):
        """Test basic table name sanitization."""
        self.assertEqual(sanitize_table_name('My Table'), 'my_table')

    def test_numeric_prefix(self):
        """Test handling of numeric prefix."""
        self.assertEqual(sanitize_table_name('123table'), 't_123table')

    def test_special_characters(self):
        """Test removal of special characters."""
        self.assertEqual(sanitize_table_name('table@#$%name'), 'table_name')

    def test_empty_string(self):
        """Test empty string handling."""
        self.assertEqual(sanitize_table_name(''), 'data_table')


class TestGetFolderPathParts(unittest.TestCase):
    """Tests for get_folder_path_parts function."""

    def test_basic_path(self):
        """Test basic path decomposition."""
        data_root = Path('/data')
        folder = Path('/data/folder_a/subfolder')
        result = get_folder_path_parts(folder, data_root)
        self.assertEqual(result, ['folder_a', 'subfolder'])

    def test_single_level(self):
        """Test single level folder."""
        data_root = Path('/data')
        folder = Path('/data/folder_a')
        result = get_folder_path_parts(folder, data_root)
        self.assertEqual(result, ['folder_a'])

    def test_folder_outside_root(self):
        """Test folder outside data root."""
        data_root = Path('/data')
        folder = Path('/other/folder')
        result = get_folder_path_parts(folder, data_root)
        self.assertEqual(result, ['folder'])


if __name__ == '__main__':
    unittest.main()
