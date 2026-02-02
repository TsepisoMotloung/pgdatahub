"""Unit tests for database module."""

import unittest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
from src.database import DatabaseManager
from sqlalchemy.types import Text, Integer, BigInteger, Float, DateTime


class TestDatabaseManager(unittest.TestCase):
    """Tests for DatabaseManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.db = DatabaseManager('postgresql+psycopg://test@localhost/test')

    @patch('src.database.create_engine')
    def test_connect(self, mock_create_engine):
        """Test database connection."""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        self.db.connect()

        mock_create_engine.assert_called_once()
        self.assertEqual(self.db.engine, mock_engine)

    def test_get_column_type_text(self):
        """Test column type mapping for text."""
        col_type = self.db.get_column_type('object')
        self.assertIsInstance(col_type, Text)

    def test_get_column_type_integer(self):
        """Test column type mapping for integer."""
        col_type = self.db.get_column_type('int64')
        self.assertIsInstance(col_type, BigInteger)

    def test_get_column_type_float(self):
        """Test column type mapping for float."""
        col_type = self.db.get_column_type('float64')
        self.assertIsInstance(col_type, Float)

    def test_get_column_type_datetime(self):
        """Test column type mapping for datetime."""
        col_type = self.db.get_column_type('datetime64[ns]')
        self.assertIsInstance(col_type, DateTime)

    def test_is_type_compatible_same_type(self):
        """Test type compatibility for same types."""
        int_type = Integer()
        is_safe, resolved = self.db.is_type_compatible(int_type, Integer())
        self.assertTrue(is_safe)
        self.assertIsInstance(resolved, Integer)

    def test_is_type_compatible_safe_widening(self):
        """Test safe type widening."""
        int_type = Integer()
        big_int_type = BigInteger()
        is_safe, resolved = self.db.is_type_compatible(int_type, big_int_type)
        self.assertTrue(is_safe)
        self.assertIsInstance(resolved, BigInteger)

    def test_is_type_compatible_unsafe(self):
        """Test unsafe type conversion."""
        int_type = Integer()
        text_type = Text()
        is_safe, resolved = self.db.is_type_compatible(int_type, text_type)
        self.assertFalse(is_safe)
        self.assertIsInstance(resolved, Text)


class TestDatabaseManagerWithMock(unittest.TestCase):
    """Tests for DatabaseManager with mocked dependencies."""

    def setUp(self):
        """Set up test fixtures."""
        self.db = DatabaseManager('postgresql+psycopg://test@localhost/test')
        self.db.engine = Mock()
        self.db.metadata = Mock()

    @patch('src.database.inspect')
    def test_table_exists_true(self, mock_inspect):
        """Test table exists check when table exists."""
        mock_inspector = Mock()
        mock_inspector.get_table_names.return_value = ['test_table', 'other_table']
        mock_inspect.return_value = mock_inspector

        result = self.db.table_exists('test_table')
        self.assertTrue(result)

    @patch('src.database.inspect')
    def test_table_exists_false(self, mock_inspect):
        """Test table exists check when table doesn't exist."""
        mock_inspector = Mock()
        mock_inspector.get_table_names.return_value = ['other_table']
        mock_inspect.return_value = mock_inspector

        result = self.db.table_exists('test_table')
        self.assertFalse(result)

    @patch('src.database.inspect')
    def test_get_table_columns(self, mock_inspect):
        """Test getting table columns."""
        mock_inspector = Mock()
        mock_inspector.get_columns.return_value = [
            {'name': 'id', 'type': Integer()},
            {'name': 'name', 'type': Text()},
        ]
        mock_inspect.return_value = mock_inspector

        columns = self.db.get_table_columns('test_table')
        self.assertIn('id', columns)
        self.assertIn('name', columns)
        self.assertIsInstance(columns['id'], Integer)
        self.assertIsInstance(columns['name'], Text)


if __name__ == '__main__':
    unittest.main()
