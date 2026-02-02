"""Unit tests for text cleaning and normalization"""
import pytest
import pandas as pd
from src.utils import (
    clean_text,
    normalize_dataframe_columns,
    pandas_to_pg_type,
    infer_schema
)


class TestCleanText:
    """Tests for clean_text function"""
    
    def test_basic_cleaning(self):
        """Test basic text cleaning"""
        assert clean_text("Hello World") == "hello_world"
        assert clean_text("Test123") == "test123"
        assert clean_text("Column Name") == "column_name"
    
    def test_special_characters(self):
        """Test special character removal"""
        assert clean_text("Column@Name!") == "column_name"
        assert clean_text("Test#Column$") == "test_column"
        assert clean_text("a-b-c") == "a_b_c"
    
    def test_multiple_underscores(self):
        """Test collapsing multiple underscores"""
        assert clean_text("a___b___c") == "a_b_c"
        assert clean_text("test  column") == "test_column"
    
    def test_leading_trailing_underscores(self):
        """Test removal of leading/trailing underscores"""
        assert clean_text("_column_") == "column"
        assert clean_text("___test___") == "test"
    
    def test_starts_with_digit(self):
        """Test handling of names starting with digits"""
        assert clean_text("123column") == "_123column"
        assert clean_text("2024_data") == "_2024_data"
    
    def test_unicode_normalization(self):
        """Test unicode character handling"""
        assert clean_text("café") == "cafe"
        assert clean_text("naïve") == "naive"
        assert clean_text("José") == "jose"
    
    def test_empty_string(self):
        """Test empty string handling"""
        assert clean_text("") == "col"
        assert clean_text("!!!") == "col"


class TestNormalizeDataFrameColumns:
    """Tests for normalize_dataframe_columns function"""
    
    def test_basic_normalization(self):
        """Test basic column normalization"""
        df = pd.DataFrame({
            "Column One": [1, 2],
            "Column Two": [3, 4]
        })
        
        result = normalize_dataframe_columns(df)
        assert list(result.columns) == ["column_one", "column_two"]
    
    def test_duplicate_columns_coalescing(self):
        """Test duplicate column handling with coalescing"""
        df = pd.DataFrame({
            "Name": ["Alice", None],
            "NAME": [None, "Bob"],
            "Age": [25, 30]
        })
        
        result = normalize_dataframe_columns(df)
        
        # Should have 2 columns: name (coalesced) and age
        assert len(result.columns) == 2
        assert "name" in result.columns
        assert "age" in result.columns
        
        # Check coalesced values
        assert result["name"].iloc[0] == "Alice"
        assert result["name"].iloc[1] == "Bob"
    
    def test_multiple_duplicates(self):
        """Test multiple duplicate columns"""
        df = pd.DataFrame({
            "Col": [1, None, None],
            "col": [None, 2, None],
            "COL": [None, None, 3]
        })
        
        result = normalize_dataframe_columns(df)
        assert len(result.columns) == 1
        assert result["col"].iloc[0] == 1
        assert result["col"].iloc[1] == 2
        assert result["col"].iloc[2] == 3
    
    def test_no_duplicates(self):
        """Test DataFrame with no duplicates"""
        df = pd.DataFrame({
            "A": [1, 2],
            "B": [3, 4],
            "C": [5, 6]
        })
        
        result = normalize_dataframe_columns(df)
        assert list(result.columns) == ["a", "b", "c"]


class TestPandasToPgType:
    """Tests for pandas to PostgreSQL type conversion"""
    
    def test_integer_types(self):
        """Test integer type conversion"""
        df = pd.DataFrame({"col": [1, 2, 3]})
        assert pandas_to_pg_type(df["col"].dtype) == "BIGINT"
    
    def test_float_types(self):
        """Test float type conversion"""
        df = pd.DataFrame({"col": [1.5, 2.5, 3.5]})
        assert pandas_to_pg_type(df["col"].dtype) == "DOUBLE PRECISION"
    
    def test_bool_types(self):
        """Test boolean type conversion"""
        df = pd.DataFrame({"col": [True, False, True]})
        assert pandas_to_pg_type(df["col"].dtype) == "BOOLEAN"
    
    def test_string_types(self):
        """Test string type conversion"""
        df = pd.DataFrame({"col": ["a", "b", "c"]})
        assert pandas_to_pg_type(df["col"].dtype) == "TEXT"


class TestInferSchema:
    """Tests for schema inference"""
    
    def test_mixed_types(self):
        """Test schema inference with mixed types"""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [95.5, 87.3, 92.1],
            "passed": [True, True, False]
        })
        
        schema = infer_schema(df)
        
        assert schema["id"] == "BIGINT"
        assert schema["name"] == "TEXT"
        assert schema["score"] == "DOUBLE PRECISION"
        assert schema["passed"] == "BOOLEAN"
    
    def test_empty_dataframe(self):
        """Test schema inference on empty DataFrame"""
        df = pd.DataFrame()
        schema = infer_schema(df)
        assert schema == {}
