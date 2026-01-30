import re
import pandas as pd
from src.utils import normalize_dataframe_columns


def test_normalize_coalesces_duplicate_columns():
    # Create a DataFrame with duplicate column names
    df = pd.DataFrame([[1, None], [None, 2], [3, None]], columns=["A", "A"])
    ndf = normalize_dataframe_columns(df)

    # After normalization, column name should be cleaned to 'a' and only appear once
    assert list(ndf.columns) == ["a"]

    # Values should coalesce: first non-null across duplicates
    assert ndf["a"].tolist() == [1, 2, 3]


def test_normalize_handles_empty_and_invalid_names():
    df = pd.DataFrame([[1, 2]], columns=[None, "!@#$"])
    ndf = normalize_dataframe_columns(df)

    # Columns should be valid identifiers and unique
    assert all(re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", c) for c in ndf.columns)
    assert len(set(ndf.columns)) == len(ndf.columns)


def test_coalesce_mixed_dtypes_prefers_non_null_values():
    df = pd.DataFrame([[1, None, 'a'], [None, 2, 'b']], columns=["X", "X", "X"])
    ndf = normalize_dataframe_columns(df)
    # Only one column remains
    assert len(ndf.columns) == 1
    # Values coalesced by first non-null across duplicates
    assert ndf.iloc[0, 0] == 1
    assert ndf.iloc[1, 0] == 2
