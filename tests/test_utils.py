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
