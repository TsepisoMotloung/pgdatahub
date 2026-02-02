"""
Text cleaning and column normalization utilities for PostgreSQL ETL.
Features: 
- Strict DATE normalization (no TIMESTAMPS)
- Intelligent type inference (UUID, JSONB, Numeric cleaning)
- SQL identifier normalization
"""
import re
import unicodedata
import logging
import json
import uuid
from typing import List, Dict, Any, Optional
from datetime import datetime, date
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# Constants
DATE_PATTERNS = [
    r'^\d{4}-\d{2}-\d{2}',         # ISO format
    r'^\d{2}/\d{2}/\d{4}',         # US/UK format
    r'^\d{4}/\d{2}/\d{2}',         # Alternative ISO
    r'^\d{1,2}-\w+-\d{4}',         # DD-MMM-YYYY
    r'^\d{1,2}/\d{1,2}/\d{2,4}',    # Flexible
]

UUID_PATTERN = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

# --- 1. STRING CLEANING & IDENTIFIERS ---

def clean_text(text: str) -> str:
    """Clean and normalize text for use as SQL identifier."""
    if not isinstance(text, str):
        text = str(text)
    
    text = text.lower()
    # Remove accents
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    # Replace non-alphanumeric with underscore
    text = re.sub(r'[^a-z0-9]+', '_', text)
    # Collapse multiple underscores and strip
    text = re.sub(r'_+', '_', text).strip('_')
    
    if text and text[0].isdigit():
        text = '_' + text
    return text or 'col'


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names and coalesce duplicate columns."""
    if df.empty:
        return df
    
    original_cols = list(df.columns)
    cleaned_cols = [clean_text(col) for col in original_cols]
    
    # Check for duplicates
    col_counts = {}
    for col in cleaned_cols:
        col_counts[col] = col_counts.get(col, 0) + 1
    
    duplicates = [col for col, count in col_counts.items() if count > 1]
    
    if not duplicates:
        df.columns = cleaned_cols
        return df

    result_data = {}
    processed_cols = set()
    
    for i, cleaned_col in enumerate(cleaned_cols):
        if cleaned_col in processed_cols:
            continue
        
        if cleaned_col not in duplicates:
            result_data[cleaned_col] = df[original_cols[i]]
        else:
            # Coalesce: find all original columns that map to this cleaned name
            indices = [j for j, c in enumerate(cleaned_cols) if c == cleaned_col]
            dup_orig = [original_cols[j] for j in indices]
            
            # Start with the first column, fill nulls with subsequent ones
            coalesced = df[dup_orig[0]].copy()
            for extra_col in dup_orig[1:]:
                coalesced = coalesced.fillna(df[extra_col])
            result_data[cleaned_col] = coalesced
            
        processed_cols.add(cleaned_col)
    
    return pd.DataFrame(result_data)


def add_metadata_columns(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Add load metadata."""
    df = df.copy()
    df['source_file'] = source_file
    df['load_date'] = date.today().isoformat()
    return df

# --- 2. INTELLIGENT INFERENCE HELPERS ---

def _is_likely_uuid(value) -> bool:
    if not isinstance(value, str): return False
    return bool(re.match(UUID_PATTERN, value.lower()))

def _is_likely_json(value) -> bool:
    if not isinstance(value, str) or len(value) < 2: return False
    if value[0] not in ('{', '[') or value[-1] not in ('}', ']'): return False
    try:
        json.loads(value)
        return True
    except:
        return False

def _is_likely_date(value) -> bool:
    if pd.isna(value) or value == '': return False
    if isinstance(value, (pd.Timestamp, datetime, date)): return True
    if isinstance(value, str):
        v = value.strip()
        return any(re.match(p, v) for p in DATE_PATTERNS)
    return False

def _clean_numeric_string(value: Any) -> Optional[str]:
    if pd.isna(value): return None
    # Remove currency, %, commas, and whitespace
    cleaned = re.sub(r'[$,%\s]', '', str(value)).replace(',', '')
    return cleaned if cleaned else None

def _is_likely_numeric(value) -> bool:
    if pd.isna(value) or value == '': return False
    if isinstance(value, (int, float, np.integer, np.floating)):
        return not pd.isna(value)
    cleaned = _clean_numeric_string(value)
    if not cleaned: return False
    try:
        float(cleaned)
        return True
    except (ValueError, TypeError):
        return False

# --- 3. CORE INFERENCE & CLEANING ---

def _safe_parse_date(value) -> Optional[date]:
    """Strictly returns a date object, no time."""
    if pd.isna(value) or value == '' or value is None:
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.date()
    if isinstance(value, date):
        return value
    
    if isinstance(value, str):
        # Try pandas to_datetime which is robust, then convert to date
        try:
            return pd.to_datetime(value).date()
        except:
            return None
    return None


def clean_dataframe_for_pg(df: pd.DataFrame, schema: Dict[str, str]) -> pd.DataFrame:
    """Prepare DataFrame values based on inferred schema."""
    df = df.copy()
    
    for col, pg_type in schema.items():
        if col not in df.columns:
            continue
            
        if pg_type == 'DATE':
            df[col] = df[col].apply(lambda x: (d := _safe_parse_date(x)) and d.isoformat() or None)
            
        elif pg_type in ['INTEGER', 'BIGINT']:
            df[col] = pd.to_numeric(df[col].apply(_clean_numeric_string), errors='coerce').apply(
                lambda x: int(x) if pd.notnull(x) else None
            )
            
        elif pg_type == 'DOUBLE PRECISION':
            df[col] = pd.to_numeric(df[col].apply(_clean_numeric_string), errors='coerce').apply(
                lambda x: float(x) if pd.notnull(x) else None
            )
            
        elif pg_type == 'BOOLEAN':
            def cast_bool(x):
                if pd.isna(x): return None
                s = str(x).strip().lower()
                if s in {'true', 'yes', 'y', '1', 't'}: return True
                if s in {'false', 'no', 'n', '0', 'f'}: return False
                return None
            df[col] = df[col].apply(cast_bool)

        elif pg_type == 'TEXT':
            df[col] = df[col].apply(lambda x: str(x).strip() if pd.notnull(x) and str(x).strip() else None)
            
    return df


def _infer_column_type(series: pd.Series) -> str:
    """Intelligently infer PostgreSQL type for a series."""
    non_nulls = series.dropna()
    if non_nulls.empty:
        return 'TEXT'

    # Check native pandas dtypes
    dtype_str = str(series.dtype).lower()
    if any(x in dtype_str for x in ['datetime', 'timestamp', 'date']):
        return 'DATE'
    
    # Sampling for analysis
    sample_size = min(200, len(non_nulls))
    sample = non_nulls.sample(sample_size, random_state=42) if len(non_nulls) > 200 else non_nulls
    total = len(sample)
    
    votes = {'DATE': 0, 'BIGINT': 0, 'DOUBLE PRECISION': 0, 'BOOLEAN': 0, 'UUID': 0, 'JSONB': 0}

    for val in sample:
        str_val = str(val).strip().lower()
        if _is_likely_uuid(str_val): 
            votes['UUID'] += 1
        elif _is_likely_json(str_val): 
            votes['JSONB'] += 1
        elif _is_likely_date(val): 
            votes['DATE'] += 1
        elif _is_likely_numeric(val):
            cleaned = _clean_numeric_string(str_val)
            if cleaned:
                try:
                    num = float(cleaned)
                    votes['BIGINT' if num.is_integer() else 'DOUBLE PRECISION'] += 1
                except: 
                    pass
        elif str_val in {'true', 'false', 'yes', 'no', 'y', 'n', '1', '0'}:
            votes['BOOLEAN'] += 1

    winning_type, count = max(votes.items(), key=lambda x: x[1])
    
    if count / total >= 0.8:
        # Refine BIGINT to INTEGER if possible
        if winning_type == 'BIGINT':
            try:
                numeric_series = pd.to_numeric(series.apply(_clean_numeric_string), errors='coerce')
                max_val = numeric_series.max()
                min_val = numeric_series.min()
                if max_val < 2147483647 and min_val > -2147483648:
                    return 'INTEGER'
            except: 
                pass
        return winning_type

    return 'TEXT'


def infer_schema(df: pd.DataFrame) -> Dict[str, str]:
    """Return dictionary of {column: pg_type}."""
    return {col: _infer_column_type(df[col]) for col in df.columns}
