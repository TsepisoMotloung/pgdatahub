"""Utility functions for PGDataHub ETL."""

import re
import hashlib
import unicodedata
from pathlib import Path
from typing import List, Set, Dict, Any
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('pgdh_etl')


def clean_text(text: Any) -> str:
    """Clean and normalize text for safe SQL identifiers.

    Args:
        text: Input text to clean

    Returns:
        Cleaned, ASCII-safe text
    """
    if text is None:
        return ''

    text = str(text)

    # Normalize Unicode to NFKD form
    text = unicodedata.normalize('NFKD', text)

    # Encode to ASCII, ignoring non-ASCII characters
    text = text.encode('ascii', 'ignore').decode('ascii')

    # Convert to lowercase
    text = text.lower()

    # Replace non-alphanumeric characters with underscore
    text = re.sub(r'[^a-z0-9]', '_', text)

    # Collapse multiple underscores
    text = re.sub(r'_+', '_', text)

    # Strip leading/trailing underscores
    text = text.strip('_')

    return text


def normalize_column_names(columns: List[str]) -> List[str]:
    """Normalize column names for database compatibility.

    Handles duplicates by keeping track and appending suffixes.

    Args:
        columns: List of original column names

    Returns:
        List of normalized, unique column names
    """
    normalized = []
    seen: Dict[str, int] = {}

    for col in columns:
        clean_col = clean_text(col)

        if not clean_col:
            clean_col = 'column'

        # Handle duplicates by adding suffix
        if clean_col in seen:
            seen[clean_col] += 1
            clean_col = f"{clean_col}_{seen[clean_col]}"
        else:
            seen[clean_col] = 0

        normalized.append(clean_col)

    return normalized


def compute_file_hash(file_path: Path) -> str:
    """Compute SHA-256 hash of a file.

    Args:
        file_path: Path to the file

    Returns:
        Hex digest of SHA-256 hash
    """
    sha256_hash = hashlib.sha256()

    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256_hash.update(chunk)

    return sha256_hash.hexdigest()


def is_valid_sql_identifier(name: str) -> bool:
    """Check if a string is a valid SQL identifier.

    Args:
        name: Identifier to validate

    Returns:
        True if valid SQL identifier
    """
    if not name:
        return False

    # Must start with letter or underscore
    if not re.match(r'^[a-zA-Z_]', name):
        return False

    # Can contain letters, digits, underscores
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        return False

    # Check for SQL reserved words (basic list)
    reserved = {
        'select', 'insert', 'update', 'delete', 'from', 'where',
        'and', 'or', 'not', 'null', 'true', 'false', 'table',
        'create', 'drop', 'alter', 'index', 'primary', 'key',
        'foreign', 'references', 'constraint', 'default',
        'unique', 'check', 'auto_increment', 'serial', 'bigserial'
    }

    return name.lower() not in reserved


def sanitize_table_name(folder_name: str) -> str:
    """Convert folder name to valid SQL table name.

    Args:
        folder_name: Original folder name

    Returns:
        Valid SQL table name
    """
    table_name = clean_text(folder_name)

    if not table_name:
        table_name = 'data_table'

    # Ensure it starts with a letter
    if not re.match(r'^[a-zA-Z]', table_name):
        table_name = 't_' + table_name

    return table_name


def get_folders(data_root: Path) -> List[Path]:
    """Get all subfolders under data root.

    Args:
        data_root: Root data directory

    Returns:
        List of folder paths
    """
    if not data_root.exists():
        logger.warning(f"Data root does not exist: {data_root}")
        return []

    folders = []

    for item in data_root.iterdir():
        if item.is_dir():
            folders.append(item)
            # Also get nested folders
            folders.extend(get_nested_folders(item))

    return folders


def get_nested_folders(folder: Path) -> List[Path]:
    """Recursively get all nested folders.

    Args:
        folder: Starting folder

    Returns:
        List of nested folder paths
    """
    nested = []

    for item in folder.iterdir():
        if item.is_dir():
            nested.append(item)
            nested.extend(get_nested_folders(item))

    return nested


def get_excel_files(folder: Path, extensions: List[str]) -> List[Path]:
    """Get all Excel files in a folder.

    Args:
        folder: Folder to search
        extensions: List of supported file extensions

    Returns:
        List of Excel file paths
    """
    files = []

    for ext in extensions:
        files.extend(folder.glob(f'*{ext}'))

    return sorted(files)


def get_folder_path_parts(folder: Path, data_root: Path) -> List[str]:
    """Get relative path parts from data root to folder.

    Args:
        folder: Target folder
        data_root: Root data directory

    Returns:
        List of path components
    """
    try:
        relative = folder.relative_to(data_root)
        return list(relative.parts)
    except ValueError:
        return [folder.name]


class MetricsCollector:
    """Collect and report ETL metrics."""

    def __init__(self):
        self.files_processed = 0
        self.files_skipped = 0
        self.rows_inserted = 0
        self.schema_changes = 0
        self.errors = 0
        self.file_times: Dict[str, float] = {}
        self.start_time: float = 0

    def start(self):
        """Start metrics collection."""
        import time
        self.start_time = time.time()

    def record_file_processed(self, file_name: str, duration: float, rows: int):
        """Record file processing metrics."""
        self.files_processed += 1
        self.rows_inserted += rows
        self.file_times[file_name] = duration

    def record_file_skipped(self):
        """Record skipped file."""
        self.files_skipped += 1

    def record_schema_change(self):
        """Record schema change."""
        self.schema_changes += 1

    def record_error(self):
        """Record error."""
        self.errors += 1

    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary."""
        import time
        total_time = time.time() - self.start_time if self.start_time else 0

        return {
            'files_processed': self.files_processed,
            'files_skipped': self.files_skipped,
            'rows_inserted': self.rows_inserted,
            'schema_changes': self.schema_changes,
            'errors': self.errors,
            'total_time_seconds': round(total_time, 2),
            'avg_time_per_file': round(
                sum(self.file_times.values()) / len(self.file_times), 2
            ) if self.file_times else 0
        }

    def log_summary(self):
        """Log metrics summary."""
        summary = self.get_summary()
        logger.info("=" * 50)
        logger.info("ETL Run Summary")
        logger.info("=" * 50)
        logger.info(f"Files processed: {summary['files_processed']}")
        logger.info(f"Files skipped: {summary['files_skipped']}")
        logger.info(f"Rows inserted: {summary['rows_inserted']}")
        logger.info(f"Schema changes: {summary['schema_changes']}")
        logger.info(f"Errors: {summary['errors']}")
        logger.info(f"Total time: {summary['total_time_seconds']}s")
        logger.info(f"Avg time per file: {summary['avg_time_per_file']}s")
        logger.info("=" * 50)
