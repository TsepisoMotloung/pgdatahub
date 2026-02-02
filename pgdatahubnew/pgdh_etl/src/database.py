"""Database management module using SQLAlchemy Core + psycopg3."""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from contextlib import contextmanager

from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Integer,
    BigInteger, Float, DateTime, Text, inspect, text, select,
    insert, delete, and_
)
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.types import TypeEngine

from .config import get_config
from .utils import logger


class DatabaseManager:
    """Manages database connections and operations."""

    # Mapping of pandas dtypes to SQLAlchemy types
    TYPE_MAPPING = {
        'object': Text,
        'string': Text,
        'int64': BigInteger,
        'int32': Integer,
        'float64': Float,
        'float32': Float,
        'bool': Text,
        'datetime64[ns]': DateTime,
    }

    def __init__(self, database_url: Optional[str] = None):
        self.config = get_config()
        self.database_url = database_url or self.config.database_url
        self.engine: Optional[Engine] = None
        self.metadata = MetaData()
        self._connection: Optional[Connection] = None

    def connect(self) -> 'DatabaseManager':
        """Initialize database connection."""
        if self.config.skip_db:
            logger.info("SKIP_DB mode: Database operations will be skipped")
            return self

        self.engine = create_engine(
            self.database_url,
            pool_pre_ping=True,
            echo=False
        )
        return self

    @contextmanager
    def transaction(self):
        """Context manager for database transactions."""
        if self.config.skip_db or not self.engine:
            yield None
            return

        conn = self.engine.connect()
        trans = conn.begin()
        try:
            yield conn
            trans.commit()
        except Exception as e:
            trans.rollback()
            raise e
        finally:
            conn.close()

    def close(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            self.engine = None

    def create_tracking_tables(self) -> None:
        """Create ETL tracking tables if they don't exist."""
        if self.config.skip_db or not self.engine:
            return

        with self.transaction() as conn:
            # etl_imports table
            if not self.table_exists('etl_imports'):
                etl_imports = Table(
                    'etl_imports',
                    self.metadata,
                    Column('id', Integer, primary_key=True, autoincrement=True),
                    Column('table_name', String(255), nullable=False, index=True),
                    Column('source_file', String(500), nullable=False),
                    Column('file_sha256', String(64), nullable=False, index=True),
                    Column('row_count', Integer, nullable=False, default=0),
                    Column('imported_at', DateTime, nullable=False, default=datetime.utcnow),
                    Column('folder_path', Text),
                )
                etl_imports.create(self.engine)
                logger.info("Created etl_imports table")

            # etl_schema_changes table
            if not self.table_exists('etl_schema_changes'):
                schema_changes = Table(
                    'etl_schema_changes',
                    self.metadata,
                    Column('id', Integer, primary_key=True, autoincrement=True),
                    Column('table_name', String(255), nullable=False, index=True),
                    Column('change_type', String(50), nullable=False),
                    Column('column_name', String(255)),
                    Column('old_type', String(100)),
                    Column('new_type', String(100)),
                    Column('source_file', String(500)),
                    Column('changed_at', DateTime, nullable=False, default=datetime.utcnow),
                    Column('details', Text),
                )
                schema_changes.create(self.engine)
                logger.info("Created etl_schema_changes table")

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        if self.config.skip_db or not self.engine:
            return False

        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()

    def get_table_columns(self, table_name: str) -> Dict[str, TypeEngine]:
        """Get column information for a table."""
        if self.config.skip_db or not self.engine:
            return {}

        inspector = inspect(self.engine)
        columns = {}

        for col in inspector.get_columns(table_name):
            columns[col['name']] = col['type']

        return columns

    def is_file_imported(self, table_name: str, file_path: Path, file_hash: str) -> bool:
        """Check if a file has already been imported.

        Args:
            table_name: Target table name
            file_path: Path to the file
            file_hash: SHA-256 hash of the file

        Returns:
            True if file was previously imported
        """
        if self.config.skip_db or not self.engine:
            return False

        etl_imports = Table('etl_imports', self.metadata, autoload_with=self.engine)

        with self.transaction() as conn:
            stmt = select(etl_imports).where(
                and_(
                    etl_imports.c.table_name == table_name,
                    etl_imports.c.source_file == str(file_path),
                    etl_imports.c.file_sha256 == file_hash
                )
            )
            result = conn.execute(stmt).fetchone()
            return result is not None

    def log_import(self, table_name: str, file_path: Path, file_hash: str,
                   row_count: int, folder_path: str = '') -> None:
        """Log a successful import.

        Args:
            table_name: Target table name
            file_path: Path to the file
            file_hash: SHA-256 hash of the file
            row_count: Number of rows inserted
            folder_path: Relative folder path
        """
        if self.config.skip_db or not self.engine:
            return

        etl_imports = Table('etl_imports', self.metadata, autoload_with=self.engine)

        with self.transaction() as conn:
            stmt = insert(etl_imports).values(
                table_name=table_name,
                source_file=str(file_path),
                file_sha256=file_hash,
                row_count=row_count,
                imported_at=datetime.utcnow(),
                folder_path=folder_path
            )
            conn.execute(stmt)

    def log_schema_change(self, table_name: str, change_type: str,
                          column_name: str = None, old_type: str = None,
                          new_type: str = None, file_path: Path = None,
                          details: Dict = None) -> None:
        """Log a schema change.

        Args:
            table_name: Target table name
            change_type: Type of change (create_table, add_column, alter_type)
            column_name: Affected column name
            old_type: Previous column type
            new_type: New column type
            file_path: Source file path
            details: Additional details as dict
        """
        if self.config.skip_db or not self.engine:
            return

        etl_schema_changes = Table('etl_schema_changes', self.metadata,
                                   autoload_with=self.engine)

        with self.transaction() as conn:
            stmt = insert(etl_schema_changes).values(
                table_name=table_name,
                change_type=change_type,
                column_name=column_name,
                old_type=old_type,
                new_type=new_type,
                source_file=str(file_path) if file_path else None,
                changed_at=datetime.utcnow(),
                details=json.dumps(details) if details else None
            )
            conn.execute(stmt)

    def get_imports_by_file(self, source_file: str = None,
                            file_hash: str = None) -> List[Dict]:
        """Get import records by file identifier.

        Args:
            source_file: Source file path
            file_hash: SHA-256 hash

        Returns:
            List of matching import records
        """
        if self.config.skip_db or not self.engine:
            return []

        etl_imports = Table('etl_imports', self.metadata, autoload_with=self.engine)

        with self.transaction() as conn:
            conditions = []
            if source_file:
                conditions.append(etl_imports.c.source_file == source_file)
            if file_hash:
                conditions.append(etl_imports.c.file_sha256 == file_hash)

            if not conditions:
                return []

            stmt = select(etl_imports).where(and_(*conditions))
            result = conn.execute(stmt)

            return [
                {
                    'table_name': row.table_name,
                    'source_file': row.source_file,
                    'file_sha256': row.file_sha256,
                    'row_count': row.row_count,
                    'imported_at': row.imported_at
                }
                for row in result
            ]

    def delete_import_records(self, source_file: str = None,
                              file_hash: str = None) -> int:
        """Delete import records.

        Args:
            source_file: Source file path
            file_hash: SHA-256 hash

        Returns:
            Number of records deleted
        """
        if self.config.skip_db or not self.engine:
            return 0

        etl_imports = Table('etl_imports', self.metadata, autoload_with=self.engine)

        with self.transaction() as conn:
            conditions = []
            if source_file:
                conditions.append(etl_imports.c.source_file == source_file)
            if file_hash:
                conditions.append(etl_imports.c.file_sha256 == file_hash)

            if not conditions:
                return 0

            stmt = delete(etl_imports).where(and_(*conditions))
            result = conn.execute(stmt)
            return result.rowcount

    def execute_raw(self, sql: str, params: Dict = None) -> Any:
        """Execute raw SQL.

        Args:
            sql: SQL statement
            params: Query parameters

        Returns:
            Query result
        """
        if self.config.skip_db or not self.engine:
            return None

        with self.transaction() as conn:
            result = conn.execute(text(sql), params or {})
            return result

    def get_column_type(self, dtype: str) -> TypeEngine:
        """Map pandas dtype to SQLAlchemy type.

        Args:
            dtype: Pandas dtype string

        Returns:
            SQLAlchemy type instance
        """
        # Extract base type from dtype string
        base_type = str(dtype).split('[')[0].split('(')[0]

        type_class = self.TYPE_MAPPING.get(base_type, Text)
        return type_class()

    def is_type_compatible(self, current_type: TypeEngine,
                           new_type: TypeEngine) -> Tuple[bool, Optional[TypeEngine]]:
        """Check if type change is compatible/safe.

        Args:
            current_type: Existing column type
            new_type: Proposed new type

        Returns:
            Tuple of (is_safe, resolved_type)
        """
        current_name = current_type.__class__.__name__
        new_name = new_type.__class__.__name__

        # Safe widenings
        safe_widenings = {
            ('Integer', 'BigInteger'): BigInteger,
            ('SmallInteger', 'Integer'): Integer,
            ('SmallInteger', 'BigInteger'): BigInteger,
            ('Float', 'Double'): Float,
        }

        if (current_name, new_name) in safe_widenings:
            return True, safe_widenings[(current_name, new_name)]()

        # Same type is always compatible
        if current_name == new_name:
            return True, current_type

        # Numeric to non-numeric is unsafe - fall back to TEXT
        numeric_types = ('Integer', 'BigInteger', 'SmallInteger', 'Float', 'Double', 'Numeric')
        if current_name in numeric_types and new_name not in numeric_types:
            return False, Text()

        # Different types - not compatible
        return False, Text()
