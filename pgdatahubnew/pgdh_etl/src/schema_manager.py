"""Schema evolution and DDL management module."""

from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

from sqlalchemy import (
    Table, Column, MetaData, String, Integer, BigInteger,
    Float, DateTime, Text, inspect, text
)
from sqlalchemy.engine import Engine
from sqlalchemy.types import TypeEngine

from .database import DatabaseManager
from .utils import logger, normalize_column_names
from .config import get_config


class SchemaManager:
    """Manages database schema evolution."""

    # Metadata columns added to every table
    METADATA_COLUMNS = [
        Column('source_file', Text),
        Column('load_timestamp', DateTime),
    ]

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.config = get_config()
        self.metadata = MetaData()

    def create_table_from_schema(self, table_name: str, columns: Dict[str, TypeEngine],
                                 file_path: Path = None) -> bool:
        """Create a new table from column schema.

        Args:
            table_name: Name of the table to create
            columns: Dict of column names to SQLAlchemy types
            file_path: Source file path for logging

        Returns:
            True if table was created
        """
        if self.config.skip_db:
            logger.info(f"SKIP_DB: Would create table {table_name}")
            return True

        if not self.db.engine:
            raise RuntimeError("Database not connected")

        # Build column definitions
        column_defs = []
        for col_name, col_type in columns.items():
            column_defs.append(Column(col_name, col_type, nullable=True))

        # Add metadata columns
        column_defs.extend(self.METADATA_COLUMNS)

        # Create table
        table = Table(table_name, self.metadata, *column_defs)
        table.create(self.db.engine)

        # Log schema change
        self.db.log_schema_change(
            table_name=table_name,
            change_type='create_table',
            file_path=file_path,
            details={'columns': list(columns.keys())}
        )

        logger.info(f"Created table: {table_name}")
        return True

    def add_column(self, table_name: str, column_name: str,
                   column_type: TypeEngine, file_path: Path = None) -> bool:
        """Add a column to an existing table.

        Args:
            table_name: Target table name
            column_name: New column name
            column_type: SQLAlchemy type for the column
            file_path: Source file path for logging

        Returns:
            True if column was added
        """
        if self.config.skip_db:
            logger.info(f"SKIP_DB: Would add column {column_name} to {table_name}")
            return True

        if not self.db.engine:
            raise RuntimeError("Database not connected")

        # Generate ALTER TABLE statement
        type_str = self._get_type_string(column_type)
        sql = f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {type_str}'

        self.db.execute_raw(sql)

        # Log schema change
        self.db.log_schema_change(
            table_name=table_name,
            change_type='add_column',
            column_name=column_name,
            new_type=type_str,
            file_path=file_path
        )

        logger.info(f"Added column {column_name} to {table_name}")
        return True

    def alter_column_type(self, table_name: str, column_name: str,
                          old_type: TypeEngine, new_type: TypeEngine,
                          file_path: Path = None) -> bool:
        """Alter column type with safety checks.

        Args:
            table_name: Target table name
            column_name: Column to alter
            old_type: Current column type
            new_type: Target column type
            file_path: Source file path for logging

        Returns:
            True if column was altered
        """
        if self.config.skip_db:
            logger.info(f"SKIP_DB: Would alter column {column_name} in {table_name}")
            return True

        if not self.db.engine:
            raise RuntimeError("Database not connected")

        # Check compatibility
        is_safe, resolved_type = self.db.is_type_compatible(old_type, new_type)

        old_type_str = self._get_type_string(old_type)
        resolved_type_str = self._get_type_string(resolved_type)

        # Generate ALTER TABLE statement
        sql = f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" TYPE {resolved_type_str}'

        # Add USING clause for type conversions that might fail
        if not is_safe:
            sql += f' USING "{column_name}"::text'

        self.db.execute_raw(sql)

        # Log schema change
        self.db.log_schema_change(
            table_name=table_name,
            change_type='alter_type',
            column_name=column_name,
            old_type=old_type_str,
            new_type=resolved_type_str,
            file_path=file_path,
            details={'safe_conversion': is_safe}
        )

        logger.info(f"Altered column {column_name} in {table_name}: "
                   f"{old_type_str} -> {resolved_type_str}")
        return True

    def sync_schema(self, table_name: str, df_columns: Dict[str, TypeEngine],
                    file_path: Path = None) -> Tuple[bool, Dict[str, TypeEngine]]:
        """Synchronize table schema with dataframe columns.

        This method handles:
        - Creating new tables
        - Adding new columns
        - Type compatibility checks and conversions

        Args:
            table_name: Target table name
            df_columns: Dict of column names to SQLAlchemy types from dataframe
            file_path: Source file path for logging

        Returns:
            Tuple of (schema_changed, resolved_columns)
        """
        schema_changed = False
        resolved_columns = dict(df_columns)

        # Check if table exists
        if not self.db.table_exists(table_name):
            # Create new table
            self.create_table_from_schema(table_name, df_columns, file_path)
            schema_changed = True

            # Add metadata columns to resolved columns
            resolved_columns['source_file'] = Text()
            resolved_columns['load_timestamp'] = DateTime()

            return schema_changed, resolved_columns

        # Table exists - check for schema changes
        existing_columns = self.db.get_table_columns(table_name)

        for col_name, col_type in df_columns.items():
            if col_name not in existing_columns:
                # New column - add it
                self.add_column(table_name, col_name, col_type, file_path)
                schema_changed = True
            else:
                # Column exists - check type compatibility
                existing_type = existing_columns[col_name]
                is_safe, resolved_type = self.db.is_type_compatible(
                    existing_type, col_type
                )

                if not is_safe or type(resolved_type) != type(existing_type):
                    # Type change needed
                    self.alter_column_type(
                        table_name, col_name, existing_type, resolved_type, file_path
                    )
                    schema_changed = True

                # Update resolved column type
                resolved_columns[col_name] = resolved_type

        return schema_changed, resolved_columns

    def _get_type_string(self, col_type: TypeEngine) -> str:
        """Convert SQLAlchemy type to PostgreSQL type string.

        Args:
            col_type: SQLAlchemy type

        Returns:
            PostgreSQL type string
        """
        type_name = col_type.__class__.__name__

        type_mapping = {
            'String': 'TEXT',
            'Text': 'TEXT',
            'Integer': 'INTEGER',
            'BigInteger': 'BIGINT',
            'SmallInteger': 'SMALLINT',
            'Float': 'DOUBLE PRECISION',
            'Double': 'DOUBLE PRECISION',
            'Numeric': 'NUMERIC',
            'DateTime': 'TIMESTAMP',
            'Date': 'DATE',
            'Boolean': 'BOOLEAN',
            'JSON': 'JSONB',
        }

        return type_mapping.get(type_name, 'TEXT')

    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """Get statistics for a table.

        Args:
            table_name: Table name

        Returns:
            Dict with table statistics
        """
        if self.config.skip_db or not self.db.engine:
            return {}

        stats = {
            'exists': self.db.table_exists(table_name),
            'columns': {},
            'row_count': 0
        }

        if not stats['exists']:
            return stats

        # Get column info
        stats['columns'] = self.db.get_table_columns(table_name)

        # Get row count
        sql = f'SELECT COUNT(*) FROM "{table_name}"'
        result = self.db.execute_raw(sql)
        if result:
            stats['row_count'] = result.scalar()

        return stats

    def revert_schema_changes(self, table_name: str, source_file: str,
                              dry_run: bool = True) -> List[Dict]:
        """Generate or execute schema change reversion.

        Args:
            table_name: Table name
            source_file: Source file that caused changes
            dry_run: If True, only generate report without executing

        Returns:
            List of revert actions
        """
        if self.config.skip_db or not self.db.engine:
            return []

        # Get schema changes for this table/file
        etl_schema_changes = self.db.metadata.tables.get('etl_schema_changes')
        if not etl_schema_changes:
            return []

        from sqlalchemy import select, and_

        with self.db.transaction() as conn:
            stmt = select(etl_schema_changes).where(
                and_(
                    etl_schema_changes.c.table_name == table_name,
                    etl_schema_changes.c.source_file == source_file
                )
            ).order_by(etl_schema_changes.c.changed_at.desc())

            result = conn.execute(stmt)
            changes = result.fetchall()

        revert_actions = []

        for change in changes:
            action = self._generate_revert_action(change, dry_run)
            if action:
                revert_actions.append(action)

        return revert_actions

    def _generate_revert_action(self, change, dry_run: bool) -> Optional[Dict]:
        """Generate a revert action for a schema change.

        Args:
            change: Schema change record
            dry_run: If True, don't execute

        Returns:
            Revert action dict or None
        """
        change_type = change.change_type
        table_name = change.table_name
        column_name = change.column_name

        action = {
            'change_type': change_type,
            'table_name': table_name,
            'column_name': column_name,
            'executed': False,
            'sql': None,
            'manual_steps': []
        }

        if change_type == 'create_table':
            # Cannot auto-revert table creation
            action['manual_steps'].append(
                f"DROP TABLE IF EXISTS \"{table_name}\" CASCADE"
            )
            action['note'] = "Table creation requires manual DROP - data loss risk"

        elif change_type == 'add_column':
            if not dry_run:
                sql = f'ALTER TABLE "{table_name}" DROP COLUMN "{column_name}"'
                try:
                    self.db.execute_raw(sql)
                    action['executed'] = True
                    action['sql'] = sql
                except Exception as e:
                    action['error'] = str(e)
                    action['manual_steps'].append(sql)
            else:
                action['sql'] = f'ALTER TABLE "{table_name}" DROP COLUMN "{column_name}"'

        elif change_type == 'alter_type':
            old_type = change.old_type
            if old_type and not dry_run:
                sql = f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" TYPE {old_type}'
                try:
                    self.db.execute_raw(sql)
                    action['executed'] = True
                    action['sql'] = sql
                except Exception as e:
                    action['error'] = str(e)
                    action['manual_steps'].append(sql)
            elif old_type:
                action['sql'] = f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" TYPE {old_type}'
            else:
                action['manual_steps'].append(
                    f"Manual review needed: Cannot determine original type for "
                    f"{table_name}.{column_name}"
                )

        return action
