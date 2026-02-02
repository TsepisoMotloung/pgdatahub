#!/usr/bin/env python3
"""Reset and backup script for PGDataHub ETL."""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from src.database import DatabaseManager
from src.config import get_config
from src.utils import logger


def create_backup(database_url: str, backup_path: Path) -> bool:
    """Create database backup using pg_dump.

    Args:
        database_url: PostgreSQL connection string
        backup_path: Path to save backup

    Returns:
        True if backup successful
    """
    try:
        # Parse database URL
        # Format: postgresql+psycopg://user:password@host:port/database
        url = database_url.replace('postgresql+psycopg://', 'postgresql://')

        # Use pg_dump for backup
        cmd = [
            'pg_dump',
            '--format', 'custom',
            '--file', str(backup_path),
            url
        ]

        logger.info(f"Creating backup: {backup_path}")
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            logger.info(f"Backup created successfully: {backup_path}")
            return True
        else:
            logger.error(f"Backup failed: {result.stderr}")
            return False

    except FileNotFoundError:
        logger.warning("pg_dump not found. Backup skipped.")
        logger.info("To enable backups, ensure pg_dump is in your PATH")
        return False
    except Exception as e:
        logger.error(f"Backup error: {e}")
        return False


def reset_database(database_url: str, confirm: bool = True) -> bool:
    """Reset database by dropping and recreating tables.

    Args:
        database_url: PostgreSQL connection string
        confirm: If True, prompt for confirmation

    Returns:
        True if reset successful
    """
    if confirm:
        response = input(
            "WARNING: This will DROP all ETL tables and data.\n"
            "Type 'RESET' to confirm: "
        )
        if response != 'RESET':
            logger.info("Reset cancelled")
            return False

    db = DatabaseManager(database_url)
    db.connect()

    try:
        # Get list of tables to drop
        tables_to_drop = []

        if db.table_exists('etl_imports'):
            tables_to_drop.append('etl_imports')
        if db.table_exists('etl_schema_changes'):
            tables_to_drop.append('etl_schema_changes')

        # Get data tables from etl_imports
        if db.table_exists('etl_imports'):
            result = db.execute_raw(
                'SELECT DISTINCT table_name FROM etl_imports'
            )
            if result:
                for row in result:
                    if row[0] not in tables_to_drop:
                        tables_to_drop.append(row[0])

        logger.info(f"Tables to drop: {tables_to_drop}")

        # Drop tables
        for table in tables_to_drop:
            try:
                db.execute_raw(f'DROP TABLE IF EXISTS "{table}" CASCADE')
                logger.info(f"Dropped table: {table}")
            except Exception as e:
                logger.error(f"Error dropping {table}: {e}")

        logger.info("Database reset complete")
        return True

    finally:
        db.close()


def list_imports(database_url: str = None) -> None:
    """List all imports in the database.

    Args:
        database_url: Optional database URL
    """
    db = DatabaseManager(database_url)
    db.connect()

    try:
        if not db.table_exists('etl_imports'):
            logger.info("No imports found (etl_imports table does not exist)")
            return

        result = db.execute_raw(
            '''
            SELECT table_name, source_file, row_count, imported_at
            FROM etl_imports
            ORDER BY imported_at DESC
            '''
        )

        if not result:
            logger.info("No imports found")
            return

        print("\n" + "=" * 100)
        print(f"{'Table':<30} {'Rows':<10} {'Imported At':<25} {'Source File'}")
        print("=" * 100)

        for row in result:
            table, source, rows, imported = row
            source_str = str(source)
            if len(source_str) > 35:
                source_str = "..." + source_str[-32:]
            print(f"{table:<30} {rows:<10} {str(imported):<25} {source_str}")

        print("=" * 100)

        # Summary
        count_result = db.execute_raw(
            'SELECT COUNT(*), SUM(row_count) FROM etl_imports'
        )
        if count_result:
            total_files, total_rows = count_result.fetchone()
            print(f"\nTotal: {total_files} files, {total_rows or 0} rows")

    finally:
        db.close()


def list_schema_changes(database_url: str = None) -> None:
    """List all schema changes in the database.

    Args:
        database_url: Optional database URL
    """
    db = DatabaseManager(database_url)
    db.connect()

    try:
        if not db.table_exists('etl_schema_changes'):
            logger.info("No schema changes found")
            return

        result = db.execute_raw(
            '''
            SELECT table_name, change_type, column_name, old_type, new_type, changed_at
            FROM etl_schema_changes
            ORDER BY changed_at DESC
            LIMIT 50
            '''
        )

        if not result:
            logger.info("No schema changes found")
            return

        print("\n" + "=" * 100)
        print(f"{'Table':<25} {'Change':<15} {'Column':<20} {'Type Change':<25} {'Changed At'}")
        print("=" * 100)

        for row in result:
            table, change, col, old, new, changed = row
            type_change = f"{old or '-'} -> {new or '-'}"
            print(f"{table:<25} {change:<15} {col or '-':<20} {type_change:<25} {str(changed)}")

        print("=" * 100)

    finally:
        db.close()


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Reset and backup tool for PGDataHub ETL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python reset_and_run.py --backup
  python reset_and_run.py --reset --yes
  python reset_and_run.py --backup --reset --run --yes
  python reset_and_run.py --list-imports
  python reset_and_run.py --list-schema-changes
        """
    )

    parser.add_argument(
        '--backup',
        action='store_true',
        help='Create database backup before reset'
    )

    parser.add_argument(
        '--backup-dir',
        default='backups',
        help='Directory for backups (default: backups)'
    )

    parser.add_argument(
        '--reset',
        action='store_true',
        help='Reset database (drop all ETL tables)'
    )

    parser.add_argument(
        '--run',
        action='store_true',
        help='Run ETL after reset'
    )

    parser.add_argument(
        '--data-root',
        default='data',
        help='Data root directory for ETL run'
    )

    parser.add_argument(
        '--yes',
        action='store_true',
        help='Skip confirmation prompts'
    )

    parser.add_argument(
        '--list-imports',
        action='store_true',
        help='List all imports'
    )

    parser.add_argument(
        '--list-schema-changes',
        action='store_true',
        help='List all schema changes'
    )

    parser.add_argument(
        '--database-url',
        help='Database URL (overrides environment)'
    )

    args = parser.parse_args()

    config = get_config()
    database_url = args.database_url or config.database_url

    # List operations
    if args.list_imports:
        list_imports(database_url)
        return 0

    if args.list_schema_changes:
        list_schema_changes(database_url)
        return 0

    # Backup
    if args.backup:
        backup_dir = Path(args.backup_dir)
        backup_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = backup_dir / f'pgdh_backup_{timestamp}.dump'

        if not create_backup(database_url, backup_path):
            if not args.yes:
                response = input("Backup failed. Continue anyway? (y/N): ")
                if response.lower() != 'y':
                    return 1

    # Reset
    if args.reset:
        if not reset_database(database_url, confirm=not args.yes):
            return 1

    # Run ETL
    if args.run:
        from src.etl import run
        success = run(data_root=args.data_root)
        return 0 if success else 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
