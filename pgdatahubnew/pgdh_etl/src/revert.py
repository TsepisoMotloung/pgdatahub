"""Revert and recovery module for ETL operations."""

import argparse
import json
from pathlib import Path
from typing import Dict, Any, Optional

from .database import DatabaseManager
from .schema_manager import SchemaManager
from .utils import logger


def revert_data_by_file(source_file: str, dry_run: bool = True,
                        database_url: Optional[str] = None) -> Dict[str, Any]:
    """Revert data imports by source file.

    Args:
        source_file: Source file path
        dry_run: If True, only generate report without executing
        database_url: Optional database URL

    Returns:
        Revert report
    """
    db = DatabaseManager(database_url)
    db.connect()

    try:
        report = {
            'operation': 'revert_by_file',
            'source_file': source_file,
            'dry_run': dry_run,
            'actions': [],
            'total_deleted_rows': 0
        }

        # Get import records
        imports = db.get_imports_by_file(source_file=source_file)

        if not imports:
            report['message'] = f"No imports found for file: {source_file}"
            return report

        logger.info(f"Found {len(imports)} import records for {source_file}")

        for imp in imports:
            table_name = imp['table_name']
            row_count = imp['row_count']
            file_hash = imp['file_sha256']

            action = {
                'table_name': table_name,
                'expected_rows': row_count,
                'file_hash': file_hash,
                'executed': False
            }

            if not dry_run:
                # Delete rows from table
                sql = f'DELETE FROM "{table_name}" WHERE source_file = :source_file'
                try:
                    result = db.execute_raw(sql, {'source_file': source_file})
                    deleted_rows = result.rowcount if result else 0
                    action['executed'] = True
                    action['deleted_rows'] = deleted_rows
                    report['total_deleted_rows'] += deleted_rows
                    logger.info(f"Deleted {deleted_rows} rows from {table_name}")
                except Exception as e:
                    action['error'] = str(e)
                    logger.error(f"Error deleting from {table_name}: {e}")
            else:
                action['sql'] = f'DELETE FROM "{table_name}" WHERE source_file = \'{source_file}\''

            report['actions'].append(action)

        if not dry_run:
            # Delete import records
            deleted = db.delete_import_records(source_file=source_file)
            report['import_records_deleted'] = deleted
            logger.info(f"Deleted {deleted} import tracking records")

        return report

    finally:
        db.close()


def revert_data_by_hash(file_hash: str, dry_run: bool = True,
                        database_url: Optional[str] = None) -> Dict[str, Any]:
    """Revert data imports by file hash.

    Args:
        file_hash: SHA-256 hash
        dry_run: If True, only generate report without executing
        database_url: Optional database URL

    Returns:
        Revert report
    """
    db = DatabaseManager(database_url)
    db.connect()

    try:
        report = {
            'operation': 'revert_by_hash',
            'file_hash': file_hash,
            'dry_run': dry_run,
            'actions': [],
            'total_deleted_rows': 0
        }

        # Get import records
        imports = db.get_imports_by_file(file_hash=file_hash)

        if not imports:
            report['message'] = f"No imports found for hash: {file_hash}"
            return report

        logger.info(f"Found {len(imports)} import records for hash {file_hash}")

        for imp in imports:
            source_file = imp['source_file']
            table_name = imp['table_name']
            row_count = imp['row_count']

            action = {
                'source_file': source_file,
                'table_name': table_name,
                'expected_rows': row_count,
                'executed': False
            }

            if not dry_run:
                sql = f'DELETE FROM "{table_name}" WHERE source_file = :source_file'
                try:
                    result = db.execute_raw(sql, {'source_file': source_file})
                    deleted_rows = result.rowcount if result else 0
                    action['executed'] = True
                    action['deleted_rows'] = deleted_rows
                    report['total_deleted_rows'] += deleted_rows
                    logger.info(f"Deleted {deleted_rows} rows from {table_name}")
                except Exception as e:
                    action['error'] = str(e)
                    logger.error(f"Error deleting from {table_name}: {e}")
            else:
                action['sql'] = f'DELETE FROM "{table_name}" WHERE source_file = \'{source_file}\''

            report['actions'].append(action)

        if not dry_run:
            deleted = db.delete_import_records(file_hash=file_hash)
            report['import_records_deleted'] = deleted
            logger.info(f"Deleted {deleted} import tracking records")

        return report

    finally:
        db.close()


def revert_schema_changes(table_name: str, source_file: str,
                          dry_run: bool = True,
                          database_url: Optional[str] = None) -> Dict[str, Any]:
    """Revert schema changes caused by a specific file.

    Args:
        table_name: Target table name
        source_file: Source file that caused changes
        dry_run: If True, only generate report without executing
        database_url: Optional database URL

    Returns:
        Revert report
    """
    db = DatabaseManager(database_url)
    db.connect()

    try:
        schema_manager = SchemaManager(db)

        report = {
            'operation': 'revert_schema_changes',
            'table_name': table_name,
            'source_file': source_file,
            'dry_run': dry_run,
            'actions': []
        }

        # Get revert actions
        actions = schema_manager.revert_schema_changes(
            table_name, source_file, dry_run
        )

        report['actions'] = actions
        report['manual_steps_required'] = any(
            a.get('manual_steps') for a in actions
        )

        if dry_run:
            report['message'] = "Dry run - no changes executed"
        else:
            executed = sum(1 for a in actions if a.get('executed'))
            report['executed_count'] = executed
            report['message'] = f"Executed {executed} of {len(actions)} revert actions"

        return report

    finally:
        db.close()


def main():
    """CLI entry point for revert operations."""
    parser = argparse.ArgumentParser(
        description='Revert ETL imports and schema changes'
    )

    parser.add_argument(
        '--by-file',
        help='Revert by source file path'
    )

    parser.add_argument(
        '--by-hash',
        help='Revert by file SHA-256 hash'
    )

    parser.add_argument(
        '--table',
        help='Table name (for schema revert)'
    )

    parser.add_argument(
        '--schema-revert',
        action='store_true',
        help='Revert schema changes'
    )

    parser.add_argument(
        '--execute',
        action='store_true',
        help='Execute revert (default is dry run)'
    )

    parser.add_argument(
        '--database-url',
        help='Database URL (overrides environment)'
    )

    parser.add_argument(
        '--output',
        help='Output file for revert report (JSON)'
    )

    args = parser.parse_args()

    dry_run = not args.execute

    if dry_run:
        logger.info("DRY RUN MODE - No changes will be executed")
        logger.info("Use --execute to perform actual revert")

    report = None

    if args.by_file:
        if args.schema_revert and args.table:
            report = revert_schema_changes(
                args.table, args.by_file, dry_run, args.database_url
            )
        else:
            report = revert_data_by_file(
                args.by_file, dry_run, args.database_url
            )

    elif args.by_hash:
        report = revert_data_by_hash(
            args.by_hash, dry_run, args.database_url
        )

    else:
        parser.error("Must specify --by-file or --by-hash")

    # Print report
    print(json.dumps(report, indent=2, default=str))

    # Save to file if specified
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        logger.info(f"Report saved to {args.output}")


if __name__ == '__main__':
    main()
