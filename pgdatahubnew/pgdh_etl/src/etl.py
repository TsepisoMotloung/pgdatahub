"""Main ETL orchestration module."""

import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

from sqlalchemy import insert

from .config import get_config
from .database import DatabaseManager
from .schema_manager import SchemaManager
from .excel_processor import ExcelProcessor
from .pause_manager import PauseManager, TransactionManager
from .utils import (
    logger, get_folders, get_excel_files, sanitize_table_name,
    get_folder_path_parts, MetricsCollector, compute_file_hash
)


class ETLOrchestrator:
    """Main ETL orchestrator class."""

    def __init__(self, data_root: Path):
        self.data_root = data_root
        self.config = get_config()

        # Initialize components
        self.db = DatabaseManager()
        self.schema_manager = SchemaManager(self.db)
        self.excel_processor = ExcelProcessor()
        self.pause_manager = PauseManager(data_root)
        self.transaction_manager = TransactionManager(self.db, self.pause_manager)
        self.metrics = MetricsCollector()

        # State tracking
        self._processed_files = 0
        self._current_folder: Optional[Path] = None
        self._current_table: Optional[str] = None

    def initialize(self) -> bool:
        """Initialize ETL components.

        Returns:
            True if initialization successful
        """
        try:
            # Connect to database
            self.db.connect()

            # Create tracking tables
            self.db.create_tracking_tables()

            logger.info("ETL initialized successfully")
            return True

        except Exception as e:
            logger.error(f"ETL initialization failed: {e}")
            return False

    def close(self) -> None:
        """Clean up resources."""
        self.db.close()

    def run(self, resume: bool = False) -> bool:
        """Run the ETL process.

        Args:
            resume: If True, resume from previous pause state

        Returns:
            True if ETL completed successfully
        """
        self.metrics.start()

        try:
            # Check for resume state
            if resume and self.pause_manager.has_pause_state():
                return self._resume_from_pause()

            # Clear any stale pause state
            self.pause_manager.clear_pause_state()

            # Discover folders
            folders = get_folders(self.data_root)
            if not folders:
                logger.warning(f"No folders found under {self.data_root}")
                return True

            logger.info(f"Discovered {len(folders)} folders to process")

            # Process each folder
            for folder in folders:
                if not self._process_folder(folder):
                    return False

            # Log final metrics
            self.metrics.log_summary()

            return True

        except Exception as e:
            logger.exception(f"ETL failed: {e}")
            self.metrics.record_error()
            return False

        finally:
            self.close()

    def _process_folder(self, folder: Path) -> bool:
        """Process a single folder.

        Args:
            folder: Folder to process

        Returns:
            True if folder processed successfully
        """
        self._current_folder = folder

        # Resolve table name
        folder_parts = get_folder_path_parts(folder, self.data_root)
        table_name = sanitize_table_name('_'.join(folder_parts))
        self._current_table = table_name

        # Resolve sheet name
        sheet_name = self.config.get_sheet_name(folder_parts)

        logger.info(f"Processing folder: {folder} -> table: {table_name}, sheet: {sheet_name}")

        # Discover Excel files
        excel_files = get_excel_files(folder, self.config.supported_extensions)
        if not excel_files:
            logger.info(f"No Excel files found in {folder}")
            return True

        logger.info(f"Found {len(excel_files)} Excel files in {folder}")

        # Start folder transaction if sectional commit enabled
        self.transaction_manager.start_folder(folder)

        try:
            for file_path in excel_files:
                if not self._process_file(file_path, table_name, sheet_name, folder):
                    return False

            # Commit folder if sectional commit enabled
            if self.config.sectional_commit:
                if not self.transaction_manager.commit_folder():
                    raise Exception("Failed to commit folder transaction")

            return True

        except Exception as e:
            logger.error(f"Error processing folder {folder}: {e}")

            # Rollback if sectional commit
            if self.config.sectional_commit:
                self.transaction_manager.rollback_folder()

            # Write pause state
            self.pause_manager.write_pause_state(
                folder, table_name,
                excel_files[0] if excel_files else folder,
                str(e)
            )

            return False

    def _process_file(self, file_path: Path, table_name: str,
                      sheet_name: str, folder: Path) -> bool:
        """Process a single Excel file.

        Args:
            file_path: Path to Excel file
            table_name: Target table name
            sheet_name: Sheet name to read
            folder: Current folder

        Returns:
            True if file processed successfully
        """
        file_start = time.time()

        try:
            # Validate file
            is_valid, errors = self.excel_processor.validate_file(file_path)
            if not is_valid:
                for error in errors:
                    logger.error(f"Validation error for {file_path}: {error}")
                self.metrics.record_error()
                return True  # Continue with other files

            # Compute file hash
            file_hash = compute_file_hash(file_path)

            # Check if already imported
            if self.db.is_file_imported(table_name, file_path, file_hash):
                logger.info(f"Skipping already imported file: {file_path}")
                self.metrics.record_file_skipped()
                self._processed_files += 1
                return True

            # Check if sheet exists
            if not self.excel_processor.has_sheet(file_path, sheet_name):
                logger.warning(f"Sheet '{sheet_name}' not found in {file_path}")
                logger.info(f"Available sheets: {self.excel_processor.get_sheet_names(file_path)}")
                return True

            # Process file in chunks
            total_rows = 0
            first_chunk = True
            resolved_columns = None

            for chunk in self.excel_processor.read_excel_streaming(file_path, sheet_name):
                if chunk.empty:
                    continue

                # Sync schema on first chunk
                if first_chunk:
                    from sqlalchemy.types import Text, DateTime

                    # Get column types from dataframe
                    df_columns = {
                        col: self.db.get_column_type(str(dtype))
                        for col, dtype in chunk.dtypes.items()
                    }

                    # Sync schema
                    _, resolved_columns = self.schema_manager.sync_schema(
                        table_name, df_columns, file_path
                    )

                    first_chunk = False

                # Add metadata columns
                chunk['source_file'] = str(file_path)
                chunk['load_timestamp'] = datetime.utcnow()

                # Insert data
                rows_inserted = self._insert_chunk(table_name, chunk)
                total_rows += rows_inserted

            # Log import
            folder_path = '/'.join(get_folder_path_parts(folder, self.data_root))
            self.db.log_import(table_name, file_path, file_hash, total_rows, folder_path)

            # Record metrics
            file_duration = time.time() - file_start
            self.metrics.record_file_processed(str(file_path), file_duration, total_rows)

            self._processed_files += 1

            # Check periodic pause
            if self.pause_manager.should_pause_periodic(self._processed_files):
                if self.config.sectional_commit:
                    self.transaction_manager.commit_folder()
                self.pause_manager.do_periodic_pause()
                if self.config.sectional_commit:
                    self.transaction_manager.start_folder(folder)

            logger.info(f"Processed {file_path}: {total_rows} rows")
            return True

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            self.metrics.record_error()

            # Write pause state
            self.pause_manager.write_pause_state(folder, table_name, file_path, str(e))

            return False

    def _insert_chunk(self, table_name: str, chunk: Any) -> int:
        """Insert a dataframe chunk into the database.

        Args:
            table_name: Target table name
            chunk: DataFrame chunk

        Returns:
            Number of rows inserted
        """
        if self.config.skip_db:
            return len(chunk)

        if not self.db.engine:
            raise RuntimeError("Database not connected")

        # Convert DataFrame to list of dicts
        records = chunk.to_dict('records')

        # Handle NaN values
        for record in records:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None

        # Get table
        table = self.db.metadata.tables.get(table_name)
        if not table:
            # Reflect table
            from sqlalchemy import Table
            table = Table(table_name, self.db.metadata, autoload_with=self.db.engine)

        # Insert data
        with self.db.transaction() as conn:
            if conn:
                stmt = insert(table).values(records)
                conn.execute(stmt)

        return len(records)

    def _resume_from_pause(self) -> bool:
        """Resume ETL from pause state.

        Returns:
            True if resume successful
        """
        state = self.pause_manager.read_pause_state()
        if not state:
            logger.warning("No pause state found to resume from")
            return self.run(resume=False)

        logger.info(f"Resuming from pause state: {state}")

        folder = Path(state['folder'])
        table_name = state['table']

        # Re-process the failed folder
        if folder.exists():
            success = self._process_folder(folder)

            if success:
                self.pause_manager.clear_pause_state()
                logger.info("Resume successful, continuing with remaining folders")

                # Continue with remaining folders
                folders = get_folders(self.data_root)
                for f in folders:
                    if f != folder:
                        if not self._process_folder(f):
                            return False

            return success
        else:
            logger.error(f"Folder from pause state no longer exists: {folder}")
            return False


class RevertManager:
    """Manages data and schema reversion."""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.config = get_config()

    def revert_by_file(self, source_file: str, dry_run: bool = True) -> Dict[str, Any]:
        """Revert imports by source file.

        Args:
            source_file: Source file path
            dry_run: If True, only generate report

        Returns:
            Revert report
        """
        report = {
            'source_file': source_file,
            'dry_run': dry_run,
            'actions': []
        }

        # Get import records
        imports = self.db.get_imports_by_file(source_file=source_file)

        if not imports:
            report['message'] = "No imports found for this file"
            return report

        for imp in imports:
            table_name = imp['table_name']
            row_count = imp['row_count']

            action = {
                'table_name': table_name,
                'row_count': row_count,
                'executed': False
            }

            if not dry_run:
                # Delete rows
                sql = f"""
                    DELETE FROM "{table_name}"
                    WHERE source_file = :source_file
                """
                try:
                    result = self.db.execute_raw(sql, {'source_file': source_file})
                    action['executed'] = True
                    action['deleted_rows'] = result.rowcount if result else 0
                except Exception as e:
                    action['error'] = str(e)

            report['actions'].append(action)

        if not dry_run:
            # Delete import records
            self.db.delete_import_records(source_file=source_file)

        return report

    def revert_by_hash(self, file_hash: str, dry_run: bool = True) -> Dict[str, Any]:
        """Revert imports by file hash.

        Args:
            file_hash: SHA-256 hash
            dry_run: If True, only generate report

        Returns:
            Revert report
        """
        report = {
            'file_hash': file_hash,
            'dry_run': dry_run,
            'actions': []
        }

        # Get import records
        imports = self.db.get_imports_by_file(file_hash=file_hash)

        if not imports:
            report['message'] = "No imports found for this hash"
            return report

        for imp in imports:
            source_file = imp['source_file']
            table_name = imp['table_name']

            action = {
                'source_file': source_file,
                'table_name': table_name,
                'executed': False
            }

            if not dry_run:
                sql = f"""
                    DELETE FROM "{table_name}"
                    WHERE source_file = :source_file
                """
                try:
                    result = self.db.execute_raw(sql, {'source_file': source_file})
                    action['executed'] = True
                    action['deleted_rows'] = result.rowcount if result else 0
                except Exception as e:
                    action['error'] = str(e)

            report['actions'].append(action)

        if not dry_run:
            self.db.delete_import_records(file_hash=file_hash)

        return report


def run(data_root: str = "data", resume: bool = False) -> bool:
    """Run the ETL process.

    Args:
        data_root: Root directory containing data folders
        resume: If True, resume from previous pause state

    Returns:
        True if ETL completed successfully
    """
    data_path = Path(data_root)

    if not data_path.exists():
        logger.error(f"Data root does not exist: {data_path}")
        return False

    orchestrator = ETLOrchestrator(data_path)

    if not orchestrator.initialize():
        return False

    return orchestrator.run(resume=resume)


def resume_from_pause(data_root: str = "data") -> bool:
    """Resume ETL from pause state.

    Args:
        data_root: Root directory containing data folders

    Returns:
        True if resume successful
    """
    return run(data_root=data_root, resume=True)


# Import pandas here to avoid circular import
import pandas as pd
