"""Data import with deduplication and metadata tracking"""
import logging
import datetime
from pathlib import Path
from typing import Optional
import pandas as pd
from sqlalchemy import text

from src.database import db
from src.config import config
from src.excel import compute_file_hash

logger = logging.getLogger(__name__)


class DataImporter:
    """Handles data import with deduplication"""
    
    def __init__(self):
        pass
    
    def _convert_nat_to_none(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert pandas NaT (Not a Time) values to None.
        NaT cannot be inserted into PostgreSQL TIMESTAMP columns,
        so convert to None which becomes SQL NULL.
        """
        # Find all object and datetime columns
        for col in df.columns:
            if df[col].dtype == 'object' or pd.api.types.is_datetime64_any_dtype(df[col]):
                # Check if column contains NaT
                if df[col].isna().any():
                    # Replace NaT with None
                    df[col] = df[col].where(pd.notna(df[col]), None)
        
        return df
    
    def is_already_imported(self, table_name: str, source_file: str, file_hash: str) -> bool:
        """
        Check if file has already been imported.
        
        Returns True if (table_name, source_file, file_sha256) exists in etl_imports.
        """
        if config.skip_db:
            return False
        
        query = text("""
            SELECT COUNT(*) as cnt
            FROM etl_imports
            WHERE table_name = :table_name
              AND source_file = :source_file
              AND file_sha256 = :file_hash
        """)
        
        with db.get_connection() as conn:
            result = conn.execute(query, {
                'table_name': table_name,
                'source_file': source_file,
                'file_hash': file_hash
            })
            count = result.fetchone()[0]
        
        return count > 0
    
    def insert_data(
        self, 
        table_name: str, 
        df: pd.DataFrame,
        source_file: str,
        file_hash: str
    ) -> int:
        """
        Insert DataFrame into table.
        
        Returns number of rows inserted.
        """
        if df.empty:
            logger.warning(f"Empty DataFrame for {table_name}, skipping insert")
            return 0
        
        if config.skip_db:
            logger.info(f"[DRY RUN] Would insert {len(df)} rows into {table_name}")
            return len(df)
        
        # Handle NaT (Not a Time) values in timestamp columns
        # Convert pandas NaT to None (SQL NULL) to avoid timestamp parsing errors
        df = self._convert_nat_to_none(df)
        
        # Convert DataFrame to records
        records = df.to_dict('records')
        
        # Build INSERT statement
        columns = list(df.columns)
        placeholders = ', '.join([f':{col}' for col in columns])
        columns_str = ', '.join([f'"{col}"' for col in columns])
        
        insert_sql = text(f"""
            INSERT INTO "{table_name}" ({columns_str})
            VALUES ({placeholders})
        """)
        
        # Execute bulk insert
        with db.get_connection() as conn:
            conn.execute(insert_sql, records)
            conn.commit()
        
        logger.info(f"Inserted {len(df)} rows into {table_name}")
        return len(df)
    
    def log_import(
        self,
        table_name: str,
        source_file: str,
        file_hash: str,
        row_count: int
    ):
        """Log completed import to etl_imports table"""
        if config.skip_db:
            return
        
        insert_sql = text("""
            INSERT INTO etl_imports
            (table_name, source_file, file_sha256, row_count, imported_at)
            VALUES
            (:table_name, :source_file, :file_hash, :row_count, :imported_at)
        """)
        
        with db.get_connection() as conn:
            conn.execute(insert_sql, {
                'table_name': table_name,
                'source_file': source_file,
                'file_hash': file_hash,
                'row_count': row_count,
                'imported_at': datetime.datetime.utcnow()
            })
            conn.commit()
        
        logger.info(f"Logged import: {source_file} -> {table_name} ({row_count} rows)")


class ImportTracker:
    """Tracks import progress for pause/resume"""
    
    def __init__(self):
        self.files_processed = 0
        self.files_skipped = 0
        self.rows_inserted = 0
        self.schema_changes = 0
        self.errors = []
    
    def increment_processed(self):
        """Increment processed file counter"""
        self.files_processed += 1
    
    def increment_skipped(self):
        """Increment skipped file counter"""
        self.files_skipped += 1
    
    def add_rows(self, count: int):
        """Add to row count"""
        self.rows_inserted += count
    
    def increment_schema_changes(self):
        """Increment schema change counter"""
        self.schema_changes += 1
    
    def add_error(self, error: str):
        """Record an error"""
        self.errors.append(error)
    
    def should_pause(self) -> bool:
        """Check if pause is needed based on config"""
        if config.pause_every <= 0:
            return False
        
        total_files = self.files_processed + self.files_skipped
        return total_files > 0 and total_files % config.pause_every == 0
    
    def get_summary(self) -> dict:
        """Get import summary statistics"""
        return {
            'files_processed': self.files_processed,
            'files_skipped': self.files_skipped,
            'total_files': self.files_processed + self.files_skipped,
            'rows_inserted': self.rows_inserted,
            'schema_changes': self.schema_changes,
            'errors': len(self.errors),
            'error_details': self.errors
        }
    
    def log_summary(self):
        """Log summary statistics"""
        summary = self.get_summary()
        logger.info("=" * 60)
        logger.info("ETL RUN SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Files processed: {summary['files_processed']}")
        logger.info(f"Files skipped: {summary['files_skipped']}")
        logger.info(f"Total files: {summary['total_files']}")
        logger.info(f"Rows inserted: {summary['rows_inserted']}")
        logger.info(f"Schema changes: {summary['schema_changes']}")
        logger.info(f"Errors: {summary['errors']}")
        
        if summary['error_details']:
            logger.error("Error details:")
            for error in summary['error_details']:
                logger.error(f"  - {error}")
        
        logger.info("=" * 60)


# Global instances
data_importer = DataImporter()
import_tracker = ImportTracker()
