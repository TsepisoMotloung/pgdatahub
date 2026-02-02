"""Main ETL orchestration logic"""
import logging
from pathlib import Path
from typing import Optional
import pandas as pd

from src.config import config
from src.database import db
from src.excel import discover_excel_files, read_excel_chunked, compute_file_hash
from src.utils import normalize_dataframe_columns, add_metadata_columns, infer_schema, clean_dataframe_for_pg
from src.schema import schema_manager
from src.importer import data_importer, import_tracker
from src.pause import PauseManager

logger = logging.getLogger(__name__)


def setup_logging(debug: bool = False):
    """Configure logging"""
    level = logging.DEBUG if debug else logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('etl.log')
        ]
    )
    
    # Reduce noise from libraries
    logging.getLogger('openpyxl').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy').setLevel(logging.WARNING)


def process_file(
    file_path: Path,
    table_name: str,
    sheet_name: str,
    pause_manager: PauseManager,
    folder_path: str
) -> bool:
    """
    Process a single Excel file.
    
    Returns True if successful, False if error occurred.
    """
    logger.info(f"Processing file: {file_path.name}")
    
    # Compute file hash for deduplication
    file_hash = compute_file_hash(file_path)
    source_file_str = str(file_path)
    
    # Check if already imported
    if data_importer.is_already_imported(table_name, source_file_str, file_hash):
        logger.info(f"File already imported, skipping: {file_path.name}")
        import_tracker.increment_skipped()
        return True
    
    try:
        total_rows = 0
        is_first_chunk_processed = False
        table_created = False
        schema = None  # Initialize schema variable
        
        # Read file in chunks
        for chunk_df, is_first_chunk in read_excel_chunked(file_path, sheet_name):
            
            if chunk_df.empty:
                continue
            
            # Normalize columns
            chunk_df = normalize_dataframe_columns(chunk_df)
            
            # Add metadata columns
            chunk_df = add_metadata_columns(chunk_df, source_file_str)
            
            # On first chunk, handle DDL
            if is_first_chunk and not is_first_chunk_processed:
                schema = infer_schema(chunk_df)
                
                if not db.table_exists(table_name):
                    # Create table
                    schema_manager.create_table(table_name, schema, source_file_str)
                    import_tracker.increment_schema_changes()
                    table_created = True
                else:
                    # Sync schema (add columns, widen types)
                    schema_manager.sync_schema(table_name, schema, source_file_str)
                
                is_first_chunk_processed = True
            
            # Clean data for PostgreSQL (convert types, handle nulls, parse dates)
            if schema:
                chunk_df = clean_dataframe_for_pg(chunk_df, schema)
            
            # Insert data
            rows_inserted = data_importer.insert_data(
                table_name=table_name,
                df=chunk_df,
                source_file=source_file_str,
                file_hash=file_hash
            )
            
            total_rows += rows_inserted
        
        # Log import completion
        if total_rows > 0:
            data_importer.log_import(table_name, source_file_str, file_hash, total_rows)
            import_tracker.add_rows(total_rows)
            import_tracker.increment_processed()
            logger.info(f"Successfully imported {total_rows} rows from {file_path.name}")
        else:
            logger.warning(f"No data imported from {file_path.name}")
            import_tracker.increment_skipped()
        
        return True
        
    except Exception as e:
        error_msg = f"Error processing {file_path.name}: {e}"
        logger.error(error_msg, exc_info=True)
        import_tracker.add_error(error_msg)
        
        # Write pause file if sectional commit is enabled
        if config.sectional_commit:
            pause_manager.write_pause_file(
                folder=folder_path,
                table=table_name,
                file=source_file_str,
                error=str(e)
            )
        
        return False


def process_folder(
    folder_parts: tuple,
    files: list,
    pause_manager: PauseManager
) -> bool:
    """
    Process all files in a folder.
    
    If sectional_commit is enabled, entire folder runs in one transaction.
    Returns True if successful, False if any file failed.
    """
    folder_path = '/'.join(folder_parts)
    logger.info("=" * 60)
    logger.info(f"Processing folder: {folder_path}")
    logger.info(f"Files: {len(files)}")
    logger.info("=" * 60)
    
    # Resolve table name and sheet name
    table_name = config.get_table_name(list(folder_parts))
    sheet_name = config.resolve_sheet_name(list(folder_parts))
    
    logger.info(f"Target table: {table_name}")
    logger.info(f"Sheet name: {sheet_name}")
    
    # Process each file
    success = True
    for file_path in files:
        file_success = process_file(
            file_path=file_path,
            table_name=table_name,
            sheet_name=sheet_name,
            pause_manager=pause_manager,
            folder_path=folder_path
        )
        
        if not file_success:
            success = False
            if config.sectional_commit:
                # Stop processing this folder on first error
                logger.error(f"Stopping folder processing due to error (sectional_commit=True)")
                break
        
        # Check if pause is needed
        if import_tracker.should_pause():
            logger.info("Pause threshold reached")
            pause_manager.execute_pause()
    
    return success


def run(data_root: Optional[str] = None):
    """
    Main ETL entry point.
    
    Args:
        data_root: Root directory containing Excel files (default: "data")
    """
    # Setup
    if data_root:
        config.data_root = Path(data_root)
    
    setup_logging(debug=config.debug)
    
    logger.info("=" * 60)
    logger.info("STARTING ETL RUN")
    logger.info("=" * 60)
    logger.info(f"Data root: {config.data_root}")
    logger.info(f"Sectional commit: {config.sectional_commit}")
    logger.info(f"Pause every: {config.pause_every} files")
    logger.info(f"Pause duration: {config.pause_seconds} seconds")
    logger.info(f"Chunk size: {config.chunk_size} rows")
    logger.info(f"Skip DB: {config.skip_db}")
    logger.info("=" * 60)
    
    # Initialize pause manager
    pause_manager = PauseManager(config.data_root)
    
    # Discover files
    folder_files = discover_excel_files(config.data_root)
    
    if not folder_files:
        logger.warning("No Excel files found")
        return
    
    # Process each folder
    for folder_parts, files in folder_files.items():
        folder_success = process_folder(folder_parts, files, pause_manager)
        
        if not folder_success and config.sectional_commit:
            logger.error("ETL stopped due to folder error")
            break
    
    # Summary
    import_tracker.log_summary()
    
    logger.info("=" * 60)
    logger.info("ETL RUN COMPLETED")
    logger.info("=" * 60)
    
    # Close database connections
    db.close()


if __name__ == "__main__":
    run()
