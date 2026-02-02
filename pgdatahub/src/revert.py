"""Revert and recovery operations"""
import logging
from typing import List, Optional
from sqlalchemy import text

from src.database import db
from src.config import config

logger = logging.getLogger(__name__)


def revert_by_file(table_name: str, source_file: str) -> int:
    """
    Revert data import by source file.
    
    Deletes all rows from table where source_file matches.
    Also removes entry from etl_imports.
    
    Returns number of rows deleted.
    """
    if config.skip_db:
        logger.info(f"[DRY RUN] Would revert {table_name} from {source_file}")
        return 0
    
    logger.info(f"Reverting import: {source_file} from {table_name}")
    
    with db.get_connection() as conn:
        # Delete data rows
        delete_sql = text(f"""
            DELETE FROM "{table_name}"
            WHERE source_file = :source_file
        """)
        result = conn.execute(delete_sql, {'source_file': source_file})
        rows_deleted = result.rowcount
        
        # Remove from etl_imports
        delete_import_sql = text("""
            DELETE FROM etl_imports
            WHERE table_name = :table_name
              AND source_file = :source_file
        """)
        conn.execute(delete_import_sql, {
            'table_name': table_name,
            'source_file': source_file
        })
        
        conn.commit()
    
    logger.info(f"Reverted {rows_deleted} rows from {table_name}")
    return rows_deleted


def revert_by_hash(table_name: str, file_hash: str) -> int:
    """
    Revert data import by file SHA-256 hash.
    
    Useful when source file path has changed.
    Returns number of rows deleted.
    """
    if config.skip_db:
        logger.info(f"[DRY RUN] Would revert {table_name} with hash {file_hash}")
        return 0
    
    logger.info(f"Reverting import by hash: {file_hash} from {table_name}")
    
    # First find the source_file
    with db.get_connection() as conn:
        find_sql = text("""
            SELECT source_file
            FROM etl_imports
            WHERE table_name = :table_name
              AND file_sha256 = :file_hash
        """)
        result = conn.execute(find_sql, {
            'table_name': table_name,
            'file_hash': file_hash
        })
        row = result.fetchone()
        
        if not row:
            logger.warning(f"No import found for hash {file_hash} in {table_name}")
            return 0
        
        source_file = row[0]
    
    # Revert using source_file
    return revert_by_file(table_name, source_file)


def revert_schema_changes(
    table_name: str,
    source_file: str,
    dry_run: bool = False
) -> List[str]:
    """
    Revert schema changes from a specific source file.
    
    WARNING: This is complex and may not always be reversible.
    Use dry_run=True to see what would be reverted.
    
    Returns list of DDL statements executed (or would be executed).
    """
    if config.skip_db:
        logger.info(f"[DRY RUN] Would revert schema changes for {table_name} from {source_file}")
        return []
    
    logger.info(f"Reverting schema changes for {table_name} from {source_file}")
    
    # Find schema changes
    with db.get_connection() as conn:
        find_sql = text("""
            SELECT id, change_type, column_name, old_type, new_type
            FROM etl_schema_changes
            WHERE table_name = :table_name
              AND source_file = :source_file
            ORDER BY changed_at DESC
        """)
        result = conn.execute(find_sql, {
            'table_name': table_name,
            'source_file': source_file
        })
        changes = result.fetchall()
    
    if not changes:
        logger.info(f"No schema changes found for {source_file} in {table_name}")
        return []
    
    ddl_statements = []
    
    for change_id, change_type, column_name, old_type, new_type in changes:
        if change_type == 'add_column':
            # Can drop column
            ddl = f'ALTER TABLE "{table_name}" DROP COLUMN "{column_name}"'
            ddl_statements.append(ddl)
            
        elif change_type == 'alter_type':
            if old_type:
                # Try to revert type change
                ddl = f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" TYPE {old_type} USING "{column_name}"::{old_type}'
                ddl_statements.append(ddl)
            else:
                logger.warning(f"Cannot revert type change for {column_name}: no old_type recorded")
        
        elif change_type == 'create_table':
            # Cannot safely drop table automatically
            logger.warning(f"Cannot automatically revert table creation: {table_name}")
            logger.warning(f"Manual action required: DROP TABLE \"{table_name}\"")
    
    if dry_run:
        logger.info("DRY RUN - would execute:")
        for ddl in ddl_statements:
            logger.info(f"  {ddl}")
        return ddl_statements
    
    # Execute DDL statements
    executed = []
    with db.get_connection() as conn:
        for ddl in ddl_statements:
            try:
                conn.execute(text(ddl))
                executed.append(ddl)
                logger.info(f"Executed: {ddl}")
            except Exception as e:
                logger.error(f"Failed to execute {ddl}: {e}")
        
        conn.commit()
    
    logger.info(f"Reverted {len(executed)} schema changes")
    return executed


def get_import_history(table_name: str, limit: int = 20) -> List[dict]:
    """Get import history for a table"""
    if config.skip_db:
        return []
    
    with db.get_connection() as conn:
        query = text("""
            SELECT source_file, file_sha256, row_count, imported_at
            FROM etl_imports
            WHERE table_name = :table_name
            ORDER BY imported_at DESC
            LIMIT :limit
        """)
        result = conn.execute(query, {
            'table_name': table_name,
            'limit': limit
        })
        
        history = []
        for row in result:
            history.append({
                'source_file': row[0],
                'file_sha256': row[1],
                'row_count': row[2],
                'imported_at': row[3]
            })
    
    return history


def get_schema_change_history(table_name: str, limit: int = 20) -> List[dict]:
    """Get schema change history for a table"""
    if config.skip_db:
        return []
    
    with db.get_connection() as conn:
        query = text("""
            SELECT change_type, column_name, old_type, new_type, source_file, changed_at
            FROM etl_schema_changes
            WHERE table_name = :table_name
            ORDER BY changed_at DESC
            LIMIT :limit
        """)
        result = conn.execute(query, {
            'table_name': table_name,
            'limit': limit
        })
        
        history = []
        for row in result:
            history.append({
                'change_type': row[0],
                'column_name': row[1],
                'old_type': row[2],
                'new_type': row[3],
                'source_file': row[4],
                'changed_at': row[5]
            })
    
    return history
