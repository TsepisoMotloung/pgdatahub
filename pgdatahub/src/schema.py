"""Schema evolution and DDL management"""
import logging
import datetime
from typing import Dict, List, Optional
from sqlalchemy import text

from src.database import db
from src.config import config

logger = logging.getLogger(__name__)


class SchemaManager:
    """Manages schema evolution with safe type widening"""
    
    TYPE_HIERARCHY = {
        # Integer types
        'SMALLINT': ['INTEGER', 'BIGINT', 'DOUBLE PRECISION', 'TEXT'],
        'INTEGER': ['BIGINT', 'DOUBLE PRECISION', 'TEXT'],
        'BIGINT': ['DOUBLE PRECISION', 'TEXT'],
        
        # Float types
        'REAL': ['DOUBLE PRECISION', 'TEXT'],
        'DOUBLE PRECISION': ['TEXT'],
        
        # Boolean
        'BOOLEAN': ['TEXT'],
        
        # Date/Time
        'DATE': ['TIMESTAMP', 'TEXT'],
        'TIMESTAMP': ['TEXT'],
        'TIMESTAMP WITHOUT TIME ZONE': ['TEXT'],
        
        # Text (terminal)
        'TEXT': [],
    }
    
    def __init__(self):
        pass
    
    def create_table(self, table_name: str, schema: Dict[str, str], source_file: str):
        """
        Create a new table with given schema.
        
        Args:
            table_name: Name of table to create
            schema: Dict of {column_name: pg_type}
            source_file: Source file for audit trail
        """
        if db.table_exists(table_name):
            logger.info(f"Table {table_name} already exists, skipping creation")
            return
        
        # Build CREATE TABLE statement
        column_defs = []
        for col_name, col_type in schema.items():
            column_defs.append(f'"{col_name}" {col_type}')
        
        ddl = f"""
        CREATE TABLE "{table_name}" (
            {', '.join(column_defs)}
        )
        """
        
        logger.info(f"Creating table: {table_name} with {len(schema)} columns")
        
        if not config.skip_db:
            db.execute_ddl(ddl)
            
            # Log schema change
            self._log_schema_change(
                table_name=table_name,
                change_type='create_table',
                column_name=None,
                old_type=None,
                new_type=None,
                source_file=source_file
            )
        else:
            logger.info(f"[DRY RUN] Would create table {table_name}")
    
    def sync_schema(
        self, 
        table_name: str, 
        target_schema: Dict[str, str],
        source_file: str
    ) -> Dict[str, str]:
        """
        Synchronize table schema with target schema.
        
        Adds missing columns and widens types as needed.
        Returns final schema.
        
        Args:
            table_name: Name of table
            target_schema: Desired schema
            source_file: Source file for audit
        """
        if not db.table_exists(table_name):
            # Table doesn't exist, will be created separately
            return target_schema
        
        current_columns = db.get_table_columns(table_name)
        
        # Check for new columns
        for col_name, target_type in target_schema.items():
            if col_name not in current_columns:
                self._add_column(table_name, col_name, target_type, source_file)
            else:
                # Check if type widening needed
                current_type = self._normalize_pg_type(str(current_columns[col_name]))
                target_type_norm = self._normalize_pg_type(target_type)
                
                if current_type != target_type_norm:
                    self._widen_column_type(
                        table_name, 
                        col_name, 
                        current_type, 
                        target_type_norm,
                        source_file
                    )
        
        # Return updated schema
        return db.get_table_columns(table_name)
    
    def _add_column(self, table_name: str, column_name: str, column_type: str, source_file: str):
        """Add a new column to table"""
        ddl = f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_type}'
        
        logger.info(f"Adding column {column_name} ({column_type}) to {table_name}")
        
        if not config.skip_db:
            db.execute_ddl(ddl)
            
            self._log_schema_change(
                table_name=table_name,
                change_type='add_column',
                column_name=column_name,
                old_type=None,
                new_type=column_type,
                source_file=source_file
            )
        else:
            logger.info(f"[DRY RUN] Would add column {column_name}")
    
    def _widen_column_type(
        self, 
        table_name: str, 
        column_name: str, 
        current_type: str,
        target_type: str,
        source_file: str
    ):
        """
        Widen column type if safe, otherwise convert to TEXT.
        """
        current_type_norm = self._normalize_pg_type(current_type)
        target_type_norm = self._normalize_pg_type(target_type)
        
        if current_type_norm == target_type_norm:
            return  # No change needed
        
        # Check if widening is safe
        new_type = self._get_safe_widening(current_type_norm, target_type_norm)
        
        if new_type:
            logger.info(f"Widening column {table_name}.{column_name}: {current_type_norm} → {new_type}")
            
            ddl = f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" TYPE {new_type}'
            
            if not config.skip_db:
                try:
                    db.execute_ddl(ddl)
                    
                    self._log_schema_change(
                        table_name=table_name,
                        change_type='alter_type',
                        column_name=column_name,
                        old_type=current_type_norm,
                        new_type=new_type,
                        source_file=source_file
                    )
                except Exception as e:
                    logger.error(f"Failed to widen type, converting to TEXT: {e}")
                    self._convert_to_text(table_name, column_name, current_type_norm, source_file)
            else:
                logger.info(f"[DRY RUN] Would widen column type")
        else:
            # Incompatible types, convert to TEXT
            logger.warning(f"Incompatible types for {table_name}.{column_name}: {current_type_norm} vs {target_type_norm}, converting to TEXT")
            self._convert_to_text(table_name, column_name, current_type_norm, source_file)
    
    def _convert_to_text(self, table_name: str, column_name: str, current_type: str, source_file: str):
        """Convert a column to TEXT"""
        ddl = f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" TYPE TEXT USING "{column_name}"::TEXT'
        
        if not config.skip_db:
            db.execute_ddl(ddl)
            
            self._log_schema_change(
                table_name=table_name,
                change_type='alter_type',
                column_name=column_name,
                old_type=current_type,
                new_type='TEXT',
                source_file=source_file
            )
        else:
            logger.info(f"[DRY RUN] Would convert {column_name} to TEXT")
    
    def _get_safe_widening(self, current_type: str, target_type: str) -> Optional[str]:
        """
        Determine safe type widening.
        Returns new type if safe, None if incompatible.
        """
        current_norm = self._normalize_pg_type(current_type)
        target_norm = self._normalize_pg_type(target_type)
        
        if current_norm not in self.TYPE_HIERARCHY:
            return 'TEXT'
        
        allowed_widenings = self.TYPE_HIERARCHY[current_norm]
        
        if target_norm in allowed_widenings:
            return target_norm
        
        return None
    
    def _normalize_pg_type(self, pg_type: str) -> str:
        """Normalize PostgreSQL type string"""
        pg_type = pg_type.upper().strip()
        
        # Handle common aliases
        type_map = {
            'INT': 'INTEGER',
            'INT4': 'INTEGER',
            'INT8': 'BIGINT',
            'FLOAT': 'DOUBLE PRECISION',
            'FLOAT8': 'DOUBLE PRECISION',
            'VARCHAR': 'TEXT',
            'CHARACTER VARYING': 'TEXT',
        }
        
        return type_map.get(pg_type, pg_type)
    
    def _log_schema_change(
        self,
        table_name: str,
        change_type: str,
        column_name: Optional[str],
        old_type: Optional[str],
        new_type: Optional[str],
        source_file: str
    ):
        """Log schema change to etl_schema_changes table"""
        if config.skip_db:
            return
        
        insert_sql = text("""
            INSERT INTO etl_schema_changes 
            (table_name, change_type, column_name, old_type, new_type, source_file, changed_at)
            VALUES 
            (:table_name, :change_type, :column_name, :old_type, :new_type, :source_file, :changed_at)
        """)
        
        with db.get_connection() as conn:
            conn.execute(insert_sql, {
                'table_name': table_name,
                'change_type': change_type,
                'column_name': column_name,
                'old_type': old_type,
                'new_type': new_type,
                'source_file': source_file,
                'changed_at': datetime.datetime.utcnow()
            })
            conn.commit()


# Global schema manager
schema_manager = SchemaManager()
