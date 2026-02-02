"""Database connection and schema management"""
import logging
from typing import Optional
from sqlalchemy import (
    create_engine, 
    MetaData, 
    Table, 
    Column, 
    String, 
    Integer, 
    BigInteger,
    DateTime,
    Text,
    inspect,
    text
)
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from src.config import config

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and schema operations"""
    
    def __init__(self):
        self.engine: Optional[Engine] = None
        self.metadata = MetaData()
        self._init_engine()
    
    def _init_engine(self):
        """Initialize SQLAlchemy engine"""
        if config.skip_db:
            logger.info("SKIP_DB=1: Running in dry-run mode, no database connection")
            return
        
        if not config.database_url:
            raise ValueError("No database URL configured. Set DATABASE_URL or config/config.json")
        
        # Create engine with psycopg3
        # NullPool for better connection management in ETL scenarios
        self.engine = create_engine(
            config.database_url,
            poolclass=NullPool,
            echo=config.debug,
            future=True
        )
        
        logger.info("Database engine initialized")
        self._ensure_metadata_tables()
    
    def _ensure_metadata_tables(self):
        """Ensure ETL metadata tables exist"""
        if not self.engine:
            return
        
        # etl_imports table
        Table(
            'etl_imports',
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('table_name', String(255), nullable=False),
            Column('source_file', String(500), nullable=False),
            Column('file_sha256', String(64), nullable=False),
            Column('row_count', Integer, nullable=False),
            Column('imported_at', DateTime, nullable=False),
        )
        
        # etl_schema_changes table
        Table(
            'etl_schema_changes',
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('table_name', String(255), nullable=False),
            Column('change_type', String(50), nullable=False),  # create_table, add_column, alter_type
            Column('column_name', String(255)),
            Column('old_type', String(100)),
            Column('new_type', String(100)),
            Column('source_file', String(500)),
            Column('changed_at', DateTime, nullable=False),
        )
        
        # Create tables if they don't exist
        self.metadata.create_all(self.engine)
        logger.info("Metadata tables ensured: etl_imports, etl_schema_changes")
    
    def get_connection(self):
        """Get a database connection"""
        if not self.engine:
            raise RuntimeError("Database not initialized (SKIP_DB mode or missing config)")
        return self.engine.connect()
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists"""
        if not self.engine:
            return False
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()
    
    def get_table_columns(self, table_name: str) -> dict:
        """
        Get column information for a table.
        Returns dict: {column_name: sqlalchemy_type}
        """
        if not self.engine:
            return {}
        
        inspector = inspect(self.engine)
        if table_name not in inspector.get_table_names():
            return {}
        
        columns = {}
        for col in inspector.get_columns(table_name):
            columns[col['name']] = col['type']
        
        logger.debug(f"Table {table_name} has {len(columns)} columns")
        return columns
    
    def execute_ddl(self, ddl_statement: str):
        """Execute a DDL statement"""
        if config.skip_db:
            logger.info(f"[DRY RUN] Would execute DDL: {ddl_statement}")
            return
        
        with self.get_connection() as conn:
            conn.execute(text(ddl_statement))
            conn.commit()
        
        logger.info(f"Executed DDL: {ddl_statement}")
    
    def close(self):
        """Close database connections"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connections closed")


# Global database manager
db = DatabaseManager()
