#!/usr/bin/env python
"""
Fix script to drop and recreate the etl_schema_changes table with correct schema.

The old table is missing the 'changed_at' column which is required by the code.
This script:
1. Drops the old etl_schema_changes table
2. Recreates it with the correct schema including 'changed_at'
"""

import logging
import sys
from src.config import config
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_schema_changes_table():
    """Drop and recreate etl_schema_changes table with correct schema"""
    
    if not config.database_url:
        logger.error("No database URL configured. Set DATABASE_URL or config/config.json")
        return False
    
    try:
        # Create a fresh engine connection
        engine = create_engine(
            config.database_url,
            poolclass=NullPool,
            future=True
        )
        
        with engine.connect() as conn:
            # Check if table exists
            inspector_result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'etl_schema_changes'
                )
            """))
            
            table_exists = inspector_result.scalar()
            
            if table_exists:
                logger.info("Found old etl_schema_changes table. Dropping it...")
                conn.execute(text("DROP TABLE IF EXISTS etl_schema_changes CASCADE"))
                conn.commit()
                logger.info("✓ Dropped etl_schema_changes table")
            else:
                logger.info("etl_schema_changes table does not exist")
            
            # Create table with correct schema
            logger.info("Creating etl_schema_changes table with correct schema...")
            conn.execute(text("""
                CREATE TABLE etl_schema_changes (
                    id SERIAL PRIMARY KEY,
                    table_name VARCHAR(255) NOT NULL,
                    change_type VARCHAR(50) NOT NULL,
                    column_name VARCHAR(255),
                    old_type VARCHAR(100),
                    new_type VARCHAR(100),
                    source_file VARCHAR(500),
                    changed_at TIMESTAMP NOT NULL
                )
            """))
            conn.commit()
            logger.info("✓ Created etl_schema_changes table")
            
            # Verify the table and columns
            result = conn.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'etl_schema_changes'
                ORDER BY ordinal_position
            """))
            
            logger.info("\nTable schema verified:")
            for row in result:
                logger.info(f"  - {row[0]}: {row[1]}")
        
        logger.info("\n✓ Fix completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"✗ Error during fix: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = fix_schema_changes_table()
    sys.exit(0 if success else 1)
