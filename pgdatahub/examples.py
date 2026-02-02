#!/usr/bin/env python
"""
Example: Using PGDataHub ETL Programmatically

This script demonstrates how to use PGDataHub as a library
rather than through the CLI.
"""
import os
from pathlib import Path

# Set environment variables before importing
os.environ['DATABASE_URL'] = 'postgresql://postgres:password@localhost:5432/example_db'
os.environ['ETL_SECTIONAL_COMMIT'] = '1'
os.environ['ETL_PAUSE_EVERY'] = '5'
os.environ['ETL_PAUSE_SECONDS'] = '10'

from src.etl import run
from src.pause import resume_from_pause, PauseManager
from src.revert import revert_by_file, get_import_history
from src.database import db
from sqlalchemy import text


def example_basic_etl():
    """Example 1: Basic ETL execution"""
    print("=" * 60)
    print("Example 1: Basic ETL")
    print("=" * 60)
    
    # Run ETL on data directory
    run(data_root="data")
    
    print("✓ ETL completed!")


def example_custom_data_root():
    """Example 2: ETL with custom data directory"""
    print("=" * 60)
    print("Example 2: Custom Data Root")
    print("=" * 60)
    
    # Create custom directory structure
    custom_dir = Path("my_data")
    custom_dir.mkdir(exist_ok=True)
    
    (custom_dir / "sales").mkdir(exist_ok=True)
    (custom_dir / "marketing").mkdir(exist_ok=True)
    
    # Run ETL
    run(data_root=str(custom_dir))
    
    print("✓ Custom data ETL completed!")


def example_query_results():
    """Example 3: Query imported data"""
    print("=" * 60)
    print("Example 3: Query Results")
    print("=" * 60)
    
    # Query a table
    with db.get_connection() as conn:
        # Get table list
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
              AND table_name NOT LIKE 'etl_%'
        """))
        
        tables = [row[0] for row in result]
        print(f"Found {len(tables)} tables:")
        
        for table in tables:
            # Count rows
            result = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"'))
            count = result.fetchone()[0]
            print(f"  - {table}: {count} rows")


def example_import_history():
    """Example 4: View import history"""
    print("=" * 60)
    print("Example 4: Import History")
    print("=" * 60)
    
    with db.get_connection() as conn:
        result = conn.execute(text("""
            SELECT table_name, COUNT(*) as file_count, SUM(row_count) as total_rows
            FROM etl_imports
            GROUP BY table_name
        """))
        
        print("Import Summary:")
        for row in result:
            print(f"  {row[0]}: {row[1]} files, {row[2]} rows")


def example_revert_import():
    """Example 5: Revert a specific import"""
    print("=" * 60)
    print("Example 5: Revert Import")
    print("=" * 60)
    
    # Get a source file to revert
    with db.get_connection() as conn:
        result = conn.execute(text("""
            SELECT table_name, source_file
            FROM etl_imports
            LIMIT 1
        """))
        row = result.fetchone()
        
        if row:
            table_name, source_file = row
            
            print(f"Reverting: {source_file} from {table_name}")
            
            # Revert
            rows_deleted = revert_by_file(table_name, source_file)
            print(f"✓ Reverted {rows_deleted} rows")
        else:
            print("No imports to revert")


def example_resume_from_failure():
    """Example 6: Resume from pause file"""
    print("=" * 60)
    print("Example 6: Resume from Pause")
    print("=" * 60)
    
    pause_manager = PauseManager(Path("data"))
    
    if pause_manager.has_pause_file():
        print("Pause file found, resuming...")
        resume_from_pause(Path("data"))
        print("✓ Resume completed!")
    else:
        print("No pause file found")


def example_schema_inspection():
    """Example 7: Inspect schema changes"""
    print("=" * 60)
    print("Example 7: Schema Changes")
    print("=" * 60)
    
    with db.get_connection() as conn:
        result = conn.execute(text("""
            SELECT table_name, change_type, column_name, old_type, new_type, changed_at
            FROM etl_schema_changes
            ORDER BY changed_at DESC
            LIMIT 10
        """))
        
        print("Recent schema changes:")
        for row in result:
            table, change_type, col, old_type, new_type, changed_at = row
            
            if change_type == 'create_table':
                print(f"  [{changed_at}] Created table: {table}")
            elif change_type == 'add_column':
                print(f"  [{changed_at}] {table}: Added column {col} ({new_type})")
            elif change_type == 'alter_type':
                print(f"  [{changed_at}] {table}.{col}: {old_type} → {new_type}")


def example_dry_run():
    """Example 8: Dry run (no database writes)"""
    print("=" * 60)
    print("Example 8: Dry Run")
    print("=" * 60)
    
    # Enable dry run
    from src.config import config
    config.skip_db = True
    
    print("Running ETL in dry-run mode...")
    run(data_root="data")
    
    print("✓ Dry run completed (check etl.log for details)")
    
    # Disable dry run
    config.skip_db = False


def example_get_table_data():
    """Example 9: Get data from specific table"""
    print("=" * 60)
    print("Example 9: Get Table Data")
    print("=" * 60)
    
    table_name = "folder_a"  # Adjust to your table
    
    if db.table_exists(table_name):
        with db.get_connection() as conn:
            # Get sample data
            result = conn.execute(text(f'SELECT * FROM "{table_name}" LIMIT 5'))
            
            print(f"Sample data from {table_name}:")
            for row in result:
                print(f"  {dict(row._mapping)}")
    else:
        print(f"Table {table_name} does not exist")


def example_batch_processing():
    """Example 10: Process multiple directories"""
    print("=" * 60)
    print("Example 10: Batch Processing")
    print("=" * 60)
    
    directories = [
        "data/2023",
        "data/2024",
        "data/archive"
    ]
    
    for directory in directories:
        if Path(directory).exists():
            print(f"\nProcessing: {directory}")
            try:
                run(data_root=directory)
                print(f"✓ Completed: {directory}")
            except Exception as e:
                print(f"✗ Failed: {directory} - {e}")


def main():
    """Run all examples"""
    print("\n" + "=" * 60)
    print("PGDataHub ETL - Programmatic Usage Examples")
    print("=" * 60 + "\n")
    
    examples = [
        ("Basic ETL", example_basic_etl),
        ("Custom Data Root", example_custom_data_root),
        ("Query Results", example_query_results),
        ("Import History", example_import_history),
        ("Schema Inspection", example_schema_inspection),
        ("Dry Run", example_dry_run),
        # Commented out examples that modify data:
        # ("Revert Import", example_revert_import),
        # ("Resume from Pause", example_resume_from_failure),
    ]
    
    print("Available examples:")
    for i, (name, _) in enumerate(examples, 1):
        print(f"  {i}. {name}")
    
    print("\nRunning examples...\n")
    
    try:
        for name, func in examples:
            try:
                func()
                print()
            except Exception as e:
                print(f"Error in {name}: {e}\n")
    finally:
        # Cleanup
        db.close()
    
    print("=" * 60)
    print("Examples completed!")
    print("=" * 60)


if __name__ == '__main__':
    # Note: Update DATABASE_URL at the top of this file before running
    main()
