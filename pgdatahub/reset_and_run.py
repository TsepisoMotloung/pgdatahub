"""Reset database and run ETL - USE WITH CAUTION"""
import sys
import subprocess
import logging
from pathlib import Path
import click

sys.path.insert(0, str(Path(__file__).parent))

from src.database import db
from src.config import config
from src.etl import run

logger = logging.getLogger(__name__)


def backup_database() -> bool:
    """
    Backup database using pg_dump if available.
    Returns True if backup successful, False otherwise.
    """
    if not config.database_url:
        logger.error("No database URL configured")
        return False
    
    try:
        # Extract database name from URL
        # Format: postgresql://user:pass@host:port/dbname
        db_name = config.database_url.split('/')[-1].split('?')[0]
        
        backup_file = f"backup_{db_name}.sql"
        
        logger.info(f"Creating backup: {backup_file}")
        
        result = subprocess.run(
            ['pg_dump', '-d', config.database_url, '-f', backup_file],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logger.info(f"Backup created successfully: {backup_file}")
            return True
        else:
            logger.error(f"Backup failed: {result.stderr}")
            return False
            
    except FileNotFoundError:
        logger.warning("pg_dump not found - skipping backup")
        return False
    except Exception as e:
        logger.error(f"Backup error: {e}")
        return False


def reset_etl_tables():
    """Drop ETL metadata tables"""
    if config.skip_db:
        logger.info("[DRY RUN] Would reset ETL tables")
        return
    
    logger.info("Resetting ETL tables...")
    
    with db.get_connection() as conn:
        # Drop metadata tables
        conn.execute("DROP TABLE IF EXISTS etl_imports CASCADE")
        conn.execute("DROP TABLE IF EXISTS etl_schema_changes CASCADE")
        conn.commit()
    
    logger.info("ETL tables reset")


def drop_data_tables():
    """
    Drop all data tables (non-ETL tables).
    
    WARNING: This is destructive!
    """
    if config.skip_db:
        logger.info("[DRY RUN] Would drop all data tables")
        return
    
    from sqlalchemy import inspect
    
    logger.info("Dropping all data tables...")
    
    inspector = inspect(db.engine)
    tables = inspector.get_table_names()
    
    # Filter out ETL metadata tables
    data_tables = [t for t in tables if t not in ('etl_imports', 'etl_schema_changes')]
    
    with db.get_connection() as conn:
        for table in data_tables:
            logger.info(f"Dropping table: {table}")
            conn.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
        conn.commit()
    
    logger.info(f"Dropped {len(data_tables)} data tables")


@click.command()
@click.option('--backup', is_flag=True, help='Create database backup before reset')
@click.option('--run', is_flag=True, help='Run ETL after reset')
@click.option('--data-root', default='data', help='Data root directory')
@click.option('--yes', is_flag=True, help='Skip confirmation prompt')
def main(backup, run_etl, data_root, yes):
    """
    Reset database and optionally run ETL.
    
    WARNING: This will DROP all tables!
    """
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Confirmation
    if not yes:
        click.echo("=" * 60)
        click.echo("WARNING: This will DROP ALL TABLES!")
        click.echo("=" * 60)
        if not click.confirm('Are you absolutely sure?'):
            click.echo("Aborted.")
            return
    
    # Backup
    if backup:
        click.echo("Creating backup...")
        if backup_database():
            click.echo("✓ Backup created")
        else:
            click.echo("✗ Backup failed")
            if not yes and not click.confirm('Continue without backup?'):
                click.echo("Aborted.")
                return
    
    # Reset
    click.echo("Resetting database...")
    try:
        drop_data_tables()
        reset_etl_tables()
        click.echo("✓ Database reset complete")
    except Exception as e:
        click.echo(f"✗ Reset failed: {e}", err=True)
        sys.exit(1)
    
    # Run ETL
    if run_etl:
        click.echo("Running ETL...")
        try:
            run(data_root=data_root)
            click.echo("✓ ETL completed")
        except Exception as e:
            click.echo(f"✗ ETL failed: {e}", err=True)
            sys.exit(1)
    
    click.echo("=" * 60)
    click.echo("All operations completed successfully!")
    click.echo("=" * 60)


if __name__ == '__main__':
    main()
