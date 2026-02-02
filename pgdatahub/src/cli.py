"""Command-line interface for PGDataHub ETL"""
import click
import logging
from pathlib import Path

from src.etl import run
from src.pause import resume_from_pause
from src.revert import revert_by_file, revert_by_hash, revert_schema_changes

logger = logging.getLogger(__name__)


@click.group()
def cli():
    """PGDataHub - Excel to PostgreSQL ETL System"""
    pass


@cli.command()
@click.option(
    '--data-root',
    default='data',
    help='Root directory containing Excel files',
    type=click.Path(exists=True, file_okay=False, dir_okay=True)
)
def etl(data_root):
    """Run the ETL process"""
    click.echo("Starting ETL process...")
    try:
        run(data_root=data_root)
        click.echo("ETL completed successfully!")
    except Exception as e:
        click.echo(f"ETL failed: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.option(
    '--data-root',
    default='data',
    help='Root directory',
    type=click.Path(file_okay=False, dir_okay=True)
)
def resume(data_root):
    """Resume ETL from pause file"""
    click.echo("Resuming ETL from pause...")
    try:
        resume_from_pause(Path(data_root))
        click.echo("Resume completed successfully!")
    except Exception as e:
        click.echo(f"Resume failed: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.option(
    '--source-file',
    help='Source file path to revert',
    type=str
)
@click.option(
    '--file-hash',
    help='SHA-256 hash of file to revert',
    type=str
)
@click.option(
    '--table',
    help='Table name to revert from',
    required=True,
    type=str
)
@click.confirmation_option(
    prompt='Are you sure you want to revert this import?'
)
def revert(source_file, file_hash, table):
    """Revert a file import"""
    if not source_file and not file_hash:
        click.echo("Error: Must specify either --source-file or --file-hash", err=True)
        raise click.Abort()
    
    try:
        if source_file:
            rows_deleted = revert_by_file(table, source_file)
            click.echo(f"Reverted {rows_deleted} rows from {table}")
        else:
            rows_deleted = revert_by_hash(table, file_hash)
            click.echo(f"Reverted {rows_deleted} rows from {table}")
    except Exception as e:
        click.echo(f"Revert failed: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.option(
    '--table',
    required=True,
    help='Table name',
    type=str
)
@click.option(
    '--source-file',
    required=True,
    help='Source file that caused schema changes',
    type=str
)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Show what would be reverted without executing'
)
def revert_schema(table, source_file, dry_run):
    """Revert schema changes from a specific file"""
    try:
        result = revert_schema_changes(table, source_file, dry_run=dry_run)
        
        if dry_run:
            click.echo("Schema changes that would be reverted:")
            for change in result:
                click.echo(f"  - {change}")
        else:
            click.echo(f"Reverted {len(result)} schema changes")
    except Exception as e:
        click.echo(f"Schema revert failed: {e}", err=True)
        raise click.Abort()


@cli.command()
def status():
    """Show ETL status and statistics"""
    from src.database import db
    from sqlalchemy import text
    
    try:
        with db.get_connection() as conn:
            # Count imports
            result = conn.execute(text("SELECT COUNT(*) FROM etl_imports"))
            import_count = result.fetchone()[0]
            
            # Count schema changes
            result = conn.execute(text("SELECT COUNT(*) FROM etl_schema_changes"))
            schema_count = result.fetchone()[0]
            
            # Recent imports
            result = conn.execute(text("""
                SELECT table_name, COUNT(*) as file_count, SUM(row_count) as total_rows
                FROM etl_imports
                GROUP BY table_name
                ORDER BY MAX(imported_at) DESC
                LIMIT 10
            """))
            recent = result.fetchall()
        
        click.echo("=" * 60)
        click.echo("ETL STATUS")
        click.echo("=" * 60)
        click.echo(f"Total imports: {import_count}")
        click.echo(f"Total schema changes: {schema_count}")
        click.echo("\nRecent tables:")
        
        for row in recent:
            click.echo(f"  {row[0]}: {row[1]} files, {row[2]} rows")
        
        click.echo("=" * 60)
        
    except Exception as e:
        click.echo(f"Failed to get status: {e}", err=True)
        raise click.Abort()


if __name__ == '__main__':
    cli()
