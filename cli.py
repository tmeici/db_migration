"""
Command-line interface for PostgreSQL Migration Tool
"""
import click
import logging
from pathlib import Path

from config import DBConfig, MigrationConfig
from database import create_engine_safe, test_connection
from migrations import (
    full_migration,
    incremental_sync,
    table_copy_delete_and_recreate,
    table_copy_delta_only,
)
from report_generator import MigrationReportGenerator

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool):
    """Configure logging"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def cli(verbose):
    """PostgreSQL Migration Tool - Production-Ready Database Migration"""
    setup_logging(verbose)


@cli.command()
@click.option('--src-host', required=True, help='Source database host')
@click.option('--src-port', default='5432', help='Source database port')
@click.option('--src-db', required=True, help='Source database name')
@click.option('--src-user', required=True, help='Source database user')
@click.option('--src-password', required=True, help='Source database password')
@click.option('--dst-host', required=True, help='Destination database host')
@click.option('--dst-port', default='5432', help='Destination database port')
@click.option('--dst-db', required=True, help='Destination database name')
@click.option('--dst-user', required=True, help='Destination database user')
@click.option('--dst-password', required=True, help='Destination database password')
@click.option('--exclude-auto/--include-auto', default=False, help='Exclude auto-generated columns')
def full(src_host, src_port, src_db, src_user, src_password,
         dst_host, dst_port, dst_db, dst_user, dst_password,
         exclude_auto):
    """Perform full database migration (drops and recreates all tables)"""
    
    src_cfg = DBConfig(src_host, src_port, src_db, src_user, src_password)
    dst_cfg = DBConfig(dst_host, dst_port, dst_db, dst_user, dst_password)
    
    # Test connections
    click.echo("Testing source connection...")
    success, msg = test_connection(src_cfg)
    if not success:
        click.echo(f"❌ Source connection failed: {msg}", err=True)
        return
    click.echo("✅ Source connection successful")
    
    click.echo("Testing destination connection...")
    success, msg = test_connection(dst_cfg)
    if not success:
        click.echo(f"❌ Destination connection failed: {msg}", err=True)
        return
    click.echo("✅ Destination connection successful")
    
    # Confirm
    if not click.confirm("\n⚠️  This will DROP and RECREATE all tables in the destination. Continue?"):
        click.echo("Aborted.")
        return
    
    # Perform migration
    src_engine = create_engine_safe(src_cfg)
    dst_engine = create_engine_safe(dst_cfg)
    config = MigrationConfig()
    
    def progress(msg):
        click.echo(msg)
    
    try:
        full_migration(
            src_engine, dst_engine,
            exclude_auto_generated=exclude_auto,
            config=config,
            progress_callback=progress
        )
        click.echo("\n✅ Migration completed successfully!")
    except Exception as e:
        click.echo(f"\n❌ Migration failed: {str(e)}", err=True)
        raise
    finally:
        src_engine.dispose()
        dst_engine.dispose()


@cli.command()
@click.option('--src-host', required=True, help='Source database host')
@click.option('--src-port', default='5432', help='Source database port')
@click.option('--src-db', required=True, help='Source database name')
@click.option('--src-user', required=True, help='Source database user')
@click.option('--src-password', required=True, help='Source database password')
@click.option('--dst-host', required=True, help='Destination database host')
@click.option('--dst-port', default='5432', help='Destination database port')
@click.option('--dst-db', required=True, help='Destination database name')
@click.option('--dst-user', required=True, help='Destination database user')
@click.option('--dst-password', required=True, help='Destination database password')
def incremental(src_host, src_port, src_db, src_user, src_password,
                dst_host, dst_port, dst_db, dst_user, dst_password):
    """Perform incremental sync (only copies new/changed data)"""
    
    src_cfg = DBConfig(src_host, src_port, src_db, src_user, src_password)
    dst_cfg = DBConfig(dst_host, dst_port, dst_db, dst_user, dst_password)
    
    # Test connections
    click.echo("Testing connections...")
    for name, cfg in [("Source", src_cfg), ("Destination", dst_cfg)]:
        success, msg = test_connection(cfg)
        if not success:
            click.echo(f"❌ {name} connection failed: {msg}", err=True)
            return
        click.echo(f"✅ {name} connection successful")
    
    # Perform migration
    src_engine = create_engine_safe(src_cfg)
    dst_engine = create_engine_safe(dst_cfg)
    config = MigrationConfig()
    
    def progress(msg):
        click.echo(msg)
    
    try:
        incremental_sync(
            src_engine, dst_engine,
            config=config,
            progress_callback=progress
        )
        click.echo("\n✅ Incremental sync completed successfully!")
    except Exception as e:
        click.echo(f"\n❌ Sync failed: {str(e)}", err=True)
        raise
    finally:
        src_engine.dispose()
        dst_engine.dispose()


@cli.command()
@click.option('--src-host', required=True, help='Source database host')
@click.option('--src-port', default='5432', help='Source database port')
@click.option('--src-db', required=True, help='Source database name')
@click.option('--src-user', required=True, help='Source database user')
@click.option('--src-password', required=True, help='Source database password')
@click.option('--dst-host', required=True, help='Destination database host')
@click.option('--dst-port', default='5432', help='Destination database port')
@click.option('--dst-db', required=True, help='Destination database name')
@click.option('--dst-user', required=True, help='Destination database user')
@click.option('--dst-password', required=True, help='Destination database password')
@click.option('--table', '-t', required=True, help='Table name to migrate')
@click.option('--mode', type=click.Choice(['recreate', 'delta']), default='delta', help='Migration mode')
@click.option('--exclude-auto/--include-auto', default=True, help='Exclude auto-generated columns')
def table(src_host, src_port, src_db, src_user, src_password,
          dst_host, dst_port, dst_db, dst_user, dst_password,
          table, mode, exclude_auto):
    """Migrate a single table"""
    
    src_cfg = DBConfig(src_host, src_port, src_db, src_user, src_password)
    dst_cfg = DBConfig(dst_host, dst_port, dst_db, dst_user, dst_password)
    
    # Test connections
    click.echo("Testing connections...")
    for name, cfg in [("Source", src_cfg), ("Destination", dst_cfg)]:
        success, msg = test_connection(cfg)
        if not success:
            click.echo(f"❌ {name} connection failed: {msg}", err=True)
            return
        click.echo(f"✅ {name} connection successful")
    
    src_engine = create_engine_safe(src_cfg)
    dst_engine = create_engine_safe(dst_cfg)
    config = MigrationConfig()
    
    def progress(msg):
        click.echo(msg)
    
    try:
        if mode == 'recreate':
            if not click.confirm(f"\n⚠️  This will DROP and RECREATE table '{table}'. Continue?"):
                click.echo("Aborted.")
                return
            
            table_copy_delete_and_recreate(
                src_engine, dst_engine, table,
                exclude_auto_generated=exclude_auto,
                config=config,
                progress_callback=progress
            )
        else:
            table_copy_delta_only(
                src_engine, dst_engine, table,
                exclude_auto_generated=exclude_auto,
                config=config,
                progress_callback=progress
            )
        
        click.echo(f"\n✅ Table '{table}' migrated successfully!")
    except Exception as e:
        click.echo(f"\n❌ Migration failed: {str(e)}", err=True)
        raise
    finally:
        src_engine.dispose()
        dst_engine.dispose()


@cli.command()
@click.option('--host', required=True, help='Database host')
@click.option('--port', default='5432', help='Database port')
@click.option('--db', required=True, help='Database name')
@click.option('--user', required=True, help='Database user')
@click.option('--password', required=True, help='Database password')
@click.option('--format', type=click.Choice(['excel', 'pdf', 'json']), default='excel', help='Report format')
@click.option('--output', '-o', help='Output file path')
def report(host, port, db, user, password, format, output):
    """Generate migration report"""
    
    cfg = DBConfig(host, port, db, user, password)
    
    # Test connection
    click.echo("Testing connection...")
    success, msg = test_connection(cfg)
    if not success:
        click.echo(f"❌ Connection failed: {msg}", err=True)
        return
    click.echo("✅ Connection successful")
    
    engine = create_engine_safe(cfg)
    reporter = MigrationReportGenerator(engine)
    
    try:
        if not output:
            output = f"migration_report_{db}.{format if format != 'json' else 'txt'}"
        
        if format == 'excel':
            reporter.generate_excel_report(output)
        elif format == 'pdf':
            reporter.generate_pdf_report(output)
        elif format == 'json':
            import json
            summary = reporter.generate_summary_report()
            with open(output, 'w') as f:
                json.dump(summary, f, indent=2)
        
        click.echo(f"\n✅ Report generated: {output}")
    except Exception as e:
        click.echo(f"\n❌ Report generation failed: {str(e)}", err=True)
        raise
    finally:
        engine.dispose()


@cli.command()
def ui():
    """Launch interactive terminal UI"""
    from frontend import run_ui
    run_ui()


if __name__ == '__main__':
    cli()
