"""
Command-line interface for PostgreSQL Migration Tool
FIXED: Better schema detection and error handling
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
from report_generator import MigrationReportGenerator, list_available_schemas
from schema_comparator import SchemaComparator
from metrics_tracker import get_tracker

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
@click.option('--src-port', default='6000', help='Source database port')
@click.option('--src-db', required=True, help='Source database name')
@click.option('--src-user', required=True, help='Source database user')
@click.option('--src-password', required=True, help='Source database password')
@click.option('--dst-host', required=True, help='Destination database host')
@click.option('--dst-port', default='6000', help='Destination database port')
@click.option('--dst-db', required=True, help='Destination database name')
@click.option('--dst-user', required=True, help='Destination database user')
@click.option('--dst-password', required=True, help='Destination database password')
@click.option('--exclude-auto/--include-auto', default=False, help='Exclude auto-generated columns')
@click.option('--schema-naming', type=click.Choice(['descriptive', 'abbreviated', 'timestamp', 'custom']), 
              default='descriptive', help='Schema naming mode')
@click.option('--custom-schema', help='Custom schema name (used with --schema-naming=custom)')
def full(src_host, src_port, src_db, src_user, src_password,
         dst_host, dst_port, dst_db, dst_user, dst_password,
         exclude_auto, schema_naming, custom_schema):
    """Perform full database migration (drops and recreates all tables)"""
    
    src_cfg = DBConfig(src_host, src_port, src_db, src_user, src_password)
    dst_cfg = DBConfig(dst_host, dst_port, dst_db, dst_user, dst_password)
    
    # Test connections
    click.echo("Testing source connection...")
    success, msg = test_connection(src_cfg)
    if not success:
        click.echo(f"✗ Source connection failed: {msg}", err=True)
        return
    click.echo("✓ Source connection successful")
    
    click.echo("Testing destination connection...")
    success, msg = test_connection(dst_cfg)
    if not success:
        click.echo(f"✗ Destination connection failed: {msg}", err=True)
        return
    click.echo("✓ Destination connection successful")
    
    # Configure migration
    config = MigrationConfig()
    config.schema_naming_mode = schema_naming
    if custom_schema:
        config.custom_schema_name = custom_schema
    
    target_schema = config.get_schema_name("full", src_db)
    
    # Confirm
    if not click.confirm(f"\n⚠️  This will DROP and RECREATE all tables in schema '{target_schema}'. Continue?"):
        click.echo("Aborted.")
        return
    
    # Perform migration
    src_engine = create_engine_safe(src_cfg)
    dst_engine = create_engine_safe(dst_cfg)
    
    def progress(msg):
        click.echo(msg)
    
    try:
        full_migration(
            src_engine, dst_engine,
            exclude_auto_generated=exclude_auto,
            config=config,
            progress_callback=progress
        )
        
        # Show metrics
        tracker = get_tracker()
        click.echo("\n" + "="*80)
        click.echo("MIGRATION METRICS:")
        click.echo("="*80)
        snapshot = tracker.get_snapshot()
        if snapshot:
            click.echo(f"Duration: {snapshot['duration']}")
            click.echo(f"Tables: {snapshot['progress']['tables']}")
            click.echo(f"Rows: {snapshot['progress']['rows']}")
            click.echo(f"Performance: {snapshot['performance']['rows_per_second']} rows/sec")
            click.echo(f"Target Schema: {snapshot['databases']['schema']}")
        
        click.echo("\n✅ Migration completed successfully!")
        click.echo(f"\n💡 To generate a report, use:")
        click.echo(f"   python main.py report --host {dst_host} --port {dst_port} --db {dst_db} --user {dst_user} --password *** --schema {target_schema}")
        
    except Exception as e:
        click.echo(f"\n✗ Migration failed: {str(e)}", err=True)
        raise
    finally:
        src_engine.dispose()
        dst_engine.dispose()


@cli.command()
@click.option('--src-host', required=True, help='Source database host')
@click.option('--src-port', default='6000', help='Source database port')
@click.option('--src-db', required=True, help='Source database name')
@click.option('--src-user', required=True, help='Source database user')
@click.option('--src-password', required=True, help='Source database password')
@click.option('--dst-host', required=True, help='Destination database host')
@click.option('--dst-port', default='6000', help='Destination database port')
@click.option('--dst-db', required=True, help='Destination database name')
@click.option('--dst-user', required=True, help='Destination database user')
@click.option('--dst-password', required=True, help='Destination database password')
@click.option('--schema-naming', type=click.Choice(['descriptive', 'abbreviated', 'timestamp', 'custom']), 
              default='descriptive', help='Schema naming mode')
@click.option('--custom-schema', help='Custom schema name')
def incremental(src_host, src_port, src_db, src_user, src_password,
                dst_host, dst_port, dst_db, dst_user, dst_password,
                schema_naming, custom_schema):
    """Perform incremental sync (only copies new/changed data)"""
    
    src_cfg = DBConfig(src_host, src_port, src_db, src_user, src_password)
    dst_cfg = DBConfig(dst_host, dst_port, dst_db, dst_user, dst_password)
    
    # Test connections
    click.echo("Testing connections...")
    for name, cfg in [("Source", src_cfg), ("Destination", dst_cfg)]:
        success, msg = test_connection(cfg)
        if not success:
            click.echo(f"✗ {name} connection failed: {msg}", err=True)
            return
        click.echo(f"✓ {name} connection successful")
    
    # Configure migration
    config = MigrationConfig()
    config.schema_naming_mode = schema_naming
    if custom_schema:
        config.custom_schema_name = custom_schema
    
    target_schema = config.get_schema_name("incremental", src_db)
    click.echo(f"\nTarget schema: {target_schema}")
    
    # Perform migration
    src_engine = create_engine_safe(src_cfg)
    dst_engine = create_engine_safe(dst_cfg)
    
    def progress(msg):
        click.echo(msg)
    
    try:
        incremental_sync(
            src_engine, dst_engine,
            config=config,
            progress_callback=progress
        )
        
        # Show metrics
        tracker = get_tracker()
        snapshot = tracker.get_snapshot()
        if snapshot:
            click.echo("\n" + "="*80)
            click.echo("MIGRATION METRICS:")
            click.echo("="*80)
            click.echo(f"Duration: {snapshot['duration']}")
            click.echo(f"Performance: {snapshot['performance']['rows_per_second']} rows/sec")
            click.echo(f"Target Schema: {snapshot['databases']['schema']}")
        
        click.echo("\n✅ Incremental sync completed successfully!")
        click.echo(f"\n💡 To generate a report, use:")
        click.echo(f"   python main.py report --host {dst_host} --port {dst_port} --db {dst_db} --user {dst_user} --password *** --schema {target_schema}")
        
    except Exception as e:
        click.echo(f"\n✗ Sync failed: {str(e)}", err=True)
        raise
    finally:
        src_engine.dispose()
        dst_engine.dispose()


@cli.command()
@click.option('--src-host', required=True, help='Source database host')
@click.option('--src-port', default='6000', help='Source database port')
@click.option('--src-db', required=True, help='Source database name')
@click.option('--src-user', required=True, help='Source database user')
@click.option('--src-password', required=True, help='Source database password')
@click.option('--dst-host', required=True, help='Destination database host')
@click.option('--dst-port', default='6000', help='Destination database port')
@click.option('--dst-db', required=True, help='Destination database name')
@click.option('--dst-user', required=True, help='Destination database user')
@click.option('--dst-password', required=True, help='Destination database password')
@click.option('--table', '-t', multiple=True, required=True, help='Table name(s) to migrate (can specify multiple)')
@click.option('--mode', type=click.Choice(['recreate', 'delta']), default='delta', help='Migration mode')
@click.option('--exclude-auto/--include-auto', default=True, help='Exclude auto-generated columns')
@click.option('--schema-naming', type=click.Choice(['descriptive', 'abbreviated', 'timestamp', 'custom']), 
              default='descriptive', help='Schema naming mode')
@click.option('--custom-schema', help='Custom schema name')
def table(src_host, src_port, src_db, src_user, src_password,
          dst_host, dst_port, dst_db, dst_user, dst_password,
          table, mode, exclude_auto, schema_naming, custom_schema):
    """Migrate one or more tables"""
    
    src_cfg = DBConfig(src_host, src_port, src_db, src_user, src_password)
    dst_cfg = DBConfig(dst_host, dst_port, dst_db, dst_user, dst_password)
    
    # Test connections
    click.echo("Testing connections...")
    for name, cfg in [("Source", src_cfg), ("Destination", dst_cfg)]:
        success, msg = test_connection(cfg)
        if not success:
            click.echo(f"✗ {name} connection failed: {msg}", err=True)
            return
        click.echo(f"✓ {name} connection successful")
    
    # Configure migration
    config = MigrationConfig()
    config.schema_naming_mode = schema_naming
    if custom_schema:
        config.custom_schema_name = custom_schema
    
    migration_type = "table_recreate" if mode == "recreate" else "table_delta"
    target_schema = config.get_schema_name(migration_type, src_db)
    
    click.echo(f"\nTarget schema: {target_schema}")
    click.echo(f"Tables to migrate: {', '.join(table)}")
    
    src_engine = create_engine_safe(src_cfg)
    dst_engine = create_engine_safe(dst_cfg)
    
    def progress(msg):
        click.echo(msg)
    
    try:
        if mode == 'recreate':
            if not click.confirm(f"\n⚠️  This will DROP and RECREATE {len(table)} table(s). Continue?"):
                click.echo("Aborted.")
                return
            
            for tbl in table:
                click.echo(f"\n{'='*60}")
                click.echo(f"Processing table: {tbl}")
                click.echo(f"{'='*60}")
                table_copy_delete_and_recreate(
                    src_engine, dst_engine, tbl,
                    target_schema=target_schema,
                    exclude_auto_generated=exclude_auto,
                    config=config,
                    progress_callback=progress
                )
        else:
            for tbl in table:
                click.echo(f"\n{'='*60}")
                click.echo(f"Processing table: {tbl}")
                click.echo(f"{'='*60}")
                table_copy_delta_only(
                    src_engine, dst_engine, tbl,
                    target_schema=target_schema,
                    exclude_auto_generated=exclude_auto,
                    config=config,
                    progress_callback=progress
                )
        
        click.echo(f"\n✅ All {len(table)} table(s) migrated successfully to {target_schema}!")
        click.echo(f"\n💡 To generate a report, use:")
        click.echo(f"   python main.py report --host {dst_host} --port {dst_port} --db {dst_db} --user {dst_user} --password *** --schema {target_schema}")
        
    except Exception as e:
        click.echo(f"\n✗ Migration failed: {str(e)}", err=True)
        raise
    finally:
        src_engine.dispose()
        dst_engine.dispose()


@cli.command()
@click.option('--src-host', required=True, help='Source database host')
@click.option('--src-port', default='6000', help='Source database port')
@click.option('--src-db', required=True, help='Source database name')
@click.option('--src-user', required=True, help='Source database user')
@click.option('--src-password', required=True, help='Source database password')
@click.option('--dst-host', required=True, help='Destination database host')
@click.option('--dst-port', default='6000', help='Destination database port')
@click.option('--dst-db', required=True, help='Destination database name')
@click.option('--dst-user', required=True, help='Destination database user')
@click.option('--dst-password', required=True, help='Destination database password')
@click.option('--src-schema', default='public', help='Source schema name')
@click.option('--dst-schema', default='migrated', help='Destination schema name')
@click.option('--output', '-o', help='Output file path for report')
@click.option('--include-row-counts/--no-row-counts', default=True, help='Include row count comparison')
def compare(src_host, src_port, src_db, src_user, src_password,
            dst_host, dst_port, dst_db, dst_user, dst_password,
            src_schema, dst_schema, output, include_row_counts):
    """Compare schemas between source and destination (Feature #3)"""
    
    src_cfg = DBConfig(src_host, src_port, src_db, src_user, src_password)
    dst_cfg = DBConfig(dst_host, dst_port, dst_db, dst_user, dst_password)
    
    # Test connections
    click.echo("Testing connections...")
    for name, cfg in [("Source", src_cfg), ("Destination", dst_cfg)]:
        success, msg = test_connection(cfg)
        if not success:
            click.echo(f"✗ {name} connection failed: {msg}", err=True)
            return
        click.echo(f"✓ {name} connection successful")
    
    click.echo(f"\nComparing schemas:")
    click.echo(f"  Source: {src_schema} in {src_db}")
    click.echo(f"  Destination: {dst_schema} in {dst_db}")
    click.echo(f"  Include row counts: {include_row_counts}")
    
    src_engine = create_engine_safe(src_cfg)
    dst_engine = create_engine_safe(dst_cfg)
    
    try:
        comparator = SchemaComparator(src_engine, dst_engine, src_schema, dst_schema)
        result = comparator.compare_schemas(include_row_counts=include_row_counts)
        
        # Generate report
        report = comparator.generate_comparison_report(result)
        
        # Display to console
        click.echo("\n" + report)
        
        # Save to file if requested
        if output:
            with open(output, 'w') as f:
                f.write(report)
            click.echo(f"\n✓ Report saved to: {output}")
        
        # Show summary
        if result.has_differences:
            click.echo("\n⚠️  DIFFERENCES DETECTED")
            return 1
        else:
            click.echo("\n✅ SCHEMAS ARE IDENTICAL")
            return 0
            
    finally:
        src_engine.dispose()
        dst_engine.dispose()


@cli.command()
@click.option('--host', required=True, help='Database host')
@click.option('--port', default='6000', help='Database port')
@click.option('--db', required=True, help='Database name')
@click.option('--user', required=True, help='Database user')
@click.option('--password', required=True, help='Database password')
@click.option('--schema', help='Schema to generate report for (if not provided, will auto-detect)')
@click.option('--format', type=click.Choice(['excel', 'pdf', 'json']), default='excel', help='Report format')
@click.option('--output', '-o', help='Output file path')
@click.option('--list-schemas', is_flag=True, help='List available migration schemas')
def report(host, port, db, user, password, schema, format, output, list_schemas):
    """Generate migration report"""
    
    cfg = DBConfig(host, port, db, user, password)
    
    # Test connection
    click.echo("Testing connection...")
    success, msg = test_connection(cfg)
    if not success:
        click.echo(f"✗ Connection failed: {msg}", err=True)
        return
    click.echo("✓ Connection successful")
    
    engine = create_engine_safe(cfg)
    
    # List schemas if requested
    if list_schemas:
        available = list_available_schemas(engine)
        if available:
            click.echo("\nAvailable migration schemas:")
            for s in available:
                click.echo(f"  • {s}")
            click.echo(f"\nTo generate a report, use:")
            click.echo(f"  python main.py report --host {host} --db {db} --user {user} --password *** --schema <schema_name>")
        else:
            click.echo("\n⚠️  No migration schemas found.")
            click.echo("Have you run any migrations yet?")
        engine.dispose()
        return
    
    # Auto-detect schema if not provided
    if not schema:
        available = list_available_schemas(engine)
        if len(available) == 1:
            schema = available[0]
            click.echo(f"\n💡 Auto-detected schema: {schema}")
        elif len(available) > 1:
            click.echo(f"\n⚠️  Multiple migration schemas found:")
            for s in available:
                click.echo(f"  • {s}")
            click.echo(f"\nPlease specify which schema to use with --schema")
            click.echo(f"Example: --schema {available[0]}")
            engine.dispose()
            return
        else:
            click.echo(f"\n✗ No migration schemas found in database '{db}'")
            click.echo("Have you run any migrations yet?")
            click.echo("Expected schema names like: migrated_full_copy, migrated_incremental_sync")
            click.echo("(Looking for schemas matching pattern: migrated_*)")
            engine.dispose()
            return
    
    try:
        # Pass the detected or specified schema
        reporter = MigrationReportGenerator(engine, schema)
        
        if not output:
            output = f"migration_report_{db}_{schema}.{format if format != 'json' else 'txt'}"
        
        click.echo(f"\nGenerating {format.upper()} report for schema '{schema}'...")
        
        if format == 'excel':
            reporter.generate_excel_report(output)
        elif format == 'pdf':
            reporter.generate_pdf_report(output)
        elif format == 'json':
            reporter.save_summary_to_file(output)
        
        click.echo(f"\n✅ Report generated: {output}")
        
    except ValueError as e:
        click.echo(f"\n✗ {str(e)}", err=True)
    except Exception as e:
        click.echo(f"\n✗ Report generation failed: {str(e)}", err=True)
        raise
    finally:
        engine.dispose()


@cli.command()
def ui():
    """Launch interactive terminal UI"""
    from frontend import run_ui
    run_ui()


@cli.command()
def metrics():
    """Show current migration metrics (Feature #17)"""
    tracker = get_tracker()
    
    current = tracker.get_current_metrics()
    
    if current:
        click.echo(tracker.get_formatted_status())
    else:
        click.echo("No active migration")
        
        # Show history
        history = tracker.get_history_summary()
        if history:
            click.echo("\nRECENT MIGRATIONS:")
            click.echo("="*80)
            for m in history[-5:]:  # Last 5
                click.echo(f"{m['migration_id']}: {m['status']} | {m['duration']} | {m['rows']} rows")


if __name__ == '__main__':
    cli()