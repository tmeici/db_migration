"""
Main migration operations with metrics tracking and dynamic schema naming
"""
import logging
from typing import Optional, Callable
from sqlalchemy.engine import Engine

from database import fetch_tables, fetch_columns, fetch_primary_key, get_row_count
from schema_manager import (
    create_table_schema_if_not_exists,
    recreate_table_schema,
    is_auto_generated_column,
    create_indexes_and_constraints
)
from data_operations import (
    fetch_all_rows,
    fetch_existing_pks,
    prepare_row,
    insert_rows,
    row_fingerprint
)
from migration_tracker import MigrationTracker
from metrics_tracker import get_tracker
from config import MigrationConfig

logger = logging.getLogger(__name__)


def full_migration(
    src: Engine, 
    dst: Engine,
    src_schema: str = "public",
    target_schema: Optional[str] = None,
    exclude_auto_generated: bool = False,
    config: Optional[MigrationConfig] = None,
    progress_callback: Optional[Callable] = None
):
    """
    Mode 1: Full migration (clean slate)
    Drops and recreates all tables with fresh data
    """
    config = config or MigrationConfig()
    
    # Generate dynamic schema name
    if target_schema is None:
        target_schema = config.get_schema_name("full", src.url.database)
    
    tracker = MigrationTracker(dst, target_schema)
    metrics = get_tracker()
    
    if progress_callback:
        progress_callback(f"Starting full migration to schema: {target_schema}")
    
    tables = fetch_tables(src, src_schema)
    total_tables = len(tables)
    
    # Start metrics tracking
    migration_id = f"full_{src.url.database}_{target_schema}"
    metrics.start_migration(
        migration_id=migration_id,
        migration_type="full_migration",
        total_tables=total_tables,
        source_db=src.url.database,
        dest_db=dst.url.database,
        target_schema=target_schema
    )
    
    total_rows = 0
    failed_tables = []
    
    for idx, table in enumerate(tables, 1):
        migration_id_db = None
        try:
            if progress_callback:
                progress_callback(f"[{idx}/{total_tables}] Processing table: {table}")
            
            # Get row count for metrics
            row_count = get_row_count(src, table, src_schema)
            table_metrics = metrics.start_table(table, row_count)
            
            # Track migration in DB
            migration_id_db = tracker.start_migration(
                table_name=table,
                source_db=src.url.database,
                source_host=src.url.host,
                migration_type="full_migration",
                target_schema=target_schema,
                metadata={"exclude_auto_generated": exclude_auto_generated}
            )
            
            # Recreate table schema
            columns = recreate_table_schema(
                src, dst, table, src_schema, target_schema,
                exclude_auto_generated, progress_callback
            )
            
            # Get column metadata
            cols_meta = [
                c for c in fetch_columns(src, src_schema, table) 
                if c["column_name"] in columns
            ]
            
            # Fetch all rows
            src_rows = fetch_all_rows(src, table, columns, src_schema)
            
            if progress_callback:
                progress_callback(f"  Copying {len(src_rows)} rows...")
            
            # Prepare and insert
            prepared_rows = [prepare_row(r, cols_meta) for r in src_rows]
            inserted = insert_rows(
                dst, table, prepared_rows, 
                target_schema, config.batch_size, progress_callback
            )
            total_rows += inserted
            
            # Update metrics
            metrics.update_table_progress(table, inserted)
            metrics.complete_table(table, success=True)
            
            # Create indexes if enabled
            if config.enable_indexes:
                create_indexes_and_constraints(
                    src, dst, table, src_schema, target_schema, progress_callback
                )
            
            # Mark as completed in DB
            tracker.complete_migration(migration_id_db, inserted, success=True)
            
            if progress_callback:
                progress_callback(f"  ✓ {table}: {inserted} rows copied")
        
        except Exception as e:
            failed_tables.append((table, str(e)))
            metrics.complete_table(table, success=False, error_message=str(e))
            if migration_id_db:
                tracker.complete_migration(migration_id_db, 0, success=False, error_message=str(e))
            if progress_callback:
                progress_callback(f"  ✗ ERROR in {table}: {str(e)}")
            logger.error(f"Error migrating table {table}: {str(e)}")
    
    # Complete migration metrics
    metrics.complete_migration(success=len(failed_tables) == 0)
    
    if progress_callback:
        progress_callback(f"\n{'='*60}")
        progress_callback(f"✅ Full migration completed to schema: {target_schema}")
        progress_callback(f"{'='*60}")
        progress_callback(f"Total tables: {total_tables}")
        progress_callback(f"Successfully migrated: {total_tables - len(failed_tables)}")
        progress_callback(f"Total rows copied: {total_rows}")
        if failed_tables:
            progress_callback(f"Failed tables: {len(failed_tables)}")
            for table, error in failed_tables:
                progress_callback(f"  - {table}: {error}")
        progress_callback(f"{'='*60}")


def incremental_sync(
    src: Engine,
    dst: Engine,
    src_schema: str = "public",
    target_schema: Optional[str] = None,
    config: Optional[MigrationConfig] = None,
    progress_callback: Optional[Callable] = None
):
    """
    Incremental sync with metrics tracking
    """
    config = config or MigrationConfig()
    
    # Generate dynamic schema name
    if target_schema is None:
        target_schema = config.get_schema_name("incremental", src.url.database)
    
    tracker = MigrationTracker(dst, target_schema)
    metrics = get_tracker()
    
    if progress_callback:
        progress_callback(f"Starting incremental sync to schema: {target_schema}")
    
    tables = fetch_tables(src, src_schema)
    
    # Start metrics tracking
    migration_id = f"incremental_{src.url.database}_{target_schema}"
    metrics.start_migration(
        migration_id=migration_id,
        migration_type="incremental_sync",
        total_tables=len(tables),
        source_db=src.url.database,
        dest_db=dst.url.database,
        target_schema=target_schema
    )
    
    total_inserted = 0
    
    for idx, table in enumerate(tables, 1):
        migration_id_db = None
        try:
            if progress_callback:
                progress_callback(f"[{idx}/{len(tables)}] {table}")
            
            row_count = get_row_count(src, table, src_schema)
            table_metrics = metrics.start_table(table, row_count)
            
            # Start tracking in DB
            migration_id_db = tracker.start_migration(
                table_name=table,
                source_db=src.url.database,
                source_host=src.url.host,
                migration_type="incremental_sync",
                target_schema=target_schema
            )
            
            # Fetch columns & exclude auto-generated fields
            all_cols = fetch_columns(src, src_schema, table)
            data_cols = [c for c in all_cols if not is_auto_generated_column(c)]
            
            if not data_cols:
                if progress_callback:
                    progress_callback("  ⚠ skipped (no comparable columns)")
                tracker.complete_migration(migration_id_db, 0, success=True)
                metrics.complete_table(table, success=True)
                continue
            
            col_names = [c["column_name"] for c in data_cols]
            
            # Ensure destination table schema
            create_table_schema_if_not_exists(
                src, dst, table, src_schema, target_schema,
                exclude_auto_generated=True, progress_callback=progress_callback
            )
            
            # Fetch source & destination rows
            src_rows = fetch_all_rows(src, table, col_names, src_schema)
            
            try:
                dst_rows = fetch_all_rows(dst, table, col_names, target_schema)
            except Exception as e:
                logger.debug(f"Could not fetch destination rows for {table}: {str(e)}")
                dst_rows = []
            
            if not src_rows:
                if progress_callback:
                    progress_callback("  ✓ source empty")
                tracker.complete_migration(migration_id_db, 0, success=True)
                metrics.complete_table(table, success=True)
                continue
            
            # Build destination hash set
            dst_hashes = {
                row_fingerprint(r, col_names)
                for r in dst_rows
            }
            
            # Detect new rows
            new_rows = []
            for r in src_rows:
                if row_fingerprint(r, col_names) not in dst_hashes:
                    new_rows.append(prepare_row(r, data_cols))
            
            if progress_callback:
                progress_callback(
                    f"  Δ {len(new_rows)} new / "
                    f"{len(src_rows)} source / "
                    f"{len(dst_rows)} dest"
                )
            
            # Insert new rows
            if new_rows:
                inserted = insert_rows(
                    dst, table, new_rows, 
                    target_schema, config.batch_size, progress_callback
                )
                total_inserted += inserted
                metrics.update_table_progress(table, inserted)
                metrics.complete_table(table, success=True)
                tracker.complete_migration(migration_id_db, inserted, success=True)
                if progress_callback:
                    progress_callback(f"  ✓ inserted {inserted}")
            else:
                metrics.complete_table(table, success=True)
                tracker.complete_migration(migration_id_db, 0, success=True)
                if progress_callback:
                    progress_callback("  ✓ already synced")
        
        except Exception as e:
            metrics.complete_table(table, success=False, error_message=str(e))
            if migration_id_db:
                tracker.complete_migration(migration_id_db, 0, success=False, error_message=str(e))
            if progress_callback:
                progress_callback(f"  ✗ ERROR in table {table}: {str(e)}")
            logger.error(f"Error in incremental sync for {table}: {str(e)}")
    
    metrics.complete_migration(success=True)
    
    if progress_callback:
        progress_callback("=" * 60)
        progress_callback(f"✅ Incremental sync COMPLETED to schema: {target_schema}")
        progress_callback(f"Total rows inserted: {total_inserted}")
        progress_callback("=" * 60)


def table_copy_delete_and_recreate(
    src: Engine,
    dst: Engine,
    table: str,
    src_schema: str = "public",
    target_schema: Optional[str] = None,
    exclude_auto_generated: bool = False,
    config: Optional[MigrationConfig] = None,
    progress_callback: Optional[Callable] = None
):
    """
    Mode 3a: Delete destination table and recreate with all data
    """
    config = config or MigrationConfig()
    
    if target_schema is None:
        target_schema = config.get_schema_name("table_recreate", src.url.database)
    
    tracker = MigrationTracker(dst, target_schema)
    
    if progress_callback:
        progress_callback(f"Delete & recreate mode for table: {table} in schema: {target_schema}")
    
    migration_id = tracker.start_migration(
        table_name=table,
        source_db=src.url.database,
        source_host=src.url.host,
        migration_type="delete_recreate",
        target_schema=target_schema,
        metadata={"exclude_auto_generated": exclude_auto_generated}
    )
    
    try:
        # Recreate table schema (drops existing)
        columns = recreate_table_schema(
            src, dst, table, src_schema, target_schema,
            exclude_auto_generated, progress_callback
        )
        
        # Get column metadata
        cols_meta = [
            c for c in fetch_columns(src, src_schema, table) 
            if c["column_name"] in columns
        ]
        
        # Fetch all rows
        src_rows = fetch_all_rows(src, table, columns, src_schema)
        
        if progress_callback:
            progress_callback(f"Copying {len(src_rows)} rows...")
        
        # Prepare and insert
        prepared_rows = [prepare_row(r, cols_meta) for r in src_rows]
        inserted = insert_rows(
            dst, table, prepared_rows,
            target_schema, config.batch_size, progress_callback
        )
        
        # Create indexes if enabled
        if config.enable_indexes:
            create_indexes_and_constraints(
                src, dst, table, src_schema, target_schema, progress_callback
            )
        
        tracker.complete_migration(migration_id, inserted, success=True)
        
        if progress_callback:
            progress_callback(f"✓ Completed: {inserted} rows copied to {target_schema}.{table}")
        
        return inserted
    
    except Exception as e:
        tracker.complete_migration(migration_id, 0, success=False, error_message=str(e))
        raise


def table_copy_delta_only(
    src: Engine,
    dst: Engine,
    table: str,
    src_schema: str = "public",
    target_schema: Optional[str] = None,
    exclude_auto_generated: bool = False,
    config: Optional[MigrationConfig] = None,
    progress_callback: Optional[Callable] = None
):
    """
    Mode 3b: Copy only delta (new) items based on primary key
    """
    config = config or MigrationConfig()
    
    if target_schema is None:
        target_schema = config.get_schema_name("table_delta", src.url.database)
    
    tracker = MigrationTracker(dst, target_schema)
    
    if progress_callback:
        progress_callback(f"Delta copy mode for table: {table} in schema: {target_schema}")
    
    migration_id = tracker.start_migration(
        table_name=table,
        source_db=src.url.database,
        source_host=src.url.host,
        migration_type="delta_only",
        target_schema=target_schema,
        metadata={"exclude_auto_generated": exclude_auto_generated}
    )
    
    try:
        # Get primary key
        pk = fetch_primary_key(src, src_schema, table)
        if not pk:
            raise ValueError(f"Table {table} has no primary key - cannot perform delta copy")
        
        # Create table if it doesn't exist
        columns = create_table_schema_if_not_exists(
            src, dst, table, src_schema, target_schema,
            exclude_auto_generated, progress_callback
        )
        
        # Get column metadata
        cols_meta = [
            c for c in fetch_columns(src, src_schema, table) 
            if c["column_name"] in columns
        ]
        
        # Fetch source rows
        src_rows = fetch_all_rows(src, table, columns, src_schema)
        
        # Fetch existing PKs from destination
        existing_pks = fetch_existing_pks(dst, table, pk, target_schema)
        
        # Filter new rows
        new_rows = [
            prepare_row(r, cols_meta)
            for r in src_rows
            if r.get(pk) not in existing_pks
        ]
        
        if progress_callback:
            progress_callback(f"Found {len(new_rows)} new rows out of {len(src_rows)} total...")
        
        # Insert new rows
        if new_rows:
            inserted = insert_rows(
                dst, table, new_rows,
                target_schema, config.batch_size, progress_callback
            )
            tracker.complete_migration(migration_id, inserted, success=True)
            if progress_callback:
                progress_callback(f"✓ Completed: {inserted} new rows copied to {target_schema}.{table}")
        else:
            tracker.complete_migration(migration_id, 0, success=True)
            if progress_callback:
                progress_callback(f"✓ Completed: No new rows to copy (up to date)")
        
        return len(new_rows)
    
    except Exception as e:
        tracker.complete_migration(migration_id, 0, success=False, error_message=str(e))
        raise