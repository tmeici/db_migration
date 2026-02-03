"""
Main migration operations with bug fixes and enhancements
"""
import logging
from typing import Optional, Callable, List
from sqlalchemy.engine import Engine

from database import fetch_tables, fetch_columns, fetch_primary_key
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
    row_fingerprint,
    get_row_count
)
from migration_tracker import MigrationTracker
from config import MigrationConfig

logger = logging.getLogger(__name__)


def full_migration(
    src: Engine, 
    dst: Engine,
    src_schema: str = "public",
    target_schema: str = "migrated",
    exclude_auto_generated: bool = False,
    config: Optional[MigrationConfig] = None,
    progress_callback: Optional[Callable] = None
):
    """
    Mode 1: Full migration (clean slate)
    Drops and recreates all tables with fresh data
    """
    config = config or MigrationConfig()
    tracker = MigrationTracker(dst, target_schema)
    
    if progress_callback:
        progress_callback("Starting full migration (clean slate)...")
    
    tables = fetch_tables(src, src_schema)
    
    total_tables = len(tables)
    total_rows = 0
    failed_tables = []
    
    for idx, table in enumerate(tables, 1):
        migration_id = None
        try:
            if progress_callback:
                progress_callback(f"[{idx}/{total_tables}] Processing table: {table}")
            
            # Track migration
            source_cfg = src.url
            migration_id = tracker.start_migration(
                table_name=table,
                source_db=source_cfg.database,
                source_host=source_cfg.host,
                migration_type="full_migration",
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
            
            # Create indexes if enabled
            if config.enable_indexes:
                create_indexes_and_constraints(
                    src, dst, table, src_schema, target_schema, progress_callback
                )
            
            # Mark as completed
            tracker.complete_migration(migration_id, inserted, success=True)
            
            if progress_callback:
                progress_callback(f"  ✓ {table}: {inserted} rows copied")
        
        except Exception as e:
            failed_tables.append((table, str(e)))
            if migration_id:
                tracker.complete_migration(migration_id, 0, success=False, error_message=str(e))
            if progress_callback:
                progress_callback(f"  ❌ ERROR in {table}: {str(e)}")
            logger.error(f"Error migrating table {table}: {str(e)}")
    
    if progress_callback:
        progress_callback(f"\n{'='*60}")
        progress_callback(f"✅ Full migration completed!")
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
    target_schema: str = "migrated",
    config: Optional[MigrationConfig] = None,
    progress_callback: Optional[Callable] = None
):
    """
    SAFE incremental sync with improved tracking:
    - Content-based (hash) comparison
    - Excludes auto-generated columns
    - Handles NULLs, JSON, floats, timestamps
    - Uses metadata to track migrations
    - Adds new data to existing migrated schema
    """
    config = config or MigrationConfig()
    tracker = MigrationTracker(dst, target_schema)
    
    if progress_callback:
        progress_callback("Starting SAFE incremental sync...")
    
    tables = fetch_tables(src, src_schema)
    
    total_inserted = 0
    source_cfg = src.url
    
    for idx, table in enumerate(tables, 1):
        migration_id = None
        try:
            if progress_callback:
                progress_callback(f"[{idx}/{len(tables)}] {table}")
            
            # Start tracking
            migration_id = tracker.start_migration(
                table_name=table,
                source_db=source_cfg.database,
                source_host=source_cfg.host,
                migration_type="incremental_sync"
            )
            
            # --------------------------------------------------
            # 1. Fetch columns & exclude auto-generated fields
            # --------------------------------------------------
            all_cols = fetch_columns(src, src_schema, table)
            data_cols = [c for c in all_cols if not is_auto_generated_column(c)]
            
            if not data_cols:
                if progress_callback:
                    progress_callback("  ⚠ skipped (no comparable columns)")
                tracker.complete_migration(migration_id, 0, success=True)
                continue
            
            col_names = [c["column_name"] for c in data_cols]
            
            # --------------------------------------------------
            # 2. Ensure destination table schema (doesn't drop)
            # --------------------------------------------------
            create_table_schema_if_not_exists(
                src, dst, table, src_schema, target_schema,
                exclude_auto_generated=True, progress_callback=progress_callback
            )
            
            # --------------------------------------------------
            # 3. Fetch source & destination rows
            # --------------------------------------------------
            src_rows = fetch_all_rows(src, table, col_names, src_schema)
            
            try:
                # FIX: Fetch from target_schema, not public schema
                dst_rows = fetch_all_rows(dst, table, col_names, target_schema)
            except Exception as e:
                logger.debug(f"Could not fetch destination rows for {table}: {str(e)}")
                dst_rows = []
            
            if not src_rows:
                if progress_callback:
                    progress_callback("  ✓ source empty")
                tracker.complete_migration(migration_id, 0, success=True)
                continue
            
            # --------------------------------------------------
            # 4. Build destination hash set
            # --------------------------------------------------
            dst_hashes = {
                row_fingerprint(r, col_names)
                for r in dst_rows
            }
            
            # --------------------------------------------------
            # 5. Detect new rows
            # --------------------------------------------------
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
            
            # --------------------------------------------------
            # 6. Insert new rows
            # --------------------------------------------------
            if new_rows:
                inserted = insert_rows(
                    dst, table, new_rows, 
                    target_schema, config.batch_size, progress_callback
                )
                total_inserted += inserted
                tracker.complete_migration(migration_id, inserted, success=True)
                if progress_callback:
                    progress_callback(f"  ✓ inserted {inserted}")
            else:
                tracker.complete_migration(migration_id, 0, success=True)
                if progress_callback:
                    progress_callback("  ✓ already synced")
        
        except Exception as e:
            if migration_id:
                tracker.complete_migration(migration_id, 0, success=False, error_message=str(e))
            if progress_callback:
                progress_callback(f"  ❌ ERROR in table {table}: {str(e)}")
            logger.error(f"Error in incremental sync for {table}: {str(e)}")
            # Continue with next table instead of failing fast
    
    if progress_callback:
        progress_callback("=" * 60)
        progress_callback("✅ Incremental sync COMPLETED")
        progress_callback(f"Total rows inserted: {total_inserted}")
        progress_callback("=" * 60)


def table_copy_delete_and_recreate(
    src: Engine,
    dst: Engine,
    table: str,
    src_schema: str = "public",
    target_schema: str = "migrated",
    exclude_auto_generated: bool = False,
    config: Optional[MigrationConfig] = None,
    progress_callback: Optional[Callable] = None
):
    """
    Mode 3a: Delete destination table and recreate with all data
    """
    config = config or MigrationConfig()
    tracker = MigrationTracker(dst, target_schema)
    
    if progress_callback:
        progress_callback(f"Delete & recreate mode for table: {table}")
    
    source_cfg = src.url
    migration_id = tracker.start_migration(
        table_name=table,
        source_db=source_cfg.database,
        source_host=source_cfg.host,
        migration_type="delete_recreate",
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
            progress_callback(f"✓ Completed: {inserted} rows copied")
        
        return inserted
    
    except Exception as e:
        tracker.complete_migration(migration_id, 0, success=False, error_message=str(e))
        raise


def table_copy_delta_only(
    src: Engine,
    dst: Engine,
    table: str,
    src_schema: str = "public",
    target_schema: str = "migrated",
    exclude_auto_generated: bool = False,
    config: Optional[MigrationConfig] = None,
    progress_callback: Optional[Callable] = None
):
    """
    Mode 3b: Copy only delta (new) items based on primary key
    Does NOT drop existing table - only adds new rows
    """
    config = config or MigrationConfig()
    tracker = MigrationTracker(dst, target_schema)
    
    if progress_callback:
        progress_callback(f"Delta copy mode for table: {table}")
    
    source_cfg = src.url
    migration_id = tracker.start_migration(
        table_name=table,
        source_db=source_cfg.database,
        source_host=source_cfg.host,
        migration_type="delta_only",
        metadata={"exclude_auto_generated": exclude_auto_generated}
    )
    
    try:
        # Get primary key
        pk = fetch_primary_key(src, src_schema, table)
        if not pk:
            raise ValueError(f"Table {table} has no primary key - cannot perform delta copy")
        
        # Create table if it doesn't exist (without dropping)
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
                progress_callback(f"✓ Completed: {inserted} new rows copied")
        else:
            tracker.complete_migration(migration_id, 0, success=True)
            if progress_callback:
                progress_callback(f"✓ Completed: No new rows to copy (up to date)")
        
        return len(new_rows)
    
    except Exception as e:
        tracker.complete_migration(migration_id, 0, success=False, error_message=str(e))
        raise
