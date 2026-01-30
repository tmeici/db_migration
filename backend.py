from dataclasses import dataclass
from typing import List, Dict, Optional, Callable
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from datetime import datetime
import logging
import re
import json

# ---------------- CONFIG ---------------- #

@dataclass
class DBConfig:
    host: str
    port: str
    database: str
    user: str
    password: str


def create_engine_safe(cfg: DBConfig) -> Engine:
    """Create SQLAlchemy engine with proper connection URL"""
    url = f"postgresql+psycopg2://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.database}"
    return create_engine(url, future=True, pool_pre_ping=True)


# ---------------- SCHEMA HELPERS ---------------- #

def fetch_tables(engine: Engine, schema="public") -> List[str]:
    """Fetch all table names from a schema"""
    sql = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = :schema AND table_type='BASE TABLE'
    ORDER BY table_name
    """
    with engine.connect() as conn:
        return [r[0] for r in conn.execute(text(sql), {"schema": schema})]


def fetch_columns(engine: Engine, schema: str, table: str) -> List[Dict]:
    """Fetch column metadata for a table"""
    sql = """
    SELECT 
        column_name, 
        data_type, 
        is_nullable, 
        column_default,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        udt_name
    FROM information_schema.columns
    WHERE table_schema=:schema AND table_name=:table
    ORDER BY ordinal_position
    """
    with engine.connect() as conn:
        return [
            dict(r._mapping)
            for r in conn.execute(text(sql), {"schema": schema, "table": table})
        ]


def fetch_enum_types(engine: Engine) -> Dict[str, List[str]]:
    """Fetch all ENUM types and their values from the database"""
    sql = """
    SELECT 
        t.typname as enum_name,
        array_agg(e.enumlabel ORDER BY e.enumsortorder) as enum_values
    FROM pg_type t
    JOIN pg_enum e ON t.oid = e.enumtypid
    JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'public'
    GROUP BY t.typname
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        return {row[0]: row[1] for row in result}


def ensure_enum_types(src: Engine, dst: Engine):
    """Create ENUM types in destination database if they don't exist"""
    enum_types = fetch_enum_types(src)
    
    with dst.begin() as conn:
        for enum_name, enum_values in enum_types.items():
            # Check if enum already exists
            check_sql = """
            SELECT 1 FROM pg_type t
            JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            WHERE t.typname = :enum_name AND n.nspname = 'public'
            """
            exists = conn.execute(text(check_sql), {"enum_name": enum_name}).fetchone()
            
            if not exists:
                # Create the ENUM type
                values_str = ", ".join([f"'{val}'" for val in enum_values])
                create_enum_sql = f"CREATE TYPE public.{enum_name} AS ENUM ({values_str})"
                conn.execute(text(create_enum_sql))


def fetch_primary_key(engine: Engine, schema: str, table: str) -> Optional[str]:
    """Fetch primary key column name"""
    sql = """
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_schema = :schema
      AND tc.table_name = :table
    LIMIT 1
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql), {"schema": schema, "table": table}).fetchone()
        return row[0] if row else None


def fetch_foreign_keys(engine: Engine, schema: str, table: str) -> List[Dict]:
    """Fetch foreign key constraints"""
    sql = """
    SELECT
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = :schema
      AND tc.table_name = :table
    """
    with engine.connect() as conn:
        return [
            dict(r._mapping)
            for r in conn.execute(text(sql), {"schema": schema, "table": table})
        ]


def fetch_indexes(engine: Engine, schema: str, table: str) -> List[Dict]:
    """Fetch index information"""
    sql = """
    SELECT
        indexname,
        indexdef
    FROM pg_indexes
    WHERE schemaname = :schema
      AND tablename = :table
      AND indexname NOT LIKE '%_pkey'
    """
    with engine.connect() as conn:
        return [
            dict(r._mapping)
            for r in conn.execute(text(sql), {"schema": schema, "table": table})
        ]


def is_auto_generated_column(col: Dict) -> bool:
    """Check if column is auto-generated (serial, sequence, timestamps, or has default nextval)"""
    col_name = col.get("column_name", "").lower()
    default = col.get("column_default", "")
    data_type = col.get("data_type", "").lower()
    
    # Check common auto-generated column names
    auto_gen_names = ["id", "created_at", "updated_at", "created_on", "updated_on", "timestamp"]
    if col_name in auto_gen_names:
        return True
    
    # Check if column name ends with _id (likely a primary key)
    if col_name.endswith("_id") and ("nextval" in str(default).lower() or "serial" in data_type):
        return True
    
    # Check for default values that indicate auto-generation
    if default:
        default = default.lower()
        if "nextval" in default or "uuid_generate" in default or "now()" in default or "current_timestamp" in default:
            return True
    
    # Check for serial types
    if "serial" in data_type:
        return True
    
    return False


# ---------------- SCHEMA OPERATIONS ---------------- #

def ensure_migrated_schema(engine: Engine):
    """Create 'migrated' schema if it doesn't exist"""
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS migrated"))


def create_table_schema_if_not_exists(
    src: Engine, 
    dst: Engine, 
    table: str, 
    exclude_auto_generated: bool = False,
    progress_callback: Optional[Callable] = None
) -> List[str]:
    """
    Create table schema in destination ONLY if it doesn't exist (for incremental sync)
    Does NOT drop existing tables
    Returns list of column names to copy
    """
    if progress_callback:
        progress_callback(f"Creating schema for table: {table}")
    
    # Ensure ENUM types exist in destination
    ensure_enum_types(src, dst)
    
    cols = fetch_columns(src, "public", table)
    pk = fetch_primary_key(src, "public", table)
    
    # Filter out auto-generated columns if requested
    if exclude_auto_generated:
        cols_to_create = [c for c in cols if not is_auto_generated_column(c)]
    else:
        cols_to_create = cols
    
    # Build column definitions
    ddl_cols = []
    sequences_to_create = []
    
    for c in cols_to_create:
        col_name = c["column_name"]
        data_type = c["data_type"]
        udt_name = c.get("udt_name", "")
        
        # Handle USER-DEFINED types (ENUMs)
        if data_type.upper() == "USER-DEFINED":
            data_type = f"public.{udt_name}"
        # Handle character types with length
        elif "character" in data_type and c.get("character_maximum_length"):
            data_type = f"{data_type}({c['character_maximum_length']})"
        # Handle numeric types with precision
        elif "numeric" in data_type and c.get("numeric_precision"):
            if c.get("numeric_scale"):
                data_type = f"{data_type}({c['numeric_precision']},{c['numeric_scale']})"
            else:
                data_type = f"{data_type}({c['numeric_precision']})"
        
        line = f'"{col_name}" {data_type}'
        
        # Add NOT NULL constraint (skip for auto-generated PKs)
        if c["is_nullable"] == "NO":
            if not (exclude_auto_generated and col_name == pk):
                line += " NOT NULL"
        
        # Handle DEFAULT values
        if c["column_default"]:
            if exclude_auto_generated and is_auto_generated_column(c):
                # Skip auto-generated defaults when excluded
                pass
            else:
                default_val = c["column_default"]
                
                # Check if default uses a sequence
                if "nextval" in default_val.lower():
                    # Extract sequence name and create it in migrated schema
                    seq_match = re.search(r"nextval\('([^']+)'", default_val)
                    if seq_match:
                        seq_name = seq_match.group(1)
                        # Create sequence in migrated schema
                        sequences_to_create.append(f'CREATE SEQUENCE IF NOT EXISTS migrated."{seq_name}"')
                        # Update default to use migrated schema sequence
                        default_val = default_val.replace(f"'{seq_name}'", f"'migrated.{seq_name}'")
                    line += f" DEFAULT {default_val}"
                else:
                    # Non-sequence default
                    line += f" DEFAULT {default_val}"
        
        ddl_cols.append(line)
    
    # Add primary key constraint if exists and not excluded
    if pk and (not exclude_auto_generated or pk in [c["column_name"] for c in cols_to_create]):
        ddl_cols.append(f'PRIMARY KEY ("{pk}")')
    
    # Build CREATE TABLE statement
    ddl = ";\n    ".join(sequences_to_create + [
        f'CREATE TABLE IF NOT EXISTS migrated."{table}" (\n        '
        + ", ".join(ddl_cols)
        + "\n    )"
    ]) + ";"
    
    # Execute DDL
    with dst.begin() as conn:
        conn.execute(text(ddl))
    
    # Return list of columns to copy
    return [c["column_name"] for c in cols_to_create]


def recreate_table_schema(
    src: Engine,
    dst: Engine,
    table: str,
    exclude_auto_generated: bool = False,
    progress_callback: Optional[Callable] = None
) -> List[str]:
    """
    Recreate table schema in destination (drops existing table first)
    Returns list of column names to copy
    """
    if progress_callback:
        progress_callback(f"Recreating schema for table: {table}")
    
    # Ensure ENUM types exist in destination
    ensure_enum_types(src, dst)
    
    cols = fetch_columns(src, "public", table)
    pk = fetch_primary_key(src, "public", table)
    
    # Filter out auto-generated columns if requested
    if exclude_auto_generated:
        cols_to_create = [c for c in cols if not is_auto_generated_column(c)]
    else:
        cols_to_create = cols
    
    # Build column definitions
    ddl_cols = []
    sequences_to_create = []
    
    for c in cols_to_create:
        col_name = c["column_name"]
        data_type = c["data_type"]
        udt_name = c.get("udt_name", "")
        
        # Handle USER-DEFINED types (ENUMs)
        if data_type.upper() == "USER-DEFINED":
            data_type = f"public.{udt_name}"
        # Handle character types with length
        elif "character" in data_type and c.get("character_maximum_length"):
            data_type = f"{data_type}({c['character_maximum_length']})"
        # Handle numeric types with precision
        elif "numeric" in data_type and c.get("numeric_precision"):
            if c.get("numeric_scale"):
                data_type = f"{data_type}({c['numeric_precision']},{c['numeric_scale']})"
            else:
                data_type = f"{data_type}({c['numeric_precision']})"
        
        line = f'"{col_name}" {data_type}'
        
        # Add NOT NULL constraint (skip for auto-generated PKs)
        if c["is_nullable"] == "NO":
            if not (exclude_auto_generated and col_name == pk):
                line += " NOT NULL"
        
        # Handle DEFAULT values
        if c["column_default"]:
            if exclude_auto_generated and is_auto_generated_column(c):
                # Skip auto-generated defaults when excluded
                pass
            else:
                default_val = c["column_default"]
                
                # Check if default uses a sequence
                if "nextval" in default_val.lower():
                    # Extract sequence name and create it in migrated schema
                    seq_match = re.search(r"nextval\('([^']+)'", default_val)
                    if seq_match:
                        seq_name = seq_match.group(1)
                        # Create sequence in migrated schema
                        sequences_to_create.append(f'CREATE SEQUENCE IF NOT EXISTS migrated."{seq_name}"')
                        # Update default to use migrated schema sequence
                        default_val = default_val.replace(f"'{seq_name}'", f"'migrated.{seq_name}'")
                    line += f" DEFAULT {default_val}"
                else:
                    # Non-sequence default
                    line += f" DEFAULT {default_val}"
        
        ddl_cols.append(line)
    
    # Add primary key constraint if exists and not excluded
    if pk and (not exclude_auto_generated or pk in [c["column_name"] for c in cols_to_create]):
        ddl_cols.append(f'PRIMARY KEY ("{pk}")')
    
    # Build DROP and CREATE TABLE statements
    drop_ddl = f'DROP TABLE IF EXISTS migrated."{table}" CASCADE'
    create_ddl = ";\n    ".join(sequences_to_create + [
        f'CREATE TABLE migrated."{table}" (\n        '
        + ", ".join(ddl_cols)
        + "\n    )"
    ]) + ";"
    
    # Execute DDL
    with dst.begin() as conn:
        conn.execute(text(drop_ddl))
        conn.execute(text(create_ddl))
    
    # Return list of columns to copy
    return [c["column_name"] for c in cols_to_create]


# ---------------- DATA OPERATIONS ---------------- #

def fetch_all_rows(engine: Engine, table: str, columns: List[str]) -> List[Dict]:
    """Fetch all rows from source table"""
    cols_str = ", ".join([f'"{c}"' for c in columns])
    sql = f'SELECT {cols_str} FROM public."{table}"'
    with engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(text(sql))]


def fetch_existing_pks(engine: Engine, table: str, pk_col: str) -> set:
    """Fetch all existing primary keys from destination table"""
    try:
        sql = f'SELECT "{pk_col}" FROM migrated."{table}"'
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            return {row[0] for row in result}
    except:
        return set()


def prepare_row(row: Dict, cols_meta: List[Dict]) -> Dict:
    """Prepare row for insertion by handling special types"""
    prepared = {}
    for col in cols_meta:
        col_name = col["column_name"]
        value = row.get(col_name)
        
        if value is None:
            prepared[col_name] = None
        else:
            data_type = col["data_type"].lower()
            
            # Handle JSON/JSONB
            if data_type in ("json", "jsonb"):
                if isinstance(value, (dict, list)):
                    prepared[col_name] = json.dumps(value)
                else:
                    prepared[col_name] = value
            # Handle arrays
            elif data_type == "array":
                if isinstance(value, list):
                    prepared[col_name] = value
                else:
                    prepared[col_name] = value
            # Handle timestamps
            elif "timestamp" in data_type or "date" in data_type:
                if isinstance(value, datetime):
                    prepared[col_name] = value.isoformat()
                else:
                    prepared[col_name] = value
            else:
                prepared[col_name] = value
    
    return prepared


def insert_rows(
    engine: Engine, 
    table: str, 
    rows: List[Dict],
    progress_callback: Optional[Callable] = None
) -> int:
    """Insert rows into destination table"""
    if not rows:
        return 0
    
    # Get column names from first row
    columns = list(rows[0].keys())
    cols_str = ", ".join([f'"{c}"' for c in columns])
    placeholders = ", ".join([f":{c}" for c in columns])
    
    sql = f'INSERT INTO migrated."{table}" ({cols_str}) VALUES ({placeholders})'
    
    batch_size = 1000
    total_inserted = 0
    
    with engine.begin() as conn:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            conn.execute(text(sql), batch)
            total_inserted += len(batch)
            
            if progress_callback and len(rows) > batch_size:
                progress_callback(f"  Inserted {total_inserted}/{len(rows)} rows...")
    
    return total_inserted


# ---------------- MIGRATION MODES ---------------- #

def full_migration(
    src: Engine, 
    dst: Engine, 
    exclude_auto_generated: bool = False,
    progress_callback: Optional[Callable] = None
):
    """
    Mode 1: Full migration (clean slate)
    Drops and recreates all tables with fresh data
    """
    if progress_callback:
        progress_callback("Starting full migration (clean slate)...")
    
    ensure_migrated_schema(dst)
    tables = fetch_tables(src, "public")
    
    total_tables = len(tables)
    total_rows = 0
    
    for idx, table in enumerate(tables, 1):
        if progress_callback:
            progress_callback(f"[{idx}/{total_tables}] Processing table: {table}")
        
        # Recreate table schema
        columns = recreate_table_schema(src, dst, table, exclude_auto_generated, progress_callback)
        
        # Get column metadata
        cols_meta = [c for c in fetch_columns(src, "public", table) if c["column_name"] in columns]
        
        # Fetch all rows
        src_rows = fetch_all_rows(src, table, columns)
        
        if progress_callback:
            progress_callback(f"[{idx}/{total_tables}] Copying {len(src_rows)} rows...")
        
        # Prepare and insert
        prepared_rows = [prepare_row(r, cols_meta) for r in src_rows]
        inserted = insert_rows(dst, table, prepared_rows, progress_callback)
        total_rows += inserted
        
        if progress_callback:
            progress_callback(f"[{idx}/{total_tables}] ✓ {table}: {inserted} rows copied")
    
    if progress_callback:
        progress_callback(f"\n{'='*60}")
        progress_callback(f"✅ Full migration completed!")
        progress_callback(f"{'='*60}")
        progress_callback(f"Total tables: {total_tables}")
        progress_callback(f"Total rows copied: {total_rows}")
        progress_callback(f"{'='*60}")


def incremental_sync(
    src: Engine, 
    dst: Engine,
    progress_callback: Optional[Callable] = None
):
    """
    Mode 2: Incremental sync (delta copy)
    Only copies new rows that don't exist in destination based on primary key
    Does NOT drop existing tables - preserves existing data
    """
    if progress_callback:
        progress_callback("Starting incremental sync...")
    
    ensure_migrated_schema(dst)
    tables = fetch_tables(src, "public")
    
    total_tables = len(tables)
    stats = {
        "tables_created": 0,
        "tables_synced": 0,
        "tables_skipped": 0,
        "tables_up_to_date": 0,
        "total_rows_synced": 0
    }
    
    for idx, table in enumerate(tables, 1):
        if progress_callback:
            progress_callback(f"[{idx}/{total_tables}] Processing table: {table}")
        
        # Get primary key
        pk = fetch_primary_key(src, "public", table)
        if not pk:
            stats["tables_skipped"] += 1
            if progress_callback:
                progress_callback(f"[{idx}/{total_tables}] ⚠ Skipped {table} (no primary key)")
            continue
        
        # Check if table exists in destination
        table_exists = False
        try:
            with dst.connect() as conn:
                conn.execute(text(f'SELECT 1 FROM migrated."{table}" LIMIT 1'))
            table_exists = True
        except:
            table_exists = False
        
        # Create table schema if it doesn't exist (without dropping)
        if not table_exists:
            stats["tables_created"] += 1
            if progress_callback:
                progress_callback(f"[{idx}/{total_tables}] Creating table schema...")
            columns = create_table_schema_if_not_exists(src, dst, table, exclude_auto_generated=False)
        else:
            # Table exists, just get columns
            stats["tables_synced"] += 1
            if progress_callback:
                progress_callback(f"[{idx}/{total_tables}] Table exists, syncing new rows...")
            columns = [c["column_name"] for c in fetch_columns(src, "public", table)]
        
        cols_meta = fetch_columns(src, "public", table)
        
        # Fetch source rows
        src_rows = fetch_all_rows(src, table, columns)
        
        # Fetch existing PKs in destination (will be empty if table was just created)
        existing_pks = fetch_existing_pks(dst, table, pk)
        
        # Filter new rows
        new_rows = [
            prepare_row(r, cols_meta)
            for r in src_rows
            if r.get(pk) not in existing_pks
        ]
        
        if progress_callback:
            progress_callback(f"[{idx}/{total_tables}] Found {len(new_rows)} new rows out of {len(src_rows)} total...")
        
        # Insert new rows
        if new_rows:
            inserted = insert_rows(dst, table, new_rows)
            stats["total_rows_synced"] += inserted
            if progress_callback:
                progress_callback(f"[{idx}/{total_tables}] ✓ {table}: {inserted} new rows synced")
        else:
            stats["tables_up_to_date"] += 1
            if progress_callback:
                progress_callback(f"[{idx}/{total_tables}] ✓ {table}: No new rows to sync (up to date)")
    
    if progress_callback:
        progress_callback(f"\n{'='*60}")
        progress_callback(f"✅ Incremental sync completed!")
        progress_callback(f"{'='*60}")
        progress_callback(f"Total tables processed: {total_tables}")
        progress_callback(f"  • New tables created: {stats['tables_created']}")
        progress_callback(f"  • Existing tables synced: {stats['tables_synced']}")
        progress_callback(f"  • Tables up to date: {stats['tables_up_to_date']}")
        progress_callback(f"  • Tables skipped (no PK): {stats['tables_skipped']}")
        progress_callback(f"  • Total new rows synced: {stats['total_rows_synced']}")
        progress_callback(f"{'='*60}")


def table_copy_delete_and_recreate(
    src: Engine,
    dst: Engine,
    table: str,
    exclude_auto_generated: bool = False,
    progress_callback: Optional[Callable] = None
):
    """
    Mode 3a: Delete destination table and recreate with all data
    """
    if progress_callback:
        progress_callback(f"Delete & recreate mode for table: {table}")
    
    ensure_migrated_schema(dst)
    
    # Recreate table schema (drops existing)
    columns = recreate_table_schema(src, dst, table, exclude_auto_generated)
    
    # Get column metadata
    cols_meta = [c for c in fetch_columns(src, "public", table) if c["column_name"] in columns]
    
    # Fetch all rows
    src_rows = fetch_all_rows(src, table, columns)
    
    if progress_callback:
        progress_callback(f"Copying {len(src_rows)} rows...")
    
    # Prepare and insert
    prepared_rows = [prepare_row(r, cols_meta) for r in src_rows]
    inserted = insert_rows(dst, table, prepared_rows, progress_callback)
    
    if progress_callback:
        progress_callback(f"✓ Completed: {inserted} rows copied")
    
    return inserted


def table_copy_delta_only(
    src: Engine,
    dst: Engine,
    table: str,
    exclude_auto_generated: bool = False,
    progress_callback: Optional[Callable] = None
):
    """
    Mode 3b: Copy only delta (new) items based on primary key
    Does NOT drop existing table - only adds new rows
    """
    if progress_callback:
        progress_callback(f"Delta copy mode for table: {table}")
    
    ensure_migrated_schema(dst)
    
    # Get primary key
    pk = fetch_primary_key(src, "public", table)
    if not pk:
        raise ValueError(f"Table {table} has no primary key - cannot perform delta copy")
    
    # Check if table exists
    table_exists = False
    try:
        with dst.connect() as conn:
            conn.execute(text(f'SELECT 1 FROM migrated."{table}" LIMIT 1'))
        table_exists = True
    except:
        table_exists = False
    
    # Create table if it doesn't exist (without dropping)
    if not table_exists:
        if progress_callback:
            progress_callback(f"Table doesn't exist, creating schema...")
        columns = create_table_schema_if_not_exists(src, dst, table, exclude_auto_generated)
    else:
        if progress_callback:
            progress_callback(f"Table exists, finding new rows...")
        columns = [c["column_name"] for c in fetch_columns(src, "public", table)]
        if exclude_auto_generated:
            # Filter out auto-generated columns
            all_cols = fetch_columns(src, "public", table)
            columns = [c["column_name"] for c in all_cols if not is_auto_generated_column(c)]
    
    # Get column metadata
    cols_meta = [c for c in fetch_columns(src, "public", table) if c["column_name"] in columns]
    
    # Fetch source rows
    src_rows = fetch_all_rows(src, table, columns)
    
    # Fetch existing PKs
    existing_pks = fetch_existing_pks(dst, table, pk)
    
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
        inserted = insert_rows(dst, table, new_rows, progress_callback)
        if progress_callback:
            progress_callback(f"✓ Completed: {inserted} new rows copied")
    else:
        if progress_callback:
            progress_callback(f"✓ Completed: No new rows to copy (up to date)")
    
    return len(new_rows)


def test_connection(cfg: DBConfig) -> tuple[bool, str]:
    """Test database connection"""
    try:
        engine = create_engine_safe(cfg)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, "Connection successful"
    except Exception as e:
        return False, f"Connection failed: {str(e)}"