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
        numeric_scale
    FROM information_schema.columns
    WHERE table_schema=:schema AND table_name=:table
    ORDER BY ordinal_position
    """
    with engine.connect() as conn:
        return [
            dict(r._mapping)
            for r in conn.execute(text(sql), {"schema": schema, "table": table})
        ]


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
    """Check if column is auto-generated (serial, sequence, or has default nextval)"""
    default = col.get("column_default", "")
    if default:
        default = default.lower()
        if "nextval" in default or "uuid_generate" in default:
            return True
    
    data_type = col.get("data_type", "").lower()
    if "serial" in data_type:
        return True
    
    return False


# ---------------- SCHEMA OPERATIONS ---------------- #

def ensure_migrated_schema(engine: Engine):
    """Create 'migrated' schema if it doesn't exist"""
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS migrated"))


def recreate_table_schema(
    src: Engine, 
    dst: Engine, 
    table: str, 
    exclude_auto_generated: bool = False,
    progress_callback: Optional[Callable] = None
) -> List[str]:
    """
    Recreate table schema in destination database under 'migrated' schema
    Returns list of column names to copy
    """
    if progress_callback:
        progress_callback(f"Creating schema for table: {table}")
    
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
        
        # Handle character types with length
        if "character" in data_type and c.get("character_maximum_length"):
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
                    # Example: nextval('attachments_id_seq'::regclass)
                    import re
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
    
    # Build complete DDL with sequences
    ddl_parts = []
    
    # Add sequence creation statements
    for seq_ddl in sequences_to_create:
        ddl_parts.append(seq_ddl + ";")
    
    # Add table creation
    ddl_parts.append(f"""
    DROP TABLE IF EXISTS migrated."{table}" CASCADE;
    
    CREATE TABLE migrated."{table}" (
        {", ".join(ddl_cols)}
    );
    """)
    
    ddl = "\n".join(ddl_parts)
    
    with dst.begin() as conn:
        conn.execute(text(ddl))
    
    return [c["column_name"] for c in cols_to_create]


# ---------------- DATA OPERATIONS ---------------- #

def fetch_all_rows(engine: Engine, table: str, columns: List[str]) -> List[Dict]:
    """Fetch all rows from a table"""
    col_list = ", ".join(f'"{c}"' for c in columns)
    sql = f'SELECT {col_list} FROM public."{table}"'
    with engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(text(sql))]


def fetch_existing_pks(engine: Engine, table: str, pk: str) -> set:
    """Fetch existing primary keys from destination table"""
    sql = f'SELECT "{pk}" FROM migrated."{table}"'
    try:
        with engine.connect() as conn:
            return {r[0] for r in conn.execute(text(sql))}
    except:
        return set()


def prepare_row(row: Dict, columns_meta: List[Dict]) -> Dict:
    """Fill NOT NULL columns with safe defaults if None and handle special types"""
    fixed = row.copy()
    
    for c in columns_meta:
        name = c["column_name"]
        if name not in fixed:
            continue
        
        data_type = c["data_type"].lower()
        value = fixed.get(name)
        
        # Handle JSON/JSONB types - convert dict/list to JSON string
        if data_type in ("json", "jsonb"):
            if value is not None and isinstance(value, (dict, list)):
                fixed[name] = json.dumps(value)
            elif value is None and c["is_nullable"] == "NO":
                fixed[name] = "{}"
        # Handle NULL values for NOT NULL columns
        elif c["is_nullable"] == "NO" and value is None:
            if "timestamp" in data_type or "date" in data_type:
                fixed[name] = datetime.utcnow()
            elif "character" in data_type or "text" in data_type:
                fixed[name] = ""
            elif "boolean" in data_type:
                fixed[name] = False
            else:
                fixed[name] = 0
    
    return fixed


def insert_rows(
    engine: Engine, 
    table: str, 
    rows: List[Dict],
    progress_callback: Optional[Callable] = None
) -> int:
    """Insert rows into destination table"""
    if not rows:
        return 0
    
    cols = rows[0].keys()
    col_list = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(f":{c}" for c in cols)
    
    sql = text(f'''
        INSERT INTO migrated."{table}" ({col_list})
        VALUES ({placeholders})
    ''')
    
    inserted = 0
    batch_size = 100
    
    with engine.begin() as conn:
        for i, r in enumerate(rows):
            conn.execute(sql, r)
            inserted += 1
            
            if progress_callback and (i + 1) % batch_size == 0:
                progress_callback(f"Inserted {inserted}/{len(rows)} rows")
    
    return inserted


def delete_all_rows(engine: Engine, table: str, progress_callback: Optional[Callable] = None):
    """Delete all rows from a table"""
    if progress_callback:
        progress_callback(f"Deleting all rows from migrated.{table}")
    
    sql = f'DELETE FROM migrated."{table}"'
    with engine.begin() as conn:
        conn.execute(text(sql))


def get_row_count(engine: Engine, table: str, schema: str = "public") -> int:
    """Get row count for a table"""
    sql = f'SELECT COUNT(*) FROM {schema}."{table}"'
    with engine.connect() as conn:
        return conn.execute(text(sql)).scalar()


# ---------------- MIGRATION MODES ---------------- #

def full_database_copy(
    src: Engine, 
    dst: Engine,
    progress_callback: Optional[Callable] = None
):
    """
    Mode 1: Complete database copy
    Copies all tables and data from source to destination.migrated schema
    """
    if progress_callback:
        progress_callback("Starting full database copy...")
    
    ensure_migrated_schema(dst)
    tables = fetch_tables(src, "public")
    
    total_tables = len(tables)
    
    for idx, table in enumerate(tables, 1):
        if progress_callback:
            progress_callback(f"[{idx}/{total_tables}] Processing table: {table}")
        
        # Create table schema
        columns = recreate_table_schema(src, dst, table, exclude_auto_generated=False)
        
        # Get column metadata
        cols_meta = fetch_columns(src, "public", table)
        
        # Fetch all rows
        src_rows = fetch_all_rows(src, table, columns)
        
        if progress_callback:
            progress_callback(f"[{idx}/{total_tables}] Copying {len(src_rows)} rows...")
        
        # Prepare and insert rows
        prepared_rows = [prepare_row(r, cols_meta) for r in src_rows]
        inserted = insert_rows(dst, table, prepared_rows)
        
        if progress_callback:
            progress_callback(f"[{idx}/{total_tables}] ✓ {table}: {inserted} rows copied")
    
    if progress_callback:
        progress_callback(f"✅ Full database copy completed! {total_tables} tables processed.")


def incremental_sync(
    src: Engine, 
    dst: Engine,
    progress_callback: Optional[Callable] = None
):
    """
    Mode 2: Incremental sync (delta copy)
    Only copies new rows that don't exist in destination based on primary key
    """
    if progress_callback:
        progress_callback("Starting incremental sync...")
    
    ensure_migrated_schema(dst)
    tables = fetch_tables(src, "public")
    
    total_tables = len(tables)
    
    for idx, table in enumerate(tables, 1):
        if progress_callback:
            progress_callback(f"[{idx}/{total_tables}] Processing table: {table}")
        
        # Get primary key
        pk = fetch_primary_key(src, "public", table)
        if not pk:
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
        
        # Create table schema if it doesn't exist
        if not table_exists:
            if progress_callback:
                progress_callback(f"[{idx}/{total_tables}] Creating table schema...")
            columns = recreate_table_schema(src, dst, table, exclude_auto_generated=False)
        else:
            # Table exists, just get columns
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
            progress_callback(f"[{idx}/{total_tables}] Found {len(new_rows)} new rows...")
        
        # Insert new rows
        inserted = insert_rows(dst, table, new_rows)
        
        if progress_callback:
            progress_callback(f"[{idx}/{total_tables}] ✓ {table}: {inserted} new rows synced")
    
    if progress_callback:
        progress_callback(f"✅ Incremental sync completed! {total_tables} tables processed.")


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
    """
    if progress_callback:
        progress_callback(f"Delta copy mode for table: {table}")
    
    ensure_migrated_schema(dst)
    
    # Get primary key
    pk = fetch_primary_key(src, "public", table)
    if not pk:
        raise ValueError(f"Table {table} has no primary key - cannot perform delta copy")
    
    # Ensure table exists in destination
    try:
        columns = recreate_table_schema(src, dst, table, exclude_auto_generated)
    except:
        columns = [c["column_name"] for c in fetch_columns(src, "public", table)]
    
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
        progress_callback(f"Found {len(new_rows)} new rows to copy...")
    
    # Insert new rows
    inserted = insert_rows(dst, table, new_rows, progress_callback)
    
    if progress_callback:
        progress_callback(f"✓ Completed: {inserted} new rows copied")
    
    return inserted


def test_connection(cfg: DBConfig) -> tuple[bool, str]:
    """Test database connection"""
    try:
        engine = create_engine_safe(cfg)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, "Connection successful"
    except Exception as e:
        return False, f"Connection failed: {str(e)}"