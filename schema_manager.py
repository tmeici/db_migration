"""
Schema management operations
"""
import re
import logging
from typing import List, Dict, Optional, Callable, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine

from database import (
    fetch_columns, fetch_enum_types, fetch_primary_key,
    fetch_foreign_keys, fetch_indexes, ensure_schema
)

logger = logging.getLogger(__name__)


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


def ensure_enum_types(src: Engine, dst: Engine, src_schema: str = "public", dst_schema: str = "public"):
    """Create ENUM types in destination database if they don't exist"""
    enum_types = fetch_enum_types(src, src_schema)
    
    with dst.begin() as conn:
        for enum_name, enum_values in enum_types.items():
            # Check if enum already exists in destination schema
            check_sql = """
            SELECT 1 FROM pg_type t
            JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            WHERE t.typname = :enum_name AND n.nspname = :schema
            """
            exists = conn.execute(
                text(check_sql), 
                {"enum_name": enum_name, "schema": dst_schema}
            ).fetchone()
            
            if not exists:
                # Create the ENUM type
                values_str = ", ".join([f"'{val}'" for val in enum_values])
                create_enum_sql = f"CREATE TYPE {dst_schema}.{enum_name} AS ENUM ({values_str})"
                conn.execute(text(create_enum_sql))
                logger.info(f"Created ENUM type: {dst_schema}.{enum_name}")


def build_column_definition(col: Dict, target_schema: str, exclude_auto_generated: bool, pk: Optional[str]) -> Tuple[str, List[str]]:
    """
    Build column definition for CREATE TABLE
    Returns: (column_definition_string, list_of_sequences_to_create)
    """
    col_name = col["column_name"]
    data_type = col["data_type"]
    udt_name = col.get("udt_name", "")
    sequences = []
    
    # Handle USER-DEFINED types (ENUMs)
    if data_type.upper() == "USER-DEFINED":
        data_type = f"{target_schema}.{udt_name}"
    # Handle character types with length
    elif "character" in data_type and col.get("character_maximum_length"):
        data_type = f"{data_type}({col['character_maximum_length']})"
    # Handle numeric types with precision
    elif "numeric" in data_type and col.get("numeric_precision"):
        if col.get("numeric_scale"):
            data_type = f"{data_type}({col['numeric_precision']},{col['numeric_scale']})"
        else:
            data_type = f"{data_type}({col['numeric_precision']})"
    
    line = f'"{col_name}" {data_type}'
    
    # Add NOT NULL constraint (skip for auto-generated PKs when excluding)
    if col["is_nullable"] == "NO":
        if not (exclude_auto_generated and col_name == pk):
            line += " NOT NULL"
    
    # Handle DEFAULT values
    if col["column_default"]:
        if exclude_auto_generated and is_auto_generated_column(col):
            # Skip auto-generated defaults when excluded
            pass
        else:
            default_val = col["column_default"]
            
            # Check if default uses a sequence
            if "nextval" in default_val.lower():
                # Extract sequence name and create it in target schema
                seq_match = re.search(r"nextval\('([^']+)'", default_val)
                if seq_match:
                    seq_name = seq_match.group(1).split('.')[-1]  # Get just the sequence name
                    sequences.append(f'CREATE SEQUENCE IF NOT EXISTS {target_schema}."{seq_name}"')
                    # Update default to use target schema sequence
                    default_val = f"nextval('{target_schema}.{seq_name}'::regclass)"
                line += f" DEFAULT {default_val}"
            else:
                # Non-sequence default
                line += f" DEFAULT {default_val}"
    
    return line, sequences


def create_table_schema_if_not_exists(
    src: Engine, 
    dst: Engine, 
    table: str,
    src_schema: str = "public",
    target_schema: str = "migrated",
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
    
    # Ensure target schema exists
    ensure_schema(dst, target_schema)
    
    # Ensure ENUM types exist in destination
    ensure_enum_types(src, dst, src_schema, target_schema)
    
    cols = fetch_columns(src, src_schema, table)
    pk = fetch_primary_key(src, src_schema, table)
    
    # Filter out auto-generated columns if requested
    if exclude_auto_generated:
        cols_to_create = [c for c in cols if not is_auto_generated_column(c)]
    else:
        cols_to_create = cols
    
    # Build column definitions
    ddl_cols = []
    sequences_to_create = []
    
    for c in cols_to_create:
        col_def, seqs = build_column_definition(c, target_schema, exclude_auto_generated, pk)
        ddl_cols.append(col_def)
        sequences_to_create.extend(seqs)
    
    # Add primary key constraint if exists and not excluded
    if pk and (not exclude_auto_generated or pk in [c["column_name"] for c in cols_to_create]):
        ddl_cols.append(f'PRIMARY KEY ("{pk}")')
    
    # Build CREATE TABLE statement
    ddl_parts = sequences_to_create + [
        f'CREATE TABLE IF NOT EXISTS {target_schema}."{table}" (\n        '
        + ",\n        ".join(ddl_cols)
        + "\n    )"
    ]
    ddl = ";\n    ".join(ddl_parts) + ";"
    
    # Execute DDL
    with dst.begin() as conn:
        conn.execute(text(ddl))
    
    logger.info(f"Created/verified table schema: {target_schema}.{table}")
    
    # Return list of columns to copy
    return [c["column_name"] for c in cols_to_create]


def recreate_table_schema(
    src: Engine,
    dst: Engine,
    table: str,
    src_schema: str = "public",
    target_schema: str = "migrated",
    exclude_auto_generated: bool = False,
    progress_callback: Optional[Callable] = None
) -> List[str]:
    """
    Recreate table schema in destination (drops existing table first)
    Returns list of column names to copy
    """
    if progress_callback:
        progress_callback(f"Recreating schema for table: {table}")
    
    # Ensure target schema exists
    ensure_schema(dst, target_schema)
    
    # Ensure ENUM types exist in destination
    ensure_enum_types(src, dst, src_schema, target_schema)
    
    cols = fetch_columns(src, src_schema, table)
    pk = fetch_primary_key(src, src_schema, table)
    
    # Filter out auto-generated columns if requested
    if exclude_auto_generated:
        cols_to_create = [c for c in cols if not is_auto_generated_column(c)]
    else:
        cols_to_create = cols
    
    # Build column definitions
    ddl_cols = []
    sequences_to_create = []
    
    for c in cols_to_create:
        col_def, seqs = build_column_definition(c, target_schema, exclude_auto_generated, pk)
        ddl_cols.append(col_def)
        sequences_to_create.extend(seqs)
    
    # Add primary key constraint if exists and not excluded
    if pk and (not exclude_auto_generated or pk in [c["column_name"] for c in cols_to_create]):
        ddl_cols.append(f'PRIMARY KEY ("{pk}")')
    
    # Build DROP and CREATE TABLE statements
    drop_ddl = f'DROP TABLE IF EXISTS {target_schema}."{table}" CASCADE'
    create_ddl = ";\n    ".join(sequences_to_create + [
        f'CREATE TABLE {target_schema}."{table}" (\n        '
        + ",\n        ".join(ddl_cols)
        + "\n    )"
    ]) + ";"
    
    # Execute DDL
    with dst.begin() as conn:
        conn.execute(text(drop_ddl))
        conn.execute(text(create_ddl))
    
    logger.info(f"Recreated table schema: {target_schema}.{table}")
    
    # Return list of columns to copy
    return [c["column_name"] for c in cols_to_create]


def create_indexes_and_constraints(
    src: Engine,
    dst: Engine,
    table: str,
    src_schema: str = "public",
    target_schema: str = "migrated",
    progress_callback: Optional[Callable] = None
):
    """Create indexes and foreign key constraints after data migration"""
    # Create indexes
    indexes = fetch_indexes(src, src_schema, table)
    
    with dst.begin() as conn:
        for idx in indexes:
            try:
                # Replace schema in index definition
                index_def = idx["indexdef"]
                index_def = index_def.replace(f"{src_schema}.", f"{target_schema}.")
                conn.execute(text(index_def))
                logger.info(f"Created index: {idx['indexname']}")
            except Exception as e:
                logger.warning(f"Failed to create index {idx['indexname']}: {str(e)}")
    
    # Note: Foreign keys are intentionally not created automatically
    # as they can cause issues with incremental syncs
    # They can be added manually after all data is migrated