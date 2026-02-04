"""
Database connection and basic operations
"""
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from typing import List, Dict, Optional, Tuple
import logging

from config import DBConfig

logger = logging.getLogger(__name__)


def create_engine_safe(cfg: DBConfig) -> Engine:
    """Create SQLAlchemy engine with proper connection URL and settings"""
    url = cfg.to_connection_string()
    return create_engine(
        url, 
        future=True, 
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        pool_recycle=3600
    )


def test_connection(cfg: DBConfig) -> Tuple[bool, str]:
    """Test database connection"""
    try:
        engine = create_engine_safe(cfg)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True, "Connection successful"
    except Exception as e:
        return False, f"Connection failed: {str(e)}"


def fetch_tables(engine: Engine, schema: str = "public") -> List[str]:
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


def fetch_enum_types(engine: Engine, schema: str = "public") -> Dict[str, List[str]]:
    """Fetch all ENUM types and their values from the database"""
    sql = """
    SELECT 
        t.typname as enum_name,
        array_agg(e.enumlabel ORDER BY e.enumsortorder) as enum_values
    FROM pg_type t
    JOIN pg_enum e ON t.oid = e.enumtypid
    JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = :schema
    GROUP BY t.typname
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"schema": schema})
        return {row[0]: row[1] for row in result}


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
        tc.constraint_name,
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


def table_exists(engine: Engine, schema: str, table: str) -> bool:
    """Check if a table exists in the specified schema"""
    sql = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = :schema AND table_name = :table
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"schema": schema, "table": table}).fetchone()
        return result is not None


def ensure_schema(engine: Engine, schema: str):
    """Create schema if it doesn't exist"""
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    logger.info(f"Schema '{schema}' ensured")


def get_row_count(engine: Engine, table: str, schema: str = "public") -> int:
    """Get total row count for a table"""
    sql = f'SELECT COUNT(*) FROM {schema}."{table}"'
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            return result.scalar()
    except Exception as e:
        logger.debug(f"Could not get row count for {schema}.{table}: {str(e)}")
        return 0