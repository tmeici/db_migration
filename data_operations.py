"""
Data fetching, preparation, and insertion operations
"""
import json
import hashlib
import logging
from datetime import datetime, date
from decimal import Decimal
from typing import List, Dict, Optional, Callable
from sqlalchemy import text
from sqlalchemy.engine import Engine

from database import fetch_columns

logger = logging.getLogger(__name__)


def normalize_value(v):
    """Normalize value for consistent hashing"""
    if v is None:
        return None
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, float):
        return round(v, 8)
    if isinstance(v, (dict, list)):
        return json.dumps(v, sort_keys=True, separators=(",", ":"))
    return v


def row_fingerprint(row: dict, columns: list[str]) -> str:
    """Generate a unique fingerprint for a row based on its data"""
    normalized = {
        col: normalize_value(row.get(col))
        for col in columns
    }
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def fetch_all_rows(engine: Engine, table: str, columns: List[str], schema: str = "public") -> List[Dict]:
    """Fetch all rows from a table"""
    if not columns:
        return []
    
    cols_str = ", ".join([f'"{c}"' for c in columns])
    sql = f'SELECT {cols_str} FROM {schema}."{table}"'
    
    try:
        with engine.connect() as conn:
            return [dict(r._mapping) for r in conn.execute(text(sql))]
    except Exception as e:
        logger.error(f"Error fetching rows from {schema}.{table}: {str(e)}")
        raise


def fetch_existing_pks(engine: Engine, table: str, pk_col: str, schema: str = "migrated") -> set:
    """Fetch all existing primary keys from destination table"""
    try:
        sql = f'SELECT "{pk_col}" FROM {schema}."{table}"'
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            return {row[0] for row in result}
    except Exception as e:
        logger.debug(f"Could not fetch existing PKs from {schema}.{table}: {str(e)}")
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
    schema: str = "migrated",
    batch_size: int = 1000,
    progress_callback: Optional[Callable] = None
) -> int:
    """Insert rows into destination table"""
    if not rows:
        return 0
    
    # Get column names from first row
    columns = list(rows[0].keys())
    cols_str = ", ".join([f'"{c}"' for c in columns])
    placeholders = ", ".join([f":{c}" for c in columns])
    
    sql = f'INSERT INTO {schema}."{table}" ({cols_str}) VALUES ({placeholders})'
    
    total_inserted = 0
    
    try:
        with engine.begin() as conn:
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                conn.execute(text(sql), batch)
                total_inserted += len(batch)
                
                if progress_callback and len(rows) > batch_size:
                    progress_callback(f"  Inserted {total_inserted}/{len(rows)} rows...")
        
        logger.info(f"Inserted {total_inserted} rows into {schema}.{table}")
    except Exception as e:
        logger.error(f"Error inserting rows into {schema}.{table}: {str(e)}")
        raise
    
    return total_inserted


def get_row_count(engine: Engine, table: str, schema: str = "public") -> int:
    """Get total row count for a table"""
    sql = f'SELECT COUNT(*) FROM {schema}."{table}"'
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            return result.scalar()
    except:
        return 0
