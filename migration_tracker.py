"""
Migration tracking and metadata management
"""
import json
import logging
from datetime import datetime
from typing import Dict, Optional, List
from sqlalchemy import text
from sqlalchemy.engine import Engine

from database import ensure_schema

logger = logging.getLogger(__name__)


class MigrationTracker:
    """Track migration operations and metadata"""
    
    def __init__(self, engine: Engine, schema: str = "migrated"):
        self.engine = engine
        self.schema = schema
        self._ensure_tracking_table()
    
    def _ensure_tracking_table(self):
        """Create migration tracking table if it doesn't exist"""
        ensure_schema(self.engine, self.schema)
        
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.schema}._migration_metadata (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(255) NOT NULL,
            source_db VARCHAR(255) NOT NULL,
            source_host VARCHAR(255),
            migration_type VARCHAR(50) NOT NULL,
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            rows_migrated INTEGER,
            status VARCHAR(50) NOT NULL,
            error_message TEXT,
            metadata JSONB,
            target_schema VARCHAR(255),
            UNIQUE(table_name, source_db, started_at)
        )
        """
        
        with self.engine.begin() as conn:
            conn.execute(text(sql))
    
    def start_migration(
        self, 
        table_name: str, 
        source_db: str,
        source_host: str,
        migration_type: str,
        target_schema: str = "migrated",
        metadata: Optional[Dict] = None
    ) -> int:
        """Record the start of a migration"""
        sql = f"""
        INSERT INTO {self.schema}._migration_metadata 
        (table_name, source_db, source_host, migration_type, started_at, status, metadata, target_schema)
        VALUES (:table_name, :source_db, :source_host, :migration_type, :started_at, :status, :metadata, :target_schema)
        RETURNING id
        """
        
        with self.engine.begin() as conn:
            result = conn.execute(
                text(sql),
                {
                    "table_name": table_name,
                    "source_db": source_db,
                    "source_host": source_host,
                    "migration_type": migration_type,
                    "started_at": datetime.now(),
                    "status": "in_progress",
                    "metadata": json.dumps(metadata or {}),
                    "target_schema": target_schema
                }
            )
            migration_id = result.scalar()
            logger.info(f"Started migration tracking for {table_name} (ID: {migration_id})")
            return migration_id
    
    def complete_migration(
        self, 
        migration_id: int, 
        rows_migrated: int,
        success: bool = True,
        error_message: Optional[str] = None
    ):
        """Mark a migration as completed"""
        sql = f"""
        UPDATE {self.schema}._migration_metadata
        SET completed_at = :completed_at,
            rows_migrated = :rows_migrated,
            status = :status,
            error_message = :error_message
        WHERE id = :id
        """
        
        with self.engine.begin() as conn:
            conn.execute(
                text(sql),
                {
                    "id": migration_id,
                    "completed_at": datetime.now(),
                    "rows_migrated": rows_migrated,
                    "status": "completed" if success else "failed",
                    "error_message": error_message
                }
            )
            logger.info(f"Migration {migration_id} marked as {'completed' if success else 'failed'}")
    
    def get_last_migration(self, table_name: str, source_db: str) -> Optional[Dict]:
        """Get the last successful migration for a table"""
        sql = f"""
        SELECT *
        FROM {self.schema}._migration_metadata
        WHERE table_name = :table_name 
          AND source_db = :source_db
          AND status = 'completed'
        ORDER BY completed_at DESC
        LIMIT 1
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(
                text(sql),
                {"table_name": table_name, "source_db": source_db}
            )
            row = result.fetchone()
            return dict(row._mapping) if row else None
    
    def get_migration_history(
        self, 
        table_name: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """Get migration history"""
        if table_name:
            sql = f"""
            SELECT *
            FROM {self.schema}._migration_metadata
            WHERE table_name = :table_name
            ORDER BY started_at DESC
            LIMIT :limit
            """
            params = {"table_name": table_name, "limit": limit}
        else:
            sql = f"""
            SELECT *
            FROM {self.schema}._migration_metadata
            ORDER BY started_at DESC
            LIMIT :limit
            """
            params = {"limit": limit}
        
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params)
            return [dict(row._mapping) for row in result]
    
    def should_incremental_sync(
        self, 
        table_name: str, 
        source_db: str,
        source_host: str
    ) -> bool:
        """Check if we should do incremental sync or full migration"""
        last_migration = self.get_last_migration(table_name, source_db)
        
        if not last_migration:
            return False
        
        # Check if source is the same
        if last_migration.get("source_host") != source_host:
            return False
        
        return True
