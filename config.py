"""
Configuration management for PostgreSQL Migration Tool
"""
from dataclasses import dataclass
from typing import Optional
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass
class DBConfig:
    """Database configuration"""
    host: str
    port: str
    database: str
    user: str
    password: str
    
    @classmethod
    def from_env(cls, prefix: str) -> 'DBConfig':
        """Load configuration from environment variables"""
        return cls(
            host=os.getenv(f"{prefix}_HOST", "localhost"),
            port=os.getenv(f"{prefix}_PORT", "6000"),
            database=os.getenv(f"{prefix}_DATABASE", ""),
            user=os.getenv(f"{prefix}_USER", "postgres"),
            password=os.getenv(f"{prefix}_PASSWORD", "")
        )
    
    def to_connection_string(self) -> str:
        """Convert to PostgreSQL connection string"""
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class MigrationConfig:
    """Migration operation configuration"""
    batch_size: int = 1000
    log_level: str = "INFO"
    enable_foreign_keys: bool = True
    enable_indexes: bool = True
    target_schema: str = "migrated"
    schema_naming_mode: str = "descriptive"  # "descriptive", "abbreviated", "timestamp", "custom"
    custom_schema_name: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> 'MigrationConfig':
        """Load migration config from environment"""
        return cls(
            batch_size=int(os.getenv("MIGRATION_BATCH_SIZE", "1000")),
            log_level=os.getenv("MIGRATION_LOG_LEVEL", "INFO"),
            enable_foreign_keys=os.getenv("MIGRATION_ENABLE_FK", "true").lower() == "true",
            enable_indexes=os.getenv("MIGRATION_ENABLE_INDEXES", "true").lower() == "true",
            target_schema=os.getenv("MIGRATION_TARGET_SCHEMA", "migrated"),
            schema_naming_mode=os.getenv("MIGRATION_SCHEMA_NAMING", "descriptive")
        )
    
    def get_schema_name(self, migration_type: str, source_db: str = "") -> str:
        """
        Generate schema name based on migration type and naming mode
        
        migration_type: 'full', 'incremental', 'table_recreate', 'table_delta', 'advanced'
        """
        if self.custom_schema_name:
            return self.custom_schema_name
        
        mode_map = {
            "full": ("full_copy", "fc"),
            "incremental": ("incremental_sync", "inc"),
            "table_recreate": ("table_recreate", "tr"),
            "table_delta": ("table_delta", "td"),
            "advanced": ("advanced_ops", "adv")
        }
        
        base_name = "migrated"
        
        if self.schema_naming_mode == "descriptive":
            suffix = mode_map.get(migration_type, (migration_type, migration_type))[0]
            return f"{base_name}_{suffix}"
        elif self.schema_naming_mode == "abbreviated":
            suffix = mode_map.get(migration_type, (migration_type, migration_type))[1]
            return f"{base_name}_{suffix}"
        elif self.schema_naming_mode == "timestamp":
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            suffix = mode_map.get(migration_type, (migration_type, migration_type))[1]
            return f"{base_name}_{suffix}_{timestamp}"
        elif self.schema_naming_mode == "source_db":
            suffix = mode_map.get(migration_type, (migration_type, migration_type))[1]
            db_abbrev = source_db[:4] if source_db else "db"
            return f"{base_name}_{suffix}_{db_abbrev}"
        else:
            return self.target_schema