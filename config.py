"""
Configuration management for PostgreSQL Migration Tool
"""
from dataclasses import dataclass
from typing import Optional
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
            port=os.getenv(f"{prefix}_PORT", "5432"),
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
    
    @classmethod
    def from_env(cls) -> 'MigrationConfig':
        """Load migration config from environment"""
        return cls(
            batch_size=int(os.getenv("MIGRATION_BATCH_SIZE", "1000")),
            log_level=os.getenv("MIGRATION_LOG_LEVEL", "INFO"),
            enable_foreign_keys=os.getenv("MIGRATION_ENABLE_FK", "true").lower() == "true",
            enable_indexes=os.getenv("MIGRATION_ENABLE_INDEXES", "true").lower() == "true",
            target_schema=os.getenv("MIGRATION_TARGET_SCHEMA", "migrated")
        )
