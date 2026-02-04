"""
Real-time metrics tracking and dashboard
Feature #17: Show different metrics of migrations
"""
import time
import logging
from typing import Dict, Optional, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from threading import Lock

logger = logging.getLogger(__name__)


@dataclass
class TableMetrics:
    """Metrics for a single table migration"""
    table_name: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str = "in_progress"  # in_progress, completed, failed
    rows_processed: int = 0
    total_rows: int = 0
    bytes_processed: int = 0
    error_message: Optional[str] = None
    
    @property
    def duration_seconds(self) -> float:
        """Get duration in seconds"""
        end_time = self.completed_at or datetime.now()
        return (end_time - self.started_at).total_seconds()
    
    @property
    def rows_per_second(self) -> float:
        """Calculate rows processed per second"""
        duration = self.duration_seconds
        if duration > 0:
            return self.rows_processed / duration
        return 0.0
    
    @property
    def progress_percentage(self) -> float:
        """Calculate progress percentage"""
        if self.total_rows > 0:
            return (self.rows_processed / self.total_rows) * 100
        return 0.0
    
    @property
    def eta_seconds(self) -> Optional[float]:
        """Estimate time remaining in seconds"""
        if self.total_rows > 0 and self.rows_processed > 0:
            rate = self.rows_per_second
            if rate > 0:
                remaining_rows = self.total_rows - self.rows_processed
                return remaining_rows / rate
        return None


@dataclass
class MigrationMetrics:
    """Overall migration metrics"""
    migration_id: str
    migration_type: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str = "in_progress"
    
    total_tables: int = 0
    tables_completed: int = 0
    tables_failed: int = 0
    
    total_rows: int = 0
    rows_processed: int = 0
    
    total_bytes: int = 0
    bytes_processed: int = 0
    
    table_metrics: Dict[str, TableMetrics] = field(default_factory=dict)
    
    source_db: str = ""
    dest_db: str = ""
    target_schema: str = ""
    
    @property
    def duration_seconds(self) -> float:
        """Get total duration in seconds"""
        end_time = self.completed_at or datetime.now()
        return (end_time - self.started_at).total_seconds()
    
    @property
    def overall_progress_percentage(self) -> float:
        """Calculate overall progress percentage"""
        if self.total_tables > 0:
            return (self.tables_completed / self.total_tables) * 100
        return 0.0
    
    @property
    def rows_per_second(self) -> float:
        """Calculate overall rows per second"""
        duration = self.duration_seconds
        if duration > 0:
            return self.rows_processed / duration
        return 0.0
    
    @property
    def mbps(self) -> float:
        """Calculate megabytes per second"""
        duration = self.duration_seconds
        if duration > 0:
            return (self.bytes_processed / (1024 * 1024)) / duration
        return 0.0
    
    @property
    def eta_seconds(self) -> Optional[float]:
        """Estimate time remaining in seconds"""
        if self.total_tables > 0 and self.tables_completed > 0:
            rate = self.tables_completed / self.duration_seconds
            if rate > 0:
                remaining_tables = self.total_tables - self.tables_completed
                return remaining_tables / rate
        return None
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        completed = self.tables_completed + self.tables_failed
        if completed > 0:
            return (self.tables_completed / completed) * 100
        return 0.0


class MetricsTracker:
    """Track and manage migration metrics in real-time"""
    
    def __init__(self):
        self.current_migration: Optional[MigrationMetrics] = None
        self.migration_history: List[MigrationMetrics] = []
        self._lock = Lock()
    
    def start_migration(
        self,
        migration_id: str,
        migration_type: str,
        total_tables: int,
        source_db: str = "",
        dest_db: str = "",
        target_schema: str = ""
    ) -> MigrationMetrics:
        """Start tracking a new migration"""
        with self._lock:
            self.current_migration = MigrationMetrics(
                migration_id=migration_id,
                migration_type=migration_type,
                started_at=datetime.now(),
                total_tables=total_tables,
                source_db=source_db,
                dest_db=dest_db,
                target_schema=target_schema
            )
            logger.info(f"Started metrics tracking for migration {migration_id}")
            return self.current_migration
    
    def start_table(self, table_name: str, total_rows: int = 0) -> TableMetrics:
        """Start tracking a table migration"""
        with self._lock:
            if not self.current_migration:
                raise RuntimeError("No active migration")
            
            table_metrics = TableMetrics(
                table_name=table_name,
                started_at=datetime.now(),
                total_rows=total_rows
            )
            self.current_migration.table_metrics[table_name] = table_metrics
            self.current_migration.total_rows += total_rows
            
            logger.debug(f"Started tracking table {table_name}")
            return table_metrics
    
    def update_table_progress(self, table_name: str, rows_processed: int, bytes_processed: int = 0):
        """Update progress for a table"""
        with self._lock:
            if not self.current_migration:
                return
            
            table_metrics = self.current_migration.table_metrics.get(table_name)
            if table_metrics:
                # Calculate delta
                rows_delta = rows_processed - table_metrics.rows_processed
                bytes_delta = bytes_processed - table_metrics.bytes_processed
                
                # Update table metrics
                table_metrics.rows_processed = rows_processed
                table_metrics.bytes_processed = bytes_processed
                
                # Update overall metrics
                self.current_migration.rows_processed += rows_delta
                self.current_migration.bytes_processed += bytes_delta
    
    def complete_table(
        self,
        table_name: str,
        success: bool = True,
        error_message: Optional[str] = None
    ):
        """Mark a table as completed"""
        with self._lock:
            if not self.current_migration:
                return
            
            table_metrics = self.current_migration.table_metrics.get(table_name)
            if table_metrics:
                table_metrics.completed_at = datetime.now()
                table_metrics.status = "completed" if success else "failed"
                table_metrics.error_message = error_message
                
                if success:
                    self.current_migration.tables_completed += 1
                else:
                    self.current_migration.tables_failed += 1
                
                logger.info(f"Table {table_name} marked as {table_metrics.status}")
    
    def complete_migration(self, success: bool = True):
        """Mark the migration as completed"""
        with self._lock:
            if self.current_migration:
                self.current_migration.completed_at = datetime.now()
                self.current_migration.status = "completed" if success else "failed"
                
                # Add to history
                self.migration_history.append(self.current_migration)
                
                logger.info(f"Migration {self.current_migration.migration_id} completed")
                self.current_migration = None
    
    def get_current_metrics(self) -> Optional[MigrationMetrics]:
        """Get current migration metrics"""
        with self._lock:
            return self.current_migration
    
    def get_snapshot(self) -> Optional[Dict]:
        """Get a snapshot of current metrics as a dictionary"""
        with self._lock:
            if not self.current_migration:
                return None
            
            m = self.current_migration
            
            # Format ETA
            eta_str = "N/A"
            if m.eta_seconds:
                eta_td = timedelta(seconds=int(m.eta_seconds))
                eta_str = str(eta_td)
            
            # Format duration
            duration_td = timedelta(seconds=int(m.duration_seconds))
            
            # Get active tables
            active_tables = []
            for table_name, tm in m.table_metrics.items():
                if tm.status == "in_progress":
                    table_eta = "N/A"
                    if tm.eta_seconds:
                        table_eta = str(timedelta(seconds=int(tm.eta_seconds)))
                    
                    active_tables.append({
                        "name": table_name,
                        "progress": f"{tm.progress_percentage:.1f}%",
                        "rows": f"{tm.rows_processed:,} / {tm.total_rows:,}",
                        "rate": f"{tm.rows_per_second:.1f} rows/sec",
                        "eta": table_eta
                    })
            
            return {
                "migration_id": m.migration_id,
                "type": m.migration_type,
                "status": m.status,
                "duration": str(duration_td),
                "progress": {
                    "tables": f"{m.tables_completed} / {m.total_tables}",
                    "percentage": f"{m.overall_progress_percentage:.1f}%",
                    "rows": f"{m.rows_processed:,} / {m.total_rows:,}",
                },
                "performance": {
                    "rows_per_second": f"{m.rows_per_second:.1f}",
                    "mbps": f"{m.mbps:.2f}",
                    "success_rate": f"{m.success_rate:.1f}%"
                },
                "eta": eta_str,
                "databases": {
                    "source": m.source_db,
                    "destination": m.dest_db,
                    "schema": m.target_schema
                },
                "active_tables": active_tables,
                "completed_tables": m.tables_completed,
                "failed_tables": m.tables_failed
            }
    
    def get_formatted_status(self) -> str:
        """Get formatted status string for display"""
        snapshot = self.get_snapshot()
        if not snapshot:
            return "No active migration"
        
        lines = []
        lines.append("=" * 80)
        lines.append(f"Migration: {snapshot['migration_id']} ({snapshot['type']})")
        lines.append(f"Status: {snapshot['status']} | Duration: {snapshot['duration']} | ETA: {snapshot['eta']}")
        lines.append("-" * 80)
        lines.append(f"Tables: {snapshot['progress']['tables']} ({snapshot['progress']['percentage']})")
        lines.append(f"Rows: {snapshot['progress']['rows']}")
        lines.append(f"Performance: {snapshot['performance']['rows_per_second']} rows/sec | {snapshot['performance']['mbps']} MB/s")
        lines.append(f"Success Rate: {snapshot['performance']['success_rate']}")
        lines.append(f"Schema: {snapshot['databases']['source']} → {snapshot['databases']['schema']}")
        
        if snapshot['active_tables']:
            lines.append("-" * 80)
            lines.append("Active Tables:")
            for table in snapshot['active_tables']:
                lines.append(f"  • {table['name']}: {table['progress']} | {table['rate']} | ETA: {table['eta']}")
        
        if snapshot['failed_tables'] > 0:
            lines.append("-" * 80)
            lines.append(f"⚠️  Failed Tables: {snapshot['failed_tables']}")
        
        lines.append("=" * 80)
        
        return "\n".join(lines)
    
    def get_history_summary(self) -> List[Dict]:
        """Get summary of historical migrations"""
        with self._lock:
            summaries = []
            for m in self.migration_history:
                summaries.append({
                    "migration_id": m.migration_id,
                    "type": m.migration_type,
                    "started_at": m.started_at.isoformat(),
                    "duration": str(timedelta(seconds=int(m.duration_seconds))),
                    "status": m.status,
                    "tables": f"{m.tables_completed}/{m.total_tables}",
                    "rows": f"{m.rows_processed:,}",
                    "avg_speed": f"{m.rows_per_second:.1f} rows/sec",
                    "success_rate": f"{m.success_rate:.1f}%"
                })
            return summaries


# Global metrics tracker instance
_global_tracker = MetricsTracker()


def get_tracker() -> MetricsTracker:
    """Get the global metrics tracker instance"""
    return _global_tracker