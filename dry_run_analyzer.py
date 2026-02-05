"""
Dry Run Analyzer - Simulates migration operations without execution
Feature #2: Interactive Migration Playground - Dry Run Visualization

This module analyzes migration plans and generates comprehensive previews
including impact analysis, dependency graphs, and risk assessments.
"""
import logging
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from sqlalchemy.engine import Engine

from database import (
    fetch_tables, fetch_columns, fetch_primary_key,
    fetch_foreign_keys, fetch_indexes, get_row_count,
    table_exists
)
from schema_manager import is_auto_generated_column

logger = logging.getLogger(__name__)


class RiskLevel(Enum):
    """Risk levels for migration operations"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class TableDependency:
    """Represents a foreign key dependency between tables"""
    source_table: str
    target_table: str
    constraint_name: str
    source_column: str
    target_column: str


@dataclass
class TableImpact:
    """Impact analysis for a single table"""
    table_name: str
    action: str  # 'create', 'drop', 'recreate', 'insert', 'update', 'delete'
    
    # Current state
    exists_in_source: bool = False
    exists_in_destination: bool = False
    source_row_count: int = 0
    dest_row_count: int = 0
    
    # Projected changes
    rows_to_insert: int = 0
    rows_to_update: int = 0
    rows_to_delete: int = 0
    estimated_size_mb: float = 0.0
    estimated_duration_seconds: float = 0.0
    
    # Schema changes
    columns_to_add: List[str] = field(default_factory=list)
    columns_to_remove: List[str] = field(default_factory=list)
    columns_to_modify: List[str] = field(default_factory=list)
    
    # Dependencies
    dependencies: List[str] = field(default_factory=list)
    dependents: List[str] = field(default_factory=list)
    
    # Risk assessment
    risk_level: RiskLevel = RiskLevel.LOW
    risk_factors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    @property
    def has_schema_changes(self) -> bool:
        """Check if table has schema changes"""
        return bool(
            self.columns_to_add or 
            self.columns_to_remove or 
            self.columns_to_modify
        )
    
    @property
    def net_row_change(self) -> int:
        """Calculate net change in row count"""
        return self.rows_to_insert - self.rows_to_delete


@dataclass
class DryRunResult:
    """Complete dry run analysis result"""
    migration_type: str
    timestamp: datetime
    source_db: str
    dest_db: str
    source_schema: str
    target_schema: str
    
    # Overall statistics
    total_tables: int = 0
    tables_to_create: int = 0
    tables_to_drop: int = 0
    tables_to_modify: int = 0
    tables_unchanged: int = 0
    
    total_rows_to_process: int = 0
    estimated_total_size_mb: float = 0.0
    estimated_total_duration_seconds: float = 0.0
    
    # Detailed impact per table
    table_impacts: Dict[str, TableImpact] = field(default_factory=dict)
    
    # Dependency analysis
    table_dependencies: List[TableDependency] = field(default_factory=list)
    execution_order: List[str] = field(default_factory=list)
    circular_dependencies: List[List[str]] = field(default_factory=list)
    
    # Risk assessment
    overall_risk_level: RiskLevel = RiskLevel.LOW
    critical_warnings: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    
    # Resource requirements
    estimated_memory_mb: float = 0.0
    estimated_disk_space_mb: float = 0.0
    concurrent_connections_needed: int = 2
    
    @property
    def is_safe_to_execute(self) -> bool:
        """Determine if migration is safe to execute"""
        return (
            self.overall_risk_level != RiskLevel.CRITICAL and
            len(self.circular_dependencies) == 0 and
            len(self.critical_warnings) == 0
        )
    
    @property
    def summary(self) -> Dict:
        """Get summary statistics"""
        return {
            "migration_type": self.migration_type,
            "timestamp": self.timestamp.isoformat(),
            "databases": {
                "source": f"{self.source_db}.{self.source_schema}",
                "destination": f"{self.dest_db}.{self.target_schema}"
            },
            "tables": {
                "total": self.total_tables,
                "to_create": self.tables_to_create,
                "to_drop": self.tables_to_drop,
                "to_modify": self.tables_to_modify,
                "unchanged": self.tables_unchanged
            },
            "data": {
                "total_rows": self.total_rows_to_process,
                "estimated_size_mb": round(self.estimated_total_size_mb, 2),
            },
            "performance": {
                "estimated_duration": f"{self.estimated_total_duration_seconds:.0f}s",
                "estimated_memory_mb": round(self.estimated_memory_mb, 2),
                "estimated_disk_mb": round(self.estimated_disk_space_mb, 2)
            },
            "risk": {
                "level": self.overall_risk_level.value,
                "is_safe": self.is_safe_to_execute,
                "critical_warnings": len(self.critical_warnings),
                "circular_dependencies": len(self.circular_dependencies)
            }
        }


class DryRunAnalyzer:
    """
    Analyzes and simulates migration operations without execution.
    
    This provides users with a comprehensive preview of what will happen
    during a migration, including impact analysis, risk assessment, and
    dependency visualization.
    """
    
    # Performance estimation constants (rows per second)
    INSERT_RATE = 5000  # Average inserts per second
    UPDATE_RATE = 3000  # Average updates per second
    DELETE_RATE = 8000  # Average deletes per second
    AVG_ROW_SIZE_BYTES = 512  # Average row size for estimation
    
    def __init__(
        self,
        src_engine: Engine,
        dst_engine: Engine,
        src_schema: str = "public",
        target_schema: str = "migrated"
    ):
        self.src_engine = src_engine
        self.dst_engine = dst_engine
        self.src_schema = src_schema
        self.target_schema = target_schema
    
    def analyze_full_migration(
        self,
        exclude_auto_generated: bool = False
    ) -> DryRunResult:
        """
        Analyze a full migration (drop and recreate all tables).
        
        Args:
            exclude_auto_generated: Whether to exclude auto-generated columns
            
        Returns:
            DryRunResult with comprehensive analysis
        """
        logger.info("Starting dry run analysis for full migration")
        
        result = DryRunResult(
            migration_type="full_migration",
            timestamp=datetime.now(),
            source_db=self.src_engine.url.database,
            dest_db=self.dst_engine.url.database,
            source_schema=self.src_schema,
            target_schema=self.target_schema
        )
        
        # Fetch all source tables
        src_tables = fetch_tables(self.src_engine, self.src_schema)
        result.total_tables = len(src_tables)
        
        # Analyze each table
        for table in src_tables:
            impact = self._analyze_table_for_full_migration(
                table, exclude_auto_generated
            )
            result.table_impacts[table] = impact
            
            # Update overall statistics
            if impact.action == "recreate":
                result.tables_to_modify += 1
            elif impact.action == "create":
                result.tables_to_create += 1
            
            result.total_rows_to_process += impact.rows_to_insert
            result.estimated_total_size_mb += impact.estimated_size_mb
            result.estimated_total_duration_seconds += impact.estimated_duration_seconds
        
        # Analyze dependencies
        result.table_dependencies = self._analyze_dependencies(src_tables)
        result.execution_order = self._determine_execution_order(
            src_tables, result.table_dependencies
        )
        result.circular_dependencies = self._detect_circular_dependencies(
            result.table_dependencies
        )
        
        # Assess risks
        self._assess_overall_risk(result)
        
        # Calculate resource requirements
        self._calculate_resource_requirements(result)
        
        logger.info(f"Dry run analysis complete: {result.total_tables} tables, "
                   f"{result.total_rows_to_process} rows")
        
        return result
    
    def analyze_incremental_sync(self) -> DryRunResult:
        """
        Analyze an incremental sync (copy only new/changed data).
        
        Returns:
            DryRunResult with comprehensive analysis
        """
        logger.info("Starting dry run analysis for incremental sync")
        
        result = DryRunResult(
            migration_type="incremental_sync",
            timestamp=datetime.now(),
            source_db=self.src_engine.url.database,
            dest_db=self.dst_engine.url.database,
            source_schema=self.src_schema,
            target_schema=self.target_schema
        )
        
        # Fetch source tables
        src_tables = fetch_tables(self.src_engine, self.src_schema)
        result.total_tables = len(src_tables)
        
        # Analyze each table
        for table in src_tables:
            impact = self._analyze_table_for_incremental_sync(table)
            result.table_impacts[table] = impact
            
            # Update overall statistics
            if not impact.exists_in_destination:
                result.tables_to_create += 1
            elif impact.has_schema_changes or impact.rows_to_insert > 0:
                result.tables_to_modify += 1
            else:
                result.tables_unchanged += 1
            
            result.total_rows_to_process += impact.rows_to_insert
            result.estimated_total_size_mb += impact.estimated_size_mb
            result.estimated_total_duration_seconds += impact.estimated_duration_seconds
        
        # Analyze dependencies
        result.table_dependencies = self._analyze_dependencies(src_tables)
        result.execution_order = self._determine_execution_order(
            src_tables, result.table_dependencies
        )
        
        # Assess risks
        self._assess_overall_risk(result)
        
        # Calculate resource requirements
        self._calculate_resource_requirements(result)
        
        logger.info(f"Dry run analysis complete: {result.tables_to_modify} tables to sync")
        
        return result
    
    def analyze_single_table(
        self,
        table: str,
        mode: str = "recreate",
        exclude_auto_generated: bool = False
    ) -> TableImpact:
        """
        Analyze a single table migration.
        
        Args:
            table: Table name
            mode: 'recreate' or 'delta'
            exclude_auto_generated: Whether to exclude auto-generated columns
            
        Returns:
            TableImpact with detailed analysis
        """
        if mode == "recreate":
            return self._analyze_table_for_full_migration(table, exclude_auto_generated)
        else:
            return self._analyze_table_for_incremental_sync(table)
    
    def _analyze_table_for_full_migration(
        self,
        table: str,
        exclude_auto_generated: bool
    ) -> TableImpact:
        """Analyze a table for full migration (drop and recreate)"""
        impact = TableImpact(
            table_name=table,
            action="recreate",
            exists_in_source=True
        )
        
        try:
            # Check if table exists in destination
            impact.exists_in_destination = table_exists(
                self.dst_engine, self.target_schema, table
            )
            
            # Get row counts
            impact.source_row_count = get_row_count(
                self.src_engine, table, self.src_schema
            )
            
            if impact.exists_in_destination:
                impact.dest_row_count = get_row_count(
                    self.dst_engine, table, self.target_schema
                )
            
            # Get columns
            src_cols = fetch_columns(self.src_engine, self.src_schema, table)
            
            if exclude_auto_generated:
                src_cols = [c for c in src_cols if not is_auto_generated_column(c)]
            
            # All rows will be inserted (table is recreated)
            impact.rows_to_insert = impact.source_row_count
            impact.rows_to_delete = impact.dest_row_count
            
            # Estimate size and duration
            impact.estimated_size_mb = (
                impact.source_row_count * self.AVG_ROW_SIZE_BYTES
            ) / (1024 * 1024)
            
            impact.estimated_duration_seconds = (
                impact.source_row_count / self.INSERT_RATE
            ) + 2  # Add overhead for schema creation
            
            # Analyze dependencies
            fks = fetch_foreign_keys(self.src_engine, self.src_schema, table)
            impact.dependencies = [fk['foreign_table_name'] for fk in fks]
            
            # Assess risks
            self._assess_table_risk(impact)
            
        except Exception as e:
            logger.error(f"Error analyzing table {table}: {str(e)}")
            impact.warnings.append(f"Analysis error: {str(e)}")
            impact.risk_level = RiskLevel.HIGH
        
        return impact
    
    def _analyze_table_for_incremental_sync(self, table: str) -> TableImpact:
        """Analyze a table for incremental sync"""
        impact = TableImpact(
            table_name=table,
            action="insert",
            exists_in_source=True
        )
        
        try:
            # Check if table exists in destination
            impact.exists_in_destination = table_exists(
                self.dst_engine, self.target_schema, table
            )
            
            # Get row counts
            impact.source_row_count = get_row_count(
                self.src_engine, table, self.src_schema
            )
            
            if impact.exists_in_destination:
                impact.dest_row_count = get_row_count(
                    self.dst_engine, table, self.target_schema
                )
                
                # Estimate new rows (simplified - actual detection is complex)
                # In real scenario, would use fingerprinting or PK comparison
                impact.rows_to_insert = max(
                    0, impact.source_row_count - impact.dest_row_count
                )
            else:
                # Table doesn't exist, all rows will be inserted
                impact.rows_to_insert = impact.source_row_count
                impact.action = "create"
            
            # Estimate size and duration
            impact.estimated_size_mb = (
                impact.rows_to_insert * self.AVG_ROW_SIZE_BYTES
            ) / (1024 * 1024)
            
            impact.estimated_duration_seconds = (
                impact.rows_to_insert / self.INSERT_RATE
            ) + 1  # Add overhead
            
            # Analyze schema changes if table exists
            if impact.exists_in_destination:
                self._analyze_schema_changes(impact)
            
            # Assess risks
            self._assess_table_risk(impact)
            
        except Exception as e:
            logger.error(f"Error analyzing table {table}: {str(e)}")
            impact.warnings.append(f"Analysis error: {str(e)}")
            impact.risk_level = RiskLevel.HIGH
        
        return impact
    
    def _analyze_schema_changes(self, impact: TableImpact):
        """Analyze schema changes between source and destination"""
        try:
            src_cols = {
                c['column_name']: c 
                for c in fetch_columns(
                    self.src_engine, self.src_schema, impact.table_name
                )
            }
            dst_cols = {
                c['column_name']: c 
                for c in fetch_columns(
                    self.dst_engine, self.target_schema, impact.table_name
                )
            }
            
            # Columns only in source
            impact.columns_to_add = list(src_cols.keys() - dst_cols.keys())
            
            # Columns only in destination
            impact.columns_to_remove = list(dst_cols.keys() - src_cols.keys())
            
            # Columns with different types
            for col_name in src_cols.keys() & dst_cols.keys():
                if src_cols[col_name]['data_type'] != dst_cols[col_name]['data_type']:
                    impact.columns_to_modify.append(col_name)
            
        except Exception as e:
            logger.warning(f"Could not analyze schema changes for {impact.table_name}: {e}")
    
    def _analyze_dependencies(self, tables: List[str]) -> List[TableDependency]:
        """Analyze foreign key dependencies between tables"""
        dependencies = []
        
        for table in tables:
            try:
                fks = fetch_foreign_keys(self.src_engine, self.src_schema, table)
                for fk in fks:
                    dependencies.append(TableDependency(
                        source_table=table,
                        target_table=fk['foreign_table_name'],
                        constraint_name=fk['constraint_name'],
                        source_column=fk['column_name'],
                        target_column=fk['foreign_column_name']
                    ))
            except Exception as e:
                logger.warning(f"Could not fetch FKs for {table}: {e}")
        
        return dependencies
    
    def _determine_execution_order(
        self,
        tables: List[str],
        dependencies: List[TableDependency]
    ) -> List[str]:
        """
        Determine optimal execution order based on dependencies.
        Uses topological sort algorithm.
        """
        # Build dependency graph
        graph = {table: set() for table in tables}
        in_degree = {table: 0 for table in tables}
        
        for dep in dependencies:
            if dep.source_table in graph and dep.target_table in graph:
                graph[dep.target_table].add(dep.source_table)
                in_degree[dep.source_table] += 1
        
        # Topological sort (Kahn's algorithm)
        queue = [table for table in tables if in_degree[table] == 0]
        execution_order = []
        
        while queue:
            # Sort for deterministic output
            queue.sort()
            table = queue.pop(0)
            execution_order.append(table)
            
            for dependent in graph[table]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        
        # If not all tables processed, there are circular dependencies
        if len(execution_order) < len(tables):
            # Add remaining tables (with circular deps) at the end
            remaining = set(tables) - set(execution_order)
            execution_order.extend(sorted(remaining))
        
        return execution_order
    
    def _detect_circular_dependencies(
        self,
        dependencies: List[TableDependency]
    ) -> List[List[str]]:
        """Detect circular dependencies in the dependency graph"""
        # Build adjacency list
        graph = {}
        for dep in dependencies:
            if dep.source_table not in graph:
                graph[dep.source_table] = []
            graph[dep.source_table].append(dep.target_table)
        
        cycles = []
        visited = set()
        rec_stack = set()
        
        def dfs(node, path):
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            if node in graph:
                for neighbor in graph[node]:
                    if neighbor not in visited:
                        dfs(neighbor, path.copy())
                    elif neighbor in rec_stack:
                        # Found a cycle
                        cycle_start = path.index(neighbor)
                        cycle = path[cycle_start:] + [neighbor]
                        if cycle not in cycles:
                            cycles.append(cycle)
            
            rec_stack.remove(node)
        
        for node in graph:
            if node not in visited:
                dfs(node, [])
        
        return cycles
    
    def _assess_table_risk(self, impact: TableImpact):
        """Assess risk level for a table migration"""
        risk_factors = []
        
        # Large table risk
        if impact.source_row_count > 1_000_000:
            risk_factors.append("Large table (>1M rows)")
            impact.risk_level = RiskLevel.MEDIUM
        
        if impact.source_row_count > 10_000_000:
            risk_factors.append("Very large table (>10M rows)")
            impact.risk_level = RiskLevel.HIGH
        
        # Schema changes risk
        if impact.columns_to_remove:
            risk_factors.append(f"Removing {len(impact.columns_to_remove)} columns")
            impact.risk_level = max(impact.risk_level, RiskLevel.MEDIUM)
        
        if impact.columns_to_modify:
            risk_factors.append(f"Modifying {len(impact.columns_to_modify)} column types")
            impact.risk_level = max(impact.risk_level, RiskLevel.HIGH)
        
        # Dependencies risk
        if len(impact.dependencies) > 5:
            risk_factors.append(f"Many dependencies ({len(impact.dependencies)} tables)")
            impact.risk_level = max(impact.risk_level, RiskLevel.MEDIUM)
        
        # Data loss risk
        if impact.rows_to_delete > 0:
            risk_factors.append(f"Will delete {impact.rows_to_delete} existing rows")
            impact.risk_level = max(impact.risk_level, RiskLevel.HIGH)
        
        impact.risk_factors = risk_factors
    
    def _assess_overall_risk(self, result: DryRunResult):
        """Assess overall migration risk"""
        # Find highest risk level among tables
        risk_levels = [
            impact.risk_level 
            for impact in result.table_impacts.values()
        ]
        
        if RiskLevel.CRITICAL in risk_levels:
            result.overall_risk_level = RiskLevel.CRITICAL
        elif RiskLevel.HIGH in risk_levels:
            result.overall_risk_level = RiskLevel.HIGH
        elif RiskLevel.MEDIUM in risk_levels:
            result.overall_risk_level = RiskLevel.MEDIUM
        else:
            result.overall_risk_level = RiskLevel.LOW
        
        # Add critical warnings
        if result.circular_dependencies:
            result.critical_warnings.append(
                f"Circular dependencies detected in {len(result.circular_dependencies)} cycles"
            )
            result.overall_risk_level = RiskLevel.CRITICAL
        
        if result.total_rows_to_process > 50_000_000:
            result.critical_warnings.append(
                f"Very large migration: {result.total_rows_to_process:,} rows"
            )
            result.overall_risk_level = max(
                result.overall_risk_level, RiskLevel.HIGH
            )
        
        # Add recommendations
        if result.estimated_total_duration_seconds > 3600:
            result.recommendations.append(
                "Consider running migration during maintenance window "
                f"(estimated {result.estimated_total_duration_seconds/3600:.1f} hours)"
            )
        
        if result.estimated_disk_space_mb > 10000:
            result.recommendations.append(
                f"Ensure sufficient disk space: ~{result.estimated_disk_space_mb:.0f} MB required"
            )
        
        if result.tables_to_drop > 0:
            result.recommendations.append(
                f"Back up data before migration: {result.tables_to_drop} tables will be dropped"
            )
    
    def _calculate_resource_requirements(self, result: DryRunResult):
        """Calculate estimated resource requirements"""
        # Memory: Assume batch processing with typical batch size
        batch_size = 1000
        result.estimated_memory_mb = (
            batch_size * self.AVG_ROW_SIZE_BYTES * result.total_tables
        ) / (1024 * 1024)
        
        # Disk space: Total data size plus overhead (indexes, temp space)
        result.estimated_disk_space_mb = result.estimated_total_size_mb * 1.5
        
        # Connections: One for source, one for destination
        result.concurrent_connections_needed = 2