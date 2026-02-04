"""
Schema comparison and difference detection
Feature #3: Show differences between source and destination schemas
"""
import logging
from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass
from sqlalchemy.engine import Engine

from database import (
    fetch_tables, fetch_columns, fetch_primary_key,
    fetch_foreign_keys, fetch_indexes, fetch_enum_types,
    table_exists, get_row_count
)

logger = logging.getLogger(__name__)


@dataclass
class ColumnDifference:
    """Represents a difference in column definition"""
    column_name: str
    difference_type: str  # 'missing_in_dest', 'missing_in_source', 'type_mismatch', 'nullable_mismatch'
    source_definition: Optional[Dict] = None
    dest_definition: Optional[Dict] = None
    details: str = ""


@dataclass
class TableDifference:
    """Represents differences for a single table"""
    table_name: str
    status: str  # 'identical', 'schema_diff', 'missing_in_dest', 'missing_in_source', 'row_count_diff'
    source_row_count: int = 0
    dest_row_count: int = 0
    column_differences: List[ColumnDifference] = None
    missing_indexes: List[str] = None
    missing_foreign_keys: List[str] = None
    
    def __post_init__(self):
        if self.column_differences is None:
            self.column_differences = []
        if self.missing_indexes is None:
            self.missing_indexes = []
        if self.missing_foreign_keys is None:
            self.missing_foreign_keys = []
    
    @property
    def has_differences(self) -> bool:
        """Check if table has any differences"""
        return (
            self.status != 'identical' or
            len(self.column_differences) > 0 or
            len(self.missing_indexes) > 0 or
            len(self.missing_foreign_keys) > 0 or
            self.source_row_count != self.dest_row_count
        )


@dataclass
class SchemaDifference:
    """Complete schema comparison result"""
    tables_only_in_source: List[str]
    tables_only_in_dest: List[str]
    tables_in_both: List[str]
    table_differences: List[TableDifference]
    enum_differences: Dict[str, str]  # enum_name -> difference description
    
    @property
    def has_differences(self) -> bool:
        """Check if schemas have any differences"""
        return (
            len(self.tables_only_in_source) > 0 or
            len(self.tables_only_in_dest) > 0 or
            any(td.has_differences for td in self.table_differences) or
            len(self.enum_differences) > 0
        )
    
    @property
    def summary(self) -> Dict:
        """Get summary statistics"""
        return {
            "tables_only_in_source": len(self.tables_only_in_source),
            "tables_only_in_dest": len(self.tables_only_in_dest),
            "tables_with_differences": sum(1 for td in self.table_differences if td.has_differences),
            "tables_identical": sum(1 for td in self.table_differences if not td.has_differences),
            "total_tables_compared": len(self.table_differences),
            "enum_differences": len(self.enum_differences)
        }


class SchemaComparator:
    """Compare schemas between source and destination databases"""
    
    def __init__(self, src_engine: Engine, dst_engine: Engine,
                 src_schema: str = "public", dst_schema: str = "migrated"):
        self.src_engine = src_engine
        self.dst_engine = dst_engine
        self.src_schema = src_schema
        self.dst_schema = dst_schema
    
    def compare_schemas(self, include_row_counts: bool = True) -> SchemaDifference:
        """
        Perform comprehensive schema comparison
        """
        logger.info(f"Comparing schemas: {self.src_schema} vs {self.dst_schema}")
        
        # Fetch table lists
        src_tables = set(fetch_tables(self.src_engine, self.src_schema))
        dst_tables = set(fetch_tables(self.dst_engine, self.dst_schema))
        
        # Categorize tables
        tables_only_in_source = sorted(src_tables - dst_tables)
        tables_only_in_dest = sorted(dst_tables - src_tables)
        tables_in_both = sorted(src_tables & dst_tables)
        
        # Compare common tables
        table_differences = []
        for table in tables_in_both:
            table_diff = self._compare_table(table, include_row_counts)
            table_differences.append(table_diff)
        
        # Compare ENUM types
        enum_differences = self._compare_enums()
        
        result = SchemaDifference(
            tables_only_in_source=tables_only_in_source,
            tables_only_in_dest=tables_only_in_dest,
            tables_in_both=tables_in_both,
            table_differences=table_differences,
            enum_differences=enum_differences
        )
        
        logger.info(f"Schema comparison complete. Summary: {result.summary}")
        return result
    
    def _compare_table(self, table: str, include_row_counts: bool) -> TableDifference:
        """Compare a single table between source and destination"""
        # Fetch columns
        src_cols = {col['column_name']: col for col in fetch_columns(self.src_engine, self.src_schema, table)}
        dst_cols = {col['column_name']: col for col in fetch_columns(self.dst_engine, self.dst_schema, table)}
        
        # Compare columns
        column_differences = []
        
        # Columns only in source
        for col_name in src_cols.keys() - dst_cols.keys():
            column_differences.append(ColumnDifference(
                column_name=col_name,
                difference_type='missing_in_dest',
                source_definition=src_cols[col_name],
                details=f"Column exists in source but not in destination"
            ))
        
        # Columns only in destination
        for col_name in dst_cols.keys() - src_cols.keys():
            column_differences.append(ColumnDifference(
                column_name=col_name,
                difference_type='missing_in_source',
                dest_definition=dst_cols[col_name],
                details=f"Column exists in destination but not in source"
            ))
        
        # Columns in both - check for differences
        for col_name in src_cols.keys() & dst_cols.keys():
            src_col = src_cols[col_name]
            dst_col = dst_cols[col_name]
            
            # Check data type
            if src_col['data_type'] != dst_col['data_type']:
                column_differences.append(ColumnDifference(
                    column_name=col_name,
                    difference_type='type_mismatch',
                    source_definition=src_col,
                    dest_definition=dst_col,
                    details=f"Type mismatch: {src_col['data_type']} vs {dst_col['data_type']}"
                ))
            
            # Check nullable
            if src_col['is_nullable'] != dst_col['is_nullable']:
                column_differences.append(ColumnDifference(
                    column_name=col_name,
                    difference_type='nullable_mismatch',
                    source_definition=src_col,
                    dest_definition=dst_col,
                    details=f"Nullable mismatch: {src_col['is_nullable']} vs {dst_col['is_nullable']}"
                ))
        
        # Compare indexes
        src_indexes = {idx['indexname'] for idx in fetch_indexes(self.src_engine, self.src_schema, table)}
        dst_indexes = {idx['indexname'] for idx in fetch_indexes(self.dst_engine, self.dst_schema, table)}
        missing_indexes = list(src_indexes - dst_indexes)
        
        # Compare foreign keys
        src_fks = {fk['constraint_name'] for fk in fetch_foreign_keys(self.src_engine, self.src_schema, table)}
        dst_fks = {fk['constraint_name'] for fk in fetch_foreign_keys(self.dst_engine, self.dst_schema, table)}
        missing_fks = list(src_fks - dst_fks)
        
        # Get row counts if requested
        src_row_count = 0
        dst_row_count = 0
        if include_row_counts:
            src_row_count = get_row_count(self.src_engine, table, self.src_schema)
            dst_row_count = get_row_count(self.dst_engine, table, self.dst_schema)
        
        # Determine status
        status = 'identical'
        if column_differences or missing_indexes or missing_fks:
            status = 'schema_diff'
        elif include_row_counts and src_row_count != dst_row_count:
            status = 'row_count_diff'
        
        return TableDifference(
            table_name=table,
            status=status,
            source_row_count=src_row_count,
            dest_row_count=dst_row_count,
            column_differences=column_differences,
            missing_indexes=missing_indexes,
            missing_foreign_keys=missing_fks
        )
    
    def _compare_enums(self) -> Dict[str, str]:
        """Compare ENUM types between schemas"""
        try:
            src_enums = fetch_enum_types(self.src_engine, self.src_schema)
            dst_enums = fetch_enum_types(self.dst_engine, self.dst_schema)
        except Exception as e:
            logger.warning(f"Could not compare ENUM types: {str(e)}")
            return {}
        
        differences = {}
        
        # ENUMs only in source
        for enum_name in src_enums.keys() - dst_enums.keys():
            differences[enum_name] = f"ENUM exists in source but not in destination"
        
        # ENUMs only in destination
        for enum_name in dst_enums.keys() - src_enums.keys():
            differences[enum_name] = f"ENUM exists in destination but not in source"
        
        # ENUMs in both - check values
        for enum_name in src_enums.keys() & dst_enums.keys():
            src_values = set(src_enums[enum_name])
            dst_values = set(dst_enums[enum_name])
            
            if src_values != dst_values:
                missing_in_dst = src_values - dst_values
                extra_in_dst = dst_values - src_values
                details = []
                if missing_in_dst:
                    details.append(f"Missing in dest: {missing_in_dst}")
                if extra_in_dst:
                    details.append(f"Extra in dest: {extra_in_dst}")
                differences[enum_name] = "; ".join(details)
        
        return differences
    
    def generate_comparison_report(self, result: SchemaDifference) -> str:
        """Generate a human-readable comparison report"""
        lines = []
        lines.append("=" * 80)
        lines.append("SCHEMA COMPARISON REPORT")
        lines.append("=" * 80)
        lines.append(f"Source Schema: {self.src_schema}")
        lines.append(f"Destination Schema: {self.dst_schema}")
        lines.append("")
        
        # Summary
        lines.append("SUMMARY:")
        lines.append("-" * 80)
        summary = result.summary
        lines.append(f"  Tables only in source:     {summary['tables_only_in_source']}")
        lines.append(f"  Tables only in dest:       {summary['tables_only_in_dest']}")
        lines.append(f"  Tables with differences:   {summary['tables_with_differences']}")
        lines.append(f"  Tables identical:          {summary['tables_identical']}")
        lines.append(f"  ENUM differences:          {summary['enum_differences']}")
        lines.append("")
        
        # Tables only in source
        if result.tables_only_in_source:
            lines.append("TABLES ONLY IN SOURCE (Not Migrated):")
            lines.append("-" * 80)
            for table in result.tables_only_in_source:
                lines.append(f"  ❌ {table}")
            lines.append("")
        
        # Tables only in destination
        if result.tables_only_in_dest:
            lines.append("TABLES ONLY IN DESTINATION:")
            lines.append("-" * 80)
            for table in result.tables_only_in_dest:
                lines.append(f"  ⚠️  {table}")
            lines.append("")
        
        # Table differences
        tables_with_diff = [td for td in result.table_differences if td.has_differences]
        if tables_with_diff:
            lines.append("TABLES WITH DIFFERENCES:")
            lines.append("-" * 80)
            for td in tables_with_diff:
                lines.append(f"\n  📋 Table: {td.table_name}")
                lines.append(f"     Status: {td.status}")
                lines.append(f"     Row counts: Source={td.source_row_count:,}, Dest={td.dest_row_count:,}")
                
                if td.column_differences:
                    lines.append(f"     Column Differences:")
                    for cd in td.column_differences:
                        lines.append(f"       • {cd.column_name}: {cd.details}")
                
                if td.missing_indexes:
                    lines.append(f"     Missing Indexes: {', '.join(td.missing_indexes)}")
                
                if td.missing_foreign_keys:
                    lines.append(f"     Missing Foreign Keys: {', '.join(td.missing_foreign_keys)}")
            lines.append("")
        
        # ENUM differences
        if result.enum_differences:
            lines.append("ENUM TYPE DIFFERENCES:")
            lines.append("-" * 80)
            for enum_name, diff in result.enum_differences.items():
                lines.append(f"  • {enum_name}: {diff}")
            lines.append("")
        
        # Conclusion
        lines.append("=" * 80)
        if not result.has_differences:
            lines.append("✅ SCHEMAS ARE IDENTICAL")
        else:
            lines.append("⚠️  SCHEMAS HAVE DIFFERENCES - Review above details")
        lines.append("=" * 80)
        
        return "\n".join(lines)


def quick_compare(src_engine: Engine, dst_engine: Engine,
                  src_schema: str = "public", dst_schema: str = "migrated",
                  include_row_counts: bool = True) -> SchemaDifference:
    """Quick comparison function"""
    comparator = SchemaComparator(src_engine, dst_engine, src_schema, dst_schema)
    return comparator.compare_schemas(include_row_counts)