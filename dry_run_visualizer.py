"""
Dry Run Visualizer - Generates formatted output for dry run results
Feature #2: Interactive Migration Playground - Visualization

Provides rich console output, ASCII graphs, and formatted reports
for dry run and analyses.
"""
import logging
from typing import Dict, List, Optional
from datetime import timedelta

from dry_run_analyzer import DryRunResult, TableImpact, RiskLevel


logger = logging.getLogger(__name__)


class DryRunVisualizer:
    """Generate visual representations of dry run and rollback results"""
    
    @staticmethod
    def format_dry_run_report(result: DryRunResult, detailed: bool = True) -> str:
        """
        Generate formatted text report for dry run analysis.
        
        Args:
            result: DryRunResult to format
            detailed: Include detailed table-by-table breakdown
            
        Returns:
            Formatted report string
        """
        lines = []
        
        # Header
        lines.append("=" * 80)
        lines.append("DRY RUN ANALYSIS REPORT")
        lines.append("=" * 80)
        lines.append(f"Migration Type: {result.migration_type}")
        lines.append(f"Timestamp: {result.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Source: {result.source_db}.{result.source_schema}")
        lines.append(f"Destination: {result.dest_db}.{result.target_schema}")
        lines.append("")
        
        # Overall Summary
        lines.append("OVERVIEW")
        lines.append("-" * 80)
        summary = result.summary
        lines.append(f"  Total Tables: {summary['tables']['total']}")
        lines.append(f"  To Create: {summary['tables']['to_create']}")
        lines.append(f"  To Modify: {summary['tables']['to_modify']}")
        lines.append(f"  To Drop: {summary['tables']['to_drop']}")
        lines.append(f"  Unchanged: {summary['tables']['unchanged']}")
        lines.append("")
        lines.append(f"  Total Rows to Process: {summary['data']['total_rows']:,}")
        lines.append(f"  Estimated Data Size: {summary['data']['estimated_size_mb']:.2f} MB")
        lines.append("")
        
        # Performance Estimates
        lines.append("PERFORMANCE ESTIMATES")
        lines.append("-" * 80)
        duration = result.estimated_total_duration_seconds
        duration_str = str(timedelta(seconds=int(duration)))
        lines.append(f"  Estimated Duration: {duration_str}")
        lines.append(f"  Memory Required: ~{result.estimated_memory_mb:.0f} MB")
        lines.append(f"  Disk Space Required: ~{result.estimated_disk_space_mb:.0f} MB")
        lines.append(f"  Concurrent Connections: {result.concurrent_connections_needed}")
        lines.append("")
        
        # Risk Assessment
        risk_icon = DryRunVisualizer._get_risk_icon(result.overall_risk_level)
        lines.append("RISK ASSESSMENT")
        lines.append("-" * 80)
        lines.append(f"  Overall Risk Level: {risk_icon} {result.overall_risk_level.value.upper()}")
        lines.append(f"  Safe to Execute: {'✅ YES' if result.is_safe_to_execute else '⚠️  NO'}")
        
        if result.critical_warnings:
            lines.append(f"  Critical Warnings: {len(result.critical_warnings)}")
            for warning in result.critical_warnings:
                lines.append(f"    ⚠️  {warning}")
        else:
            lines.append("  Critical Warnings: None")
        
        if result.circular_dependencies:
            lines.append(f"  Circular Dependencies: {len(result.circular_dependencies)} detected")
            for cycle in result.circular_dependencies[:3]:  # Show first 3
                cycle_str = " → ".join(cycle)
                lines.append(f"    🔄 {cycle_str}")
        
        lines.append("")
        
        # Recommendations
        if result.recommendations:
            lines.append("RECOMMENDATIONS")
            lines.append("-" * 80)
            for rec in result.recommendations:
                lines.append(f"  💡 {rec}")
            lines.append("")
        
        # Execution Order
        if result.execution_order and len(result.execution_order) <= 20:
            lines.append("EXECUTION ORDER")
            lines.append("-" * 80)
            for i, table in enumerate(result.execution_order, 1):
                lines.append(f"  {i:2d}. {table}")
            lines.append("")
        
        # Detailed Table Analysis
        if detailed and result.table_impacts:
            lines.append("DETAILED TABLE ANALYSIS")
            lines.append("-" * 80)
            
            # Group by risk level
            tables_by_risk = {
                RiskLevel.CRITICAL: [],
                RiskLevel.HIGH: [],
                RiskLevel.MEDIUM: [],
                RiskLevel.LOW: []
            }
            
            for table_name, impact in result.table_impacts.items():
                tables_by_risk[impact.risk_level].append((table_name, impact))
            
            # Show high/critical risk tables first
            for risk_level in [RiskLevel.CRITICAL, RiskLevel.HIGH, RiskLevel.MEDIUM]:
                tables = tables_by_risk[risk_level]
                if tables:
                    risk_icon = DryRunVisualizer._get_risk_icon(risk_level)
                    lines.append(f"\n  {risk_icon} {risk_level.value.upper()} RISK TABLES ({len(tables)}):")
                    
                    for table_name, impact in sorted(tables, key=lambda x: x[1].source_row_count, reverse=True)[:10]:
                        lines.append(f"\n    📋 {table_name}")
                        lines.append(f"       Action: {impact.action}")
                        lines.append(f"       Rows: {impact.source_row_count:,} → {impact.rows_to_insert:,}")
                        lines.append(f"       Size: {impact.estimated_size_mb:.2f} MB")
                        lines.append(f"       Duration: {impact.estimated_duration_seconds:.1f}s")
                        
                        if impact.dependencies:
                            lines.append(f"       Dependencies: {', '.join(impact.dependencies[:5])}")
                        
                        if impact.risk_factors:
                            for factor in impact.risk_factors:
                                lines.append(f"       ⚠️  {factor}")
                        
                        if impact.warnings:
                            for warning in impact.warnings:
                                lines.append(f"       ⚠️  {warning}")
            
            # Show count of low risk tables
            low_risk_count = len(tables_by_risk[RiskLevel.LOW])
            if low_risk_count > 0:
                lines.append(f"\n  ✅ LOW RISK TABLES: {low_risk_count} (not shown)")
        
        lines.append("")
        lines.append("=" * 80)
        
        if result.is_safe_to_execute:
            lines.append("✅ DRY RUN COMPLETE - MIGRATION IS SAFE TO EXECUTE")
        else:
            lines.append("⚠️  DRY RUN COMPLETE - REVIEW WARNINGS BEFORE PROCEEDING")
        
        lines.append("=" * 80)
        
        return "\n".join(lines)
    

    
    @staticmethod
    def create_ascii_bar_chart(
        data: Dict[str, int],
        title: str,
        max_width: int = 50
    ) -> str:
        """
        Create simple ASCII bar chart.
        
        Args:
            data: Dictionary of label -> value
            title: Chart title
            max_width: Maximum width of bars
            
        Returns:
            ASCII bar chart
        """
        if not data:
            return f"{title}\n(No data)"
        
        lines = []
        lines.append(title)
        lines.append("-" * (max_width + 30))
        
        max_value = max(data.values())
        
        for label, value in sorted(data.items(), key=lambda x: x[1], reverse=True):
            if max_value > 0:
                bar_width = int((value / max_value) * max_width)
            else:
                bar_width = 0
            
            bar = "█" * bar_width
            lines.append(f"{label:<20} {bar} {value:,}")
        
        lines.append("")
        return "\n".join(lines)
    
    @staticmethod
    def _get_risk_icon(risk_level: RiskLevel) -> str:
        """Get icon for risk level"""
        icons = {
            RiskLevel.LOW: "✅",
            RiskLevel.MEDIUM: "⚠️ ",
            RiskLevel.HIGH: "🔴",
            RiskLevel.CRITICAL: "💥"
        }
        return icons.get(risk_level, "❓")
    
    @staticmethod
    def format_quick_summary(result: DryRunResult) -> str:
        """
        Generate quick one-line summary.
        
        Args:
            result: DryRunResult
            
        Returns:
            One-line summary string
        """
        risk_icon = DryRunVisualizer._get_risk_icon(result.overall_risk_level)
        safe_icon = "✅" if result.is_safe_to_execute else "⚠️ "
        
        duration = str(timedelta(seconds=int(result.estimated_total_duration_seconds)))
        
        return (
            f"{safe_icon} {result.migration_type}: "
            f"{result.total_tables} tables, "
            f"{result.total_rows_to_process:,} rows, "
            f"~{duration}, "
            f"Risk: {risk_icon} {result.overall_risk_level.value}"
        )