"""
Report generation for migration operations
FIXED: Better error handling and schema detection
"""
import os
import logging
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from sqlalchemy import text

from migration_tracker import MigrationTracker
from data_operations import get_row_count
from database import fetch_tables
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class MigrationReportGenerator:
    """Generate reports for migration operations"""
    
    def __init__(self, dst_engine: Engine, target_schema: str = "migrated"):
        self.engine = dst_engine
        
        # Auto-detect schema if default "migrated" is provided
        if target_schema == "migrated":
            available = self._list_migration_schemas()
            if not available:
                raise ValueError(
                    f"No migration schemas found. Have you run any migrations yet?\n"
                    f"Migration schemas should be named like: migrated_full_copy, migrated_incremental_sync, etc.\n"
                    f"Note: Looking for schemas matching pattern 'migrated_*' (with underscore)"
                )
            elif len(available) == 1:
                # Auto-select the only available schema
                self.target_schema = available[0]
                logger.info(f"Auto-detected schema: {self.target_schema}")
            else:
                # Multiple schemas found - auto-select the most recent one
                # Schemas are ordered, so last one is typically the most recent
                self.target_schema = available[-1]
                logger.info(f"Multiple schemas found: {', '.join(available)}")
                logger.info(f"Auto-selected most recent: {self.target_schema}")
        else:
            self.target_schema = target_schema
        
        # Check if selected schema exists
        if not self._schema_exists():
            available = self._list_migration_schemas()
            if available:
                logger.warning(
                    f"Schema '{self.target_schema}' not found. "
                    f"Available migration schemas: {', '.join(available)}"
                )
                raise ValueError(
                    f"Schema '{self.target_schema}' does not exist. "
                    f"Available schemas: {', '.join(available)}\n"
                    f"Use: --schema <schema_name>"
                )
            else:
                raise ValueError(
                    f"Schema '{self.target_schema}' not found and no migration schemas detected. "
                    f"Have you run any migrations yet?"
                )
        
        self.tracker = MigrationTracker(dst_engine, self.target_schema)
    
    def _schema_exists(self) -> bool:
        """Check if the target schema exists"""
        sql = """
        SELECT EXISTS(
            SELECT 1 FROM information_schema.schemata 
            WHERE schema_name = :schema_name
        )
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), {"schema_name": self.target_schema})
                return result.scalar()
        except Exception as e:
            logger.error(f"Error checking schema existence: {e}")
            return False
    
    def _list_migration_schemas(self) -> List[str]:
        """List all schemas that look like migration schemas (with suffixes only)"""
        sql = """
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name LIKE 'migrated_%'
        ORDER BY schema_name
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error listing schemas: {e}")
            return []
    
    def generate_excel_report(self, output_path: str, limit: int = 1000) -> str:
        """Generate Excel report of migration history"""
        # Ensure reports directory exists
        import os
        reports_dir = "reports"
        if not os.path.exists(reports_dir):
            os.makedirs(reports_dir)
            logger.info(f"Created reports directory: {reports_dir}")
        
        # Adjust output path to be in reports directory
        if not output_path.startswith(reports_dir):
            output_path = os.path.join(reports_dir, os.path.basename(output_path))
        
        history = self.tracker.get_migration_history(limit=limit)
        
        if not history:
            logger.warning(f"No migration history found in schema '{self.target_schema}'")
            # Create a minimal report anyway
            summary_data = {
                'Metric': [
                    'Total Migrations',
                    'Successful Migrations',
                    'Failed Migrations',
                    'Total Rows Migrated',
                    'Unique Tables Migrated',
                    'Schema',
                    'Status'
                ],
                'Value': [
                    0,
                    0,
                    0,
                    0,
                    0,
                    self.target_schema,
                    'No migrations found - schema may be empty or migrations not tracked'
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            logger.info(f"Excel report generated (empty): {output_path}")
            return output_path
        
        # Convert to DataFrame
        df = pd.DataFrame(history)
        
        # Format timestamps
        for col in ['started_at', 'completed_at']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        # Create Excel writer
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Write main history
            df.to_excel(writer, sheet_name='Migration History', index=False)
            
            # Create summary sheet
            summary_data = {
                'Metric': [
                    'Schema Name',
                    'Total Migrations',
                    'Successful Migrations',
                    'Failed Migrations',
                    'Total Rows Migrated',
                    'Unique Tables Migrated',
                    'Source Database(s)',
                    'Destination Database',
                ],
                'Value': [
                    self.target_schema,
                    len(df),
                    len(df[df['status'] == 'completed']),
                    len(df[df['status'] == 'failed']),
                    int(df[df['status'] == 'completed']['rows_migrated'].sum()),
                    df['table_name'].nunique(),
                    ', '.join(df['source_db'].unique()),
                    self.engine.url.database,
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Table-wise summary
            table_summary = df.groupby('table_name').agg({
                'rows_migrated': 'sum',
                'id': 'count',
                'status': lambda x: (x == 'completed').sum()
            }).rename(columns={
                'rows_migrated': 'Total Rows',
                'id': 'Total Migrations',
                'status': 'Successful Migrations'
            })
            table_summary.to_excel(writer, sheet_name='By Table')
        
        logger.info(f"Excel report generated: {output_path}")
        return output_path
    
    def generate_pdf_report(
        self, 
        output_path: str, 
        src_engine: Optional[Engine] = None,
        src_schema: str = "public"
    ) -> str:
        """Generate PDF report with detailed migration information"""
        # Ensure reports directory exists
        import os
        reports_dir = "reports"
        if not os.path.exists(reports_dir):
            os.makedirs(reports_dir)
            logger.info(f"Created reports directory: {reports_dir}")
        
        # Adjust output path to be in reports directory
        if not output_path.startswith(reports_dir):
            output_path = os.path.join(reports_dir, os.path.basename(output_path))
        
        doc = SimpleDocTemplate(output_path, pagesize=letter)
        story = []
        styles = getSampleStyleSheet()
        
        # Custom styles
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#2C3E50'),
            spaceAfter=30,
            alignment=TA_CENTER
        )
        
        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontSize=16,
            textColor=colors.HexColor('#34495E'),
            spaceAfter=12,
            spaceBefore=12
        )
        
        # Title
        title = Paragraph("PostgreSQL Migration Report", title_style)
        story.append(title)
        
        # Report metadata
        metadata_text = (
            f"<b>Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br/>"
            f"<b>Destination Database:</b> {self.engine.url.database}<br/>"
            f"<b>Target Schema:</b> {self.target_schema}<br/>"
        )
        
        metadata = Paragraph(metadata_text, styles['Normal'])
        story.append(metadata)
        story.append(Spacer(1, 20))
        
        # Migration History Summary
        history = self.tracker.get_migration_history(limit=100)
        
        if history:
            story.append(Paragraph("Migration History Summary", heading_style))
            
            df = pd.DataFrame(history)
            
            summary_data = [
                ['Metric', 'Value'],
                ['Schema Name', self.target_schema],
                ['Destination Database', self.engine.url.database],
                ['Total Migrations', str(len(df))],
                ['Successful', str(len(df[df['status'] == 'completed']))],
                ['Failed', str(len(df[df['status'] == 'failed']))],
                ['Total Rows Migrated', f"{int(df[df['status'] == 'completed']['rows_migrated'].sum()):,}"],
                ['Unique Tables', str(df['table_name'].nunique())],
            ]
            
            summary_table = Table(summary_data, colWidths=[3*inch, 3*inch])
            summary_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498DB')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]))
            story.append(summary_table)
            story.append(Spacer(1, 20))
            
            # Recent Migrations
            story.append(Paragraph("Recent Migrations", heading_style))
            
            recent = df.head(10)
            migration_data = [['Table', 'Type', 'Rows', 'Status', 'Completed']]
            
            for _, row in recent.iterrows():
                migration_data.append([
                    row['table_name'],
                    row['migration_type'],
                    str(int(row['rows_migrated']) if pd.notna(row['rows_migrated']) else 0),
                    row['status'],
                    row['completed_at'].strftime('%Y-%m-%d %H:%M') if pd.notna(row['completed_at']) else 'N/A'
                ])
            
            migrations_table = Table(migration_data, colWidths=[1.5*inch, 1.3*inch, 0.8*inch, 1*inch, 1.4*inch])
            migrations_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498DB')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]))
            story.append(migrations_table)
        else:
            # No history found
            story.append(Paragraph("No Migration History", heading_style))
            no_data = Paragraph(
                f"No migration history found in schema '{self.target_schema}'. "
                f"This schema may be empty or migrations were not tracked.",
                styles['Normal']
            )
            story.append(no_data)
        
        # Current State
        if src_engine:
            story.append(PageBreak())
            story.append(Paragraph("Current State Comparison", heading_style))
            
            src_tables = fetch_tables(src_engine, src_schema)
            dst_tables = fetch_tables(self.engine, self.target_schema)
            
            state_data = [['Table', 'Source Rows', 'Destination Rows', 'Status']]
            
            for table in sorted(set(src_tables + dst_tables)):
                src_count = get_row_count(src_engine, table, src_schema) if table in src_tables else 0
                dst_count = get_row_count(self.engine, table, self.target_schema) if table in dst_tables else 0
                
                if src_count == dst_count and src_count > 0:
                    status = "Synced"
                elif dst_count == 0:
                    status = "Not Migrated"
                elif dst_count < src_count:
                    status = "Partial"
                else:
                    status = "More in Dest"
                
                state_data.append([
                    table,
                    f"{src_count:,}",
                    f"{dst_count:,}",
                    status
                ])
            
            state_table = Table(state_data, colWidths=[2*inch, 1.5*inch, 1.5*inch, 1.5*inch])
            state_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498DB')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]))
            story.append(state_table)
        
        # Build PDF
        doc.build(story)
        logger.info(f"PDF report generated: {output_path}")
        return output_path
    
    def generate_summary_report(self) -> Dict:
        """Generate a summary dictionary of migration status"""
        history = self.tracker.get_migration_history(limit=1000)
        
        if not history:
            return {
                'schema': self.target_schema,
                'database': self.engine.url.database,
                'total_migrations': 0,
                'successful': 0,
                'failed': 0,
                'total_rows': 0,
                'unique_tables': 0,
                'tables': [],
                'message': 'No migration history found'
            }
        
        df = pd.DataFrame(history)
        
        table_stats = []
        for table in df['table_name'].unique():
            table_df = df[df['table_name'] == table]
            completed_df = table_df[table_df['status'] == 'completed']
            
            table_stats.append({
                'table_name': table,
                'total_migrations': len(table_df),
                'successful_migrations': len(completed_df),
                'total_rows_migrated': int(completed_df['rows_migrated'].sum()),
                'last_migration': table_df['started_at'].max().isoformat() if not table_df.empty else None
            })
        
        return {
            'schema': self.target_schema,
            'database': self.engine.url.database,
            'source_databases': list(df['source_db'].unique()),
            'total_migrations': len(df),
            'successful': len(df[df['status'] == 'completed']),
            'failed': len(df[df['status'] == 'failed']),
            'total_rows': int(df[df['status'] == 'completed']['rows_migrated'].sum()),
            'unique_tables': df['table_name'].nunique(),
            'tables': table_stats
        }
    
    def save_summary_to_file(self, output_path: str) -> str:
        """Save summary report to JSON file in reports directory"""
        import os
        import json
        
        # Ensure reports directory exists
        reports_dir = "reports"
        if not os.path.exists(reports_dir):
            os.makedirs(reports_dir)
            logger.info(f"Created reports directory: {reports_dir}")
        
        # Adjust output path to be in reports directory
        if not output_path.startswith(reports_dir):
            output_path = os.path.join(reports_dir, os.path.basename(output_path))
        
        summary = self.generate_summary_report()
        
        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Summary report saved: {output_path}")
        return output_path


def list_available_schemas(engine: Engine) -> List[str]:
    """Helper function to list all available migration schemas (with suffixes only)"""
    sql = """
    SELECT schema_name 
    FROM information_schema.schemata 
    WHERE schema_name LIKE 'migrated_%'
    ORDER BY schema_name
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            return [row[0] for row in result]
    except Exception as e:
        logger.error(f"Error listing schemas: {e}")
        return []