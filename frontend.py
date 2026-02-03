"""
Textual-based Terminal UI for PostgreSQL Migration Tool
"""
from textual.app import App, ComposeResult
from textual.widgets import (
    Header, Footer, Button, Static, Input, 
    Select, Checkbox, Label, Log, ListView, ListItem
)
from textual.containers import Vertical, Horizontal, Container, ScrollableContainer
from textual.screen import Screen
from textual.binding import Binding
import logging

from config import DBConfig, MigrationConfig
from database import create_engine_safe, fetch_tables, test_connection
from migrations import (
    full_migration,
    incremental_sync,
    table_copy_delete_and_recreate,
    table_copy_delta_only,
)
from report_generator import MigrationReportGenerator

logger = logging.getLogger(__name__)


class WelcomeScreen(Screen):
    """Welcome screen with migration mode selection"""
    
    BINDINGS = [
        Binding("q", "quit", "Quit"),
    ]
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Static("🗄️ PostgreSQL Migration Tool v2.0", classes="title"),
            Static("Production-Ready Database Migration", classes="subtitle"),
            Static(""),
            Button("📦 Full Database Copy", id="full", variant="primary"),
            Static("  • Complete database replication"),
            Static("  • Drops and recreates all tables"),
            Static(""),
            Button("🔄 Incremental Sync (Smart Delta)", id="delta", variant="success"),
            Static("  • Hash-based content comparison"),
            Static("  • Only copies new/changed data"),
            Static("  • Preserves existing data"),
            Static(""),
            Button("📋 Table-Level Operations", id="advanced", variant="warning"),
            Static("  • Select specific tables"),
            Static("  • Choose per-table copy mode"),
            Static("  • Fine-grained control"),
            Static(""),
            Button("📊 Generate Reports", id="reports", variant="default"),
            Static("  • View migration history"),
            Static("  • Export to PDF/Excel"),
            Static(""),
            Button("❌ Exit", id="exit", variant="error"),
            classes="welcome-container"
        )
        yield Footer()
    
    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "exit":
            self.app.exit()
        elif event.button.id == "reports":
            self.app.push_screen(ReportGenerationScreen())
        else:
            self.app.mode = event.button.id
            self.app.push_screen(SourceDBScreen())


class SourceDBScreen(Screen):
    """Source database configuration screen"""
    
    BINDINGS = [
        Binding("escape", "back", "Back"),
    ]
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Static("📊 SOURCE DATABASE", classes="section-title"),
            Static(""),
            Label("Host:"),
            Input(placeholder="localhost", id="host", value="localhost"),
            Label("Port:"),
            Input(placeholder="5432", id="port", value="5432"),
            Label("Database:"),
            Input(placeholder="source_db", id="db"),
            Label("User:"),
            Input(placeholder="postgres", id="user", value="postgres"),
            Label("Password:"),
            Input(placeholder="password", password=True, id="pwd"),
            Static(""),
            Horizontal(
                Button("Test Connection", id="test", variant="default"),
                Button("Next →", id="next", variant="primary"),
                Button("← Back", id="back", variant="default"),
                classes="button-row"
            ),
            Static("", id="status"),
            classes="form-container"
        )
        yield Footer()
    
    def action_back(self):
        self.app.pop_screen()
    
    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "back":
            self.app.pop_screen()
        elif event.button.id == "test":
            self.test_connection()
        elif event.button.id == "next":
            self.save_and_next()
    
    def test_connection(self):
        cfg = self.get_config()
        status = self.query_one("#status", Static)
        status.update("Testing connection...")
        
        success, message = test_connection(cfg)
        if success:
            status.update(f"✅ {message}")
        else:
            status.update(f"❌ {message}")
    
    def get_config(self) -> DBConfig:
        return DBConfig(
            host=self.query_one("#host", Input).value,
            port=self.query_one("#port", Input).value,
            database=self.query_one("#db", Input).value,
            user=self.query_one("#user", Input).value,
            password=self.query_one("#pwd", Input).value,
        )
    
    def save_and_next(self):
        cfg = self.get_config()
        success, message = test_connection(cfg)
        
        if not success:
            self.query_one("#status", Static).update(f"❌ {message}")
            return
        
        self.app.src_cfg = cfg
        self.app.push_screen(DestDBScreen())


class DestDBScreen(Screen):
    """Destination database configuration screen"""
    
    BINDINGS = [
        Binding("escape", "back", "Back"),
    ]
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Static("🎯 DESTINATION DATABASE", classes="section-title"),
            Static(""),
            Label("Host:"),
            Input(placeholder="localhost", id="host", value="localhost"),
            Label("Port:"),
            Input(placeholder="5432", id="port", value="5432"),
            Label("Database:"),
            Input(placeholder="dest_db", id="db"),
            Label("User:"),
            Input(placeholder="postgres", id="user", value="postgres"),
            Label("Password:"),
            Input(placeholder="password", password=True, id="pwd"),
            Static(""),
            Horizontal(
                Button("Test Connection", id="test", variant="default"),
                Button("Next →", id="next", variant="primary"),
                Button("← Back", id="back", variant="default"),
                classes="button-row"
            ),
            Static("", id="status"),
            classes="form-container"
        )
        yield Footer()
    
    def action_back(self):
        self.app.pop_screen()
    
    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "back":
            self.app.pop_screen()
        elif event.button.id == "test":
            self.test_connection()
        elif event.button.id == "next":
            self.save_and_next()
    
    def test_connection(self):
        cfg = self.get_config()
        status = self.query_one("#status", Static)
        status.update("Testing connection...")
        
        success, message = test_connection(cfg)
        if success:
            status.update(f"✅ {message}")
        else:
            status.update(f"❌ {message}")
    
    def get_config(self) -> DBConfig:
        return DBConfig(
            host=self.query_one("#host", Input).value,
            port=self.query_one("#port", Input).value,
            database=self.query_one("#db", Input).value,
            user=self.query_one("#user", Input).value,
            password=self.query_one("#pwd", Input).value,
        )
    
    def save_and_next(self):
        cfg = self.get_config()
        success, message = test_connection(cfg)
        
        if not success:
            self.query_one("#status", Static).update(f"❌ {message}")
            return
        
        self.app.dst_cfg = cfg
        
        # Navigate based on mode
        if self.app.mode == "advanced":
            self.app.push_screen(TableSelectionScreen())
        else:
            self.app.push_screen(ConfirmScreen())


class TableSelectionScreen(Screen):
    """Screen for selecting tables in advanced mode"""
    
    BINDINGS = [
        Binding("escape", "back", "Back"),
        Binding("a", "select_all", "Select All"),
        Binding("d", "deselect_all", "Deselect All"),
    ]
    
    def __init__(self):
        super().__init__()
        self.table_checkboxes = {}
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield ScrollableContainer(
            Static("📋 SELECT TABLES TO MIGRATE", classes="section-title"),
            Static(""),
            Static("Available tables in source database:", classes="subtitle"),
            Static("💡 Use checkboxes to select tables. Press 'A' to select all, 'D' to deselect all.", classes="info"),
            Static("", id="table-list"),
            Static(""),
            Horizontal(
                Button("Select All (A)", id="select_all", variant="default"),
                Button("Deselect All (D)", id="deselect_all", variant="default"),
                classes="button-row"
            ),
            Static(""),
            Static("Copy Mode:", classes="subtitle"),
            Select(
                [
                    ("Delete & Recreate (Full refresh)", "delete_recreate"),
                    ("Delta Only (Add new rows)", "delta_only"),
                ],
                id="copy_mode",
                value="delta_only"
            ),
            Static(""),
            Static("Options:", classes="subtitle"),
            Checkbox("Exclude auto-generated columns (id, serial, created_at, updated_at, etc.)", id="exclude_auto", value=True),
            Static(""),
            Horizontal(
                Button("← Back", id="back", variant="default"),
                Button("Next →", id="next", variant="primary"),
                classes="button-row"
            ),
            Static("", id="status"),
            classes="form-container-scrollable"
        )
        yield Footer()
    
    def on_mount(self):
        self.load_tables()
    
    def load_tables(self):
        try:
            src_engine = create_engine_safe(self.app.src_cfg)
            tables = fetch_tables(src_engine, "public")
            
            # Create container for checkboxes
            table_list = self.query_one("#table-list", Static)
            
            # Build checkbox list
            checkbox_container = Container(id="checkbox-container")
            
            for table in tables:
                cb = Checkbox(table, id=f"table_{table}")
                self.table_checkboxes[table] = cb
                checkbox_container.mount(cb)
            
            # Mount the container
            table_list.update("")
            self.query_one(ScrollableContainer).mount(checkbox_container, before=self.query_one("#status"))
            
            src_engine.dispose()
        except Exception as e:
            self.query_one("#status", Static).update(f"❌ Error loading tables: {str(e)}")
    
    def action_back(self):
        self.app.pop_screen()
    
    def action_select_all(self):
        """Select all tables"""
        for checkbox in self.table_checkboxes.values():
            checkbox.value = True
    
    def action_deselect_all(self):
        """Deselect all tables"""
        for checkbox in self.table_checkboxes.values():
            checkbox.value = False
    
    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "back":
            self.app.pop_screen()
        elif event.button.id == "select_all":
            self.action_select_all()
        elif event.button.id == "deselect_all":
            self.action_deselect_all()
        elif event.button.id == "next":
            self.save_and_next()
    
    def save_and_next(self):
        # Get selected tables from checkboxes
        selected = [
            table_name 
            for table_name, checkbox in self.table_checkboxes.items() 
            if checkbox.value
        ]
        
        if not selected:
            self.query_one("#status", Static).update("❌ Please select at least one table")
            return
        
        # Save selections
        self.app.selected_tables = selected
        self.app.copy_mode = self.query_one("#copy_mode", Select).value
        self.app.exclude_auto = self.query_one("#exclude_auto", Checkbox).value
        
        self.app.push_screen(ConfirmScreen())


class ConfirmScreen(Screen):
    """Confirmation screen before migration"""
    
    BINDINGS = [
        Binding("escape", "back", "Back"),
    ]
    
    def compose(self) -> ComposeResult:
        mode_text = {
            "full": "Full Database Copy",
            "delta": "Incremental Sync (Smart Delta)",
            "advanced": "Table-Level Operations"
        }
        
        yield Header()
        yield Container(
            Static("✅ CONFIRM MIGRATION", classes="section-title"),
            Static(""),
            Static("Migration Details:", classes="subtitle"),
            Static(f"  Mode: {mode_text.get(self.app.mode, 'Unknown')}"),
            Static(f"  Source: {self.app.src_cfg.user}@{self.app.src_cfg.host}:{self.app.src_cfg.port}/{self.app.src_cfg.database}"),
            Static(f"  Destination: {self.app.dst_cfg.user}@{self.app.dst_cfg.host}:{self.app.dst_cfg.port}/{self.app.dst_cfg.database}"),
            Static(""),
            self._get_mode_specific_info(),
            Static(""),
            Static("⚠️  IMPORTANT:", classes="warning"),
            Static("  • Tables will be created in 'migrated' schema"),
            Static("  • Original 'public' schema will NOT be modified"),
            Static("  • All operations are tracked for reporting"),
            Static(""),
            Horizontal(
                Button("← Back", id="back", variant="default"),
                Button("🚀 Start Migration", id="proceed", variant="success"),
                Button("❌ Cancel", id="cancel", variant="error"),
                classes="button-row"
            ),
            classes="form-container"
        )
        yield Footer()
    
    def _get_mode_specific_info(self) -> Static:
        if self.app.mode == "full":
            return Static("  • All tables will be dropped and recreated")
        elif self.app.mode == "delta":
            return Static("  • Only new/changed rows will be synchronized\n  • Existing data preserved")
        elif self.app.mode == "advanced":
            tables_text = f"{len(self.app.selected_tables)} table(s) selected: {', '.join(self.app.selected_tables[:3])}"
            if len(self.app.selected_tables) > 3:
                tables_text += f" ... and {len(self.app.selected_tables) - 3} more"
            mode_text = "Delete & Recreate" if self.app.copy_mode == "delete_recreate" else "Delta Only"
            auto_text = "Yes" if self.app.exclude_auto else "No"
            return Static(f"  • {tables_text}\n  • Copy mode: {mode_text}\n  • Exclude auto-generated: {auto_text}")
        return Static("")
    
    def action_back(self):
        self.app.pop_screen()
    
    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "back":
            self.app.pop_screen()
        elif event.button.id == "cancel":
            self.app.exit()
        elif event.button.id == "proceed":
            self.app.push_screen(ProgressScreen())


class ProgressScreen(Screen):
    """Migration progress screen"""
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Static("🔄 MIGRATION IN PROGRESS", classes="section-title"),
            Static(""),
            Log(id="log", highlight=True, auto_scroll=True),
            Static(""),
            Static("⏳ Please wait...", id="status"),
            Static(""),
            Button("Close", id="close", variant="default", disabled=True),
            classes="progress-container"
        )
        yield Footer()
    
    def on_mount(self):
        self.run_worker(self.run_migration, thread=True)
    
    def run_migration(self):
        log = self.query_one("#log", Log)
        status = self.query_one("#status", Static)
        
        def log_progress(message: str):
            log.write_line(message)
        
        try:
            src_engine = create_engine_safe(self.app.src_cfg)
            dst_engine = create_engine_safe(self.app.dst_cfg)
            
            config = MigrationConfig()
            
            if self.app.mode == "full":
                log_progress("Starting full database copy...")
                full_migration(
                    src_engine, dst_engine,
                    exclude_auto_generated=False,
                    config=config,
                    progress_callback=log_progress
                )
                
            elif self.app.mode == "delta":
                log_progress("Starting incremental sync...")
                incremental_sync(
                    src_engine, dst_engine,
                    config=config,
                    progress_callback=log_progress
                )
                
            elif self.app.mode == "advanced":
                log_progress("Starting table-level operations...")
                log_progress(f"Exclude auto-generated: {self.app.exclude_auto}")
                log_progress(f"Copy mode: {self.app.copy_mode}")
                
                for idx, table in enumerate(self.app.selected_tables, 1):
                    log_progress(f"\n[{idx}/{len(self.app.selected_tables)}] Processing: {table}")
                    
                    if self.app.copy_mode == "delete_recreate":
                        table_copy_delete_and_recreate(
                            src_engine, 
                            dst_engine, 
                            table,
                            exclude_auto_generated=self.app.exclude_auto,
                            config=config,
                            progress_callback=log_progress
                        )
                    else:  # delta_only
                        table_copy_delta_only(
                            src_engine,
                            dst_engine,
                            table,
                            exclude_auto_generated=self.app.exclude_auto,
                            config=config,
                            progress_callback=log_progress
                        )
                
                log_progress(f"\n✅ All {len(self.app.selected_tables)} tables processed successfully!")
            
            status.update("✅ Migration completed successfully!")
            self.query_one("#close", Button).disabled = False
            
            # Cleanup
            src_engine.dispose()
            dst_engine.dispose()
            
        except Exception as e:
            log_progress(f"\n❌ ERROR: {str(e)}")
            import traceback
            log_progress(traceback.format_exc())
            status.update("❌ Migration failed!")
            self.query_one("#close", Button).disabled = False
    
    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "close":
            self.app.pop_screen()


class ReportGenerationScreen(Screen):
    """Screen for generating migration reports"""
    
    BINDINGS = [
        Binding("escape", "back", "Back"),
    ]
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Static("📊 REPORT GENERATION", classes="section-title"),
            Static(""),
            Static("Configure destination database to generate reports:", classes="subtitle"),
            Static(""),
            Label("Host:"),
            Input(placeholder="localhost", id="host", value="localhost"),
            Label("Port:"),
            Input(placeholder="5432", id="port", value="5432"),
            Label("Database:"),
            Input(placeholder="dest_db", id="db"),
            Label("User:"),
            Input(placeholder="postgres", id="user", value="postgres"),
            Label("Password:"),
            Input(placeholder="password", password=True, id="pwd"),
            Static(""),
            Horizontal(
                Button("Generate Excel Report", id="excel", variant="primary"),
                Button("Generate PDF Report", id="pdf", variant="success"),
                classes="button-row"
            ),
            Static(""),
            Button("← Back", id="back", variant="default"),
            Static("", id="status"),
            classes="form-container"
        )
        yield Footer()
    
    def action_back(self):
        self.app.pop_screen()
    
    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "back":
            self.app.pop_screen()
        elif event.button.id == "excel":
            self.generate_excel()
        elif event.button.id == "pdf":
            self.generate_pdf()
    
    def get_config(self) -> DBConfig:
        return DBConfig(
            host=self.query_one("#host", Input).value,
            port=self.query_one("#port", Input).value,
            database=self.query_one("#db", Input).value,
            user=self.query_one("#user", Input).value,
            password=self.query_one("#pwd", Input).value,
        )
    
    def generate_excel(self):
        status = self.query_one("#status", Static)
        status.update("Generating Excel report...")
        
        try:
            cfg = self.get_config()
            engine = create_engine_safe(cfg)
            
            reporter = MigrationReportGenerator(engine)
            output_path = f"migration_report_{cfg.database}.xlsx"
            reporter.generate_excel_report(output_path)
            
            status.update(f"✅ Excel report generated: {output_path}")
            engine.dispose()
        except Exception as e:
            status.update(f"❌ Error: {str(e)}")
    
    def generate_pdf(self):
        status = self.query_one("#status", Static)
        status.update("Generating PDF report...")
        
        try:
            cfg = self.get_config()
            engine = create_engine_safe(cfg)
            
            reporter = MigrationReportGenerator(engine)
            output_path = f"migration_report_{cfg.database}.pdf"
            reporter.generate_pdf_report(output_path)
            
            status.update(f"✅ PDF report generated: {output_path}")
            engine.dispose()
        except Exception as e:
            status.update(f"❌ Error: {str(e)}")


# Main Application
class MigrationApp(App):
    """PostgreSQL Migration Tool v2.0"""
    
    CSS = """
    Screen {
        align: center middle;
    }
    
    .welcome-container, .form-container {
        width: 80;
        height: auto;
        border: solid $primary;
        padding: 1 2;
        background: $surface;
    }
    
    .form-container-scrollable {
        width: 90;
        height: 40;
        border: solid $primary;
        padding: 1 2;
        background: $surface;
    }
    
    .progress-container {
        width: 90;
        height: auto;
        border: solid $primary;
        padding: 1 2;
        background: $surface;
    }
    
    .title {
        text-align: center;
        text-style: bold;
        color: $accent;
        margin-bottom: 1;
    }
    
    .section-title {
        text-align: center;
        text-style: bold;
        color: $secondary;
        margin-bottom: 1;
    }
    
    .subtitle {
        color: $text-muted;
        margin: 1 0;
    }
    
    .info {
        color: $text-muted;
        margin: 0 0 1 0;
        text-align: center;
        text-style: italic;
    }
    
    .warning {
        color: $warning;
        text-style: bold;
    }
    
    Button {
        margin: 1 1;
        min-width: 20;
    }
    
    Input {
        margin: 0 0 1 0;
    }
    
    Label {
        margin: 1 0 0 0;
        color: $text;
    }
    
    .button-row {
        height: auto;
        align: center middle;
    }
    
    Checkbox {
        margin: 0 0 1 0;
    }
    
    #checkbox-container {
        height: auto;
        margin: 1 0;
    }
    
    Log {
        height: 25;
        border: solid $primary;
        margin: 1 0;
    }
    
    #status {
        text-align: center;
        margin: 1 0;
    }
    
    Select {
        margin: 1 0;
    }
    """
    
    BINDINGS = [
        Binding("q", "quit", "Quit", show=True),
    ]
    
    def __init__(self):
        super().__init__()
        self.mode = None
        self.src_cfg = None
        self.dst_cfg = None
        self.selected_tables = []
        self.copy_mode = "delta_only"  # Default to delta_only for safety
        self.exclude_auto = True
    
    def on_mount(self):
        self.push_screen(WelcomeScreen())
    
    def action_quit(self):
        self.exit()


def run_ui():
    """Run the Terminal UI"""
    app = MigrationApp()
    app.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    run_ui()