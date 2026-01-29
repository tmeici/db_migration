# frontend.py
from textual.app import App, ComposeResult
from textual.widgets import (
    Header, Footer, Button, Static, Input, 
    DataTable, Select, Checkbox, Label, Log
)
from textual.containers import Vertical, Horizontal, Container
from textual.screen import Screen
from textual.binding import Binding

from backend import (
    DBConfig,
    create_engine_safe,
    full_database_copy,
    incremental_sync,
    table_copy_delete_and_recreate,
    table_copy_delta_only,
    fetch_tables,
    test_connection,
    get_row_count,
)

# ---------------- SCREENS ----------------

class WelcomeScreen(Screen):
    """Welcome screen with migration mode selection"""
    
    BINDINGS = [
        Binding("q", "quit", "Quit"),
    ]
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Static("🗄️ PostgreSQL Migration Tool", classes="title"),
            Static("Select your migration mode:", classes="subtitle"),
            Static(""),
            Button("📦 Full Database Copy", id="full", variant="primary"),
            Static("  • Copies all tables and data to destination.migrated schema"),
            Static("  • Complete database replication"),
            Static(""),
            Button("🔄 Incremental Sync (Delta)", id="delta", variant="success"),
            Static("  • Copies only new rows based on primary key"),
            Static("  • Efficient for ongoing synchronization"),
            Static(""),
            Button("📋 Table-Level Operations", id="advanced", variant="warning"),
            Static("  • Select specific tables to migrate"),
            Static("  • Choose copy mode per table"),
            Static("  • Exclude auto-generated fields option"),
            Static(""),
            Button("❌ Exit", id="exit", variant="error"),
            classes="welcome-container"
        )
        yield Footer()
    
    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "exit":
            self.app.exit()
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
    ]
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Static("📋 SELECT TABLES TO MIGRATE", classes="section-title"),
            Static(""),
            Static("Available tables in source database:", classes="subtitle"),
            DataTable(id="tables", zebra_stripes=True),
            Static(""),
            Horizontal(
                Button("Select All", id="select_all", variant="default"),
                Button("Deselect All", id="deselect_all", variant="default"),
                classes="button-row"
            ),
            Static(""),
            Horizontal(
                Button("← Back", id="back", variant="default"),
                Button("Next →", id="next", variant="primary"),
                classes="button-row"
            ),
            Static("", id="status"),
            classes="form-container"
        )
        yield Footer()
    
    def on_mount(self):
        self.selected_tables = set()
        self.load_tables()
    
    def load_tables(self):
        try:
            engine = create_engine_safe(self.app.src_cfg)
            tables = fetch_tables(engine, "public")
            
            table = self.query_one("#tables", DataTable)
            table.add_columns("Select", "Table Name", "Row Count")
            
            for tbl in tables:
                try:
                    count = get_row_count(engine, tbl)
                except:
                    count = "N/A"
                table.add_row("☐", tbl, str(count))
            
            self.query_one("#status", Static).update(f"Found {len(tables)} tables")
        except Exception as e:
            self.query_one("#status", Static).update(f"❌ Error: {str(e)}")
    
    def on_data_table_row_selected(self, event: DataTable.RowSelected):
        table = self.query_one("#tables", DataTable)
        row_key = event.row_key
        
        # Get table name from row
        row_data = table.get_row(row_key)
        table_name = row_data[1]
        
        # Toggle selection
        if table_name in self.selected_tables:
            self.selected_tables.remove(table_name)
            table.update_cell(row_key, "Select", "☐")
        else:
            self.selected_tables.add(table_name)
            table.update_cell(row_key, "Select", "☑")
    
    def action_back(self):
        self.app.pop_screen()
    
    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "back":
            self.app.pop_screen()
        elif event.button.id == "select_all":
            table = self.query_one("#tables", DataTable)
            for row_key in table.rows:
                row_data = table.get_row(row_key)
                table_name = row_data[1]
                self.selected_tables.add(table_name)
                table.update_cell(row_key, "Select", "☑")
        elif event.button.id == "deselect_all":
            table = self.query_one("#tables", DataTable)
            for row_key in table.rows:
                table.update_cell(row_key, "Select", "☐")
            self.selected_tables.clear()
        elif event.button.id == "next":
            if not self.selected_tables:
                self.query_one("#status", Static).update("⚠ Please select at least one table")
                return
            
            self.app.selected_tables = list(self.selected_tables)
            self.app.push_screen(TableOptionsScreen())


class TableOptionsScreen(Screen):
    """Screen for configuring table copy options"""
    
    BINDINGS = [
        Binding("escape", "back", "Back"),
    ]
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Static("⚙️ CONFIGURE TABLE COPY OPTIONS", classes="section-title"),
            Static(""),
            Static(f"Selected {len(self.app.selected_tables)} table(s):", classes="subtitle"),
            Static(", ".join(self.app.selected_tables[:5]) + ("..." if len(self.app.selected_tables) > 5 else "")),
            Static(""),
            Label("Copy Mode:"),
            Select(
                [
                    ("Delete & Recreate", "delete_recreate"),
                    ("Copy Delta Only (New Rows)", "delta_only"),
                ],
                id="copy_mode",
                value="delete_recreate"
            ),
            Static(""),
            Horizontal(
                Checkbox("Exclude auto-generated fields", id="exclude_auto", value=False),
                classes="checkbox-row"
            ),
            Static(""),
            Static("Auto-generated fields include:", classes="info"),
            Static("  • Serial columns (SERIAL, BIGSERIAL)"),
            Static("  • Columns with nextval() defaults"),
            Static("  • UUID generation defaults"),
            Static(""),
            Horizontal(
                Button("← Back", id="back", variant="default"),
                Button("Next →", id="next", variant="primary"),
                classes="button-row"
            ),
            classes="form-container"
        )
        yield Footer()
    
    def action_back(self):
        self.app.pop_screen()
    
    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "back":
            self.app.pop_screen()
        elif event.button.id == "next":
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
            "delta": "Incremental Sync (Delta)",
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
            Static("  • This operation cannot be undone"),
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
            return Static("  • All tables will be copied")
        elif self.app.mode == "delta":
            return Static("  • Only new rows will be synchronized")
        elif self.app.mode == "advanced":
            tables_text = f"{len(self.app.selected_tables)} table(s) selected"
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
            Button("View Details", id="toggle_log", variant="default"),
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
            
            if self.app.mode == "full":
                log_progress("Starting full database copy...")
                full_database_copy(src_engine, dst_engine, log_progress)
                
            elif self.app.mode == "delta":
                log_progress("Starting incremental sync...")
                incremental_sync(src_engine, dst_engine, log_progress)
                
            elif self.app.mode == "advanced":
                log_progress("Starting table-level operations...")
                
                for idx, table in enumerate(self.app.selected_tables, 1):
                    log_progress(f"\n[{idx}/{len(self.app.selected_tables)}] Processing: {table}")
                    
                    if self.app.copy_mode == "delete_recreate":
                        table_copy_delete_and_recreate(
                            src_engine, 
                            dst_engine, 
                            table,
                            exclude_auto_generated=self.app.exclude_auto,
                            progress_callback=log_progress
                        )
                    else:  # delta_only
                        table_copy_delta_only(
                            src_engine,
                            dst_engine,
                            table,
                            exclude_auto_generated=self.app.exclude_auto,
                            progress_callback=log_progress
                        )
                
                log_progress(f"\n✅ All {len(self.app.selected_tables)} tables processed successfully!")
            
            status.update("✅ Migration completed successfully!")
            
        except Exception as e:
            log_progress(f"\n❌ ERROR: {str(e)}")
            status.update("❌ Migration failed!")
    
    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "toggle_log":
            log = self.query_one("#log", Log)
            log.display = not log.display


# ---------------- MAIN APP ----------------

class MigrationApp(App):
    """PostgreSQL Migration Tool"""
    
    CSS = """
    Screen {
        align: center middle;
    }
    
    .welcome-container, .form-container, .progress-container {
        width: 80;
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
        margin: 0 2;
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
    
    .checkbox-row {
        height: auto;
        margin: 1 0;
    }
    
    DataTable {
        height: 15;
        margin: 1 0;
    }
    
    Log {
        height: 20;
        border: solid $primary;
        margin: 1 0;
    }
    
    #status {
        text-align: center;
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
        self.copy_mode = "delete_recreate"
        self.exclude_auto = False
    
    def on_mount(self):
        self.push_screen(WelcomeScreen())
    
    def action_quit(self):
        self.exit()


if __name__ == "__main__":
    app = MigrationApp()
    app.run()