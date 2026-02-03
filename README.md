# PostgreSQL Migration Tool v2.0

A production-ready, feature-rich PostgreSQL database migration tool with multiple interfaces (CLI, Terminal UI), comprehensive reporting, and intelligent incremental sync capabilities.

## 🌟 Features

### Core Features
- **Full Database Migration**: Complete database replication with schema and data
- **Incremental Sync**: Hash-based content comparison for efficient delta synchronization
- **Table-Level Operations**: Fine-grained control over individual table migrations
- **Smart Column Handling**: Automatic detection and optional exclusion of auto-generated columns
- **ENUM Type Support**: Proper handling of PostgreSQL ENUM types
- **Migration Tracking**: Comprehensive metadata tracking for all operations

### Fixed Issues (v2.0)
- ✅ Fixed table operations fetching from wrong schema
- ✅ Improved incremental sync with proper tracking
- ✅ Added migration metadata storage
- ✅ Enhanced error handling and recovery
- ✅ Better connection pooling

### New Features (v2.0)
- 📊 **Report Generation**: Excel, PDF, and JSON reports
- 🎯 **Smart Incremental Sync**: Reuses existing migrated schema for same source/destination pairs
- 📈 **Migration History**: Track all operations with detailed metadata
- 🔄 **Connection Pooling**: Improved performance and reliability
- 📝 **Comprehensive Logging**: Detailed operation logs

## 📋 Requirements

- Python 3.8+
- PostgreSQL 10+
- pip (Python package manager)

## 🚀 Installation

### 1. Clone or Download
```bash
# If using git
git clone <repository-url>
cd postgres-migration-tool

# Or just create the directory and copy files
mkdir postgres-migration-tool
cd postgres-migration-tool
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment (Optional)
```bash
cp .env.example .env
# Edit .env with your database credentials
```

## 💻 Usage

### Method 1: Interactive Terminal UI (Recommended)
The easiest way to use the tool:

```bash
python main.py ui
```

Or:
```bash
python frontend.py
```

This launches an interactive terminal interface where you can:
- Select migration mode
- Configure source/destination databases
- Select specific tables (in advanced mode)
- Monitor progress in real-time
- Generate reports

### Method 2: Command-Line Interface (CLI)

#### Full Database Migration
```bash
python main.py full \
  --src-host localhost \
  --src-port 5432 \
  --src-db source_db \
  --src-user postgres \
  --src-password password123 \
  --dst-host localhost \
  --dst-port 5432 \
  --dst-db dest_db \
  --dst-user postgres \
  --dst-password password456 \
  --exclude-auto
```

#### Incremental Sync
```bash
python main.py incremental \
  --src-host localhost \
  --src-db source_db \
  --src-user postgres \
  --src-password password123 \
  --dst-host localhost \
  --dst-db dest_db \
  --dst-user postgres \
  --dst-password password456
```

#### Single Table Migration
```bash
python main.py table \
  --src-host localhost \
  --src-db source_db \
  --src-user postgres \
  --src-password password123 \
  --dst-host localhost \
  --dst-db dest_db \
  --dst-user postgres \
  --dst-password password456 \
  --table users \
  --mode delta \
  --exclude-auto
```

#### Generate Reports
```bash
# Excel Report
python main.py report \
  --host localhost \
  --db dest_db \
  --user postgres \
  --password password456 \
  --format excel \
  --output migration_report.xlsx

# PDF Report
python main.py report \
  --host localhost \
  --db dest_db \
  --user postgres \
  --password password456 \
  --format pdf \
  --output migration_report.pdf

# JSON Summary
python main.py report \
  --host localhost \
  --db dest_db \
  --user postgres \
  --password password456 \
  --format json \
  --output migration_summary.json
```

## 📊 Migration Modes Explained

### 1. Full Database Copy
- **Use Case**: Initial migration or complete refresh
- **Behavior**: 
  - Drops all existing tables in `migrated` schema
  - Recreates schema from source
  - Copies all data
- **Warning**: Destructive operation - existing data will be lost

### 2. Incremental Sync (Smart Delta)
- **Use Case**: Ongoing synchronization
- **Behavior**:
  - Compares data using content hashing
  - Only inserts new/changed rows
  - Preserves existing data
  - Uses same source/destination repeatedly
- **Benefits**: 
  - Efficient - only transfers differences
  - Safe - doesn't delete existing data
  - Intelligent - tracks migration history

### 3. Table-Level Operations
- **Use Case**: Fine-grained control
- **Modes**:
  - **Delete & Recreate**: Drops and recreates specific tables
  - **Delta Only**: Adds only new rows based on primary key
- **Benefits**: Target specific tables, control per-table behavior

## 🔧 Configuration

### Environment Variables (.env file)
```bash
# Source Database
SOURCE_HOST=localhost
SOURCE_PORT=5432
SOURCE_DATABASE=source_db
SOURCE_USER=postgres
SOURCE_PASSWORD=your_password

# Destination Database
DEST_HOST=localhost
DEST_PORT=5432
DEST_DATABASE=dest_db
DEST_USER=postgres
DEST_PASSWORD=your_password

# Migration Settings
MIGRATION_BATCH_SIZE=1000          # Rows per batch
MIGRATION_LOG_LEVEL=INFO           # DEBUG, INFO, WARNING, ERROR
MIGRATION_ENABLE_FK=true           # Enable foreign keys
MIGRATION_ENABLE_INDEXES=true     # Enable indexes
MIGRATION_TARGET_SCHEMA=migrated   # Target schema name
```

### Auto-Generated Columns
The tool can automatically detect and exclude:
- Primary key sequences (serial, bigserial)
- Timestamp columns (created_at, updated_at)
- UUID generators
- Columns with nextval() defaults

## 📁 Project Structure

```
postgres-migration-tool/
├── main.py                 # Main entry point
├── cli.py                  # Command-line interface
├── frontend.py             # Terminal UI (Textual)
├── config.py               # Configuration management
├── database.py             # Database connections and basic operations
├── schema_manager.py       # Schema operations (DDL)
├── data_operations.py      # Data operations (DML)
├── migrations.py           # Core migration logic
├── migration_tracker.py    # Migration metadata tracking
├── report_generator.py     # Report generation (Excel, PDF)
├── requirements.txt        # Python dependencies
├── .env.example           # Environment configuration template
└── README.md              # This file
```

## 🐛 Troubleshooting

### Connection Issues
```bash
# Test source connection
python -c "from database import *; from config import *; cfg = DBConfig('host', '5432', 'db', 'user', 'pass'); print(test_connection(cfg))"
```

### Permission Issues
Ensure your PostgreSQL user has:
- `SELECT` on source tables
- `CREATE` on destination database
- `INSERT` on destination tables

### ENUM Type Errors
The tool automatically handles ENUM types. If you encounter errors:
1. Check that ENUMs exist in source
2. Verify destination user can create types
3. Check for ENUM name conflicts

### Memory Issues
For large databases, adjust:
```bash
MIGRATION_BATCH_SIZE=500  # Reduce batch size
```

## 📈 Performance Tips

1. **Use Incremental Sync** for ongoing operations
2. **Adjust batch size** based on row width
3. **Disable indexes** during initial load, recreate after
4. **Use connection pooling** (already configured)
5. **Monitor progress** using verbose logging

## 🔒 Security Best Practices

1. **Never commit .env file** with real credentials
2. **Use read-only user** for source database when possible
3. **Restrict destination user** to only the migrated schema
4. **Use SSL connections** in production
5. **Audit migration logs** regularly

## 📝 Migration Workflow Example

```bash
# Step 1: Initial full migration
python main.py ui
# Select: Full Database Copy

# Step 2: Verify with reports
python main.py report --host localhost --db dest_db --user postgres --password xxx --format pdf

# Step 3: Ongoing incremental syncs
python main.py incremental --src-host src --src-db mydb ... --dst-host dst --dst-db mydb ...

# Step 4: Monitor with reports
python main.py report --host dst --db mydb --user postgres --password xxx --format excel
```

## 🆘 Support & Contributing

### Getting Help
1. Check this README
2. Review error logs
3. Test connections individually
4. Check PostgreSQL logs

### Reporting Issues
Include:
- PostgreSQL version
- Python version
- Error messages
- Migration mode used
- Sample table structure (if relevant)

## 📜 License

This tool is provided as-is for database migration purposes.

## 🎯 Roadmap

Future enhancements:
- [ ] Web-based UI
- [ ] Parallel table migration
- [ ] Data transformation support
- [ ] Cross-database support (MySQL, etc.)
- [ ] Automated testing
- [ ] Docker containerization

---

**Version**: 2.0
**Last Updated**: 2024
**Author**: Database Migration Tool Team
