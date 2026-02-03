# PostgreSQL Migration Tool - Step-by-Step Guide

## 📦 Complete Setup and Usage Instructions

### Prerequisites Check

Before starting, ensure you have:
- ✅ Python 3.8 or higher installed
- ✅ PostgreSQL databases (source and destination)
- ✅ Database credentials with appropriate permissions
- ✅ Terminal/Command Prompt access

---

## Step 1: Setup Environment

### For Windows:
```cmd
# Open Command Prompt or PowerShell

# Navigate to your desired directory
cd C:\Users\YourName\Documents

# Create project directory
mkdir postgres-migration-tool
cd postgres-migration-tool

# Copy all provided files into this directory
```

### For Linux/Mac:
```bash
# Open Terminal

# Navigate to your desired directory
cd ~/Documents

# Create project directory
mkdir postgres-migration-tool
cd postgres-migration-tool

# Copy all provided files into this directory
```

---

## Step 2: Install Dependencies

### Windows:
```cmd
# Install Python packages
pip install -r requirements.txt

# If you get SSL errors, try:
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt
```

### Linux/Mac:
```bash
# Install Python packages
pip3 install -r requirements.txt

# Or use a virtual environment (recommended):
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

---

## Step 3: Verify Installation

```bash
# Test if all imports work
python -c "import sqlalchemy, textual, reportlab, pandas; print('✅ All dependencies installed')"
```

If you see "✅ All dependencies installed", you're ready to proceed!

---

## Step 4: Running the Tool

### Option A: Interactive UI (Easiest - Recommended for Beginners)

```bash
# Start the interactive terminal UI
python main.py ui
```

**What you'll see:**
1. Welcome screen with 4 migration modes
2. Step-by-step configuration screens
3. Real-time progress logs
4. Success/error messages

**Navigation:**
- Use **Arrow Keys** or **Tab** to move between fields/buttons
- Use **Enter** to click buttons
- Use **Q** to quit anytime
- Use **Escape** to go back

### Option B: Command Line (For Scripts/Automation)

#### Example 1: Full Database Migration
```bash
python main.py full \
  --src-host localhost \
  --src-port 5432 \
  --src-db my_source_database \
  --src-user postgres \
  --src-password my_source_password \
  --dst-host localhost \
  --dst-port 5432 \
  --dst-db my_dest_database \
  --dst-user postgres \
  --dst-password my_dest_password
```

#### Example 2: Incremental Sync
```bash
python main.py incremental \
  --src-host localhost \
  --src-db my_source_database \
  --src-user postgres \
  --src-password my_source_password \
  --dst-host localhost \
  --dst-db my_dest_database \
  --dst-user postgres \
  --dst-password my_dest_password
```

#### Example 3: Migrate Single Table
```bash
python main.py table \
  --src-host localhost \
  --src-db my_source_database \
  --src-user postgres \
  --src-password my_source_password \
  --dst-host localhost \
  --dst-db my_dest_database \
  --dst-user postgres \
  --dst-password my_dest_password \
  --table users \
  --mode delta
```

---

## Step 5: Understanding Migration Modes

### Mode 1: Full Database Copy
**When to use:**
- First-time migration
- Complete refresh needed
- Source and destination schemas differ significantly

**What it does:**
1. Connects to both databases
2. Fetches all tables from source
3. **Drops** existing tables in `migrated` schema (if they exist)
4. Creates fresh schema
5. Copies all data

**⚠️ Warning:** This deletes existing data in destination!

### Mode 2: Incremental Sync (Smart Delta)
**When to use:**
- Regular synchronization
- Keeping databases in sync
- Adding new records only

**What it does:**
1. Compares source and destination data using hashes
2. Identifies new/changed rows
3. **Only inserts** differences
4. Preserves existing destination data
5. Tracks migration history

**✅ Safe:** Doesn't delete existing data!

### Mode 3: Table-Level Operations
**When to use:**
- Need to migrate specific tables
- Different tables need different strategies

**Sub-modes:**
- **Delete & Recreate:** Drops and recreates selected tables
- **Delta Only:** Adds only new rows to selected tables

### Mode 4: Generate Reports
**When to use:**
- After migration to verify
- Regular auditing
- Compliance reporting

**Report types:**
- **Excel:** Detailed migration history with multiple sheets
- **PDF:** Professional formatted report
- **JSON:** Machine-readable summary

---

## Step 6: Common Workflows

### Workflow 1: Initial Setup
```bash
# Day 1: Full migration
python main.py ui
> Select: Full Database Copy
> Configure source and destination
> Start migration

# Verify
python main.py report --host localhost --db dest_db --user postgres --password xxx --format pdf
```

### Workflow 2: Daily Sync
```bash
# Every day: Incremental sync
python main.py incremental \
  --src-host production-db.example.com \
  --src-db prod_db \
  --src-user readonly_user \
  --src-password xxx \
  --dst-host localhost \
  --dst-db dev_db \
  --dst-user postgres \
  --dst-password xxx

# Check what was synced
python main.py report --host localhost --db dev_db --user postgres --password xxx --format excel
```

### Workflow 3: Selective Table Update
```bash
# Update only specific tables
python main.py ui
> Select: Table-Level Operations
> Select tables: users, orders, products
> Mode: Delta Only
> Start migration
```

---

## Step 7: File Outputs

### Where Files Are Created

**Excel Reports:**
- Location: Current directory
- Filename: `migration_report_<database_name>.xlsx`
- Contents: 
  - Sheet 1: Migration History
  - Sheet 2: Summary Statistics
  - Sheet 3: By Table Analysis

**PDF Reports:**
- Location: Current directory
- Filename: `migration_report_<database_name>.pdf`
- Contents:
  - Title page with metadata
  - Summary statistics
  - Recent migrations table
  - Current state comparison (if source provided)

**Logs:**
- Displayed in terminal/UI
- Can redirect: `python main.py full ... > migration.log 2>&1`

---

## Step 8: Checking Results

### In PostgreSQL:
```sql
-- Connect to destination database
psql -h localhost -U postgres -d dest_db

-- List all schemas
\dn

-- List tables in migrated schema
\dt migrated.*

-- Check row count
SELECT COUNT(*) FROM migrated.users;

-- View migration metadata
SELECT * FROM migrated._migration_metadata ORDER BY started_at DESC LIMIT 10;
```

### Using the Tool:
```bash
# Generate a report
python main.py report \
  --host localhost \
  --db dest_db \
  --user postgres \
  --password xxx \
  --format excel

# Open the Excel file to see detailed stats
```

---

## Step 9: Troubleshooting

### Problem: "Connection failed"
**Solution:**
```bash
# Test connection manually
python -c "
from config import DBConfig
from database import test_connection
cfg = DBConfig('localhost', '5432', 'your_db', 'postgres', 'password')
print(test_connection(cfg))
"
```

### Problem: "Permission denied"
**Solution:**
```sql
-- Run in PostgreSQL as superuser
GRANT CONNECT ON DATABASE dest_db TO your_user;
GRANT CREATE ON DATABASE dest_db TO your_user;
GRANT ALL ON SCHEMA migrated TO your_user;
```

### Problem: "Table already exists"
**Solution:**
- Use "Incremental Sync" mode instead of "Full Copy"
- Or manually drop tables: `DROP SCHEMA migrated CASCADE;`

### Problem: "Memory error with large tables"
**Solution:**
Edit `.env` file:
```bash
MIGRATION_BATCH_SIZE=500  # Reduce from 1000
```

### Problem: Module not found
**Solution:**
```bash
# Reinstall dependencies
pip install -r requirements.txt --force-reinstall
```

---

## Step 10: Advanced Features

### Using Environment Variables
```bash
# Create .env file
cp .env.example .env

# Edit .env with your credentials
nano .env  # or use any text editor

# Run without specifying credentials
python main.py full  # Reads from .env
```

### Scheduling Regular Syncs

**Windows (Task Scheduler):**
1. Open Task Scheduler
2. Create Basic Task
3. Trigger: Daily
4. Action: Start a program
5. Program: `python`
6. Arguments: `C:\path\to\main.py incremental --src-host ... --dst-host ...`

**Linux (Cron):**
```bash
# Edit crontab
crontab -e

# Add line (runs daily at 2 AM):
0 2 * * * cd /path/to/postgres-migration-tool && /usr/bin/python3 main.py incremental --src-host ... --dst-host ... >> /var/log/migration.log 2>&1
```

### Monitoring
```bash
# Watch migration in real-time
python main.py incremental ... 2>&1 | tee migration.log

# Check log file
tail -f migration.log
```

---

## Quick Reference Commands

```bash
# Start UI
python main.py ui

# Full migration
python main.py full --src-host SRC --src-db SRCDB --src-user USER --src-password PWD --dst-host DST --dst-db DSTDB --dst-user USER --dst-password PWD

# Incremental sync
python main.py incremental --src-host SRC --src-db SRCDB --src-user USER --src-password PWD --dst-host DST --dst-db DSTDB --dst-user USER --dst-password PWD

# Single table
python main.py table --src-host SRC --src-db SRCDB --src-user USER --src-password PWD --dst-host DST --dst-db DSTDB --dst-user USER --dst-password PWD --table TABLE_NAME --mode delta

# Generate report
python main.py report --host HOST --db DB --user USER --password PWD --format excel --output report.xlsx

# Help
python main.py --help
python main.py full --help
python main.py incremental --help
```

---

## 🎯 You're Ready!

Start with:
```bash
python main.py ui
```

And follow the interactive prompts. The tool will guide you through each step!

---

## 📞 Need Help?

1. Check the error message carefully
2. Review this guide's troubleshooting section
3. Check PostgreSQL logs: `tail -f /var/log/postgresql/postgresql-*.log`
4. Verify credentials and permissions
5. Test with a small test database first

Good luck with your migration! 🚀
