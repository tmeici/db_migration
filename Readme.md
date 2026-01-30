# PostgreSQL Migration Tool - Complete Guide 🗄️

A professional, production-ready database migration tool with an interactive terminal UI for PostgreSQL databases. Built with safety, efficiency, and ease of use in mind.

---

## 📑 Table of Contents

1. [Overview](#overview)
2. [Key Features](#key-features)
3. [When to Use Each Mode](#when-to-use-each-mode)
4. [Prerequisites](#prerequisites)
5. [Installation](#installation)
6. [Quick Start](#quick-start)
7. [Migration Modes Explained](#migration-modes-explained)
8. [Architecture](#architecture)
9. [Safety Guarantees](#safety-guarantees)
10. [Usage Examples](#usage-examples)
11. [Troubleshooting](#troubleshooting)
12. [Best Practices](#best-practices)
13. [Advanced Features](#advanced-features)
14. [API Reference](#api-reference)

---

## Overview

This tool provides a safe, user-friendly way to migrate PostgreSQL databases with three distinct modes:

- **Full Database Copy**: Complete database replication
- **Incremental Sync**: Delta synchronization (only new rows)
- **Table Operations**: Granular control over specific tables

**Critical Safety Feature**: All migrations create tables in a separate `migrated` schema, ensuring your original `public` schema remains **completely untouched**.

---

## Key Features

### 🎯 Three Migration Modes
1. **Full Database Copy** - Complete replication
2. **Incremental Sync** - Delta updates only
3. **Table Operations** - Fine-grained control

### 🛡️ Safety Features
- ✅ Separate `migrated` schema (original data never modified)
- ✅ Connection testing before operations
- ✅ Transaction safety
- ✅ Comprehensive error handling
- ✅ Progress tracking with detailed logs

### 💻 User Interface
- ✅ Beautiful terminal UI (Textual framework)
- ✅ Step-by-step guided workflow
- ✅ Real-time progress monitoring
- ✅ Visual table selection
- ✅ Clear status messages

### 🔧 Technical Features
- ✅ All PostgreSQL data types supported (including JSON/JSONB)
- ✅ Automatic sequence handling
- ✅ Primary key detection
- ✅ Auto-generated field exclusion
- ✅ NULL value handling
- ✅ Comprehensive statistics

---

## When to Use Each Mode

### 📦 Full Database Copy

**Use When:**
- ✅ First-time setup of a new environment
- ✅ Creating complete backups
- ✅ Setting up staging/development databases
- ✅ You want a complete fresh copy
- ✅ Schema or structure has changed
- ✅ You need to reset all data

**Don't Use When:**
- ❌ You want to preserve existing data in destination
- ❌ You only need to sync new records
- ❌ Database is very large and you have bandwidth/time constraints

**Prerequisites:**
- Source database accessible
- Destination database accessible
- Sufficient storage space in destination
- Write permissions on destination database
- CREATE SCHEMA privilege

**What Happens:**
```
1. Creates 'migrated' schema in destination
2. DROPS all existing tables in 'migrated' schema
3. Recreates all tables with fresh structure
4. Copies ALL data from source
5. Result: Complete replica in destination.migrated
```

**Example Scenario:**
```
Situation: Setting up a staging environment
Source: production_db (1000 tables, 10GB)
Destination: staging_db (empty or old data)
Action: Full Database Copy
Result: Complete copy of production in staging_db.migrated
Time: ~30 minutes for 10GB
```

---

### 🔄 Incremental Sync (Delta Copy)

**Use When:**
- ✅ You want to keep destination data updated
- ✅ Only new records need to be copied
- ✅ Running regular synchronization (hourly/daily)
- ✅ Bandwidth/time is limited
- ✅ Existing data should be preserved
- ✅ You have primary keys on all tables

**Don't Use When:**
- ❌ Tables don't have primary keys
- ❌ Records can be modified (updates won't sync)
- ❌ Records can be deleted (deletions won't sync)
- ❌ You need a complete refresh

**Prerequisites:**
- **CRITICAL**: Tables MUST have primary keys
- Source database accessible
- Destination database accessible
- Migrated schema may or may not exist (will be created if needed)
- Write permissions on destination

**What Happens:**
```
1. Checks if 'migrated' schema exists (creates if needed)
2. For each table:
   a. If table doesn't exist: Creates it and copies all data
   b. If table exists:
      - Fetches existing primary keys from destination
      - Compares with source
      - Copies ONLY rows with new primary keys
3. Result: Destination has all data, preserving existing records
```

**How It Works:**
```python
# Pseudo-code
source_ids = {1, 2, 3, 4, 5, 6, 7}
destination_ids = {1, 2, 3, 4, 5}
new_rows = source_ids - destination_ids  # {6, 7}
# Only rows 6 and 7 are copied
```

**Example Scenario:**
```
Situation: Daily sync from production to analytics
Source: production_db (1000 new orders/day)
Destination: analytics_db.migrated (has yesterday's data)
Action: Incremental Sync
Result: Only today's 1000 new orders copied
Time: ~2 minutes
```

**Important Notes:**
- ⚠️ Only detects NEW rows (by primary key)
- ⚠️ Does NOT sync updates to existing rows
- ⚠️ Does NOT sync deletions
- ⚠️ Tables without primary keys are skipped

---

### 📋 Table Operations

**Use When:**
- ✅ You need control over specific tables
- ✅ Only certain tables need migration
- ✅ You want to exclude auto-generated fields
- ✅ Different tables need different strategies
- ✅ Testing migrations on subset of data

**Don't Use When:**
- ❌ You need to migrate entire database quickly
- ❌ All tables need same treatment

**Prerequisites:**
- Source database accessible
- Destination database accessible
- Tables selected by user
- Write permissions on destination
- For delta mode: Tables must have primary keys

#### Option A: Delete & Recreate

**Use When:**
- ✅ You want fresh data for specific tables
- ✅ Schema has changed
- ✅ You want to reset specific tables
- ✅ Don't need to preserve existing data

**What Happens:**
```
1. DROPS selected tables in migrated schema
2. Recreates tables
3. Copies ALL data
4. Optional: Exclude auto-generated fields
```

**Example:**
```
Tables: users, products (3 out of 100 tables)
Mode: Delete & Recreate
Exclude Auto-generated: Yes
Result: Fresh copy of users & products without serial IDs
```

#### Option B: Delta Only

**Use When:**
- ✅ Preserve existing data in selected tables
- ✅ Only add new records
- ✅ Selective incremental sync

**Prerequisites:**
- Tables MUST have primary keys

**What Happens:**
```
1. If table doesn't exist: Creates and copies all
2. If table exists: Copies only new rows (like incremental sync)
3. Optional: Exclude auto-generated fields
```

**Example:**
```
Tables: orders, invoices
Mode: Delta Only
Result: Only new orders/invoices added, existing preserved
```

---

### Exclude Auto-Generated Fields Feature

**What Are Auto-Generated Fields?**
- SERIAL columns (auto-incrementing IDs)
- Columns with `nextval()` sequences
- UUID generation defaults
- Any column with automatic values

**When to Exclude:**
- ✅ Copying to a system that will generate its own IDs
- ✅ Avoiding ID conflicts
- ✅ Importing data into existing tables
- ✅ Data migration across different systems

**When NOT to Exclude:**
- ❌ You need exact replica with same IDs
- ❌ Foreign key relationships depend on IDs
- ❌ You're doing a backup

**Example:**
```sql
-- Source table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,        -- Auto-generated
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW()  -- Auto-generated
);

-- With exclude_auto_generated = True
CREATE TABLE migrated.users (
    email VARCHAR(255),
    -- id and created_at excluded
);
```

---

## Prerequisites

### System Requirements
- Python 3.8 or higher
- PostgreSQL 9.6 or higher
- Network access to source and destination databases
- Sufficient disk space on destination

### Database Requirements

#### Source Database
- ✅ Read access (SELECT permission)
- ✅ Access to `information_schema`
- ✅ Network connectivity

#### Destination Database
- ✅ Write access (CREATE, INSERT permissions)
- ✅ CREATE SCHEMA privilege
- ✅ Sufficient storage space
- ✅ Network connectivity

### User Permissions

Minimum required permissions:

```sql
-- Source database (read-only)
GRANT CONNECT ON DATABASE source_db TO migration_user;
GRANT USAGE ON SCHEMA public TO migration_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO migration_user;

-- Destination database (read-write)
GRANT CONNECT ON DATABASE dest_db TO migration_user;
GRANT CREATE ON DATABASE dest_db TO migration_user;
GRANT ALL PRIVILEGES ON SCHEMA migrated TO migration_user;
```

### Network Requirements
- Port 5432 (or custom PostgreSQL port) accessible
- Firewall rules allowing connection
- No proxy issues

---

## Installation

### Step 1: Clone or Download

```bash
# If using git
git clone <repository-url>
cd postgresql-migration-tool

# Or extract downloaded files
unzip postgresql-migration-tool.zip
cd postgresql-migration-tool
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

**Required packages:**
- sqlalchemy>=2.0.0
- psycopg2-binary>=2.9.0
- textual>=0.50.0

### Step 3: Verify Installation

```bash
python test_setup.py
```

This will test:
- Module imports
- Database connectivity
- Schema operations
- Migration readiness

---

## Quick Start

### Running the Application

```bash
python frontend.py
```

### First Migration - Walkthrough

#### Step 1: Choose Mode
```
📦 Full Database Copy          → Complete replication
🔄 Incremental Sync (Delta)    → Only new rows
📋 Table-Level Operations       → Granular control
```

Select based on your needs.

#### Step 2: Configure Source Database
```
Host: localhost
Port: 5432
Database: source_db
User: postgres
Password: ••••••••
```

Click **"Test Connection"** - Should see ✅

#### Step 3: Configure Destination Database
```
Host: localhost  
Port: 5432
Database: dest_db
User: postgres
Password: ••••••••
```

Click **"Test Connection"** - Should see ✅

#### Step 4: (Advanced Mode Only) Select Tables
- View all tables with row counts
- Select specific tables
- Choose copy mode
- Toggle auto-generated field exclusion

#### Step 5: Confirm and Execute
Review settings and click **"🚀 Start Migration"**

#### Step 6: Monitor Progress
Watch real-time logs:
```
[1/9] Processing table: users
[1/9] Creating table schema...
[1/9] Copying 150 rows...
[1/9] ✓ users: 150 rows copied
```

#### Step 7: Verify Results
```sql
-- Connect to destination database
psql -h localhost -U postgres -d dest_db

-- List schemas
\dn

-- List tables in migrated schema
\dt migrated.*

-- Check data
SELECT COUNT(*) FROM migrated.users;
SELECT * FROM migrated.users LIMIT 5;
```

---

## Migration Modes Explained

### Visual Comparison

```
┌─────────────────────────────────────────────────────────┐
│                   SOURCE DATABASE                        │
│                   (public schema)                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐             │
│  │  users   │  │  orders  │  │ products │             │
│  │ 1000 rows│  │ 5000 rows│  │ 500 rows │             │
│  └──────────┘  └──────────┘  └──────────┘             │
└─────────────────────────────────────────────────────────┘
                        │
                        │ Migration
                        ▼
┌─────────────────────────────────────────────────────────┐
│               DESTINATION DATABASE                       │
├─────────────────────────────────────────────────────────┤
│  public schema          │  migrated schema              │
│  (UNTOUCHED)           │  (NEW DATA)                   │
│                        │                                │
│  [Original tables]     │  ┌──────────┐                 │
│                        │  │  users   │  ← Migrated     │
│                        │  │  orders  │  ← Data         │
│                        │  │ products │  ← Here         │
│                        │  └──────────┘                 │
└─────────────────────────────────────────────────────────┘
```

### Mode 1: Full Database Copy

**Workflow:**
```
Source                  Destination (migrated schema)
────────               ───────────────────────────────
users (1000 rows)   →  DROP TABLE IF EXISTS users
                       CREATE TABLE users
                       INSERT 1000 rows

orders (5000 rows)  →  DROP TABLE IF EXISTS orders  
                       CREATE TABLE orders
                       INSERT 5000 rows

products (500 rows) →  DROP TABLE IF EXISTS products
                       CREATE TABLE products
                       INSERT 500 rows
```

**Statistics Example:**
```
✅ Full database copy completed!
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Total tables copied: 9
Total rows copied: 6,500
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### Mode 2: Incremental Sync

**First Run (Tables Don't Exist):**
```
Source                  Destination (migrated schema)
────────               ───────────────────────────────
users (1000 rows)   →  CREATE TABLE users (no DROP)
                       INSERT 1000 rows

orders (5000 rows)  →  CREATE TABLE orders (no DROP)
                       INSERT 5000 rows
```

**Second Run (Tables Exist, New Data):**
```
Source                  Destination (migrated schema)
────────               ───────────────────────────────
users (1050 rows)   →  Table exists
  - Check PKs          - Has: {1..1000}
  - New: {1001..1050}  - Insert ONLY: {1001..1050}
                       
orders (5200 rows)  →  Table exists
  - Check PKs          - Has: {1..5000}
  - New: {5001..5200}  - Insert ONLY: {5001..5200}
```

**Statistics Example:**
```
✅ Incremental sync completed!
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Total tables processed: 9
  • New tables created: 0
  • Existing tables synced: 8
  • Tables up to date: 5
  • Tables skipped (no PK): 1
  • Total new rows synced: 250
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### Mode 3: Table Operations

**Delete & Recreate Mode:**
```
Selected: users, orders (2 out of 9 tables)
Mode: Delete & Recreate
Exclude Auto-generated: Yes
────────────────────────────────────────
users (1000 rows)   →  DROP TABLE users
                       CREATE TABLE users (no SERIAL id)
                       INSERT 1000 rows (excluding id)

orders (5000 rows)  →  DROP TABLE orders
                       CREATE TABLE orders (no SERIAL id)  
                       INSERT 5000 rows (excluding id)

[products, etc. - NOT touched]
```

**Delta Only Mode:**
```
Selected: users, orders (2 out of 9 tables)
Mode: Delta Only
────────────────────────────────────────
users               →  Table exists
  - Has: {1..1000}     - Insert new: {1001..1050}
  
orders              →  Table exists
  - Has: {1..5000}     - Insert new: {5001..5200}

[products, etc. - NOT touched]
```

---

## Architecture

### Project Structure

```
postgresql-migration-tool/
│
├── backend.py                 # Core migration engine (750+ lines)
│   ├── Database connections
│   ├── Schema introspection
│   ├── Migration strategies
│   └── Data operations
│
├── frontend.py               # Terminal UI (550+ lines)
│   ├── Interactive screens
│   ├── User input handling
│   ├── Progress display
│   └── Workflow management
│
├── requirements.txt          # Dependencies
├── README_COMPLETE.md        # This file
├── FIXES_AND_IMPROVEMENTS.md # Technical documentation
├── QUICKSTART.md            # Quick tutorial
├── example_usage.py         # Programmatic examples
├── test_setup.py            # Installation verification
└── config.example.py        # Configuration template
```

### Backend Architecture

```
┌─────────────────────────────────────────────┐
│           Backend (backend.py)              │
├─────────────────────────────────────────────┤
│                                             │
│  ┌──────────────────────────────────────┐  │
│  │    Connection Management             │  │
│  │  - create_engine_safe()              │  │
│  │  - test_connection()                 │  │
│  └──────────────────────────────────────┘  │
│                                             │
│  ┌──────────────────────────────────────┐  │
│  │    Schema Operations                 │  │
│  │  - fetch_tables()                    │  │
│  │  - fetch_columns()                   │  │
│  │  - fetch_primary_key()               │  │
│  │  - create_table_schema_if_not_exists()│ │
│  │  - recreate_table_schema()           │  │
│  └──────────────────────────────────────┘  │
│                                             │
│  ┌──────────────────────────────────────┐  │
│  │    Data Operations                   │  │
│  │  - fetch_all_rows()                  │  │
│  │  - prepare_row() [Type handling]    │  │
│  │  - insert_rows()                     │  │
│  └──────────────────────────────────────┘  │
│                                             │
│  ┌──────────────────────────────────────┐  │
│  │    Migration Modes                   │  │
│  │  - full_database_copy()              │  │
│  │  - incremental_sync()                │  │
│  │  - table_copy_delete_and_recreate()  │  │
│  │  - table_copy_delta_only()           │  │
│  └──────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
```

### Frontend Architecture

```
┌─────────────────────────────────────────────┐
│          Frontend (frontend.py)             │
├─────────────────────────────────────────────┤
│                                             │
│  Screen Flow:                               │
│  ┌────────────┐                             │
│  │  Welcome   │ → Choose mode               │
│  └─────┬──────┘                             │
│        ▼                                    │
│  ┌────────────┐                             │
│  │ Source DB  │ → Enter credentials         │
│  └─────┬──────┘                             │
│        ▼                                    │
│  ┌────────────┐                             │
│  │  Dest DB   │ → Enter credentials         │
│  └─────┬──────┘                             │
│        ▼                                    │
│  ┌────────────┐                             │
│  │ [Advanced: │ → Select tables (optional)  │
│  │  Tables]   │                             │
│  └─────┬──────┘                             │
│        ▼                                    │
│  ┌────────────┐                             │
│  │  Confirm   │ → Review settings           │
│  └─────┬──────┘                             │
│        ▼                                    │
│  ┌────────────┐                             │
│  │  Progress  │ → Watch migration           │
│  └────────────┘                             │
└─────────────────────────────────────────────┘
```

### Data Flow

```
┌────────────┐
│   User     │
└─────┬──────┘
      │
      ▼
┌────────────────┐
│   Frontend     │ ← Interactive UI
│   (Textual)    │
└─────┬──────────┘
      │
      ▼
┌────────────────┐
│   Backend      │ ← Business logic
│   (SQLAlchemy) │
└─────┬──────────┘
      │
      ▼
┌────────────────────────────────┐
│   Source DB    │    Dest DB    │
│   (public)     │  (migrated)   │
└────────────────────────────────┘
```

---

## Safety Guarantees

### 1. Separate Schema Isolation

**Guarantee:** Your original `public` schema is **NEVER** modified.

```sql
-- Destination database structure
public schema          ← UNTOUCHED (your original data)
  └── [Original tables never modified]

migrated schema        ← ALL migration data goes here
  └── [All migrated tables]
```

**How to rollback:**
```sql
-- Simply drop the migrated schema
DROP SCHEMA migrated CASCADE;

-- Your public schema remains intact
```

### 2. Transaction Safety

- Each table operation runs in a transaction
- If one table fails, others continue
- No partial data commits

### 3. Connection Testing

- Both databases tested before migration starts
- Clear error messages if connection fails
- No operations attempted on unreachable databases

### 4. Primary Key Validation

- Incremental sync validates primary keys exist
- Tables without PKs are skipped (with warning)
- No silent failures

### 5. Type Safety

- All PostgreSQL data types properly handled
- JSON/JSONB automatically serialized
- NULL values handled gracefully

### 6. Error Isolation

- One table failure doesn't stop entire migration
- Detailed error messages for debugging
- Progress preserved (already-copied tables remain)

---

## Usage Examples

### Example 1: Initial Database Setup

**Scenario:** Setting up a new staging environment

```bash
# Run the tool
python frontend.py

# Selections:
Mode: Full Database Copy
Source: production_db
Destination: staging_db

# Result:
✅ Complete replica in staging_db.migrated
```

**Verification:**
```sql
-- Connect to staging
psql -h localhost -U postgres -d staging_db

-- Check tables
\dt migrated.*

-- Compare counts
SELECT 'source' as db, COUNT(*) FROM public.users
UNION ALL
SELECT 'destination', COUNT(*) FROM migrated.users;
```

### Example 2: Daily Analytics Sync

**Scenario:** Update analytics database daily with new orders

```bash
# Run daily (cron job or scheduled task)
python frontend.py

# Selections:
Mode: Incremental Sync
Source: production_db
Destination: analytics_db

# Result:
✅ Only new rows copied (fast and efficient)
```

**Cron job setup:**
```bash
# Edit crontab
crontab -e

# Add daily sync at 2 AM
0 2 * * * cd /path/to/tool && python frontend.py
```

### Example 3: Selective Table Refresh

**Scenario:** Refresh only customer-related tables

```bash
python frontend.py

# Selections:
Mode: Table-Level Operations
Tables: customers, orders, invoices
Copy Mode: Delete & Recreate
Exclude Auto-generated: Yes

# Result:
✅ Only selected tables refreshed without IDs
```

### Example 4: Programmatic Usage

```python
from backend import (
    DBConfig,
    create_engine_safe,
    incremental_sync
)

# Configure databases
source = DBConfig(
    host="prod-server",
    port="5432",
    database="production",
    user="readonly_user",
    password="secure_password"
)

destination = DBConfig(
    host="analytics-server",
    port="5432",
    database="analytics",
    user="writer_user",
    password="secure_password"
)

# Create engines
src_engine = create_engine_safe(source)
dst_engine = create_engine_safe(destination)

# Run incremental sync
def log(msg):
    print(f"[{datetime.now()}] {msg}")

incremental_sync(src_engine, dst_engine, log)
```

---

## Troubleshooting

### Connection Issues

**Problem:** "Connection failed: could not connect to server"

**Solutions:**
1. Check PostgreSQL is running:
   ```bash
   sudo systemctl status postgresql
   ```

2. Verify host and port:
   ```bash
   psql -h localhost -p 5432 -U postgres -l
   ```

3. Check firewall:
   ```bash
   sudo ufw status
   sudo firewall-cmd --list-all
   ```

4. Test network connectivity:
   ```bash
   telnet db-host 5432
   nc -zv db-host 5432
   ```

### Permission Errors

**Problem:** "permission denied for schema migrated"

**Solution:**
```sql
-- Grant necessary permissions
GRANT CREATE ON DATABASE dest_db TO your_user;
GRANT ALL PRIVILEGES ON SCHEMA migrated TO your_user;
```

**Problem:** "permission denied for table users"

**Solution:**
```sql
-- Source database
GRANT SELECT ON ALL TABLES IN SCHEMA public TO your_user;

-- For future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
GRANT SELECT ON TABLES TO your_user;
```

### Primary Key Issues

**Problem:** "Table has no primary key - cannot perform delta copy"

**Solutions:**

1. **Add primary key:**
   ```sql
   ALTER TABLE problem_table ADD PRIMARY KEY (id);
   ```

2. **Use Full Copy instead:**
   - Switch to Full Database Copy mode
   - Or use Delete & Recreate in Table Operations

3. **Skip the table:**
   - Table will be skipped in Incremental mode (shown in logs)

### Memory Issues

**Problem:** Out of memory during large table copy

**Solutions:**

1. **Use Table Operations:**
   - Migrate tables one at a time
   - Smaller batches easier on memory

2. **Increase system memory:**
   ```bash
   # Linux - increase swap
   sudo fallocate -l 4G /swapfile
   sudo mkswap /swapfile
   sudo swapon /swapfile
   ```

3. **Optimize batch size:**
   ```python
   # In backend.py, modify BATCH_SIZE
   # Currently: 100 rows per batch
   ```

### Sequence Errors

**Problem:** "sequence does not exist"

**Status:** ✅ **FIXED** in current version

The tool now automatically:
- Detects sequence-based defaults
- Creates sequences in `migrated` schema
- Updates references appropriately

If you still see this error:
1. Update to latest version
2. Report as a bug

### JSON Type Errors

**Problem:** "can't adapt type 'dict'"

**Status:** ✅ **FIXED** in current version

The tool now automatically serializes JSON/JSONB columns.

If you still see this error:
1. Update to latest version
2. Check PostgreSQL driver: `pip install --upgrade psycopg2-binary`

### Slow Performance

**Problem:** Migration is very slow

**Solutions:**

1. **Check network speed:**
   ```bash
   # Test bandwidth
   iperf -c destination-host
   ```

2. **Use compression:**
   ```python
   # Add to connection string
   url = f"postgresql://...?sslmode=require&compression=true"
   ```

3. **Increase batch size:**
   - Modify `BATCH_SIZE` in backend.py
   - Trade-off: memory vs. speed

4. **Check database load:**
   ```sql
   -- Check active connections
   SELECT count(*) FROM pg_stat_activity;
   
   -- Check slow queries
   SELECT query, query_start 
   FROM pg_stat_activity 
   WHERE state = 'active';
   ```

---

## Best Practices

### 1. Always Test Connections First

```bash
# Use the built-in test button
# Or test manually:
psql -h host -p port -U user -d database -c "SELECT 1"
```

### 2. Start with Small Datasets

For first-time use:
- Test with Table Operations on 1-2 tables
- Verify results
- Then proceed to full migrations

### 3. Schedule Off-Peak Hours

```bash
# For production migrations
# Schedule during low-traffic periods
0 2 * * * /path/to/migration_script.sh
```

### 4. Monitor Disk Space

```bash
# Check destination database size
psql -c "SELECT pg_size_pretty(pg_database_size('dest_db'))"

# Check available space
df -h /var/lib/postgresql
```

### 5. Regular Backups

```bash
# Before major migrations
pg_dump -h localhost -U postgres source_db > backup.sql
```

### 6. Use Read-Only Users

```sql
-- Create read-only user for source
CREATE ROLE migration_reader WITH LOGIN PASSWORD 'secure_pass';
GRANT CONNECT ON DATABASE source_db TO migration_reader;
GRANT USAGE ON SCHEMA public TO migration_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO migration_reader;
```

### 7. Document Your Migrations

```bash
# Keep a log
echo "$(date): Migrated production to staging" >> migration_log.txt
```

### 8. Verify After Migration

```sql
-- Compare row counts
SELECT 
    src.relname as table_name,
    src.n_live_tup as source_rows,
    dst.n_live_tup as dest_rows,
    src.n_live_tup - dst.n_live_tup as difference
FROM pg_stat_user_tables src
JOIN pg_stat_user_tables dst ON src.relname = dst.relname
WHERE dst.schemaname = 'migrated';
```

### 9. Clean Up Old Migrations

```sql
-- Remove old migrated schema if needed
DROP SCHEMA migrated CASCADE;

-- Or rename for archival
ALTER SCHEMA migrated RENAME TO migrated_archive_20240129;
```

### 10. Use Transactions for Safety

All operations are already transaction-safe, but for custom scripts:

```python
with engine.begin() as conn:
    # All operations here are in a transaction
    # Auto-rollback on error
    conn.execute(text("..."))
```

---

## Advanced Features

### 1. Custom Configuration

Create `config.py`:

```python
from backend import DBConfig

# Define your databases
PROD_DB = DBConfig(
    host="prod.example.com",
    port="5432",
    database="production",
    user="readonly",
    password="secure_pass"
)

STAGING_DB = DBConfig(
    host="staging.example.com",
    port="5432",
    database="staging",
    user="admin",
    password="secure_pass"
)
```

Use in scripts:

```python
from config import PROD_DB, STAGING_DB
from backend import create_engine_safe, full_database_copy

src = create_engine_safe(PROD_DB)
dst = create_engine_safe(STAGING_DB)

full_database_copy(src, dst)
```

### 2. Selective Table Migration

```python
from backend import table_copy_delta_only

tables_to_sync = ['users', 'orders', 'products']

for table in tables_to_sync:
    print(f"Syncing {table}...")
    table_copy_delta_only(
        src_engine,
        dst_engine,
        table,
        exclude_auto_generated=True,
        progress_callback=print
    )
```

### 3. Custom Progress Callbacks

```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def custom_callback(message):
    logger.info(message)
    # Also send to monitoring system
    send_to_datadog(message)

incremental_sync(src, dst, custom_callback)
```

### 4. Scheduled Automation

**Linux (cron):**
```bash
# /etc/cron.d/db-migration
0 2 * * * user cd /path/to/tool && python migration_script.py >> /var/log/migration.log 2>&1
```

**Windows (Task Scheduler):**
```powershell
# Create scheduled task
$action = New-ScheduledTaskAction -Execute "python" -Argument "C:\path\to\migration_script.py"
$trigger = New-ScheduledTaskTrigger -Daily -At 2am
Register-ScheduledTask -Action $action -Trigger $trigger -TaskName "DB Migration"
```

### 5. Notifications

```python
import smtplib
from email.mime.text import MIMEText

def notify_completion(stats):
    msg = MIMEText(f"Migration completed: {stats}")
    msg['Subject'] = 'Database Migration Complete'
    msg['From'] = 'migration@example.com'
    msg['To'] = 'admin@example.com'
    
    smtp = smtplib.SMTP('smtp.gmail.com', 587)
    smtp.starttls()
    smtp.login('user', 'pass')
    smtp.send_message(msg)
    smtp.quit()

# Use after migration
full_database_copy(src, dst, progress_callback=log)
notify_completion("Success!")
```

### 6. Parallel Table Processing

```python
from concurrent.futures import ThreadPoolExecutor
import threading

def migrate_table(table):
    # Each table in its own thread
    local_src = create_engine_safe(SOURCE_CONFIG)
    local_dst = create_engine_safe(DEST_CONFIG)
    
    table_copy_delta_only(local_src, local_dst, table)
    print(f"✅ {table} completed")

tables = fetch_tables(engine, "public")

# Migrate 5 tables at a time
with ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(migrate_table, tables)
```

### 7. Data Validation

```python
def validate_migration(src, dst, table):
    """Verify row counts match"""
    src_count = get_row_count(src, table, "public")
    dst_count = get_row_count(dst, table, "migrated")
    
    if src_count == dst_count:
        print(f"✅ {table}: {src_count} rows (verified)")
        return True
    else:
        print(f"⚠️ {table}: Source={src_count}, Dest={dst_count}")
        return False

# After migration
tables = fetch_tables(src_engine, "public")
for table in tables:
    validate_migration(src_engine, dst_engine, table)
```

---

## API Reference

### Database Configuration

```python
@dataclass
class DBConfig:
    host: str       # Database host
    port: str       # Port number
    database: str   # Database name
    user: str       # Username
    password: str   # Password
```

### Connection Functions

```python
def create_engine_safe(cfg: DBConfig) -> Engine:
    """Create SQLAlchemy engine from config"""
    
def test_connection(cfg: DBConfig) -> tuple[bool, str]:
    """Test database connection
    Returns: (success: bool, message: str)
    """
```

### Schema Functions

```python
def fetch_tables(engine: Engine, schema: str = "public") -> List[str]:
    """Get list of table names"""
    
def fetch_columns(engine: Engine, schema: str, table: str) -> List[Dict]:
    """Get column metadata for table"""
    
def fetch_primary_key(engine: Engine, schema: str, table: str) -> Optional[str]:
    """Get primary key column name"""
```

### Migration Functions

```python
def full_database_copy(
    src: Engine, 
    dst: Engine,
    progress_callback: Optional[Callable] = None
):
    """Complete database copy with DROP and CREATE"""

def incremental_sync(
    src: Engine, 
    dst: Engine,
    progress_callback: Optional[Callable] = None
):
    """Sync only new rows (preserves existing data)"""

def table_copy_delete_and_recreate(
    src: Engine,
    dst: Engine,
    table: str,
    exclude_auto_generated: bool = False,
    progress_callback: Optional[Callable] = None
) -> int:
    """Drop and recreate specific table"""

def table_copy_delta_only(
    src: Engine,
    dst: Engine,
    table: str,
    exclude_auto_generated: bool = False,
    progress_callback: Optional[Callable] = None
) -> int:
    """Copy only new rows for specific table"""
```

### Progress Callbacks

All migration functions accept optional progress callbacks:

```python
def my_callback(message: str):
    print(f"[LOG] {message}")

full_database_copy(src, dst, progress_callback=my_callback)
```

---

## Performance Guidelines

### Small Database (< 1 GB)
- **Mode:** Any mode works well
- **Time:** 5-10 minutes for full copy
- **Frequency:** Can run hourly if needed

### Medium Database (1-10 GB)
- **Mode:** Incremental Sync recommended for regular updates
- **Time:** 30-60 minutes for full copy, 5-10 min for incremental
- **Frequency:** Daily incremental, weekly full copy

### Large Database (> 10 GB)
- **Mode:** Incremental Sync strongly recommended
- **Time:** Hours for full copy, minutes for incremental
- **Frequency:** Incremental only, avoid full copies
- **Strategy:** Use Table Operations to migrate in batches

### Very Large Database (> 100 GB)
- **Mode:** Table Operations with selective migration
- **Strategy:** 
  - Migrate critical tables first
  - Use parallel processing
  - Schedule during maintenance windows
  - Consider streaming replication instead

---

## Security Considerations

### 1. Credentials Storage

**❌ Don't:**
```python
# Hard-coded passwords
password = "my_password"
```

**✅ Do:**
```python
# Environment variables
import os
password = os.environ.get('DB_PASSWORD')

# Or use config files with restricted permissions
chmod 600 config.ini
```

### 2. Network Security

- Use SSL/TLS for connections
- Restrict source IPs with firewall
- Use VPN for cross-network migrations
- Enable PostgreSQL SSL mode

```python
# Force SSL
url = f"postgresql://{user}:{pass}@{host}/{db}?sslmode=require"
```

### 3. Audit Logging

```python
import logging

logging.basicConfig(
    filename='/var/log/db_migration.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

# All operations are logged
```

### 4. Principle of Least Privilege

```sql
-- Source: read-only access
GRANT SELECT ON ALL TABLES TO migration_user;

-- Destination: write to migrated schema only
GRANT CREATE ON SCHEMA migrated TO migration_user;
REVOKE ALL ON SCHEMA public FROM migration_user;
```

---

## FAQ

### Q: Will this tool modify my source database?
**A:** No. The tool only reads from source (SELECT operations only).

### Q: What happens to my original tables in destination?
**A:** Nothing. All migrated data goes to the `migrated` schema. Your `public` schema is untouched.

### Q: Can I run incremental sync multiple times?
**A:** Yes! That's the point. First run creates tables, subsequent runs only add new rows.

### Q: What if a table has no primary key?
**A:** In Incremental Sync mode, tables without PKs are skipped (with a warning). Use Full Copy instead.

### Q: Does incremental sync detect updates to existing rows?
**A:** No. It only detects new rows (new primary keys). Updates to existing rows are NOT synced.

### Q: Does it handle foreign keys?
**A:** Table structures are copied, but foreign key constraints are not recreated. Data integrity is maintained.

### Q: Can I migrate to a different PostgreSQL version?
**A:** Yes, as long as both versions support the data types used. Test thoroughly.

### Q: How do I rollback a migration?
**A:** Simply: `DROP SCHEMA migrated CASCADE;`

### Q: Can I migrate specific schemas other than 'public'?
**A:** Currently, the tool works with the 'public' schema. Modification required for other schemas.

### Q: What about sequences? Do they sync?
**A:** Yes, sequences are created in the migrated schema automatically.

### Q: Can I schedule automated migrations?
**A:** Yes! Use cron (Linux) or Task Scheduler (Windows). See examples in documentation.

---

## Support and Contributing

### Getting Help

1. Check this README thoroughly
2. Review QUICKSTART.md for tutorials
3. Run `python test_setup.py` to diagnose issues
4. Check logs for error messages

### Reporting Issues

When reporting issues, include:
- PostgreSQL versions (source and destination)
- Python version
- Full error message
- Steps to reproduce
- Migration mode used

### Feature Requests

This tool is designed to be extensible. Suggested features:
- Schema migration support
- Cross-database support (MySQL, etc.)
- Web UI
- REST API
- Real-time replication
- Conflict resolution

---

## License

This project is provided as-is for database migration purposes.

---

## Changelog

### Version 2.0 (Current)
- ✅ Fixed incremental sync (no longer drops tables)
- ✅ Added comprehensive statistics
- ✅ Improved progress messages
- ✅ JSON/JSONB support
- ✅ Sequence handling in migrated schema
- ✅ Better error messages
- ✅ Table-level operations working correctly

### Version 1.0
- Initial release
- Full database copy
- Basic incremental sync
- Table operations

---

## Acknowledgments

Built with:
- **SQLAlchemy** - Database toolkit
- **psycopg2** - PostgreSQL adapter
- **Textual** - Terminal UI framework

---

## Quick Reference Card

```
┌─────────────────────────────────────────────────────────────┐
│                    QUICK REFERENCE                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  🚀 Start:        python frontend.py                       │
│  🧪 Test:         python test_setup.py                     │
│  📚 Examples:     python example_usage.py                  │
│                                                             │
│  ─────────────────────────────────────────────────────────  │
│                                                             │
│  MODE 1: Full Copy                                          │
│  • Use: Initial setup, complete backup                      │
│  • Speed: Slow (copies everything)                          │
│  • Safety: Drops/recreates tables                           │
│                                                             │
│  MODE 2: Incremental                                        │
│  • Use: Regular updates, new rows only                      │
│  • Speed: Fast (only new data)                              │
│  • Safety: Preserves existing data                          │
│  • Requires: Primary keys on all tables                     │
│                                                             │
│  MODE 3: Table Operations                                   │
│  • Use: Selective migration, fine control                   │
│  • Options: Delete/Recreate OR Delta Only                   │
│  • Feature: Can exclude auto-generated fields               │
│                                                             │
│  ─────────────────────────────────────────────────────────  │
│                                                             │
│  💾 Data Location: destination.migrated schema              │
│  🛡️ Original Data: NEVER modified (public schema safe)     │
│  🔄 Rollback: DROP SCHEMA migrated CASCADE;                 │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

**End of Documentation**

For the latest updates and more examples, check the project repository.

Happy migrating! 🎉