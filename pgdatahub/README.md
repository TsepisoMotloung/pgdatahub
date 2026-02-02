# PGDataHub ETL 🔧💡

**Robust, idempotent, resumable Excel → PostgreSQL ETL system**

A production-ready ETL pipeline that ingests Excel files from multiple folders into separate PostgreSQL tables with automatic schema evolution, deduplication, pause/resume capabilities, and comprehensive audit trails.

## Features ✨

- **Automatic Schema Evolution**: Safely widens types, adds columns as needed
- **Deduplication**: SHA-256 based file deduplication prevents reimports
- **Pause & Resume**: Configurable periodic pauses with full resumability
- **Large File Support**: Streams Excel files in chunks (no memory issues)
- **Audit Trail**: Tracks all imports and schema changes
- **Revert & Recovery**: Roll back imports by file or hash
- **Idempotent**: Safe to re-run without side effects
- **Sectional Commits**: Folder-level transaction control

## Technology Stack 🧱

- **Language**: Python 3.10+
- **Database**: PostgreSQL
- **DB Library**: SQLAlchemy Core + psycopg3
- **Excel Parsing**: pandas + openpyxl (streaming mode)
- **Config**: YAML + environment variables
- **CLI**: Click

## Installation 📦

### Prerequisites

- Python 3.10 or higher
- PostgreSQL 12+
- pip

### Setup

```bash
# Clone repository
git clone <your-repo-url>
cd pgdatahub

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Or install in development mode
pip install -e .
```

### Configuration

1. **Database Configuration** (choose one):

   ```bash
   # Option 1: Environment variable (recommended)
   export DATABASE_URL="postgresql://user:password@localhost:5432/pgdatahub"
   
   # Option 2: Config file
   cp config/config.json.example config/config.json
   # Edit config/config.json with your credentials
   ```

2. **ETL Configuration**:

   ```bash
   # Copy environment template
   cp .env.example .env
   # Edit .env with your preferences
   ```

3. **Edit ETL Config** (`config/etl_config.yaml`):

   ```yaml
   default_sheet: Sheet1
   
   folder_a:
     sheet: Policies
   
   folder_b:
     nested:
       sheet: Claims
   ```

## Usage 🚀

### Directory Structure

Place your Excel files in the `data/` directory:

```
data/
├── folder_a/           # Maps to table: folder_a
│   ├── file1.xlsx
│   └── file2.xlsm
├── folder_b/
│   └── nested/         # Maps to table: folder_b_nested
│       └── file3.xlsx
```

**Rules**:
- Each subfolder maps to one table
- Folder names are case-insensitive
- Table name = folder path parts joined with `_`

### Basic ETL Run

```bash
# Run ETL
python main.py etl --data-root data

# Or using CLI directly
python -m src.cli etl --data-root data
```

### With Pause Configuration

```bash
# Pause every 3 files for 30 seconds
export ETL_PAUSE_EVERY=3
export ETL_PAUSE_SECONDS=30
export ETL_SECTIONAL_COMMIT=1

python main.py etl
```

### Resume from Pause

If ETL fails, it writes `.etl_pause.json`. Resume with:

```bash
python main.py resume --data-root data
```

### Dry Run (No Database Writes)

```bash
export SKIP_DB=1
python main.py etl
```

### Check Status

```bash
python main.py status
```

### Revert Operations

```bash
# Revert by source file
python main.py revert \
  --table folder_a \
  --source-file /path/to/file.xlsx

# Revert by file hash
python main.py revert \
  --table folder_a \
  --file-hash abc123...

# Revert schema changes (dry run first!)
python main.py revert-schema \
  --table folder_a \
  --source-file /path/to/file.xlsx \
  --dry-run
```

### Reset & Run (DESTRUCTIVE!)

```bash
# With backup
python reset_and_run.py --backup --run --data-root data

# Without confirmation (for automation)
python reset_and_run.py --backup --run --yes
```

## Configuration Reference ⚙️

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | - | PostgreSQL connection URL |
| `ETL_SECTIONAL_COMMIT` | 0 | Folder-level transactions (1=enabled) |
| `ETL_PAUSE_EVERY` | 0 | Pause every N files (0=disabled) |
| `ETL_PAUSE_SECONDS` | 30 | Pause duration in seconds |
| `ETL_CHUNK_SIZE` | 10000 | Rows per chunk for large files |
| `SKIP_DB` | 0 | Dry run mode (1=enabled) |
| `DEBUG` | 0 | Debug logging (1=enabled) |

### ETL Config YAML

Maps folder paths to Excel sheet names:

```yaml
default_sheet: Sheet1  # Fallback sheet name

# Case-insensitive folder matching
sales:
  sheet: SalesData
  2024:
    sheet: Q1_Sales

marketing:
  campaigns:
    sheet: CampaignMetrics
```

## How It Works 🔁

### High-Level Flow

1. **Discovery**: Scan `data/` for folders and Excel files
2. **Resolution**: Map folders → table names, sheets
3. **Processing**: For each file:
   - Compute SHA-256 hash
   - Check if already imported (skip if yes)
   - Stream Excel sheet in chunks
   - Normalize columns (lowercase, ASCII, deduplicate)
   - Create/evolve schema as needed
   - Insert data with metadata columns
   - Log import to `etl_imports`
4. **Pause**: Periodic pauses if configured
5. **Summary**: Log statistics

### Schema Evolution

**Safe Type Widenings**:
- `INTEGER` → `BIGINT` → `DOUBLE PRECISION` → `TEXT`
- `FLOAT` → `DOUBLE PRECISION` → `TEXT`
- `DATE` → `TIMESTAMP` → `TEXT`

**Column Operations**:
- New columns: `ALTER TABLE ADD COLUMN`
- Type conflicts: Convert to `TEXT`
- All changes logged in `etl_schema_changes`

### Metadata Tables

#### `etl_imports`

Tracks completed imports:

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key |
| `table_name` | VARCHAR | Target table |
| `source_file` | VARCHAR | File path |
| `file_sha256` | VARCHAR(64) | File hash |
| `row_count` | INTEGER | Rows imported |
| `imported_at` | TIMESTAMP | Import time |

#### `etl_schema_changes`

Tracks DDL operations:

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key |
| `table_name` | VARCHAR | Target table |
| `change_type` | VARCHAR | create_table, add_column, alter_type |
| `column_name` | VARCHAR | Column affected |
| `old_type` | VARCHAR | Previous type |
| `new_type` | VARCHAR | New type |
| `source_file` | VARCHAR | File causing change |
| `changed_at` | TIMESTAMP | Change time |

### Added Metadata Columns

Every imported table includes:

- `source_file` (TEXT): Original Excel file path
- `load_timestamp` (TIMESTAMP): UTC import time

## Testing ✅

### Run Unit Tests

```bash
# All tests
pytest

# With coverage
pytest --cov=src --cov-report=html

# Specific test file
pytest tests/test_utils.py

# Specific test
pytest tests/test_utils.py::TestCleanText::test_basic_cleaning
```

### Run Integration Tests

```bash
# Requires PostgreSQL
pytest -m integration
```

### CI/CD

The system includes CI-ready tests that:
- Use temporary databases
- Test pause/resume logic
- Validate schema evolution
- Check deduplication

## Architecture 📐

```
pgdatahub/
├── src/
│   ├── config.py         # Configuration management
│   ├── database.py       # SQLAlchemy engine, connections
│   ├── excel.py          # Excel reading, streaming
│   ├── utils.py          # Text cleaning, normalization
│   ├── schema.py         # Schema evolution, DDL
│   ├── importer.py       # Data import, deduplication
│   ├── pause.py          # Pause/resume mechanism
│   ├── revert.py         # Recovery operations
│   ├── etl.py            # Main orchestration
│   └── cli.py            # Command-line interface
├── config/
│   ├── etl_config.yaml   # Sheet mappings
│   └── config.json       # Database config (optional)
├── tests/
│   ├── test_utils.py
│   ├── test_excel.py
│   └── ...
├── data/                 # Excel files go here
├── main.py               # Entry point
├── reset_and_run.py      # Reset script
└── requirements.txt
```

## Best Practices 💡

### Performance

- **Large Files**: Use chunking (default 10,000 rows)
- **Bulk Inserts**: SQLAlchemy Core for speed
- **Connection Pooling**: NullPool for ETL workloads
- **Periodic Commits**: Balance durability vs. performance

### Safety

- **Never auto-DROP**: Explicit confirmation required
- **Audit Trail**: All changes logged
- **Idempotent**: Safe to re-run
- **Backups**: Use `reset_and_run.py --backup`

### Monitoring

- **Logs**: Check `etl.log` for details
- **Status**: Use `python main.py status`
- **Pause Files**: Check `.etl_pause.json` for failures

## Troubleshooting 🔧

### Common Issues

**Database connection failed**:
```bash
# Check DATABASE_URL
echo $DATABASE_URL

# Test connection
psql $DATABASE_URL -c "SELECT 1"
```

**Memory issues with large files**:
```bash
# Reduce chunk size
export ETL_CHUNK_SIZE=5000
```

**Schema conflicts**:
```bash
# Check schema changes
python main.py status

# Dry-run revert
python main.py revert-schema --table mytable --source-file file.xlsx --dry-run
```

**Pause file exists**:
```bash
# Resume from pause
python main.py resume

# Or manually delete
rm .etl_pause.json
```

## Contributing 🤝

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run test suite: `pytest`
6. Submit pull request

## License 📄

[Your License Here]

## Support 💬

For issues, questions, or contributions:
- Open an issue on GitHub
- Email: [your-email]
- Documentation: [link-to-docs]

---

**Built with ❤️ using SQLAlchemy + psycopg3**
