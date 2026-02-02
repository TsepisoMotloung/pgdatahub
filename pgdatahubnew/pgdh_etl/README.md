# PGDataHub ETL 🔧💡

A robust, idempotent, resumable Excel → PostgreSQL ETL system built with Python, SQLAlchemy Core, and psycopg3.

## Features

- **Multi-folder ingestion**: Each folder maps to a separate PostgreSQL table
- **Schema evolution**: Safe DDL changes as new columns/types appear
- **Deduplication**: SHA-256 based file tracking prevents re-imports
- **Pause & Resume**: Sectional commits with automatic recovery
- **Memory efficient**: Streaming Excel processing for large files
- **Full audit trail**: Track all imports and schema changes
- **Revert support**: Rollback data and schema changes

## Quick Start

### Installation

```bash
pip install -r requirements.txt
```

### Configuration

1. Set your database URL:
```bash
export DATABASE_URL="postgresql+psycopg://user:password@localhost:5432/dbname"
```

Or use `config.json` for connection details.

2. Configure sheet mappings in `etl_config.yaml`:
```yaml
default_sheet: Sheet1
folder_a:
  sheet: Policies
folder_b:
  nested:
    sheet: Claims
```

3. Organize your data:
```
data/
├── folder_a/          # → table: folder_a
│   ├── file1.xlsx
│   └── file2.xlsx
└── folder_b/          # → table: folder_b
    └── file3.xlsx
```

### Run ETL

```bash
python main.py --data-root data
```

With pause/resume:
```bash
ETL_SECTIONAL_COMMIT=1 ETL_PAUSE_EVERY=10 ETL_PAUSE_SECONDS=30 python main.py
```

Dry run (no DB writes):
```bash
SKIP_DB=1 python main.py --data-root data
```

### Resume from Pause

```bash
python main.py --data-root data --resume
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | From config.json |
| `ETL_SECTIONAL_COMMIT` | Enable folder-level transactions | 0 |
| `ETL_PAUSE_EVERY` | Pause after N files (0=disable) | 0 |
| `ETL_PAUSE_SECONDS` | Seconds to pause | 30 |
| `ETL_CHUNK_SIZE` | Rows per chunk | 10000 |
| `SKIP_DB` | Dry run mode | 0 |

## Database Schema

### etl_imports
Tracks completed imports with file hashes.

```sql
CREATE TABLE etl_imports (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    source_file VARCHAR(500) NOT NULL,
    file_sha256 VARCHAR(64) NOT NULL,
    row_count INTEGER DEFAULT 0,
    imported_at TIMESTAMP DEFAULT NOW(),
    folder_path TEXT
);
```

### etl_schema_changes
Tracks all DDL mutations.

```sql
CREATE TABLE etl_schema_changes (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    change_type VARCHAR(50) NOT NULL,  -- create_table | add_column | alter_type
    column_name VARCHAR(255),
    old_type VARCHAR(100),
    new_type VARCHAR(100),
    source_file VARCHAR(500),
    changed_at TIMESTAMP DEFAULT NOW(),
    details TEXT
);
```

## Tools

### Reset and Backup

```bash
# Create backup only
python reset_and_run.py --backup

# Reset with confirmation
python reset_and_run.py --reset --yes

# Backup, reset, and run
python reset_and_run.py --backup --reset --run --yes

# List imports
python reset_and_run.py --list-imports

# List schema changes
python reset_and_run.py --list-schema-changes
```

### Revert Operations

```bash
# Dry run revert by file
python -m src.revert --by-file "/path/to/file.xlsx"

# Execute revert
python -m src.revert --by-file "/path/to/file.xlsx" --execute

# Revert by hash
python -m src.revert --by-hash "abc123..."

# Revert schema changes
python -m src.revert --by-file "/path/to/file.xlsx" --table table_name --schema-revert --execute
```

## Testing

```bash
# Run unit tests
python -m pytest tests/ -v

# Run integration tests (requires database)
SKIP_INTEGRATION_TESTS=0 python -m pytest tests/test_integration.py -v
```

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Config    │────▶│  ETL Engine  │────▶│  Database   │
│  (YAML/Env) │     │              │     │ (PostgreSQL)│
└─────────────┘     └──────┬───────┘     └─────────────┘
                           │
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
    ┌────────────┐  ┌────────────┐  ┌────────────┐
    │   Excel    │  │   Schema   │  │   Pause    │
    │ Processor  │  │  Manager   │  │  Manager   │
    └────────────┘  └────────────┘  └────────────┘
```

## License

MIT License
