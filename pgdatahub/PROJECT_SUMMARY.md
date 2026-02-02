# PGDataHub ETL - Project Summary

## What This Is

A **production-ready, enterprise-grade ETL system** that imports Excel files from multiple folders into PostgreSQL tables with automatic schema evolution, deduplication, pause/resume, and comprehensive auditing.

## Key Features ⭐

✅ **Automatic Schema Evolution** - Tables adapt to new columns and data types  
✅ **SHA-256 Deduplication** - Never import the same file twice  
✅ **Pause & Resume** - Handle long-running jobs with configurable breaks  
✅ **Large File Support** - Stream Excel files in chunks (no memory issues)  
✅ **Complete Audit Trail** - Track every import and schema change  
✅ **Revert & Recovery** - Roll back imports by file or hash  
✅ **Idempotent Operations** - Safe to re-run anytime  
✅ **SQLAlchemy + psycopg3** - Modern, performant database access  

## Project Structure

```
pgdatahub/
├── .github/
│   └── workflows/
│       └── ci.yml              # GitHub Actions CI/CD
│
├── config/
│   ├── config.json.example     # Database config template
│   └── etl_config.yaml         # Sheet name mappings
│
├── data/                       # Excel files go here
│   ├── folder_a/
│   │   └── policies_2024.xlsx
│   └── folder_b/
│       └── nested/
│           └── claims_q1.xlsx
│
├── src/                        # Core ETL engine
│   ├── __init__.py
│   ├── cli.py                  # Command-line interface
│   ├── config.py               # Configuration management
│   ├── database.py             # SQLAlchemy + psycopg3
│   ├── etl.py                  # Main orchestration
│   ├── excel.py                # Excel streaming & hashing
│   ├── importer.py             # Data insertion & tracking
│   ├── pause.py                # Pause/resume mechanism
│   ├── revert.py               # Recovery operations
│   ├── schema.py               # Schema evolution
│   └── utils.py                # Text cleaning & normalization
│
├── tests/                      # Comprehensive test suite
│   ├── __init__.py
│   ├── test_excel.py
│   ├── test_integration.py
│   └── test_utils.py
│
├── .env.example                # Environment variables template
├── .gitignore                  # Git ignore rules
├── ARCHITECTURE.md             # Technical architecture doc
├── CHANGELOG.md                # Version history
├── main.py                     # Main entry point
├── examples.py                 # Programmatic usage examples
├── pytest.ini                  # Test configuration
├── QUICKSTART.md               # 5-minute getting started
├── README.md                   # Main documentation
├── requirements.txt            # Python dependencies
├── reset_and_run.py           # Database reset utility
├── setup.py                    # Package setup
└── verify_setup.py            # Installation checker
```

## Quick Commands

```bash
# Install
pip install -r requirements.txt

# Configure
export DATABASE_URL="postgresql://user:pass@localhost/db"

# Run ETL
python main.py etl --data-root data

# Check status
python main.py status

# Resume from failure
python main.py resume

# Revert import
python main.py revert --table mytable --source-file /path/to/file.xlsx

# Run tests
pytest

# Verify setup
python verify_setup.py
```

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.10+ |
| Database | PostgreSQL 12+ |
| DB Library | SQLAlchemy Core + psycopg3 |
| Excel | pandas + openpyxl |
| Config | YAML + environment variables |
| CLI | Click |
| Testing | pytest + pytest-cov |
| CI/CD | GitHub Actions |

## How It Works

1. **Discovery**: Scans folders for Excel files
2. **Resolution**: Maps folders → tables, sheets
3. **Deduplication**: SHA-256 hash check
4. **Streaming**: Reads Excel in memory-safe chunks
5. **Normalization**: Cleans columns, handles duplicates
6. **Schema Evolution**: Creates/alters tables safely
7. **Import**: Bulk inserts with metadata
8. **Audit**: Logs everything to `etl_imports` and `etl_schema_changes`

## Key Design Decisions

### ✅ SQLAlchemy Core (not ORM)
- Better performance for ETL
- More control over SQL
- Explicit transaction management

### ✅ psycopg3 (not psycopg2)
- Modern async support
- Better performance
- Type safety

### ✅ Streaming Excel
- No memory issues with large files
- Configurable chunk size
- Progress reporting

### ✅ SHA-256 Deduplication
- Deterministic file identification
- Handles file renames/moves
- Fast lookup in database

### ✅ Folder-Level Transactions (Optional)
- Atomic folder imports
- Rollback on error
- Configurable via `ETL_SECTIONAL_COMMIT`

### ✅ Idempotent by Design
- Safe to re-run anytime
- No side effects
- Explicit confirmation for destructive ops

## Database Schema

### Data Tables (Dynamic)
Each folder creates a table with normalized columns plus:
- `source_file` (TEXT)
- `load_timestamp` (TIMESTAMP)

### Metadata Tables

**`etl_imports`**: Tracks completed imports
- `table_name`, `source_file`, `file_sha256`
- `row_count`, `imported_at`

**`etl_schema_changes`**: Tracks all DDL
- `table_name`, `change_type`, `column_name`
- `old_type`, `new_type`, `changed_at`

## Configuration

### Environment Variables
```bash
DATABASE_URL=postgresql://...
ETL_SECTIONAL_COMMIT=1
ETL_PAUSE_EVERY=5
ETL_PAUSE_SECONDS=30
ETL_CHUNK_SIZE=10000
SKIP_DB=0
DEBUG=0
```

### YAML Config (`config/etl_config.yaml`)
```yaml
default_sheet: Sheet1
folder_a:
  sheet: Policies
folder_b:
  nested:
    sheet: Claims
```

## Testing

- **Unit tests**: Text cleaning, normalization, type inference
- **Integration tests**: Full ETL workflow with PostgreSQL
- **CI/CD**: GitHub Actions with matrix testing (Python 3.10, 3.11, 3.12)
- **Coverage**: Comprehensive test coverage with reporting

## Documentation

| File | Purpose |
|------|---------|
| `README.md` | Complete feature documentation |
| `QUICKSTART.md` | 5-minute getting started guide |
| `ARCHITECTURE.md` | Technical architecture & design |
| `CHANGELOG.md` | Version history |
| Inline docs | Docstrings in all modules |

## Use Cases

✅ **Corporate Data Integration**
- Import department Excel files into central database
- Automatic schema updates as templates evolve
- Audit trail for compliance

✅ **Data Migration**
- Migrate legacy Excel-based systems to PostgreSQL
- Preserve data lineage
- Resume capability for large migrations

✅ **Regular Data Ingestion**
- Scheduled imports from shared drives
- Deduplication prevents double-imports
- Pause during business hours

✅ **Data Consolidation**
- Combine data from multiple sources
- Standardize column names
- Handle schema differences gracefully

## Production Ready Features

✅ **Error Handling**: Comprehensive exception handling  
✅ **Logging**: Structured logging with levels  
✅ **Monitoring**: Status commands and metrics  
✅ **Recovery**: Pause files and resume capability  
✅ **Validation**: Input validation and checks  
✅ **Security**: No credential logging, parameterized queries  
✅ **Performance**: Bulk inserts, connection pooling  
✅ **Scalability**: Chunked processing, configurable limits  

## What's NOT Included (Future Work)

- ❌ Web UI (command-line only)
- ❌ Real-time monitoring dashboard
- ❌ Email notifications
- ❌ Parallel folder processing
- ❌ Cloud storage integration (S3, GCS)
- ❌ Data validation rules
- ❌ Custom transformation hooks
- ❌ Multi-database support

## Getting Help

1. **Read the docs**: Start with `QUICKSTART.md`
2. **Check logs**: Review `etl.log` for details
3. **Run tests**: `pytest -v` to verify setup
4. **Check status**: `python main.py status`
5. **Verify setup**: `python verify_setup.py`

## License

[Your License Here]

## Contributors

[Your Name/Team]

---

**Built with ❤️ and careful attention to production requirements**

Last Updated: 2024-01-30  
Version: 1.0.0
