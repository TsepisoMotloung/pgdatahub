# PGDataHub ETL Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Sources                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │ folder_a │  │ folder_b │  │ folder_c │  ...             │
│  │  *.xlsx  │  │  *.xlsx  │  │  *.xlsx  │                  │
│  └──────────┘  └──────────┘  └──────────┘                  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  ETL Orchestrator                            │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │  Discovery   │─▶│  Resolution  │─▶│  Processing  │     │
│  │  (folders &  │  │  (table &    │  │  (stream &   │     │
│  │   files)     │  │   sheet)     │  │   insert)    │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ Deduplication│  │    Schema    │  │  Pause &     │     │
│  │  (SHA-256)   │  │  Evolution   │  │  Resume      │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  PostgreSQL Database                         │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ Data Tables  │  │ etl_imports  │  │etl_schema    │     │
│  │ (folder_a,   │  │  (tracking)  │  │  _changes    │     │
│  │  folder_b)   │  │              │  │  (audit)     │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└─────────────────────────────────────────────────────────────┘
```

## Component Architecture

### 1. Configuration Layer (`src/config.py`)

**Responsibilities:**
- Load YAML and JSON configuration
- Resolve database connection
- Provide environment variable handling
- Map folder paths to table/sheet names

**Key Classes:**
- `Config`: Central configuration manager

### 2. Database Layer (`src/database.py`)

**Responsibilities:**
- SQLAlchemy engine management
- Connection pooling (NullPool for ETL)
- Metadata table creation
- DDL execution
- Table introspection

**Key Classes:**
- `DatabaseManager`: Database connection and operations

### 3. Excel Processing (`src/excel.py`)

**Responsibilities:**
- Discover Excel files in directory tree
- Compute file hashes (SHA-256)
- Stream Excel sheets in chunks
- Handle multiple Excel formats

**Key Functions:**
- `discover_excel_files()`: Recursive file discovery
- `read_excel_chunked()`: Memory-efficient streaming
- `compute_file_hash()`: File deduplication

### 4. Data Utilities (`src/utils.py`)

**Responsibilities:**
- Text normalization for SQL identifiers
- Column name cleaning and deduplication
- Type inference (pandas → PostgreSQL)
- Metadata column injection

**Key Functions:**
- `clean_text()`: ASCII-safe normalization
- `normalize_dataframe_columns()`: Handle duplicates
- `infer_schema()`: Type mapping

### 5. Schema Management (`src/schema.py`)

**Responsibilities:**
- Table creation
- Column addition
- Type widening (safe evolution)
- DDL logging

**Key Classes:**
- `SchemaManager`: Schema evolution orchestration

**Type Hierarchy:**
```
INTEGER → BIGINT → DOUBLE PRECISION → TEXT
FLOAT → DOUBLE PRECISION → TEXT
DATE → TIMESTAMP → TEXT
```

### 6. Data Import (`src/importer.py`)

**Responsibilities:**
- Deduplication checking
- Bulk data insertion
- Import logging
- Progress tracking

**Key Classes:**
- `DataImporter`: Data insertion operations
- `ImportTracker`: Statistics and progress

### 7. Pause/Resume (`src/pause.py`)

**Responsibilities:**
- Pause file management
- Periodic pause execution
- Resume orchestration

**Key Classes:**
- `PauseManager`: Pause/resume coordination

### 8. Revert Operations (`src/revert.py`)

**Responsibilities:**
- Data deletion by file/hash
- Schema change reversion
- Import history queries

**Key Functions:**
- `revert_by_file()`: Remove imported data
- `revert_by_hash()`: Remove by SHA-256
- `revert_schema_changes()`: DDL rollback

### 9. ETL Orchestration (`src/etl.py`)

**Responsibilities:**
- Main workflow coordination
- Folder iteration
- File processing
- Error handling
- Summary reporting

**Key Functions:**
- `run()`: Main entry point
- `process_folder()`: Folder-level processing
- `process_file()`: File-level processing

### 10. CLI Interface (`src/cli.py`)

**Responsibilities:**
- Command-line interface
- User interaction
- Validation and confirmation

**Commands:**
- `etl`: Run ETL
- `resume`: Resume from pause
- `revert`: Revert imports
- `status`: Show statistics

## Data Flow

### Import Flow

```
1. Discover Files
   └─▶ discover_excel_files()
       └─▶ Returns: {folder_parts: [file_paths]}

2. For Each Folder:
   └─▶ Resolve table_name and sheet_name
   
   3. For Each File:
      ├─▶ Compute SHA-256 hash
      ├─▶ Check if already imported
      │   └─▶ Skip if duplicate
      │
      ├─▶ Stream Excel in chunks
      │   └─▶ For each chunk:
      │       ├─▶ Normalize columns
      │       ├─▶ Add metadata
      │       ├─▶ (First chunk) Sync schema
      │       └─▶ Insert data
      │
      ├─▶ Log import completion
      │
      └─▶ Check pause threshold
          └─▶ Execute pause if needed

4. Summary and cleanup
```

### Schema Evolution Flow

```
1. Infer schema from DataFrame
   └─▶ infer_schema()

2. Check if table exists
   ├─▶ NO: Create table
   │   └─▶ CREATE TABLE with columns
   │
   └─▶ YES: Sync schema
       ├─▶ Check each column
       │   ├─▶ New column? ADD COLUMN
       │   └─▶ Type mismatch? WIDEN or TEXT
       │
       └─▶ Log all changes to etl_schema_changes
```

## Database Schema

### Data Tables (Dynamic)

Each folder creates a table:

```sql
CREATE TABLE "folder_name" (
    -- User columns (from Excel)
    col1 TYPE1,
    col2 TYPE2,
    ...
    
    -- Metadata columns (auto-added)
    source_file TEXT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
);
```

### Metadata Tables

#### `etl_imports`

```sql
CREATE TABLE etl_imports (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    source_file VARCHAR(500) NOT NULL,
    file_sha256 VARCHAR(64) NOT NULL,
    row_count INTEGER NOT NULL,
    imported_at TIMESTAMP NOT NULL,
    
    UNIQUE(table_name, source_file, file_sha256)
);
```

#### `etl_schema_changes`

```sql
CREATE TABLE etl_schema_changes (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    change_type VARCHAR(50) NOT NULL,  -- create_table | add_column | alter_type
    column_name VARCHAR(255),
    old_type VARCHAR(100),
    new_type VARCHAR(100),
    source_file VARCHAR(500),
    changed_at TIMESTAMP NOT NULL
);
```

## Configuration Files

### `config/etl_config.yaml`

```yaml
default_sheet: Sheet1

folder_a:
  sheet: DataSheet
  
folder_b:
  nested:
    sheet: NestedData
```

### Environment Variables

```bash
DATABASE_URL=postgresql://user:pass@host:port/db
ETL_SECTIONAL_COMMIT=1
ETL_PAUSE_EVERY=5
ETL_PAUSE_SECONDS=30
ETL_CHUNK_SIZE=10000
SKIP_DB=0
DEBUG=0
```

## Error Handling

### Levels

1. **File-level**: Single file error
   - Log error
   - Continue to next file (default)
   - OR stop folder (if `ETL_SECTIONAL_COMMIT=1`)

2. **Folder-level**: Entire folder fails
   - Write `.etl_pause.json`
   - Stop ETL (if `ETL_SECTIONAL_COMMIT=1`)
   - Resume capability

3. **System-level**: Database connection, config errors
   - Immediate abort
   - Clear error message

### Recovery

```
Error → .etl_pause.json → Resume → Success → Delete pause file
```

## Performance Considerations

### Memory

- **Chunked reading**: Process Excel in configurable chunks
- **Streaming**: No full-file loads
- **NullPool**: No connection caching overhead

### Speed

- **Bulk inserts**: SQLAlchemy Core batch operations
- **Minimal DDL**: Schema changes only when needed
- **Skip duplicates**: Early exit on hash match

### Scalability

- **Configurable chunks**: Adjust for available memory
- **Pause mechanism**: Long-running jobs with breakpoints
- **Folder isolation**: Independent table creation

## Security

- **Credential management**: Environment variables preferred
- **No logging of secrets**: Passwords never logged
- **SQL injection protection**: Parameterized queries
- **Audit trail**: All operations tracked

## Testing Strategy

### Unit Tests
- Text cleaning and normalization
- Type inference
- Column deduplication

### Integration Tests
- Full ETL workflow
- Schema evolution
- Deduplication
- Pause/resume
- Revert operations

### CI/CD
- PostgreSQL service in GitHub Actions
- Matrix testing (Python 3.10, 3.11, 3.12)
- Coverage reporting

## Extension Points

### Custom Transformations

Add hooks in `src/etl.py::process_file()`:

```python
# After normalize_dataframe_columns
chunk_df = custom_transform(chunk_df)
```

### Custom Type Mapping

Extend `src/utils.py::pandas_to_pg_type()`:

```python
def pandas_to_pg_type(dtype):
    # Add custom mappings
    if custom_condition:
        return 'CUSTOM_TYPE'
    # ... existing logic
```

### Additional Data Sources

Create new module `src/csv.py` similar to `src/excel.py`.

## Monitoring & Observability

### Logs

- `etl.log`: All operations
- Console output: Progress and errors
- Debug mode: Verbose SQL and operations

### Metrics

- Files processed/skipped
- Rows inserted
- Schema changes
- Processing time
- Error count

### Database Queries

```sql
-- Recent imports
SELECT * FROM etl_imports ORDER BY imported_at DESC LIMIT 10;

-- Schema change history
SELECT * FROM etl_schema_changes WHERE table_name = 'mytable';

-- Import statistics
SELECT table_name, COUNT(*) as files, SUM(row_count) as rows
FROM etl_imports
GROUP BY table_name;
```

---

**Last Updated**: 2024-01-30  
**Version**: 1.0.0
