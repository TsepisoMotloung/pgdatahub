# Changelog

All notable changes to PGDataHub ETL will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-01-30

### Added
- Initial release of PGDataHub ETL system
- Excel to PostgreSQL data pipeline with streaming support
- Automatic schema evolution with safe type widening
- SHA-256 based file deduplication
- Pause and resume functionality with configurable intervals
- Comprehensive audit trail via `etl_imports` and `etl_schema_changes` tables
- Revert operations by file or hash
- Schema change reversion with dry-run support
- CLI interface with Click
- SQLAlchemy Core + psycopg3 for PostgreSQL connectivity
- Chunked Excel reading for large file support
- Column normalization and deduplication
- Metadata columns (`source_file`, `load_timestamp`)
- Sectional commit (folder-level transactions)
- Comprehensive test suite
- CI/CD workflow with GitHub Actions
- Extensive documentation (README, QUICKSTART, inline docs)

### Features
- Case-insensitive folder to table mapping
- Configurable sheet name resolution via YAML
- Environment-based configuration
- Dry-run mode (SKIP_DB)
- Debug logging support
- Import status tracking
- Reset and backup utilities

### Technical
- Python 3.10+ support
- PostgreSQL 12+ compatible
- Type-safe schema evolution
- Memory-efficient streaming for large files
- Idempotent operations
- Connection pooling optimizations

## [Unreleased]

### Planned
- Support for additional Excel formats (.xlsb)
- Parallel folder processing
- Email notifications on failure
- Web dashboard for monitoring
- Support for CSV input files
- Data validation rules
- Custom transformation hooks
- Multi-database support (MySQL, SQLite)
