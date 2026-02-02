# Quick Start Guide 🚀

Get up and running with PGDataHub in 5 minutes!

## Prerequisites Check ✓

```bash
# Check Python version (need 3.10+)
python --version

# Check PostgreSQL
psql --version

# Check pip
pip --version
```

## Step 1: Install Dependencies

```bash
# Navigate to project directory
cd pgdatahub

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install packages
pip install -r requirements.txt
```

## Step 2: Setup Database

```bash
# Create database
createdb pgdatahub

# Or using psql
psql -U postgres -c "CREATE DATABASE pgdatahub;"
```

## Step 3: Configure Connection

```bash
# Set database URL
export DATABASE_URL="postgresql://postgres:yourpassword@localhost:5432/pgdatahub"

# Or create config file
cp config/config.json.example config/config.json
# Edit config.json with your credentials
```

## Step 4: Verify Setup

```bash
# Test with included sample data
python main.py etl --data-root data

# Check results
python main.py status
```

You should see output like:

```
ETL STATUS
============================================================
Total imports: 2
Total schema changes: 2

Recent tables:
  folder_a: 1 files, 3 rows
  folder_b_nested: 1 files, 2 rows
============================================================
```

## Step 5: Add Your Data

```bash
# Create folders for your data
mkdir -p data/my_department/my_team

# Copy Excel files
cp /path/to/your/file.xlsx data/my_department/my_team/

# Update config if needed
nano config/etl_config.yaml
```

Add configuration:

```yaml
my_department:
  my_team:
    sheet: DataSheet  # Name of Excel sheet to read
```

## Step 6: Run ETL

```bash
# Run with pause configuration
export ETL_PAUSE_EVERY=5
export ETL_PAUSE_SECONDS=10
export ETL_SECTIONAL_COMMIT=1

python main.py etl
```

## Verify Results

```bash
# Check import status
python main.py status

# Connect to database
psql $DATABASE_URL

# Query your data
\dt  # List tables
SELECT * FROM my_department_my_team LIMIT 10;
```

## Common Next Steps

### Enable Debug Logging

```bash
export DEBUG=1
python main.py etl
```

### Test Dry Run

```bash
export SKIP_DB=1
python main.py etl
# Check etl.log for what would happen
```

### Setup Scheduled Runs

```bash
# Add to crontab
crontab -e

# Run every hour
0 * * * * cd /path/to/pgdatahub && /path/to/venv/bin/python main.py etl
```

### Handle Large Files

```bash
# Reduce chunk size for memory-constrained systems
export ETL_CHUNK_SIZE=5000

# Or increase for faster processing
export ETL_CHUNK_SIZE=20000
```

## Troubleshooting

### Can't connect to database?

```bash
# Test connection
psql $DATABASE_URL -c "SELECT version();"

# Check PostgreSQL is running
ps aux | grep postgres
```

### Import not working?

```bash
# Check logs
tail -f etl.log

# Enable debug mode
export DEBUG=1
python main.py etl
```

### Need to reset?

```bash
# CAUTION: This drops all tables!
python reset_and_run.py --backup --run
```

## Getting Help

- Check the [README.md](README.md) for full documentation
- Review logs in `etl.log`
- Check `.etl_pause.json` if ETL stopped
- Use `python main.py status` for current state

## What's Next?

- Learn about [Schema Evolution](README.md#schema-evolution)
- Setup [Pause & Resume](README.md#pause--resume)
- Configure [Revert Operations](README.md#revert-operations)
- Read about [Best Practices](README.md#best-practices-)

Happy ETL-ing! 🎉
