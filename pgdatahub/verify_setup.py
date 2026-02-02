#!/usr/bin/env python
"""Verification script to check PGDataHub installation and setup"""
import sys
from pathlib import Path

# Colors for terminal output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
RESET = '\033[0m'

def check(condition, message):
    """Print check result"""
    if condition:
        print(f"{GREEN}✓{RESET} {message}")
        return True
    else:
        print(f"{RED}✗{RESET} {message}")
        return False

def warn(message):
    """Print warning"""
    print(f"{YELLOW}⚠{RESET} {message}")

def main():
    print("=" * 60)
    print("PGDataHub Installation Verification")
    print("=" * 60)
    print()
    
    all_good = True
    
    # Check Python version
    print("Checking Python version...")
    version = sys.version_info
    if check(version >= (3, 10), f"Python {version.major}.{version.minor}.{version.micro}"):
        pass
    else:
        all_good = False
        print(f"  Required: Python 3.10+")
    print()
    
    # Check dependencies
    print("Checking dependencies...")
    deps = {
        'sqlalchemy': 'SQLAlchemy',
        'psycopg': 'psycopg3',
        'pandas': 'pandas',
        'openpyxl': 'openpyxl',
        'yaml': 'PyYAML',
        'click': 'Click'
    }
    
    for module, name in deps.items():
        try:
            __import__(module)
            check(True, f"{name} installed")
        except ImportError:
            check(False, f"{name} NOT installed")
            all_good = False
    print()
    
    # Check directory structure
    print("Checking directory structure...")
    required_dirs = ['src', 'config', 'data', 'tests']
    for dir_name in required_dirs:
        path = Path(dir_name)
        check(path.exists() and path.is_dir(), f"{dir_name}/ directory exists")
        if not path.exists():
            all_good = False
    print()
    
    # Check config files
    print("Checking configuration files...")
    config_yaml = Path('config/etl_config.yaml')
    check(config_yaml.exists(), "config/etl_config.yaml exists")
    if not config_yaml.exists():
        warn("Create config/etl_config.yaml for sheet mappings")
    
    config_json = Path('config/config.json')
    if not config_json.exists():
        warn("config/config.json not found (DATABASE_URL env var needed)")
    print()
    
    # Check database connection
    print("Checking database connection...")
    try:
        import os
        from src.config import config
        
        if config.database_url:
            check(True, "Database URL configured")
            
            if not config.skip_db:
                try:
                    from src.database import db
                    conn = db.get_connection()
                    conn.close()
                    check(True, "Database connection successful")
                except Exception as e:
                    check(False, f"Database connection failed: {e}")
                    all_good = False
            else:
                warn("SKIP_DB=1, database not tested")
        else:
            check(False, "Database URL not configured")
            print("  Set DATABASE_URL environment variable or create config/config.json")
            all_good = False
    except Exception as e:
        check(False, f"Configuration error: {e}")
        all_good = False
    print()
    
    # Check test data
    print("Checking test data...")
    excel_files = list(Path('data').rglob('*.xlsx'))
    if excel_files:
        check(True, f"Found {len(excel_files)} Excel file(s) in data/")
        for f in excel_files[:3]:  # Show first 3
            print(f"  - {f}")
    else:
        warn("No Excel files in data/ directory")
        print("  Add Excel files to test the ETL")
    print()
    
    # Summary
    print("=" * 60)
    if all_good:
        print(f"{GREEN}✓ All checks passed!{RESET}")
        print()
        print("Ready to run ETL:")
        print("  python main.py etl --data-root data")
    else:
        print(f"{RED}✗ Some checks failed{RESET}")
        print()
        print("Please fix the issues above before running ETL.")
    print("=" * 60)
    
    return 0 if all_good else 1

if __name__ == '__main__':
    sys.exit(main())
