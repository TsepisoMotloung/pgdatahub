#!/usr/bin/env python3
"""
Detailed PostgreSQL permissions test
Tests the exact same operations that the ETL app performs
"""
import os
import sys
import psycopg2
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text, DateTime
from sqlalchemy.sql import text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Colors for output
GREEN = '\033[0;32m'
RED = '\033[0;31m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'

def print_success(msg):
    print(f"{GREEN}✓{NC} {msg}")

def print_error(msg):
    print(f"{RED}✗{NC} {msg}")

def print_info(msg):
    print(f"{BLUE}ℹ{NC} {msg}")

def print_warning(msg):
    print(f"{YELLOW}⚠{NC} {msg}")

print("=" * 60)
print("PostgreSQL Detailed Permissions Test")
print("=" * 60)
print()

# Get DATABASE_URL
url = os.environ.get('DATABASE_URL') 
# url = "postgresql+psycopg2://neondb_owner:npg_YIuXCjpRF4G3@ep-crimson-base-ah3ykmb9-pooler.c-3.us-east-1.aws.neon.tech:5432/neondb?sslmode=require&channel_binding=require"
if not url:
    print_error("DATABASE_URL not set in environment!")
    sys.exit(1)

# Mask password for display
masked = url.split('@')[1] if '@' in url else url
print_info(f"Testing connection to: ...@{masked}")
print()

# ============================================================
# Test 1: Basic psycopg2 connection
# ============================================================
print("Test 1: Direct psycopg2 Connection")
print("-" * 60)
try:
    # Normalize URL for psycopg2
    psycopg2_url = url.replace('postgresql+psycopg2://', 'postgresql://')
    conn = psycopg2.connect(psycopg2_url)
    cur = conn.cursor()
    
    cur.execute("SELECT current_user, current_database(), version()")
    user, db, version = cur.fetchone()
    print_success(f"Connected as user: {user}")
    print_success(f"Database: {db}")
    print_info(f"Version: {version.split(',')[0]}")
    
    conn.close()
except Exception as e:
    print_error(f"Connection failed: {e}")
    sys.exit(1)

print()

# ============================================================
# Test 2: SQLAlchemy connection (what the app uses)
# ============================================================
print("Test 2: SQLAlchemy Engine Connection")
print("-" * 60)
try:
    # Ensure proper driver format
    if url.startswith("postgresql://") and not url.startswith("postgresql+psycopg2://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    
    engine = create_engine(url)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT current_user, current_database()"))
        row = result.fetchone()
        print_success(f"SQLAlchemy connected as: {row[0]} to {row[1]}")
except Exception as e:
    print_error(f"SQLAlchemy connection failed: {e}")
    sys.exit(1)

print()

# ============================================================
# Test 3: Check schema permissions
# ============================================================
print("Test 3: Schema Permissions")
print("-" * 60)
try:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT privilege_type 
            FROM information_schema.schema_privileges 
            WHERE grantee = current_user 
              AND table_schema = 'public'
        """))
        privs = [row[0] for row in result]
        
        if privs:
            print_success(f"Privileges on 'public' schema: {', '.join(privs)}")
        else:
            print_warning("No explicit privileges found (may have inherited permissions)")
        
        # Check if user is superuser
        result = conn.execute(text("""
            SELECT rolsuper, rolcreatedb, rolcreaterole
            FROM pg_roles 
            WHERE rolname = current_user
        """))
        row = result.fetchone()
        if row:
            print_info(f"Superuser: {row[0]}, Can create DB: {row[1]}, Can create roles: {row[2]}")
except Exception as e:
    print_error(f"Failed to check permissions: {e}")

print()

# ============================================================
# Test 4: Simple CREATE TABLE (like psycopg2 test)
# ============================================================
print("Test 4: Simple CREATE TABLE with psycopg2")
print("-" * 60)
try:
    conn = psycopg2.connect(psycopg2_url)
    cur = conn.cursor()
    
    cur.execute("DROP TABLE IF EXISTS __simple_test__")
    cur.execute("CREATE TABLE __simple_test__ (id SERIAL PRIMARY KEY, name TEXT)")
    cur.execute("INSERT INTO __simple_test__ (name) VALUES ('test')")
    cur.execute("SELECT * FROM __simple_test__")
    result = cur.fetchone()
    print_success(f"Simple table created and data inserted: {result}")
    
    cur.execute("DROP TABLE __simple_test__")
    conn.commit()
    conn.close()
    print_success("Simple table dropped successfully")
except Exception as e:
    print_error(f"Simple CREATE TABLE failed: {e}")
    if conn:
        conn.rollback()
        conn.close()

print()

# ============================================================
# Test 5: CREATE TABLE via SQLAlchemy (what the app uses)
# ============================================================
print("Test 5: CREATE TABLE via SQLAlchemy MetaData")
print("-" * 60)
try:
    md = MetaData()
    test_table = Table(
        '__sqlalchemy_test__',
        md,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('name', Text),
        Column('created_at', DateTime)
    )
    
    print_info("Creating table via SQLAlchemy MetaData.create_all()...")
    md.create_all(engine, tables=[test_table])
    print_success("Table created successfully via SQLAlchemy")
    
    # Insert test data
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO __sqlalchemy_test__ (name) VALUES ('test')"))
        result = conn.execute(text("SELECT * FROM __sqlalchemy_test__"))
        row = result.fetchone()
        print_success(f"Data inserted and retrieved: {row}")
    
    # Drop table
    md.drop_all(engine, tables=[test_table])
    print_success("Table dropped successfully")
    
except Exception as e:
    print_error(f"SQLAlchemy CREATE TABLE failed: {e}")
    print()
    print_info("Full error details:")
    import traceback
    traceback.print_exc()

print()

# ============================================================
# Test 6: Create the actual ETL tables
# ============================================================
print("Test 6: Create ETL Schema Tables (etl_schema_changes)")
print("-" * 60)
try:
    md = MetaData()
    etl_schema_changes = Table(
        'etl_schema_changes',
        md,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('table_name', Text, nullable=False),
        Column('column_name', Text),
        Column('old_type', Text),
        Column('new_type', Text),
        Column('change_type', Text, nullable=False),
        Column('change_timestamp', DateTime),
        Column('migration_query', Text),
        Column('source_file', Text)
    )
    
    print_info("Attempting to create etl_schema_changes table...")
    md.create_all(engine, tables=[etl_schema_changes])
    print_success("etl_schema_changes table created successfully!")
    
    # Verify it exists
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'etl_schema_changes'
            )
        """))
        exists = result.scalar()
        if exists:
            print_success("Verified: etl_schema_changes table exists in database")
        else:
            print_warning("Table creation succeeded but table not found in information_schema")
    
except Exception as e:
    print_error(f"Failed to create etl_schema_changes: {e}")
    print()
    print_info("Full error details:")
    import traceback
    traceback.print_exc()
    print()
    print_warning("This is the EXACT error your application is encountering!")

print()

# ============================================================
# Test 7: Check existing tables
# ============================================================
print("Test 7: List Existing Tables in 'public' Schema")
print("-" * 60)
try:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name, table_type
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """))
        tables = result.fetchall()
        if tables:
            for table_name, table_type in tables:
                print_info(f"  • {table_name} ({table_type})")
        else:
            print_warning("No tables found in public schema")
except Exception as e:
    print_error(f"Failed to list tables: {e}")

print()
print("=" * 60)
print("Test Complete")
print("=" * 60)