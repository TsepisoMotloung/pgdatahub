#!/usr/bin/env python3
"""
Database Connection Diagnostic Tool
Tests if your PostgreSQL server is reachable and properly configured.
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("=" * 70)
print("DATABASE CONNECTION DIAGNOSTIC")
print("=" * 70)

# Step 1: Check configuration
print("\n1. Checking configuration...")
db_url = os.environ.get("DATABASE_URL")

if db_url:
    print("   ✓ DATABASE_URL found in environment")
    # Parse URL to show host (mask password)
    try:
        from urllib.parse import urlparse
        parsed = urlparse(db_url)
        print(f"   Host: {parsed.hostname}")
        print(f"   Port: {parsed.port or 5432}")
        print(f"   Database: {parsed.path.lstrip('/')}")
        print(f"   User: {parsed.username}")
        host = parsed.hostname
        port = parsed.port or 5432
    except Exception as e:
        print(f"   ✗ Could not parse DATABASE_URL: {e}")
        sys.exit(1)
else:
    print("   ✗ DATABASE_URL not found")
    print("   Checking config.json...")
    
    import json
    if os.path.exists("config.json"):
        with open("config.json") as f:
            config = json.load(f)
        db_config = config.get("database", {})
        host = db_config.get("host", "localhost")
        port = db_config.get("port", 5432)
        print(f"   Host: {host}")
        print(f"   Port: {port}")
        print(f"   Database: {db_config.get('database')}")
        print(f"   User: {db_config.get('user')}")
    else:
        print("   ✗ config.json not found")
        print("\n   ERROR: No database configuration found!")
        print("   Create a .env file with DATABASE_URL or config.json")
        sys.exit(1)

# Step 2: Test network connectivity
print(f"\n2. Testing network connectivity to {host}:{port}...")
import socket
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    result = sock.connect_ex((host, port))
    sock.close()
    
    if result == 0:
        print(f"   ✓ Port {port} is open and accepting connections")
    else:
        print(f"   ✗ Cannot connect to {host}:{port}")
        print(f"   Error code: {result}")
        print("\n   POSSIBLE CAUSES:")
        print("   • PostgreSQL server is not running")
        print("   • Firewall is blocking the connection")
        print("   • Wrong host/port in configuration")
        print("   • Server is not listening on this network interface")
        sys.exit(1)
except socket.gaierror:
    print(f"   ✗ Cannot resolve hostname: {host}")
    print("   Check if the hostname is correct")
    sys.exit(1)
except Exception as e:
    print(f"   ✗ Network error: {e}")
    sys.exit(1)

# Step 3: Test PostgreSQL connection
print("\n3. Testing PostgreSQL connection...")
try:
    import psycopg2
    
    if db_url:
        # Use DATABASE_URL
        url = db_url.replace("postgresql+psycopg2://", "postgresql://")
        conn = psycopg2.connect(url)
    else:
        # Use config.json
        conn = psycopg2.connect(**db_config)
    
    print("   ✓ Successfully connected to PostgreSQL!")
    
    # Test a simple query
    with conn.cursor() as cur:
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        print(f"   ✓ PostgreSQL version: {version[:60]}...")
        
        cur.execute("SELECT current_database();")
        db = cur.fetchone()[0]
        print(f"   ✓ Connected to database: {db}")
    
    conn.close()
    print("\n" + "=" * 70)
    print("SUCCESS! Database is reachable and working.")
    print("=" * 70)
    
except psycopg2.OperationalError as e:
    print(f"   ✗ PostgreSQL connection failed: {e}")
    print("\n   TROUBLESHOOTING:")
    print("   1. Verify PostgreSQL is running:")
    print("      $ sudo systemctl status postgresql")
    print("   2. Check if server is listening on the correct interface:")
    print("      $ sudo netstat -tlnp | grep 5432")
    print("   3. Verify credentials are correct")
    print("   4. Check PostgreSQL logs for errors")
    sys.exit(1)
except Exception as e:
    print(f"   ✗ Unexpected error: {e}")
    sys.exit(1)
