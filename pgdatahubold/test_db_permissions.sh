#!/bin/bash

# Test PostgreSQL connection and permissions
# Usage: ./test_db_permissions.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================="
echo "PostgreSQL Connection & Permissions Test"
echo "========================================="
echo ""

# Load DATABASE_URL from .env if it exists
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
    echo -e "${GREEN}✓${NC} Loaded DATABASE_URL from .env"
else
    echo -e "${YELLOW}⚠${NC} No .env file found, using environment variable"
fi

if [ -z "$DATABASE_URL" ]; then
    echo -e "${RED}✗${NC} DATABASE_URL not set!"
    exit 1
fi

echo ""
echo "Testing connection..."

# Extract connection details for display (mask password)
MASKED_URL=$(echo "$DATABASE_URL" | sed -E 's/(:[^:@]+)@/:*****@/')
echo "URL: $MASKED_URL"
echo ""

# Test 1: Basic connection
echo "Test 1: Basic Connection"
psql "$DATABASE_URL" -c "SELECT version();" > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓${NC} Connection successful"
else
    echo -e "${RED}✗${NC} Connection failed!"
fi

# Test 2: Check current user
echo ""
echo "Test 2: Current User & Database"
psql "$DATABASE_URL" -t -c "SELECT current_user, current_database();" | while read -r line; do
    echo -e "${GREEN}✓${NC} $line"
done

# Test 3: Check schema privileges
echo ""
echo "Test 3: Schema Privileges on 'public'"
psql "$DATABASE_URL" -t -c "
SELECT 
    'User: ' || grantee || ' | Schema: ' || table_schema || ' | Privilege: ' || privilege_type
FROM information_schema.schema_privileges 
WHERE grantee = current_user 
  AND table_schema = 'public';" | while read -r line; do
    if [ ! -z "$line" ]; then
        echo -e "${GREEN}✓${NC} $line"
    fi
done

# Test 4: Try to create a test table
echo ""
echo "Test 4: CREATE TABLE Permission"
psql "$DATABASE_URL" <<EOF 2>&1
DROP TABLE IF EXISTS __test_permissions__;
CREATE TABLE __test_permissions__ (id SERIAL PRIMARY KEY, test TEXT);
INSERT INTO __test_permissions__ (test) VALUES ('success');
SELECT * FROM __test_permissions__;
DROP TABLE __test_permissions__;
EOF

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓${NC} CREATE TABLE successful"
else
    echo -e "${RED}✗${NC} CREATE TABLE failed - permission denied!"
    echo ""
    echo "Checking detailed permissions..."
    psql "$DATABASE_URL" -c "
    SELECT 
        r.rolname as user,
        CASE WHEN r.rolsuper THEN 'Yes' ELSE 'No' END as is_superuser,
        CASE WHEN r.rolcreatedb THEN 'Yes' ELSE 'No' END as can_create_db,
        CASE WHEN r.rolcreaterole THEN 'Yes' ELSE 'No' END as can_create_role
    FROM pg_roles r 
    WHERE r.rolname = current_user;"
    exit 1
fi

# Test 5: Check if etl_schema_changes table exists
echo ""
echo "Test 5: Check Existing ETL Tables"
psql "$DATABASE_URL" -t -c "
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
  AND table_name IN ('etl_schema_changes', 'etl_imports');" | while read -r table; do
    if [ ! -z "$table" ]; then
        echo -e "${GREEN}✓${NC} Found table: $table"
    fi
done

EXISTING=$(psql "$DATABASE_URL" -t -c "
SELECT COUNT(*) 
FROM information_schema.tables 
WHERE table_schema = 'public' 
  AND table_name IN ('etl_schema_changes', 'etl_imports');")

if [ "$EXISTING" -eq "0" ]; then
    echo -e "${YELLOW}⚠${NC} No ETL tables found (will be created on first run)"
fi

# Test 6: Check for schema ownership
echo ""
echo "Test 6: Schema Ownership"
psql "$DATABASE_URL" -c "
SELECT 
    n.nspname as schema_name,
    r.rolname as owner
FROM pg_namespace n
JOIN pg_roles r ON n.nspowner = r.oid
WHERE n.nspname = 'public';"

echo ""
echo "========================================="
echo -e "${GREEN}All tests passed!${NC}"
echo "========================================="