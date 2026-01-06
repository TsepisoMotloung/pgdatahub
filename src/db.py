import logging
from sqlalchemy import create_engine, MetaData, Table, Column, text
from sqlalchemy import Integer, BigInteger, Float, Numeric, String, DateTime, Date, Boolean, LargeBinary, Text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import column
from datetime import datetime
logger = logging.getLogger(__name__)


def get_engine(db_config: dict):
    # Accept either a URL string in db_config['url'] or a dict with keys
    if not db_config:
        raise ValueError("Database configuration not provided")
    if "url" in db_config:
        return create_engine(db_config["url"])
    user = db_config.get("user")
    password = db_config.get("password")
    host = db_config.get("host", "localhost")
    port = db_config.get("port", 5432)
    database = db_config.get("database")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)


def ensure_schema_table(engine):
    # Create the etl_schema_changes table in a database-agnostic way
    from sqlalchemy import Table, Column, Integer, Text, MetaData

    md = MetaData()
    table = Table(
        "etl_schema_changes",
        md,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("table_name", Text, nullable=False),
        Column("column_name", Text),
        Column("old_type", Text),
        Column("new_type", Text),
        Column("change_type", Text, nullable=False),
        Column("change_timestamp", DateTime),
        Column("migration_query", Text),
        Column("source_file", Text),
    )
    md.create_all(engine, tables=[table])


def reflect_table_columns(engine, table_name):
    md = MetaData()
    try:
        table = Table(table_name, md, autoload_with=engine)
    except Exception:
        return {}
    cols = {}
    for c in table.columns:
        cols[c.name] = str(c.type)
    return cols


def pandas_dtype_to_sqlalchemy(dtype):
    # dtype is a numpy/pandas dtype name
    t = str(dtype)
    if "int" in t:
        return Integer
    if "float" in t:
        return Float
    if "datetime" in t:
        return DateTime
    if "bool" in t:
        return Boolean
    if "object" in t or "str" in t:
        return String
    return String


def create_table_from_df(engine, table_name, df):
    # Use pandas to_sql to create table
    try:
        df.to_sql(table_name, engine, if_exists="fail", index=False)
        logger.info("Table %s created", table_name)
        return True
    except Exception as e:
        logger.error("Failed to create table %s: %s", table_name, e)
        raise


def add_column(engine, table_name, column_name, sqlalchemy_type):
    q = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {sqlalchemy_type}"
    with engine.begin() as conn:
        conn.execute(text(q))
    return q


# Very small set of safe widenings allowed
SAFE_WIDENINGS = [
    ("SMALLINT", "INTEGER"),
    ("INTEGER", "BIGINT"),
    ("INTEGER", "DOUBLE PRECISION"),
    ("REAL", "DOUBLE PRECISION"),
    ("VARCHAR", "TEXT"),
]


def is_safe_widening(old_type, new_type):
    o = old_type.upper() if old_type else ""
    n = new_type.upper() if new_type else ""
    return (o, n) in SAFE_WIDENINGS


def alter_column_type(engine, table_name, column_name, new_type):
    q = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {new_type} USING {column_name}::{new_type}"
    with engine.begin() as conn:
        conn.execute(text(q))
    return q


def log_schema_change(engine, table_name, column_name, old_type, new_type, change_type, migration_query, source_file=None):
    sql = text(
        "INSERT INTO etl_schema_changes (table_name, column_name, old_type, new_type, change_type, change_timestamp, migration_query, source_file)"
        " VALUES (:table_name, :column_name, :old_type, :new_type, :change_type, :change_timestamp, :migration_query, :source_file)"
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "table_name": table_name,
                "column_name": column_name,
                "old_type": old_type,
                "new_type": new_type,
                "change_type": change_type,
                "change_timestamp": datetime.utcnow(),
                "migration_query": migration_query,
                "source_file": source_file,
            },
        )


def insert_dataframe(engine, table_name, df):
    try:
        df.to_sql(table_name, engine, if_exists="append", index=False, method="multi")
    except Exception as e:
        logger.error("Failed to insert rows into %s: %s", table_name, e)
        raise
