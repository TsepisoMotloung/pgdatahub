"""PGDataHub ETL Setup"""
from setuptools import setup, find_packages

setup(
    name="pgdatahub",
    version="1.0.0",
    description="Robust Excel to PostgreSQL ETL system",
    author="Your Name",
    python_requires=">=3.10",
    packages=find_packages(),
    install_requires=[
        "sqlalchemy>=2.0.0",
        "psycopg[binary]>=3.1.0",
        "pandas>=2.0.0",
        "openpyxl>=3.1.0",
        "xlrd>=2.0.0",
        "pyyaml>=6.0",
        "click>=8.1.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "pgdatahub=src.cli:cli",
        ],
    },
)
