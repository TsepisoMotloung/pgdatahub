"""Main entry point for PGDataHub ETL"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.cli import cli

if __name__ == '__main__':
    cli()
