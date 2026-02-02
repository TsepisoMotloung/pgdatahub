#!/usr/bin/env python3
"""Main entry point for PGDataHub ETL."""

import argparse
import sys
from pathlib import Path

from src.etl import run, resume_from_pause
from src.utils import logger


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='PGDataHub ETL - Excel to PostgreSQL ETL System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment Variables:
  DATABASE_URL          PostgreSQL connection string (preferred)
  ETL_SECTIONAL_COMMIT  Set to 1 for folder-level transactions
  ETL_PAUSE_EVERY       Pause after N files (0 to disable)
  ETL_PAUSE_SECONDS     Seconds to pause between batches
  ETL_CHUNK_SIZE        Rows to read per chunk (default: 10000)
  SKIP_DB               Set to 1 for dry run (no DB writes)

Examples:
  python main.py --data-root data
  python main.py --data-root data --resume
  ETL_SECTIONAL_COMMIT=1 ETL_PAUSE_EVERY=5 python main.py
        """
    )

    parser.add_argument(
        '--data-root',
        default='data',
        help='Root directory containing data folders (default: data)'
    )

    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from previous pause state'
    )

    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 1.0.0'
    )

    args = parser.parse_args()

    data_root = Path(args.data_root)

    if not data_root.exists():
        logger.error(f"Data root does not exist: {data_root}")
        logger.info("Creating data directory structure...")
        data_root.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created: {data_root}")
        logger.info("Please add your Excel files to subdirectories and run again.")
        return 1

    if args.resume:
        logger.info("Resuming ETL from pause state...")
        success = resume_from_pause(str(data_root))
    else:
        logger.info("Starting PGDataHub ETL...")
        success = run(str(data_root))

    if success:
        logger.info("ETL completed successfully")
        return 0
    else:
        logger.error("ETL failed")
        return 1


if __name__ == '__main__':
    sys.exit(main())
