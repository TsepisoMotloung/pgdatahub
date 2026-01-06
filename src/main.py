import logging
import argparse
from .config import load_db_config
from .etl import run
from .db import get_engine

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Folder-driven Excel → PostgreSQL ETL")
    parser.add_argument("--data-root", default="Data", help="Root folder containing tables (default: Data)")
    parser.add_argument("--config", default="etl_config.yaml", help="Path to ETL YAML config")
    parser.add_argument("--db-config", default=None, help="Path to DB config.json (optional)")
    args = parser.parse_args()

    db_conf = load_db_config(args.db_config if args.db_config else "config.json")
    engine = get_engine(db_conf)

    # configure logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    logger.info("Starting ETL for data root: %s", args.data_root)
    run(data_root=args.data_root, etl_config_path=args.config, db_config=db_conf)


if __name__ == "__main__":
    main()
