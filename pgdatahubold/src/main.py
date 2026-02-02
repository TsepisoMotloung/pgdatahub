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


def interactive_menu():
    """Simple command-based UI for Import and Revert operations."""
    db_conf = load_db_config()
    engine = get_engine(db_conf)

    while True:
        print("\n1) Import data\n2) Revert\n3) Exit")
        choice = input("Choose: ").strip()
        if choice == "1":
            default_root = "data"
            data_root = input(f"Data root (default '{default_root}'): ").strip() or default_root
            # prefer existing case-variants if user input doesn't match an existing folder
            if not os.path.isdir(data_root):
                alt = data_root.lower()
                alt2 = data_root.capitalize()
                if os.path.isdir(alt):
                    print(f"Using data directory '{alt}'")
                    data_root = alt
                elif os.path.isdir(alt2):
                    print(f"Using data directory '{alt2}'")
                    data_root = alt2

            cfg_path = input("ETL config path (default 'etl_config.yaml'): ").strip() or "etl_config.yaml"
            run(data_root=data_root, etl_config_path=cfg_path, db_config=db_conf)
        elif choice == "2":
            print("\n1) Revert import\n2) Revert schema\n3) Back")
            sub = input("Choose: ").strip()
            if sub == "1":
                table = input("Table name: ").strip()
                identifier = input("File SHA256 or source file name: ").strip()
                mode = input("Interpret input as (s)ha or (n)ame? (s/n, default: detect): ").strip().lower() or "detect"
                dry = input("Dry run? (y/n): ").strip().lower().startswith("y")
                backups_dir = input(f"Backups directory (default '{None}'): ").strip() or None
                from .db import revert_import, get_backups_root
                try:
                    # decide which revert to call
                    if mode == "s":
                        arg = identifier
                    elif mode == "n":
                        arg = identifier
                    else:
                        arg = identifier

                    if dry:
                        res = revert_import(engine, table, arg, dry_run=True)
                        print("DRY RUN: following would be deleted:")
                        for r in res:
                            print(r)
                        print("Note: a backup would be created at:", res[0].get("planned_backup") if res else get_backups_root())
                    else:
                        # ask for confirmation
                        res = revert_import(engine, table, arg, dry_run=True)
                        if not res:
                            print("No matching import found or nothing to delete.")
                            continue
                        planned = res[0].get("planned_backup")
                        bd = backups_dir or get_backups_root()
                        print(f"This will back up to: {bd}/{table}/<timestamp>_<uniq>.csv and delete {res[0].get('count')} rows.")
                        ok = input("Proceed with backup and revert? (y/n): ").strip().lower().startswith("y")
                        if not ok:
                            print("Aborting.")
                            continue
                        # perform revert, passing backups_root so backups/ location can be controlled
                        deleted = revert_import(engine, table, arg, dry_run=False, backups_root=bd)
                        print(f"Reverted import: deleted {deleted} rows")
                except Exception as e:
                    print(f"Error during revert: {e}")
            elif sub == "2":
                table = input("Table name: ").strip()
                source = input("Source file (as logged in etl_schema_changes): ").strip()
                dry = input("Dry run? (y/n): ").strip().lower().startswith("y")
                backups_dir = input(f"Backups directory (default '{None}'): ").strip() or None
                from .db import revert_schema_changes, get_backups_root, backup_table_rows
                try:
                    if dry:
                        actions = revert_schema_changes(engine, table, source, dry_run=True)
                        print("DRY RUN: actions planned:")
                        for a in actions:
                            print(a)
                        print("Note: a full-table backup would be created at:", actions[0].get("planned_backup") if actions else get_backups_root())
                    else:
                        # dry-run to show planned actions
                        actions = revert_schema_changes(engine, table, source, dry_run=True)
                        print("Planned actions:")
                        for a in actions:
                            print(a)
                        bd = backups_dir or get_backups_root()
                        print(f"This will back up the full table to: {bd}/{table}/<timestamp>_<uniq>.csv")
                        ok = input("Proceed with backup and apply schema revert? (y/n): ").strip().lower().startswith("y")
                        if not ok:
                            print("Aborting.")
                            continue
                        # create backup now so we can show actual path, then call revert_schema_changes with skip_backup=True
                        backup_path, backed = backup_table_rows(engine, table, backups_root=bd)
                        print(f"Created backup: {backup_path} ({backed} rows)")
                        performed = revert_schema_changes(engine, table, source, dry_run=False, backups_root=bd, skip_backup=True)
                        print(f"Reverted schema changes: performed {performed} actions")
                except Exception as e:
                    print(f"Error during schema revert: {e}")
            else:
                continue
        elif choice == "3":
            print("Exiting.")
            break
        else:
            print("Invalid choice")


if __name__ == "__main__":
    main()