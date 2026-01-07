import os
import yaml


DEFAULT_CONFIG_FILE = "etl_config.yaml"


def load_etl_config(path: str = None) -> dict:
    path = path or DEFAULT_CONFIG_FILE
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}
    return cfg


def get_sheet_name_for_folder(cfg: dict, folder_parts: list) -> str:
    """Traverse the nested config dict using folder_parts and return sheet_name.

    For example, folder_parts ['sales','clients'] will look for cfg['sales']['clients']['sheet_name']
    or fallback to a closer ancestor with 'sheet_name' defined.
    """
    node = cfg
    found_sheet = None
    for part in folder_parts:
        if not isinstance(node, dict):
            break
        node = node.get(part)
        if not node:
            break
        if isinstance(node, dict) and "sheet_name" in node:
            found_sheet = node.get("sheet_name")
    return found_sheet


def load_db_config(path: str = "config.json") -> dict:
    """Load DB configuration as fallback (keeps compatibility with project)."""
    import json

    if os.environ.get("DATABASE_URL"):
        url = os.environ.get("DATABASE_URL")
        # Normalize common PostgreSQL URL patterns to ensure psycopg2 driver is explicit.
        # e.g., convert 'postgresql://user:pass@host:port/db' to 'postgresql+psycopg2://user:pass@host:port/db'
        if url.startswith("postgresql://") and not url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
        return {"url": url}

    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            cfg = json.load(fh)
        return cfg.get("database", {})

    return {}
