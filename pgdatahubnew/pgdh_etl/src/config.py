"""Configuration management for PGDataHub ETL."""

import os
import json
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Configuration manager for ETL operations."""

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or "config.json"
        self.etl_config_path = "etl_config.yaml"
        self._config: Dict[str, Any] = {}
        self._etl_config: Dict[str, Any] = {}
        self._load_configs()

    def _load_configs(self) -> None:
        """Load all configuration files."""
        self._load_json_config()
        self._load_yaml_config()

    def _load_json_config(self) -> None:
        """Load JSON configuration file."""
        if Path(self.config_path).exists():
            with open(self.config_path, 'r') as f:
                content = f.read()
                # Replace environment variables
                content = os.path.expandvars(content)
                self._config = json.loads(content)

    def _load_yaml_config(self) -> None:
        """Load YAML ETL configuration file."""
        if Path(self.etl_config_path).exists():
            with open(self.etl_config_path, 'r') as f:
                self._etl_config = yaml.safe_load(f) or {}

    @property
    def database_url(self) -> str:
        """Get database URL with priority: env var > config file."""
        # Priority 1: Environment variable
        db_url = os.getenv('DATABASE_URL')
        if db_url:
            return db_url

        # Priority 2: Build from config.json
        db_config = self._config.get('database', {})
        host = db_config.get('host', 'localhost')
        port = db_config.get('port', 5432)
        database = db_config.get('database', 'pgdh_db')
        user = db_config.get('user', 'pgdh_user')
        password = db_config.get('password', '')

        if not password:
            password = os.getenv('DB_PASSWORD', '')

        return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"

    @property
    def chunk_size(self) -> int:
        """Get chunk size for Excel processing."""
        env_chunk = os.getenv('ETL_CHUNK_SIZE')
        if env_chunk:
            return int(env_chunk)
        return self._config.get('etl', {}).get('chunk_size', 10000)

    @property
    def sectional_commit(self) -> bool:
        """Whether to use sectional commit (folder-level transactions)."""
        return os.getenv('ETL_SECTIONAL_COMMIT', '0') == '1'

    @property
    def pause_every(self) -> int:
        """Number of files to process before pausing."""
        return int(os.getenv('ETL_PAUSE_EVERY', '0'))

    @property
    def pause_seconds(self) -> int:
        """Seconds to pause between batches."""
        return int(os.getenv('ETL_PAUSE_SECONDS', '0'))

    @property
    def skip_db(self) -> bool:
        """Whether to skip database writes (dry run mode)."""
        return os.getenv('SKIP_DB', '0') == '1'

    @property
    def supported_extensions(self) -> list:
        """List of supported Excel file extensions."""
        return self._config.get('etl', {}).get(
            'supported_extensions',
            ['.xlsx', '.xls', '.xlsm', '.xlsb']
        )

    @property
    def case_sensitive_folders(self) -> bool:
        """Whether folder names are case-sensitive."""
        return self._config.get('etl', {}).get('case_sensitive_folders', False)

    def get_sheet_name(self, folder_path_parts: list) -> str:
        """Resolve sheet name for a given folder path.

        Args:
            folder_path_parts: List of folder path components

        Returns:
            Sheet name to use for Excel files in this folder
        """
        current = self._etl_config

        for part in folder_path_parts:
            if not self.case_sensitive_folders:
                part = part.lower()

            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                # Return default if path not found
                return self._etl_config.get('default_sheet', 'Sheet1')

        if isinstance(current, dict) and 'sheet' in current:
            return current['sheet']

        return self._etl_config.get('default_sheet', 'Sheet1')

    def get_default_sheet(self) -> str:
        """Get default sheet name."""
        return self._etl_config.get('default_sheet', 'Sheet1')


# Global config instance
_config_instance: Optional[Config] = None


def get_config(config_path: Optional[str] = None) -> Config:
    """Get or create global config instance."""
    global _config_instance
    if _config_instance is None or config_path:
        _config_instance = Config(config_path)
    return _config_instance
