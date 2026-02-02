"""Configuration management for PGDataHub ETL"""
import os
import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any
import yaml

logger = logging.getLogger(__name__)


class Config:
    """Central configuration manager"""
    
    def __init__(self):
        self.data_root: Path = Path("data")
        self.config_dir: Path = Path("config")
        self.etl_config_path: Path = self.config_dir / "etl_config.yaml"
        self.db_config_path: Path = self.config_dir / "config.json"
        
        # Load ETL config
        self.etl_config: Dict[str, Any] = self._load_etl_config()
        
        # Database connection
        self.database_url: Optional[str] = self._get_database_url()
        
        # ETL behavior
        self.sectional_commit: bool = self._get_bool_env("ETL_SECTIONAL_COMMIT", False)
        self.pause_every: int = int(os.getenv("ETL_PAUSE_EVERY", "0"))
        self.pause_seconds: int = int(os.getenv("ETL_PAUSE_SECONDS", "30"))
        self.chunk_size: int = int(os.getenv("ETL_CHUNK_SIZE", "10000"))
        
        # Runtime flags
        self.skip_db: bool = self._get_bool_env("SKIP_DB", False)
        self.debug: bool = self._get_bool_env("DEBUG", False)
        
    def _load_etl_config(self) -> Dict[str, Any]:
        """Load ETL configuration from YAML"""
        if not self.etl_config_path.exists():
            logger.warning(f"ETL config not found: {self.etl_config_path}")
            return {"default_sheet": "Sheet1"}
        
        try:
            with open(self.etl_config_path, 'r') as f:
                config = yaml.safe_load(f) or {}
                logger.info(f"Loaded ETL config from {self.etl_config_path}")
                return config
        except Exception as e:
            logger.error(f"Failed to load ETL config: {e}")
            return {"default_sheet": "Sheet1"}
    
    def _get_database_url(self) -> Optional[str]:
        """Get database URL from environment or config file"""
        # Try environment variable first
        url = os.getenv("DATABASE_URL")
        if url:
            logger.info("Using DATABASE_URL from environment")
            return url
        
        # Fallback to config.json
        if not self.db_config_path.exists():
            logger.error(f"No DATABASE_URL and config file not found: {self.db_config_path}")
            return None
        
        try:
            with open(self.db_config_path, 'r') as f:
                config = json.load(f)
                db_config = config.get("database", {})
                
                url = (
                    f"postgresql://{db_config['user']}:{db_config['password']}"
                    f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
                )
                logger.info(f"Using database config from {self.db_config_path}")
                return url
        except Exception as e:
            logger.error(f"Failed to load database config: {e}")
            return None
    
    def _get_bool_env(self, key: str, default: bool = False) -> bool:
        """Get boolean from environment variable"""
        value = os.getenv(key, "").lower()
        if value in ("1", "true", "yes"):
            return True
        elif value in ("0", "false", "no"):
            return False
        return default
    
    def resolve_sheet_name(self, folder_parts: list[str]) -> str:
        """
        Resolve Excel sheet name from folder path parts.
        
        Example:
            folder_parts = ["folder_b", "nested"]
            Returns "Claims" based on config
        """
        config = self.etl_config
        
        # Walk through path parts
        for part in folder_parts:
            part_lower = part.lower()
            
            # Check case-insensitive match
            for key in config.keys():
                if key.lower() == part_lower and isinstance(config[key], dict):
                    config = config[key]
                    break
        
        # Get sheet name
        sheet = config.get("sheet", self.etl_config.get("default_sheet", "Sheet1"))
        
        logger.debug(f"Resolved sheet name '{sheet}' for path: {'/'.join(folder_parts)}")
        return sheet
    
    def get_table_name(self, folder_parts: list[str]) -> str:
        """
        Generate table name from folder path.
        Joins with underscores, lowercase.
        """
        table_name = "_".join(folder_parts).lower()
        logger.debug(f"Generated table name: {table_name}")
        return table_name


# Global config instance
config = Config()
