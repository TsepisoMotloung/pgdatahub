"""Pause and resume mechanism for ETL"""
import json
import logging
import time
import datetime
from pathlib import Path
from typing import Optional, Dict, Any

from src.config import config

logger = logging.getLogger(__name__)


class PauseManager:
    """Manages ETL pause and resume functionality"""
    
    PAUSE_FILE = ".etl_pause.json"
    
    def __init__(self, data_root: Path):
        self.pause_file_path = data_root / self.PAUSE_FILE
    
    def write_pause_file(
        self,
        folder: str,
        table: str,
        file: str,
        error: str
    ):
        """
        Write pause file on error.
        
        Records where the ETL stopped so it can be resumed.
        """
        pause_data = {
            "folder": folder,
            "table": table,
            "file": file,
            "error": error,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        
        try:
            with open(self.pause_file_path, 'w') as f:
                json.dump(pause_data, f, indent=2)
            
            logger.info(f"Wrote pause file: {self.pause_file_path}")
            logger.info(f"Paused at: folder={folder}, file={file}")
        except Exception as e:
            logger.error(f"Failed to write pause file: {e}")
    
    def read_pause_file(self) -> Optional[Dict[str, Any]]:
        """
        Read pause file if it exists.
        
        Returns None if no pause file exists.
        """
        if not self.pause_file_path.exists():
            return None
        
        try:
            with open(self.pause_file_path, 'r') as f:
                pause_data = json.load(f)
            
            logger.info(f"Found pause file: {pause_data}")
            return pause_data
        except Exception as e:
            logger.error(f"Failed to read pause file: {e}")
            return None
    
    def delete_pause_file(self):
        """Delete pause file after successful resume"""
        if self.pause_file_path.exists():
            try:
                self.pause_file_path.unlink()
                logger.info("Deleted pause file")
            except Exception as e:
                logger.error(f"Failed to delete pause file: {e}")
    
    def has_pause_file(self) -> bool:
        """Check if pause file exists"""
        return self.pause_file_path.exists()
    
    def execute_pause(self):
        """
        Execute a pause (sleep) based on configuration.
        
        Called periodically during ETL run.
        """
        if config.pause_seconds > 0:
            logger.info(f"Pausing for {config.pause_seconds} seconds...")
            time.sleep(config.pause_seconds)
            logger.info("Resuming ETL")


def resume_from_pause(data_root: Path):
    """
    Resume ETL from pause file.
    
    Re-runs the folder that was paused.
    Deletes pause file on success.
    """
    pause_manager = PauseManager(data_root)
    pause_data = pause_manager.read_pause_file()
    
    if not pause_data:
        logger.info("No pause file found, nothing to resume")
        return
    
    logger.info("=" * 60)
    logger.info("RESUMING FROM PAUSE")
    logger.info("=" * 60)
    logger.info(f"Folder: {pause_data['folder']}")
    logger.info(f"Table: {pause_data['table']}")
    logger.info(f"Last file: {pause_data['file']}")
    logger.info(f"Error: {pause_data['error']}")
    logger.info(f"Paused at: {pause_data['timestamp']}")
    logger.info("=" * 60)
    
    # Import here to avoid circular dependency
    from src.etl import run
    
    try:
        # Re-run ETL (will skip already imported files)
        run(data_root=data_root)
        
        # Success - delete pause file
        pause_manager.delete_pause_file()
        logger.info("Resume completed successfully")
        
    except Exception as e:
        logger.error(f"Resume failed: {e}", exc_info=True)
        raise
