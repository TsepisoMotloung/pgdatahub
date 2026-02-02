"""Pause and resume management for ETL operations."""

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from .utils import logger
from .config import get_config


class PauseManager:
    """Manages ETL pause state and resume operations."""

    PAUSE_FILE = '.etl_pause.json'

    def __init__(self, data_root: Path):
        self.data_root = data_root
        self.config = get_config()
        self.pause_file_path = data_root / self.PAUSE_FILE

    def write_pause_state(self, folder: Path, table: str, file: Path,
                          error: str) -> None:
        """Write pause state to file.

        Args:
            folder: Current folder being processed
            table: Target table name
            file: Current file being processed
            error: Error message
        """
        pause_state = {
            'folder': str(folder),
            'table': table,
            'file': str(file),
            'error': error,
            'timestamp': datetime.utcnow().isoformat(),
            'data_root': str(self.data_root)
        }

        with open(self.pause_file_path, 'w') as f:
            json.dump(pause_state, f, indent=2)

        logger.error(f"ETL paused. State saved to {self.pause_file_path}")
        logger.error(f"Error: {error}")

    def read_pause_state(self) -> Optional[Dict[str, Any]]:
        """Read pause state from file.

        Returns:
            Pause state dict or None if no pause file exists
        """
        if not self.pause_file_path.exists():
            return None

        try:
            with open(self.pause_file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error reading pause file: {e}")
            return None

    def clear_pause_state(self) -> bool:
        """Clear pause state file.

        Returns:
            True if pause file was removed
        """
        if self.pause_file_path.exists():
            try:
                self.pause_file_path.unlink()
                logger.info(f"Pause file removed: {self.pause_file_path}")
                return True
            except Exception as e:
                logger.error(f"Error removing pause file: {e}")
                return False
        return True

    def has_pause_state(self) -> bool:
        """Check if a pause state exists.

        Returns:
            True if pause file exists
        """
        return self.pause_file_path.exists()

    def should_pause_periodic(self, files_processed: int) -> bool:
        """Check if periodic pause should occur.

        Args:
            files_processed: Number of files processed so far

        Returns:
            True if should pause
        """
        if self.config.pause_every <= 0:
            return False

        return files_processed > 0 and files_processed % self.config.pause_every == 0

    def do_periodic_pause(self) -> None:
        """Execute periodic pause."""
        if self.config.pause_seconds > 0:
            logger.info(f"Periodic pause: sleeping for {self.config.pause_seconds} seconds")
            time.sleep(self.config.pause_seconds)

    def get_resume_folder(self) -> Optional[Path]:
        """Get folder to resume from.

        Returns:
            Folder path or None if no pause state
        """
        state = self.read_pause_state()
        if state:
            return Path(state['folder'])
        return None


class TransactionManager:
    """Manages transactions with sectional commit support."""

    def __init__(self, db_manager, pause_manager: PauseManager):
        self.db = db_manager
        self.pause_manager = pause_manager
        self.config = get_config()
        self._pending_inserts: list = []
        self._current_folder: Optional[Path] = None

    def start_folder(self, folder: Path) -> None:
        """Start processing a new folder.

        Args:
            folder: Folder being processed
        """
        self._current_folder = folder
        self._pending_inserts = []

    def add_insert(self, table_name: str, data: Dict[str, Any]) -> None:
        """Queue an insert operation.

        Args:
            table_name: Target table
            data: Row data to insert
        """
        self._pending_inserts.append((table_name, data))

    def commit_folder(self) -> bool:
        """Commit all pending inserts for current folder.

        Returns:
            True if commit successful
        """
        if not self._pending_inserts:
            return True

        if self.config.skip_db:
            logger.info(f"SKIP_DB: Would commit {len(self._pending_inserts)} rows")
            self._pending_inserts = []
            return True

        try:
            with self.db.transaction() as conn:
                if conn:  # conn is None in skip_db mode
                    from sqlalchemy import insert

                    # Group by table
                    by_table: Dict[str, list] = {}
                    for table_name, data in self._pending_inserts:
                        if table_name not in by_table:
                            by_table[table_name] = []
                        by_table[table_name].append(data)

                    # Bulk insert per table
                    for table_name, rows in by_table.items():
                        table = self.db.metadata.tables.get(table_name)
                        if table:
                            conn.execute(insert(table), rows)

            self._pending_inserts = []
            return True

        except Exception as e:
            logger.error(f"Commit failed for folder {self._current_folder}: {e}")
            return False

    def rollback_folder(self) -> None:
        """Rollback pending inserts for current folder."""
        count = len(self._pending_inserts)
        self._pending_inserts = []
        logger.info(f"Rolled back {count} pending inserts")
