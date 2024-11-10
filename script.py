from __future__ import annotations

from logging import getLogger

from utilities.logging import setup_logging

setup_logging(
    files_when="S", files_interval=10, files_max_bytes=150, files_backup_count=3
)


logger = getLogger(__name__)
logger.info("Hello, world!")
