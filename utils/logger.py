from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
import os


def setup_logging() -> logging.Logger:
    level_name = (os.getenv("LOG_LEVEL") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logger = logging.getLogger("parsing_bot")
    if logger.handlers:
        return logger

    logger.setLevel(level)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    log_file = os.getenv("AUTOBUY_LOG_FILE") or "autobuy.log"
    max_bytes = int((os.getenv("LOG_MAX_BYTES") or str(15 * 1024 * 1024)).strip())
    backup_count = int((os.getenv("LOG_ROTATE_KEEP") or "2").strip())
    file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger


logger = setup_logging()
