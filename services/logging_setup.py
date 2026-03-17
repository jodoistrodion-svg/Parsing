from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler


def setup_logging(log_file: str, max_bytes: int, backup_count: int) -> logging.Logger:
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    if not root.handlers:
        root.setLevel(logging.INFO)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        root.addHandler(stream_handler)

    autobuy_logger = logging.getLogger("autobuy")
    autobuy_logger.setLevel(logging.INFO)

    has_file_handler = any(
        isinstance(h, RotatingFileHandler) and getattr(h, "baseFilename", "").endswith(log_file)
        for h in autobuy_logger.handlers
    )
    if not has_file_handler:
        file_handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        autobuy_logger.addHandler(file_handler)

    autobuy_logger.propagate = True
    return autobuy_logger
