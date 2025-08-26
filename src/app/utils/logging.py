from __future__ import annotations

import logging
from pathlib import Path


def setup_logging(level: str = "INFO", log_file: str = "logs/pscope.log") -> None:
    root = logging.getLogger()
    if root.handlers:
        return

    path = Path(log_file)
    path.parent.mkdir(parents=True, exist_ok=True)

    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    fmt = "[%(asctime)s] %(levelname)s: %(message)s"
    formatter = logging.Formatter(fmt)

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    file_handler = logging.FileHandler(path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

