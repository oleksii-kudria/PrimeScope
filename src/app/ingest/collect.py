"""Collect step opens raw files for processing."""

from __future__ import annotations

from pathlib import Path

from app.pipeline.status import DONE
from app.utils.logging import get_logger
from app.collectors.files import list_csv_in_dir


DEFAULT_INPUT_DIRS = [
    "data/raw/dhcp",
    "data/raw/arm",
    "data/raw/mkp",
    "data/raw/other",
    "data/raw/ubiq",
    "data/raw/siem",
]

logger = get_logger(__name__)


def run(**kwargs) -> int:  # noqa: D401
    """Run the collect step."""
    try:
        root = Path(__file__).resolve().parents[3]
        for rel in DEFAULT_INPUT_DIRS:
            dir_path = root / rel
            if not dir_path.is_dir():
                logger.info("collect: dir not found: %s", rel)
                continue
            # Inventory of CSV files handled in validate step
            list_csv_in_dir(str(dir_path))
    except Exception as exc:  # pragma: no cover - minimal error handling
        logger.error("collect: unexpected error: %s", exc)
        return 1
    return DONE

