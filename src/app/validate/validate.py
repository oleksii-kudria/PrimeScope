"""Validate step performs basic content checks."""

from __future__ import annotations

from pathlib import Path

from app.pipeline.status import SKIPPED
from app.utils.logging import get_logger
from app.collectors.files import list_csv_in_dir, read_headers


logger = get_logger(__name__)


def run(**kwargs) -> int:
    """Run the validate step."""
    try:
        root = Path(__file__).resolve().parents[3]
        raw_dir = root / "data" / "raw"
        files = list_csv_in_dir(str(raw_dir), recursive=True)
        if not files:
            logger.info("validate: no csv found")
        else:
            for path in files:
                headers = read_headers(path)
                logger.info(
                    "validate: %s headers: %s", Path(path).name, ", ".join(headers)
                )
    except Exception as exc:  # pragma: no cover - minimal error handling
        logger.error("validate: unexpected error: %s", exc)
        return 1
    return SKIPPED

