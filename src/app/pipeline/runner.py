"""Skeleton runner for executing pipeline steps."""

from __future__ import annotations

import importlib
import time
from typing import Dict

from app.utils.logging import get_logger


MODULES: Dict[str, str] = {
    "collect": "app.ingest.collect",
    "validate": "app.validate.validate",
    "normalize": "app.processors.normalize",
    "interim": "app.stage.interim",
    "checks": "app.quality.checks",
    "report": "app.reporters.report",
}


logger = get_logger(__name__)


def run_flow(*, flow: list[str], **kwargs) -> int:
    """Execute the given flow of pipeline steps."""
    for step in flow:
        start = time.perf_counter()
        logger.info("▶ step=%s status=start", step)
        try:
            module = importlib.import_module(MODULES[step])
            code = module.run(**kwargs)
            duration = time.perf_counter() - start
            if code == 0:
                logger.info("✓ step=%s status=done duration=%.3fs", step, duration)
            else:
                logger.info("✖ step=%s status=error duration=%.3fs", step, duration)
                return code
        except Exception:
            duration = time.perf_counter() - start
            logger.info("✖ step=%s status=error duration=%.3fs", step, duration)
            return 1
    return 0

