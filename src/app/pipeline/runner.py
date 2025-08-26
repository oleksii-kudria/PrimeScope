"""Skeleton runner for executing pipeline steps."""

from __future__ import annotations

import importlib
from typing import Dict


MODULES: Dict[str, str] = {
    "collect": "app.ingest.collect",
    "validate": "app.validate.validate",
    "normalize": "app.processors.normalize",
    "interim": "app.stage.interim",
    "checks": "app.quality.checks",
    "report": "app.reporters.report",
}


def run_flow(*, flow: list[str], **kwargs) -> int:
    """Execute the given flow of pipeline steps."""
    for step in flow:
        module = importlib.import_module(MODULES[step])
        code = module.run(**kwargs)
        if code != 0:
            return code
    return 0

