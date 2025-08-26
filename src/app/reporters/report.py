"""Report step produces final summaries."""

from __future__ import annotations

from app.pipeline.status import SKIPPED


def run(**kwargs) -> int:
    """Run the report step."""
    return SKIPPED

