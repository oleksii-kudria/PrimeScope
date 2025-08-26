"""Interim step builds temporary artifacts."""

from __future__ import annotations

from app.pipeline.status import SKIPPED


def run(**kwargs) -> int:
    """Run the interim step."""
    return SKIPPED

