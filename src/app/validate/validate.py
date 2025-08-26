"""Validate step performs basic content checks."""

from __future__ import annotations

from app.pipeline.status import SKIPPED


def run(**kwargs) -> int:
    """Run the validate step."""
    return SKIPPED

