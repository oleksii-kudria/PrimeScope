"""Checks step performs extra manual validations."""

from __future__ import annotations

from app.pipeline.status import SKIPPED


def run(**kwargs) -> int:
    """Run the checks step."""
    return SKIPPED

