"""Normalize step prepares data for further processing."""

from __future__ import annotations

from app.pipeline.status import SKIPPED


def run(**kwargs) -> int:
    """Run the normalize step."""
    return SKIPPED

