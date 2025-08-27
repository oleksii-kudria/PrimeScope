"""Utilities for working with CSV files.

This module centralises file-handling helpers shared across steps.
"""

from __future__ import annotations

from pathlib import Path
import csv
from typing import Iterator, Iterable


def list_csv_in_dir(
    dir_path: str,
    ignore_suffixes: list[str] | None = None,
    recursive: bool = False,
) -> list[str]:
    """Return sorted list of CSV file paths within *dir_path*.

    Files whose names end with any suffix in *ignore_suffixes* are skipped.
    When *recursive* is True, the search descends into subdirectories.
    """

    if ignore_suffixes is None:
        ignore_suffixes = ["example.csv"]

    base = Path(dir_path)
    if recursive:
        paths: Iterable[Path] = base.rglob("*.csv")
    else:
        paths = base.glob("*.csv")

    result: list[str] = []
    for p in paths:
        name = p.name.lower()
        if any(name.endswith(s.lower()) for s in ignore_suffixes):
            continue
        if p.is_file():
            result.append(str(p))
    return sorted(result)


def read_headers(path: str) -> list[str]:
    """Return list of header names from CSV file at *path*."""

    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        return next(reader, [])


def open_csv_rows(path: str) -> Iterator[list[str]]:
    """Yield rows from CSV file at *path* (excluding the header)."""

    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        next(reader, None)  # skip header
        for row in reader:
            yield row
