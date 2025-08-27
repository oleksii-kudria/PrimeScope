"""Validate step performs basic content checks."""

from __future__ import annotations

from pathlib import Path

from app.pipeline.status import DONE
from app.utils.logging import get_logger
from app.collectors.files import list_csv_in_dir, read_headers


logger = get_logger(__name__)


def _load_dataset_dirs(config_path: Path) -> list[str]:
    """Return dataset directories from *schemas.yml* config."""

    dirs: list[str] | None = None
    if config_path.exists():
        text = config_path.read_text(encoding="utf-8")
        try:
            import yaml  # type: ignore

            data = yaml.safe_load(text) or {}
            validate = data.get("validate") or {}
            datasets = validate.get("datasets") or {}
            temp: list[str] = []
            for info in datasets.values():
                rel = info.get("dir")
                if isinstance(rel, str):
                    temp.append(rel)
            dirs = temp
        except Exception:
            dirs = None
        if dirs is None:
            temp_dirs: list[str] = []
            in_validate = False
            in_datasets = False
            for line in text.splitlines():
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                if stripped.startswith("validate:"):
                    in_validate = True
                    in_datasets = False
                    continue
                if in_validate and stripped.startswith("datasets:"):
                    in_datasets = True
                    continue
                if in_validate and in_datasets and stripped.startswith("dir:"):
                    val = stripped.split(":", 1)[1].strip().strip('"').strip("'")
                    temp_dirs.append(val)
            if temp_dirs:
                dirs = temp_dirs
    return dirs or []


def run(**kwargs) -> int:
    """Run the validate step."""
    try:
        root = Path(__file__).resolve().parents[3]

        # --- CSV inventory ---
        config_path = root / "configs" / "schemas.yml"
        for rel in _load_dataset_dirs(config_path):
            dir_path = root / rel
            files = list_csv_in_dir(
                str(dir_path), ignore_suffixes=["example.csv"], recursive=False
            )
            if not files:
                logger.info("validate: no csv in: %s", rel)
            else:
                names = [Path(p).name for p in files]
                logger.info(
                    "validate: files in %s: %s", rel, ", ".join(sorted(names))
                )

        # --- Existing header/content checks ---
        raw_dir = root / "data" / "raw"
        files = list_csv_in_dir(str(raw_dir), recursive=True)
        for path in files:
            headers = read_headers(path)
            logger.info(
                "validate: %s headers: %s", Path(path).name, ", ".join(headers)
            )
    except Exception as exc:  # pragma: no cover - minimal error handling
        logger.error("validate: unexpected error: %s", exc)
        return 1
    return DONE

