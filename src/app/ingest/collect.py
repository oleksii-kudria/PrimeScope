"""Collect step opens raw files for processing."""

from __future__ import annotations

from pathlib import Path

from app.pipeline.status import DONE
from app.utils.logging import get_logger


DEFAULT_INPUT_DIRS = [
    "data/raw/dhcp",
    "data/raw/arm",
    "data/raw/mkp",
    "data/raw/other",
    "data/raw/ubiq",
    "data/raw/siem",
]

logger = get_logger(__name__)


def _load_input_dirs(config_path: Path) -> list[str]:
    """Return list of input directories from config or defaults."""
    dirs: list[str] | None = None
    if config_path.exists():
        text = config_path.read_text(encoding="utf-8")
        try:
            import yaml  # type: ignore

            data = yaml.safe_load(text) or {}
            collect = data.get("collect") or {}
            cfg_dirs = collect.get("input_dirs")
            if isinstance(cfg_dirs, list):
                dirs = [str(d) for d in cfg_dirs]
        except Exception:
            dirs = None
        if dirs is None:
            temp_dirs: list[str] = []
            in_collect = False
            in_list = False
            for line in text.splitlines():
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                if stripped.startswith("collect:"):
                    in_collect = True
                    in_list = False
                    continue
                if in_collect and stripped.startswith("input_dirs:"):
                    in_list = True
                    continue
                if in_collect and in_list:
                    if stripped.startswith("- "):
                        temp_dirs.append(stripped[2:].strip())
                    elif not stripped.startswith("#"):
                        break
            if temp_dirs:
                dirs = temp_dirs
    return dirs or DEFAULT_INPUT_DIRS


def run(**kwargs) -> int:  # noqa: D401
    """Run the collect step."""
    try:
        root = Path(__file__).resolve().parents[3]
        config_path = root / "configs" / "base.yml"
        input_dirs = _load_input_dirs(config_path)
        for rel in input_dirs:
            dir_path = root / rel
            if not dir_path.is_dir():
                logger.info("collect: dir not found: %s", rel)
                continue
            files = [
                p.name
                for p in dir_path.iterdir()
                if p.is_file()
                and p.suffix.lower() == ".csv"
                and not p.name.lower().endswith("example.csv")
            ]
            if not files:
                logger.info("collect: no csv in: %s", rel)
            else:
                logger.info(
                    "collect: files in %s: %s", rel, ", ".join(sorted(files))
                )
    except Exception as exc:  # pragma: no cover - minimal error handling
        logger.error("collect: unexpected error: %s", exc)
        return 1
    return DONE

