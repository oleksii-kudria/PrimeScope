"""Collect step opens raw files for processing."""

from __future__ import annotations

import json
import hashlib
from pathlib import Path

from app.pipeline.status import DONE
from app.utils.logging import get_logger


logger = get_logger(__name__)


def _hash_file(path: Path) -> str:
    data = path.read_bytes()
    return hashlib.sha256(data).hexdigest()


def _current_schemas_hash(root: Path) -> str:
    cfg = root / "configs" / "schemas.yml"
    return _hash_file(cfg)


def _verify_fingerprints(manifest: dict, root: Path) -> bool:
    for ds in manifest.get("datasets", {}).values():
        for info in ds.get("files", []):
            file_path = root / info["path"]
            if not file_path.exists():
                return False
            stat = file_path.stat()
            fp = info.get("fingerprint", {})
            if fp.get("size") != stat.st_size or fp.get("mtime") != int(stat.st_mtime):
                return False
    return True


def run(*, validated_manifest: dict | None = None, **kwargs) -> int:  # noqa: D401
    """Run the collect step."""
    try:
        root = Path(__file__).resolve().parents[3]
        manifest_path = root / ".pscope" / "latest.json"
        if validated_manifest is None:
            if not manifest_path.exists():
                logger.error("collect: manifest missing -> run 'validate' first")
                return 1
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        else:
            manifest = validated_manifest

        total_files = sum(
            len(ds.get("files", [])) for ds in manifest.get("datasets", {}).values()
        )
        logger.info(
            "collect: using manifest %s (schemas_hash=%s, files=%d)",
            str(manifest_path),
            manifest.get("schemas_hash", ""),
            total_files,
        )

        current_hash = _current_schemas_hash(root)
        if manifest.get("schemas_hash") != current_hash:
            logger.error("collect: manifest stale (schema changed) -> run 'validate'")
            return 1

        if not _verify_fingerprints(manifest, root):
            logger.error("collect: manifest stale (inputs changed) -> run 'validate'")
            return 1
    except Exception as exc:  # pragma: no cover - minimal error handling
        logger.error("collect: unexpected error: %s", exc)
        return 1
    return DONE

