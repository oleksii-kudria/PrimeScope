"""Collect step consolidates validated CSV files.

This step reads the manifest produced by :mod:`validate` and for each
dataset that contains validated files exports a single CSV file under
``data/stage/collect``.  Only canonical fields described in the manifest are
written and real headers are renamed to their canonical counterparts.

The implementation intentionally avoids external dependencies and relies only
on the Python standard library.
"""

from __future__ import annotations

import json
import hashlib
from pathlib import Path
import csv
from collections import OrderedDict

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

        datasets = manifest.get("datasets", {})
        logger.info(
            "collect: using manifest %s (schemas_hash=%s, datasets=%d)",
            str(manifest_path),
            manifest.get("schemas_hash", ""),
            len(datasets),
        )

        current_hash = _current_schemas_hash(root)
        if manifest.get("schemas_hash") != current_hash:
            logger.error("collect: manifest stale (schema changed) -> run 'validate'")
            return 1

        if not _verify_fingerprints(manifest, root):
            logger.error("collect: manifest stale (inputs changed) -> run 'validate'")
            return 1

        out_dir = root / "data" / "stage" / "collect"
        out_dir.mkdir(parents=True, exist_ok=True)

        datasets_written = 0

        for ds_name, ds in datasets.items():
            files = ds.get("files") or []
            if not files:
                logger.info("collect: skipped %s: no files", ds_name)
                continue

            # Build canonical field order and headers map common to all files
            fields_order: list[str] = []
            headers_map: OrderedDict[str, str] = OrderedDict()

            first = True
            for info in files:
                file_map = info.get("headers_map") or {}
                if first:
                    # preserve order from columns_present if available
                    order = info.get("columns_present") or list(file_map.keys())
                    for canon in order:
                        real = file_map.get(canon)
                        if real:
                            headers_map[canon] = real
                            fields_order.append(canon)
                    first = False
                else:
                    # keep only fields present with identical headers
                    to_drop = [
                        canon
                        for canon, real in headers_map.items()
                        if file_map.get(canon) != real
                    ]
                    for canon in to_drop:
                        headers_map.pop(canon, None)
                        if canon in fields_order:
                            fields_order.remove(canon)

            canon_fields_ds = fields_order
            if not canon_fields_ds:
                logger.info("collect: skipped %s: no fields", ds_name)
                continue

            out_path = out_dir / f"{ds_name}.csv"
            logger.info(
                "collect: %s: files=%d, fields=[%s] -> out=%s",
                ds_name,
                len(files),
                ",".join(canon_fields_ds),
                str(out_path.relative_to(root)),
            )

            rows_in = 0
            rows_out = 0
            real_headers = [headers_map[c] for c in canon_fields_ds]

            with out_path.open("w", newline="", encoding="utf-8") as out_fh:
                writer = csv.writer(out_fh)
                writer.writerow(canon_fields_ds)

                for info in files:
                    file_path = root / info.get("path", "")
                    rel_path = info.get("path", "")
                    try:
                        with file_path.open("r", newline="", encoding="utf-8") as in_fh:
                            reader = csv.DictReader(in_fh)
                            if reader.fieldnames is None:
                                reader.fieldnames = []
                            missing = [
                                h for h in real_headers if h not in reader.fieldnames
                            ]
                            if missing:
                                logger.error(
                                    "collect: %s: missing column(s) %s in %s -> run 'validate'",
                                    ds_name,
                                    ",".join(missing),
                                    rel_path,
                                )
                                return 1

                            for row in reader:
                                rows_in += 1
                                writer.writerow([row.get(h, "") for h in real_headers])
                                rows_out += 1
                    except FileNotFoundError:
                        logger.error(
                            "collect: %s: file not found %s -> run 'validate'",
                            ds_name,
                            rel_path,
                        )
                        return 1

            logger.info(
                "collect: %s: rows_in=%d, rows_out=%d", ds_name, rows_in, rows_out
            )
            datasets_written += 1

        logger.info("collect: done (datasets_written=%d)", datasets_written)
        return DONE
    except Exception as exc:  # pragma: no cover - minimal error handling
        logger.error("collect: unexpected error: %s", exc)
        return 1

