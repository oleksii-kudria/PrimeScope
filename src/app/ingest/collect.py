"""Collect step reads validated CSV files and prepares stage tables."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Iterable

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


def _simple_yaml_parse(text: str) -> dict:
    """Very small YAML subset parser used when PyYAML is unavailable."""

    import ast

    root: dict = {}
    stack: list[tuple[int, dict]] = [(0, root)]

    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        level = indent // 2
        line = raw_line.strip()
        if ":" not in line:
            continue
        key, value_part = line.split(":", 1)
        key = key.strip()
        value_part = value_part.strip()
        if value_part and not (
            (value_part.startswith('"') and value_part.endswith('"'))
            or (value_part.startswith("'") and value_part.endswith("'"))
        ):
            if "#" in value_part:
                value_part = value_part.split("#", 1)[0].strip()

        while stack and stack[-1][0] >= level + 1:
            stack.pop()
        current = stack[-1][1]

        if not value_part:
            new_dict: dict = {}
            current[key] = new_dict
            stack.append((level + 1, new_dict))
            continue

        if value_part.startswith("{") or value_part.startswith("["):
            try:
                value = ast.literal_eval(value_part)
            except Exception:
                value = {}
        elif value_part in {"true", "false"}:
            value = value_part == "true"
        elif (value_part.startswith('"') and value_part.endswith('"')) or (
            value_part.startswith("'") and value_part.endswith("'")
        ):
            value = value_part[1:-1]
        else:
            try:
                value = ast.literal_eval(value_part)
            except Exception:
                value = value_part

        current[key] = value

    return root


def _read_config(root: Path) -> dict:
    cfg_path = root / "configs" / "schemas.yml"
    if not cfg_path.exists():
        return {}
    text = cfg_path.read_text(encoding="utf-8")
    try:  # prefer PyYAML when available
        import yaml  # type: ignore

        return yaml.safe_load(text) or {}
    except Exception:
        return _simple_yaml_parse(text)


def _normalize_mac(value: str, *, allowed: Iterable[str] | None = None) -> str:
    if not value:
        return ""
    v = value.strip()
    if allowed and v.casefold() in {a.casefold() for a in allowed}:
        return ""
    cleaned = v.replace(":", "").replace("-", "")
    cleaned = cleaned.encode("ascii", "ignore").decode("ascii")
    return cleaned.upper()


def _combo_label(mac: bool, randmac: bool, ip: bool) -> str:
    parts = []
    if mac:
        parts.append("mac")
    if randmac:
        parts.append("randmac")
    if ip:
        parts.append("ip")
    return "+".join(parts) if parts else "none"


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

        config = _read_config(root)
        collect_cfg = config.get("collect") or {}
        primary_candidates = collect_cfg.get("primary_candidates") or [
            "ubiq",
            "dhcp",
            "siem",
        ]
        outputs_dir = collect_cfg.get("outputs_dir") or "data/stage/collect"
        outputs_path = root / outputs_dir
        outputs_path.mkdir(parents=True, exist_ok=True)

        datasets_cfg = (config.get("validate") or {}).get("datasets") or {}
        ds_fields: dict[str, list[str]] = {}
        allow_literals: dict[str, dict[str, set[str]]] = {}
        for ds_name, ds_cfg in datasets_cfg.items():
            fields_cfg = ds_cfg.get("fields") or {}
            ds_fields[ds_name] = list(fields_cfg.keys())
            field_allow: dict[str, set[str]] = {}
            for canon, finfo in fields_cfg.items():
                rule = finfo.get("rule") or {}
                allowed = rule.get("allow_literals") or []
                if allowed:
                    field_allow[canon] = {str(a).casefold() for a in allowed}
            allow_literals[ds_name] = field_allow

        datasets = manifest.get("datasets", {})
        available = {name for name, ds in datasets.items() if ds.get("files")}
        primary = [ds for ds in primary_candidates if ds in available]
        if not primary:
            logger.error("collect: no primary datasets available")
            return 1
        secondary = sorted(available - set(primary))
        logger.info("collect: primary=%s; secondary=%s", primary, secondary)

        mac_index: dict[str, set[tuple[str, int]]] = {}
        ip_index: dict[str, set[tuple[str, int]]] = {}

        stage_paths: list[Path] = []

        for ds_name in sorted(available):
            stage_file = outputs_path / f"{ds_name}.csv"
            stage_paths.append(stage_file)
            ds_manifest = datasets[ds_name]
            files = ds_manifest.get("files", [])
            field_names = ds_fields.get(ds_name, [])
            header = [
                "row_id",
                "source",
                "file",
                *field_names,
                "mac_norm",
                "randmac_norm",
                "ip_norm",
            ]

            row_id = 1
            with stage_file.open("w", newline="", encoding="utf-8") as out_f:
                writer = csv.writer(out_f)
                writer.writerow(header)
                for f in files:
                    file_path = root / f["path"]
                    headers_map = f.get("headers_map", {})
                    with open(file_path, newline="", encoding="utf-8") as fh:
                        reader = csv.DictReader(fh)
                        for row in reader:
                            values = []
                            for canon in field_names:
                                real = headers_map.get(canon)
                                values.append(row.get(real, "") if real else "")
                            row_dict = dict(zip(field_names, values))
                            mac_norm = _normalize_mac(
                                row_dict.get("mac", ""),
                                allowed=allow_literals.get(ds_name, {}).get("mac"),
                            )
                            randmac_norm = _normalize_mac(
                                row_dict.get("randmac", ""),
                                allowed=allow_literals.get(ds_name, {}).get("randmac"),
                            )
                            ip_val = (row_dict.get("ip") or "").strip()
                            if ip_val.casefold() in allow_literals.get(ds_name, {}).get("ip", set()):
                                ip_norm = ""
                            else:
                                ip_norm = ip_val
                            writer.writerow(
                                [
                                    row_id,
                                    ds_name,
                                    f["path"],
                                    *values,
                                    mac_norm,
                                    randmac_norm,
                                    ip_norm,
                                ]
                            )
                            if ds_name in primary:
                                if mac_norm:
                                    mac_index.setdefault(mac_norm, set()).add(
                                        (ds_name, row_id)
                                    )
                                if ip_norm:
                                    ip_index.setdefault(ip_norm, set()).add(
                                        (ds_name, row_id)
                                    )
                            row_id += 1

        logger.info(
            "collect: primary indexes built (mac=%d, ip=%d) across %s",
            len(mac_index),
            len(ip_index),
            primary,
        )

        link_paths: list[Path] = []
        for ds_name in secondary:
            stage_file = outputs_path / f"{ds_name}.csv"
            links_file = outputs_path / f"links_{ds_name}.csv"
            link_paths.append(links_file)

            total = matched = unmatched = ambiguous_rows = 0
            with stage_file.open("r", newline="", encoding="utf-8") as in_f, links_file.open(
                "w", newline="", encoding="utf-8"
            ) as out_f:
                reader = csv.DictReader(in_f)
                header = [
                    f"{ds_name}_row_id",
                    "primary_dataset",
                    "primary_row_id",
                    "mac_match",
                    "randmac_match",
                    "ip_match",
                    "match_combo",
                    "is_best",
                    "ambiguous",
                ]
                writer = csv.writer(out_f)
                writer.writerow(header)

                for row in reader:
                    total += 1
                    sec_row_id = int(row["row_id"])
                    mac_val = row.get("mac_norm", "")
                    rand_val = row.get("randmac_norm", "")
                    ip_val = row.get("ip_norm", "")

                    C_mac = mac_index.get(mac_val, set()) if mac_val else set()
                    C_rand = mac_index.get(rand_val, set()) if rand_val else set()
                    C_ip = ip_index.get(ip_val, set()) if ip_val else set()
                    candidates = C_mac | C_rand | C_ip

                    cand_list = []
                    for cand in candidates:
                        ds_c, row_c = cand
                        mac_match = cand in C_mac
                        rand_match = cand in C_rand
                        ip_match = cand in C_ip
                        combo = _combo_label(mac_match, rand_match, ip_match)
                        score = (2 if mac_match else 0) + (1 if rand_match else 0) + (
                            1 if ip_match else 0
                        )
                        cand_list.append(
                            {
                                "primary_dataset": ds_c,
                                "primary_row_id": row_c,
                                "mac_match": mac_match,
                                "randmac_match": rand_match,
                                "ip_match": ip_match,
                                "match_combo": combo,
                                "score": score,
                            }
                        )

                    if not cand_list:
                        unmatched += 1
                        writer.writerow(
                            [sec_row_id, "", "", False, False, False, "none", True, False]
                        )
                        continue

                    matched += 1
                    best_score = max(c["score"] for c in cand_list)
                    best_candidates = [c for c in cand_list if c["score"] == best_score]
                    ambiguous_flag = len(best_candidates) > 1
                    if ambiguous_flag:
                        ambiguous_rows += 1
                    for c in cand_list:
                        is_best = c["score"] == best_score
                        writer.writerow(
                            [
                                sec_row_id,
                                c["primary_dataset"],
                                c["primary_row_id"],
                                c["mac_match"],
                                c["randmac_match"],
                                c["ip_match"],
                                c["match_combo"],
                                is_best,
                                ambiguous_flag if is_best else False,
                            ]
                        )

            logger.info(
                "collect: %s rows=%d → matched_rows=%d unmatched_rows=%d ambiguous_rows=%d",
                ds_name,
                total,
                matched,
                unmatched,
                ambiguous_rows,
            )

        if stage_paths:
            logger.info(
                "collect: saved stage to %s",
                ", ".join(str(p.relative_to(root)) for p in stage_paths),
            )
        if link_paths:
            logger.info(
                "collect: saved links to %s",
                ", ".join(str(p.relative_to(root)) for p in link_paths),
            )

    except Exception as exc:  # pragma: no cover - minimal error handling
        logger.error("collect: unexpected error: %s", exc)
        return 1
    return DONE

