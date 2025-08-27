"""Validate step performs basic content checks."""

from __future__ import annotations

from pathlib import Path
import ipaddress
import re

from app.pipeline.status import DONE
from app.utils.logging import get_logger
from app.collectors.files import (
    list_csv_in_dir,
    read_headers,
    open_csv_rows,
)


logger = get_logger(__name__)


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


def _build_normalizer(settings: dict):
    remove_bom = settings.get("remove_bom", False)
    trim = settings.get("trim", False)
    collapse = settings.get("collapse_spaces", False)
    casefold = settings.get("casefold", False)

    def normalize(value: str, *, is_first: bool = False) -> str:
        if is_first and remove_bom and value.startswith("\ufeff"):
            value = value.lstrip("\ufeff")
        if trim:
            value = value.strip()
        if collapse:
            value = " ".join(value.split())
        if casefold:
            value = value.casefold()
        return value

    return normalize


def _apply_rule(
    value: str,
    rule: dict,
    required: bool,
    canonical: str,
    rule_name: str,
) -> str | None:
    """Validate *value* against *rule* and return error code or None."""

    kind = rule.get("kind", "any")
    text = value.strip()

    if kind == "any":
        return None

    if kind == "ip":
        if not text:
            return f"empty_value:{canonical}" if required else None
        try:
            ip_obj = ipaddress.ip_address(text)
        except ValueError:
            return "invalid_ip"
        version = rule.get("version", "any")
        if version == "v4" and ip_obj.version != 4:
            return "invalid_ip"
        if version == "v6" and ip_obj.version != 6:
            return "invalid_ip"
        return None

    if kind == "mac":
        if not text:
            return f"empty_value:{canonical}" if required else None
        cleaned = text.replace(":", "").replace("-", "")
        if len(cleaned) != 12:
            return "invalid_mac"
        try:
            int(cleaned, 16)
        except ValueError:
            return "invalid_mac"
        return None

    if kind == "nonempty":
        if not text:
            return f"empty_value:{canonical}"
        return None

    if kind == "regex":
        if not text:
            return f"empty_value:{canonical}" if required else None
        pattern = rule.get("_compiled")
        if pattern and not pattern.fullmatch(text):
            return f"invalid_{rule_name}"
        return None

    return None


def run(**kwargs) -> int:
    """Run the validate step."""

    try:
        root = Path(__file__).resolve().parents[3]
        config_path = root / "configs" / "schemas.yml"
        if not config_path.exists():
            logger.error("validate: відсутній configs/schemas.yml")
            return 1

        text = config_path.read_text(encoding="utf-8")
        try:  # prefer PyYAML when available
            import yaml  # type: ignore

            config = yaml.safe_load(text) or {}
        except Exception:
            config = _simple_yaml_parse(text)
        validate_cfg = config.get("validate") or {}
        settings = validate_cfg.get("settings") or {}
        rules_cfg = validate_cfg.get("rules") or {}
        datasets_cfg = validate_cfg.get("datasets") or {}

        if not datasets_cfg:
            logger.error(
                "validate: відсутній блок validate.datasets у configs/schemas.yml"
            )
            return 1

        normalize = _build_normalizer(settings.get("normalize_headers", {}))
        ignore_suffixes = settings.get("ignore_suffixes", ["example.csv"])

        # prepare rules (compile regexes)
        rules: dict[str, dict] = {}
        for name, info in rules_cfg.items():
            info = info or {}
            if info.get("kind") == "regex":
                pattern = info.get("pattern")
                try:
                    info["_compiled"] = re.compile(pattern) if pattern else None
                except re.error:
                    info["_compiled"] = None
            rules[name] = info

        # --- inventory ---
        dataset_files: dict[str, list[str]] = {}
        for ds_name, ds in datasets_cfg.items():
            rel = ds.get("dir")
            if not isinstance(rel, str):
                continue
            dir_path = root / rel
            files = list_csv_in_dir(
                str(dir_path), ignore_suffixes=ignore_suffixes, recursive=False
            )
            dataset_files[ds_name] = files
            if not files:
                logger.info("validate: no csv in: %s", rel)
            else:
                names = [Path(p).name for p in files]
                logger.info(
                    "validate: files in %s: %s", rel, ", ".join(sorted(names))
                )

        missing_msgs: list[str] = []
        content_msgs: list[str] = []
        files_with_missing: set[str] = set()
        files_with_content: set[str] = set()

        for ds_name, ds in datasets_cfg.items():
            fields_cfg = ds.get("fields") or {}
            if not fields_cfg:
                logger.info("validate: skipped dataset %s: no schema", ds_name)
                continue

            files = dataset_files.get(ds_name, [])
            if not files:
                continue

            field_defs = {}
            alias_map: dict[str, str] = {}
            for canonical, info in fields_cfg.items():
                aliases_raw = info.get("headers") or []
                aliases_norm = [normalize(h) for h in aliases_raw]
                field_defs[canonical] = {
                    "aliases": aliases_norm,
                    "aliases_raw": aliases_raw,
                    "required": bool(info.get("required")),
                    "check": info.get("check", "any"),
                }
                for a in aliases_norm:
                    if a not in alias_map:
                        alias_map[a] = canonical

            for path in files:
                rel_path = str(Path(path).relative_to(root))
                headers = read_headers(path)
                norm_headers = [normalize(h, is_first=i == 0) for i, h in enumerate(headers)]
                found: dict[str, tuple[str, int]] = {}
                for idx, nh in enumerate(norm_headers):
                    canonical = alias_map.get(nh)
                    if canonical and canonical not in found:
                        found[canonical] = (headers[idx], idx)

                missing_fields: list[str] = []
                for canonical, info in field_defs.items():
                    if info["required"] and canonical not in found:
                        aliases = "|".join(info["aliases_raw"])
                        missing_fields.append(f"{canonical}[aliases={aliases}]")

                if missing_fields:
                    msg = (
                        f"validate: missing required in {rel_path}: "
                        f"{', '.join(missing_fields)}"
                    )
                    logger.error(msg)
                    missing_msgs.append(msg)
                    files_with_missing.add(rel_path)
                else:
                    logger.info("validate: headers ok: %s", rel_path)

                file_has_error = False
                if found:
                    for row_idx, row in enumerate(open_csv_rows(path), start=1):
                        for canonical, (real_header, col_idx) in found.items():
                            value = row[col_idx] if col_idx < len(row) else ""
                            info = field_defs[canonical]
                            rule_name = info["check"]
                            rule = rules.get(rule_name, {"kind": "any"})
                            err = _apply_rule(
                                value, rule, info["required"], canonical, rule_name
                            )
                            if err:
                                file_has_error = True
                                msg = (
                                    f"validate: content error: {rel_path} @row={row_idx} "
                                    f"field={canonical} code={err} value=\"{value}\""
                                )
                                logger.error(msg)
                                content_msgs.append(msg)
                                files_with_content.add(rel_path)

                if not file_has_error:
                    logger.info("validate: content ok: %s", rel_path)

        total_issues = len(missing_msgs) + len(content_msgs)
        logger.info(
            "validate: errors summary: files_with_missing=%d, files_with_content_errors=%d, total_issues=%d",
            len(files_with_missing),
            len(files_with_content),
            total_issues,
        )

        exit_code = DONE
        if (
            settings.get("stop_on_missing_required", True) and missing_msgs
        ) or (settings.get("stop_on_content_error", True) and content_msgs):
            exit_code = 1
        return exit_code

    except Exception as exc:  # pragma: no cover - minimal error handling
        logger.error("validate: unexpected error: %s", exc)
        return 1

