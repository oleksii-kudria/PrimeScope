from __future__ import annotations

import argparse
from typing import Dict, List

from app.utils.logging import get_logger


# Global registry for option specifications
_OPTIONS: Dict[str, Dict[str, object]] = {}

logger = get_logger(__name__)


def _print_general_help() -> None:
    print("PrimeScope CLI")
    print("Доступні опції")
    for name, spec in _OPTIONS.items():
        print(f"  {name} - {spec['about']}")
    print("Приклади")
    print("  python scripts/processor.py")
    print("  python scripts/processor.py help")
    print("  python scripts/processor.py help help")


def _help_handler(args: List[str]) -> int:
    if not args:
        _print_general_help()
        return 0
    name = args[0]
    if name in _OPTIONS:
        spec = _OPTIONS[name]
        print(f"{name} - {spec['about']}")
        print(f"Використання: {spec['usage']}")
        flags = spec.get("flags")
        if flags:
            print("\nПрапорці")
            for flag, desc in flags:
                print(f"  {flag:<24} {desc}")
        notes = spec.get("notes")
        if notes:
            print("")
            for line in notes:
                print(line)
        examples = spec.get("examples")
        if examples:
            print("\nПриклади")
            for desc, cmd in examples:
                print(f"  {cmd}  # {desc}")
        return 0
    print(f"Невідома опція для help: {name}")
    _print_general_help()
    return 2


class _RunArgumentParser(argparse.ArgumentParser):
    """Argument parser that raises exceptions instead of exiting."""

    def error(self, message: str) -> None:  # type: ignore[override]
        raise ValueError(message)


def _run_handler(args: List[str]) -> int:
    """Handler for the run option."""
    parser = _RunArgumentParser(prog="run", add_help=False, allow_abbrev=False)
    parser.add_argument("--from", dest="from_step")
    parser.add_argument("--to", dest="to_step")
    parser.add_argument("--skip")
    parser.add_argument("--only")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true")
    parser.add_argument("--clean-first", dest="clean_first", action="store_true")
    parser.add_argument("--yes", action="store_true")

    try:
        ns = parser.parse_args(args)
    except ValueError as exc:
        msg = str(exc)
        if msg.startswith("unrecognized arguments: "):
            arg = msg.split(": ", 1)[1].split()[0]
            logger.error("run: unexpected argument '%s'", arg)
            logger.error("Hint: see 'python3 scripts/processor.py help run'")
        else:
            logger.error("run: %s", msg)
            logger.error("Hint: see 'python3 scripts/processor.py help run'")
        return 2

    from app.pipeline import flows, runner

    steps = flows.STEPS

    # Validate flags
    skip_steps = [s.strip() for s in ns.skip.split(",") if s.strip()] if ns.skip else []

    if ns.only and (ns.from_step or ns.to_step or ns.skip):
        logger.error("run: --only is mutually exclusive with --from/--to/--skip")
        return 2

    errors = []
    if ns.only and ns.only not in steps:
        errors.append(("only", ns.only))
    if ns.from_step and ns.from_step not in steps:
        errors.append(("from", ns.from_step))
    if ns.to_step and ns.to_step not in steps:
        errors.append(("to", ns.to_step))
    for s in skip_steps:
        if s not in steps:
            errors.append(("skip", s))

    if errors:
        for flag, value in errors:
            logger.error("run: unknown step '%s' for --%s", value, flag)
        logger.error("Allowed steps: %s", ", ".join(steps))
        return 2

    if ns.from_step and ns.to_step:
        if steps.index(ns.from_step) > steps.index(ns.to_step):
            order = "→".join(steps)
            logger.error(
                "run: invalid range --from %s --to %s (order: %s)",
                ns.from_step,
                ns.to_step,
                order,
            )
            return 2

    if ns.only:
        plan = [ns.only]
    else:
        start = steps.index(ns.from_step) if ns.from_step else 0
        end = steps.index(ns.to_step) + 1 if ns.to_step else len(steps)
        plan = [s for s in steps[start:end] if s not in skip_steps]

    logger.info("run: start")
    logger.info("run: plan = %s", ", ".join(plan))

    kwargs = {
        "dry_run": ns.dry_run,
        "clean_first": ns.clean_first,
        "yes": ns.yes,
    }

    code = runner.run_flow(flow=plan, **kwargs)
    logger.info("run: done")
    return code


def get_options() -> Dict[str, Dict[str, object]]:
    """Return registry of CLI options."""
    if "help" not in _OPTIONS:
        _OPTIONS["help"] = {
            "about": "Показує список доступних опцій та приклади використання",
            "usage": "python scripts/processor.py help [<option>]",
            "handler": _help_handler,
        }
    if "run" not in _OPTIONS:
        from app.pipeline import flows

        allowed_steps = ", ".join(flows.STEPS)
        _OPTIONS["run"] = {
            "about": "Запускає повний цикл: validate → collect → normalize → interim → checks → report",
            "usage": "python scripts/processor.py run [опції]",
            "flags": [
                ("--from STEP", "почати з кроку"),
                ("--to STEP", "закінчити на кроці"),
                ("--skip STEP[,STEP]", "пропустити вказані кроки"),
                ("--dry-run", "лише показати план без запису файлів"),
                ("--clean-first", "очистити data/interim перед запуском (окрім *.example.csv)"),
                (
                    "--yes",
                    "автоматично підтверджувати потенційно руйнівні дії (для --clean-first)",
                ),
            ],
            "notes": [
                f"Allowed steps: {allowed_steps}",
                "--only не можна комбінувати з --from/--to/--skip",
            ],
            "examples": [
                ("Повний цикл", "python3 scripts/processor.py run"),
                ("Сухий прогін (без змін на диску)", "python3 scripts/processor.py run --dry-run"),
                ("Очистити проміжні файли й запустити цикл", "python3 scripts/processor.py run --clean-first --yes"),
                ("Запустити частину пайплайну", "python3 scripts/processor.py run --from collect --to report"),
                ("Пропустити додаткові перевірки", "python3 scripts/processor.py run --skip checks"),
                ("Запустити тільки один крок (лише collect)", "python3 scripts/processor.py run --only collect"),
                ("Пропустити кілька кроків", "python3 scripts/processor.py run --skip normalize,checks"),
            ],
            "handler": _run_handler,
        }
    return _OPTIONS
