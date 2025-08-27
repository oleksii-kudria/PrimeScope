from __future__ import annotations

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
        examples = spec.get("examples")
        if examples:
            print("\nПриклади")
            for desc, cmd in examples:
                print(f"  {cmd}  # {desc}")
        return 0
    print(f"Невідома опція для help: {name}")
    _print_general_help()
    return 2


def _run_handler(args: List[str]) -> int:
    """Handler for the run option."""
    logger.info("run: start")
    kwargs = {}
    for arg in args:
        if arg.startswith("--") and "=" in arg:
            key, value = arg[2:].split("=", 1)
            kwargs[key.replace("-", "_")] = value
    from app.pipeline import flows, runner
    code = runner.run_flow(flow=flows.DEFAULT_FLOW, **kwargs)
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
        _OPTIONS["run"] = {
            "about": "Запускає повний цикл: validate → collect → normalize → interim → checks → report",
            "usage": "python scripts/processor.py run [опції]",
            "flags": [
                (
                    "--from STEP",
                    "почати з кроку (validate|collect|normalize|interim|checks|report)",
                ),
                ("--to STEP", "закінчити на кроці"),
                ("--skip STEP[,STEP]", "пропустити вказані кроки"),
                ("--dry-run", "лише показати план без запису файлів"),
                (
                    "--clean-first",
                    "очистити data/interim перед запуском (окрім *.example.csv)",
                ),
                (
                    "--yes",
                    "автоматично підтверджувати потенційно руйнівні дії (для --clean-first)",
                ),
            ],
            "examples": [
                ("Повний цикл", "python3 scripts/processor.py run"),
                ("Сухий прогін (без змін на диску)", "python3 scripts/processor.py run --dry-run"),
                ("Очистити проміжні файли й запустити цикл", "python3 scripts/processor.py run --clean-first --yes"),
                (
                    "Запустити частину пайплайну (лише від normalize до report)",
                    "python3 scripts/processor.py run --from normalize --to report",
                ),
                ("Пропустити додаткові перевірки", "python3 scripts/processor.py run --skip checks"),
                ("Запустити тільки один крок (лише collect)", "python3 scripts/processor.py run --from collect --to collect"),
                ("Пропустити кілька кроків", "python3 scripts/processor.py run --skip normalize,checks"),
            ],
            "handler": _run_handler,
        }
    return _OPTIONS
