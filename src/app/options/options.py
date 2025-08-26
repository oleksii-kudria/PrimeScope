from __future__ import annotations

import argparse
from typing import Dict, List


# Global registry for option specifications
_OPTIONS: Dict[str, Dict[str, object]] = {}


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
        return 0
    print(f"Невідома опція для help: {name}")
    _print_general_help()
    return 2


class _RunArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:  # type: ignore[override]
        raise ValueError(message)


def _run_handler(args: List[str]) -> int:
    parser = _RunArgumentParser(prog="run")
    parser.add_argument("--from", dest="from_step")
    parser.add_argument("--to", dest="to_step")
    parser.add_argument("--skip")
    parser.add_argument("--clean-first", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--yes", action="store_true")
    try:
        ns = parser.parse_args(args)
    except ValueError:
        print("Некоректні аргументи для run")
        print(f"Використання: {_OPTIONS['run']['usage']}")
        return 2
    skip = ns.skip.split(",") if ns.skip else []
    from app.pipeline import flows, runner
    return runner.run_flow(
        flow=flows.DEFAULT_FLOW,
        from_step=ns.from_step,
        to_step=ns.to_step,
        skip=skip,
        clean_first=ns.clean_first,
        dry_run=ns.dry_run,
        yes=ns.yes,
    )


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
            "about": "Запустити повний цикл обробки: collect → normalize → dedupe → verify → report",
            "usage": "python scripts/processor.py run [--from STEP] [--to STEP] [--skip STEP[,STEP]] [--clean-first] [--dry-run] [--yes]",
            "handler": _run_handler,
        }
    return _OPTIONS
