from __future__ import annotations

from typing import Callable, Dict, List


# Global registry for option specifications
_OPTIONS: Dict[str, Dict[str, object]] = {}


def _print_general_help() -> None:
    print("PrimeScope CLI")
    print("Доступні опції")
    for name, spec in _OPTIONS.items():
        print(f"  {name} - {spec['about']}")
    print("Приклади")
    print("  python script/processor.py")
    print("  python script/processor.py help")
    print("  python script/processor.py help help")


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


def get_options() -> Dict[str, Dict[str, object]]:
    """Return registry of CLI options."""
    if "help" not in _OPTIONS:
        _OPTIONS["help"] = {
            "about": "Показує список доступних опцій та приклади використання",
            "usage": "python script/processor.py help [<option>]",
            "handler": _help_handler,
        }
    return _OPTIONS
