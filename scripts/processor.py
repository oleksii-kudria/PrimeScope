from __future__ import annotations

import sys
from pathlib import Path
from typing import List

# Ensure src directory is on the Python path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from app.options.options import get_options


def main(argv: List[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    options = get_options()

    if not argv:
        return options["help"]["handler"]([])

    name, args = argv[0], argv[1:]
    if name in options:
        try:
            handler = options[name]["handler"]
            return handler(args)
        except Exception:
            return 1
    print(f"Невідома опція: {name}")
    options["help"]["handler"]([])
    return 2


if __name__ == "__main__":
    sys.exit(main())
