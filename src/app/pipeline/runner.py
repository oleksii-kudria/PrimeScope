from __future__ import annotations

from pathlib import Path
import importlib
import shutil
from typing import List, Optional


ROOT = Path(__file__).resolve().parents[2]
INTERIM_DIR = ROOT / "data" / "interim"


def _select_steps(flow: List[str], from_step: Optional[str], to_step: Optional[str], skip: List[str]) -> Optional[List[str]]:
    try:
        start = flow.index(from_step) if from_step else 0
    except ValueError:
        print(f"Невідомий крок для --from: {from_step}")
        return None
    try:
        end = flow.index(to_step) + 1 if to_step else len(flow)
    except ValueError:
        print(f"Невідомий крок для --to: {to_step}")
        return None
    steps = flow[start:end]
    return [s for s in steps if s not in skip]


def _load_step_module(step: str):
    for mod_name in (f"app.processors.{step}", f"app.collectors.{step}"):
        try:
            return importlib.import_module(mod_name)
        except ModuleNotFoundError:
            continue
    return None


def run_flow(*, flow: List[str], from_step: Optional[str], to_step: Optional[str], skip: List[str], clean_first: bool, dry_run: bool, yes: bool) -> int:
    steps = _select_steps(flow, from_step, to_step, skip)
    if steps is None:
        return 2
    if clean_first:
        if not yes:
            print("--clean-first потребує підтвердження --yes")
            return 2
        if dry_run:
            print("[dry-run] Очистити дані в data/interim")
        else:
            if INTERIM_DIR.exists():
                for item in INTERIM_DIR.iterdir():
                    if item.is_file():
                        item.unlink()
                    else:
                        shutil.rmtree(item)
            print("Очищено data/interim")
    if dry_run:
        print("План виконання:", ", ".join(steps))
        return 0
    for step in steps:
        module = _load_step_module(step)
        if module and hasattr(module, "main"):
            try:
                code = module.main()
            except Exception as exc:
                print(f"Помилка кроку {step}: {exc}")
                return 1
            if code != 0:
                print(f"Крок {step} завершився з кодом {code}")
                return 1
            continue
        print(f"Немає реалізації для кроку {step}")
    return 0
