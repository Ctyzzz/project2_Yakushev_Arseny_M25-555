from __future__ import annotations

from pathlib import Path

BASE_DIR = Path.cwd()

DATA_DIR = BASE_DIR / "data"
META_FILE = BASE_DIR / "db_meta.json"

ID_COLUMN = "ID"

SUPPORTED_TYPES: dict[str, type] = {
    "int": int,
    "str": str,
    "bool": bool,
}

PROMPT_TEXT = ">>>Введите команду: "

YES_ANSWERS = {"y", "yes", "д", "да"}
