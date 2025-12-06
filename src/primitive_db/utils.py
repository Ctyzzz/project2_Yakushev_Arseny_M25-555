from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import prompt

from src.primitive_db.constants import DATA_DIR


def ensure_data_dir() -> None:
    """Ensure data/ directory exists."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_metadata(filepath: Path) -> dict[str, Any]:
    """Load metadata from JSON file. If not found, return empty dict."""
    try:
        return json.loads(filepath.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}


def save_metadata(filepath: Path, data: dict[str, Any]) -> None:
    """Save metadata to JSON file."""
    filepath.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def table_path(table_name: str) -> Path:
    """Return path to data file for table."""
    ensure_data_dir()
    return DATA_DIR / f"{table_name}.json"


def load_table_data(table_name: str) -> list[dict[str, Any]]:
    """Load table data list from data/<table>.json. If file missing, return []"""
    path = table_path(table_name)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []


def save_table_data(table_name: str, data: list[dict[str, Any]]) -> None:
    """Save table data list to data/<table>.json."""
    path = table_path(table_name)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def delete_table_file(table_name: str) -> None:
    """Remove table data file if it exists."""
    path = table_path(table_name)
    if path.exists():
        path.unlink()


def ask_string(text: str) -> str:
    """Read non-empty string using prompt (it повторяет ввод при пустой строке)."""
    return prompt.string(text)
