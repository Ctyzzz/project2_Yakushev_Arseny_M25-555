from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.primitive_db.constants import ID_COLUMN, SUPPORTED_TYPES
from src.primitive_db.decorators import DBError, InvalidValue, confirm_action


@dataclass(frozen=True)
class Column:
    name: str
    type_name: str


def _normalize_meta(meta: dict[str, Any]) -> dict[str, Any]:
    meta.setdefault("tables", {})
    return meta


def _parse_column_spec(spec: str) -> Column:
    if ":" not in spec:
        raise InvalidValue(spec, "Ожидалось столбец:тип")
    name, type_name = spec.split(":", 1)
    if not name.isidentifier():
        raise InvalidValue(name, "Плохое имя столбца")
    if name == ID_COLUMN:
        raise InvalidValue(name, "Столбец ID задаётся автоматически")
    if type_name not in SUPPORTED_TYPES:
        raise InvalidValue(type_name, "Неподдерживаемый тип")
    return Column(name=name, type_name=type_name)


def _schema_columns(meta: dict[str, Any], table_name: str) -> list[Column]:
    meta = _normalize_meta(meta)
    if table_name not in meta["tables"]:
        raise KeyError(table_name)
    cols = meta["tables"][table_name]["columns"]
    return [Column(**c) for c in cols]


def _schema_map(cols: list[Column]) -> dict[str, str]:
    return {c.name: c.type_name for c in cols}


def _coerce_value(type_name: str, raw: Any) -> Any:
    if type_name not in SUPPORTED_TYPES:
        raise InvalidValue(type_name, "Неподдерживаемый тип")

    if type_name == "str":
        if not isinstance(raw, str):
            raw = str(raw)
        return raw

    if type_name == "int":
        if isinstance(raw, bool):
            raise InvalidValue(raw, "Ожидалось int")
        try:
            return int(raw)
        except Exception as exc:
            raise InvalidValue(raw, "Ожидалось int") from exc

    if type_name == "bool":
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, str):
            v = raw.strip().lower()
            if v in {"true", "1", "yes", "y", "да", "д"}:
                return True
            if v in {"false", "0", "no", "n", "нет", "н"}:
                return False
        raise InvalidValue(raw, "Ожидалось bool")

    raise InvalidValue(raw, "Неподдерживаемый тип")


def create_table(
    metadata: dict[str, Any],
    table_name: str,
    columns: list[str],
) -> dict[str, Any]:
    """Create table in metadata, auto add ID:int first."""
    metadata = _normalize_meta(metadata)

    if not table_name.isidentifier():
        raise InvalidValue(table_name, "Плохое имя таблицы")

    if table_name in metadata["tables"]:
        raise DBError(f'Таблица "{table_name}" уже существует.')

    user_cols = [_parse_column_spec(c) for c in columns]
    full_cols = [Column(ID_COLUMN, "int"), *user_cols]

    metadata["tables"][table_name] = {
        "columns": [{"name": c.name, "type_name": c.type_name} for c in full_cols]
    }
    return metadata


@confirm_action("удаление таблицы")
def drop_table(metadata: dict[str, Any], table_name: str) -> dict[str, Any]:
    """Drop table from metadata (dangerous)."""
    metadata = _normalize_meta(metadata)
    if table_name not in metadata["tables"]:
        raise DBError(f'Таблица "{table_name}" не существует.')
    metadata["tables"].pop(table_name)
    return metadata


def list_tables(metadata: dict[str, Any]) -> list[str]:
    """Return list of table names."""
    metadata = _normalize_meta(metadata)
    return sorted(metadata["tables"].keys())


def insert_record(
    metadata: dict[str, Any],
    table_name: str,
    values: list[Any],
    table_data: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    """Insert record, validate types/count, auto-generate ID."""
    cols = _schema_columns(metadata, table_name)
    schema = _schema_map(cols)
    user_cols = [c for c in cols if c.name != ID_COLUMN]

    if len(values) != len(user_cols):
        raise InvalidValue(values, "Неверное количество значений для insert")

    record: dict[str, Any] = {}
    for col, raw_val in zip(user_cols, values, strict=True):
        record[col.name] = _coerce_value(schema[col.name], raw_val)

    max_id = 0
    for row in table_data:
        try:
            max_id = max(max_id, int(row.get(ID_COLUMN, 0)))
        except Exception:
            continue

    new_id = max_id + 1
    record[ID_COLUMN] = new_id

    table_data.append(record)
    return table_data, new_id


def select_records(
    metadata: dict[str, Any],
    table_name: str,
    table_data: list[dict[str, Any]],
    where_clause: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Select rows by optional equals where_clause."""
    _ = _schema_columns(metadata, table_name)  # validate table exists
    if not where_clause:
        return list(table_data)

    cols = _schema_columns(metadata, table_name)
    schema = _schema_map(cols)

    if len(where_clause) != 1:
        raise InvalidValue(where_clause, "where поддерживает одно условие col = value")

    (col_name, raw_val), = where_clause.items()
    if col_name not in schema:
        raise KeyError(col_name)

    val = _coerce_value(schema[col_name], raw_val)
    return [r for r in table_data if r.get(col_name) == val]


def update_records(
    metadata: dict[str, Any],
    table_name: str,
    table_data: list[dict[str, Any]],
    set_clause: dict[str, Any],
    where_clause: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[int]]:
    """Update rows matching where_clause; return updated ids."""
    cols = _schema_columns(metadata, table_name)
    schema = _schema_map(cols)

    if not set_clause:
        raise InvalidValue(set_clause, "set не может быть пустым")
    if ID_COLUMN in set_clause:
        raise InvalidValue(ID_COLUMN, "Нельзя обновлять ID")

    if len(where_clause) != 1:
        raise InvalidValue(where_clause, "where поддерживает одно условие col = value")

    (w_col, w_raw), = where_clause.items()
    if w_col not in schema:
        raise KeyError(w_col)
    w_val = _coerce_value(schema[w_col], w_raw)

    coerced_set: dict[str, Any] = {}
    for k, v in set_clause.items():
        if k not in schema:
            raise KeyError(k)
        coerced_set[k] = _coerce_value(schema[k], v)

    updated_ids: list[int] = []
    for row in table_data:
        if row.get(w_col) == w_val:
            row.update(coerced_set)
            try:
                updated_ids.append(int(row.get(ID_COLUMN)))
            except Exception:
                pass

    return table_data, updated_ids


@confirm_action("удаление записи")
def delete_records(
    metadata: dict[str, Any],
    table_name: str,
    table_data: list[dict[str, Any]],
    where_clause: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[int]]:
    """Delete rows matching where_clause; return deleted ids."""
    cols = _schema_columns(metadata, table_name)
    schema = _schema_map(cols)

    if len(where_clause) != 1:
        raise InvalidValue(where_clause, "where поддерживает одно условие col = value")

    (w_col, w_raw), = where_clause.items()
    if w_col not in schema:
        raise KeyError(w_col)
    w_val = _coerce_value(schema[w_col], w_raw)

    kept: list[dict[str, Any]] = []
    deleted_ids: list[int] = []

    for row in table_data:
        if row.get(w_col) == w_val:
            try:
                deleted_ids.append(int(row.get(ID_COLUMN)))
            except Exception:
                pass
        else:
            kept.append(row)

    return kept, deleted_ids


def table_info(
    metadata: dict[str, Any],
    table_name: str,
    table_data: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return info dict about table."""
    cols = _schema_columns(metadata, table_name)
    cols_str = ", ".join([f"{c.name}:{c.type_name}" for c in cols])
    return {
        "table": table_name,
        "columns": cols_str,
        "count": len(table_data),
    }
