from __future__ import annotations

import os
from typing import Any

from prettytable import PrettyTable

from src.primitive_db import core
from src.primitive_db.constants import ID_COLUMN, META_FILE, PROMPT_TEXT
from src.primitive_db.decorators import create_cacher, handle_db_errors, log_time
from src.primitive_db.parser import Command, parse_command
from src.primitive_db.utils import (
    ask_string,
    delete_table_file,
    load_metadata,
    load_table_data,
    save_metadata,
    save_table_data,
    table_path,
)


def print_help() -> None:
    """Print help message (как в методичке, но объединено)."""
    print("\n***Операции с данными***\n")
    print("Функции:")
    print('<command> create_table <имя> <столбец:тип> ... - создать таблицу')
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя> - удалить таблицу")
    print("<command> info <имя> - вывести информацию о таблице\n")

    print("<command> insert into <таблица> values (v1, v2, ...) - создать запись")
    print("<command> select from <таблица> [where col = value] - прочитать записи")
    print(
        "<command> update <таблица> set col = value where col = value - обновить запись"
    )
    print("<command> delete from <таблица> where col = value - удалить запись\n")

    print("Общие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")


def _ensure_meta_shape(meta: dict[str, Any]) -> dict[str, Any]:
    meta.setdefault("tables", {})
    return meta


def _columns_for_table(meta: dict[str, Any], table_name: str) -> list[str]:
    cols = meta["tables"][table_name]["columns"]
    return [c["name"] for c in cols]


def _user_columns_for_table(meta: dict[str, Any], table_name: str) -> list[str]:
    cols = _columns_for_table(meta, table_name)
    return [c for c in cols if c != ID_COLUMN]


def _make_table_view(columns: list[str], rows: list[dict[str, Any]]) -> PrettyTable:
    pt = PrettyTable()
    pt.field_names = columns
    for r in rows:
        pt.add_row([r.get(c) for c in columns])
    return pt


@handle_db_errors
def _handle_create_table(cmd: Command) -> None:
    meta = _ensure_meta_shape(load_metadata(META_FILE))
    updated = core.create_table(meta, cmd.table or "", cmd.columns or [])
    save_metadata(META_FILE, updated)
    save_table_data(cmd.table or "", [])
    cols = updated["tables"][cmd.table]["columns"]
    cols_str = ", ".join([f'{c["name"]}:{c["type_name"]}' for c in cols])
    print(f'Таблица "{cmd.table}" успешно создана со столбцами: {cols_str}')


@handle_db_errors
def _handle_list_tables() -> None:
    meta = _ensure_meta_shape(load_metadata(META_FILE))
    tables = core.list_tables(meta)
    if not tables:
        print("Таблиц нет.")
        return
    for t in tables:
        print(f"- {t}")


@handle_db_errors
def _handle_drop_table(cmd: Command) -> None:
    meta = _ensure_meta_shape(load_metadata(META_FILE))
    updated = core.drop_table(meta, cmd.table or "")
    if updated is None:
        return
    save_metadata(META_FILE, updated)
    delete_table_file(cmd.table or "")
    print(f'Таблица "{cmd.table}" успешно удалена.')


@log_time
@handle_db_errors
def _handle_insert(cmd: Command) -> None:
    meta = _ensure_meta_shape(load_metadata(META_FILE))
    table = cmd.table or ""
    data = load_table_data(table)

    if cmd.name == "insert_kv":
        user_cols = _user_columns_for_table(meta, table)
        kv = cmd.set_clause or {}
        values = []
        for col in user_cols:
            if col not in kv:
                raise core.InvalidValue(col, "Все поля обязательны")
            values.append(kv[col])
    else:
        values = cmd.values or []

    data, new_id = core.insert_record(meta, table, values, data)
    save_table_data(table, data)
    print(f'Запись с ID={new_id} успешно добавлена в таблицу "{table}".')


@log_time
@handle_db_errors
def _handle_update(cmd: Command) -> None:
    meta = _ensure_meta_shape(load_metadata(META_FILE))
    table = cmd.table or ""
    data = load_table_data(table)

    data, updated_ids = core.update_records(
        meta,
        table,
        data,
        cmd.set_clause or {},
        cmd.where_clause or {},
    )
    save_table_data(table, data)

    if len(updated_ids) == 1:
        print(f'Запись с ID={updated_ids[0]} в таблице "{table}" успешно обновлена.')
    else:
        print(f"Обновлено записей: {len(updated_ids)}.")


@log_time
@handle_db_errors
def _handle_delete(cmd: Command) -> None:
    meta = _ensure_meta_shape(load_metadata(META_FILE))
    table = cmd.table or ""
    data = load_table_data(table)

    result = core.delete_records(meta, table, data, cmd.where_clause or {})
    if result is None:
        return
    data, deleted_ids = result
    save_table_data(table, data)

    if len(deleted_ids) == 1:
        print(f'Запись с ID={deleted_ids[0]} успешно удалена из таблицы "{table}".')
    else:
        print(f"Удалено записей: {len(deleted_ids)}.")


@handle_db_errors
def _handle_info(cmd: Command) -> None:
    meta = _ensure_meta_shape(load_metadata(META_FILE))
    table = cmd.table or ""
    data = load_table_data(table)
    info = core.table_info(meta, table, data)
    print(f"Таблица: {info['table']}")
    print(f"Столбцы: {info['columns']}")
    print(f"Количество записей: {info['count']}")


def run() -> None:
    """Main loop."""
    print_help()
    cacher = create_cacher()

    while True:
        line = ask_string(PROMPT_TEXT).strip()

        try:
            cmd = parse_command(line)
        except Exception:
            bad = line.split(maxsplit=1)[0] if line.strip() else ""
            if bad:
                print(f"Функции {bad} нет. Попробуйте снова.")
            continue

        if cmd.name == "help":
            print_help()
            continue

        if cmd.name == "exit":
            return

        if cmd.name == "create_table":
            _handle_create_table(cmd)
            continue

        if cmd.name == "list_tables":
            _handle_list_tables()
            continue

        if cmd.name == "drop_table":
            _handle_drop_table(cmd)
            continue

        if cmd.name == "info":
            _handle_info(cmd)
            continue

        if cmd.name in {"insert", "insert_kv"}:
            _handle_insert(cmd)
            continue

        if cmd.name == "update":
            _handle_update(cmd)
            continue

        if cmd.name == "delete":
            _handle_delete(cmd)
            continue

        if cmd.name == "select":
            table = cmd.table or ""
            path = table_path(table)
            mtime = os.path.getmtime(path) if path.exists() else 0.0
            key = (table, str(cmd.where_clause), mtime)

            def compute() -> list[dict[str, Any]]:
                meta = _ensure_meta_shape(load_metadata(META_FILE))
                data = load_table_data(table)
                return core.select_records(meta, table, data, cmd.where_clause)

            rows = cacher(key, compute)
            meta = _ensure_meta_shape(load_metadata(META_FILE))
            columns = _columns_for_table(meta, table)
            if rows:
                print(_make_table_view(columns, rows))
            else:
                print("Записей нет.")
            continue

        print(f"Функции {cmd.name} нет. Попробуйте снова.")
