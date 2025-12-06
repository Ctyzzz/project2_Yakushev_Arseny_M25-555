from __future__ import annotations

import csv
import io
import re
import shlex
from dataclasses import dataclass
from typing import Any

from src.primitive_db.decorators import InvalidValue


@dataclass(frozen=True)
class Command:
    name: str
    table: str | None = None
    columns: list[str] | None = None
    values: list[Any] | None = None
    set_clause: dict[str, Any] | None = None
    where_clause: dict[str, Any] | None = None


_SQL_INSERT_RE = re.compile(
    r"^insert\s+into\s+(?P<table>\w+)\s+values\s*\((?P<inside>.*)\)\s*$",
    re.IGNORECASE,
)
_SQL_SELECT_RE = re.compile(
    r"^select\s+from\s+(?P<table>\w+)(?:\s+where\s+(?P<where>.+))?\s*$",
    re.IGNORECASE,
)
_SQL_DELETE_RE = re.compile(
    r"^delete\s+from\s+(?P<table>\w+)\s+where\s+(?P<where>.+)\s*$",
    re.IGNORECASE,
)
_SQL_UPDATE_RE = re.compile(
    r"^update\s+(?P<table>\w+)\s+set\s+(?P<set>.+?)\s+where\s+(?P<where>.+)\s*$",
    re.IGNORECASE,
)

_COND_RE = re.compile(r"^(?P<col>\w+)\s*=\s*(?P<val>.+)$")
_ASSIGN_RE = re.compile(r"^(?P<col>\w+)\s*=\s*(?P<val>.+)$")


def _parse_literal(raw: str) -> Any:
    raw = raw.strip()
    if len(raw) >= 2 and raw[0] == raw[-1] == '"':
        return raw[1:-1]
    low = raw.lower()
    if low in {"true", "false"}:
        return low == "true"
    if re.fullmatch(r"-?\d+", raw):
        return int(raw)
    return raw


def _parse_csv_values(inside: str) -> list[Any]:
    reader = csv.reader(io.StringIO(inside), skipinitialspace=True)
    row = next(reader, [])
    if row is None:
        return []
    return [_parse_literal(x) for x in row if x.strip() != ""]


def _parse_condition(text: str) -> dict[str, Any]:
    m = _COND_RE.match(text.strip())
    if not m:
        raise InvalidValue(text, "Ожидалось условие вида col = value")
    return {m.group("col"): _parse_literal(m.group("val"))}


def _parse_assignments(text: str) -> dict[str, Any]:
    parts = _parse_csv_values(text)
    out: dict[str, Any] = {}
    for part in parts:
        if not isinstance(part, str):
            raise InvalidValue(part, "Ожидалось присваивание col = value")
        m = _ASSIGN_RE.match(part.strip())
        if not m:
            raise InvalidValue(part, "Ожидалось присваивание col = value")
        out[m.group("col")] = _parse_literal(m.group("val"))
    return out


def parse_command(line: str) -> Command:
    line = line.strip()
    if not line:
        raise InvalidValue(line, "Пустая команда")

    m = _SQL_INSERT_RE.match(line)
    if m:
        return Command(
            name="insert",
            table=m.group("table"),
            values=_parse_csv_values(m.group("inside")),
        )

    m = _SQL_SELECT_RE.match(line)
    if m:
        where = m.group("where")
        return Command(
            name="select",
            table=m.group("table"),
            where_clause=_parse_condition(where) if where else None,
        )

    m = _SQL_DELETE_RE.match(line)
    if m:
        return Command(
            name="delete",
            table=m.group("table"),
            where_clause=_parse_condition(m.group("where")),
        )

    m = _SQL_UPDATE_RE.match(line)
    if m:
        return Command(
            name="update",
            table=m.group("table"),
            set_clause=_parse_assignments(m.group("set")),
            where_clause=_parse_condition(m.group("where")),
        )

    tokens = shlex.split(line)
    cmd = tokens[0].lower()

    if cmd in {"exit", "help", "list_tables"}:
        return Command(name=cmd)

    if cmd == "info":
        if len(tokens) != 2:
            raise InvalidValue(line, "Использование: info <таблица>")
        return Command(name="info", table=tokens[1])

    if cmd == "create_table":
        if len(tokens) < 3:
            raise InvalidValue(
                line,
                "Использование: create_table <name> <col:type> ...",
            )
        return Command(name="create_table", table=tokens[1], columns=tokens[2:])

    if cmd == "drop_table":
        if len(tokens) != 2:
            raise InvalidValue(line, "Использование: drop_table <name>")
        return Command(name="drop_table", table=tokens[1])

    if cmd == "insert":
        if len(tokens) < 3:
            raise InvalidValue(line, "Использование: insert <table> col=value ...")
        table = tokens[1]
        pairs = tokens[2:]
        kv: dict[str, Any] = {}
        for p in pairs:
            if "=" not in p:
                raise InvalidValue(p, "Ожидалось col=value")
            k, v = p.split("=", 1)
            kv[k] = _parse_literal(v)
        return Command(name="insert_kv", table=table, set_clause=kv)

    if cmd == "select":
        if len(tokens) < 2:
            raise InvalidValue(
                line,
                "Использование: select <table> [where col = value]",
            )
        table = tokens[1]
        if len(tokens) == 2:
            return Command(name="select", table=table)
        if len(tokens) >= 3 and tokens[2].lower() == "where":
            cond = " ".join(tokens[3:])
            return Command(
                name="select",
                table=table,
                where_clause=_parse_condition(cond),
            )
        raise InvalidValue(
            line,
            "Использование: select <table> [where col = value]",
        )

    if cmd == "update":
        if len(tokens) < 4 or tokens[2].lower() != "set":
            raise InvalidValue(
                line,
                "Использование: update <table> set col=value ... where col = value",
            )
        table = tokens[1]
        if "where" not in [t.lower() for t in tokens]:
            raise InvalidValue(line, "update требует where")
        idx = [t.lower() for t in tokens].index("where")
        set_pairs = tokens[3:idx]
        where_text = " ".join(tokens[idx + 1 :])
        set_clause: dict[str, Any] = {}
        for p in set_pairs:
            if "=" not in p:
                raise InvalidValue(p, "Ожидалось col=value")
            k, v = p.split("=", 1)
            set_clause[k] = _parse_literal(v)
        return Command(
            name="update",
            table=table,
            set_clause=set_clause,
            where_clause=_parse_condition(where_text),
        )

    if cmd == "delete":
        if len(tokens) < 4 or tokens[2].lower() != "where":
            raise InvalidValue(line, "Использование: delete <table> where col = value")
        table = tokens[1]
        cond = " ".join(tokens[3:])
        return Command(name="delete", table=table, where_clause=_parse_condition(cond))

    raise InvalidValue(tokens[0], "Неизвестная команда")
