from __future__ import annotations

import time
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

from src.primitive_db.constants import YES_ANSWERS
from src.primitive_db.utils import ask_string

T = TypeVar("T")


class DBError(Exception):
    """Base DB error."""


class InvalidValue(ValueError):
    """Value is invalid by project rules."""

    def __init__(self, value: Any, message: str = "Некорректное значение") -> None:
        super().__init__(message)
        self.value = value


def handle_db_errors(func: Callable[..., T]) -> Callable[..., T | None]:
    """Centralized error handling for DB operations."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T | None:
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            print(
                "Ошибка: Файл данных не найден. "
                "Возможно, база данных не инициализирована."
            )
            return None
        except KeyError as exc:
            print(f"Ошибка: Таблица или столбец {exc} не найден.")
            return None
        except InvalidValue as exc:
            print(f"Некорректное значение: {exc.value}. Попробуйте снова.")
            return None
        except ValueError as exc:
            print(f"Ошибка валидации: {exc}")
            return None
        except DBError as exc:
            print(f"Ошибка: {exc}")
            return None
        except Exception as exc:
            print(f"Произошла непредвиденная ошибка: {exc}")
            return None

    return wrapper


def confirm_action(
    action_name: str,
) -> Callable[[Callable[..., T]], Callable[..., T | None]]:
    """Ask confirmation before dangerous operations."""

    def decorator(func: Callable[..., T]) -> Callable[..., T | None]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T | None:
            answer = ask_string(
                f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: '
            ).strip().lower()
            if answer not in YES_ANSWERS:
                print("Операция отменена.")
                return None
            return func(*args, **kwargs)

        return wrapper

    return decorator


def log_time(func: Callable[..., T]) -> Callable[..., T]:
    """Measure execution time with time.monotonic and print in required format."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        start = time.monotonic()
        result = func(*args, **kwargs)
        elapsed = time.monotonic() - start
        print(f"Функция {func.__name__} выполнилась за {elapsed:.3f} секунд.")
        return result

    return wrapper


def create_cacher() -> Callable[[Any, Callable[[], T]], T]:
    """
    Return cache_result(key, value_func) with dict stored in closure.
    If key in cache -> return cached value, else compute, store, return.
    """
    cache: dict[Any, Any] = {}

    def cache_result(key: Any, value_func: Callable[[], T]) -> T:
        if key in cache:
            return cache[key]
        value = value_func()
        cache[key] = value
        return value

    return cache_result
