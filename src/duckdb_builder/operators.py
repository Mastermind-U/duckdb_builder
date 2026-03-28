from typing import Any, Callable

OPERATORS: dict[str, type[_AbstractOperator]] = {}


class _AbstractOperator:
    def __init__(self, col_ref: str) -> None:
        self._col_ref: str = col_ref

    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        raise NotImplementedError()


def register_operator(
    symbol: str,
) -> Callable[[type[_AbstractOperator]], type[_AbstractOperator]]:
    def decorator(cls: type[_AbstractOperator]) -> type[_AbstractOperator]:
        OPERATORS[symbol] = cls
        return cls

    return decorator


@register_operator("=")
class EqualOperator(_AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} = ?", (value,)


@register_operator("!=")
class NotEqualOperator(_AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} != ?", (value,)


@register_operator("<")
class LessThanOperator(_AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} < ?", (value,)


@register_operator(">")
class GreaterThanOperator(_AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} > ?", (value,)


@register_operator("<=")
class LessThanOrEqualOperator(_AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} <= ?", (value,)


@register_operator(">=")
class GreaterThanOrEqualOperator(_AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} >= ?", (value,)


@register_operator("LIKE")
class LikeOperator(_AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} LIKE ?", (value,)


@register_operator("ILIKE")
class IlikeOperator(_AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} ILIKE ?", (value,)


@register_operator("IN")
class InOperator(_AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        placeholders: str = ", ".join("?" * len(value))
        return f"{self._col_ref} IN ({placeholders})", tuple(value)
