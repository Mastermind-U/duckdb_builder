from typing import Any, Callable

OPERATORS: dict[str, type[AbstractOperator]] = {}


class AbstractOperator:
    sql_symbol: str = ""

    def __init__(self, col_ref: str) -> None:
        self._col_ref: str = col_ref

    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        raise NotImplementedError()

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        raise NotImplementedError()


def register_operator(
    symbol: str,
) -> Callable[[type[AbstractOperator]], type[AbstractOperator]]:
    def decorator(cls: type[AbstractOperator]) -> type[AbstractOperator]:
        OPERATORS[symbol] = cls
        cls.sql_symbol = symbol
        return cls

    return decorator


@register_operator("=")
class EqualOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} = ?", (value,)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} = {value_ref}", tuple()


@register_operator("!=")
class NotEqualOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} != ?", (value,)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} != {value_ref}", tuple()


@register_operator("<")
class LessThanOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} < ?", (value,)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} < {value_ref}", tuple()


@register_operator(">")
class GreaterThanOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} > ?", (value,)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} > {value_ref}", tuple()


@register_operator("<=")
class LessThanOrEqualOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} <= ?", (value,)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} <= {value_ref}", tuple()


@register_operator(">=")
class GreaterThanOrEqualOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} >= ?", (value,)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} >= {value_ref}", tuple()


@register_operator("LIKE")
class LikeOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} LIKE ?", (value,)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} LIKE {value_ref}", tuple()


@register_operator("ILIKE")
class IlikeOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} ILIKE ?", (value,)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} ILIKE {value_ref}", tuple()


@register_operator("IN")
class InOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        placeholders: str = ", ".join("?" * len(value))
        return f"{self._col_ref} IN ({placeholders})", tuple(value)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} IN ({value_ref})", tuple()


@register_operator("NOT IN")
class NotInOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        placeholders: str = ", ".join("?" * len(value))
        return f"{self._col_ref} NOT IN ({placeholders})", tuple(value)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} NOT IN ({value_ref})", tuple()
