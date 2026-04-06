from typing import Any


class AbstractOperator:
    sql_symbol: str = ""

    def __init__(self, col_ref: str) -> None:
        self._col_ref: str = col_ref

    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        raise NotImplementedError()

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        raise NotImplementedError()


class EqualOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} = ?", (value,)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} = {value_ref}", tuple()


class NotEqualOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} != ?", (value,)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} != {value_ref}", tuple()


class LessThanOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} < ?", (value,)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} < {value_ref}", tuple()


class GreaterThanOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} > ?", (value,)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} > {value_ref}", tuple()


class LessThanOrEqualOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} <= ?", (value,)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} <= {value_ref}", tuple()


class GreaterThanOrEqualOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} >= ?", (value,)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} >= {value_ref}", tuple()


class LikeOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} LIKE ?", (value,)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} LIKE {value_ref}", tuple()


class IlikeOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} ILIKE ?", (value,)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} ILIKE {value_ref}", tuple()


class InOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        placeholders: str = ", ".join("?" * len(value))
        return f"{self._col_ref} IN ({placeholders})", tuple(value)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} IN ({value_ref})", tuple()


class NotInOperator(AbstractOperator):
    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        placeholders: str = ", ".join("?" * len(value))
        return f"{self._col_ref} NOT IN ({placeholders})", tuple(value)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} NOT IN ({value_ref})", tuple()


class TextOperator(AbstractOperator):
    def __init__(self, col_ref: str, sql_symbol: str) -> None:
        super().__init__(col_ref)
        self.sql_symbol: str = sql_symbol

    def to_sql(self, value: Any) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} {self.sql_symbol} ?", (value,)

    def to_sql_ref(self, value_ref: str) -> tuple[str, tuple[Any, ...]]:
        return f"{self._col_ref} {self.sql_symbol} {value_ref}", tuple()
