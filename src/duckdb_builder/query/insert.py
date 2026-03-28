from typing import Any, Self

from duckdb_builder.composite_table import Table
from duckdb_builder.query.abstract_query import AbstractQuery


class insert(AbstractQuery):
    def __init__(
        self,
        table: Table,
        *,
        or_replace: bool = False,
        or_ignore: bool = False,
    ) -> None:
        super().__init__(table=table, columns=())
        self._values: dict[str, Any] = {}
        self._or_replace: bool = or_replace
        self._or_ignore: bool = or_ignore

    def values(self, **values: Any) -> Self:
        if not values:
            raise ValueError("No values provided for insert")
        self._values.update(values)
        return self

    def build_query(self) -> tuple[str, tuple[Any, ...]]:
        if not self._values:
            raise ValueError("No values provided for insert")
        table = self._get_table()
        with_sql, with_params = self._build_with_clause()

        columns = list(self._values.keys())
        col_names = ", ".join(f'"{col}"' for col in columns)
        placeholders = ", ".join("?" * len(columns))
        params = tuple(self._values[col] for col in columns)

        insert_stmnt = "INSERT"

        if self._or_replace and self._or_ignore:
            raise ValueError("Cannot use both or_replace and or_ignore")
        if self._or_replace:
            insert_stmnt += " OR REPLACE"
        elif self._or_ignore:
            insert_stmnt += " OR IGNORE"

        query = (
            f'{with_sql}{insert_stmnt} INTO "{table.get_table_name()}" '
            f"({col_names}) VALUES ({placeholders})"
        )

        return query, tuple(with_params) + params
