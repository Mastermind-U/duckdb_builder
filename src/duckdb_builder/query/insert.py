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

        into_clause = self._build_clause(
            "INTO",
            "INTO",
            f'"{table.get_table_name()}" ({col_names})',
        )
        values_clause = self._build_clause(
            "VALUES",
            "VALUES",
            f"({placeholders})",
        )
        query = self._build_clause(
            "INSERT",
            insert_stmnt,
            f"{into_clause} {values_clause}",
        )

        query_parts = [query]
        if with_sql:
            query_parts.insert(0, with_sql)

        return self._apply_compile_expressions(
            " ".join(query_parts),
            tuple(with_params) + params,
        )
