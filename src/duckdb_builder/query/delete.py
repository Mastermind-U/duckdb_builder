from copy import copy
from typing import Any, Self

from duckdb_builder.composite_table import Column, FunctionCall, Table
from duckdb_builder.query.abstract_query import AbstractQuery


class delete(AbstractQuery):
    def __init__(self, table: Table | None = None) -> None:
        super().__init__(table=table, columns=())
        self._returning_columns: tuple[Column | FunctionCall, ...] = ()
        self._returning_all: bool = False

    def returning(self, *columns: Column | FunctionCall) -> Self:
        if not columns:
            self._returning_all = True
            self._returning_columns = ()
            return self

        if not self._returning_all:
            self._returning_columns += columns

        return self

    def build_query(self) -> tuple[str, tuple[Any, ...]]:
        table = self._get_table()
        with_sql, with_params = self._build_with_clause()
        query = (
            f'{with_sql}DELETE FROM "{table.get_table_name()}" '  # noqa: S608
            f'AS "{table.get_alias()}"'
        )
        params: list[Any] = list(with_params)

        if self._where_condition:
            where_sql, where_params = self._where_condition.to_sql()
            query += f" WHERE {where_sql}"
            params.extend(where_params)

        if self._returning_all:
            query += " RETURNING *"
            return query, tuple(params)

        if self._returning_columns:
            returning_parts: list[str] = []

            for col in self._returning_columns:
                if isinstance(col, FunctionCall):
                    func_sql, func_params = col.to_sql()
                    returning_parts.append(func_sql)
                    params.extend(func_params)
                else:
                    returning_parts.append(f'"{col.table_alias}"."{col.name}"')

            query += f" RETURNING {', '.join(returning_parts)}"

        return query, tuple(params)

    def from_(self, table: Table | AbstractQuery) -> Self:
        qs = copy(self)
        qs._table = table if isinstance(table, Table) else Table(table)
        return qs
