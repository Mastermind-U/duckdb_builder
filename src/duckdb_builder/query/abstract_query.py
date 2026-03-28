from copy import copy
from typing import Any, Self

from duckdb_builder.composite_table import (
    Column,
    Condition,
    FunctionCall,
    QueryLike,
    Table,
)


class AbstractQuery:
    def __init__(
        self,
        table: Table | None,
        columns: tuple[Column | FunctionCall, ...] = (),
        ) -> None:
        self._table: Table | None = table
        self._columns: tuple[Column | FunctionCall, ...] = columns
        self._where_condition: Condition | None = None
        self._ctes: list[tuple[str, QueryLike]] = []
        self._with_recursive: bool = False

    def _get_table(self) -> Table:
        if self._table is None:
            raise ValueError("FROM clause is required")
        return self._table

    def where(
        self,
        *conditions: Condition,
    ) -> Self:
        qs = copy(self)
        combined_condition: Condition | None = None

        for condition in conditions:
            if combined_condition is None:
                combined_condition = condition
            else:
                combined_condition = combined_condition & condition

        if combined_condition:
            if qs._where_condition is None:
                qs._where_condition = combined_condition
            else:
                qs._where_condition = qs._where_condition & combined_condition

        return qs

    def where_by(
        self,
        **kwargs: Any,
    ) -> Self:
        qs = copy(self)
        combined_condition: Condition | None = None
        table = self._get_table()

        for key, value in kwargs.items():
            col: Column = Column(
                key,
                table.get_alias(),
            )
            condition = Condition(
                column=col,
                operator="=",
                value=value,
            )
            if combined_condition is None:
                combined_condition = condition
            else:
                combined_condition = combined_condition & condition

        if combined_condition:
            if qs._where_condition is None:
                qs._where_condition = combined_condition
            else:
                qs._where_condition = qs._where_condition & combined_condition

        return qs

    def with_(self, *, recursive: bool = False, **ctes: QueryLike) -> Self:
        if not ctes:
            raise ValueError("No CTEs provided for with_")

        qs = copy(self)
        qs._ctes = self._ctes.copy()
        qs._with_recursive = self._with_recursive or recursive

        for name, query in ctes.items():
            if not hasattr(query, "build_query"):
                raise TypeError(f"CTE '{name}' must be query-like")
            qs._ctes.append((name, query))

        return qs

    def _build_with_clause(self) -> tuple[str, list[Any]]:
        if not self._ctes:
            return "", []

        with_parts: list[str] = []
        params: list[Any] = []

        for name, query in self._ctes:
            query_sql, query_params = query.build_query()
            with_parts.append(f'"{name}" AS ({query_sql})')
            params.extend(query_params)

        recursive_part = " RECURSIVE" if self._with_recursive else ""
        return f"WITH{recursive_part} {', '.join(with_parts)} ", params

    def build_query(self) -> tuple[str, tuple[Any, ...]]:
        raise NotImplementedError()

    def compile(self) -> tuple[str, tuple[Any, ...]]:
        return self.build_query()
