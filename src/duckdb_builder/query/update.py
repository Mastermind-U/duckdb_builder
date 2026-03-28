from typing import Any, Self

from duckdb_builder.composite_table import Column, FunctionCall, Table
from duckdb_builder.query.abstract_query import AbstractQuery


class update(AbstractQuery):
    def __init__(self, table: Table) -> None:
        super().__init__(table=table, columns=())
        self._values: dict[str, Any] = {}

    def set(self, **values: Any) -> Self:
        if not values:
            raise ValueError("No values provided for update")
        self._values.update(values)
        return self

    def build_query(self) -> tuple[str, tuple[Any, ...]]:
        if not self._values:
            raise ValueError("No values provided for update")

        table = self._get_table()
        with_sql, with_params = self._build_with_clause()
        table_alias = table.get_alias()
        assignments: list[str] = []
        params: list[Any] = []

        for column_name, value in self._values.items():
            column_ref = f'"{table_alias}"."{column_name}"'

            if isinstance(value, Column):
                assignments.append(f"{column_ref} = {value.get_ref()}")
            elif isinstance(value, FunctionCall):
                value_sql, value_params = value.to_sql()
                assignments.append(f"{column_ref} = {value_sql}")
                params.extend(value_params)
            else:
                assignments.append(f"{column_ref} = ?")
                params.append(value)

        set_clause = self._build_clause(
            "SET",
            "SET",
            ", ".join(assignments),
        )
        query = self._build_clause(
            "UPDATE",
            "UPDATE",
            f'"{table.get_table_name()}" AS "{table_alias}" {set_clause}',
        )

        if self._where_condition:
            where_sql, where_params = self._where_condition.to_sql()
            query += f" {self._build_clause('WHERE', 'WHERE', where_sql)}"
            params.extend(where_params)

        return self._apply_compile_expressions(
            f"{with_sql} {query}" if with_sql else query,
            tuple(with_params + params),
        )
