from collections.abc import Iterator
from typing import Any, Self

from sql_fusion.composite_table import (
    AbstractQuery,
    AliasRegistry,
    BinaryExpression,
    Column,
    FunctionCall,
    Table,
)
from sql_fusion.params import get_qmark_params


class update(AbstractQuery):
    def __init__(self, table: Table) -> None:
        super().__init__(table=table, columns=())
        self._values: dict[str, Any] = {}

    def set(self, **values: Any) -> Self:
        if not values:
            raise ValueError("No values provided for update")
        self._values.update(values)
        return self

    def build_query(
        self,
        alias_registry: AliasRegistry | None = None,
        params: Iterator[str] | None = None,
    ) -> tuple[str, tuple[Any, ...]]:
        if not self._values:
            raise ValueError("No values provided for update")

        registry = alias_registry or self._alias_registry
        params = params or get_qmark_params()
        table = self._get_table()
        with_sql, with_params = self._build_with_clause(registry, params)
        alias = registry.get_alias_for_table(table)
        assignments: list[str] = []
        bound_params: list[Any] = []

        for column_name, value in self._values.items():
            column_ref = f'"{column_name}"'

            if isinstance(value, Column):
                assignments.append(
                    f"{column_ref} = {value.get_ref(registry)}",
                )
            elif isinstance(value, (FunctionCall, BinaryExpression)):
                value_sql, value_params = value.to_sql(registry, params)
                assignments.append(f"{column_ref} = {value_sql}")
                bound_params.extend(value_params)
            elif isinstance(value, AbstractQuery):
                value_sql, value_params = value.build_query(registry, params)
                assignments.append(f"{column_ref} = ({value_sql})")
                bound_params.extend(value_params)
            else:
                assignments.append(f"{column_ref} = {next(params)}")
                bound_params.append(value)

        set_clause = self._build_clause(
            "SET",
            "SET",
            ", ".join(assignments),
        )
        query = self._build_clause(
            "UPDATE",
            "UPDATE",
            f'"{table.get_name()}" AS "{alias.name}" {set_clause}',
        )

        if self._where_condition:
            where_sql, where_params = self._where_condition.to_sql(
                registry,
                params,
            )
            query += f" {self._build_clause('WHERE', 'WHERE', where_sql)}"
            bound_params.extend(where_params)

        return self._apply_compile_expressions(
            f"{with_sql} {query}" if with_sql else query,
            tuple(with_params + bound_params),
        )
