from typing import Any, Self

from sql_fusion.composite_table import (
    AbstractQuery,
    AliasRegistry,
    BinaryExpression,
    Column,
    FunctionCall,
    Table,
)


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
    ) -> tuple[str, tuple[Any, ...]]:
        if not self._values:
            raise ValueError("No values provided for update")

        registry = alias_registry or self._alias_registry
        table = self._get_table()
        with_sql, with_params = self._build_with_clause(registry)
        alias = registry.get_alias_for_table(table)
        assignments: list[str] = []
        params: list[Any] = []

        for column_name, value in self._values.items():
            column_ref = f'"{column_name}"'

            if isinstance(value, Column):
                assignments.append(
                    f"{column_ref} = {value.get_ref(registry)}",
                )
            elif isinstance(value, (FunctionCall, BinaryExpression)):
                value_sql, value_params = value.to_sql(registry)
                assignments.append(f"{column_ref} = {value_sql}")
                params.extend(value_params)
            elif isinstance(value, AbstractQuery):
                value_sql, value_params = value.build_query(registry)
                assignments.append(f"{column_ref} = ({value_sql})")
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
            f'"{table.get_name()}" AS "{alias.name}" {set_clause}',
        )

        if self._where_condition:
            where_sql, where_params = self._where_condition.to_sql(registry)
            query += f" {self._build_clause('WHERE', 'WHERE', where_sql)}"
            params.extend(where_params)

        return self._apply_compile_expressions(
            f"{with_sql} {query}" if with_sql else query,
            tuple(with_params + params),
        )
