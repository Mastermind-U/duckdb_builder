from typing import Any

from sql_fusion.composite_table import AbstractQuery, AliasRegistry


class _set_operation(AbstractQuery):
    def __init__(
        self,
        query1: AbstractQuery,
        query2: AbstractQuery,
    ) -> None:
        super().__init__(table=None, columns=())
        self._query1 = query1
        self._query2 = query2

    def _operator_sql(self) -> str:
        raise NotImplementedError()

    def _render_query(
        self,
        query: AbstractQuery,
        alias_registry: AliasRegistry,
    ) -> tuple[str, tuple[Any, ...]]:
        return query.build_query(alias_registry)

    def build_query(
        self,
        alias_registry: AliasRegistry | None = None,
    ) -> tuple[str, tuple[Any, ...]]:
        registry = alias_registry or self._alias_registry
        params: list[Any] = []

        with_sql, with_params = self._build_with_clause(registry)
        params.extend(with_params)

        left_sql, left_params = self._render_query(self._query1, registry)
        right_sql, right_params = self._render_query(self._query2, registry)

        query_parts: list[str] = []
        if with_sql:
            query_parts.append(with_sql)
        query_parts.append(
            f"{left_sql} {self._operator_sql()} {right_sql}",
        )

        params.extend(left_params)
        params.extend(right_params)

        return self._apply_compile_expressions(
            " ".join(query_parts),
            tuple(params),
        )


class union(_set_operation):
    def __init__(
        self,
        query1: AbstractQuery,
        query2: AbstractQuery,
        all_: bool = False,
        by_name: bool = False,
    ) -> None:
        super().__init__(query1, query2)
        self._all = all_
        self._by_name = by_name

    def _operator_sql(self) -> str:
        operator = "UNION"
        if self._all:
            operator += " ALL"
        if self._by_name:
            operator += " BY NAME"
        return operator


class intersect(_set_operation):
    def __init__(
        self,
        query1: AbstractQuery,
        query2: AbstractQuery,
        all_: bool = False,
    ) -> None:
        super().__init__(query1, query2)
        self._all = all_

    def _operator_sql(self) -> str:
        operator = "INTERSECT"
        if self._all:
            operator += " ALL"
        return operator


class except_(_set_operation):
    def __init__(
        self,
        query1: AbstractQuery,
        query2: AbstractQuery,
        all_: bool = False,
    ) -> None:
        super().__init__(query1, query2)
        self._all = all_

    def _operator_sql(self) -> str:
        operator = "EXCEPT"
        if self._all:
            operator += " ALL"
        return operator
