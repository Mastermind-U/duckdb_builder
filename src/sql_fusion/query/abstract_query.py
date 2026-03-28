from collections.abc import Callable
from copy import copy
from typing import Any, Self

from sql_fusion.composite_table import (
    Alias,
    Column,
    Condition,
    FunctionCall,
    QueryLike,
    Table,
)
from sql_fusion.operators import EqualOperator

CompileExpression = Callable[
    [str, tuple[Any, ...]],
    tuple[str, tuple[Any, ...]],
]


class AbstractQuery:
    def __init__(
        self,
        table: Table | None,
        columns: tuple[Column | Alias | FunctionCall, ...] = (),
    ) -> None:
        self._table: Table | None = table
        self._columns: tuple[Column | Alias | FunctionCall, ...] = columns
        self._where_condition: Condition | None = None
        self._ctes: list[tuple[str, QueryLike]] = []
        self._with_recursive: bool = False
        self._compile_expressions: list[CompileExpression] = []
        self._before_clause_comments: dict[str, list[tuple[str, bool]]] = {}
        self._after_clause_comments: dict[str, list[tuple[str, bool]]] = {}

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

    def compile_expression(self, expression: CompileExpression) -> Self:
        qs = copy(self)
        qs._compile_expressions = self._compile_expressions.copy()
        qs._compile_expressions.append(expression)
        return qs

    def comment(self, text: str, *, hint: bool = False) -> Self:
        def _add_comment(
            sql: str,
            params: tuple[Any, ...],
        ) -> tuple[str, tuple[Any, ...]]:
            prefix = "+ " if hint else " "
            return f"/*{prefix}{text} */\n{sql}", params

        return self.compile_expression(_add_comment)

    def explain(
        self,
        *,
        analyze: bool = False,
        verbose: bool = False,
    ) -> Self:
        def _add_explain(
            sql: str,
            params: tuple[Any, ...],
        ) -> tuple[str, tuple[Any, ...]]:
            explain_parts = ["EXPLAIN"]
            if analyze:
                explain_parts.append("ANALYZE")
            if verbose:
                explain_parts.append("VERBOSE")
            explain_parts.append(sql)
            return " ".join(explain_parts), params

        return self.compile_expression(_add_explain)

    def analyze(self, *, verbose: bool = False) -> Self:
        return self.explain(analyze=True, verbose=verbose)

    def before_clause(
        self,
        clause: str,
        text: str,
        *,
        hint: bool = False,
    ) -> Self:
        qs = copy(self)
        qs._before_clause_comments = {
            key: value.copy()
            for key, value in self._before_clause_comments.items()
        }
        qs._after_clause_comments = {
            key: value.copy()
            for key, value in self._after_clause_comments.items()
        }
        clause_key = clause.upper()
        qs._before_clause_comments.setdefault(clause_key, []).append(
            (text, hint),
        )
        return qs

    def after_clause(
        self,
        clause: str,
        text: str,
        *,
        hint: bool = False,
    ) -> Self:
        qs = copy(self)
        qs._before_clause_comments = {
            key: value.copy()
            for key, value in self._before_clause_comments.items()
        }
        qs._after_clause_comments = {
            key: value.copy()
            for key, value in self._after_clause_comments.items()
        }
        clause_key = clause.upper()
        qs._after_clause_comments.setdefault(clause_key, []).append(
            (text, hint),
        )
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
                operator=EqualOperator,
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
        keyword = f"WITH{recursive_part}"
        return self._build_clause(
            "WITH",
            keyword,
            ", ".join(with_parts),
        ), params

    def _apply_compile_expressions(
        self,
        sql: str,
        params: tuple[Any, ...],
    ) -> tuple[str, tuple[Any, ...]]:
        for expression in self._compile_expressions:
            sql, params = expression(sql, params)

        return sql, params

    def _build_clause(
        self,
        clause: str,
        keyword: str,
        body: str = "",
    ) -> str:
        before_comments = self._render_clause_comments(
            self._before_clause_comments.get(clause.upper(), []),
            leading=False,
        )
        after_comments = self._render_clause_comments(
            self._after_clause_comments.get(clause.upper(), []),
            leading=True,
        )

        if body:
            separator = "" if after_comments else " "
            return (
                f"{before_comments}{keyword}{after_comments}{separator}{body}"
            )

        return f"{before_comments}{keyword}{after_comments}"

    @staticmethod
    def _render_clause_comments(
        comments: list[tuple[str, bool]],
        *,
        leading: bool,
    ) -> str:
        if not comments:
            return ""

        rendered = [
            f"/*+ {text} */" if hint else f"/* {text} */"
            for text, hint in comments
        ]
        if leading:
            return "".join(f" {comment}\n" for comment in rendered)
        return "".join(f"{comment}\n" for comment in rendered)

    def build_query(self) -> tuple[str, tuple[Any, ...]]:
        raise NotImplementedError()

    def compile(self) -> tuple[str, tuple[Any, ...]]:
        return self.build_query()
