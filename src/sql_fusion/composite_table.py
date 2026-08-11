from __future__ import annotations

from collections.abc import Iterable
from copy import copy
from typing import Any, Callable, ClassVar, Self, TypeAlias, cast

from sql_fusion.operators import (
    AbstractOperator,
    EqualOperator,
    GreaterThanOperator,
    GreaterThanOrEqualOperator,
    IlikeOperator,
    InOperator,
    LessThanOperator,
    LessThanOrEqualOperator,
    LikeOperator,
    NotEqualOperator,
    NotInOperator,
    TextOperator,
)

CompileExpression = Callable[
    [str, tuple[Any, ...]],
    tuple[str, tuple[Any, ...]],
]
OperatorFactory = Callable[[str], AbstractOperator]
WindowFrameBoundary: TypeAlias = "str | FrameBoundary"  # noqa: UP040
WindowFrameSpec: TypeAlias = (  # noqa: UP040
    "WindowFrameBoundary | tuple[WindowFrameBoundary, WindowFrameBoundary]"
)


class AliasRegistry:
    """Registry for managing unique table aliases."""

    def __init__(self) -> None:
        self._counter: int = 0
        self._mapping: dict[Table, Alias] = {}

    def get_next_alias(self) -> str:
        """Generate the next unique alias (a, b, c, ..., z, aa, ab, ...)."""
        alias = ""
        n = self._counter
        while True:
            alias = chr(ord("a") + (n % 26)) + alias
            n //= 26
            if n == 0:
                break
        self._counter += 1
        return alias

    def get_alias_for_table(self, table: Table) -> Alias:
        if table not in self._mapping:
            self._mapping[table] = Alias(self.get_next_alias())
        return self._mapping[table]

    def reset(self) -> None:
        self._counter = 0
        self._mapping.clear()


class AbstractQuery:
    def __init__(
        self,
        table: Table | None,
        columns: tuple[
            Column
            | Alias
            | FunctionCall
            | FilteredFunctionCall
            | WindowFunctionCall,
            ...,
        ] = (),
    ) -> None:
        self._table: Table | None = table
        self._columns: tuple[
            Column
            | Alias
            | FunctionCall
            | FilteredFunctionCall
            | WindowFunctionCall,
            ...,
        ] = columns
        self._where_condition: Condition | None = None
        self._ctes: list[tuple[str, AbstractQuery]] = []
        self._with_recursive: bool = False
        self._compile_expressions: list[CompileExpression] = []
        self._before_clause_comments: dict[str, list[tuple[str, bool]]] = {}
        self._after_clause_comments: dict[str, list[tuple[str, bool]]] = {}
        self._alias_registry: AliasRegistry = AliasRegistry()

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
        self._alias_registry.get_alias_for_table(table)

        for key, value in kwargs.items():
            col: Column = Column(key)
            col._attach_table(table)  # pyright: ignore[reportPrivateUsage]
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

    def with_(self, *, recursive: bool = False, **ctes: AbstractQuery) -> Self:
        if not ctes:
            raise ValueError("No CTEs provided for with_")

        qs = copy(self)
        qs._ctes = self._ctes.copy()
        qs._with_recursive = self._with_recursive or recursive
        qs._ctes.extend(ctes.items())

        return qs

    def _build_with_clause(
        self,
        alias_registry: AliasRegistry | None = None,
    ) -> tuple[str, list[Any]]:
        if not self._ctes:
            return "", []

        registry = alias_registry or self._alias_registry
        with_parts: list[str] = []
        params: list[Any] = []

        for name, query in self._ctes:
            query_sql, query_params = query.build_query(registry)
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

    def build_query(
        self,
        alias_registry: AliasRegistry | None = None,
    ) -> tuple[str, tuple[Any, ...]]:
        raise NotImplementedError()

    def compile(self) -> tuple[str, tuple[Any, ...]]:
        return self.build_query()


class ComparableExpression:
    def _cond(
        self,
        operator: type[AbstractOperator] | OperatorFactory,
        other: object,
    ) -> Condition:
        return Condition(column=self, operator=operator, value=other)

    def __eq__(self, other: object) -> Condition:  # type: ignore[override]
        return self._cond(EqualOperator, other)

    def __ne__(self, other: object) -> Condition:  # type: ignore[override]
        return self._cond(NotEqualOperator, other)

    def __lt__(self, other: Any) -> Condition:
        return self._cond(LessThanOperator, other)

    def __gt__(self, other: Any) -> Condition:
        return self._cond(GreaterThanOperator, other)

    def __le__(self, other: Any) -> Condition:
        return self._cond(LessThanOrEqualOperator, other)

    def __ge__(self, other: Any) -> Condition:
        return self._cond(GreaterThanOrEqualOperator, other)

    def __hash__(self) -> int:
        raise TypeError(f"unhashable type: '{type(self).__name__}'")

    def get_ref(self, alias_registry: AliasRegistry) -> str:
        raise NotImplementedError()

    def _binary_expression(
        self,
        operator: str,
        other: Any,
        *,
        reverse: bool = False,
    ) -> BinaryExpression:
        if reverse:
            return BinaryExpression(other, operator, self)
        return BinaryExpression(self, operator, other)

    def __add__(self, other: Any) -> BinaryExpression:
        return self._binary_expression("+", other)

    def __radd__(self, other: Any) -> BinaryExpression:
        return self._binary_expression("+", other, reverse=True)

    def __sub__(self, other: Any) -> BinaryExpression:
        return self._binary_expression("-", other)

    def __rsub__(self, other: Any) -> BinaryExpression:
        return self._binary_expression("-", other, reverse=True)

    def __mul__(self, other: Any) -> BinaryExpression:
        return self._binary_expression("*", other)

    def __rmul__(self, other: Any) -> BinaryExpression:
        return self._binary_expression("*", other, reverse=True)

    def __truediv__(self, other: Any) -> BinaryExpression:
        return self._binary_expression("/", other)

    def __rtruediv__(self, other: Any) -> BinaryExpression:
        return self._binary_expression("/", other, reverse=True)


class BinaryExpression(ComparableExpression):
    def __init__(self, left: Any, operator: str, right: Any) -> None:
        self.left: Any = left
        self.operator: str = operator
        self.right: Any = right

    @staticmethod
    def _render_operand(
        operand: Any,
        alias_registry: AliasRegistry,
    ) -> tuple[str, tuple[Any, ...]]:
        if isinstance(operand, BinaryExpression):
            sql, params = operand.to_sql(alias_registry)
            return f"({sql})", params
        if isinstance(
            operand,
            (FunctionCall, FilteredFunctionCall, WindowFunctionCall),
        ):
            return operand.to_sql(alias_registry)
        if isinstance(operand, Column | Alias):
            return operand.get_ref(alias_registry), tuple()
        if isinstance(operand, ComparableExpression):
            return operand.get_ref(alias_registry), tuple()
        return "?", (operand,)

    def to_sql(
        self,
        alias_registry: AliasRegistry,
    ) -> tuple[str, tuple[Any, ...]]:
        left_sql, left_params = self._render_operand(self.left, alias_registry)
        right_sql, right_params = self._render_operand(
            self.right,
            alias_registry,
        )
        return (
            f"{left_sql} {self.operator} {right_sql}",
            left_params + right_params,
        )

    def get_ref(self, alias_registry: AliasRegistry) -> str:
        return self.to_sql(alias_registry)[0]


class Condition:
    def __init__(  # noqa: PLR0913
        self,
        column: ComparableExpression | FunctionCall | None = None,
        operator: (
            type[AbstractOperator] | AbstractOperator | OperatorFactory | None
        ) = None,
        value: object | None = None,
        is_and: bool = True,
        left: Condition | None = None,
        right: Condition | None = None,
        negated: bool = False,
    ) -> None:
        self.column: ComparableExpression | FunctionCall | None = column
        self.operator: (
            type[AbstractOperator] | AbstractOperator | OperatorFactory | None
        ) = operator
        self.value: object | None = value
        self.is_and: bool = is_and
        self.left: Condition | None = left
        self.right: Condition | None = right
        self.negated: bool = negated

    @staticmethod
    def _render_expression(
        value: ComparableExpression | FunctionCall | WindowFunctionCall,
        alias_registry: AliasRegistry,
    ) -> tuple[str, tuple[Any, ...]]:
        if isinstance(
            value,
            (
                BinaryExpression,
                FunctionCall,
                FilteredFunctionCall,
                WindowFunctionCall,
            ),
        ):
            return value.to_sql(alias_registry)
        return value.get_ref(alias_registry), tuple()

    @staticmethod
    def _resolve_operator(
        operator: type[AbstractOperator] | AbstractOperator | OperatorFactory,
        col_ref: str,
    ) -> AbstractOperator:
        if isinstance(operator, AbstractOperator):
            return operator
        return operator(col_ref)

    def __and__(self, other: Condition) -> Condition:
        return Condition(is_and=True, left=self, right=other)

    def __or__(self, other: Condition) -> Condition:
        return Condition(is_and=False, left=self, right=other)

    def __invert__(self) -> Condition:
        result = copy(self)
        result.negated = not self.negated
        return result

    def to_sql(
        self,
        alias_registry: AliasRegistry,
    ) -> tuple[str, tuple[Any, ...]]:
        def apply_negation(
            sql: str,
            params: tuple[Any, ...],
        ) -> tuple[str, tuple[Any, ...]]:
            if self.negated:
                return (f"NOT ({sql})" if sql else "NOT", params)
            return sql, params

        if self.left and self.right:
            left_sql, left_params = self.left.to_sql(alias_registry)
            right_sql, right_params = self.right.to_sql(alias_registry)
            operator_str: str = "AND" if self.is_and else "OR"
            return apply_negation(
                f"({left_sql} {operator_str} {right_sql})",
                left_params + right_params,
            )

        if not self.column:
            return apply_negation("", tuple())

        col_ref, col_params = self._render_expression(
            self.column,
            alias_registry,
        )
        operator_spec = self.operator
        if operator_spec is None:
            return apply_negation(col_ref, col_params)

        operator = self._resolve_operator(operator_spec, col_ref)

        if isinstance(self.value, (ComparableExpression, FunctionCall)):
            value_sql, value_params = self._render_expression(
                self.value,
                alias_registry,
            )
            sql, op_params = operator.to_sql_ref(value_sql)
            return apply_negation(sql, col_params + value_params + op_params)

        if isinstance(self.value, AbstractQuery):
            subquery_sql, subquery_params = self.value.build_query(
                alias_registry,
            )
            sql, op_params = operator.to_sql_ref(subquery_sql)
            return apply_negation(
                sql,
                col_params + subquery_params + op_params,
            )

        sql, op_params = operator.to_sql(self.value)
        return apply_negation(sql, col_params + op_params)


class Alias(ComparableExpression):
    """Represents a named SQL alias."""

    def __init__(self, name: str) -> None:
        self.name: str = name

    def get_ref(self, alias_registry: AliasRegistry) -> str:  # noqa: ARG002
        return f'"{self.name}"'

    def __repr__(self) -> str:
        return f"Alias({self.name!r})"

    def to_sql(self, alias_registry: AliasRegistry) -> str:
        return self.get_ref(alias_registry)


class FunctionCall(ComparableExpression):
    """Represents a SQL function call with arguments."""

    def __init__(self, name: str, *args: Any) -> None:
        """Initialize a function call.

        Args:
            name: The SQL function name (e.g., 'SUM', 'COUNT', 'MAX').
            *args: Arguments to pass to the function.

        """
        self.name: str = name
        self.args: tuple[Any, ...] = args
        self._alias: Alias | None = None

    def as_(self, alias: Alias | str) -> FunctionCall:
        """Attach a named alias to the function call."""
        result = copy(self)
        result._alias = alias if isinstance(alias, Alias) else Alias(alias)
        return result

    def filter(self, condition: Condition) -> FilteredFunctionCall:
        """Attach a SQL FILTER clause to the function call."""
        return FilteredFunctionCall(self, condition)

    def over(  # noqa: PLR0913
        self,
        window: Window | Alias | str | None = None,
        *,
        partition_by: WindowExpression | tuple[WindowExpression, ...] = (),
        order_by: (
            WindowExpression
            | tuple[WindowExpression, ...]
            | tuple[tuple[WindowExpression, bool], ...]
        ) = (),
        descending: bool = False,
        rows: WindowFrameSpec | None = None,
        range_: WindowFrameSpec | None = None,
        groups: WindowFrameSpec | None = None,
        exclude: str | None = None,
    ) -> WindowFunctionCall:
        """Apply an OVER clause to this function call."""
        window_spec = Window.from_over_parts(
            window,
            partition_by=partition_by,
            order_by=order_by,
            descending=descending,
            rows=rows,
            range_=range_,
            groups=groups,
            exclude=exclude,
        )
        return WindowFunctionCall(self, window_spec)

    def get_alias(self) -> Alias | None:
        return self._alias

    def to_sql(
        self,
        alias_registry: AliasRegistry,
        *,
        include_alias: bool = False,
    ) -> tuple[str, tuple[Any, ...]]:
        """Convert function call to SQL and extract parameters.

        Returns:
            Tuple of (sql_string, parameters_tuple)

        """
        sql_args: list[str] = []
        params: list[Any] = []

        for arg in self.args:
            if isinstance(arg, Column):
                sql_args.append(arg.get_ref(alias_registry))

            elif isinstance(arg, FunctionCall):
                # Nested function call
                nested_sql, nested_params = arg.to_sql(alias_registry)
                sql_args.append(nested_sql)
                params.extend(nested_params)
            elif arg == "*":
                # Special case for COUNT(*)
                sql_args.append("*")
            elif isinstance(arg, str):
                # String literal - parameterized
                sql_args.append("?")
                params.append(arg)
            elif isinstance(arg, (int, float)):
                # Numeric literal - parameterized
                sql_args.append("?")
                params.append(arg)
            else:
                # Other types - parameterized
                sql_args.append("?")
                params.append(arg)

        args_sql = ", ".join(sql_args)
        sql = f"{self.name}({args_sql})"
        if include_alias and self._alias is not None:
            sql = f"{sql} AS {self._alias.get_ref(alias_registry)}"
        return sql, tuple(params)

    def __repr__(self) -> str:
        args_repr = ", ".join(repr(arg) for arg in self.args)
        if self._alias is None:
            return f"FunctionCall({self.name}({args_repr}))"
        return f"FunctionCall({self.name}({args_repr}) AS {self._alias!r})"

    def __hash__(self) -> int:
        raise TypeError(f"unhashable type: '{type(self).__name__}'")


class FunctionRegistry:
    """Dynamic SQL function registry using __getattr__.

    Allows arbitrary function calls without defining each one explicitly.
    Any attribute access
    returns a callable that creates FunctionCall instances.
    """

    def __getattr__(self, name: str) -> Callable[..., FunctionCall]:
        """Return a callable for creating FunctionCall instances.

        Args:
            name: The function name (e.g., 'sum', 'count', 'my_custom_func').

        Returns:
            A callable that creates FunctionCall instances with uppercase name.

        """

        def function_call(*args: Any) -> FunctionCall:
            return FunctionCall(name.upper(), *args)

        return function_call


func = FunctionRegistry()

WindowExpression = ComparableExpression | FunctionCall


class FilteredFunctionCall(ComparableExpression):
    """Represents a SQL function call with a FILTER clause."""

    def __init__(self, function: FunctionCall, condition: Condition) -> None:
        self.function: FunctionCall = function
        self.condition: Condition = condition
        self._alias: Alias | None = None

    def as_(self, alias: Alias | str) -> FilteredFunctionCall:
        result = copy(self)
        result._alias = alias if isinstance(alias, Alias) else Alias(alias)
        return result

    def over(  # noqa: PLR0913
        self,
        window: Window | Alias | str | None = None,
        *,
        partition_by: WindowExpression | tuple[WindowExpression, ...] = (),
        order_by: (
            WindowExpression
            | tuple[WindowExpression, ...]
            | tuple[tuple[WindowExpression, bool], ...]
        ) = (),
        descending: bool = False,
        rows: WindowFrameSpec | None = None,
        range_: WindowFrameSpec | None = None,
        groups: WindowFrameSpec | None = None,
        exclude: str | None = None,
    ) -> WindowFunctionCall:
        window_spec = Window.from_over_parts(
            window,
            partition_by=partition_by,
            order_by=order_by,
            descending=descending,
            rows=rows,
            range_=range_,
            groups=groups,
            exclude=exclude,
        )
        return WindowFunctionCall(self, window_spec)

    def get_alias(self) -> Alias | None:
        return self._alias

    def to_sql(
        self,
        alias_registry: AliasRegistry,
        *,
        include_alias: bool = False,
    ) -> tuple[str, tuple[Any, ...]]:
        function_sql, function_params = self.function.to_sql(alias_registry)
        condition_sql, condition_params = self.condition.to_sql(
            alias_registry,
        )
        sql = f"{function_sql} FILTER (WHERE {condition_sql})"
        if include_alias and self._alias is not None:
            sql = f"{sql} AS {self._alias.get_ref(alias_registry)}"
        return sql, function_params + condition_params

    def get_ref(self, alias_registry: AliasRegistry) -> str:
        return self.to_sql(alias_registry)[0]

    def __repr__(self) -> str:
        if self._alias is None:
            return f"FilteredFunctionCall({self.function!r})"
        return f"FilteredFunctionCall({self.function!r} AS {self._alias!r})"

    def __hash__(self) -> int:
        raise TypeError(f"unhashable type: '{type(self).__name__}'")


class FrameBoundary:
    """Represents a SQL window frame boundary."""

    def __init__(self, sql: str) -> None:
        self.sql: str = sql

    def to_sql(self) -> str:
        return self.sql

    def __str__(self) -> str:
        return self.to_sql()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.sql!r})"


class _FrameFactory:
    keyword: ClassVar[str]

    @classmethod
    def between(
        cls,
        start: WindowFrameBoundary,
        end: WindowFrameBoundary,
    ) -> tuple[WindowFrameBoundary, WindowFrameBoundary]:
        return (start, end)

    @classmethod
    def unbounded_preceding(cls) -> FrameBoundary:
        return FrameBoundary("UNBOUNDED PRECEDING")

    @classmethod
    def unbounded_following(cls) -> FrameBoundary:
        return FrameBoundary("UNBOUNDED FOLLOWING")

    @classmethod
    def current_row(cls) -> FrameBoundary:
        return FrameBoundary("CURRENT ROW")

    @classmethod
    def preceding(cls, value: int) -> FrameBoundary:
        return FrameBoundary(f"{value} PRECEDING")

    @classmethod
    def following(cls, value: int) -> FrameBoundary:
        return FrameBoundary(f"{value} FOLLOWING")

    @classmethod
    def interval_preceding(cls, value: int, unit: str) -> FrameBoundary:
        return FrameBoundary(f"INTERVAL {value} {unit} PRECEDING")

    @classmethod
    def interval_following(cls, value: int, unit: str) -> FrameBoundary:
        return FrameBoundary(f"INTERVAL {value} {unit} FOLLOWING")


class Rows(_FrameFactory):
    """Factory for ROWS window frame boundaries."""

    keyword = "ROWS"


class Range(_FrameFactory):
    """Factory for RANGE window frame boundaries."""

    keyword = "RANGE"


class Groups(_FrameFactory):
    """Factory for GROUPS window frame boundaries."""

    keyword = "GROUPS"


class Window:
    """Represents a SQL window specification."""

    Rows: ClassVar[type[Rows]] = Rows
    Range: ClassVar[type[Range]] = Range
    Groups: ClassVar[type[Groups]] = Groups

    def __init__(  # noqa: PLR0913
        self,
        name: Alias | str | None = None,
        *,
        base: Alias | str | None = None,
        partition_by: WindowExpression | tuple[WindowExpression, ...] = (),
        order_by: (
            WindowExpression
            | tuple[WindowExpression, ...]
            | tuple[tuple[WindowExpression, bool], ...]
        ) = (),
        descending: bool = False,
        rows: WindowFrameSpec | None = None,
        range_: WindowFrameSpec | None = None,
        groups: WindowFrameSpec | None = None,
        exclude: str | None = None,
    ) -> None:
        frame_count = sum(
            frame is not None for frame in (rows, range_, groups)
        )
        if frame_count > 1:
            raise ValueError("Only one frame type can be used")

        self.name: Alias | None = (
            name
            if isinstance(name, Alias)
            else Alias(name)
            if name is not None
            else None
        )
        self.base: Alias | None = (
            base
            if isinstance(base, Alias)
            else Alias(base)
            if base is not None
            else None
        )
        self.partition_by: tuple[WindowExpression, ...] = (
            self._normalize_expressions(partition_by)
        )
        self.order_by: tuple[tuple[WindowExpression, bool], ...] = (
            self._normalize_order_by(order_by, descending=descending)
        )
        self.frame_type: str | None = None
        if rows is not None:
            self.frame_type = "ROWS"
        elif range_ is not None:
            self.frame_type = "RANGE"
        elif groups is not None:
            self.frame_type = "GROUPS"
        self.frame: WindowFrameSpec | None = rows or range_ or groups
        self.exclude: str | None = exclude

    @classmethod
    def from_over_parts(  # noqa: PLR0913
        cls,
        window: Window | Alias | str | None = None,
        *,
        partition_by: WindowExpression | tuple[WindowExpression, ...] = (),
        order_by: (
            WindowExpression
            | tuple[WindowExpression, ...]
            | tuple[tuple[WindowExpression, bool], ...]
        ) = (),
        descending: bool = False,
        rows: WindowFrameSpec | None = None,
        range_: WindowFrameSpec | None = None,
        groups: WindowFrameSpec | None = None,
        exclude: str | None = None,
    ) -> Window:
        has_inline_parts = (
            cls._has_expressions(partition_by)
            or cls._has_order_by(order_by)
            or rows is not None
            or range_ is not None
            or groups is not None
            or exclude is not None
        )
        if isinstance(window, Window):
            if has_inline_parts:
                raise ValueError(
                    "Window object references cannot include inline parts",
                )
            if window.name is None:
                return window
            return Window(name=window.name)

        if window is None:
            return Window(
                partition_by=partition_by,
                order_by=order_by,
                descending=descending,
                rows=rows,
                range_=range_,
                groups=groups,
                exclude=exclude,
            )

        if has_inline_parts:
            raise ValueError(
                "Named window references cannot include inline window parts",
            )
        return Window(name=window)

    @staticmethod
    def _has_expressions(
        expressions: WindowExpression | tuple[WindowExpression, ...],
    ) -> bool:
        return not (isinstance(expressions, tuple) and not expressions)

    @staticmethod
    def _has_order_by(
        expressions: (
            WindowExpression
            | tuple[WindowExpression, ...]
            | tuple[tuple[WindowExpression, bool], ...]
        ),
    ) -> bool:
        return not (isinstance(expressions, tuple) and not expressions)

    @staticmethod
    def _normalize_expressions(
        expressions: WindowExpression | tuple[WindowExpression, ...],
    ) -> tuple[WindowExpression, ...]:
        if isinstance(expressions, tuple):
            return expressions
        return (expressions,)

    @staticmethod
    def _normalize_order_by(
        expressions: (
            WindowExpression
            | tuple[WindowExpression, ...]
            | tuple[tuple[WindowExpression, bool], ...]
        ),
        *,
        descending: bool,
    ) -> tuple[tuple[WindowExpression, bool], ...]:
        if isinstance(expressions, tuple):
            if not expressions:
                return ()
            if all(isinstance(item, tuple) for item in expressions):
                typed_expressions = cast(
                    "tuple[tuple[WindowExpression, bool], ...]",
                    expressions,
                )
                order_by_parts: list[tuple[WindowExpression, bool]] = []
                for expression, item_descending in typed_expressions:
                    order_by_parts.append((expression, item_descending))
                return tuple(order_by_parts)

            typed_order_expressions = cast(
                "tuple[WindowExpression, ...]",
                expressions,
            )
            order_by_parts = []
            for expression in typed_order_expressions:
                order_by_parts.append((expression, descending))
            return tuple(order_by_parts)
        return ((expressions, descending),)

    @staticmethod
    def _render_expression(
        expression: WindowExpression,
        alias_registry: AliasRegistry,
    ) -> tuple[str, tuple[Any, ...]]:
        if isinstance(expression, (FunctionCall, WindowFunctionCall)):
            return expression.to_sql(alias_registry)
        return expression.get_ref(alias_registry), tuple()

    @staticmethod
    def _render_frame(
        frame_type: str,
        frame: WindowFrameSpec,
    ) -> str:
        if isinstance(frame, tuple):
            start = Window._render_frame_boundary(frame[0])
            end = Window._render_frame_boundary(frame[1])
            return f"{frame_type} BETWEEN {start} AND {end}"
        return f"{frame_type} {Window._render_frame_boundary(frame)}"

    @staticmethod
    def _render_frame_boundary(boundary: WindowFrameBoundary) -> str:
        if isinstance(boundary, FrameBoundary):
            return boundary.to_sql()
        return boundary

    def get_ref(self, alias_registry: AliasRegistry) -> str:
        if self.name is None:
            raise ValueError("Anonymous window has no reference name")
        return self.name.get_ref(alias_registry)

    def to_sql(
        self,
        alias_registry: AliasRegistry,
        *,
        include_name: bool = False,
    ) -> tuple[str, tuple[Any, ...]]:
        params: list[Any] = []
        parts: list[str] = []

        if self.base is not None:
            parts.append(self.base.get_ref(alias_registry))

        if self.partition_by:
            partition_parts: list[str] = []
            for expression in self.partition_by:
                sql, expression_params = self._render_expression(
                    expression,
                    alias_registry,
                )
                partition_parts.append(sql)
                params.extend(expression_params)
            parts.append(f"PARTITION BY {', '.join(partition_parts)}")

        if self.order_by:
            order_parts: list[str] = []
            for expression, descending in self.order_by:
                sql, expression_params = self._render_expression(
                    expression,
                    alias_registry,
                )
                if descending:
                    sql = f"{sql} DESC"
                order_parts.append(sql)
                params.extend(expression_params)
            parts.append(f"ORDER BY {', '.join(order_parts)}")

        if self.frame_type is not None and self.frame is not None:
            parts.append(self._render_frame(self.frame_type, self.frame))

        if self.exclude is not None:
            parts.append(f"EXCLUDE {self.exclude}")

        sql = " ".join(parts)
        if include_name:
            if self.name is None:
                raise ValueError("Named WINDOW clause requires a window name")
            sql = f"{self.name.get_ref(alias_registry)} AS ({sql})"

        return sql, tuple(params)


class WindowFunctionCall(ComparableExpression):
    """Represents a SQL function call with an OVER clause."""

    def __init__(
        self,
        function: FunctionCall | FilteredFunctionCall,
        window: Window,
    ) -> None:
        self.function: FunctionCall | FilteredFunctionCall = function
        self.window: Window = window
        self._alias: Alias | None = None

    def as_(self, alias: Alias | str) -> WindowFunctionCall:
        result = copy(self)
        result._alias = alias if isinstance(alias, Alias) else Alias(alias)
        return result

    def get_alias(self) -> Alias | None:
        return self._alias

    def to_sql(
        self,
        alias_registry: AliasRegistry,
        *,
        include_alias: bool = False,
    ) -> tuple[str, tuple[Any, ...]]:
        func_sql, func_params = self.function.to_sql(alias_registry)

        if self.window.name is not None and not (
            self.window.partition_by
            or self.window.order_by
            or self.window.base is not None
            or self.window.frame_type is not None
            or self.window.exclude is not None
        ):
            window_sql = self.window.get_ref(alias_registry)
            window_params: tuple[Any, ...] = ()
        else:
            window_body, window_params = self.window.to_sql(alias_registry)
            window_sql = f"({window_body})"

        sql = f"{func_sql} OVER {window_sql}"
        if include_alias and self._alias is not None:
            sql = f"{sql} AS {self._alias.get_ref(alias_registry)}"

        return sql, func_params + window_params

    def get_ref(self, alias_registry: AliasRegistry) -> str:
        return self.to_sql(alias_registry)[0]

    def __repr__(self) -> str:
        if self._alias is None:
            return f"WindowFunctionCall({self.function!r})"
        return f"WindowFunctionCall({self.function!r} AS {self._alias!r})"

    def __hash__(self) -> int:
        raise TypeError(f"unhashable type: '{type(self).__name__}'")


def text_op(
    column: ComparableExpression | FunctionCall,
    operator: str,
    value: object,
) -> Condition:
    return Condition(
        column=column,
        operator=lambda col_ref: TextOperator(col_ref, operator),
        value=value,
    )


class Column(ComparableExpression):
    def __init__(self, name: str) -> None:
        self.name: str = name

    def _attach_table(self, table: Table) -> None:
        self.table = table

    def get_ref(self, alias_registry: AliasRegistry) -> str:
        alias = alias_registry.get_alias_for_table(self.table)
        return f'"{alias.name}"."{self.name}"'

    def like(self, pattern: str) -> Condition:
        return Condition(column=self, operator=LikeOperator, value=pattern)

    def ilike(self, pattern: str) -> Condition:
        return Condition(column=self, operator=IlikeOperator, value=pattern)

    def in_(self, values: tuple[Any, ...] | list[Any] | Any) -> Condition:
        return Condition(column=self, operator=InOperator, value=values)

    def not_in(self, values: tuple[Any, ...] | list[Any] | Any) -> Condition:
        return Condition(column=self, operator=NotInOperator, value=values)


class Table:
    def __init__(
        self,
        name: str | AbstractQuery,
        *columns: Column,
    ) -> None:
        self._table_name: str = ""
        self._subquery: AbstractQuery | None = None
        self.columns: dict[str, Column] = {}

        if isinstance(name, AbstractQuery):
            self._subquery = name
        else:
            self._table_name = name

        if columns:
            for col in columns:
                col._attach_table(self)  # pyright: ignore[reportPrivateUsage]
                self.columns[col.name] = col

    def get_name(self) -> str:
        if self._subquery is not None:
            raise ValueError("Table is a subquery, no name available")
        return self._table_name

    def to_sql(
        self,
        alias_registry: AliasRegistry | None = None,
    ) -> tuple[str, tuple[Any, ...]]:
        if self._subquery is not None:
            subquery_sql, subquery_params = self._subquery.build_query(
                alias_registry,
            )
            return f"({subquery_sql})", subquery_params

        return f'"{self._table_name}"', tuple()

    def __dir__(self) -> Iterable[str]:
        default_dir = super().__dir__()
        cols = list(self.columns.keys())
        cols.extend(default_dir)
        return cols

    def __getattr__(self, column_name: str) -> Column:
        if column_name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' "
                f"object has no attribute '{column_name}'",
            )

        if self.columns:
            return self.columns[column_name]

        column = Column(column_name)
        column._attach_table(self)  # pyright: ignore[reportPrivateUsage]
        return column
