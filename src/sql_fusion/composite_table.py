from copy import copy
from typing import Any, Callable, Protocol, TypeGuard, cast

from .operators import (
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
)


class QueryLike(Protocol):
    def build_query(self) -> tuple[str, tuple[Any, ...]]: ...


class ComparableExpression:
    def _cond(
        self,
        operator: type[AbstractOperator],
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

    def get_ref(self) -> str:
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
    def _render_operand(operand: Any) -> tuple[str, tuple[Any, ...]]:
        if isinstance(operand, BinaryExpression):
            sql, params = operand.to_sql()
            return f"({sql})", params
        if isinstance(operand, FunctionCall):
            return operand.to_sql()
        if isinstance(operand, Column | Alias):
            return operand.get_ref(), tuple()
        if isinstance(operand, ComparableExpression):
            return operand.get_ref(), tuple()
        return "?", (operand,)

    def to_sql(self) -> tuple[str, tuple[Any, ...]]:
        left_sql, left_params = self._render_operand(self.left)
        right_sql, right_params = self._render_operand(self.right)
        return (
            f"{left_sql} {self.operator} {right_sql}",
            left_params + right_params,
        )

    def get_ref(self) -> str:
        return self.to_sql()[0]


class Condition:
    def __init__(  # noqa: PLR0913
        self,
        column: ComparableExpression | FunctionCall | None = None,
        operator: type[AbstractOperator] | None = None,
        value: object | None = None,
        is_and: bool = True,
        left: Condition | None = None,
        right: Condition | None = None,
        negated: bool = False,
    ) -> None:
        self.column: ComparableExpression | FunctionCall | None = column
        self.operator: type[AbstractOperator] | None = operator
        self.value: object | None = value
        self.is_and: bool = is_and
        self.left: Condition | None = left
        self.right: Condition | None = right
        self.negated: bool = negated

    @staticmethod
    def _render_expression(
        value: ComparableExpression | FunctionCall,
    ) -> tuple[str, tuple[Any, ...]]:
        if isinstance(value, (BinaryExpression, FunctionCall)):
            return value.to_sql()
        return value.get_ref(), tuple()

    def __and__(self, other: Condition) -> Condition:
        return Condition(is_and=True, left=self, right=other)

    def __or__(self, other: Condition) -> Condition:
        return Condition(is_and=False, left=self, right=other)

    def __invert__(self) -> Condition:
        result = copy(self)
        result.negated = not self.negated
        return result

    def to_sql(self) -> tuple[str, tuple[Any, ...]]:
        def apply_negation(
            sql: str,
            params: tuple[Any, ...],
        ) -> tuple[str, tuple[Any, ...]]:
            if self.negated:
                return (f"NOT ({sql})" if sql else "NOT", params)
            return sql, params

        if self.left and self.right:
            left_sql, left_params = self.left.to_sql()
            right_sql, right_params = self.right.to_sql()
            operator_str: str = "AND" if self.is_and else "OR"
            return apply_negation(
                f"({left_sql} {operator_str} {right_sql})",
                left_params + right_params,
            )

        if not self.column:
            return apply_negation("", tuple())

        col_ref, col_params = self._render_expression(self.column)
        operator_class = self.operator
        if operator_class is None:
            return apply_negation(col_ref, col_params)

        if isinstance(self.value, (ComparableExpression, FunctionCall)):
            value_sql, value_params = self._render_expression(self.value)
            sql, op_params = operator_class(col_ref).to_sql_ref(value_sql)
            return apply_negation(sql, col_params + value_params + op_params)

        if _is_query_like(self.value):
            subquery_sql, subquery_params = self.value.build_query()
            return apply_negation(
                f"{col_ref} {operator_class.sql_symbol} ({subquery_sql})",
                col_params + subquery_params,
            )

        sql, op_params = operator_class(col_ref).to_sql(self.value)
        return apply_negation(sql, col_params + op_params)


class Alias(ComparableExpression):
    """Represents a named SQL alias."""

    def __init__(self, name: str) -> None:
        self.name: str = name

    def get_ref(self) -> str:
        return f'"{self.name}"'

    def __repr__(self) -> str:
        return f"Alias({self.name!r})"

    def to_sql(self) -> str:
        return self.get_ref()


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

    def to_sql(
        self,
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
                # Column reference like "a"."id"
                sql_args.append(f'"{arg.table_alias}"."{arg.name}"')
            elif isinstance(arg, FunctionCall):
                # Nested function call
                nested_sql, nested_params = arg.to_sql()
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
            sql = f"{sql} AS {self._alias.get_ref()}"
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


class Column(ComparableExpression):
    def __init__(self, name: str, table_alias: str) -> None:
        self.name: str = name
        self.table_alias: str = table_alias

    def get_ref(self) -> str:
        return f'"{self.table_alias}"."{self.name}"'

    def like(self, pattern: str) -> Condition:
        return Condition(column=self, operator=LikeOperator, value=pattern)

    def ilike(self, pattern: str) -> Condition:
        return Condition(column=self, operator=IlikeOperator, value=pattern)

    def in_(self, values: tuple[Any, ...] | list[Any] | Any) -> Condition:
        return Condition(column=self, operator=InOperator, value=values)

    def not_in(self, values: tuple[Any, ...] | list[Any] | Any) -> Condition:
        return Condition(column=self, operator=NotInOperator, value=values)


class Table:
    _alias_counter: int = 0

    def __init__(
        self,
        name: str | QueryLike,
        alias: str | None = None,
    ) -> None:
        self._table_name: str = ""
        self._subquery: QueryLike | None = None

        if _is_query_like(name):
            self._subquery = name
        else:
            self._table_name = cast("str", name)

        if alias:
            self._alias: str = alias
        else:
            # Generate unique alias (a, b, c, ...)
            # Increment the counter for each new instance
            self._alias = chr(ord("a") + (Table._alias_counter % 26))
            Table._alias_counter += 1

    @classmethod
    def reset_alias_counter(cls) -> None:
        """Reset the alias counter. Useful for testing."""
        cls._alias_counter = 0

    def get_table_name(self) -> str:
        if self._subquery is not None:
            return self.to_sql()[0]
        return self._table_name

    def get_alias(self) -> str:
        return self._alias

    def to_sql(self) -> tuple[str, tuple[Any, ...]]:
        if self._subquery is not None:
            subquery_sql, subquery_params = self._subquery.build_query()
            return f"({subquery_sql})", subquery_params

        return f'"{self._table_name}"', tuple()

    def __getattr__(self, column_name: str) -> Column:
        if column_name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' "
                f"object has no attribute '{column_name}'",
            )
        return Column(column_name, self._alias)


def _is_query_like(value: object | None) -> TypeGuard[QueryLike]:
    return value is not None and hasattr(value, "build_query")
