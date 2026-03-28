from copy import copy
from typing import Any, Callable

from .operators import OPERATORS


class Condition:
    def __init__(  # noqa: PLR0913
        self,
        column: Column | FunctionCall | None = None,
        operator: str = "",
        value: object | None = None,
        is_and: bool = True,
        left: Condition | None = None,
        right: Condition | None = None,
    ) -> None:
        self.column: Column | FunctionCall | None = column
        self.operator: str = operator
        self.value: object | None = value
        self.is_and: bool = is_and
        self.left: Condition | None = left
        self.right: Condition | None = right

    def __and__(self, other: Condition) -> Condition:
        return Condition(is_and=True, left=self, right=other)

    def __or__(self, other: Condition) -> Condition:
        return Condition(is_and=False, left=self, right=other)

    def __invert__(self) -> Condition:
        result = copy(self)
        result.operator = (
            f"NOT ({result.operator})" if self.operator else "NOT"
        )
        return result

    def to_sql(self) -> tuple[str, tuple[Any, ...]]:
        if self.left and self.right:
            left_sql, left_params = self.left.to_sql()
            right_sql, right_params = self.right.to_sql()
            operator_str: str = "AND" if self.is_and else "OR"
            return (
                f"({left_sql} {operator_str} {right_sql})",
                left_params + right_params,
            )

        if self.column:
            # Handle FunctionCall objects

            if isinstance(self.column, FunctionCall):
                func_sql, func_params = self.column.to_sql()
                params: list[Any] = list(func_params)

                # Handle value - could be Column or literal
                if isinstance(self.value, Column):
                    value_ref: str = (
                        f'"{self.value.table_alias}"."{self.value.name}"'
                    )
                    return (
                        f"{func_sql} {self.operator} {value_ref}",
                        tuple(params),
                    )
                if isinstance(self.value, FunctionCall):
                    value_sql, value_params = self.value.to_sql()
                    params.extend(value_params)
                    return f"{func_sql} {self.operator} {value_sql}", tuple(
                        params,
                    )
                # Literal value
                params.append(self.value)
                return f"{func_sql} {self.operator} ?", tuple(params)

            # Handle regular Column objects
            col_ref: str = f'"{self.column.table_alias}"."{self.column.name}"'

            # Handle JOIN conditions where value is a Column
            if isinstance(self.value, Column):
                return self.value.ref_to_sql(col_ref, self.operator), tuple()

            if self.operator in OPERATORS:
                operator_class = OPERATORS[self.operator]
                return operator_class(col_ref).to_sql(self.value)

            return "", tuple()

        return "", tuple()


class FunctionCall:
    """Represents a SQL function call with arguments."""

    def __init__(self, name: str, *args: Any) -> None:
        """Initialize a function call.

        Args:
            name: The SQL function name (e.g., 'SUM', 'COUNT', 'MAX').
            *args: Arguments to pass to the function.

        """
        self.name: str = name
        self.args: tuple[Any, ...] = args

    def to_sql(self) -> tuple[str, tuple[Any, ...]]:
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
        return f"{self.name}({args_sql})", tuple(params)

    def __eq__(self, other: object) -> Any:  # type: ignore[override]

        return Condition(column=self, operator="=", value=other)

    def __ne__(self, other: object) -> Any:  # type: ignore[override]

        return Condition(column=self, operator="!=", value=other)

    def __lt__(self, other: Any) -> Any:

        return Condition(column=self, operator="<", value=other)

    def __gt__(self, other: Any) -> Any:

        return Condition(column=self, operator=">", value=other)

    def __le__(self, other: Any) -> Any:

        return Condition(column=self, operator="<=", value=other)

    def __ge__(self, other: Any) -> Any:

        return Condition(column=self, operator=">=", value=other)

    def __repr__(self) -> str:
        args_repr = ", ".join(repr(arg) for arg in self.args)
        return f"FunctionCall({self.name}({args_repr}))"

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


class Column:
    def __init__(self, name: str, table_alias: str) -> None:
        self.name: str = name
        self.table_alias: str = table_alias

    def __eq__(self, other: object) -> Condition:  # type: ignore[override]
        return Condition(column=self, operator="=", value=other)

    def __ne__(self, other: object) -> Condition:  # type: ignore[override]
        return Condition(column=self, operator="!=", value=other)

    def __lt__(self, other: Any) -> Condition:
        return Condition(column=self, operator="<", value=other)

    def __gt__(self, other: Any) -> Condition:
        return Condition(column=self, operator=">", value=other)

    def __le__(self, other: Any) -> Condition:
        return Condition(column=self, operator="<=", value=other)

    def __ge__(self, other: Any) -> Condition:
        return Condition(column=self, operator=">=", value=other)

    def like(self, pattern: str) -> Condition:
        return Condition(column=self, operator="LIKE", value=pattern)

    def ilike(self, pattern: str) -> Condition:
        return Condition(column=self, operator="ILIKE", value=pattern)

    def in_(self, values: tuple[Any, ...] | list[Any]) -> Condition:
        return Condition(column=self, operator="IN", value=values)

    def __hash__(self) -> int:
        raise TypeError(f"unhashable type: '{type(self).__name__}'")

    def get_ref(self) -> str:
        return f'"{self.table_alias}"."{self.name}"'

    def ref_to_sql(self, col_ref: str, operator: str) -> str:
        value_ref: str = self.get_ref()
        cref = ""

        if operator == "=":
            cref = f"{col_ref} = {value_ref}"
        if operator == "!=":
            cref = f"{col_ref} != {value_ref}"
        if operator == "<":
            cref = f"{col_ref} < {value_ref}"
        if operator == ">":
            cref = f"{col_ref} > {value_ref}"
        if operator == "<=":
            cref = f"{col_ref} <= {value_ref}"
        if operator == ">=":
            cref = f"{col_ref} >= {value_ref}"

        return cref


class Table:
    _alias_counter: int = 0

    def __init__(self, name: str, alias: str | None = None) -> None:
        self._table_name: str = name
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
        return self._table_name

    def get_alias(self) -> str:
        return self._alias

    def __getattr__(self, column_name: str) -> Column:
        if column_name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' "
                f"object has no attribute '{column_name}'",
            )
        return Column(column_name, self._alias)
