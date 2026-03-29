from copy import copy
from typing import Any, Self

from sql_fusion.composite_table import (
    AbstractQuery,
    Alias,
    Column,
    Condition,
    FunctionCall,
    Table,
)
from sql_fusion.operators import EqualOperator


class select(AbstractQuery):
    def __init__(self, *columns: Column | Alias | FunctionCall) -> None:
        super().__init__(table=None, columns=columns)
        self._having_condition: Condition | None = None
        self._group_by_columns: tuple[Column, ...] = ()
        self._group_by_type: str = "normal"
        self._grouping_sets: tuple[tuple[Column, ...], ...] = ()
        self._order_by_columns: tuple[
            tuple[Column | Alias | FunctionCall, bool],
            ...,
        ] = ()
        self._joins: list[
            tuple[str, Table, Condition | None]
        ] = []  # (join_type, table, condition or None for CROSS JOIN)
        self._limit: int | None = None
        self._offset: int | None = None
        self._distinct: bool = False

    def build_query(self) -> tuple[str, tuple[Any, ...]]:  # noqa: PLR0912
        params: list[Any] = []
        with_sql, with_params = self._build_with_clause()
        params.extend(with_params)
        table = self._get_table()

        if not self._columns:
            col_part: str = "*"
        else:
            col_parts: list[str] = []

            for col in self._columns:
                if isinstance(col, FunctionCall):
                    # Handle function calls
                    func_sql, func_params = col.to_sql(include_alias=True)
                    col_parts.append(func_sql)
                    params.extend(func_params)
                elif isinstance(col, Alias):
                    col_parts.append(col.to_sql())
                else:
                    # Handle regular columns
                    col_parts.append(f'"{col.table_alias}"."{col.name}"')

            col_part = ", ".join(col_parts)

        distinct_part = "SELECT DISTINCT" if self._distinct else "SELECT"
        table_sql, table_params = table.to_sql()
        query_parts: list[str] = []
        if with_sql:
            query_parts.append(with_sql)
        query_parts.append(
            self._build_clause("SELECT", distinct_part, col_part),
        )
        query_parts.append(
            self._build_clause(
                "FROM",
                "FROM",
                f'{table_sql} AS "{table.get_alias()}"',
            ),
        )
        params.extend(table_params)

        # Add JOIN clauses
        if self._joins:
            joins_sql, joins_params = self._build_joins()
            query_parts.append(joins_sql)
            params.extend(joins_params)

        if self._where_condition:
            where_sql, where_params = self._where_condition.to_sql()
            query_parts.append(self._build_clause("WHERE", "WHERE", where_sql))
            params.extend(where_params)

        if (
            self._group_by_columns
            or self._grouping_sets
            or self._group_by_type == "all"
        ):
            group_by_sql, group_by_params = self._build_group_by_clause()
            query_parts.append(group_by_sql)
            params.extend(group_by_params)

        if self._having_condition:
            having_sql, having_params = self._having_condition.to_sql()
            query_parts.append(
                self._build_clause("HAVING", "HAVING", having_sql),
            )
            params.extend(having_params)

        if self._order_by_columns:
            order_parts: list[str] = []
            for col, descending in self._order_by_columns:
                if isinstance(col, FunctionCall):
                    col_sql, col_params = col.to_sql()
                    params.extend(col_params)
                elif isinstance(col, Alias):
                    col_sql = col.to_sql()
                else:
                    col_sql = f'"{col.table_alias}"."{col.name}"'

                if descending:
                    col_sql = f"{col_sql} DESC"
                order_parts.append(col_sql)

            query_parts.append(
                self._build_clause(
                    "ORDER BY",
                    "ORDER BY",
                    ", ".join(order_parts),
                ),
            )

        if self._limit is not None:
            query_parts.append(
                self._build_clause("LIMIT", "LIMIT", str(self._limit)),
            )

        if self._offset is not None:
            query_parts.append(
                self._build_clause("OFFSET", "OFFSET", str(self._offset)),
            )

        return self._apply_compile_expressions(
            " ".join(query_parts),
            tuple(params),
        )

    def _build_joins(self) -> tuple[str, list[Any]]:
        """Build JOIN clauses and return SQL string and parameters."""
        joins_sql_parts: list[str] = []
        joins_params: list[Any] = []

        for join_type, join_table, condition in self._joins:
            join_sql, join_params = join_table.to_sql()
            join_body = f'{join_sql} AS "{join_table.get_alias()}"'
            joins_params.extend(join_params)

            # CROSS JOIN doesn't have an ON clause
            if condition is not None:
                condition_sql, condition_params = condition.to_sql()
                join_body += f" ON {condition_sql}"
                joins_params.extend(condition_params)

            joins_sql_parts.append(
                self._build_clause(
                    "JOIN",
                    f"{join_type} JOIN",
                    join_body,
                ),
            )

        return " ".join(joins_sql_parts), joins_params

    def join(
        self,
        table: Table | AbstractQuery,
        condition: Condition,
    ) -> Self:
        """Add an INNER JOIN clause."""
        qs = copy(self)
        qs._joins = self._joins.copy()
        if isinstance(table, AbstractQuery):
            table = Table(table)
        qs._joins.append(("INNER", table, condition))
        return qs

    def left_join(
        self,
        table: Table | AbstractQuery,
        condition: Condition,
    ) -> Self:
        """Add a LEFT JOIN clause."""
        qs = copy(self)
        qs._joins = self._joins.copy()
        if isinstance(table, AbstractQuery):
            table = Table(table)
        qs._joins.append(("LEFT", table, condition))
        return qs

    def right_join(
        self,
        table: Table | AbstractQuery,
        condition: Condition,
    ) -> Self:
        """Add a RIGHT JOIN clause."""
        qs = copy(self)
        qs._joins = self._joins.copy()
        if isinstance(table, AbstractQuery):
            table = Table(table)
        qs._joins.append(("RIGHT", table, condition))
        return qs

    def full_join(
        self,
        table: Table | AbstractQuery,
        condition: Condition,
    ) -> Self:
        """Add a FULL OUTER JOIN clause."""
        qs = copy(self)
        qs._joins = self._joins.copy()
        if isinstance(table, AbstractQuery):
            table = Table(table)
        qs._joins.append(("FULL OUTER", table, condition))
        return qs

    def cross_join(self, table: Table | AbstractQuery) -> Self:
        """Add a CROSS JOIN clause (cartesian product)."""
        qs = copy(self)
        qs._joins = self._joins.copy()
        if isinstance(table, AbstractQuery):
            table = Table(table)
        qs._joins.append(("CROSS", table, None))
        return qs

    def semi_join(
        self,
        table: Table | AbstractQuery,
        condition: Condition,
    ) -> Self:
        """Add a SEMI JOIN clause (exists check)."""
        qs = copy(self)
        qs._joins = self._joins.copy()
        if isinstance(table, AbstractQuery):
            table = Table(table)
        qs._joins.append(("SEMI", table, condition))
        return qs

    def anti_join(
        self,
        table: Table | AbstractQuery,
        condition: Condition,
    ) -> Self:
        """Add an ANTI JOIN clause (not exists check)."""
        qs = copy(self)
        qs._joins = self._joins.copy()
        if isinstance(table, AbstractQuery):
            table = Table(table)
        qs._joins.append(("ANTI", table, condition))
        return qs

    def limit(self, n: int) -> Self:
        if n < 0:
            raise ValueError("LIMIT must be non-negative")
        qs = copy(self)
        qs._limit = n
        return qs

    def offset(self, n: int) -> Self:
        if n < 0:
            raise ValueError("OFFSET must be non-negative")
        qs = copy(self)
        qs._offset = n
        return qs

    def order_by(
        self,
        *columns: Column | Alias | FunctionCall,
        descending: bool = False,
    ) -> Self:
        if not columns:
            raise ValueError("order_by() requires at least one column")

        qs = copy(self)
        qs._order_by_columns = self._order_by_columns + tuple(
            (column, descending) for column in columns
        )
        return qs

    def distinct(self) -> Self:
        """Add DISTINCT clause to select only unique rows."""
        qs = copy(self)
        qs._distinct = True
        return qs

    def _build_group_by_clause(self) -> tuple[str, list[Any]]:
        if self._group_by_type == "all":
            return self._build_clause("GROUP BY", "GROUP BY", "ALL"), []

        col_refs: str = ", ".join(
            f'"{col.table_alias}"."{col.name}"'
            for col in self._group_by_columns
        )

        if self._group_by_type == "rollup":
            return (
                self._build_clause(
                    "GROUP BY",
                    "GROUP BY",
                    f"ROLLUP ({col_refs})",
                ),
                [],
            )

        if self._group_by_type == "cube":
            return (
                self._build_clause(
                    "GROUP BY",
                    "GROUP BY",
                    f"CUBE ({col_refs})",
                ),
                [],
            )

        if self._group_by_type == "grouping_sets":
            gr_sets = (
                self._extract_col_set(col_set) if col_set else "()"
                for col_set in self._grouping_sets
            )

            sets_sql: str = ", ".join(gr_sets)
            return (
                self._build_clause(
                    "GROUP BY",
                    "GROUP BY",
                    f"GROUPING SETS ({sets_sql})",
                ),
                [],
            )

        return self._build_clause("GROUP BY", "GROUP BY", col_refs), []

    @staticmethod
    def _extract_col_set(col_set: tuple[Column, ...]) -> str:
        col_gen = [f'"{col.table_alias}"."{col.name}"' for col in col_set]
        st = ", ".join(col_gen)
        return f"({st})"

    def having(self, *conditions: Condition) -> Self:
        if not self._group_by_columns and self._group_by_type == "normal":
            raise ValueError("Cannot use having() without group_by()")

        qs = copy(self)
        combined_condition: Condition | None = None

        for condition in conditions:
            if combined_condition is None:
                combined_condition = condition
            else:
                combined_condition = combined_condition & condition

        if combined_condition:
            if qs._having_condition is None:
                qs._having_condition = combined_condition
            else:
                qs._having_condition = (
                    qs._having_condition & combined_condition
                )

        return qs

    def having_by(self, **kwargs: Any) -> Self:
        if not self._group_by_columns and self._group_by_type == "normal":
            raise ValueError("Cannot use having_by() without group_by()")

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
            if qs._having_condition is None:
                qs._having_condition = combined_condition
            else:
                qs._having_condition = (
                    qs._having_condition & combined_condition
                )

        return qs

    def group_by(self, *columns: Column) -> Self:
        qs = copy(self)
        if not columns:
            qs._group_by_type = "all"
        else:
            qs._group_by_columns = columns
            qs._group_by_type = "normal"
        return qs

    def group_by_rollup(self, *columns: Column) -> Self:
        if not columns:
            raise ValueError("group_by_rollup() requires at least one column")

        qs = copy(self)
        qs._group_by_columns = columns
        qs._group_by_type = "rollup"
        return qs

    def group_by_cube(self, *columns: Column) -> Self:
        if not columns:
            raise ValueError("group_by_cube() requires at least one column")

        qs = copy(self)
        qs._group_by_columns = columns
        qs._group_by_type = "cube"
        return qs

    def group_by_grouping_sets(self, *column_sets: tuple[Column, ...]) -> Self:
        if not column_sets:
            raise ValueError(
                "group_by_grouping_sets() requires at least one set",
            )

        qs = copy(self)
        qs._group_by_type = "grouping_sets"
        qs._grouping_sets = column_sets
        return qs

    def from_(self, table: Table | AbstractQuery) -> Self:
        qs = copy(self)
        qs._table = table if isinstance(table, Table) else Table(table)
        return qs
