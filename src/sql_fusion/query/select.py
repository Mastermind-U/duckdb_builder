from copy import copy
from typing import Any, Self

from sql_fusion.composite_table import (
    AbstractQuery,
    Alias,
    AliasRegistry,
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

    def build_query(  # noqa: C901, PLR0912, PLR0915
        self,
        alias_registry: AliasRegistry | None = None,
    ) -> tuple[str, tuple[Any, ...]]:
        registry = alias_registry or self._alias_registry
        params: list[Any] = []
        with_sql, with_params = self._build_with_clause(registry)
        params.extend(with_params)
        table = self._get_table()
        table_sql, table_params, alias = self._prepare_table_entry(
            table,
            registry,
        )
        joins_data = self._prepare_join_entries(registry)

        if not self._columns:
            col_part: str = "*"
        else:
            col_parts: list[str] = []

            for col in self._columns:
                if isinstance(col, FunctionCall):
                    # Handle function calls
                    func_sql, func_params = col.to_sql(
                        registry,
                        include_alias=True,
                    )
                    col_parts.append(func_sql)
                    params.extend(func_params)
                elif isinstance(col, Alias):
                    col_parts.append(col.to_sql(registry))
                else:
                    # Handle regular columns from any table in scope.
                    col_parts.append(col.get_ref(registry))

            col_part = ", ".join(col_parts)

        distinct_part = "SELECT DISTINCT" if self._distinct else "SELECT"
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
                f'{table_sql} AS "{alias.name}"',
            ),
        )
        params.extend(table_params)

        # Add JOIN clauses
        if joins_data:
            joins_sql, joins_params = self._build_joins_from_entries(
                registry,
                joins_data,
            )
            query_parts.append(joins_sql)
            params.extend(joins_params)

        if self._where_condition:
            where_sql, where_params = self._where_condition.to_sql(registry)
            query_parts.append(self._build_clause("WHERE", "WHERE", where_sql))
            params.extend(where_params)

        if (
            self._group_by_columns
            or self._grouping_sets
            or self._group_by_type == "all"
        ):
            group_by_sql, group_by_params = self._build_group_by_clause(
                registry,
            )
            query_parts.append(group_by_sql)
            params.extend(group_by_params)

        if self._having_condition:
            having_sql, having_params = self._having_condition.to_sql(registry)
            query_parts.append(
                self._build_clause("HAVING", "HAVING", having_sql),
            )
            params.extend(having_params)

        if self._order_by_columns:
            order_parts: list[str] = []
            for col, descending in self._order_by_columns:
                if isinstance(col, FunctionCall):
                    col_sql, col_params = col.to_sql(registry)
                    params.extend(col_params)
                elif isinstance(col, Alias):
                    col_sql = col.to_sql(registry)
                else:
                    col_sql = col.get_ref(registry)

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

    def _prepare_table_entry(
        self,
        table: Table,
        alias_registry: AliasRegistry,
    ) -> tuple[str, tuple[Any, ...], Alias]:
        if table._subquery is not None:  # pyright: ignore[reportPrivateUsage]
            table_sql, table_params = table.to_sql(alias_registry)
            alias = alias_registry.get_alias_for_table(table)
            return table_sql, table_params, alias

        alias = alias_registry.get_alias_for_table(table)
        table_sql, table_params = table.to_sql(alias_registry)
        return table_sql, table_params, alias

    def _prepare_join_entries(
        self,
        alias_registry: AliasRegistry,
    ) -> list[
        tuple[str, Table, Condition | None, str, tuple[Any, ...], Alias]
    ]:
        join_entries: list[
            tuple[str, Table, Condition | None, str, tuple[Any, ...], Alias]
        ] = []

        for join_type, join_table, condition in self._joins:
            join_sql, join_params, alias = self._prepare_table_entry(
                join_table,
                alias_registry,
            )
            join_entries.append(
                (
                    join_type,
                    join_table,
                    condition,
                    join_sql,
                    join_params,
                    alias,
                ),
            )

        return join_entries

    def _build_joins_from_entries(
        self,
        alias_registry: AliasRegistry,
        join_entries: list[
            tuple[str, Table, Condition | None, str, tuple[Any, ...], Alias]
        ],
    ) -> tuple[str, list[Any]]:
        """Build JOIN clauses and return SQL string and parameters."""
        joins_sql_parts: list[str] = []
        joins_params: list[Any] = []

        for (
            join_type,
            _join_table,
            condition,
            join_sql,
            join_params,
            alias,
        ) in join_entries:
            join_body = f'{join_sql} AS "{alias.name}"'
            joins_params.extend(join_params)

            # CROSS JOIN doesn't have an ON clause
            if condition is not None:
                condition_sql, condition_params = condition.to_sql(
                    alias_registry,
                )
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

    def _build_group_by_clause(
        self,
        alias_registry: AliasRegistry | None = None,
    ) -> tuple[str, list[Any]]:
        registry = alias_registry or self._alias_registry
        if self._group_by_type == "all":
            return self._build_clause("GROUP BY", "GROUP BY", "ALL"), []

        col_refs: str = ", ".join(
            col.get_ref(registry) for col in self._group_by_columns
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
                self._extract_col_set_with_registry(col_set, registry)
                if col_set
                else "()"
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

    def _extract_col_set(self, col_set: tuple[Column, ...]) -> str:
        return self._extract_col_set_with_registry(
            col_set,
            self._alias_registry,
        )

    def _extract_col_set_with_registry(
        self,
        col_set: tuple[Column, ...],
        alias_registry: AliasRegistry,
    ) -> str:
        col_gen: list[str] = []

        for col in col_set:
            col_gen.append(col.get_ref(alias_registry))

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
        qs._alias_registry.get_alias_for_table(table)

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
