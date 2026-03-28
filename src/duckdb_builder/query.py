"""Query builder."""

from copy import copy
from typing import Any, Self

from .abstract_query import AbstractQuery
from .composite_table import Column, Condition, FunctionCall, Table


class select(AbstractQuery):
    def __init__(self, *columns: Column | FunctionCall) -> None:
        super().__init__(table=None, columns=columns)
        self._having_condition: Condition | None = None
        self._group_by_columns: tuple[Column, ...] = ()
        self._group_by_type: str = "normal"
        self._grouping_sets: tuple[tuple[Column, ...], ...] = ()
        self._joins: list[
            tuple[str, Table, Condition | None]
        ] = []  # (join_type, table, condition or None for CROSS JOIN)
        self._limit: int | None = None
        self._offset: int | None = None
        self._distinct: bool = False

    def build_query(self) -> tuple[str, tuple[Any, ...]]:
        params: list[Any] = []
        table = self._get_table()

        if not self._columns:
            col_part: str = "*"
        else:
            col_parts: list[str] = []

            for col in self._columns:
                if isinstance(col, FunctionCall):
                    # Handle function calls
                    func_sql, func_params = col.to_sql()
                    col_parts.append(func_sql)
                    params.extend(func_params)
                else:
                    # Handle regular columns
                    col_parts.append(f'"{col.table_alias}"."{col.name}"')

            col_part = ", ".join(col_parts)

        distinct_part = "DISTINCT " if self._distinct else ""
        table_sql, table_params = table.to_sql()
        query: str = (
            f"SELECT {distinct_part}{col_part} "  # noqa: S608
            f"FROM {table_sql} "
            f'AS "{table.get_alias()}"'
        )
        params.extend(table_params)

        # Add JOIN clauses
        if self._joins:
            joins_sql, joins_params = self._build_joins()
            query += joins_sql
            params.extend(joins_params)

        if self._where_condition:
            where_sql, where_params = self._where_condition.to_sql()
            query += f" WHERE {where_sql}"
            params.extend(where_params)

        if (
            self._group_by_columns
            or self._grouping_sets
            or self._group_by_type == "all"
        ):
            query += self._build_group_by_clause()

        if self._having_condition:
            having_sql, having_params = self._having_condition.to_sql()
            query += f" HAVING {having_sql}"
            params.extend(having_params)

        if self._limit is not None:
            query += f" LIMIT {self._limit}"

        if self._offset is not None:
            query += f" OFFSET {self._offset}"

        return query, tuple(params)

    def _build_joins(self) -> tuple[str, list[Any]]:
        """Build JOIN clauses and return SQL string and parameters."""
        joins_sql: str = ""
        joins_params: list[Any] = []

        for join_type, join_table, condition in self._joins:
            joins_sql += f" {join_type} JOIN "
            join_sql, join_params = join_table.to_sql()
            joins_sql += f'{join_sql} AS "{join_table.get_alias()}"'
            joins_params.extend(join_params)

            # CROSS JOIN doesn't have an ON clause
            if condition is not None:
                condition_sql, condition_params = condition.to_sql()
                joins_sql += f" ON {condition_sql}"
                joins_params.extend(condition_params)

        return joins_sql, joins_params

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

    def distinct(self) -> Self:
        """Add DISTINCT clause to select only unique rows."""
        qs = copy(self)
        qs._distinct = True
        return qs

    def _build_group_by_clause(self) -> str:
        if self._group_by_type == "all":
            return " GROUP BY ALL"

        col_refs: str = ", ".join(
            f'"{col.table_alias}"."{col.name}"'
            for col in self._group_by_columns
        )

        if self._group_by_type == "rollup":
            return f" GROUP BY ROLLUP ({col_refs})"

        if self._group_by_type == "cube":
            return f" GROUP BY CUBE ({col_refs})"

        if self._group_by_type == "grouping_sets":
            gr_sets = (
                self._extract_col_set(col_set) if col_set else "()"
                for col_set in self._grouping_sets
            )

            sets_sql: str = ", ".join(gr_sets)
            return f" GROUP BY GROUPING SETS ({sets_sql})"

        return f" GROUP BY {col_refs}"

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
                operator="=",
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


class insert(AbstractQuery):
    def __init__(
        self,
        table: Table,
        *,
        or_replace: bool = False,
        or_ignore: bool = False,
    ) -> None:
        super().__init__(table=table, columns=())
        self._values: dict[str, Any] = {}
        self._or_replace: bool = or_replace
        self._or_ignore: bool = or_ignore

    def values(self, **values: Any) -> Self:
        if not values:
            raise ValueError("No values provided for insert")
        self._values.update(values)
        return self

    def build_query(self) -> tuple[str, tuple[Any, ...]]:
        if not self._values:
            raise ValueError("No values provided for insert")
        table = self._get_table()

        columns = list(self._values.keys())
        col_names = ", ".join(f'"{col}"' for col in columns)
        placeholders = ", ".join("?" * len(columns))
        params = tuple(self._values[col] for col in columns)

        insert_stmnt = "INSERT"

        if self._or_replace and self._or_ignore:
            raise ValueError("Cannot use both or_replace and or_ignore")
        if self._or_replace:
            insert_stmnt += " OR REPLACE"
        elif self._or_ignore:
            insert_stmnt += " OR IGNORE"

        query = (
            f'{insert_stmnt} INTO "{table.get_table_name()}" '
            f"({col_names}) VALUES ({placeholders})"
        )

        return query, params


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

        query = (
            f'UPDATE "{table.get_table_name()}" '  # noqa: S608
            f'AS "{table_alias}" '
            f"SET {', '.join(assignments)}"
        )

        if self._where_condition:
            where_sql, where_params = self._where_condition.to_sql()
            query += f" WHERE {where_sql}"
            params.extend(where_params)

        return query, tuple(params)


class delete(AbstractQuery):
    def __init__(self, table: Table) -> None:
        super().__init__(table=table, columns=())
        self._returning_columns: tuple[Column | FunctionCall, ...] = ()
        self._returning_all: bool = False

    def returning(self, *columns: Column | FunctionCall) -> Self:
        if not columns:
            self._returning_all = True
            self._returning_columns = ()
            return self

        if not self._returning_all:
            self._returning_columns += columns

        return self

    def build_query(self) -> tuple[str, tuple[Any, ...]]:
        table = self._get_table()
        query = (
            f'DELETE FROM "{table.get_table_name()}" '  # noqa: S608
            f'AS "{table.get_alias()}"'
        )
        params: list[Any] = []

        if self._where_condition:
            where_sql, where_params = self._where_condition.to_sql()
            query += f" WHERE {where_sql}"
            params.extend(where_params)

        if self._returning_all:
            query += " RETURNING *"
            return query, tuple(params)

        if self._returning_columns:
            returning_parts: list[str] = []

            for col in self._returning_columns:
                if isinstance(col, FunctionCall):
                    func_sql, func_params = col.to_sql()
                    returning_parts.append(func_sql)
                    params.extend(func_params)
                else:
                    returning_parts.append(f'"{col.table_alias}"."{col.name}"')

            query += f" RETURNING {', '.join(returning_parts)}"

        return query, tuple(params)
