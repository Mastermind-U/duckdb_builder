from copy import copy
from typing import Any, Self

from .composite_table import Column, Condition, FunctionCall, Table


class AbstractQuery:
    def __init__(
        self,
        table: Table | None,
        columns: tuple[Column | FunctionCall, ...] = (),
    ) -> None:
        self._table: Table | None = table
        self._columns: tuple[Column | FunctionCall, ...] = columns
        self._where_condition: Condition | None = None

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
                operator="=",
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

    def build_query(self) -> tuple[str, tuple[Any, ...]]:
        raise NotImplementedError()

    def as_tuple(self) -> tuple[str, tuple[Any, ...]]:
        return self.build_query()
