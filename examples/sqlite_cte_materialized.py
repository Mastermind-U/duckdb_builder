from __future__ import annotations

import re
from typing import Any

from sql_fusion import Table, func, select
from sql_fusion.composite_table import CompileExpression


def sqlite_cte_materialization(
    *,
    materialized: dict[str, bool],
) -> CompileExpression:
    """Rewrite SQLite CTEs to use MATERIALIZED or NOT MATERIALIZED."""

    def rewrite(
        sql: str,
        params: tuple[Any, ...],
    ) -> tuple[str, tuple[Any, ...]]:
        for cte_name, is_materialized in materialized.items():
            marker = "MATERIALIZED" if is_materialized else "NOT MATERIALIZED"
            sql = re.sub(
                rf'("{re.escape(cte_name)}")\s+AS\s+\(',
                rf"\1 AS {marker} (",
                sql,
                count=1,
            )
        return sql, params

    return rewrite


def build_queries() -> None:
    orders = Table("orders")
    users = Table("users")
    paid_orders = Table("paid_orders")

    paid_orders_cte = (
        select(orders.user_id, orders.total)
        .from_(orders)
        .where_by(status="paid")
    )

    materialized_query, materialized_params = (
        select(users.name, func.sum(paid_orders.total))
        .with_(paid_orders=paid_orders_cte)
        .from_(paid_orders)
        .join(users, paid_orders.user_id == users.id)
        .group_by(users.name)
        .compile_expression(
            sqlite_cte_materialization(
                materialized={"paid_orders": True},
            ),
        )
        .compile()
    )

    not_materialized_query, not_materialized_params = (
        select(users.name, func.sum(paid_orders.total))
        .with_(paid_orders=paid_orders_cte)
        .from_(paid_orders)
        .join(users, paid_orders.user_id == users.id)
        .group_by(users.name)
        .compile_expression(
            sqlite_cte_materialization(
                materialized={"paid_orders": False},
            ),
        )
        .compile()
    )

    print("MATERIALIZED")
    print(materialized_query)
    print(materialized_params)
    print()
    print("NOT MATERIALIZED")
    print(not_materialized_query)
    print(not_materialized_params)


if __name__ == "__main__":
    build_queries()
