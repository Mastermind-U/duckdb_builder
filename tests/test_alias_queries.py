"""Tests for SQL aliases."""

from sql_fusion import Alias, Table, func, select
from sql_fusion.composite_table import AliasRegistry


def test_function_call_as_alias_in_select() -> None:
    """Test aliasing a function call in SELECT."""
    orders = Table("orders")
    count_orders = Alias("count_orders")

    query = (
        select(
            orders.status,
            func.count(orders.id).as_(count_orders),
        )
        .from_(orders)
        .group_by(orders.status)
    )

    sql, params = query.build_query()

    assert sql == (
        'SELECT "a"."status", COUNT("a"."id") AS "count_orders" '
        'FROM "orders" AS "a" '
        'GROUP BY "a"."status"'
    )
    assert params == ()


def test_alias_condition_renders_quoted_name() -> None:
    """Test Alias comparison produces a quoted identifier."""
    count_orders = Alias("count_orders")
    orders_ge = 3
    reg = AliasRegistry()

    condition_sql, params = (count_orders >= orders_ge).to_sql(reg)

    assert condition_sql == '"count_orders" >= ?'
    assert params == (orders_ge,)


def test_alias_can_be_used_in_having_clause() -> None:
    """Test using Alias in HAVING."""
    orders = Table("orders")
    count_orders = Alias("count_orders")
    orders_ge = 3
    query = (
        select(
            orders.status,
            func.count(orders.id).as_(count_orders),
            func.sum(orders.total),
        )
        .from_(orders)
        .group_by(orders.status)
        .having(count_orders >= orders_ge)
    )

    sql, params = query.build_query()

    assert sql == (
        'SELECT "a"."status", COUNT("a"."id") AS "count_orders", '
        'SUM("a"."total") '
        'FROM "orders" AS "a" '
        'GROUP BY "a"."status" '
        'HAVING "count_orders" >= ?'
    )
    assert params == (3,)
