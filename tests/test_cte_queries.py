"""Tests for CTE / WITH clause support."""

import pytest

from duckdb_builder import Table, select


def test_select_with_single_cte() -> None:
    orders = Table("orders")
    recent_orders = (
        select(orders.user_id, orders.total)
        .from_(orders)
        .where_by(status="paid")
    )

    query, params = (
        select()
        .with_(recent_orders=recent_orders)
        .from_(Table("recent_orders"))
        .compile()
    )

    assert query == (
        'WITH "recent_orders" AS ('
        'SELECT "a"."user_id", "a"."total" '
        'FROM "orders" AS "a" '
        'WHERE "a"."status" = ?'
        ') SELECT * FROM "recent_orders" AS "b"'
    )
    assert params == ("paid",)


def test_select_with_multiple_ctes_preserves_parameter_order() -> None:
    orders = Table("orders")
    users = Table("users")

    paid_orders = select(orders.user_id).from_(orders).where_by(status="paid")
    active_users = select(users.id).from_(users).where_by(active=True)

    query, params = (
        select()
        .with_(paid_orders=paid_orders, active_users=active_users)
        .from_(Table("active_users"))
        .compile()
    )

    assert query == (
        'WITH "paid_orders" AS ('
        'SELECT "a"."user_id" FROM "orders" AS "a" WHERE "a"."status" = ?'
        '), "active_users" AS ('
        'SELECT "b"."id" FROM "users" AS "b" WHERE "b"."active" = ?'
        ') SELECT * FROM "active_users" AS "c"'
    )
    assert params == ("paid", True)


def test_select_with_multiple_with_calls_merges_ctes() -> None:
    orders = Table("orders")
    users = Table("users")

    paid_orders = select(orders.user_id).from_(orders).where_by(status="paid")
    active_users = select(users.id).from_(users).where_by(active=True)

    query, params = (
        select()
        .with_(paid_orders=paid_orders)
        .with_(active_users=active_users)
        .from_(Table("active_users"))
        .compile()
    )

    assert query == (
        'WITH "paid_orders" AS ('
        'SELECT "a"."user_id" FROM "orders" AS "a" WHERE "a"."status" = ?'
        '), "active_users" AS ('
        'SELECT "b"."id" FROM "users" AS "b" WHERE "b"."active" = ?'
        ') SELECT * FROM "active_users" AS "c"'
    )
    assert params == ("paid", True)


def test_select_with_recursive_cte() -> None:
    nodes = Table("nodes")
    tree = select(nodes.id, nodes.parent_id).from_(nodes).where_by(active=True)

    query, params = (
        select()
        .with_(recursive=True, tree=tree)
        .from_(Table("tree"))
        .compile()
    )

    assert query == (
        'WITH RECURSIVE "tree" AS ('
        'SELECT "a"."id", "a"."parent_id" '
        'FROM "nodes" AS "a" '
        'WHERE "a"."active" = ?'
        ') SELECT * FROM "tree" AS "b"'
    )
    assert params == (True,)


def test_with_requires_cte_queries() -> None:
    with pytest.raises(TypeError, match="must be query-like"):
        select().with_(bad_cte=object())  # type: ignore
