"""Tests for UPDATE queries."""

import pytest

from sql_fusion import Table, func, select, update


def test_update_set_returns_sql() -> None:
    table = Table("users")
    query, params = update(table).set(name="Alice", status="active").compile()
    assert query == 'UPDATE "users" AS "a" SET "name" = ?, "status" = ?'
    assert params == ("Alice", "active")


def test_update_set_method_merges_calls() -> None:
    table = Table("users")
    query, params = (
        update(table).set(id=2).set(name="Carol", status="pending").compile()
    )
    assert query == (
        'UPDATE "users" AS "a" SET "id" = ?, "name" = ?, "status" = ?'
    )
    assert params == (2, "Carol", "pending")


def test_update_with_where_clause() -> None:
    table = Table("users")
    user_id = 5
    query, params = (
        update(table)
        .set(status="inactive")
        .where(table.id == user_id)
        .compile()
    )
    assert query == 'UPDATE "users" AS "a" SET "status" = ? WHERE "a"."id" = ?'
    assert params == ("inactive", 5)


def test_update_with_subquery_in_where_clause() -> None:
    users = Table("users")
    orders = Table("orders")
    paid_order_user_ids = (
        select(orders.user_id).from_(orders).where_by(status="paid")
    )
    query, params = (
        update(users)
        .set(status="inactive")
        .where(users.id.in_(paid_order_user_ids))
        .compile()
    )
    assert query == (
        'UPDATE "users" AS "a" SET "status" = ? '
        'WHERE "a"."id" IN '
        '(SELECT "b"."user_id" '
        'FROM "orders" AS "b" '
        'WHERE "b"."status" = ?)'
    )
    assert params == ("inactive", "paid")


def test_update_set_column_by_column_expression() -> None:
    table = Table("users")
    query, params = update(table).set(counter=table.counter + 1).compile()
    assert query == ('UPDATE "users" AS "a" SET "counter" = "a"."counter" + ?')
    assert params == (1,)


def test_update_set_subquery_expression() -> None:
    users = Table("users")
    orders = Table("orders")
    subquery = (
        select(func.max(orders.total))
        .from_(orders)
        .where(orders.user_id == users.id)
    )

    query, params = update(users).set(total=subquery).compile()

    assert query == (
        'UPDATE "users" AS "a" '
        'SET "total" = (SELECT MAX("b"."total") '
        'FROM "orders" AS "b" '
        'WHERE "b"."user_id" = "a"."id")'
    )
    assert params == ()


def test_update_set_subquery_expression_with_params() -> None:
    users = Table("users")
    orders = Table("orders")
    subquery = (
        select(func.max(orders.total))
        .from_(orders)
        .where(orders.user_id == users.id)
        .where(orders.status == "completed")
    )

    query, params = update(users).set(total=subquery).compile()

    assert query == (
        'UPDATE "users" AS "a" '
        'SET "total" = (SELECT MAX("b"."total") '
        'FROM "orders" AS "b" '
        'WHERE ("b"."user_id" = "a"."id" AND "b"."status" = ?))'
    )
    assert params == ("completed",)


def test_update_set_multiple_subquery_expressions() -> None:
    users = Table("users")
    orders = Table("orders")

    max_total = (
        select(func.max(orders.total))
        .from_(orders)
        .where(orders.user_id == users.id)
    )
    order_count = (
        select(func.count("*")).from_(orders).where(orders.user_id == users.id)
    )

    query, params = (
        update(users).set(total=max_total, order_count=order_count).compile()
    )

    assert query == (
        'UPDATE "users" AS "a" '
        'SET "total" = (SELECT MAX("b"."total") '
        'FROM "orders" AS "b" '
        'WHERE "b"."user_id" = "a"."id"), '
        '"order_count" = (SELECT COUNT(*) '
        'FROM "orders" AS "b" '
        'WHERE "b"."user_id" = "a"."id")'
    )
    assert params == ()


def test_update_values_must_be_provided_once() -> None:
    table = Table("users")
    builder = update(table)
    with pytest.raises(ValueError, match="No values provided for update"):
        builder.compile()
    with pytest.raises(ValueError, match="No values provided for update"):
        builder.set().compile()
