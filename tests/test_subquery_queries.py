"""Tests for subquery support."""

from duckdb_builder.composite_table import Table
from duckdb_builder.query import select


def test_select_from_subquery() -> None:
    orders = Table("orders")
    paid_orders = select(orders.user_id).from_(orders).where_by(status="paid")
    query, params = select().from_(paid_orders).as_tuple()
    assert query == (
        "SELECT * "
        'FROM (SELECT "a"."user_id" '
        'FROM "orders" AS "a" '
        'WHERE "a"."status" = ?) AS "b"'
    )
    assert params == ("paid",)


def test_select_from_clause_with_columns() -> None:
    users = Table("users")
    query, params = select(users.name).from_(users).as_tuple()
    assert query == 'SELECT "a"."name" FROM "users" AS "a"'
    assert params == ()


def test_in_subquery_in_where() -> None:
    users = Table("users")
    orders = Table("orders")
    paid_order_user_ids = (
        select(orders.user_id).from_(orders).where_by(status="paid")
    )
    query, params = (
        select()
        .from_(users)
        .where(users.id.in_(paid_order_user_ids))
        .as_tuple()
    )
    assert query == (
        "SELECT * "
        'FROM "users" AS "a" '
        'WHERE "a"."id" IN '
        '(SELECT "b"."user_id" '
        'FROM "orders" AS "b" '
        'WHERE "b"."status" = ?)'
    )
    assert params == ("paid",)


def test_not_in_subquery_in_where() -> None:
    users = Table("users")
    banned_users = Table("banned_users")
    banned_ids = select(banned_users.user_id).from_(banned_users)
    query, params = (
        select().from_(users).where(users.id.not_in(banned_ids)).as_tuple()
    )
    assert query == (
        "SELECT * "
        'FROM "users" AS "a" '
        'WHERE "a"."id" NOT IN '
        '(SELECT "b"."user_id" FROM "banned_users" AS "b")'
    )
    assert params == ()
