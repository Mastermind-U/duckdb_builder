"""Tests for DELETE queries."""

from duckdb_builder.composite_table import func
from duckdb_builder.query import Table, delete


def test_delete_returning_all() -> None:
    table = Table("users")
    query, params = delete(table).returning().as_tuple()
    assert query == 'DELETE FROM "users" AS "a" RETURNING *'
    assert params == ()


def test_delete_returning_method_merges_calls() -> None:
    table = Table("users")
    query, params = (
        delete(table).returning(table.id).returning(table.name).as_tuple()
    )
    assert query == 'DELETE FROM "users" AS "a" RETURNING "a"."id", "a"."name"'
    assert params == ()


def test_delete_with_where_and_returning_expression() -> None:
    table = Table("users")
    query, params = (
        delete(table)
        .where(table.status == "inactive")
        .returning(table.id, func.upper(table.email))
        .as_tuple()
    )
    assert query == (
        'DELETE FROM "users" AS "a" '
        'WHERE "a"."status" = ? '
        'RETURNING "a"."id", UPPER("a"."email")'
    )
    assert params == ("inactive",)


def test_delete_where_only_is_allowed() -> None:
    table = Table("users")
    user_id = 3
    query, params = delete(table).where(table.id == user_id).as_tuple()
    assert query == 'DELETE FROM "users" AS "a" WHERE "a"."id" = ?'
    assert params == (user_id,)
