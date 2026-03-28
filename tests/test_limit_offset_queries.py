"""Tests for LIMIT and OFFSET clauses."""

import pytest

from duckdb_builder.composite_table import Table
from duckdb_builder.query import select


def test_limit_basic() -> None:
    """Test basic LIMIT clause."""
    users = Table("users")
    q = select().from_(users).limit(10)
    sql, params = q.build_query()
    assert sql == 'SELECT * FROM "users" AS "a" LIMIT 10'
    assert params == ()


def test_limit_with_columns() -> None:
    """Test LIMIT with specific columns."""
    users = Table("users")
    q = select(users.name, users.email).from_(users).limit(5)
    sql, params = q.build_query()
    assert sql == 'SELECT "a"."name", "a"."email" FROM "users" AS "a" LIMIT 5'
    assert params == ()


def test_limit_with_where() -> None:
    """Test LIMIT with WHERE clause."""
    users = Table("users")
    q = select().from_(users).where_by(status="active").limit(20)
    sql, params = q.build_query()
    assert (
        sql == 'SELECT * FROM "users" AS "a" WHERE "a"."status" = ? LIMIT 20'
    )
    assert params == ("active",)


def test_limit_with_group_by() -> None:
    """Test LIMIT with GROUP BY clause."""
    users = Table("users")
    q = select(users.category).from_(users).group_by(users.category).limit(3)
    sql, params = q.build_query()
    assert (
        sql
        == 'SELECT "a"."category" FROM "users" AS "a" GROUP BY "a"."category" LIMIT 3'
    )
    assert params == ()


def test_limit_zero() -> None:
    """Test LIMIT 0 is allowed."""
    users = Table("users")
    q = select().from_(users).limit(0)
    sql, params = q.build_query()
    assert sql == 'SELECT * FROM "users" AS "a" LIMIT 0'
    assert params == ()


def test_limit_negative_raises_error() -> None:
    """Test that negative LIMIT raises ValueError."""
    users = Table("users")
    with pytest.raises(ValueError, match="LIMIT must be non-negative"):
        select().from_(users).limit(-1)


def test_offset_basic() -> None:
    """Test basic OFFSET clause."""
    users = Table("users")
    q = select().from_(users).offset(10)
    sql, params = q.build_query()
    assert sql == 'SELECT * FROM "users" AS "a" OFFSET 10'
    assert params == ()


def test_offset_with_columns() -> None:
    """Test OFFSET with specific columns."""
    users = Table("users")
    q = select(users.id, users.name).from_(users).offset(50)
    sql, params = q.build_query()
    assert sql == 'SELECT "a"."id", "a"."name" FROM "users" AS "a" OFFSET 50'
    assert params == ()


def test_offset_with_where() -> None:
    """Test OFFSET with WHERE clause."""
    users = Table("users")
    q = select().from_(users).where_by(score=100).offset(100)
    sql, params = q.build_query()
    assert (
        sql == 'SELECT * FROM "users" AS "a" WHERE "a"."score" = ? OFFSET 100'
    )
    assert params == (100,)


def test_offset_zero() -> None:
    """Test OFFSET 0 is allowed."""
    users = Table("users")
    q = select().from_(users).offset(0)
    sql, params = q.build_query()
    assert sql == 'SELECT * FROM "users" AS "a" OFFSET 0'
    assert params == ()


def test_offset_negative_raises_error() -> None:
    """Test that negative OFFSET raises ValueError."""
    users = Table("users")
    with pytest.raises(ValueError, match="OFFSET must be non-negative"):
        select().from_(users).offset(-10)


def test_limit_and_offset() -> None:
    """Test LIMIT with OFFSET clause."""
    users = Table("users")
    q = select().from_(users).limit(10).offset(5)
    sql, params = q.build_query()
    assert sql == 'SELECT * FROM "users" AS "a" LIMIT 10 OFFSET 5'
    assert params == ()


def test_offset_then_limit() -> None:
    """Test that order doesn't matter (offset then limit)."""
    users = Table("users")
    q = select().from_(users).offset(10).limit(20)
    sql, params = q.build_query()
    assert sql == 'SELECT * FROM "users" AS "a" LIMIT 20 OFFSET 10'
    assert params == ()


def test_limit_offset_with_where() -> None:
    """Test LIMIT and OFFSET with WHERE clause."""
    users = Table("users")
    q = select().from_(users).where_by(status="active").limit(10).offset(5)
    sql, params = q.build_query()
    assert (
        sql
        == 'SELECT * FROM "users" AS "a" WHERE "a"."status" = ? LIMIT 10 OFFSET 5'
    )
    assert params == ("active",)


def test_limit_offset_with_group_by() -> None:
    """Test LIMIT and OFFSET with GROUP BY clause."""
    orders = Table("orders")
    q = (
        select(orders.customer_id)
        .from_(orders)
        .group_by(orders.customer_id)
        .limit(15)
        .offset(3)
    )
    sql, params = q.build_query()
    assert (
        sql
        == 'SELECT "a"."customer_id" FROM "orders" AS "a" GROUP BY "a"."customer_id" LIMIT 15 OFFSET 3'
    )
    assert params == ()


def test_limit_offset_with_having() -> None:
    """Test LIMIT and OFFSET with HAVING clause."""
    orders = Table("orders")
    q = (
        select(orders.customer_id)
        .from_(orders)
        .group_by(orders.customer_id)
        .having(orders.customer_id == "ABC123")
        .limit(5)
        .offset(2)
    )
    sql, params = q.build_query()
    assert (
        sql
        == 'SELECT "a"."customer_id" FROM "orders" AS "a" GROUP BY "a"."customer_id" HAVING "a"."customer_id" = ? LIMIT 5 OFFSET 2'
    )
    assert params == ("ABC123",)


def test_limit_offset_with_join() -> None:
    """Test LIMIT and OFFSET with JOIN clause."""
    users = Table("users")
    orders = Table("orders")
    q = (
        select(users.name, orders.total)
        .from_(users)
        .join(orders, users.id == orders.user_id)
        .limit(10)
        .offset(5)
    )
    sql, params = q.build_query()
    assert (
        sql
        == 'SELECT "a"."name", "b"."total" FROM "users" AS "a" INNER JOIN "orders" AS "b" ON "a"."id" = "b"."user_id" LIMIT 10 OFFSET 5'
    )
    assert params == ()


def test_limit_offset_with_multiple_joins() -> None:
    """Test LIMIT and OFFSET with multiple JOINs."""
    users = Table("users")
    orders = Table("orders")
    items = Table("items")
    q = (
        select()
        .from_(users)
        .join(orders, users.id == orders.user_id)
        .join(items, orders.id == items.order_id)
        .limit(8)
        .offset(16)
    )
    sql, params = q.build_query()
    assert (
        sql
        == 'SELECT * FROM "users" AS "a" INNER JOIN "orders" AS "b" ON "a"."id" = "b"."user_id" INNER JOIN "items" AS "c" ON "b"."id" = "c"."order_id" LIMIT 8 OFFSET 16'
    )
    assert params == ()


def test_complex_query_with_limit_offset() -> None:
    """Test complex query with JOIN, WHERE, GROUP BY, LIMIT, OFFSET."""
    users = Table("users")
    orders = Table("orders")
    q = (
        select(users.name, orders.total)
        .from_(users)
        .join(orders, users.id == orders.user_id)
        .where_by(status="completed")
        .group_by(users.name)
        .limit(5)
        .offset(10)
    )
    sql, params = q.build_query()
    assert (
        sql
        == 'SELECT "a"."name", "b"."total" FROM "users" AS "a" INNER JOIN "orders" AS "b" ON "a"."id" = "b"."user_id" WHERE "a"."status" = ? GROUP BY "a"."name" LIMIT 5 OFFSET 10'
    )
    assert params == ("completed",)


def test_pagination_first_page() -> None:
    """Test pagination - first page with limit 20."""
    users = Table("users")
    q = select().from_(users).limit(20).offset(0)
    sql, params = q.build_query()
    assert sql == 'SELECT * FROM "users" AS "a" LIMIT 20 OFFSET 0'
    assert params == ()


def test_pagination_second_page() -> None:
    """Test pagination - second page with limit 20."""
    users = Table("users")
    q = select().from_(users).limit(20).offset(20)
    sql, params = q.build_query()
    assert sql == 'SELECT * FROM "users" AS "a" LIMIT 20 OFFSET 20'
    assert params == ()


def test_pagination_third_page() -> None:
    """Test pagination - third page with limit 20."""
    users = Table("users")
    q = select().from_(users).limit(20).offset(40)
    sql, params = q.build_query()
    assert sql == 'SELECT * FROM "users" AS "a" LIMIT 20 OFFSET 40'
    assert params == ()


def test_large_limit_and_offset() -> None:
    """Test large LIMIT and OFFSET values."""
    users = Table("users")
    q = select().from_(users).limit(1000000).offset(9999999)
    sql, params = q.build_query()
    assert sql == 'SELECT * FROM "users" AS "a" LIMIT 1000000 OFFSET 9999999'
    assert params == ()
