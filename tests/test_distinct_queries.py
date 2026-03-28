"""Tests for DISTINCT clause."""

from duckdb_builder.composite_table import Table
from duckdb_builder.query import select

# ============================================================================
# Basic DISTINCT Tests
# ============================================================================


def test_distinct_all_columns() -> None:
    """Test DISTINCT with all columns."""
    users = Table("users")
    q = select(users).distinct()
    sql, params = q.build_query()

    assert sql == ('SELECT DISTINCT * FROM "users" AS "a"')
    assert params == ()


def test_distinct_single_column() -> None:
    """Test DISTINCT with single column."""
    users = Table("users")
    q = select(users, users.category).distinct()
    sql, params = q.build_query()

    assert sql == ('SELECT DISTINCT "a"."category" FROM "users" AS "a"')
    assert params == ()


def test_distinct_multiple_columns() -> None:
    """Test DISTINCT with multiple columns."""
    users = Table("users")
    q = select(users, users.name, users.email).distinct()
    sql, params = q.build_query()

    assert sql == (
        'SELECT DISTINCT "a"."name", "a"."email" FROM "users" AS "a"'
    )
    assert params == ()


def test_distinct_three_columns() -> None:
    """Test DISTINCT with three columns."""
    users = Table("users")
    q = select(
        users,
        users.first_name,
        users.last_name,
        users.email,
    ).distinct()
    sql, params = q.build_query()

    assert sql == (
        "SELECT DISTINCT "
        '"a"."first_name", "a"."last_name", "a"."email" '
        'FROM "users" AS "a"'
    )
    assert params == ()


# ============================================================================
# DISTINCT with JOINs Tests
# ============================================================================


def test_distinct_with_inner_join() -> None:
    """Test DISTINCT with INNER JOIN."""
    users = Table("users")
    orders = Table("orders")
    q = (
        select(users, users.name)
        .join(orders, users.id == orders.user_id)
        .distinct()
    )
    sql, params = q.build_query()

    assert sql == (
        'SELECT DISTINCT "a"."name" '
        'FROM "users" AS "a" '
        'INNER JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id"'
    )
    assert params == ()


def test_distinct_with_left_join() -> None:
    """Test DISTINCT with LEFT JOIN."""
    users = Table("users")
    orders = Table("orders")
    q = (
        select(users, users.name, orders.total)
        .left_join(orders, users.id == orders.user_id)
        .distinct()
    )
    sql, params = q.build_query()

    assert sql == (
        'SELECT DISTINCT "a"."name", "b"."total" '
        'FROM "users" AS "a" '
        'LEFT JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id"'
    )
    assert params == ()


def test_distinct_with_multiple_joins() -> None:
    """Test DISTINCT with multiple JOINs."""
    users = Table("users")
    orders = Table("orders")
    items = Table("items")
    q = (
        select(users, users.name)
        .join(orders, users.id == orders.user_id)
        .join(items, orders.id == items.order_id)
        .distinct()
    )
    sql, params = q.build_query()

    assert sql == (
        'SELECT DISTINCT "a"."name" '
        'FROM "users" AS "a" '
        'INNER JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id" '
        'INNER JOIN "items" AS "c" '
        'ON "b"."id" = "c"."order_id"'
    )
    assert params == ()


def test_distinct_with_cross_join() -> None:
    """Test DISTINCT with CROSS JOIN."""
    table_a = Table("table_a")
    table_b = Table("table_b")
    q = select(table_a, table_a.id).cross_join(table_b).distinct()
    sql, params = q.build_query()

    assert sql == (
        'SELECT DISTINCT "a"."id" '
        'FROM "table_a" AS "a" '
        'CROSS JOIN "table_b" AS "b"'
    )
    assert params == ()


# ============================================================================
# DISTINCT with LIMIT/OFFSET Tests
# ============================================================================


def test_distinct_with_limit() -> None:
    """Test DISTINCT with LIMIT clause."""
    users = Table("users")
    q = select(users, users.city).distinct().limit(5)
    sql, params = q.build_query()

    assert sql == ('SELECT DISTINCT "a"."city" FROM "users" AS "a" LIMIT 5')
    assert params == ()


def test_distinct_with_offset() -> None:
    """Test DISTINCT with OFFSET clause."""
    users = Table("users")
    q = select(users, users.country).distinct().offset(10)
    sql, params = q.build_query()

    assert sql == (
        'SELECT DISTINCT "a"."country" FROM "users" AS "a" OFFSET 10'
    )
    assert params == ()


def test_distinct_with_limit_and_offset() -> None:
    """Test DISTINCT with LIMIT and OFFSET clauses."""
    users = Table("users")
    q = select(users, users.category).distinct().limit(10).offset(5)
    sql, params = q.build_query()

    assert sql == (
        'SELECT DISTINCT "a"."category" FROM "users" AS "a" LIMIT 10 OFFSET 5'
    )
    assert params == ()


def test_distinct_with_offset_then_limit() -> None:
    """Test DISTINCT with OFFSET then LIMIT (order shouldn't matter)."""
    users = Table("users")
    q = select(users, users.status).distinct().offset(20).limit(15)
    sql, params = q.build_query()

    assert sql == (
        'SELECT DISTINCT "a"."status" FROM "users" AS "a" LIMIT 15 OFFSET 20'
    )
    assert params == ()


# ============================================================================
# DISTINCT with GROUP BY Tests
# ============================================================================


def test_distinct_with_group_by() -> None:
    """Test DISTINCT with GROUP BY clause."""
    users = Table("users")
    q = select(users, users.category).group_by(users.category).distinct()
    sql, params = q.build_query()

    assert sql == (
        'SELECT DISTINCT "a"."category" '
        'FROM "users" AS "a" '
        'GROUP BY "a"."category"'
    )
    assert params == ()


def test_distinct_with_group_by_rollup() -> None:
    """Test DISTINCT with GROUP BY ROLLUP."""
    users = Table("users")
    q = select(users, users.region).group_by_rollup(users.region).distinct()
    sql, params = q.build_query()

    assert sql == (
        'SELECT DISTINCT "a"."region" '
        'FROM "users" AS "a" '
        'GROUP BY ROLLUP ("a"."region")'
    )
    assert params == ()


def test_distinct_with_group_by_and_having() -> None:
    """Test DISTINCT with GROUP BY and HAVING clauses."""
    users = Table("users")
    uid = 5
    q = (
        select(users, users.category)
        .group_by(users.category)
        .having(users.id > uid)
        .distinct()
    )
    sql, params = q.build_query()

    assert sql == (
        'SELECT DISTINCT "a"."category" '
        'FROM "users" AS "a" '
        'GROUP BY "a"."category" '
        'HAVING "a"."id" > ?'
    )
    assert params == (5,)


# ============================================================================
# Complex DISTINCT Queries Tests
# ============================================================================


def test_distinct_with_join_and_limit() -> None:
    """Test DISTINCT with JOIN and LIMIT."""
    users = Table("users")
    orders = Table("orders")
    q = (
        select(users, users.name)
        .join(orders, users.id == orders.user_id)
        .distinct()
        .limit(10)
    )
    sql, params = q.build_query()

    assert sql == (
        'SELECT DISTINCT "a"."name" '
        'FROM "users" AS "a" '
        'INNER JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id" '
        "LIMIT 10"
    )
    assert params == ()


def test_distinct_with_multiple_joins_and_limit_offset() -> None:
    """Test DISTINCT with multiple JOINs, LIMIT, and OFFSET."""
    users = Table("users")
    orders = Table("orders")
    items = Table("items")
    q = (
        select(users, users.name, items.product_id)
        .join(orders, users.id == orders.user_id)
        .join(items, orders.id == items.order_id)
        .distinct()
        .limit(20)
        .offset(10)
    )
    sql, params = q.build_query()

    assert sql == (
        'SELECT DISTINCT "a"."name", "c"."product_id" '
        'FROM "users" AS "a" '
        'INNER JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id" '
        'INNER JOIN "items" AS "c" '
        'ON "b"."id" = "c"."order_id" '
        "LIMIT 20 "
        "OFFSET 10"
    )
    assert params == ()


def test_distinct_with_semi_join() -> None:
    """Test DISTINCT with SEMI JOIN."""
    city_airport = Table("city_airport")
    airport_names = Table("airport_names")
    q = (
        select(city_airport, city_airport.city)
        .semi_join(
            airport_names,
            city_airport.iata == airport_names.iata,
        )
        .distinct()
    )
    sql, params = q.build_query()

    assert sql == (
        'SELECT DISTINCT "a"."city" '
        'FROM "city_airport" AS "a" '
        'SEMI JOIN "airport_names" AS "b" '
        'ON "a"."iata" = "b"."iata"'
    )
    assert params == ()


def test_distinct_with_anti_join() -> None:
    """Test DISTINCT with ANTI JOIN."""
    city_airport = Table("city_airport")
    airport_names = Table("airport_names")
    q = (
        select(city_airport, city_airport.city)
        .anti_join(
            airport_names,
            city_airport.iata == airport_names.iata,
        )
        .distinct()
    )
    sql, params = q.build_query()

    assert sql == (
        'SELECT DISTINCT "a"."city" '
        'FROM "city_airport" AS "a" '
        'ANTI JOIN "airport_names" AS "b" '
        'ON "a"."iata" = "b"."iata"'
    )
    assert params == ()


# ============================================================================
# DISTINCT Method Chaining Tests
# ============================================================================


def test_distinct_can_be_called_before_limit() -> None:
    """Test that distinct() can be called before limit()."""
    users = Table("users")
    q = select(users, users.name).distinct().limit(5)
    sql, _params = q.build_query()

    assert "DISTINCT" in sql
    assert "LIMIT 5" in sql


def test_distinct_can_be_called_after_join() -> None:
    """Test that distinct() can be called after join()."""
    users = Table("users")
    orders = Table("orders")
    q = select(users).join(orders, users.id == orders.user_id).distinct()
    sql, _params = q.build_query()

    assert "DISTINCT" in sql
    assert "INNER JOIN" in sql


def test_distinct_idempotent() -> None:
    """Test that calling distinct() multiple times works correctly."""
    users = Table("users")
    q = select(users).distinct().distinct()
    sql, _params = q.build_query()

    # Should still have DISTINCT (only appears once in SQL)
    assert sql.count("DISTINCT") == 1


# ============================================================================
# DISTINCT with Parameterized Values Tests
# ============================================================================


def test_distinct_preserves_parameters() -> None:
    """Test that DISTINCT doesn't affect parameterization."""
    users = Table("users")
    q = select(users, users.name, users.status).distinct()
    sql, params = q.build_query()

    assert sql == (
        'SELECT DISTINCT "a"."name", "a"."status" FROM "users" AS "a"'
    )
    assert params == ()


# ============================================================================
# Edge Cases Tests
# ============================================================================


def test_distinct_with_star_and_join() -> None:
    """Test DISTINCT * with JOIN."""
    users = Table("users")
    orders = Table("orders")
    q = select(users).join(orders, users.id == orders.user_id).distinct()
    sql, params = q.build_query()

    assert sql == (
        "SELECT DISTINCT * "
        'FROM "users" AS "a" '
        'INNER JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id"'
    )
    assert params == ()


def test_distinct_pagination_scenario() -> None:
    """Test DISTINCT with pagination scenario."""
    users = Table("users")
    q = select(users, users.city).distinct().limit(50).offset(100)
    sql, _params = q.build_query()

    assert "DISTINCT" in sql
    assert "LIMIT 50" in sql
    assert "OFFSET 100" in sql
