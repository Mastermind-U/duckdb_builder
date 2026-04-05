"""Tests for SQL function calls."""

from sql_fusion import Table, func, select


def test_func_sum_single_column() -> None:
    """Test SUM aggregate function."""
    users = Table("users")
    q = select(func.sum(users.age)).from_(users)
    sql, params = q.build_query()
    assert sql == ('SELECT SUM("a"."age") FROM "users" AS "a"')
    assert params == ()


def test_func_count_all() -> None:
    """Test COUNT(*) function."""
    users = Table("users")
    q = select(func.count("*")).from_(users)
    sql, params = q.build_query()
    assert sql == ('SELECT COUNT(*) FROM "users" AS "a"')
    assert params == ()


def test_func_count_column() -> None:
    """Test COUNT with column."""
    users = Table("users")
    q = select(func.count(users.id)).from_(users)
    sql, params = q.build_query()
    assert sql == ('SELECT COUNT("a"."id") FROM "users" AS "a"')
    assert params == ()


def test_func_max() -> None:
    """Test MAX function."""
    orders = Table("orders")
    q = select(func.max(orders.total)).from_(orders)
    sql, params = q.build_query()
    assert sql == ('SELECT MAX("a"."total") FROM "orders" AS "a"')
    assert params == ()


def test_func_min() -> None:
    """Test MIN function."""
    orders = Table("orders")
    q = select(func.min(orders.total)).from_(orders)
    sql, params = q.build_query()
    assert sql == ('SELECT MIN("a"."total") FROM "orders" AS "a"')
    assert params == ()


def test_func_avg() -> None:
    """Test AVG function."""
    orders = Table("orders")
    q = select(func.avg(orders.total)).from_(orders)
    sql, params = q.build_query()
    assert sql == ('SELECT AVG("a"."total") FROM "orders" AS "a"')
    assert params == ()


def test_multiple_aggregate_functions() -> None:
    """Test multiple aggregate functions in SELECT."""
    orders = Table("orders")
    q = select(
        func.count(orders.id),
        func.sum(orders.total),
        func.avg(orders.total),
    ).from_(orders)
    sql, params = q.build_query()
    assert sql == (
        'SELECT COUNT("a"."id"), SUM("a"."total"), AVG("a"."total") '
        'FROM "orders" AS "a"'
    )
    assert params == ()


def test_aggregate_with_regular_column() -> None:
    """Test mixing regular columns with aggregate functions."""
    orders = Table("orders")
    q = select(orders.customer_id, func.sum(orders.total)).from_(orders)
    sql, params = q.build_query()
    assert sql == (
        'SELECT "a"."customer_id", SUM("a"."total") FROM "orders" AS "a"'
    )
    assert params == ()


def test_aggregate_with_group_by() -> None:
    """Test aggregate function with GROUP BY."""
    orders = Table("orders")
    q = (
        select(orders.customer_id, func.sum(orders.total))
        .from_(orders)
        .group_by(orders.customer_id)
    )
    sql, params = q.build_query()
    assert sql == (
        'SELECT "a"."customer_id", SUM("a"."total") '
        'FROM "orders" AS "a" '
        'GROUP BY "a"."customer_id"'
    )
    assert params == ()


def test_aggregate_with_group_by_multiple_cols() -> None:
    """Test aggregate with GROUP BY multiple columns."""
    orders = Table("orders")
    q = (
        select(
            orders.customer_id,
            orders.status,
            func.count(orders.id),
            func.sum(orders.total),
        )
        .from_(orders)
        .group_by(orders.customer_id, orders.status)
    )
    sql, params = q.build_query()
    assert sql == (
        'SELECT "a"."customer_id", "a"."status", '
        'COUNT("a"."id"), SUM("a"."total") '
        'FROM "orders" AS "a" '
        'GROUP BY "a"."customer_id", "a"."status"'
    )
    assert params == ()


def test_aggregate_with_having() -> None:
    """Test aggregate function with HAVING clause."""
    orders = Table("orders")
    orders_num = 5
    q = (
        select(orders.customer_id, func.count(orders.id))
        .from_(orders)
        .group_by(orders.customer_id)
        .having(func.count(orders.id) > orders_num)
    )
    sql, params = q.build_query()
    assert sql == (
        'SELECT "a"."customer_id", COUNT("a"."id") '
        'FROM "orders" AS "a" '
        'GROUP BY "a"."customer_id" '
        'HAVING COUNT("a"."id") > ?'
    )
    assert params == (5,)


def test_aggregate_example_without_alias() -> None:
    """Test the original README example without an explicit alias."""
    orders = Table("orders")
    orders_min = 3

    q = (
        select(
            orders.status,
            func.count(orders.id),
            func.sum(orders.total),
        )
        .from_(orders)
        .group_by(orders.status)
        .having(func.count(orders.id) >= orders_min)
    )

    sql, params = q.build_query()

    assert sql == (
        'SELECT "a"."status", COUNT("a"."id"), SUM("a"."total") '
        'FROM "orders" AS "a" '
        'GROUP BY "a"."status" '
        'HAVING COUNT("a"."id") >= ?'
    )
    assert params == (3,)


def test_custom_function() -> None:
    """Test custom user-defined function."""
    users = Table("users")
    q = select(func.my_custom_func(users.name)).from_(users)
    sql, params = q.build_query()
    assert sql == ('SELECT MY_CUSTOM_FUNC("a"."name") FROM "users" AS "a"')
    assert params == ()


def test_custom_function_multiple_args() -> None:
    """Test custom function with multiple arguments."""
    users = Table("users")
    q = select(func.concat(users.first_name, users.last_name)).from_(users)
    sql, params = q.build_query()
    assert sql == (
        'SELECT CONCAT("a"."first_name", "a"."last_name") FROM "users" AS "a"'
    )
    assert params == ()


def test_custom_function_with_literals() -> None:
    """Test custom function with literal arguments."""
    users = Table("users")
    q = select(func.coalesce(users.email, "no-email@example.com")).from_(users)
    sql, params = q.build_query()
    assert sql == ('SELECT COALESCE("a"."email", ?) FROM "users" AS "a"')
    assert params == ("no-email@example.com",)


def test_custom_function_with_numeric_literal() -> None:
    """Test custom function with numeric literal."""
    orders = Table("orders")
    q = select(func.round(orders.total, 2)).from_(orders)
    sql, params = q.build_query()
    assert sql == ('SELECT ROUND("a"."total", ?) FROM "orders" AS "a"')
    assert params == (2,)


def test_aggregate_with_where() -> None:
    """Test aggregate functions with WHERE clause."""
    orders = Table("orders")
    q = (
        select(func.sum(orders.total))
        .from_(orders)
        .where_by(status="completed")
    )
    sql, params = q.build_query()
    assert sql == (
        'SELECT SUM("a"."total") FROM "orders" AS "a" WHERE "a"."status" = ?'
    )
    assert params == ("completed",)


def test_aggregate_with_where_and_group_by() -> None:
    """Test aggregate with WHERE and GROUP BY."""
    orders = Table("orders")
    q = (
        select(orders.customer_id, func.sum(orders.total))
        .from_(orders)
        .where_by(status="completed")
        .group_by(orders.customer_id)
    )
    sql, params = q.build_query()
    assert sql == (
        'SELECT "a"."customer_id", SUM("a"."total") '
        'FROM "orders" AS "a" '
        'WHERE "a"."status" = ? '
        'GROUP BY "a"."customer_id"'
    )
    assert params == ("completed",)


def test_aggregate_with_join() -> None:
    """Test aggregate function with JOIN."""
    orders = Table("orders")
    items = Table("items")
    q = (
        select(orders.id, func.count(items.id))
        .from_(orders)
        .join(items, orders.id == items.order_id)
    )
    sql, params = q.build_query()
    assert sql == (
        'SELECT "a"."id", COUNT("b"."id") '
        'FROM "orders" AS "a" '
        'INNER JOIN "items" AS "b" ON "a"."id" = "b"."order_id"'
    )
    assert params == ()


def test_aggregate_with_multiple_joins() -> None:
    """Test aggregate with multiple JOINs."""
    users = Table("users")
    orders = Table("orders")
    items = Table("items")
    q = (
        select(users.name, func.sum(items.price))
        .from_(users)
        .join(orders, users.id == orders.user_id)
        .join(items, orders.id == items.order_id)
    )
    sql, params = q.build_query()
    assert sql == (
        'SELECT "a"."name", SUM("c"."price") '
        'FROM "users" AS "a" '
        'INNER JOIN "orders" AS "b" ON "a"."id" = "b"."user_id" '
        'INNER JOIN "items" AS "c" ON "b"."id" = "c"."order_id"'
    )
    assert params == ()


def test_complex_aggregate_query() -> None:
    """Test complex query with aggregates, JOIN, WHERE, GROUP BY, HAVING."""
    users = Table("users")
    orders = Table("orders")
    orders_num = 3
    q = (
        select(users.name, func.count(orders.id), func.sum(orders.total))
        .from_(users)
        .join(orders, users.id == orders.user_id)
        .where_by(status="active")
        .group_by(users.name)
        .having(func.count(orders.id) >= orders_num)
    )
    sql, params = q.build_query()
    assert sql == (
        'SELECT "a"."name", COUNT("b"."id"), SUM("b"."total") '
        'FROM "users" AS "a" '
        'INNER JOIN "orders" AS "b" ON "a"."id" = "b"."user_id" '
        'WHERE "a"."status" = ? '
        'GROUP BY "a"."name" '
        'HAVING COUNT("b"."id") >= ?'
    )
    assert params == ("active", 3)


def test_aggregate_with_limit() -> None:
    """Test aggregate with LIMIT."""
    users = Table("users")
    orders = Table("orders")
    q = (
        select(users.id, func.count(orders.id))
        .from_(users)
        .join(orders, users.id == orders.user_id)
        .group_by(users.id)
        .limit(10)
    )
    sql, params = q.build_query()
    assert sql == (
        'SELECT "a"."id", COUNT("b"."id") '
        'FROM "users" AS "a" '
        'INNER JOIN "orders" AS "b" ON "a"."id" = "b"."user_id" '
        'GROUP BY "a"."id" '
        "LIMIT 10"
    )
    assert params == ()


def test_aggregate_with_distinct() -> None:
    """Test aggregate with DISTINCT."""
    orders = Table("orders")
    q = select(func.count(orders.id)).from_(orders).distinct()
    sql, params = q.build_query()
    assert sql == ('SELECT DISTINCT COUNT("a"."id") FROM "orders" AS "a"')
    assert params == ()


def test_nested_function_calls() -> None:
    """Test nested function calls."""
    users = Table("users")
    q = select(func.upper(func.trim(users.name))).from_(users)
    sql, params = q.build_query()
    assert sql == ('SELECT UPPER(TRIM("a"."name")) FROM "users" AS "a"')
    assert params == ()


def test_deeply_nested_functions() -> None:
    """Test deeply nested functions."""
    users = Table("users")
    q = select(func.round(func.avg(func.cast(users.age, "float")), 2)).from_(
        users,
    )
    sql, params = q.build_query()
    assert (
        sql == 'SELECT ROUND(AVG(CAST("a"."age", ?)), ?) FROM "users" AS "a"'
    )
    assert params == ("float", 2)


def test_nested_with_multiple_args() -> None:
    """Test nested functions with multiple arguments."""
    orders = Table("orders")
    q = select(func.substring(orders.description, 1, 10)).from_(orders)
    sql, params = q.build_query()
    assert (
        sql == 'SELECT SUBSTRING("a"."description", ?, ?) FROM "orders" AS "a"'
    )
    assert params == (1, 10)


def test_lowercase_function_name_uppercase_in_sql() -> None:
    """Test that lowercase function names are uppercased in SQL."""
    users = Table("users")
    q = select(func.upper(users.name)).from_(users)
    sql, _params = q.build_query()
    assert "UPPER(" in sql
    assert "upper(" not in sql


def test_mixed_case_function_name() -> None:
    """Test mixed case function names."""
    users = Table("users")
    q = select(func.MyCustomFunc(users.id)).from_(users)
    sql, _params = q.build_query()
    assert "MYCUSTOMFUNC(" in sql


def test_function_with_no_args() -> None:
    """Test function with no arguments."""
    users = Table("users")
    q = select(func.now()).from_(users)
    sql, params = q.build_query()
    assert sql == ('SELECT NOW() FROM "users" AS "a"')
    assert params == ()


def test_function_with_only_literals() -> None:
    """Test function with only literal arguments."""
    users = Table("users")
    q = select(func.concat("Hello", "World")).from_(users)
    sql, params = q.build_query()
    assert sql == ('SELECT CONCAT(?, ?) FROM "users" AS "a"')
    assert params == ("Hello", "World")


def test_function_preserves_parameter_order() -> None:
    """Test that function preserves parameter order."""
    users = Table("users")
    q = select(func.concat(users.first_name, ".", users.last_name)).from_(
        users,
    )
    sql, params = q.build_query()
    assert 'CONCAT("a"."first_name", ?, "a"."last_name")' in sql
    assert params == (".",)
