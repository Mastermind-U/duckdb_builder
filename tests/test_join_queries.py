"""Tests for JOIN queries."""

from sql_fusion import Table, select


def test_inner_join_basic() -> None:
    """Test basic INNER JOIN."""
    users = Table("users")
    orders = Table("orders")
    q = select().from_(users).join(orders, users.id == orders.user_id)
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "users" AS "a" '
        'INNER JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id"'
    )
    assert params == ()


def test_inner_join_with_columns() -> None:
    """Test INNER JOIN with specific columns selected."""
    users = Table("users")
    orders = Table("orders")
    q = (
        select(users.name, orders.total)
        .from_(users)
        .join(orders, users.id == orders.user_id)
    )
    sql, params = q.build_query()
    assert sql == (
        'SELECT "a"."name", "b"."total" '
        'FROM "users" AS "a" '
        'INNER JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id"'
    )
    assert params == ()


def test_inner_join_with_where() -> None:
    """Test INNER JOIN with WHERE clause."""
    users = Table("users")
    orders = Table("orders")
    q = (
        select()
        .from_(users)
        .join(orders, users.id == orders.user_id)
        .where_by(status="completed")
    )
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "users" AS "a" '
        'INNER JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id" '
        'WHERE "a"."status" = ?'
    )
    assert params == ("completed",)


def test_inner_join_with_group_by() -> None:
    """Test INNER JOIN with GROUP BY."""
    users = Table("users")
    orders = Table("orders")
    q = (
        select(users.name, orders.total)
        .from_(users)
        .join(orders, users.id == orders.user_id)
        .group_by(users.name)
    )
    sql, params = q.build_query()
    assert sql == (
        'SELECT "a"."name", "b"."total" '
        'FROM "users" AS "a" '
        'INNER JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id" '
        'GROUP BY "a"."name"'
    )
    assert params == ()


def test_left_join_basic() -> None:
    """Test basic LEFT JOIN."""
    users = Table("users")
    orders = Table("orders")
    q = select().from_(users).left_join(orders, users.id == orders.user_id)
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "users" AS "a" '
        'LEFT JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id"'
    )
    assert params == ()


def test_left_join_with_columns() -> None:
    """Test LEFT JOIN with specific columns selected."""
    users = Table("users")
    orders = Table("orders")
    q = (
        select(users.name, orders.total)
        .from_(users)
        .left_join(orders, users.id == orders.user_id)
    )
    sql, params = q.build_query()
    assert sql == (
        'SELECT "a"."name", "b"."total" '
        'FROM "users" AS "a" '
        'LEFT JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id"'
    )
    assert params == ()


def test_left_join_with_where() -> None:
    """Test LEFT JOIN with WHERE clause."""
    users = Table("users")
    orders = Table("orders")
    q = (
        select()
        .from_(users)
        .left_join(orders, users.id == orders.user_id)
        .where_by(status="active")
    )
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "users" AS "a" '
        'LEFT JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id" '
        'WHERE "a"."status" = ?'
    )
    assert params == ("active",)


def test_right_join_basic() -> None:
    """Test basic RIGHT JOIN."""
    users = Table("users")
    orders = Table("orders")
    q = select().from_(users).right_join(orders, users.id == orders.user_id)
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "users" AS "a" '
        'RIGHT JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id"'
    )
    assert params == ()


def test_right_join_with_where() -> None:
    """Test RIGHT JOIN with WHERE clause."""
    users = Table("users")
    orders = Table("orders")
    q = (
        select()
        .from_(users)
        .right_join(orders, users.id == orders.user_id)
        .where_by(total=100)
    )
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "users" AS "a" '
        'RIGHT JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id" '
        'WHERE "a"."total" = ?'
    )
    assert params == (100,)


def test_full_outer_join_basic() -> None:
    """Test basic FULL OUTER JOIN."""
    users = Table("users")
    orders = Table("orders")
    q = select().from_(users).full_join(orders, users.id == orders.user_id)
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "users" AS "a" '
        'FULL OUTER JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id"'
    )
    assert params == ()


def test_full_outer_join_with_columns() -> None:
    """Test FULL OUTER JOIN with specific columns."""
    users = Table("users")
    orders = Table("orders")
    q = (
        select(users.name, orders.amount)
        .from_(users)
        .full_join(orders, users.id == orders.user_id)
    )
    sql, params = q.build_query()
    assert sql == (
        'SELECT "a"."name", "b"."amount" '
        'FROM "users" AS "a" '
        'FULL OUTER JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id"'
    )
    assert params == ()


def test_cross_join_basic() -> None:
    """Test basic CROSS JOIN (cartesian product)."""
    table_a = Table("table_a")
    table_b = Table("table_b")
    q = select().from_(table_a).cross_join(table_b)
    sql, params = q.build_query()
    assert sql == (
        'SELECT * FROM "table_a" AS "a" CROSS JOIN "table_b" AS "b"'
    )
    assert params == ()


def test_cross_join_with_columns() -> None:
    """Test CROSS JOIN with specific columns."""
    table_a = Table("table_a")
    table_b = Table("table_b")
    q = select(table_a.id, table_b.value).from_(table_a).cross_join(table_b)
    sql, params = q.build_query()
    assert sql == (
        'SELECT "a"."id", "b"."value" '
        'FROM "table_a" AS "a" '
        'CROSS JOIN "table_b" AS "b"'
    )
    assert params == ()


def test_cross_join_with_where() -> None:
    """Test CROSS JOIN with WHERE clause."""
    table_a = Table("table_a")
    table_b = Table("table_b")
    q = (
        select()
        .from_(table_a)
        .cross_join(table_b)
        .where_by(category="premium")
    )
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "table_a" AS "a" '
        'CROSS JOIN "table_b" AS "b" '
        'WHERE "a"."category" = ?'
    )
    assert params == ("premium",)


def test_semi_join_basic() -> None:
    """Test basic SEMI JOIN (exists check)."""
    city_airport = Table("city_airport")
    airport_names = Table("airport_names")
    q = (
        select()
        .from_(city_airport)
        .semi_join(airport_names, city_airport.iata == airport_names.iata)
    )
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "city_airport" AS "a" '
        'SEMI JOIN "airport_names" AS "b" '
        'ON "a"."iata" = "b"."iata"'
    )
    assert params == ()


def test_semi_join_with_columns() -> None:
    """Test SEMI JOIN with specific columns."""
    city_airport = Table("city_airport")
    airport_names = Table("airport_names")
    q = (
        select(city_airport.city, city_airport.iata)
        .from_(city_airport)
        .semi_join(airport_names, city_airport.iata == airport_names.iata)
    )
    sql, params = q.build_query()
    assert sql == (
        'SELECT "a"."city", "a"."iata" '
        'FROM "city_airport" AS "a" '
        'SEMI JOIN "airport_names" AS "b" '
        'ON "a"."iata" = "b"."iata"'
    )
    assert params == ()


def test_semi_join_with_where() -> None:
    """Test SEMI JOIN with WHERE clause."""
    city_airport = Table("city_airport")
    airport_names = Table("airport_names")
    q = (
        select()
        .from_(city_airport)
        .semi_join(airport_names, city_airport.iata == airport_names.iata)
        .where_by(country="USA")
    )
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "city_airport" AS "a" '
        'SEMI JOIN "airport_names" AS "b" '
        'ON "a"."iata" = "b"."iata" '
        'WHERE "a"."country" = ?'
    )
    assert params == ("USA",)


def test_anti_join_basic() -> None:
    """Test basic ANTI JOIN (not exists check)."""
    city_airport = Table("city_airport")
    airport_names = Table("airport_names")
    q = (
        select()
        .from_(city_airport)
        .anti_join(airport_names, city_airport.iata == airport_names.iata)
    )
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "city_airport" AS "a" '
        'ANTI JOIN "airport_names" AS "b" '
        'ON "a"."iata" = "b"."iata"'
    )
    assert params == ()


def test_anti_join_with_columns() -> None:
    """Test ANTI JOIN with specific columns."""
    city_airport = Table("city_airport")
    airport_names = Table("airport_names")
    q = (
        select(city_airport.city, city_airport.iata)
        .from_(city_airport)
        .anti_join(airport_names, city_airport.iata == airport_names.iata)
    )
    sql, params = q.build_query()
    assert sql == (
        'SELECT "a"."city", "a"."iata" '
        'FROM "city_airport" AS "a" '
        'ANTI JOIN "airport_names" AS "b" '
        'ON "a"."iata" = "b"."iata"'
    )
    assert params == ()


def test_anti_join_with_where() -> None:
    """Test ANTI JOIN with WHERE clause."""
    city_airport = Table("city_airport")
    airport_names = Table("airport_names")
    q = (
        select()
        .from_(city_airport)
        .anti_join(airport_names, city_airport.iata == airport_names.iata)
        .where_by(status="inactive")
    )
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "city_airport" AS "a" '
        'ANTI JOIN "airport_names" AS "b" '
        'ON "a"."iata" = "b"."iata" '
        'WHERE "a"."status" = ?'
    )
    assert params == ("inactive",)


def test_multiple_joins_inner_left() -> None:
    """Test chaining INNER JOIN and LEFT JOIN."""
    users = Table("users")
    orders = Table("orders")
    items = Table("items")
    q = (
        select()
        .from_(users)
        .join(orders, users.id == orders.user_id)
        .left_join(items, orders.id == items.order_id)
    )
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "users" AS "a" '
        'INNER JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id" '
        'LEFT JOIN "items" AS "c" '
        'ON "b"."id" = "c"."order_id"'
    )
    assert params == ()


def test_multiple_joins_with_where_and_group_by() -> None:
    """Test multiple JOINs with WHERE and GROUP BY."""
    users = Table("users")
    orders = Table("orders")
    payments = Table("payments")
    q = (
        select(users.name)
        .from_(users)
        .join(orders, users.id == orders.user_id)
        .join(payments, orders.id == payments.order_id)
        .where_by(status="completed")
        .group_by(users.name)
    )
    sql, params = q.build_query()
    assert sql == (
        'SELECT "a"."name" '
        'FROM "users" AS "a" '
        'INNER JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id" '
        'INNER JOIN "payments" AS "c" '
        'ON "b"."id" = "c"."order_id" '
        'WHERE "a"."status" = ? '
        'GROUP BY "a"."name"'
    )
    assert params == ("completed",)


def test_join_semi_anti_combination() -> None:
    """Test combining INNER, SEMI, and ANTI JOINs."""
    table_a = Table("table_a")
    table_b = Table("table_b")
    table_c = Table("table_c")
    table_d = Table("table_d")
    q = (
        select()
        .from_(table_a)
        .join(table_b, table_a.id == table_b.a_id)
        .semi_join(table_c, table_a.status == table_c.status)
        .anti_join(table_d, table_a.id == table_d.a_id)
    )
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "table_a" AS "a" '
        'INNER JOIN "table_b" AS "b" '
        'ON "a"."id" = "b"."a_id" '
        'SEMI JOIN "table_c" AS "c" '
        'ON "a"."status" = "c"."status" '
        'ANTI JOIN "table_d" AS "d" '
        'ON "a"."id" = "d"."a_id"'
    )
    assert params == ()


def test_cross_join_with_inner_join() -> None:
    """Test CROSS JOIN combined with INNER JOIN."""
    table_a = Table("table_a")
    table_b = Table("table_b")
    table_c = Table("table_c")
    q = (
        select()
        .from_(table_a)
        .cross_join(table_b)
        .join(table_c, table_a.id == table_c.a_id)
    )
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "table_a" AS "a" '
        'CROSS JOIN "table_b" AS "b" '
        'INNER JOIN "table_c" AS "c" '
        'ON "a"."id" = "c"."a_id"'
    )
    assert params == ()


def test_join_with_not_equal_operator() -> None:
    """Test JOIN with != operator."""
    users = Table("users")
    banned = Table("banned_users")
    q = select().from_(users).join(banned, users.id != banned.user_id)
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "users" AS "a" '
        'INNER JOIN "banned_users" AS "b" '
        'ON "a"."id" != "b"."user_id"'
    )
    assert params == ()


def test_join_with_greater_than_operator() -> None:
    """Test JOIN with > operator."""
    orders1 = Table("orders1")
    orders2 = Table("orders2")
    q = select().from_(orders1).join(orders2, orders1.amount > orders2.amount)
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "orders1" AS "a" '
        'INNER JOIN "orders2" AS "b" '
        'ON "a"."amount" > "b"."amount"'
    )
    assert params == ()


def test_join_with_less_than_or_equal_operator() -> None:
    """Test JOIN with <= operator."""
    prices1 = Table("prices1")
    prices2 = Table("prices2")
    q = select().from_(prices1).join(prices2, prices1.price <= prices2.price)
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "prices1" AS "a" '
        'INNER JOIN "prices2" AS "b" '
        'ON "a"."price" <= "b"."price"'
    )
    assert params == ()


def test_join_with_multiple_conditions() -> None:
    """Test JOIN with multiple ON conditions (AND)."""
    orders = Table("orders")
    invoices = Table("invoices")
    condition = (orders.id == invoices.order_id) & (
        orders.customer_id == invoices.customer_id
    )
    q = select().from_(orders).join(invoices, condition)
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "orders" AS "a" '
        'INNER JOIN "invoices" AS "b" '
        'ON ("a"."id" = "b"."order_id" '
        'AND "a"."customer_id" = "b"."customer_id")'
    )
    assert params == ()


def test_join_with_or_conditions() -> None:
    """Test JOIN with OR conditions."""
    table_a = Table("table_a")
    table_b = Table("table_b")
    condition = (table_a.id == table_b.primary_id) | (
        table_a.id == table_b.secondary_id
    )
    q = select().from_(table_a).join(table_b, condition)
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "table_a" AS "a" '
        'INNER JOIN "table_b" AS "b" '
        'ON ("a"."id" = "b"."primary_id" OR "a"."id" = "b"."secondary_id")'
    )
    assert params == ()


def test_join_with_where_parameterized() -> None:
    """Test JOIN followed by WHERE with parameterized values."""
    users = Table("users")
    orders = Table("orders")
    age = 18
    q = (
        select()
        .from_(users)
        .join(orders, users.id == orders.user_id)
        .where(users.age > age)
        .where_by(status="active", country="USA")
    )
    sql, params = q.build_query()
    assert sql == (
        "SELECT * "
        'FROM "users" AS "a" '
        'INNER JOIN "orders" AS "b" '
        'ON "a"."id" = "b"."user_id" '
        'WHERE ("a"."age" > ? AND ("a"."status" = ? AND "a"."country" = ?))'
    )
    assert params == (18, "active", "USA")


def test_self_join_uses_generated_aliases() -> None:
    """Test joining the same table name twice gets distinct aliases."""
    left = Table("xxx")
    right = Table("xxx")

    q = (
        select(left.id, right.id)
        .from_(left)
        .join(right, left.parent_id == right.id)
    )
    sql, params = q.build_query()

    assert sql == (
        'SELECT "a"."id", "b"."id" '
        'FROM "xxx" AS "a" '
        'INNER JOIN "xxx" AS "b" '
        'ON "a"."parent_id" = "b"."id"'
    )
    assert params == ()
