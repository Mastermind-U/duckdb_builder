"""Integration tests for duckdb backend, fully compatible with Postgres."""

from collections.abc import Iterator
from typing import Any

import pytest

from sql_fusion import Table, delete, func, insert, select, update

MIN_USER_AGE = 30
MIN_JOIN_TOTAL = 100
MAX_PENDING_TOTAL = 60
NEW_USER_ID = 6
UPDATED_USER_ID = 2
DELETED_USER_ID = 4


@pytest.fixture
def duckdb_db() -> Iterator[Any]:
    """Create an in-memory duckdb database for integration tests."""
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect(":memory:")
    _create_schema(connection)
    _seed_data(connection)

    try:
        yield connection
    finally:
        connection.close()


def _create_schema(connection: Any) -> None:
    connection.execute(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            status TEXT NOT NULL,
            country TEXT NOT NULL,
            email TEXT NOT NULL
        )
        """,
    )
    connection.execute(
        """
        CREATE TABLE join_left (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL
        )
        """,
    )
    connection.execute(
        """
        CREATE TABLE join_right (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL
        )
        """,
    )
    connection.execute(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            total INTEGER NOT NULL,
            status TEXT NOT NULL
        )
        """,
    )


def _seed_data(connection: Any) -> None:
    connection.executemany(
        "INSERT INTO users VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, "Alice", 34, "active", "US", "alice@example.com"),
            (2, "Bob", 28, "inactive", "CA", "bob@example.com"),
            (3, "Carol", 41, "active", "US", "carol@example.com"),
            (4, "Dave", 25, "inactive", "DE", "dave@example.com"),
            (5, "Erin", 36, "pending", "US", "erin@foo.com"),
        ],
    )
    connection.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?)",
        [
            (1, 1, 120, "completed"),
            (2, 1, 50, "pending"),
            (3, 3, 200, "completed"),
            (4, 4, 80, "cancelled"),
            (5, 5, 300, "completed"),
            (6, 2, 20, "completed"),
        ],
    )
    connection.executemany(
        "INSERT INTO join_left VALUES (?, ?)",
        [
            (1, "L1"),
            (2, "L2"),
            (3, "L3"),
        ],
    )
    connection.executemany(
        "INSERT INTO join_right VALUES (?, ?)",
        [
            (2, "R2"),
            (3, "R3"),
            (4, "R4"),
        ],
    )
    _create_indexes(connection)
    connection.commit()


def _create_indexes(connection: Any) -> None:
    connection.execute(
        "CREATE INDEX idx_users_status ON users(status)",
    )
    connection.execute(
        "CREATE INDEX idx_users_country ON users(country)",
    )
    connection.execute(
        "CREATE INDEX idx_orders_user_id ON orders(user_id)",
    )
    connection.execute(
        "CREATE INDEX idx_orders_status ON orders(status)",
    )


def _fetch_rows(
    connection: Any,
    sql: str,
    params: tuple[Any, ...],
) -> list[tuple[Any, ...]]:
    return connection.execute(sql, params).fetchall()


def _sorted_rows(rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    return sorted(rows, key=repr)


def _order_by_second_column_desc_limit_two(
    sql: str,
    params: tuple[Any, ...],
) -> tuple[str, tuple[Any, ...]]:
    return f"{sql} ORDER BY 2 DESC LIMIT 2", params


def test_complex_user_filter(duckdb_db: Any) -> None:
    users = Table("users")
    query, params = (
        select(users.id, users.name)
        .from_(users)
        .where(
            (users.age >= MIN_USER_AGE)
            & ((users.status == "active") | (users.status == "pending"))
            & users.country.not_in(["DE", "FR"]),
        )
        .where(
            users.name.like("A%") | users.email.like("%@foo.com"),
        )
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert sorted(rows) == [(1, "Alice"), (5, "Erin")]


def test_like_filter(duckdb_db: Any) -> None:
    users = Table("users")
    query, params = (
        select(users.name).from_(users).where(users.name.like("A%")).compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert rows == [("Alice",)]


def test_ilike_filter(duckdb_db: Any) -> None:
    users = Table("users")
    query, params = (
        select(users.name).from_(users).where(users.name.ilike("a%")).compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert rows == [("Alice",)]


def test_ilike_suffix_filter(duckdb_db: Any) -> None:
    users = Table("users")
    query, params = (
        select(users.email)
        .from_(users)
        .where(users.email.ilike("%@foo.com"))
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert rows == [("erin@foo.com",)]


def test_ilike_contains_filter(duckdb_db: Any) -> None:
    users = Table("users")
    query, params = (
        select(users.email)
        .from_(users)
        .where(users.email.ilike("%example%"))
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert _sorted_rows(rows) == [
        ("alice@example.com",),
        ("bob@example.com",),
        ("carol@example.com",),
        ("dave@example.com",),
    ]


def test_in_filter(duckdb_db: Any) -> None:
    users = Table("users")
    query, params = (
        select(users.name)
        .from_(users)
        .where(users.country.in_(["US", "CA"]))
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert _sorted_rows(rows) == [
        ("Alice",),
        ("Bob",),
        ("Carol",),
        ("Erin",),
    ]


def test_not_in_filter(duckdb_db: Any) -> None:
    users = Table("users")
    query, params = (
        select(users.name)
        .from_(users)
        .where(users.country.not_in(["US", "CA"]))
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert rows == [("Dave",)]


def test_complex_join_filter(duckdb_db: Any) -> None:
    users = Table("users")
    orders = Table("orders")
    query, params = (
        select(users.id, users.name, orders.id, orders.total)
        .from_(users)
        .join(orders, users.id == orders.user_id)
        .where_by(status="active")
        .where(users.country.in_(["US", "CA"]))
        .where(
            ((orders.status == "completed") & (orders.total >= MIN_JOIN_TOTAL))
            | (
                (orders.status == "pending")
                & (orders.total < MAX_PENDING_TOTAL)
            ),
        )
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert sorted(rows) == [
        (1, "Alice", 1, 120),
        (1, "Alice", 2, 50),
        (3, "Carol", 3, 200),
    ]


def test_complex_subquery_filter(duckdb_db: Any) -> None:
    users = Table("users")
    orders = Table("orders")

    completed_users = (
        select(orders.user_id).from_(orders).where_by(status="completed")
    )
    cancelled_users = (
        select(orders.user_id).from_(orders).where_by(status="cancelled")
    )

    query, params = (
        select(users.id, users.name)
        .from_(users)
        .where_by(status="active")
        .where(users.id.in_(completed_users))
        .where(users.id.not_in(cancelled_users))
        .where((users.age >= MIN_USER_AGE) & users.country.in_(["US", "CA"]))
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert sorted(rows) == [(1, "Alice"), (3, "Carol")]


def test_group_by_with_aggregation(duckdb_db: Any) -> None:
    orders = Table("orders")
    query, params = (
        select(
            orders.status,
            func.count(orders.id),
            func.sum(orders.total),
        )
        .from_(orders)
        .group_by(orders.status)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert sorted(rows) == [
        ("cancelled", 1, 80),
        ("completed", 4, 640),
        ("pending", 1, 50),
    ]


def test_inner_join(duckdb_db: Any) -> None:
    left = Table("join_left")
    right = Table("join_right")
    query, params = (
        select(left.id, left.label, right.label)
        .from_(left)
        .join(right, left.id == right.id)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert _sorted_rows(rows) == [
        (2, "L2", "R2"),
        (3, "L3", "R3"),
    ]


def test_left_join(duckdb_db: Any) -> None:
    left = Table("join_left")
    right = Table("join_right")
    query, params = (
        select(left.id, left.label, right.label)
        .from_(left)
        .left_join(right, left.id == right.id)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert _sorted_rows(rows) == [
        (1, "L1", None),
        (2, "L2", "R2"),
        (3, "L3", "R3"),
    ]


def test_right_join(duckdb_db: Any) -> None:
    left = Table("join_left")
    right = Table("join_right")
    query, params = (
        select(left.id, left.label, right.label)
        .from_(left)
        .right_join(right, left.id == right.id)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert _sorted_rows(rows) == [
        (2, "L2", "R2"),
        (3, "L3", "R3"),
        (None, None, "R4"),
    ]


def test_full_join(duckdb_db: Any) -> None:
    left = Table("join_left")
    right = Table("join_right")
    query, params = (
        select(left.id, left.label, right.label)
        .from_(left)
        .full_join(right, left.id == right.id)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert _sorted_rows(rows) == [
        (1, "L1", None),
        (2, "L2", "R2"),
        (3, "L3", "R3"),
        (None, None, "R4"),
    ]


def test_cross_join(duckdb_db: Any) -> None:
    left = Table("join_left")
    right = Table("join_right")
    query, params = (
        select(left.id, right.id).from_(left).cross_join(right).compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert _sorted_rows(rows) == [
        (1, 2),
        (1, 3),
        (1, 4),
        (2, 2),
        (2, 3),
        (2, 4),
        (3, 2),
        (3, 3),
        (3, 4),
    ]


def test_semi_join(duckdb_db: Any) -> None:
    left = Table("join_left")
    right = Table("join_right")
    query, params = (
        select(left.id, left.label)
        .from_(left)
        .semi_join(right, left.id == right.id)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert _sorted_rows(rows) == [
        (2, "L2"),
        (3, "L3"),
    ]


def test_anti_join(duckdb_db: Any) -> None:
    left = Table("join_left")
    right = Table("join_right")
    query, params = (
        select(left.id, left.label)
        .from_(left)
        .anti_join(right, left.id == right.id)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert _sorted_rows(rows) == [
        (1, "L1"),
    ]


def test_cte_with_join_and_aggregation(duckdb_db: Any) -> None:
    orders = Table("orders")
    users = Table("users")
    paid_orders = Table("paid_orders")

    paid_orders_cte = (
        select(orders.user_id, orders.total)
        .from_(orders)
        .where_by(status="completed")
    )

    query, params = (
        select(users.name, func.sum(paid_orders.total))
        .with_(paid_orders=paid_orders_cte)
        .from_(paid_orders)
        .join(users, paid_orders.user_id == users.id)
        .group_by(users.name)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert _sorted_rows(rows) == [
        ("Alice", 120),
        ("Bob", 20),
        ("Carol", 200),
        ("Erin", 300),
    ]


def test_cte_with_custom_compile_expression(duckdb_db: Any) -> None:
    orders = Table("orders")
    completed_users = Table("completed_users")
    users = Table("users")

    completed_users_cte = (
        select(orders.user_id).from_(orders).where_by(status="completed")
    )

    query, params = (
        select(users.name, users.age)
        .with_(completed_users=completed_users_cte)
        .from_(completed_users)
        .join(users, completed_users.user_id == users.id)
        .compile_expression(_order_by_second_column_desc_limit_two)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert rows == [
        ("Carol", 41),
        ("Erin", 36),
    ]


def test_custom_compile_expression_with_aggregation(duckdb_db: Any) -> None:
    orders = Table("orders")
    query, params = (
        select(orders.status, func.sum(orders.total))
        .from_(orders)
        .group_by(orders.status)
        .compile_expression(_order_by_second_column_desc_limit_two)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert rows == [
        ("completed", 640),
        ("cancelled", 80),
    ]


def test_insert_user_row(duckdb_db: Any) -> None:
    users = Table("users")

    query, params = (
        insert(users)
        .values(
            id=6,
            name="Frank",
            age=31,
            status="active",
            country="US",
            email="frank@example.com",
        )
        .compile()
    )
    _fetch_rows(duckdb_db, query, params)

    rows = _fetch_rows(
        duckdb_db,
        "SELECT id, name, age, status, country, email FROM users WHERE id = ?",
        (NEW_USER_ID,),
    )

    assert rows == [
        (NEW_USER_ID, "Frank", 31, "active", "US", "frank@example.com"),
    ]


def test_update_user_row(duckdb_db: Any) -> None:
    users = Table("users")

    query, params = (
        update(users)
        .set(status="active", age=29)
        .where(users.id == UPDATED_USER_ID)
        .compile()
    )
    _fetch_rows(duckdb_db, query, params)

    rows = _fetch_rows(
        duckdb_db,
        "SELECT id, name, age, status FROM users WHERE id = ?",
        (UPDATED_USER_ID,),
    )

    assert rows == [
        (UPDATED_USER_ID, "Bob", 29, "active"),
    ]


def test_update_with_all_binary_expression_operators(
    duckdb_db: Any,
) -> None:
    duckdb_db.execute(
        """
        CREATE TABLE counters (
            id INTEGER PRIMARY KEY,
            add_value DOUBLE NOT NULL,
            sub_value DOUBLE NOT NULL,
            mul_value DOUBLE NOT NULL,
            div_value DOUBLE NOT NULL
        )
        """,
    )
    duckdb_db.execute(
        "INSERT INTO counters VALUES (?, ?, ?, ?, ?)",
        (1, 10.0, 10.0, 10.0, 10.0),
    )

    counters = Table("counters")
    query, params = (
        update(counters)
        .set(
            add_value=counters.add_value + 1,
            sub_value=counters.sub_value - 1,
            mul_value=counters.mul_value * 2,
            div_value=counters.div_value / 2,
        )
        .where(
            (1 + counters.add_value == 11)
            & (20 - counters.sub_value == 10)
            & (2 * counters.mul_value == 20)
            & (100 / counters.div_value == 10),
        )
        .compile()
    )
    _fetch_rows(duckdb_db, query, params)

    rows = _fetch_rows(
        duckdb_db,
        (
            "SELECT add_value, sub_value, mul_value, div_value "
            "FROM counters WHERE id = ?"
        ),
        (1,),
    )

    assert rows == [(11.0, 9.0, 20.0, 5.0)]


def test_delete_user_row(duckdb_db: Any) -> None:
    users = Table("users")

    query, params = (
        delete().from_(users).where(users.id == DELETED_USER_ID).compile()
    )
    _fetch_rows(duckdb_db, query, params)

    rows = _fetch_rows(
        duckdb_db,
        "SELECT COUNT(*) FROM users WHERE id = ?",
        (DELETED_USER_ID,),
    )

    assert rows == [(0,)]
