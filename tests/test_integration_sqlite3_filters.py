"""Integration tests for sqlite3 backend."""

import sqlite3
from collections.abc import Iterator
from typing import Any

import pytest

from sql_fusion import Alias, Table, delete, func, insert, select, update

MIN_USER_AGE = 30
MIN_JOIN_TOTAL = 100
MAX_PENDING_TOTAL = 60
NEW_USER_ID = 6
UPDATED_USER_ID = 2
DELETED_USER_ID = 4


@pytest.fixture
def sqlite_db() -> Iterator[sqlite3.Connection]:
    """Create an in-memory sqlite database for integration tests."""
    connection = sqlite3.connect(":memory:")
    _create_schema(connection)
    _seed_data(connection)

    try:
        yield connection
    finally:
        connection.close()


def _create_schema(connection: sqlite3.Connection) -> None:
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
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            total INTEGER NOT NULL,
            status TEXT NOT NULL
        )
        """,
    )


def _seed_data(connection: sqlite3.Connection) -> None:
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
    connection.commit()


def _fetch_rows(
    connection: sqlite3.Connection,
    sql: str,
    params: tuple[Any, ...],
) -> list[tuple[Any, ...]]:
    return connection.execute(sql, params).fetchall()


def test_complex_user_filter(sqlite_db: sqlite3.Connection) -> None:
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

    rows = _fetch_rows(sqlite_db, query, params)

    assert sorted(rows) == [(1, "Alice"), (5, "Erin")]


def test_complex_join_filter(sqlite_db: sqlite3.Connection) -> None:
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

    rows = _fetch_rows(sqlite_db, query, params)

    assert sorted(rows) == [
        (1, "Alice", 1, 120),
        (1, "Alice", 2, 50),
        (3, "Carol", 3, 200),
    ]


def test_complex_subquery_filter(sqlite_db: sqlite3.Connection) -> None:
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

    rows = _fetch_rows(sqlite_db, query, params)

    assert sorted(rows) == [(1, "Alice"), (3, "Carol")]


def test_insert_user_row(sqlite_db: sqlite3.Connection) -> None:
    users = Table("users")

    query, params = (
        insert(users)
        .values(
            id=NEW_USER_ID,
            name="Frank",
            age=31,
            status="active",
            country="US",
            email="frank@example.com",
        )
        .compile()
    )
    _fetch_rows(sqlite_db, query, params)

    rows = _fetch_rows(
        sqlite_db,
        "SELECT id, name, age, status, country, email FROM users WHERE id = ?",
        (NEW_USER_ID,),
    )

    assert rows == [
        (NEW_USER_ID, "Frank", 31, "active", "US", "frank@example.com"),
    ]


def test_update_user_row(sqlite_db: sqlite3.Connection) -> None:
    users = Table("users")

    query, params = (
        update(users)
        .set(status="active", age=29)
        .where(users.id == UPDATED_USER_ID)
        .compile()
    )
    _fetch_rows(sqlite_db, query, params)

    rows = _fetch_rows(
        sqlite_db,
        "SELECT id, name, age, status FROM users WHERE id = ?",
        (UPDATED_USER_ID,),
    )

    assert rows == [
        (UPDATED_USER_ID, "Bob", 29, "active"),
    ]


def test_update_with_all_binary_expression_operators(
    sqlite_db: sqlite3.Connection,
) -> None:
    sqlite_db.execute(
        """
        CREATE TABLE counters (
            id INTEGER PRIMARY KEY,
            add_value REAL NOT NULL,
            sub_value REAL NOT NULL,
            mul_value REAL NOT NULL,
            div_value REAL NOT NULL
        )
        """,
    )
    sqlite_db.execute(
        "INSERT INTO counters VALUES (?, ?, ?, ?, ?)",
        (1, 10.0, 10.0, 10.0, 10.0),
    )
    sqlite_db.commit()

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
    _fetch_rows(sqlite_db, query, params)

    rows = _fetch_rows(
        sqlite_db,
        (
            "SELECT add_value, sub_value, mul_value, div_value "
            "FROM counters WHERE id = ?"
        ),
        (1,),
    )

    assert rows == [(11.0, 9.0, 20.0, 5.0)]


def test_update_with_subquery_in_set_clause(
    sqlite_db: sqlite3.Connection,
) -> None:
    sqlite_db.execute(
        """
        CREATE TABLE target_rows (
            id INTEGER PRIMARY KEY,
            group_id INTEGER NOT NULL,
            x INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """,
    )
    sqlite_db.execute(
        """
        CREATE TABLE source_rows (
            id INTEGER PRIMARY KEY,
            group_id INTEGER NOT NULL,
            xxx INTEGER NOT NULL
        )
        """,
    )
    sqlite_db.executemany(
        "INSERT INTO target_rows VALUES (?, ?, ?, ?, ?)",
        [
            (1, 10, 0, 1, 1),
            (2, 10, 0, 1, 2),
            (3, 20, 0, 1, 1),
        ],
    )
    sqlite_db.executemany(
        "INSERT INTO source_rows VALUES (?, ?, ?)",
        [
            (1, 10, 7),
            (2, 10, 11),
            (3, 20, 5),
        ],
    )
    sqlite_db.commit()

    target = Table("target_rows")
    source = Table("source_rows")
    subquery = (
        select(func.max(source.xxx))
        .from_(source)
        .where(source.group_id == target.group_id)
    )
    query, params = (
        update(target)
        .set(x=subquery)
        .where(target.created_at == target.updated_at)
        .compile()
    )
    _fetch_rows(sqlite_db, query, params)

    rows = _fetch_rows(
        sqlite_db,
        "SELECT id, x FROM target_rows ORDER BY id",
        (),
    )

    assert rows == [(1, 11), (2, 0), (3, 5)]


def test_update_with_multiple_subqueries_in_set_clause(
    sqlite_db: sqlite3.Connection,
) -> None:
    sqlite_db.execute(
        """
        CREATE TABLE target_rows (
            id INTEGER PRIMARY KEY,
            group_id INTEGER NOT NULL,
            max_x INTEGER NOT NULL,
            source_count INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """,
    )
    sqlite_db.execute(
        """
        CREATE TABLE source_rows (
            id INTEGER PRIMARY KEY,
            group_id INTEGER NOT NULL,
            xxx INTEGER NOT NULL
        )
        """,
    )
    sqlite_db.executemany(
        "INSERT INTO target_rows VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, 10, 0, 0, 1, 1),
            (2, 10, 0, 0, 1, 2),
            (3, 20, 0, 0, 1, 1),
        ],
    )
    sqlite_db.executemany(
        "INSERT INTO source_rows VALUES (?, ?, ?)",
        [
            (1, 10, 7),
            (2, 10, 11),
            (3, 20, 5),
            (4, 20, 8),
        ],
    )
    sqlite_db.commit()

    target = Table("target_rows")
    source = Table("source_rows")
    max_x = (
        select(func.max(source.xxx))
        .from_(source)
        .where(source.group_id == target.group_id)
    )
    source_count = (
        select(func.count("*"))
        .from_(source)
        .where(source.group_id == target.group_id)
    )
    query, params = (
        update(target)
        .set(max_x=max_x, source_count=source_count)
        .where(target.created_at == target.updated_at)
        .compile()
    )
    _fetch_rows(sqlite_db, query, params)

    rows = _fetch_rows(
        sqlite_db,
        "SELECT id, max_x, source_count FROM target_rows ORDER BY id",
        (),
    )

    assert rows == [(1, 11, 2), (2, 0, 0), (3, 8, 2)]


def test_delete_user_row(sqlite_db: sqlite3.Connection) -> None:
    users = Table("users")

    query, params = (
        delete().from_(users).where(users.id == DELETED_USER_ID).compile()
    )
    _fetch_rows(sqlite_db, query, params)

    rows = _fetch_rows(
        sqlite_db,
        "SELECT COUNT(*) FROM users WHERE id = ?",
        (DELETED_USER_ID,),
    )

    assert rows == [(0,)]


def test_grouped_aggregate_query_with_alias_having(
    sqlite_db: sqlite3.Connection,
) -> None:
    """Test aliasing an aggregate and reusing it in HAVING."""
    orders = Table("orders")
    count_orders = Alias("count_orders")

    orders_ge = 3

    query, params = (
        select(
            orders.status,
            func.count(orders.id).as_(count_orders),
            func.sum(orders.total),
        )
        .from_(orders)
        .group_by(orders.status)
        .having(count_orders >= orders_ge)
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)

    assert rows == [("completed", 4, 640)]


def test_grouped_aggregate_query_without_alias_having(
    sqlite_db: sqlite3.Connection,
) -> None:
    """Test the original aggregate example without a named alias."""
    orders = Table("orders")
    orders_ge = 3

    query, params = (
        select(
            orders.status,
            func.count(orders.id),
            func.sum(orders.total),
        )
        .from_(orders)
        .group_by(orders.status)
        .having(func.count(orders.id) >= orders_ge)
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)

    assert rows == [("completed", 4, 640)]
