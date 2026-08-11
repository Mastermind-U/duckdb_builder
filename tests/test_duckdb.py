"""Integration tests for duckdb backend, fully compatible with Postgres."""

from collections.abc import Iterator
from typing import Any

import pytest

from sql_fusion import (
    Table,
    Window,
    delete,
    func,
    insert,
    select,
    text_op,
    union,
    update,
)

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


def test_postgres_style_array_contains_filter(duckdb_db: Any) -> None:
    duckdb_db.execute(
        """
        CREATE TABLE user_tags (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            tags TEXT[] NOT NULL
        )
        """,
    )
    duckdb_db.executemany(
        "INSERT INTO user_tags VALUES (?, ?, ?)",
        [
            (1, "Alice", ["coffee", "music"]),
            (2, "Bob", ["tea"]),
            (3, "Carol", ["coffee", "books"]),
        ],
    )

    users = Table("user_tags")
    query, params = (
        select(users.name)
        .from_(users)
        .where(text_op(users.tags, "@>", ["coffee"]))
        .order_by(users.name)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert rows == [("Alice",), ("Carol",)]


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


def test_self_join_with_generated_aliases(duckdb_db: Any) -> None:
    duckdb_db.execute(
        """
        CREATE TABLE xxx (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER
        )
        """,
    )
    duckdb_db.executemany(
        "INSERT INTO xxx VALUES (?, ?)",
        [
            (1, None),
            (2, 1),
            (3, 1),
        ],
    )

    left = Table("xxx")
    right = Table("xxx")
    query, params = (
        select(left.id, right.id)
        .from_(left)
        .join(right, left.parent_id == right.id)
        .order_by(left.id)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert rows == [(2, 1), (3, 1)]


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


def test_update_with_subquery_in_set_clause(duckdb_db: Any) -> None:
    duckdb_db.execute(
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
    duckdb_db.execute(
        """
        CREATE TABLE source_rows (
            id INTEGER PRIMARY KEY,
            group_id INTEGER NOT NULL,
            xxx INTEGER NOT NULL
        )
        """,
    )
    duckdb_db.executemany(
        "INSERT INTO target_rows VALUES (?, ?, ?, ?, ?)",
        [
            (1, 10, 0, 1, 1),
            (2, 10, 0, 1, 2),
            (3, 20, 0, 1, 1),
        ],
    )
    duckdb_db.executemany(
        "INSERT INTO source_rows VALUES (?, ?, ?)",
        [
            (1, 10, 7),
            (2, 10, 11),
            (3, 20, 5),
        ],
    )

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
    _fetch_rows(duckdb_db, query, params)

    rows = _fetch_rows(
        duckdb_db,
        *select(target.id, target.x)
        .from_(target)
        .order_by(target.id)
        .compile(),
    )

    assert rows == [(1, 11), (2, 0), (3, 5)]


def test_update_with_multiple_subqueries_in_set_clause(
    duckdb_db: Any,
) -> None:
    duckdb_db.execute(
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
    duckdb_db.execute(
        """
        CREATE TABLE source_rows (
            id INTEGER PRIMARY KEY,
            group_id INTEGER NOT NULL,
            xxx INTEGER NOT NULL
        )
        """,
    )
    duckdb_db.executemany(
        "INSERT INTO target_rows VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, 10, 0, 0, 1, 1),
            (2, 10, 0, 0, 1, 2),
            (3, 20, 0, 0, 1, 1),
        ],
    )
    duckdb_db.executemany(
        "INSERT INTO source_rows VALUES (?, ?, ?)",
        [
            (1, 10, 7),
            (2, 10, 11),
            (3, 20, 5),
            (4, 20, 8),
        ],
    )

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
    _fetch_rows(duckdb_db, query, params)

    rows = _fetch_rows(
        duckdb_db,
        *(
            select(target.id, target.max_x, target.source_count)
            .from_(target)
            .order_by(target.id)
            .compile()
        ),
    )

    assert rows == [(1, 11, 2), (2, 0, 0), (3, 8, 2)]


def test_update_with_ordered_subquery_in_set_clause(duckdb_db: Any) -> None:
    duckdb_db.execute(
        """
        CREATE TABLE activity_users (
            id INTEGER PRIMARY KEY,
            last_activity TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """,
    )
    duckdb_db.execute(
        """
        CREATE TABLE posts (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
        """,
    )
    duckdb_db.executemany(
        "INSERT INTO activity_users VALUES (?, ?, ?, ?)",
        [
            (
                1,
                "2000-01-01 00:00:00",
                "2024-01-01 00:00:00",
                "2024-01-01 00:00:00",
            ),
            (
                2,
                "2000-01-01 00:00:00",
                "2024-01-02 00:00:00",
                "2024-01-02 00:00:00",
            ),
            (
                3,
                "2000-01-01 00:00:00",
                "2024-01-03 00:00:00",
                "2024-01-04 00:00:00",
            ),
        ],
    )
    duckdb_db.executemany(
        "INSERT INTO posts VALUES (?, ?, ?)",
        [
            (1, 1, "2024-01-05 10:00:00"),
            (2, 2, "2024-01-03 09:00:00"),
            (3, 1, "2024-01-01 08:00:00"),
        ],
    )

    users = Table("activity_users")
    posts = Table("posts")
    subquery = (
        select(posts.created_at)
        .from_(posts)
        .order_by(posts.created_at)
        .limit(1)
    )
    query, params = (
        update(users)
        .set(last_activity=subquery)
        .where(users.created_at == users.updated_at)
        .compile()
    )
    _fetch_rows(duckdb_db, query, params)

    rows = _fetch_rows(
        duckdb_db,
        *select(users.id, users.last_activity)
        .from_(users)
        .order_by(users.id)
        .compile(),
    )

    assert rows == [
        (1, "2024-01-01 08:00:00"),
        (2, "2024-01-01 08:00:00"),
        (3, "2000-01-01 00:00:00"),
    ]


def test_union_all_by_name(duckdb_db: Any) -> None:
    duckdb_db.execute(
        """
        CREATE TABLE union_left (
            id INTEGER NOT NULL,
            name TEXT NOT NULL
        )
        """,
    )
    duckdb_db.execute(
        """
        CREATE TABLE union_right (
            id INTEGER NOT NULL,
            name TEXT NOT NULL
        )
        """,
    )
    duckdb_db.executemany(
        "INSERT INTO union_left VALUES (?, ?)",
        [
            (1, "Alice"),
        ],
    )
    duckdb_db.executemany(
        "INSERT INTO union_right VALUES (?, ?)",
        [
            (2, "Bob"),
        ],
    )

    left = Table("union_left")
    right = Table("union_right")
    left_query = select(left.id, left.name).from_(left)
    right_query = select(right.name, right.id).from_(right)

    query, params = union(
        left_query,
        right_query,
        all_=True,
        by_name=True,
    ).compile()

    rows = _fetch_rows(duckdb_db, query, params)

    assert rows == [(1, "Alice"), (2, "Bob")]


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


def test_duckdb_complex_window_query(duckdb_db: Any) -> None:
    orders = Table("orders")
    users = Table("users")
    ranked_orders = Table(
        select(
            orders.user_id,
            orders.id,
            orders.total,
            func.rank().over("user_total_order").as_("total_rank"),
            func.sum(orders.total)
            .over("user_running_id")
            .as_(
                "running_total",
            ),
            func.count("*")
            .over(partition_by=orders.user_id)
            .as_(
                "order_count",
            ),
        )
        .from_(orders)
        .window(
            "user_total_order",
            partition_by=orders.user_id,
            order_by=orders.total,
            descending=True,
        )
        .window(
            "user_running_id",
            partition_by=orders.user_id,
            order_by=orders.id,
            rows=Window.Rows.between(
                Window.Rows.unbounded_preceding(),
                Window.Rows.current_row(),
            ),
        ),
    )

    query, params = (
        select(
            users.name,
            ranked_orders.id,
            ranked_orders.total,
            ranked_orders.total_rank,
            ranked_orders.running_total,
            ranked_orders.order_count,
        )
        .from_(ranked_orders)
        .join(users, ranked_orders.user_id == users.id)
        .where(
            (users.status == "active") | (ranked_orders.order_count > 1),
        )
        .where(ranked_orders.total_rank <= 2)
        .order_by(users.name, ranked_orders.total_rank, ranked_orders.id)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)

    assert rows == [
        ("Alice", 1, 120, 1, 120, 2),
        ("Alice", 2, 50, 2, 170, 2),
        ("Carol", 3, 200, 1, 200, 1),
    ]


def _create_generation_history(connection: Any) -> None:
    connection.execute(
        """
        CREATE TABLE "Generation History" (
            "MWh" INTEGER NOT NULL,
            "Date" DATE NOT NULL,
            "Plant" TEXT NOT NULL
        )
        """,
    )
    connection.executemany(
        'INSERT INTO "Generation History" VALUES (?, ?, ?)',
        [
            (564337, "2019-01-02", "Boston"),
            (507405, "2019-01-03", "Boston"),
            (528523, "2019-01-04", "Boston"),
            (469538, "2019-01-05", "Boston"),
            (474163, "2019-01-06", "Boston"),
            (507213, "2019-01-07", "Boston"),
            (613040, "2019-01-08", "Boston"),
            (582588, "2019-01-09", "Boston"),
            (499506, "2019-01-10", "Boston"),
            (482014, "2019-01-11", "Boston"),
            (486134, "2019-01-12", "Boston"),
            (531518, "2019-01-13", "Boston"),
            (118860, "2019-01-02", "Worcester"),
            (101977, "2019-01-03", "Worcester"),
            (106054, "2019-01-04", "Worcester"),
            (92182, "2019-01-05", "Worcester"),
            (94492, "2019-01-06", "Worcester"),
            (99932, "2019-01-07", "Worcester"),
            (118854, "2019-01-08", "Worcester"),
            (113506, "2019-01-09", "Worcester"),
            (96644, "2019-01-10", "Worcester"),
            (93806, "2019-01-11", "Worcester"),
            (98963, "2019-01-12", "Worcester"),
            (107170, "2019-01-13", "Worcester"),
        ],
    )


def test_duckdb_docs_generation_history_row_number(duckdb_db: Any) -> None:
    _create_generation_history(duckdb_db)
    history = Table("Generation History")

    query, params = (
        select(
            history.Plant,
            history.Date,
            func.row_number()
            .over(partition_by=history.Plant, order_by=history.Date)
            .as_("Row"),
        )
        .from_(history)
        .order_by(history.Plant, history.Date)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)
    expected = _fetch_rows(
        duckdb_db,
        """
        SELECT
            "Plant",
            "Date",
            row_number() OVER (
                PARTITION BY "Plant" ORDER BY "Date"
            ) AS "Row"
        FROM "Generation History"
        ORDER BY 1, 2
        """,
        (),
    )

    assert rows == expected


def test_duckdb_docs_rows_between_window(duckdb_db: Any) -> None:
    duckdb_db.execute(
        """
        CREATE TABLE results (
            points INTEGER NOT NULL
        )
        """,
    )
    duckdb_db.executemany(
        "INSERT INTO results VALUES (?)",
        [(10,), (20,), (30,), (40,)],
    )
    results = Table("results")

    query, params = (
        select(
            results.points,
            func.sum(results.points)
            .over(
                rows=Window.Rows.between(
                    Window.Rows.preceding(1),
                    Window.Rows.following(1),
                ),
            )
            .as_("we"),
        )
        .from_(results)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)
    expected = _fetch_rows(
        duckdb_db,
        """
        SELECT points,
            sum(points) OVER (
                ROWS BETWEEN 1 PRECEDING
                         AND 1 FOLLOWING) AS we
        FROM results
        """,
        (),
    )

    assert rows == expected


def test_duckdb_docs_range_window_exclude_current_row(
    duckdb_db: Any,
) -> None:
    duckdb_db.execute(
        """
        CREATE TABLE results (
            event TEXT NOT NULL,
            date DATE NOT NULL,
            athlete TEXT NOT NULL,
            time DOUBLE NOT NULL
        )
        """,
    )
    duckdb_db.executemany(
        "INSERT INTO results VALUES (?, ?, ?, ?)",
        [
            ("100m", "2024-01-01", "Alice", 12.0),
            ("100m", "2024-01-05", "Bob", 11.5),
            ("100m", "2024-01-20", "Carol", 11.0),
            ("200m", "2024-01-03", "Alice", 24.0),
            ("200m", "2024-01-11", "Bob", 23.0),
        ],
    )
    results = Table("results")

    query, params = (
        select(
            results.event,
            results.date,
            results.athlete,
            func.avg(results.time).over("w").as_("recent"),
        )
        .from_(results)
        .window(
            "w",
            partition_by=results.event,
            order_by=results.date,
            range_=Window.Range.between(
                Window.Range.interval_preceding(10, "DAYS"),
                Window.Range.interval_following(10, "DAYS"),
            ),
            exclude="CURRENT ROW",
        )
        .order_by(results.event, results.date, results.athlete)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)
    expected = _fetch_rows(
        duckdb_db,
        """
        SELECT
            event,
            date,
            athlete,
            avg(time) OVER w AS recent,
        FROM results
        WINDOW w AS (
            PARTITION BY event
            ORDER BY date
            RANGE BETWEEN INTERVAL 10 DAYS PRECEDING
                      AND INTERVAL 10 DAYS FOLLOWING
                EXCLUDE CURRENT ROW
        )
        ORDER BY event, date, athlete
        """,
        (),
    )

    assert rows == expected


def test_duckdb_docs_generation_history_range_framing_average(
    duckdb_db: Any,
) -> None:
    _create_generation_history(duckdb_db)
    history = Table("Generation History")

    query, params = (
        select(
            history.Plant,
            history.Date,
            func.avg(history.MWh)
            .over(
                partition_by=history.Plant,
                order_by=history.Date,
                range_=Window.Range.between(
                    Window.Range.interval_preceding(3, "DAYS"),
                    Window.Range.interval_following(3, "DAYS"),
                ),
            )
            .as_("MWh 7-day Moving Average"),
        )
        .from_(history)
        .order_by(history.Plant, history.Date)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)
    expected = _fetch_rows(
        duckdb_db,
        """
        SELECT "Plant", "Date",
            avg("MWh") OVER (
                PARTITION BY "Plant"
                ORDER BY "Date" ASC
                RANGE BETWEEN INTERVAL 3 DAYS PRECEDING
                          AND INTERVAL 3 DAYS FOLLOWING)
                AS "MWh 7-day Moving Average"
        FROM "Generation History"
        ORDER BY 1, 2
        """,
        (),
    )

    assert rows == expected


def test_duckdb_docs_generation_history_groups_framing(
    duckdb_db: Any,
) -> None:
    _create_generation_history(duckdb_db)
    history = Table("Generation History")

    query, params = (
        select(
            history.Date,
            history.Plant,
            func.avg(history.MWh)
            .over(
                order_by=history.Date,
                groups=Window.Groups.between(
                    Window.Groups.preceding(3),
                    Window.Groups.following(3),
                ),
            )
            .as_("MWh 7-day Moving Average"),
        )
        .from_(history)
        .order_by(history.Date, history.Plant)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)
    expected = _fetch_rows(
        duckdb_db,
        """
        SELECT "Date", "Plant",
            avg("MWh") OVER (
                ORDER BY "Date" ASC
                GROUPS BETWEEN 3 PRECEDING
                       AND 3 FOLLOWING)
                AS "MWh 7-day Moving Average"
        FROM "Generation History"
        ORDER BY 1, 2
        """,
        (),
    )

    assert rows == expected


def test_duckdb_docs_generation_history_seven_day_windows(
    duckdb_db: Any,
) -> None:
    _create_generation_history(duckdb_db)
    history = Table("Generation History")

    query, params = (
        select(
            history.Plant,
            history.Date,
            func.min(history.MWh)
            .over("seven")
            .as_(
                "MWh 7-day Moving Minimum",
            ),
            func.avg(history.MWh)
            .over("seven")
            .as_(
                "MWh 7-day Moving Average",
            ),
            func.max(history.MWh)
            .over("seven")
            .as_(
                "MWh 7-day Moving Maximum",
            ),
        )
        .from_(history)
        .window(
            "seven",
            partition_by=history.Plant,
            order_by=history.Date,
            range_=Window.Range.between(
                Window.Range.interval_preceding(3, "DAYS"),
                Window.Range.interval_following(3, "DAYS"),
            ),
        )
        .order_by(history.Plant, history.Date)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)
    expected = _fetch_rows(
        duckdb_db,
        """
        SELECT "Plant", "Date",
            min("MWh") OVER seven AS "MWh 7-day Moving Minimum",
            avg("MWh") OVER seven AS "MWh 7-day Moving Average",
            max("MWh") OVER seven AS "MWh 7-day Moving Maximum"
        FROM "Generation History"
        WINDOW seven AS (
            PARTITION BY "Plant"
            ORDER BY "Date" ASC
            RANGE BETWEEN INTERVAL 3 DAYS PRECEDING
                      AND INTERVAL 3 DAYS FOLLOWING)
        ORDER BY 1, 2
        """,
        (),
    )

    assert rows == expected


def test_duckdb_docs_generation_history_multiple_named_windows(
    duckdb_db: Any,
) -> None:
    _create_generation_history(duckdb_db)
    history = Table("Generation History")

    query, params = (
        select(
            history.Plant,
            history.Date,
            func.min(history.MWh)
            .over("seven")
            .as_(
                "MWh 7-day Moving Minimum",
            ),
            func.avg(history.MWh)
            .over("seven")
            .as_(
                "MWh 7-day Moving Average",
            ),
            func.max(history.MWh)
            .over("seven")
            .as_(
                "MWh 7-day Moving Maximum",
            ),
            func.min(history.MWh)
            .over("three")
            .as_(
                "MWh 3-day Moving Minimum",
            ),
            func.avg(history.MWh)
            .over("three")
            .as_(
                "MWh 3-day Moving Average",
            ),
            func.max(history.MWh)
            .over("three")
            .as_(
                "MWh 3-day Moving Maximum",
            ),
        )
        .from_(history)
        .window(
            "seven",
            partition_by=history.Plant,
            order_by=history.Date,
            range_=Window.Range.between(
                Window.Range.interval_preceding(3, "DAYS"),
                Window.Range.interval_following(3, "DAYS"),
            ),
        )
        .window(
            "three",
            partition_by=history.Plant,
            order_by=history.Date,
            range_=Window.Range.between(
                Window.Range.interval_preceding(1, "DAYS"),
                Window.Range.interval_following(1, "DAYS"),
            ),
        )
        .order_by(history.Plant, history.Date)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)
    expected = _fetch_rows(
        duckdb_db,
        """
        SELECT "Plant", "Date",
            min("MWh") OVER seven AS "MWh 7-day Moving Minimum",
            avg("MWh") OVER seven AS "MWh 7-day Moving Average",
            max("MWh") OVER seven AS "MWh 7-day Moving Maximum",
            min("MWh") OVER three AS "MWh 3-day Moving Minimum",
            avg("MWh") OVER three AS "MWh 3-day Moving Average",
            max("MWh") OVER three AS "MWh 3-day Moving Maximum"
        FROM "Generation History"
        WINDOW
            seven AS (
                PARTITION BY "Plant"
                ORDER BY "Date" ASC
                RANGE BETWEEN INTERVAL 3 DAYS PRECEDING
                          AND INTERVAL 3 DAYS FOLLOWING),
            three AS (
                PARTITION BY "Plant"
                ORDER BY "Date" ASC
                RANGE BETWEEN INTERVAL 1 DAYS PRECEDING
                AND INTERVAL 1 DAYS FOLLOWING)
        ORDER BY 1, 2
        """,
        (),
    )

    assert rows == expected


def test_duckdb_docs_generation_history_seven_day_iqr(
    duckdb_db: Any,
) -> None:
    _create_generation_history(duckdb_db)
    history = Table("Generation History")

    query, params = (
        select(
            history.Plant,
            history.Date,
            func.min(history.MWh)
            .over("seven")
            .as_(
                "MWh 7-day Moving Minimum",
            ),
            func.quantile_cont(history.MWh, [0.25, 0.5, 0.75])
            .over("seven")
            .as_("MWh 7-day Moving IQR"),
            func.max(history.MWh)
            .over("seven")
            .as_(
                "MWh 7-day Moving Maximum",
            ),
        )
        .from_(history)
        .window(
            "seven",
            partition_by=history.Plant,
            order_by=history.Date,
            range_=Window.Range.between(
                Window.Range.interval_preceding(3, "DAYS"),
                Window.Range.interval_following(3, "DAYS"),
            ),
        )
        .order_by(history.Plant, history.Date)
        .compile()
    )

    rows = _fetch_rows(duckdb_db, query, params)
    expected = _fetch_rows(
        duckdb_db,
        """
        SELECT "Plant", "Date",
            min("MWh") OVER seven AS "MWh 7-day Moving Minimum",
            quantile_cont("MWh", [0.25, 0.5, 0.75]) OVER seven
                AS "MWh 7-day Moving IQR",
            max("MWh") OVER seven AS "MWh 7-day Moving Maximum",
        FROM "Generation History"
        WINDOW seven AS (
            PARTITION BY "Plant"
            ORDER BY "Date" ASC
            RANGE BETWEEN INTERVAL 3 DAYS PRECEDING
                      AND INTERVAL 3 DAYS FOLLOWING)
        ORDER BY 1, 2
        """,
        (),
    )

    assert rows == expected
