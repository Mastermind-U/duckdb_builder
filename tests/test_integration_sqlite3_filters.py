"""Integration tests for sqlite3 backend."""

import re
import sqlite3
from collections.abc import Iterator
from typing import Any

import pytest

from sql_fusion import (
    Alias,
    Table,
    Window,
    delete,
    except_,
    func,
    insert,
    intersect,
    select,
    union,
    update,
)
from sql_fusion.composite_table import CompileExpression

MIN_USER_AGE = 30
MIN_JOIN_TOTAL = 100
MAX_PENDING_TOTAL = 60
NEW_USER_ID = 6
UPDATED_USER_ID = 2
DELETED_USER_ID = 4


def sqlite_cte_materialization(
    *,
    materialized: dict[str, bool],
) -> CompileExpression:
    """Rewrite SQLite CTEs to use MATERIALIZED or NOT MATERIALIZED."""

    def rewrite(
        sql: str,
        params: tuple[Any, ...],
    ) -> tuple[str, tuple[Any, ...]]:
        for cte_name, is_materialized in materialized.items():
            marker = (
                "MATERIALIZED" if is_materialized else ("NOT MATERIALIZED")
            )
            sql = re.sub(
                rf'("{re.escape(cte_name)}")\s+AS\s+\(',
                rf"\1 AS {marker} (",
                sql,
                count=1,
            )
        return sql, params

    return rewrite


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


def test_self_join_with_generated_aliases(
    sqlite_db: sqlite3.Connection,
) -> None:
    sqlite_db.execute(
        """
        CREATE TABLE xxx (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER
        )
        """,
    )
    sqlite_db.executemany(
        "INSERT INTO xxx VALUES (?, ?)",
        [
            (1, None),
            (2, 1),
            (3, 1),
        ],
    )
    sqlite_db.commit()

    left = Table("xxx")
    right = Table("xxx")
    query, params = (
        select(left.id, right.id)
        .from_(left)
        .join(right, left.parent_id == right.id)
        .order_by(left.id)
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)

    assert rows == [(2, 1), (3, 1)]


def test_complex_subquery_filter(sqlite_db: sqlite3.Connection) -> None:
    users = Table("users")
    orders = Table("orders")

    completed_users = (
        select(orders.user_id).from_(orders).where_by(status="completed")
    )
    cancelled_users = (
        select(orders.user_id).from_(orders).where_by(status="cancelled")
    )

    query = (
        select(users.id, users.name)
        .from_(users)
        .where_by(status="active")
        .where(users.id.in_(completed_users))
        .where(users.id.not_in(cancelled_users))
        .where((users.age >= MIN_USER_AGE) & users.country.in_(["US", "CA"]))
    )

    rows = _fetch_rows(sqlite_db, *query.compile())

    assert sorted(rows) == [(1, "Alice"), (3, "Carol")]


@pytest.mark.parametrize(
    ("is_materialized", "expected_sql"),
    [
        (
            True,
            'WITH "paid_orders" AS MATERIALIZED ('
            'SELECT "a"."user_id", "a"."total" FROM "orders" AS "a" '
            'WHERE "a"."status" = ?'
            ') SELECT "c"."name", SUM("b"."total") FROM "paid_orders" AS "b" '
            'INNER JOIN "users" AS "c" ON "b"."user_id" = "c"."id" '
            'GROUP BY "c"."name"',
        ),
        (
            False,
            'WITH "paid_orders" AS NOT MATERIALIZED ('
            'SELECT "a"."user_id", "a"."total" FROM "orders" AS "a" '
            'WHERE "a"."status" = ?'
            ') SELECT "c"."name", SUM("b"."total") FROM "paid_orders" AS "b" '
            'INNER JOIN "users" AS "c" ON "b"."user_id" = "c"."id" '
            'GROUP BY "c"."name"',
        ),
    ],
)
def test_sqlite_cte_materialization(
    sqlite_db: sqlite3.Connection,
    is_materialized: bool,
    expected_sql: str,
) -> None:
    sqlite_db.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?)",
        [
            (7, 1, 120, "paid"),
            (8, 3, 200, "paid"),
            (9, 5, 300, "paid"),
        ],
    )
    sqlite_db.commit()

    orders = Table("orders")
    users = Table("users")
    paid_orders = Table("paid_orders")

    paid_orders_cte = (
        select(orders.user_id, orders.total)
        .from_(orders)
        .where_by(status="paid")
    )

    query, params = (
        select(users.name, func.sum(paid_orders.total))
        .with_(paid_orders=paid_orders_cte)
        .from_(paid_orders)
        .join(users, paid_orders.user_id == users.id)
        .group_by(users.name)
        .compile_expression(
            sqlite_cte_materialization(
                materialized={"paid_orders": is_materialized},
            ),
        )
        .compile()
    )

    assert query == expected_sql
    assert params == ("paid",)

    rows = _fetch_rows(sqlite_db, query, params)

    assert sorted(rows) == [
        ("Alice", 120),
        ("Carol", 200),
        ("Erin", 300),
    ]


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

    query = (
        update(users)
        .set(status="active", age=29)
        .where(users.id == UPDATED_USER_ID)
    )
    _fetch_rows(sqlite_db, *query.compile())

    query2 = (
        select(users.id, users.name, users.age, users.status)
        .from_(users)
        .where(users.id == UPDATED_USER_ID)
    )

    rows = _fetch_rows(sqlite_db, *query2.compile())

    assert rows == [(UPDATED_USER_ID, "Bob", 29, "active")]


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
        *select(target.id, target.x)
        .from_(target)
        .order_by(target.id)
        .compile(),
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
        *(
            select(target.id, target.max_x, target.source_count)
            .from_(target)
            .order_by(target.id)
            .compile()
        ),
    )

    assert rows == [(1, 11, 2), (2, 0, 0), (3, 8, 2)]


def test_update_with_ordered_subquery_in_set_clause(
    sqlite_db: sqlite3.Connection,
) -> None:
    sqlite_db.execute(
        """
        CREATE TABLE activity_users (
            id INTEGER PRIMARY KEY,
            last_activity TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """,
    )
    sqlite_db.execute(
        """
        CREATE TABLE posts (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
        """,
    )
    sqlite_db.executemany(
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
    sqlite_db.executemany(
        "INSERT INTO posts VALUES (?, ?, ?)",
        [
            (1, 1, "2024-01-05 10:00:00"),
            (2, 2, "2024-01-03 09:00:00"),
            (3, 1, "2024-01-01 08:00:00"),
        ],
    )
    sqlite_db.commit()

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
    _fetch_rows(sqlite_db, query, params)

    rows = _fetch_rows(
        sqlite_db,
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


def test_sqlite_complex_window_query(
    sqlite_db: sqlite3.Connection,
) -> None:
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

    rows = _fetch_rows(sqlite_db, query, params)

    assert rows == [
        ("Alice", 1, 120, 1, 120, 2),
        ("Alice", 2, 50, 2, 170, 2),
        ("Carol", 3, 200, 1, 200, 1),
    ]


def test_sqlite_union_all_compound_query(
    sqlite_db: sqlite3.Connection,
) -> None:
    sqlite_db.execute(
        """
        CREATE TABLE premium_users (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL
        )
        """,
    )
    sqlite_db.executemany(
        "INSERT INTO premium_users VALUES (?, ?)",
        [
            (1, "active"),
            (3, "active"),
        ],
    )
    sqlite_db.commit()

    users = Table("users")
    premium_users = Table("premium_users")
    active_users = select(users.id).from_(users).where_by(status="active")
    premium_active_users = (
        select(premium_users.id).from_(premium_users).where_by(status="active")
    )

    query, params = union(
        active_users,
        premium_active_users,
        all_=True,
    ).compile()

    rows = _fetch_rows(sqlite_db, query, params)

    assert sorted(rows) == [(1,), (1,), (3,), (3,)]


def test_sqlite_intersect_compound_query(
    sqlite_db: sqlite3.Connection,
) -> None:
    sqlite_db.execute(
        """
        CREATE TABLE premium_users (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL
        )
        """,
    )
    sqlite_db.executemany(
        "INSERT INTO premium_users VALUES (?, ?)",
        [
            (1, "active"),
            (3, "active"),
            (5, "active"),
        ],
    )
    sqlite_db.commit()

    users = Table("users")
    premium_users = Table("premium_users")
    active_users = select(users.id).from_(users).where_by(status="active")
    premium_active_users = (
        select(premium_users.id).from_(premium_users).where_by(status="active")
    )

    query, params = intersect(active_users, premium_active_users).compile()

    rows = _fetch_rows(sqlite_db, query, params)

    assert sorted(rows) == [(1,), (3,)]


def test_sqlite_except_compound_query(
    sqlite_db: sqlite3.Connection,
) -> None:
    sqlite_db.execute(
        """
        CREATE TABLE premium_users (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL
        )
        """,
    )
    sqlite_db.executemany(
        "INSERT INTO premium_users VALUES (?, ?)",
        [
            (1, "active"),
            (3, "active"),
        ],
    )
    sqlite_db.commit()

    users = Table("users")
    premium_users = Table("premium_users")
    all_users = select(users.id).from_(users)
    premium_user_ids = select(premium_users.id).from_(premium_users)

    query, params = except_(all_users, premium_user_ids).compile()

    rows = _fetch_rows(sqlite_db, query, params)

    assert sorted(rows) == [(2,), (4,), (5,)]


def _create_sqlite_window_t0(connection: sqlite3.Connection) -> None:
    connection.execute("CREATE TABLE t0(x INTEGER PRIMARY KEY, y TEXT)")
    connection.executemany(
        "INSERT INTO t0 VALUES (?, ?)",
        [(1, "aaa"), (2, "ccc"), (3, "bbb")],
    )
    connection.commit()


def _create_sqlite_window_t1(connection: sqlite3.Connection) -> None:
    connection.execute("CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c)")
    connection.executemany(
        "INSERT INTO t1 VALUES (?, ?, ?)",
        [
            (1, "A", "one"),
            (2, "B", "two"),
            (3, "C", "three"),
            (4, "D", "one"),
            (5, "E", "two"),
            (6, "F", "three"),
            (7, "G", "one"),
        ],
    )
    connection.commit()


def _create_sqlite_window_t2(connection: sqlite3.Connection) -> None:
    connection.execute("CREATE TABLE t2(a, b)")
    connection.executemany(
        "INSERT INTO t2 VALUES (?, ?)",
        [
            ("a", "one"),
            ("a", "two"),
            ("a", "three"),
            ("b", "four"),
            ("c", "five"),
            ("c", "six"),
        ],
    )
    connection.commit()


def test_sqlite_docs_row_number_ordered_window(
    sqlite_db: sqlite3.Connection,
) -> None:
    _create_sqlite_window_t0(sqlite_db)
    t0 = Table("t0")

    query, params = (
        select(
            t0.x,
            t0.y,
            func.row_number().over(order_by=t0.y).as_("row_number"),
        )
        .from_(t0)
        .order_by(t0.x)
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)
    expected = _fetch_rows(
        sqlite_db,
        """
        SELECT x, y, row_number() OVER (ORDER BY y) AS row_number
        FROM t0 ORDER BY x
        """,
        (),
    )

    assert rows == expected


def test_sqlite_docs_two_named_windows(
    sqlite_db: sqlite3.Connection,
) -> None:
    _create_sqlite_window_t0(sqlite_db)
    t0 = Table("t0")

    query, params = (
        select(
            t0.x,
            t0.y,
            func.row_number().over("win1"),
            func.rank().over("win2"),
        )
        .from_(t0)
        .window(
            "win1",
            order_by=t0.y,
            range_=Window.Range.between(
                Window.Range.unbounded_preceding(),
                Window.Range.current_row(),
            ),
        )
        .window("win2", partition_by=t0.y, order_by=t0.x)
        .order_by(t0.x)
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)
    expected = _fetch_rows(
        sqlite_db,
        """
        SELECT x, y, row_number() OVER win1, rank() OVER win2
        FROM t0
        WINDOW win1 AS (
            ORDER BY y RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
               win2 AS (PARTITION BY y ORDER BY x)
        ORDER BY x
        """,
        (),
    )

    assert rows == expected


def test_sqlite_docs_aggregate_rows_frame(
    sqlite_db: sqlite3.Connection,
) -> None:
    _create_sqlite_window_t1(sqlite_db)
    t1 = Table("t1")

    query, params = (
        select(
            t1.a,
            t1.b,
            func.group_concat(t1.b, ".")
            .over(
                order_by=t1.a,
                rows=Window.Rows.between(
                    Window.Rows.preceding(1),
                    Window.Rows.following(1),
                ),
            )
            .as_("group_concat"),
        )
        .from_(t1)
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)
    expected = _fetch_rows(
        sqlite_db,
        """
        SELECT a, b, group_concat(b, '.') OVER (
          ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        ) AS group_concat FROM t1
        """,
        (),
    )

    assert rows == expected


def test_sqlite_docs_partition_by_range_following(
    sqlite_db: sqlite3.Connection,
) -> None:
    _create_sqlite_window_t1(sqlite_db)
    t1 = Table("t1")

    query, params = (
        select(
            t1.c,
            t1.a,
            t1.b,
            func.group_concat(t1.b, ".")
            .over(
                partition_by=t1.c,
                order_by=t1.a,
                range_=Window.Range.between(
                    Window.Range.current_row(),
                    Window.Range.unbounded_following(),
                ),
            )
            .as_("group_concat"),
        )
        .from_(t1)
        .order_by(t1.c, t1.a)
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)
    expected = _fetch_rows(
        sqlite_db,
        """
        SELECT c, a, b, group_concat(b, '.') OVER (
          PARTITION BY c ORDER BY a
          RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS group_concat
        FROM t1 ORDER BY c, a
        """,
        (),
    )

    assert rows == expected


def test_sqlite_docs_default_range_frame_with_peers(
    sqlite_db: sqlite3.Connection,
) -> None:
    _create_sqlite_window_t1(sqlite_db)
    t1 = Table("t1")

    query, params = (
        select(
            t1.a,
            t1.b,
            t1.c,
            func.group_concat(t1.b, ".")
            .over(order_by=t1.c)
            .as_("group_concat"),
        )
        .from_(t1)
        .order_by(t1.a)
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)
    expected = _fetch_rows(
        sqlite_db,
        """
        SELECT a, b, c,
               group_concat(b, '.') OVER (ORDER BY c) AS group_concat
        FROM t1 ORDER BY a
        """,
        (),
    )

    assert rows == expected


def test_sqlite_docs_frame_boundaries_current_to_unbounded(
    sqlite_db: sqlite3.Connection,
) -> None:
    _create_sqlite_window_t1(sqlite_db)
    t1 = Table("t1")

    query, params = (
        select(
            t1.c,
            t1.a,
            t1.b,
            func.group_concat(t1.b, ".")
            .over(
                order_by=(t1.c, t1.a),
                rows=Window.Rows.between(
                    Window.Rows.current_row(),
                    Window.Rows.unbounded_following(),
                ),
            )
            .as_("group_concat"),
        )
        .from_(t1)
        .order_by(t1.c, t1.a)
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)
    expected = _fetch_rows(
        sqlite_db,
        """
        SELECT c, a, b, group_concat(b, '.') OVER (
          ORDER BY c, a ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS group_concat
        FROM t1 ORDER BY c, a
        """,
        (),
    )

    assert rows == expected


def test_sqlite_docs_exclude_clause_variants(
    sqlite_db: sqlite3.Connection,
) -> None:
    _create_sqlite_window_t1(sqlite_db)
    t1 = Table("t1")
    frame = Window.Groups.between(
        Window.Groups.unbounded_preceding(),
        Window.Groups.current_row(),
    )

    query, params = (
        select(
            t1.c,
            t1.a,
            t1.b,
            func.group_concat(t1.b, ".")
            .over(order_by=t1.c, groups=frame, exclude="NO OTHERS")
            .as_("no_others"),
            func.group_concat(t1.b, ".")
            .over(order_by=t1.c, groups=frame, exclude="CURRENT ROW")
            .as_("current_row"),
            func.group_concat(t1.b, ".")
            .over(order_by=t1.c, groups=frame, exclude="GROUP")
            .as_("grp"),
            func.group_concat(t1.b, ".")
            .over(order_by=t1.c, groups=frame, exclude="TIES")
            .as_("ties"),
        )
        .from_(t1)
        .order_by(t1.c, t1.a)
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)
    expected = _fetch_rows(
        sqlite_db,
        """
        SELECT c, a, b,
          group_concat(b, '.') OVER (
            ORDER BY c GROUPS BETWEEN UNBOUNDED PRECEDING
            AND CURRENT ROW EXCLUDE NO OTHERS
          ) AS no_others,
          group_concat(b, '.') OVER (
            ORDER BY c GROUPS BETWEEN UNBOUNDED PRECEDING
            AND CURRENT ROW EXCLUDE CURRENT ROW
          ) AS current_row,
          group_concat(b, '.') OVER (
            ORDER BY c GROUPS BETWEEN UNBOUNDED PRECEDING
            AND CURRENT ROW EXCLUDE GROUP
          ) AS grp,
          group_concat(b, '.') OVER (
            ORDER BY c GROUPS BETWEEN UNBOUNDED PRECEDING
            AND CURRENT ROW EXCLUDE TIES
          ) AS ties
        FROM t1 ORDER BY c, a
        """,
        (),
    )

    assert rows == expected


def test_sqlite_docs_filter_clause(
    sqlite_db: sqlite3.Connection,
) -> None:
    _create_sqlite_window_t1(sqlite_db)
    t1 = Table("t1")

    query, params = (
        select(
            t1.c,
            t1.a,
            t1.b,
            func.group_concat(t1.b, ".")
            .filter(t1.c != "two")
            .over(order_by=t1.a)
            .as_("group_concat"),
        )
        .from_(t1)
        .order_by(t1.a)
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)
    expected = _fetch_rows(
        sqlite_db,
        """
        SELECT c, a, b, group_concat(b, '.') FILTER (WHERE c!='two') OVER (
          ORDER BY a
        ) AS group_concat
        FROM t1 ORDER BY a
        """,
        (),
    )

    assert rows == expected


def test_sqlite_docs_ranking_functions(
    sqlite_db: sqlite3.Connection,
) -> None:
    _create_sqlite_window_t2(sqlite_db)
    t2 = Table("t2")

    query, params = (
        select(
            t2.a.as_("a") if hasattr(t2.a, "as_") else t2.a,
            func.row_number().over("win").as_("row_number"),
            func.rank().over("win").as_("rank"),
            func.dense_rank().over("win").as_("dense_rank"),
            func.percent_rank().over("win").as_("percent_rank"),
            func.cume_dist().over("win").as_("cume_dist"),
        )
        .from_(t2)
        .window("win", order_by=t2.a)
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)
    expected = _fetch_rows(
        sqlite_db,
        """
        SELECT a                        AS a,
               row_number() OVER win    AS row_number,
               rank() OVER win          AS rank,
               dense_rank() OVER win    AS dense_rank,
               percent_rank() OVER win  AS percent_rank,
               cume_dist() OVER win     AS cume_dist
        FROM t2
        WINDOW win AS (ORDER BY a)
        """,
        (),
    )

    assert rows == expected


def test_sqlite_docs_ntile_functions(
    sqlite_db: sqlite3.Connection,
) -> None:
    _create_sqlite_window_t2(sqlite_db)
    t2 = Table("t2")

    query, params = (
        select(
            t2.a,
            t2.b,
            func.ntile(2).over("win").as_("ntile_2"),
            func.ntile(4).over("win").as_("ntile_4"),
        )
        .from_(t2)
        .window("win", order_by=t2.a)
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)
    expected = _fetch_rows(
        sqlite_db,
        """
        SELECT a                        AS a,
               b                        AS b,
               ntile(2) OVER win        AS ntile_2,
               ntile(4) OVER win        AS ntile_4
        FROM t2
        WINDOW win AS (ORDER BY a)
        """,
        (),
    )

    assert rows == expected


def test_sqlite_docs_value_window_functions(
    sqlite_db: sqlite3.Connection,
) -> None:
    _create_sqlite_window_t1(sqlite_db)
    t1 = Table("t1")

    query, params = (
        select(
            t1.b.as_("b") if hasattr(t1.b, "as_") else t1.b,
            func.lead(t1.b, 2, "n/a").over("win").as_("lead"),
            func.lag(t1.b).over("win").as_("lag"),
            func.first_value(t1.b).over("win").as_("first_value"),
            func.last_value(t1.b).over("win").as_("last_value"),
            func.nth_value(t1.b, 3).over("win").as_("nth_value_3"),
        )
        .from_(t1)
        .window(
            "win",
            order_by=t1.b,
            rows=Window.Rows.between(
                Window.Rows.unbounded_preceding(),
                Window.Rows.current_row(),
            ),
        )
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)
    expected = _fetch_rows(
        sqlite_db,
        """
        SELECT b                          AS b,
               lead(b, 2, 'n/a') OVER win AS lead,
               lag(b) OVER win            AS lag,
               first_value(b) OVER win    AS first_value,
               last_value(b) OVER win     AS last_value,
               nth_value(b, 3) OVER win   AS nth_value_3
        FROM t1
        WINDOW win AS (
            ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        """,
        (),
    )

    assert rows == expected


def test_sqlite_docs_window_chaining(
    sqlite_db: sqlite3.Connection,
) -> None:
    _create_sqlite_window_t1(sqlite_db)
    t1 = Table("t1")

    query, params = (
        select(
            func.group_concat(t1.b, ".")
            .over(
                Window(
                    base="win",
                    rows=Window.Rows.between(
                        Window.Rows.unbounded_preceding(),
                        Window.Rows.current_row(),
                    ),
                ),
            )
            .as_("group_concat"),
        )
        .from_(t1)
        .window("win", partition_by=t1.a, order_by=t1.c)
        .compile()
    )

    rows = _fetch_rows(sqlite_db, query, params)
    expected = _fetch_rows(
        sqlite_db,
        """
        SELECT group_concat(b, '.') OVER (
          win ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS group_concat
        FROM t1
        WINDOW win AS (PARTITION BY a ORDER BY c)
        """,
        (),
    )

    assert rows == expected
