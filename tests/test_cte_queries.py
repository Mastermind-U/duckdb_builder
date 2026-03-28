"""Tests for CTE / WITH clause support."""

import pytest

from duckdb_builder import Table, select, update


def test_select_with_single_cte() -> None:
    orders = Table("orders")
    recent_orders = (
        select(orders.user_id, orders.total)
        .from_(orders)
        .where_by(status="paid")
    )

    query, params = (
        select()
        .with_(recent_orders=recent_orders)
        .from_(Table("recent_orders"))
        .compile()
    )

    assert query == (
        'WITH "recent_orders" AS ('
        'SELECT "a"."user_id", "a"."total" '
        'FROM "orders" AS "a" '
        'WHERE "a"."status" = ?'
        ') SELECT * FROM "recent_orders" AS "b"'
    )
    assert params == ("paid",)


def test_select_with_multiple_ctes_preserves_parameter_order() -> None:
    orders = Table("orders")
    users = Table("users")

    paid_orders = select(orders.user_id).from_(orders).where_by(status="paid")
    active_users = select(users.id).from_(users).where_by(active=True)

    query, params = (
        select()
        .with_(paid_orders=paid_orders, active_users=active_users)
        .from_(Table("active_users"))
        .compile()
    )

    assert query == (
        'WITH "paid_orders" AS ('
        'SELECT "a"."user_id" FROM "orders" AS "a" WHERE "a"."status" = ?'
        '), "active_users" AS ('
        'SELECT "b"."id" FROM "users" AS "b" WHERE "b"."active" = ?'
        ') SELECT * FROM "active_users" AS "c"'
    )
    assert params == ("paid", True)


def test_select_with_multiple_with_calls_merges_ctes() -> None:
    orders = Table("orders")
    users = Table("users")

    paid_orders = select(orders.user_id).from_(orders).where_by(status="paid")
    active_users = select(users.id).from_(users).where_by(active=True)

    query, params = (
        select()
        .with_(paid_orders=paid_orders)
        .with_(active_users=active_users)
        .from_(Table("active_users"))
        .compile()
    )

    assert query == (
        'WITH "paid_orders" AS ('
        'SELECT "a"."user_id" FROM "orders" AS "a" WHERE "a"."status" = ?'
        '), "active_users" AS ('
        'SELECT "b"."id" FROM "users" AS "b" WHERE "b"."active" = ?'
        ') SELECT * FROM "active_users" AS "c"'
    )
    assert params == ("paid", True)


def test_select_with_recursive_cte() -> None:
    nodes = Table("nodes")
    tree = select(nodes.id, nodes.parent_id).from_(nodes).where_by(active=True)

    query, params = (
        select()
        .with_(recursive=True, tree=tree)
        .from_(Table("tree"))
        .compile()
    )

    assert query == (
        'WITH RECURSIVE "tree" AS ('
        'SELECT "a"."id", "a"."parent_id" '
        'FROM "nodes" AS "a" '
        'WHERE "a"."active" = ?'
        ') SELECT * FROM "tree" AS "b"'
    )
    assert params == (True,)


def test_clause_comment_after_with_inserts_hint_into_cte_header() -> None:
    orders = Table("orders")
    recent_orders = (
        select(orders.user_id)
        .from_(orders)
        .where_by(status="paid")
    )

    query, params = (
        select()
        .with_(recent_orders=recent_orders)
        .after_clause("WITH", "SeqScan (ta) IndexScan (tb)", hint=True)
        .from_(Table("recent_orders"))
        .compile()
    )

    assert query == (
        'WITH /*+ SeqScan (ta) IndexScan (tb) */\n'
        '"recent_orders" AS (SELECT "a"."user_id" FROM "orders" AS "a" '
        'WHERE "a"."status" = ?) '
        'SELECT * FROM "recent_orders" AS "b"'
    )
    assert params == ("paid",)


def test_clause_comment_after_from_inserts_hint_before_table_name() -> None:
    users = Table("users")

    query, params = (
        select(users.id)
        .from_(users)
        .after_clause("FROM", "SeqScan (a)", hint=True)
        .compile()
    )

    assert query == (
        'SELECT "a"."id" FROM /*+ SeqScan (a) */\n"users" AS "a"'
    )
    assert params == ()


def test_clause_comments_work_on_update_clauses() -> None:
    users = Table("users")

    query, params = (
        update(users)
        .before_clause("UPDATE", "debug")
        .after_clause("SET", "SeqScan (a)", hint=True)
        .set(status="inactive")
        .compile()
    )

    assert query == (
        '/* debug */\n'
        'UPDATE "users" AS "a" SET /*+ SeqScan (a) */\n"a"."status" = ?'
    )
    assert params == ("inactive",)


def test_comment_wrapper_prefixes_query() -> None:
    users = Table("users")

    query, params = select(users.id).from_(users).comment("debug").compile()

    assert query == '/* debug */\nSELECT "a"."id" FROM "users" AS "a"'
    assert params == ()


def test_hint_comment_wrapper_prefixes_query() -> None:
    users = Table("users")

    query, params = (
        select(users.id)
        .from_(users)
        .comment("SeqScan (a)", hint=True)
        .compile()
    )

    assert query == '/*+ SeqScan (a) */\nSELECT "a"."id" FROM "users" AS "a"'
    assert params == ()


def test_explain_and_analyze_wrappers() -> None:
    users = Table("users")

    explain_query, explain_params = (
        select(users.id).from_(users).explain().compile()
    )
    analyze_query, analyze_params = (
        select(users.id).from_(users).analyze(verbose=True).compile()
    )

    assert explain_query == 'EXPLAIN SELECT "a"."id" FROM "users" AS "a"'
    assert explain_params == ()
    assert analyze_query == (
        'EXPLAIN ANALYZE VERBOSE SELECT "a"."id" FROM "users" AS "a"'
    )
    assert analyze_params == ()


def test_compile_expression_applies_to_nested_cte_queries() -> None:
    orders = Table("orders")
    paid_orders_table = Table("paid_orders")

    paid_orders = (
        select(orders.user_id)
        .from_(orders)
        .where_by(status="paid")
        .comment("cte")
    )

    query, params = (
        select(paid_orders_table.id)
        .with_(paid_orders=paid_orders)
        .from_(paid_orders_table)
        .explain()
        .compile()
    )

    assert query == (
        'EXPLAIN WITH "paid_orders" AS ('
        '/* cte */\nSELECT "a"."user_id" FROM "orders" AS "a" '
        'WHERE "a"."status" = ?'
        ') SELECT "b"."id" FROM "paid_orders" AS "b"'
    )
    assert params == ("paid",)


def test_with_requires_cte_queries() -> None:
    with pytest.raises(TypeError, match="must be query-like"):
        select().with_(bad_cte=object())  # type: ignore
