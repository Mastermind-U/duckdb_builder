from sql_fusion import (
    Table,
    func,
    get_format_specifier,
    get_numbered_params,
    select,
    union,
    update,
)


def test_compile_uses_custom_params_for_functions_and_in() -> None:
    users = Table("users")

    query, bound_params = (
        select(func.coalesce(users.email, "missing"))
        .from_(users)
        .where(users.id.in_([1, 2]))
        .compile(get_numbered_params)
    )

    assert query == (
        'SELECT COALESCE("a"."email", $1) FROM "users" AS "a" '
        'WHERE "a"."id" IN ($2, $3)'
    )
    assert bound_params == ("missing", 1, 2)


def test_compile_uses_format_specifier_params() -> None:
    users = Table("users")

    query, bound_params = (
        select(users.id)
        .from_(users)
        .where((users.status == "active") & users.id.in_([1, 2]))
        .compile(get_format_specifier)
    )

    assert query == (
        'SELECT "a"."id" FROM "users" AS "a" '
        'WHERE ("a"."status" = %s AND "a"."id" IN (%s, %s))'
    )
    assert bound_params == ("active", 1, 2)


def test_compile_uses_one_param_iterator_across_cte_and_outer_query() -> None:
    users = Table("users")
    orders = Table("orders")
    active_value = True
    paid_orders = (
        select(orders.user_id).from_(orders).where(orders.status == "paid")
    )

    query, bound_params = (
        select(users.id)
        .from_(users)
        .with_(paid=paid_orders)
        .where(users.active == active_value)
        .compile(get_numbered_params)
    )

    assert query == (
        'WITH "paid" AS (SELECT "a"."user_id" FROM "orders" AS "a" '
        'WHERE "a"."status" = $1) '
        'SELECT "b"."id" FROM "users" AS "b" '
        'WHERE "b"."active" = $2'
    )
    assert bound_params == ("paid", True)


def test_compile_uses_one_param_iterator_across_set_operations() -> None:
    active_users = Table("active_users")
    archived_users = Table("archived_users")
    active_query = (
        select(active_users.id)
        .from_(active_users)
        .where_by(
            status="active",
        )
    )
    archived_query = (
        select(archived_users.id)
        .from_(archived_users)
        .where_by(
            status="inactive",
        )
    )

    query, bound_params = union(active_query, archived_query).compile(
        get_numbered_params,
    )

    assert query == (
        'SELECT "a"."id" FROM "active_users" AS "a" '
        'WHERE "a"."status" = $1 '
        "UNION "
        'SELECT "b"."id" FROM "archived_users" AS "b" '
        'WHERE "b"."status" = $2'
    )
    assert bound_params == ("active", "inactive")


def test_compile_uses_one_param_iterator_across_update_subquery() -> None:
    users = Table("users")
    orders = Table("orders")
    max_total = (
        select(func.max(orders.total))
        .from_(orders)
        .where((orders.user_id == users.id) & (orders.status == "completed"))
    )

    query, bound_params = (
        update(users)
        .set(rank=users.rank + 1, total=max_total)
        .where(users.id == 5)
        .compile(get_numbered_params)
    )

    assert query == (
        'UPDATE "users" AS "a" '
        'SET "rank" = "a"."rank" + $1, '
        '"total" = (SELECT MAX("b"."total") FROM "orders" AS "b" '
        'WHERE ("b"."user_id" = "a"."id" AND "b"."status" = $2)) '
        'WHERE "a"."id" = $3'
    )
    assert bound_params == (1, "completed", 5)
