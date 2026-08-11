"""Tests for SQL window functions."""

import pytest

from sql_fusion import Alias, Table, Window, func, select


def test_inline_window_function() -> None:
    orders = Table("orders")

    query, params = (
        select(
            orders.user_id,
            orders.id,
            func.row_number()
            .over(
                partition_by=orders.user_id,
                order_by=orders.total,
                descending=True,
            )
            .as_("order_rank"),
        )
        .from_(orders)
        .compile()
    )

    assert query == (
        'SELECT "a"."user_id", "a"."id", '
        'ROW_NUMBER() OVER (PARTITION BY "a"."user_id" '
        'ORDER BY "a"."total" DESC) AS "order_rank" '
        'FROM "orders" AS "a"'
    )
    assert params == ()


def test_window_function_with_frame() -> None:
    orders = Table("orders")

    query, params = (
        select(
            orders.user_id,
            orders.id,
            func.sum(orders.total)
            .over(
                partition_by=orders.user_id,
                order_by=orders.id,
                rows=("UNBOUNDED PRECEDING", "CURRENT ROW"),
            )
            .as_("running_total"),
        )
        .from_(orders)
        .compile()
    )

    assert query == (
        'SELECT "a"."user_id", "a"."id", '
        'SUM("a"."total") OVER (PARTITION BY "a"."user_id" '
        'ORDER BY "a"."id" ROWS BETWEEN UNBOUNDED PRECEDING '
        'AND CURRENT ROW) AS "running_total" '
        'FROM "orders" AS "a"'
    )
    assert params == ()


def test_window_function_with_frame_objects() -> None:
    orders = Table("orders")

    query, params = (
        select(
            func.sum(orders.total)
            .over(
                order_by=orders.id,
                rows=Window.Rows.between(
                    Window.Rows.unbounded_preceding(),
                    Window.Rows.current_row(),
                ),
            )
            .as_("running_total"),
            func.avg(orders.total)
            .over(
                order_by=orders.created_at,
                range_=Window.Range.between(
                    Window.Range.interval_preceding(3, "DAYS"),
                    Window.Range.interval_following(3, "DAYS"),
                ),
            )
            .as_("moving_average"),
            func.count("*")
            .over(
                order_by=orders.status,
                groups=Window.Groups.between(
                    Window.Groups.preceding(1),
                    Window.Groups.following(1),
                ),
            )
            .as_("nearby_groups"),
        )
        .from_(orders)
        .compile()
    )

    assert query == (
        'SELECT SUM("a"."total") OVER (ORDER BY "a"."id" '
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
        'AS "running_total", AVG("a"."total") OVER '
        '(ORDER BY "a"."created_at" RANGE BETWEEN INTERVAL 3 DAYS '
        'PRECEDING AND INTERVAL 3 DAYS FOLLOWING) AS "moving_average", '
        'COUNT(*) OVER (ORDER BY "a"."status" GROUPS BETWEEN 1 PRECEDING '
        'AND 1 FOLLOWING) AS "nearby_groups" FROM "orders" AS "a"'
    )
    assert params == ()


def test_window_function_with_single_frame_object() -> None:
    orders = Table("orders")

    query, params = (
        select(
            func.sum(orders.total)
            .over(order_by=orders.id, rows=Window.Rows.current_row())
            .as_("current_total"),
        )
        .from_(orders)
        .compile()
    )

    assert query == (
        'SELECT SUM("a"."total") OVER (ORDER BY "a"."id" '
        'ROWS CURRENT ROW) AS "current_total" FROM "orders" AS "a"'
    )
    assert params == ()


def test_named_window_clause() -> None:
    orders = Table("orders")
    by_user = Window(
        "by_user",
        partition_by=orders.user_id,
        order_by=orders.id,
        rows=("UNBOUNDED PRECEDING", "CURRENT ROW"),
    )

    query, params = (
        select(
            orders.user_id,
            orders.id,
            func.sum(orders.total).over("by_user").as_("running_total"),
        )
        .from_(orders)
        .window(by_user)
        .compile()
    )
    direct_query, direct_params = (
        select(
            orders.user_id,
            orders.id,
            func.sum(orders.total).over(by_user).as_("running_total"),
        )
        .from_(orders)
        .window(
            "by_user",
            partition_by=orders.user_id,
            order_by=orders.id,
            rows=("UNBOUNDED PRECEDING", "CURRENT ROW"),
        )
        .compile()
    )

    expected = (
        'SELECT "a"."user_id", "a"."id", '
        'SUM("a"."total") OVER "by_user" AS "running_total" '
        'FROM "orders" AS "a" '
        'WINDOW "by_user" AS (PARTITION BY "a"."user_id" '
        'ORDER BY "a"."id" ROWS BETWEEN UNBOUNDED PRECEDING '
        "AND CURRENT ROW)"
    )
    assert query == expected
    assert direct_query == expected
    assert params == ()
    assert direct_params == ()


def test_order_by_window_function() -> None:
    orders = Table("orders")
    total_by_user = Alias("total_by_user")

    query, params = (
        select(
            orders.user_id,
            func.sum(orders.total)
            .over(partition_by=orders.user_id)
            .as_(total_by_user),
        )
        .from_(orders)
        .order_by(total_by_user, descending=True)
        .compile()
    )

    assert query == (
        'SELECT "a"."user_id", '
        'SUM("a"."total") OVER (PARTITION BY "a"."user_id") '
        'AS "total_by_user" FROM "orders" AS "a" '
        'ORDER BY "total_by_user" DESC'
    )
    assert params == ()


def test_over_named_window_rejects_inline_parts() -> None:
    orders = Table("orders")

    with pytest.raises(ValueError, match="Named window references"):
        func.sum(orders.total).over(
            "by_user",
            partition_by=orders.user_id,
        )


def test_window_rejects_rows_and_range() -> None:
    orders = Table("orders")

    with pytest.raises(ValueError, match="Only one frame"):
        Window(
            partition_by=orders.user_id,
            rows=("UNBOUNDED PRECEDING", "CURRENT ROW"),
            range_=("UNBOUNDED PRECEDING", "CURRENT ROW"),
        )
