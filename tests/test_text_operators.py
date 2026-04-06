"""Tests for text-based SQL operators."""

from sql_fusion import Table, select, text_op
from sql_fusion.composite_table import AliasRegistry


def test_text_operator_renders_sql_and_params() -> None:
    users = Table("users")
    registry = AliasRegistry()

    sql, params = text_op(users.tags, "@>", ["coffee"]).to_sql(registry)

    assert sql == '"a"."tags" @> ?'
    assert params == (["coffee"],)


def test_text_operator_combines_with_regular_conditions() -> None:
    users = Table("users")

    query, params = (
        select(users.name)
        .from_(users)
        .where((users.name == "bob") | text_op(users.tags, "@>", ["coffee"]))
        .compile()
    )

    assert query == (
        'SELECT "a"."name" FROM "users" AS "a" '
        'WHERE ("a"."name" = ? OR "a"."tags" @> ?)'
    )
    assert params == ("bob", ["coffee"])
