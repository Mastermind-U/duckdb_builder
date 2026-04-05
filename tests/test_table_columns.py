"""Tests for Table columns support."""

import pytest

from sql_fusion import Column, Table, select

users = Table("users", Column("id"), Column("name"))


def test_table_with_predefined_columns_exposes_them() -> None:
    assert list(users.columns) == ["id", "name"]
    assert users.id is users.columns["id"]
    assert users.name is users.columns["name"]

    query, params = select(users.id, users.name).from_(users).compile()

    assert query == 'SELECT "a"."id", "a"."name" FROM "users" AS "a"'
    assert params == ()


def test_table_with_predefined_columns_missing_columns() -> None:
    with pytest.raises(KeyError):
        _ = users.age
