import pytest

from duckdb_builder.query import Table, insert


def test_insert_builder_values_returns_sql() -> None:
    table = Table("users")

    query, params = (
        insert(table).values(id=1, name="Alice", status="active").as_tuple()
    )

    assert (
        query
        == 'INSERT INTO "users" ("id", "name", "status") VALUES (?, ?, ?)'
    )
    assert params == (1, "Alice", "active")


def test_insert_constructor_values_still_supported() -> None:
    table = Table("users")

    query, params = insert(table).values(name="Bob", city="Berlin").as_tuple()

    assert query == 'INSERT INTO "users" ("name", "city") VALUES (?, ?)'
    assert params == ("Bob", "Berlin")


def test_insert_values_method_merges_calls() -> None:
    table = Table("users")

    query, params = (
        insert(table)
        .values(id=2)
        .values(name="Carol", status="pending")
        .as_tuple()
    )

    assert (
        query
        == 'INSERT INTO "users" ("id", "name", "status") VALUES (?, ?, ?)'
    )
    assert params == (2, "Carol", "pending")


def test_insert_values_must_provide_values_once() -> None:
    table = Table("users")

    builder = insert(table)
    with pytest.raises(ValueError, match="No values provided for insert"):
        builder.as_tuple()

    with pytest.raises(ValueError, match="No values provided for insert"):
        builder.values().as_tuple()
