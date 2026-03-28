import pytest

from duckdb_builder.query import Table, select


def test_group_by_single_column() -> None:
    table = Table("users")
    query, params = (
        select(table.department, table.total)
        .from_(table)
        .group_by(table.department)
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."department", "a"."total" FROM "users" AS "a" GROUP BY "a"."department"'
    )
    assert params == ()


def test_group_by_multiple_columns() -> None:
    table = Table("sales")
    query, params = (
        select(table.city, table.street_name, table.income)
        .from_(table)
        .group_by(table.city, table.street_name)
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."city", "a"."street_name", "a"."income" FROM "sales" AS "a" GROUP BY "a"."city", "a"."street_name"'
    )
    assert params == ()


def test_group_by_with_where_clause() -> None:
    table = Table("employees")
    query, params = (
        select(table.department, table.salary)
        .from_(table)
        .where_by(active=True)
        .group_by(table.department)
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."department", "a"."salary" FROM "employees" AS "a" WHERE "a"."active" = ? GROUP BY "a"."department"'
    )
    assert params == (True,)


def test_group_by_with_having_condition() -> None:
    table = Table("orders")
    query, params = (
        select(table.customer_id, table.total_amount)
        .from_(table)
        .group_by(table.customer_id)
        .having_by(total_amount=">1000")
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."customer_id", "a"."total_amount" FROM "orders" AS "a" GROUP BY "a"."customer_id" HAVING "a"."total_amount" = ?'
    )
    assert params == (">1000",)


def test_group_by_with_where_and_having() -> None:
    table = Table("transactions")
    query, params = (
        select(table.account_id, table.amount)
        .from_(table)
        .where(table.status == "completed")
        .group_by(table.account_id)
        .having_by(amount=">500")
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."account_id", "a"."amount" FROM "transactions" AS "a" WHERE "a"."status" = ? GROUP BY "a"."account_id" HAVING "a"."amount" = ?'
    )
    assert params == ("completed", ">500")


def test_group_by_all_basic() -> None:
    table = Table("users")
    query, params = select().from_(table).group_by().as_tuple()
    assert query == 'SELECT * FROM "users" AS "a" GROUP BY ALL'
    assert params == ()


def test_group_by_all_with_specific_columns() -> None:
    table = Table("users")
    query, params = (
        select(table.name, table.value).from_(table).group_by().as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."name", "a"."value" FROM "users" AS "a" GROUP BY ALL'
    )
    assert params == ()


def test_group_by_all_with_where_clause() -> None:
    table = Table("users")
    query, params = (
        select().from_(table).where_by(active=True).group_by().as_tuple()
    )
    assert (
        query
        == 'SELECT * FROM "users" AS "a" WHERE "a"."active" = ? GROUP BY ALL'
    )
    assert params == (True,)


def test_group_by_all_with_having() -> None:
    table = Table("users")
    query, params = (
        select().from_(table).group_by().having_by(value=">100").as_tuple()
    )
    assert (
        query
        == 'SELECT * FROM "users" AS "a" GROUP BY ALL HAVING "a"."value" = ?'
    )
    assert params == (">100",)


def test_group_by_all_with_where_and_having() -> None:
    table = Table("data")
    query, params = (
        select(table.id, table.value)
        .from_(table)
        .where_by(type="numeric")
        .group_by()
        .having_by(value=">50")
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."id", "a"."value" FROM "data" AS "a" WHERE "a"."type" = ? GROUP BY ALL HAVING "a"."value" = ?'
    )
    assert params == ("numeric", ">50")


def test_group_by_rollup_two_columns() -> None:
    table = Table("sales")
    query, params = (
        select(table.city, table.street_name, table.income)
        .from_(table)
        .group_by_rollup(table.city, table.street_name)
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."city", "a"."street_name", "a"."income" FROM "sales" AS "a" GROUP BY ROLLUP ("a"."city", "a"."street_name")'
    )
    assert params == ()


def test_group_by_rollup_single_column() -> None:
    table = Table("users")
    query, params = (
        select(table.department, table.count)
        .from_(table)
        .group_by_rollup(table.department)
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."department", "a"."count" FROM "users" AS "a" GROUP BY ROLLUP ("a"."department")'
    )
    assert params == ()


def test_group_by_rollup_with_where_clause() -> None:
    table = Table("orders")
    query, params = (
        select(table.year, table.month, table.sales)
        .from_(table)
        .where_by(status="completed")
        .group_by_rollup(table.year, table.month)
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."year", "a"."month", "a"."sales" FROM "orders" AS "a" WHERE "a"."status" = ? GROUP BY ROLLUP ("a"."year", "a"."month")'
    )
    assert params == ("completed",)


def test_group_by_rollup_with_having() -> None:
    table = Table("sales")
    query, params = (
        select(table.city, table.region, table.total)
        .from_(table)
        .group_by_rollup(table.city, table.region)
        .having_by(total=">1000")
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."city", "a"."region", "a"."total" FROM "sales" AS "a" GROUP BY ROLLUP ("a"."city", "a"."region") HAVING "a"."total" = ?'
    )
    assert params == (">1000",)


def test_group_by_rollup_without_columns_raises_error() -> None:
    table = Table("users")
    with pytest.raises(
        ValueError, match="group_by_rollup\\(\\) requires at least one column",
    ):
        select().from_(table).group_by_rollup()


def test_group_by_cube_two_columns() -> None:
    table = Table("sales")
    query, params = (
        select(table.city, table.street_name, table.income)
        .from_(table)
        .group_by_cube(table.city, table.street_name)
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."city", "a"."street_name", "a"."income" FROM "sales" AS "a" GROUP BY CUBE ("a"."city", "a"."street_name")'
    )
    assert params == ()


def test_group_by_cube_single_column() -> None:
    table = Table("products")
    query, params = (
        select(table.category, table.quantity)
        .from_(table)
        .group_by_cube(table.category)
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."category", "a"."quantity" FROM "products" AS "a" GROUP BY CUBE ("a"."category")'
    )
    assert params == ()


def test_group_by_cube_with_where_clause() -> None:
    table = Table("analytics")
    query, params = (
        select(table.dimension1, table.dimension2, table.value)
        .from_(table)
        .where_by(year=2024)
        .group_by_cube(table.dimension1, table.dimension2)
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."dimension1", "a"."dimension2", "a"."value" FROM "analytics" AS "a" WHERE "a"."year" = ? GROUP BY CUBE ("a"."dimension1", "a"."dimension2")'
    )
    assert params == (2024,)


def test_group_by_cube_with_having() -> None:
    table = Table("metrics")
    query, params = (
        select(table.region, table.product, table.sum_value)
        .from_(table)
        .group_by_cube(table.region, table.product)
        .having_by(sum_value=">5000")
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."region", "a"."product", "a"."sum_value" FROM "metrics" AS "a" GROUP BY CUBE ("a"."region", "a"."product") HAVING "a"."sum_value" = ?'
    )
    assert params == (">5000",)


def test_group_by_cube_without_columns_raises_error() -> None:
    table = Table("data")
    with pytest.raises(
        ValueError, match="group_by_cube\\(\\) requires at least one column",
    ):
        select().from_(table).group_by_cube()


def test_group_by_grouping_sets_multiple_sets() -> None:
    table = Table("sales")
    query, params = (
        select(table.city, table.street_name, table.income)
        .from_(table)
        .group_by_grouping_sets(
            (table.city, table.street_name),
            (table.city,),
            (table.street_name,),
            (),
        )
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."city", "a"."street_name", "a"."income" FROM "sales" AS "a" GROUP BY GROUPING SETS (("a"."city", "a"."street_name"), ("a"."city"), ("a"."street_name"), ())'
    )
    assert params == ()


def test_group_by_grouping_sets_single_set() -> None:
    table = Table("users")
    query, params = (
        select(table.department, table.count)
        .from_(table)
        .group_by_grouping_sets((table.department,))
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."department", "a"."count" FROM "users" AS "a" GROUP BY GROUPING SETS (("a"."department"))'
    )
    assert params == ()


def test_group_by_grouping_sets_with_empty_set() -> None:
    table = Table("data")
    query, params = (
        select(table.col1, table.col2, table.total)
        .from_(table)
        .group_by_grouping_sets((table.col1, table.col2), (table.col1,), ())
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."col1", "a"."col2", "a"."total" FROM "data" AS "a" GROUP BY GROUPING SETS (("a"."col1", "a"."col2"), ("a"."col1"), ())'
    )
    assert params == ()


def test_group_by_grouping_sets_with_where_clause() -> None:
    table = Table("orders")
    query, params = (
        select(table.year, table.month, table.amount)
        .from_(table)
        .where_by(status="confirmed")
        .group_by_grouping_sets((table.year, table.month), (table.year,), ())
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."year", "a"."month", "a"."amount" FROM "orders" AS "a" WHERE "a"."status" = ? GROUP BY GROUPING SETS (("a"."year", "a"."month"), ("a"."year"), ())'
    )
    assert params == ("confirmed",)


def test_group_by_grouping_sets_with_having() -> None:
    table = Table("reports")
    query, params = (
        select(table.region, table.zone, table.revenue)
        .from_(table)
        .group_by_grouping_sets(
            (table.region, table.zone), (table.region,), (),
        )
        .having_by(revenue=">10000")
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."region", "a"."zone", "a"."revenue" FROM "reports" AS "a" GROUP BY GROUPING SETS (("a"."region", "a"."zone"), ("a"."region"), ()) HAVING "a"."revenue" = ?'
    )
    assert params == (">10000",)


def test_group_by_grouping_sets_without_sets_raises_error() -> None:
    table = Table("data")
    with pytest.raises(
        ValueError,
        match="group_by_grouping_sets\\(\\) requires at least one set",
    ):
        select().from_(table).group_by_grouping_sets()


def test_having_without_group_by_raises_error() -> None:
    table = Table("users")
    with pytest.raises(
        ValueError, match="Cannot use having\\(\\) without group_by\\(\\)",
    ):
        select().from_(table).having()


def test_having_by_without_group_by_raises_error() -> None:
    table = Table("data")
    with pytest.raises(
        ValueError, match="Cannot use having_by\\(\\) without group_by\\(\\)",
    ):
        select().from_(table).having_by(count=">5")


def test_group_by_with_filter_chaining() -> None:
    table = Table("employees")
    query, params = (
        select(table.department, table.salary)
        .from_(table)
        .where_by(status="active")
        .where_by(salary=">50000")
        .group_by(table.department)
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."department", "a"."salary" FROM "employees" AS "a" WHERE ("a"."status" = ? AND "a"."salary" = ?) GROUP BY "a"."department"'
    )
    assert params == ("active", ">50000")


def test_group_by_rollup_with_multiple_having_conditions() -> None:
    table = Table("sales")
    query, params = (
        select(table.city, table.product, table.total)
        .from_(table)
        .group_by_rollup(table.city, table.product)
        .having_by(total=">1000")
        .having_by(city="New York")
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."city", "a"."product", "a"."total" FROM "sales" AS "a" GROUP BY ROLLUP ("a"."city", "a"."product") HAVING ("a"."total" = ? AND "a"."city" = ?)'
    )
    assert params == (">1000", "New York")


def test_group_by_cube_with_filter_and_having() -> None:
    table = Table("metrics")
    query, params = (
        select(table.category, table.subcategory, table.value)
        .from_(table)
        .where_by(year=2024)
        .group_by_cube(table.category, table.subcategory)
        .having_by(value=">100")
        .as_tuple()
    )
    assert (
        query
        == 'SELECT "a"."category", "a"."subcategory", "a"."value" FROM "metrics" AS "a" WHERE "a"."year" = ? GROUP BY CUBE ("a"."category", "a"."subcategory") HAVING "a"."value" = ?'
    )
    assert params == (2024, ">100")
