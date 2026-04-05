"""Tests for arithmetic SQL expressions."""

from sql_fusion import Table
from sql_fusion.composite_table import AliasRegistry


def test_column_arithmetic_operators_render_sql() -> None:
    """Test that arithmetic operators render SQL and params correctly."""
    users = Table("users")
    reg = AliasRegistry()

    add_sql, add_params = (users.counter + 1).to_sql(reg)
    sub_sql, sub_params = (users.counter - 1).to_sql(reg)
    mul_sql, mul_params = (users.counter * 2).to_sql(reg)
    div_sql, div_params = (users.counter / 2).to_sql(reg)
    radd_sql, radd_params = (1 + users.counter).to_sql(reg)
    rsub_sql, rsub_params = (1 - users.counter).to_sql(reg)
    rmul_sql, rmul_params = (2 * users.counter).to_sql(reg)
    rdiv_sql, rdiv_params = (2 / users.counter).to_sql(reg)

    assert add_sql == '"a"."counter" + ?'
    assert add_params == (1,)
    assert sub_sql == '"a"."counter" - ?'
    assert sub_params == (1,)
    assert mul_sql == '"a"."counter" * ?'
    assert mul_params == (2,)
    assert div_sql == '"a"."counter" / ?'
    assert div_params == (2,)
    assert radd_sql == '? + "a"."counter"'
    assert radd_params == (1,)
    assert rsub_sql == '? - "a"."counter"'
    assert rsub_params == (1,)
    assert rmul_sql == '? * "a"."counter"'
    assert rmul_params == (2,)
    assert rdiv_sql == '? / "a"."counter"'
    assert rdiv_params == (2,)


def test_nested_arithmetic_expression_keeps_parentheses_and_param_order() -> (
    None
):
    """Test nested arithmetic expressions keep grouping and parameter order."""
    users = Table("users")
    reg = AliasRegistry()

    expr = (users.counter + 1) * (users.score - 2)
    sql, params = expr.to_sql(reg)

    assert sql == '("a"."counter" + ?) * ("a"."score" - ?)'
    assert params == (1, 2)
