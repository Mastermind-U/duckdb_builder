# Changelog

## 1.0.0 - 2026-04-05

Initial stable release of `sql_fusion`.

### Added

- Chainable SQL builders for `SELECT`, `INSERT`, `UPDATE`, and `DELETE`.
- Public API exports:
  - `Table`
  - `Alias`
  - `select`
  - `insert`
  - `update`
  - `delete`
  - `func`
- Automatic table aliasing with stable, generated aliases.
- Support for quoted SQL identifiers and fully parameterized queries.
- Subquery support in `FROM`, `WHERE`, `SET`, `IN`, and `NOT IN`.
- Common table expressions with `WITH` and `WITH RECURSIVE`.
- SQL function calls through a dynamic `func` registry.
- Expression support for:
  - comparison operators: `==`, `!=`, `<`, `<=`, `>`, `>=`
  - boolean composition: `AND`, `OR`, `NOT`
  - arithmetic composition: `+`, `-`, `*`, `/`
  - `LIKE`, `ILIKE`, `IN`, `NOT IN`
- Query annotations and compile-time transformations:
  - `comment()`
  - `before_clause()`
  - `after_clause()`
  - `compile_expression()`
  - `explain()`
  - `analyze()`
- `SELECT` features:
  - `DISTINCT`
  - `WHERE` and `where_by()`
  - `JOIN`
  - `LEFT JOIN`
  - `RIGHT JOIN`
  - `FULL OUTER JOIN`
  - `CROSS JOIN`
  - `SEMI JOIN`
  - `ANTI JOIN`
  - `GROUP BY`
  - `GROUP BY ALL`
  - `GROUP BY ROLLUP`
  - `GROUP BY CUBE`
  - `GROUPING SETS`
  - `HAVING` and `having_by()`
  - `ORDER BY`
  - `LIMIT`
  - `OFFSET`
- `INSERT` features:
  - `values()`
  - merged value calls
  - `INSERT OR REPLACE`
  - `INSERT OR IGNORE`
- `UPDATE` features:
  - `set()`
  - merged assignment calls
  - column-to-column assignments
  - arithmetic expressions in assignments
  - function calls in assignments
  - subqueries in assignments
- `DELETE` features:
  - `from_()`
  - `WHERE`
  - `RETURNING`
  - `RETURNING *`
  - function expressions in `RETURNING`
- Backend-friendly output shape of `(sql, params)` for easy execution with DB-API style drivers.
- Compatibility focus for SQLite3, DuckDB, and other drivers that accept parameterized SQL.

### Notes

- Runtime dependencies are intentionally kept at zero.
- The project targets Python 3.14+.
