# Changelog

## 1.2.2 - 2026-04-07

### Added

- Fix arguments for set operations

## 1.2.1 - 2026-04-07

### Added

- Support for Python 3.11.

## 1.2.0 - 2026-04-07

### Added

- Support for SQL set operations:
  - `union(query1, query2, all=False, by_name=False)`
  - `intersect(q1, q2, all_=False)`
  - `except_(q1, q2, all_=False)`
- Public API exports for `union`, `intersect`, and `except_`.
- Integration coverage for SQLite compound queries using `UNION`, `INTERSECT`,
  and `EXCEPT`.
- DuckDB integration coverage for `UNION ALL BY NAME`.
- Unit tests for the new set operation builders and SQL rendering.
- README updates documenting the new set operation support.

### Notes

- `UNION ALL BY NAME` is especially useful when two result sets contain the
  same logical columns in different orders.
- The set operation builders preserve parameter ordering across both sides of
  the compound query.

## 1.1.0 - 2026-04-06

### Added

- `text_op(column, operator, value)` helper for backend-specific SQL operators.
- Support for raw text operators in conditions, including PostgreSQL array
  containment syntax such as `@>`.
- Public API export for `text_op`.
- README examples showing how to use `text_op()` for array containment filters.
- DuckDB integration coverage for PostgreSQL-style array containment on
  `TEXT[]` values.

### Notes

- `text_op()` is intended for dialect-specific operators that are not covered by
  the built-in comparison helpers.
- DuckDB supports the listed array operators on list-typed columns, so the
  new integration test is executed in the default test suite.

## 1.0.1 - 2026-04-05

### Added

- `Table` now accepts predefined `Column` objects at construction time.
- Predeclared table columns are available through attribute access at runtime.
- Public API export for `Column` to support table column declarations.
- README examples for the new column declaration style.

### Notes

- Legacy dynamic table column access is still supported for existing code.

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
