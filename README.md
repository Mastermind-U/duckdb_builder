# SQL Fusion

SQL Fusion is a lightweight, chainable SQL query builder for Python with zero dependencies.

It focuses on one job:

- build parameterized SQL
- keep the query syntax readable
- stay flexible enough for SQLite3, DuckDB, PostgreSQL, and other DB-API style backends

The library does not execute SQL itself. It returns:

- the SQL string
- the parameter tuple

That makes it easy to plug into your own connection layer.

## Table of Contents

- [What You Get](#what-you-get)
- [Installation](#installation)
- [Public API](#public-api)
- [Quickstart: SQLite3](#quickstart-sqlite3)
- [Quickstart: DuckDB](#quickstart-duckdb)
- [Quickstart: psycopg3](#quickstart-psycopg3)
- [Query Basics](#query-basics)
- [Method Reference](#method-reference)
- [Functions](#functions)
- [CTEs](#ctes)
- [Custom Compile Expressions](#custom-compile-expressions)
- [What To Remember](#what-to-remember)
- [Potential Additions](#potential-additions)

## What You Get

- `SELECT`, `INSERT`, `UPDATE`, and `DELETE` builders
- automatic table aliases
- composable conditions with `AND`, `OR`, and `NOT`
- joins, subqueries, and CTEs
- aggregate and custom SQL functions through `func`
- backend-specific SQL rewrites through compile expressions

## Installation

The project targets Python 3.14 or newer.

For local development:

```bash
uv sync
```

Or install it in editable mode:

```bash
pip install -e .
```

## Public API

```python
from sql_fusion import Table, delete, func, insert, select, update
```

### Core Objects

- `Table` represents a real table or a subquery.
- `select` creates a `SELECT` builder.
- `insert` creates an `INSERT` builder.
- `update` creates an `UPDATE` builder.
- `delete` creates a `DELETE` builder.
- `func` is a dynamic SQL function registry.

## Quickstart: SQLite3

SQLite3 is the easiest way to start because it accepts the default `?` placeholders directly.

```python
import sqlite3

from sql_fusion import Table, insert, select, update

users = Table("users")

conn = sqlite3.connect(":memory:")
conn.execute(
    """
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL
    )
    """,
)

insert_query, insert_params = (
    insert(users)
    .values(id=1, name="Alice", status="active")
    .compile()
)
conn.execute(insert_query, insert_params)

select_query, select_params = (
    select(users.id, users.name)
    .from_(users)
    .where_by(status="active")
    .compile()
)
rows = conn.execute(select_query, select_params).fetchall()

update_query, update_params = (
    update(users)
    .set(status="inactive")
    .where(users.id == 1)
    .compile()
)
conn.execute(update_query, update_params)
```

Expected style of generated SQL:

```sql
SELECT "a"."id", "a"."name" FROM "users" AS "a" WHERE "a"."status" = ?
```

## Quickstart: DuckDB

DuckDB works with the default `?` placeholders directly, so you can execute
queries without any SQL rewriting.

```python
import duckdb
from sql_fusion import Table, select


users = Table("users")

query = (
    select(users.id, users.name)
    .from_(users)
    .where(users.status == "active")
)

duck_sql, duck_params = query.compile()
duck_conn = duckdb.connect(":memory:")
duck_conn.execute("CREATE TABLE users (id INTEGER, name TEXT, status TEXT)")
duck_conn.execute(duck_sql, duck_params).fetchall()
```

## Quickstart: psycopg3

psycopg3 usually expects `%s` placeholders instead of `?`. The simplest way
to support it is to add a compile expression that rewrites placeholders at the
very end.

```python
from typing import Any

import psycopg

from sql_fusion import Table, select


def to_psycopg3(sql: str, params: tuple[Any, ...]) -> tuple[str, tuple[Any, ...]]:
    return sql.replace("?", "%s"), params


users = Table("users")

query = (
    select(users.id, users.name)
    .from_(users)
    .where(users.status == "active")
)

pg_sql, pg_params = query.compile_expression(to_psycopg3).compile()
pg_conn = psycopg.connect("dbname=example user=example password=example")
pg_conn.execute(pg_sql, pg_params).fetchall()
```

If you only target DuckDB, no rewrite is needed. If you target psycopg3, the
compile expression keeps the query builder backend-agnostic while still
producing driver-friendly SQL.

## Query Basics

### Tables and Aliases

`Table` automatically assigns aliases in creation order:

```python
users = Table("users")   # alias "a"
orders = Table("orders") # alias "b"
```

If you want a stable alias, provide one yourself:

```python
users = Table("users", alias="u")
```

`Table` can also wrap a subquery. In practice, you usually pass a query
builder directly to `from_()` or `join()`, and the library wraps it for you.

### Conditions

Columns support the usual comparison operators:

- `==`
- `!=`
- `<`
- `<=`
- `>`
- `>=`

They also support SQL helpers:

- `.like(pattern)`
- `.ilike(pattern)`
- `.in_(values)`
- `.not_in(values)`

Conditions can be combined with:

- `&` for `AND`
- `|` for `OR`
- `~` for `NOT`

Example:

```python
query = (
    select(users.id, users.name)
    .from_(users)
    .where(
        (users.age >= 18)
        & ((users.status == "active") | (users.status == "pending"))
        & users.country.not_in(["DE", "FR"])
    )
)
```

### Join Example

```python
users = Table("users")
orders = Table("orders")

query = (
    select(users.id, users.name, orders.total)
    .from_(users)
    .join(orders, users.id == orders.user_id)
    .where_by(status="active")
)
```

This produces a standard `INNER JOIN`. If you need a different join type, use:

- `left_join()`
- `right_join()`
- `full_join()`
- `cross_join()`
- `semi_join()`
- `anti_join()`

### Having Example

```python
orders = Table("orders")

query = (
    select(
        orders.status,
        func.count(orders.id),
        func.sum(orders.total),
    )
    .from_(orders)
    .group_by(orders.status)
    .having(func.count(orders.id) >= 3)
)
```

`HAVING` works after grouping and is ideal for filtering aggregates, for
example "only statuses with at least 3 orders".

## Method Reference

### Shared Query Methods

These methods are available on the shared query builders.

| Method | Purpose | Notes |
| --- | --- | --- |
| `where(*conditions)` | Add explicit conditions | Multiple conditions are combined with `AND`. Repeated calls merge safely. |
| `where_by(**kwargs)` | Build equality filters from keyword arguments | Uses the current `FROM` table alias. `where_by(status="active")` becomes `status = ?`. |
| `with_(recursive=False, **ctes)` | Add one or more CTEs | Repeated calls merge CTEs. `recursive=True` emits `WITH RECURSIVE`. |
| `compile_expression(fn)` | Add a final SQL transformation step | `fn` receives `(sql, params)` and must return `(sql, params)`. |
| `comment(text, hint=False)` | Prefix the query with a SQL comment | `hint=True` renders optimizer-style comments like `/*+ ... */`. |
| `before_clause(clause, text, hint=False)` | Insert a comment before a clause | `clause` is case-insensitive, such as `"FROM"` or `"UPDATE"`. |
| `after_clause(clause, text, hint=False)` | Insert a comment after a clause keyword | Useful for hints and debug annotations. |
| `explain(analyze=False, verbose=False)` | Wrap the query in `EXPLAIN` | Can be chained with other compile expressions. |
| `analyze(verbose=False)` | Shortcut for `EXPLAIN ANALYZE` | Equivalent to `explain(analyze=True, verbose=verbose)`. |
| `compile()` | Build the final SQL and parameters | Returns `(sql, params)`. |

### `select(...)`

```python
query = select(users.id, users.name)
```

Constructor:

- `select(*columns)`

If no columns are provided, the builder emits `SELECT *`.

#### `select` Methods

| Method | Purpose | Notes |
| --- | --- | --- |
| `from_(table)` | Set the source table or subquery | Accepts a `Table` or another query builder. |
| `join(table, condition)` | Add an `INNER JOIN` | The default join type. |
| `left_join(table, condition)` | Add a `LEFT JOIN` | Keeps unmatched left rows. |
| `right_join(table, condition)` | Add a `RIGHT JOIN` | Keeps unmatched right rows. |
| `full_join(table, condition)` | Add a `FULL OUTER JOIN` | Keeps rows from both sides. |
| `cross_join(table)` | Add a `CROSS JOIN` | No `ON` clause. |
| `semi_join(table, condition)` | Add a `SEMI JOIN` | Backend support depends on the database. |
| `anti_join(table, condition)` | Add an `ANTI JOIN` | Backend support depends on the database. |
| `limit(n)` | Limit the number of rows | `n` must be non-negative. |
| `offset(n)` | Skip the first `n` rows | `n` must be non-negative. |
| `distinct()` | Add `DISTINCT` | Safe to chain more than once. |
| `group_by(*columns)` | Add a standard `GROUP BY` | With no columns, emits `GROUP BY ALL`. |
| `group_by_rollup(*columns)` | Add `GROUP BY ROLLUP (...)` | Requires at least one column. |
| `group_by_cube(*columns)` | Add `GROUP BY CUBE (...)` | Requires at least one column. |
| `group_by_grouping_sets(*column_sets)` | Add `GROUPING SETS` | Requires at least one set. Empty tuples become `()`. |
| `having(*conditions)` | Add a `HAVING` clause | Requires grouping. |
| `having_by(**kwargs)` | Add equality-based `HAVING` filters | Requires grouping. |

### `insert(table, or_replace=False, or_ignore=False)`

```python
query = insert(users).values(id=1, name="Alice")
```

#### `insert` Methods

| Method | Purpose | Notes |
| --- | --- | --- |
| `values(**kwargs)` | Add column values | Multiple calls merge into one row payload. |
| `compile()` | Build `INSERT` SQL | Raises if no values were provided. |

Behavior notes:

- `or_replace=True` emits `INSERT OR REPLACE`
- `or_ignore=True` emits `INSERT OR IGNORE`
- both flags together raise an error

### `update(table)`

```python
query = update(users).set(status="inactive")
```

#### `update` Methods

| Method | Purpose | Notes |
| --- | --- | --- |
| `set(**kwargs)` | Add assignments for the `SET` clause | Multiple calls merge assignments. |
| `where(...)` / `where_by(...)` | Restrict the rows to update | Works like the shared query methods. |
| `compile()` | Build `UPDATE` SQL | Raises if no values were provided. |

Behavior notes:

- column references in `SET` are table-qualified by default
- if a backend needs a different style, use `compile_expression(...)`

### `delete(table=None)`

```python
query = delete().from_(users).where(users.id == 1)
```

#### `delete` Methods

| Method | Purpose | Notes |
| --- | --- | --- |
| `from_(table)` | Set the target table | Required before compiling. |
| `returning(*columns)` | Add a `RETURNING` clause | With no arguments, emits `RETURNING *`. Multiple calls merge columns. |
| `where(...)` / `where_by(...)` | Restrict the rows to delete | Works like the shared query methods. |
| `compile()` | Build `DELETE` SQL | Returns `(sql, params)`. |

## Functions

`func` is a dynamic SQL function registry. It converts attribute access into an uppercased SQL function name.

```python
from sql_fusion import Table, func, select

orders = Table("orders")

query = select(
    func.count("*"),
    func.sum(orders.total),
    func.coalesce(orders.status, "unknown"),
).from_(orders)
```

Examples:

- `func.count("*")` -> `COUNT(*)`
- `func.sum(table.total)` -> `SUM("a"."total")`
- `func.my_custom_func(table.name)` -> `MY_CUSTOM_FUNC("a"."name")`
- nested calls are supported, for example `func.round(func.avg(...), 2)`

String and numeric literals are parameterized automatically.

## CTEs

CTEs are supported through `with_()`.

```python
orders = Table("orders")
users = Table("users")
paid_orders = Table("paid_orders")

paid_orders_cte = (
    select(orders.user_id, orders.total)
    .from_(orders)
    .where_by(status="paid")
)

query, params = (
    select(users.name, func.sum(paid_orders.total))
    .with_(paid_orders=paid_orders_cte)
    .from_(paid_orders)
    .join(users, paid_orders.user_id == users.id)
    .group_by(users.name)
    .compile()
)
```

### CTE Rules

- `with_()` accepts query-like objects only
- repeated `with_()` calls merge CTEs
- `recursive=True` emits `WITH RECURSIVE`
- parameter order is preserved across all nested queries
- CTE names are quoted automatically

### Recursive CTE Example

```python
nodes = Table("nodes")

tree = select(nodes.id, nodes.parent_id).from_(nodes).where_by(active=True)

query, params = (
    select()
    .with_(recursive=True, tree=tree)
    .from_(Table("tree"))
    .compile()
)
```

## Custom Compile Expressions

`compile_expression()` is the escape hatch for backend-specific SQL tweaks.
It receives the final SQL string and parameter tuple, then returns a modified pair.

This is useful for:

- placeholder rewrites
- backend-specific syntax adjustments
- adding `ORDER BY`, `LIMIT`, or other final SQL fragments

### Example: psycopg3 Placeholder Rewrite

```python
from typing import Any


def to_psycopg3(sql: str, params: tuple[Any, ...]) -> tuple[str, tuple[Any, ...]]:
    return sql.replace("?", "%s"), params
```

### Example: Append Sorting and Pagination

```python
def order_by_second_column_desc_limit_two(
    sql: str,
    params: tuple[Any, ...],
) -> tuple[str, tuple[Any, ...]]:
    return f"{sql} ORDER BY 2 DESC LIMIT 2", params
```

Then attach it to any query:

```python
query, params = (
    select(users.id, users.name)
    .from_(users)
    .compile_expression(order_by_second_column_desc_limit_two)
    .compile()
)
```

### Built-in Compile Helpers

The library also exposes a few built-in compile-time helpers:

- `comment(text, hint=False)` prefixes the query with a comment
- `before_clause(clause, text, hint=False)` injects a comment before a clause
- `after_clause(clause, text, hint=False)` injects a comment after a clause
- `explain()` wraps the query in `EXPLAIN`
- `analyze()` wraps the query in `EXPLAIN ANALYZE`

## What To Remember

- `compile()` returns `(sql, params)`
- SQL identifiers are quoted with double quotes
- values are parameterized with placeholders
- query builders are chainable
- repeated calls to many methods merge rather than overwrite
- backend support still depends on the database you execute against
