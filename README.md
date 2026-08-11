# sql_fusion: Python SQL Query Builder

[![PyPI](https://img.shields.io/pypi/v/sql_fusion.svg)](https://pypi.org/project/sql_fusion/)
[![Python](https://img.shields.io/pypi/pyversions/sql_fusion.svg)](https://pypi.org/project/sql_fusion/)
[![Source](https://img.shields.io/badge/source-GitHub-24292f.svg)](https://github.com/Mastermind-U/sql_fusion)

sql_fusion is a lightweight Python SQL query builder with a fluent,
composable API and zero runtime dependencies. It helps you build
parameterized SQL queries in Python without an ORM, then returns the SQL
string and parameter tuple for your own database connection layer.

Use sql_fusion when you want a Python query builder for SQLite, DuckDB,
PostgreSQL-style DB-API drivers, or other backends where generated SQL and
placeholder adaptation fit your execution layer.

- PyPI package: [sql_fusion](https://pypi.org/project/sql_fusion/)
- GitHub repository: [Mastermind-U/sql_fusion](https://github.com/Mastermind-U/sql_fusion)

It focuses on one job: building SQL expressions and statements while keeping
execution outside the library.

- build parameterized SQL with a chainable Python API
- keep the query syntax readable
- stay flexible enough for SQLite3, DuckDB, psycopg3, and other DB-API style backends

The library does not execute SQL itself. It returns:

- the SQL string
- the parameter tuple

That makes it easy to plug into your own connection layer.

## Table of Contents

- [Motivation](#motivation)
- [Links](#links)
- [Python SQL Query Builder Features](#python-sql-query-builder-features)
- [Installation](#installation)
- [Public API](#public-api)
- [Quick Start: Python Query Builder for SQLite](#quick-start-python-query-builder-for-sqlite)
- [Quick Start: Python Query Builder for DuckDB](#quick-start-python-query-builder-for-duckdb)
- [PostgreSQL / psycopg3 Placeholder Example](#postgresql--psycopg3-placeholder-example)
- [Fluent SQL Builder API Basics](#fluent-sql-builder-api-basics)
- [Subquery Example](#subquery-example)
- [Set Operations](#set-operations)
- [Method Reference](#method-reference)
- [Functions](#functions)
- [CTEs](#ctes)
- [Backend-Specific SQL Output with Compile Expressions](#backend-specific-sql-output-with-compile-expressions)
- [What To Remember](#what-to-remember)
- [PyPika and SQLAlchemy Alternatives](#pypika-and-sqlalchemy-alternatives)
- [FAQ](#faq)
- [Python SQL Query Builder Comparison](#python-sql-query-builder-comparison)

## Motivation

SQL builders often look similar from the outside, but they make very different trade-offs in practice:

- some are template-driven and mainly render filter fragments
- some are lightweight CRUD helpers with a small API surface
- some are broad SQL toolkits with dialect systems and advanced composition features
- some keep SQL parameterized, while others render a finished SQL string directly

This README compares sql_fusion with several other Python query builders so it
is easier to see where the library fits and what it is intentionally optimized
for.

## Links

- PyPI: https://pypi.org/project/sql_fusion/
- GitHub: https://github.com/Mastermind-U/sql_fusion
- Documentation: https://github.com/Mastermind-U/sql_fusion/blob/main/README.md

### Why sql_fusion?

sql_fusion is built for the middle ground: a lightweight Python SQL builder
for people who want to compose SQL in Python without adopting an ORM or a
large database toolkit.

- it has zero runtime dependencies
- it exposes a chainable, fluent SQL builder API
- it returns `(sql, params)` and leaves execution to the caller
- it is not an ORM and does not manage sessions, models, migrations, or schemas
- it provides type hints across the public API for editor and static-analysis support
- it builds `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements
- it supports joins, subqueries, CTEs, set operations, grouping helpers, functions, and window expressions
- it adds automatic table alias management for composed queries
- it exposes `compile_expression()` for placeholder rewrites and backend-specific SQL output

In short, the goal is to keep the ergonomics of a composable SQL query builder
for Python while still covering the SQL building blocks that matter in
application code.

### Why not SQLAlchemy ORM or Core?

SQLAlchemy is an excellent tool, but it is also a much heavier and more universal system:

- it brings a larger abstraction surface than this project needs
- it is optimized for a broad ORM and Core ecosystem, not only for a small SQL builder
- some connectors and databases still do not have first-class SQLAlchemy integrations, which can make adoption less straightforward in mixed environments

SQLAlchemy Core is closer to sql_fusion than the full ORM, but it still carries more machinery than this project is meant to expose:

- it is part of a broader ecosystem with dialects, compilation layers, and extra conventions
- it can feel more verbose when you only want a small chainable builder
- some connectors and databases still do not have smooth SQLAlchemy Core support, so portability can depend on the backend

sql_fusion is intentionally narrower so it can stay lightweight, easy to embed,
and practical for DB-API style backends without extra complexity.

## Python SQL Query Builder Features

- `SELECT`, `INSERT`, `UPDATE`, and `DELETE` builders
- automatic table aliases
- composable conditions with `AND`, `OR`, and `NOT`
- joins, subqueries, and CTEs
- set operations with `UNION`, `INTERSECT`, and `EXCEPT`
- ordering and grouping with `GROUP BY`, `ROLLUP`, `CUBE`, and `GROUPING SETS`
- aggregate and custom SQL functions through `func`
- backend-specific SQL rewrites through compile expressions

## Installation

sql_fusion targets Python 3.11 or newer. Install the Python SQL query builder
from PyPI:

```bash
pip install sql_fusion
```
```bash
uv add sql_fusion
```

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
from sql_fusion import (
    Alias,
    Column,
    Table,
    delete,
    except_,
    func,
    insert,
    intersect,
    select,
    union,
    text_op,
    update,
)
```

### Core Objects

- `Table` represents a real table or a subquery.
- `Column` is the reusable column object used by `Table` when you want to
  predeclare columns.
- `select` creates a `SELECT` builder.
- `insert` creates an `INSERT` builder.
- `update` creates an `UPDATE` builder.
- `delete` creates a `DELETE` builder.
- `func` is a dynamic SQL function registry.
- `text_op` builds a condition with a raw SQL operator such as `@>`.
- `Alias` represents a reusable SQL alias for aggregate expressions and
  `HAVING` conditions.

## Quick Start: Python Query Builder for SQLite

SQLite3 is the easiest way to start because it accepts the default `?`
placeholders directly. This example shows how to build SQL queries in Python
without an ORM and execute the generated `(sql, params)` pair with `sqlite3`.

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

## Quick Start: Python Query Builder for DuckDB

DuckDB works with the default `?` placeholders directly, so you can execute
queries without any SQL rewriting. The same fluent SQL builder API composes the
query; DuckDB handles execution.

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

## PostgreSQL / psycopg3 Placeholder Example

psycopg3 usually expects `%s` placeholders instead of `?`. For a Python query
builder targeting Postgres through psycopg3, the simplest approach is to add a
compile expression that rewrites placeholders at the very end.

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

## Fluent SQL Builder API Basics

sql_fusion acts as a Python SQL expression builder: tables expose columns,
columns produce conditions, and query objects compose clauses before compiling
to SQL and parameters.

### Tables and Aliases

`Table` automatically assigns aliases in creation order:

```python
users = Table("users")   # alias "a"
orders = Table("orders") # alias "b"
```

`Table` can also wrap a subquery. In practice, you usually pass a query
builder directly to `from_()` or `join()`, and the library wraps it for you.

If you want explicit, hint-friendly columns on a table instance, pass them
when you create it:

```python
from sql_fusion import Column, Table, select


users = Table(
    "users",
    Column("id"),
    Column("name"),
)


query = select(users.id, users.name).from_(users)
```

This style keeps the column list declared in one place and is verified at
runtime when you access `users.id` / `users.name`.

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
- `text_op(column, operator, value)` for backend-specific operators such as
  PostgreSQL array containment (`@>`).

Use `|` for SQL `OR`. Python's `or` cannot be overloaded for SQL expressions.

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

For PostgreSQL-style array containment, `text_op()` lets you pass the operator
symbol directly:

```python
users = Table("users", Column("name"), Column("tags"))

query = (
    select(users.name)
    .from_(users)
    .where((users.name == "bob") | text_op(users.tags, "@>", ["coffee"]))
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

### Subquery Example

Subqueries work both as a source table and inside conditions.

```python
orders = Table("orders")
users = Table("users")

paid_order_user_ids = (
    select(orders.user_id)
    .from_(orders)
    .where_by(status="paid")
)

query, params = (
    select(users.id, users.name)
    .from_(users)
    .where(users.id.in_(paid_order_user_ids))
    .compile()
)
```

The same idea also works in `FROM`:

```python
orders = Table("orders")

paid_orders = (
    select(orders.user_id, orders.total)
    .from_(orders)
    .where_by(status="paid")
)

query, params = select().from_(paid_orders).compile()
```

## Set Operations

sql_fusion supports compound queries through three small wrapper classes:

- `union(query1, query2, all_=False, by_name=False)`
- `intersect(query1, query2, all_=False)`
- `except_(query1, query2, all_=False)`

Each builder accepts two query objects and returns a new query that compiles to
the matching SQL set operation.

### `union(...)`

```python
users = Table("users")
archived_users = Table("archived_users")

active_users = select(users.id, users.name).from_(users).where_by(status="active")
archived_active_users = (
    select(archived_users.id, archived_users.name)
    .from_(archived_users)
    .where_by(status="active")
)

query, params = union(active_users, archived_active_users).compile()
```

Use `all=True` for `UNION ALL`:

```python
query, params = union(active_users, archived_active_users, all_=True).compile()
```

Use `by_name=True` when the two result sets expose the same logical columns in
different orders:

```python
left = select(users.id, users.name).from_(users)
right = select(archived_users.name, archived_users.id).from_(archived_users)

query, params = union(left, right, all_=True, by_name=True).compile()
```

### `intersect(...)`

```python
users = Table("users")
premium_users = Table("premium_users")

active_users = select(users.id).from_(users).where_by(status="active")
premium_active_users = (
    select(premium_users.id).from_(premium_users).where_by(status="active")
)

query, params = intersect(active_users, premium_active_users).compile()
```

Use `all_=True` for `INTERSECT ALL`:

```python
query, params = intersect(
    active_users,
    premium_active_users,
    all_=True,
).compile()
```

### `except_(...)`

```python
users = Table("users")
banned_users = Table("banned_users")

all_users = select(users.id).from_(users)
banned_user_ids = select(banned_users.id).from_(banned_users)

query, params = except_(all_users, banned_user_ids).compile()
```

Use `all_=True` for `EXCEPT ALL`:

```python
query, params = except_(all_users, banned_user_ids, all_=True).compile()
```

These builders preserve the parameter order from left to right, so the returned
`params` tuple can be passed directly to DB-API drivers.

### Having Example

```python
orders = Table("orders")
count_orders = Alias("count_orders")

query = (
    select(
        orders.status,
        func.count(orders.id).as_(count_orders),
        func.sum(orders.total),
    )
    .from_(orders)
    .group_by(orders.status)
    .having(count_orders >= 3)
)
```

`HAVING` works after grouping and is ideal for filtering aggregates, for
example "only statuses with at least 3 orders".

Because `as` is a reserved Python keyword, the method is exposed as
`as_()`.

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
| `left_join(table, condition, *, is_outer=False)` | Add a `LEFT JOIN` | Set `is_outer=True` for `LEFT OUTER JOIN`. |
| `right_join(table, condition, *, is_outer=False)` | Add a `RIGHT JOIN` | Set `is_outer=True` for `RIGHT OUTER JOIN`. |
| `full_join(table, condition, *, is_outer=True)` | Add a `FULL JOIN` | Set `is_outer=False` for `FULL JOIN`; defaults to `FULL OUTER JOIN`. |
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
| `window(name, ...)` | Add a named `WINDOW` clause | Supports `base`, `partition_by`, `order_by`, `descending`, `rows`, `range_`, `groups`, and `exclude`. |
| `order_by(*columns, descending=False)` | Add `ORDER BY` | Repeated calls merge columns. `descending=True` applies `DESC`. |

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
from sql_fusion import Alias, Table, Window, func, select

orders = Table("orders")
count_orders = Alias("count_orders")

query = select(
    func.count("*"),
    func.count(orders.id).as_(count_orders),
    func.sum(orders.total),
    func.coalesce(orders.status, "unknown"),
).from_(orders)
```

Examples:

- `func.count("*")` -> `COUNT(*)`
- `func.sum(table.total)` -> `SUM("a"."total")`
- `func.my_custom_func(table.name)` -> `MY_CUSTOM_FUNC("a"."name")`
- nested calls are supported, for example `func.round(func.avg(...), 2)`
- `func.count(table.id).as_(Alias("count_orders"))` -> `COUNT("a"."id") AS "count_orders"`

String and numeric literals are parameterized automatically.

## Window Functions

Call `.over()` on any `func` expression to render an `OVER` clause.

```python
orders = Table("orders")

query, params = (
    select(
        orders.user_id,
        orders.id,
        func.rank()
        .over(
            partition_by=orders.user_id,
            order_by=orders.total,
            descending=True,
        )
        .as_("total_rank"),
        func.sum(orders.total)
        .over(
            partition_by=orders.user_id,
            order_by=orders.id,
            rows=Window.Rows.between(
                Window.Rows.unbounded_preceding(),
                Window.Rows.current_row(),
            ),
        )
        .as_("running_total"),
    )
    .from_(orders)
    .compile()
)
```

Reusable named windows are supported with `select.window()`.

```python
query, params = (
    select(
        orders.user_id,
        orders.id,
        func.sum(orders.total).over("by_user").as_("running_total"),
    )
    .from_(orders)
    .window(
        "by_user",
        partition_by=orders.user_id,
        order_by=orders.id,
        rows=Window.Rows.between(Window.Rows.unbounded_preceding(), Window.Rows.current_row()),
    )
    .compile()
)
```

Examples:

- `func.row_number().over(order_by=orders.id)` -> `ROW_NUMBER() OVER (ORDER BY "a"."id")`
- `func.sum(orders.total).over(partition_by=orders.user_id)` -> `SUM("a"."total") OVER (PARTITION BY "a"."user_id")`
- `func.rank().over("ranked")` -> `RANK() OVER "ranked"`
- `.window("ranked", partition_by=..., order_by=...)` -> `WINDOW "ranked" AS (...)`
- `rows=Window.Rows.between(Window.Rows.unbounded_preceding(), Window.Rows.current_row())` -> `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
- `range_=Window.Range.between(Window.Range.interval_preceding(3, "DAYS"), Window.Range.interval_following(3, "DAYS"))` -> `RANGE BETWEEN INTERVAL 3 DAYS PRECEDING AND INTERVAL 3 DAYS FOLLOWING`
- `exclude="CURRENT ROW"` appends `EXCLUDE CURRENT ROW` to the window specification
- `func.sum(table.amount).filter(table.kind != "x").over(order_by=table.id)` -> `SUM("a"."amount") FILTER (WHERE "a"."kind" != ?) OVER (...)`
- `Window(base="ranked", rows=Window.Rows.between(Window.Rows.unbounded_preceding(), Window.Rows.current_row()))` renders a chained window specification

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

## Backend-Specific SQL Output with Compile Expressions

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

## PyPika and SQLAlchemy Alternatives

sql_fusion may fit when you are looking for a PyPika alternative or a
SQLAlchemy query builder alternative for a narrower job: build parameterized
SQL in Python, then execute it through your own connection layer.

SQLAlchemy is a broad database toolkit with ORM, Core, dialect, engine, and
connection-management layers. sql_fusion does not try to replace that full
ecosystem. It is a lightweight Python SQL builder for projects that want a
small fluent API and no execution layer.

PyPika is a mature fluent SQL builder with broad dialect features. sql_fusion
is positioned differently: it returns `(sql, params)` by default, keeps runtime
dependencies at zero, and uses compile hooks when the final SQL needs a
backend-specific placeholder or syntax rewrite.

## FAQ

### What is sql_fusion?

sql_fusion is a Python SQL query builder. It provides a fluent, composable API
for building SQL statements and expressions in Python, then compiling them to a
SQL string and parameter tuple.

### Is sql_fusion an ORM?

No. sql_fusion is not an ORM. It does not define models, track sessions,
execute queries, manage relationships, run migrations, or own your connection
layer. It is for building SQL queries without an ORM.

### What databases does sql_fusion support?

sql_fusion generates parameterized SQL with `?` placeholders and quoted
identifiers. The test suite verifies execution with SQLite and DuckDB. The
README also shows how to adapt placeholders for PostgreSQL through psycopg3.
Other DB-API style backends, including MySQL drivers, may work when the
generated SQL and any placeholder rewrites match the database, but they are not
claimed as fully tested dialects.

### How is sql_fusion different from SQLAlchemy?

SQLAlchemy is a comprehensive toolkit with ORM, Core, dialect, engine, and
connection abstractions. sql_fusion is intentionally smaller: it builds SQL and
parameters, then leaves execution and database integration to your own code.
That makes it useful when you want a SQLAlchemy query builder alternative for a
small, composable query-building layer.

### Is sql_fusion an alternative to PyPika?

sql_fusion can be considered a PyPika alternative when you want a lightweight,
typed SQL query builder for Python that returns `(sql, params)` instead of only
a rendered SQL string. PyPika remains a mature project with broad dialect and
SQL-generation features, so the right choice depends on the API and output
model you need.

### Does sql_fusion support type hints / static typing?

Yes. sql_fusion is implemented with type hints and exposes typed public APIs,
so editors and static analysis tools can reason about the builder methods and
return values. It can fit searches for a typed or type-safe SQL builder in
Python at the API level, but it should not be described as a full compile-time
schema validator.

## Python SQL Query Builder Comparison


| Project | Focus | SQL coverage | SQL injection protected | Automatic alias management | Window functions | Advanced features | Dialect/output model | Takeaway |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Py-QueryBuilder | Template-driven filter rendering | No direct CRUD builder; it renders a `WHERE` fragment into a Jinja template | Yes, via JinjaSQL qmark placeholders and a separate params list | No, subquery and join aliases are template-defined rather than auto-managed by the builder | No dedicated API; possible only by writing window SQL in templates | Nested rule groups, operator mapping, field pruning | Jinja2 + JinjaSQL, SQL formatting | Fits UI-driven search forms, not full-statement composition |
| simple-query-builder-python | Small mutable CRUD helper | `SELECT`, `INSERT`, `UPDATE`, `DELETE` | Mostly yes, because execution uses `?` placeholders and a params tuple; `get_sql(with_values=True)` can inline values for display | No, subquery and join aliases are supplied manually in the input data | No dedicated API | `JOIN`, `GROUP BY`, `HAVING`, `UNION`, `EXCEPT`, `INTERSECT`, `LIMIT`, `OFFSET` | SQLite-first, raw SQL string builder | Simple and approachable, but the SQL surface is modest |
| sqlquerybuilder | Django-ORM-style queryset wrapper | Basic read/write queries | No, it renders a ready SQL string with values embedded into the query text | No, subquery and join aliases are handled manually in query strings | Not general-purpose; uses `ROW_NUMBER() OVER (...)` internally for one SQL Server pagination path | Filters and excludes, joins, grouping, ordering, `extra()`, slicing, `with_nolock()` | SQLite-oriented, with SQL Server pagination branches in code | Convenient for ORM-like chaining, but not aimed at deep SQL composition |
| python-sql | Rich Pythonic SQL builder | `SELECT`, `INSERT`, `UPDATE`, `DELETE` | Yes, it keeps placeholders separate from args and can switch param styles via flavor | Partial, it can auto-alias tables and some subqueries, while join aliases are still often explicit | Rich support: named windows, aggregate/window functions, `FILTER`, `ROWS` / `RANGE` / `GROUPS`, `EXCLUDE` | `JOIN`, subqueries, CTEs, `DISTINCT ON`, windows, `RETURNING`, `MERGE`, `UNION` / `INTERSECT` / `EXCEPT` | Dialect/flavor system with multiple param styles | Very broad SQL coverage and strong backend flexibility |
| PyPika | Mature fluent query builder | `SELECT`, `INSERT`, `UPDATE`, `DELETE` | No by default, it renders literal SQL strings with values injected into the output | Partial, it auto-aliases some subqueries and duplicate joins, but most table and join aliases are explicit | Broad analytics helpers: ranking/value/aggregate windows, partition/order, `ROWS` / `RANGE`, and `QUALIFY` | `JOIN`, subqueries, CTEs, set operations, analytics/window helpers, DDL support | Dialect-aware with vendor-specific extensions | One of the broadest and most extensible builders in the set |
| SQLFactory | General-purpose SQL builder | `SELECT`, `INSERT`, `UPDATE`, `DELETE` | Yes, it emits placeholders and keeps args separately | No, subquery and join aliases are mostly explicit and part of the statement shape | Yes: `WindowableFunction.over(...)`, ranking/value functions, partition/order, and frame objects | `JOIN`, subselects, CTEs, window functions, set operations, `INSERT ... SELECT`, MySQL-style duplicate-key handling | MySQL / SQLite / PostgreSQL / Oracle / custom dialects, async execution helpers | Full-featured and explicit, with a heavier API than lightweight builders |
| sql_fusion | Lightweight chainable builder | `SELECT`, `INSERT`, `UPDATE`, `DELETE` | Yes, it returns `(sql, params)` and leaves binding to the caller | Yes, it auto-assigns stable table aliases and reuses them for subqueries and joins | Yes: `func.*().over(...)`, `select.window(...)`, `FILTER`, named/chained windows, `ROWS` / `RANGE` / `GROUPS`, `EXCLUDE` | `JOIN` variants including `CROSS`, `SEMI`, `ANTI`, subqueries, recursive CTEs, `ROLLUP`, `CUBE`, `GROUPING SETS`, functions, comments, `EXPLAIN` / `ANALYZE`, `DELETE RETURNING` | Backend-agnostic, `compile_expression()` hook for rewrites | Fits compact, composable query building with post-processing hooks and no execution layer |

## Syntax Comparison

The examples below are representative shapes, not copy-paste snippets for every library. Where a library exposes a `Table`
object, the snippet uses it.

| Project | Typical syntax shape |
| --- | --- |
| sql_fusion | `users = Table("users"); orders = Table("orders"); select(users.id, users.name).from_(users).join(orders, users.id == orders.user_id).where(users.active == True).compile()` |
| PyPika | `users = Table("users"); orders = Table("orders"); Query.from_(users).join(orders).on(users.id == orders.user_id).select(users.id, users.name).where(users.active == True).get_sql()` |
| python-sql | `user = Table("users"); tuple(user.select(user.name, where=user.active == True))` |
| SQLFactory | `users = Table("users"); orders = Table("orders"); Select(users.id, users.name, table=users, join=[Join(orders, Eq("users.id", "orders.user_id"))]).where(Eq("users.active", True))` |
| simple-query-builder-python | `qb.select("users").where([["active", "=", True]]).join("orders", on=[["users.id", "=", "orders.user_id"]]).all()` |
| sqlquerybuilder | `Queryset("users").filter(active=True).join("orders", on="users.id=orders.user_id")` |
| Py-QueryBuilder | `QueryBuilder("app.users", filters).render("query.sql", query)` |

### Window Syntax Comparison

| Project | Representative window syntax |
| --- | --- |
| sql_fusion | `func.sum(orders.total).filter(orders.status != "cancelled").over(partition_by=orders.user_id, order_by=orders.created_at, rows=Window.Rows.between(Window.Rows.unbounded_preceding(), Window.Rows.current_row())).as_("running_total")` |
| PyPika | `an.Sum(t.amount).over(t.account_id).orderby(t.date).rows(an.Preceding(), an.CURRENT_ROW).as_("running_total")` |
| python-sql | `Sum(t.amount, filter_=t.status != "cancelled", window=Window([t.user_id], order_by=[t.created_at], frame="ROWS", start="UNBOUNDED PRECEDING", end="CURRENT ROW"))` |
| SQLFactory | `Sum("amount").over(partition_by=["user_id"], order=[("created_at", Direction.ASC)], frame=Frame(FrameType.ROWS, Preceding(), CurrentRow()))` |
| simple-query-builder-python | No dedicated window API; pass a raw selected expression if needed. |
| sqlquerybuilder | No general-purpose window API; `ROW_NUMBER() OVER (...)` appears only in an internal pagination branch. |
| Py-QueryBuilder | No dedicated window API; write the window expression in the Jinja SQL template. |
