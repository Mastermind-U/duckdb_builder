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

- [Motivation](#motivation)
- [What You Get](#what-you-get)
- [Installation](#installation)
- [Public API](#public-api)
- [Quickstart: SQLite3](#quickstart-sqlite3)
- [Quickstart: DuckDB](#quickstart-duckdb)
- [Quickstart: psycopg3](#quickstart-psycopg3)
- [Query Basics](#query-basics)
- [Subquery Example](#subquery-example)
- [Set Operations](#set-operations)
- [Method Reference](#method-reference)
- [Functions](#functions)
- [CTEs](#ctes)
- [Custom Compile Expressions](#custom-compile-expressions)
- [What To Remember](#what-to-remember)
- [Feature Comparison](#feature-comparison)

## Motivation

SQL builders often look similar from the outside, but they make very different trade-offs in practice:

- some are template-driven and mainly render filter fragments
- some are lightweight CRUD helpers with a small API surface
- some are broad SQL toolkits with dialect systems and advanced composition features
- some keep SQL parameterized, while others render a finished SQL string directly

This README compares SQL Fusion with several other Python query builders so it is easier to see where the library fits and what it is intentionally optimized for.

### Why SQL Fusion?

SQL Fusion is built for the middle ground:

- it stays small and chainable instead of turning into a full ORM
- it keeps SQL parameterized by default, so the caller controls execution safely
- it gives you SQL-like syntax inside Python, with type hints and a clean separation of clauses, inspired by SQLAlchemy Core but without the heavy machinery of a full expression system
- it supports real SQL building blocks like joins, subqueries, CTEs, and grouping helpers without forcing a dialect-specific API
- it adds automatic alias management so common queries stay readable even as they grow
- it exposes `compile_expression()` for the cases where the final SQL needs a backend-specific rewrite

In short, the goal is to keep the ergonomics of a lightweight builder while still covering the parts of SQL that matter in real applications.

## What You Get

- `SELECT`, `INSERT`, `UPDATE`, and `DELETE` builders
- automatic table aliases
- composable conditions with `AND`, `OR`, and `NOT`
- joins, subqueries, and CTEs
- set operations with `UNION`, `INTERSECT`, and `EXCEPT`
- ordering and grouping with `GROUP BY`, `ROLLUP`, `CUBE`, and `GROUPING SETS`
- aggregate and custom SQL functions through `func`
- backend-specific SQL rewrites through compile expressions

## Installation

The project targets Python 3.14 or newer.
Install it from PyPI:

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
- `text(column, operator, value)` for backend-specific operators such as
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

For PostgreSQL-style array containment, `text()` lets you pass the operator
symbol directly:

```python
users = Table("users", Column("name"), Column("tags"))

query = (
    select(users.name)
    .from_(users)
    .where(users.name == "bob" or text_op(users.tags, "@>", ["coffee"]))
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

SQL Fusion supports compound queries through three small wrapper classes:

- `union(query1, query2, all=False, by_name=False)`
- `intersect(q1, q2, all_=False)`
- `except_(q1, q2, all_=False)`

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
query, params = union(active_users, archived_active_users, all=True).compile()
```

Use `by_name=True` when the two result sets expose the same logical columns in
different orders:

```python
left = select(users.id, users.name).from_(users)
right = select(archived_users.name, archived_users.id).from_(archived_users)

query, params = union(left, right, all=True, by_name=True).compile()
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
from sql_fusion import Alias, Table, func, select

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

## Feature Comparison


| Project | Focus | SQL coverage | SQL injection protected | Automatic alias management | Advanced features | Dialect/output model | Takeaway |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Py-QueryBuilder | Template-driven filter rendering | No direct CRUD builder; it renders a `WHERE` fragment into a Jinja template | Yes, via JinjaSQL qmark placeholders and a separate params list | No, subquery and join aliases are template-defined rather than auto-managed by the builder | Nested rule groups, operator mapping, field pruning | Jinja2 + JinjaSQL, SQL formatting | Best for UI-driven search forms, not for composing full statements |
| simple-query-builder-python | Small mutable CRUD helper | `SELECT`, `INSERT`, `UPDATE`, `DELETE` | Mostly yes, because execution uses `?` placeholders and a params tuple; `get_sql(with_values=True)` can inline values for display | No, subquery and join aliases are supplied manually in the input data | `JOIN`, `GROUP BY`, `HAVING`, `UNION`, `EXCEPT`, `INTERSECT`, `LIMIT`, `OFFSET` | SQLite-first, raw SQL string builder | Simple and approachable, but the SQL surface is modest |
| sqlquerybuilder | Django-ORM-style queryset wrapper | Basic read/write queries | No, it renders a ready SQL string with values embedded into the query text | No, subquery and join aliases are handled manually in query strings | Filters and excludes, joins, grouping, ordering, `extra()`, slicing, `with_nolock()` | SQLite-oriented, with SQL Server pagination branches in code | Convenient for ORM-like chaining, but not aimed at deep SQL composition |
| python-sql | Rich Pythonic SQL builder | `SELECT`, `INSERT`, `UPDATE`, `DELETE` | Yes, it keeps placeholders separate from args and can switch param styles via flavor | Partial, it can auto-alias tables and some subqueries, while join aliases are still often explicit | `JOIN`, subqueries, CTEs, `DISTINCT ON`, windows, `RETURNING`, `MERGE`, `UNION` / `INTERSECT` / `EXCEPT` | Dialect/flavor system with multiple param styles | Very broad SQL coverage and strong backend flexibility |
| PyPika | Mature fluent query builder | `SELECT`, `INSERT`, `UPDATE`, `DELETE` | No by default, it renders literal SQL strings with values injected into the output | Partial, it auto-aliases some subqueries and duplicate joins, but most table and join aliases are explicit | `JOIN`, subqueries, CTEs, set operations, analytics/window helpers, DDL support | Dialect-aware with vendor-specific extensions | One of the broadest and most extensible builders in the set |
| SQLFactory | General-purpose SQL builder | `SELECT`, `INSERT`, `UPDATE`, `DELETE` | Yes, it emits placeholders and keeps args separately | No, subquery and join aliases are mostly explicit and part of the statement shape | `JOIN`, subselects, CTEs, window functions, set operations, `INSERT ... SELECT`, MySQL-style duplicate-key handling | MySQL / SQLite / PostgreSQL / Oracle / custom dialects, async execution helpers | Full-featured and explicit, with a heavier API than lightweight builders |
| SQL Fusion | Lightweight chainable builder | `SELECT`, `INSERT`, `UPDATE`, `DELETE` | Yes, it returns `(sql, params)` and leaves binding to the caller | Yes, it auto-assigns stable table aliases and reuses them for subqueries and joins | `JOIN` variants including `CROSS`, `SEMI`, `ANTI`, subqueries, recursive CTEs, `ROLLUP`, `CUBE`, `GROUPING SETS`, functions, comments, `EXPLAIN` / `ANALYZE`, `DELETE RETURNING` | Backend-agnostic, `compile_expression()` hook for rewrites | Best when you want a compact, composable builder with post-processing hooks and no execution layer |

## Syntax Comparison

The examples below are representative shapes, not copy-paste snippets for every library. Where a library exposes a `Table`
object, the snippet uses it.

| Project | Typical syntax shape |
| --- | --- |
| SQL Fusion | `users = Table("users"); orders = Table("orders"); select(users.id, users.name).from_(users).join(orders, users.id == orders.user_id).where(users.active == True).compile()` |
| PyPika | `users = Table("users"); orders = Table("orders"); Query.from_(users).join(orders).on(users.id == orders.user_id).select(users.id, users.name).where(users.active == True).get_sql()` |
| python-sql | `user = Table("users"); tuple(user.select(user.name, where=user.active == True))` |
| SQLFactory | `users = Table("users"); orders = Table("orders"); Select(users.id, users.name, table=users, join=[Join(orders, Eq("users.id", "orders.user_id"))]).where(Eq("users.active", True))` |
| simple-query-builder-python | `qb.select("users").where([["active", "=", True]]).join("orders", on=[["users.id", "=", "orders.user_id"]]).all()` |
| sqlquerybuilder | `Queryset("users").filter(active=True).join("orders", on="users.id=orders.user_id")` |
| Py-QueryBuilder | `QueryBuilder("app.users", filters).render("query.sql", query)` |
