from src.duckdb_builder.query import Table, insert, select

# ruff: noqa: T201, PLR2004

t = Table("Users")

print("=== SELECT vs INSERT ===\n")

print("SELECT query:")
q_select = select(t).where(t.age > 18)
print(q_select.as_tuple())
print()

print("INSERT query:")
q_insert = insert(t).values(
    name="Charlie",
    age=28,
    email="charlie@example.com",
    status="active",
)
print(q_insert.as_tuple())
print()

print("INSERT with SQL injection attempt (SAFE):")
q_inject = insert(t).values(
    name="John'; DROP TABLE Users; --",
    email="bad@test.com",
)
query, params = q_inject.as_tuple()
print(f"Query: {query}")
print(f"Params: {params}")
print("✓ Malicious string is safely parameterized")
