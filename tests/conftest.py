from typing import Iterator

import pytest

from duckdb_builder.composite_table import Table


@pytest.fixture(autouse=True)
def reset_table_alias_counter() -> Iterator[None]:
    """Reset Table alias counter before each test."""
    Table.reset_alias_counter()
    yield
    Table.reset_alias_counter()
