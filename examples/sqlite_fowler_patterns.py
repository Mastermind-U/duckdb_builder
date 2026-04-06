"""Enterprise patterns.

Data mappers and unit of work,
using SQL Fusion to build queries.
Allows to make ORM-like patterns without an actual ORM,
and to keep SQL generation separate from execution and domain logic.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any, Protocol, Self, TypeVar, cast

from sql_fusion import Table, delete, insert, select, update


@dataclass(slots=True, eq=False)
class User:
    id: int | None
    name: str
    email: str


@dataclass(slots=True, eq=False)
class Post:
    id: int | None
    user_id: int
    title: str
    body: str


EntityT = TypeVar("EntityT")


class Mapper(Protocol[EntityT]):
    def find_by_id(self, key: int) -> EntityT | None: ...

    def find_all(self) -> list[EntityT]: ...

    def insert(self, entity: EntityT) -> None: ...

    def update(self, entity: EntityT) -> None: ...

    def delete(self, entity: EntityT) -> None: ...


class IdentityMap:
    """Cache loaded entities by their type and primary key."""

    def __init__(self) -> None:
        self._items: dict[tuple[type[Any], int], Any] = {}

    def get(self, entity_type: type[EntityT], key: int) -> EntityT | None:
        return cast("EntityT | None", self._items.get((entity_type, key)))

    def add(
        self,
        entity_type: type[EntityT],
        key: int,
        entity: EntityT,
    ) -> None:
        self._items[(entity_type, key)] = entity

    def remove(self, entity_type: type[EntityT], key: int) -> None:
        self._items.pop((entity_type, key), None)

    def snapshot(self) -> dict[str, list[int]]:
        result: dict[str, list[int]] = {}
        for (entity_type, key), _entity in self._items.items():
            result.setdefault(entity_type.__name__, []).append(key)

        for keys in result.values():
            keys.sort()

        return result

    def clear(self) -> None:
        self._items.clear()


class MapperRegistry:
    """Map domain classes to their mappers."""

    def __init__(self, identity_map: IdentityMap) -> None:
        self.identity_map = identity_map
        self._mappers: dict[type[Any], Mapper[Any]] = {}

    def register(
        self,
        entity_type: type[EntityT],
        mapper: Mapper[EntityT],
    ) -> None:
        self._mappers[entity_type] = cast("Mapper[Any]", mapper)

    def mapper_for(self, entity_type: type[EntityT]) -> Mapper[EntityT]:
        return cast("Mapper[EntityT]", self._mappers[entity_type])


class UserMapper:
    """A concrete mapper that uses sql_fusion to build SQLite statements."""

    table = Table("users")

    def __init__(
        self,
        connection: sqlite3.Connection,
        identity_map: IdentityMap,
    ) -> None:
        self._connection = connection
        self._identity_map = identity_map

    def _execute(self, query: Any) -> sqlite3.Cursor:
        sql, params = query.compile()
        print(sql)
        print(params)
        return self._connection.execute(sql, params)

    def _row_to_user(self, row: sqlite3.Row) -> User:
        user_id = int(row["id"])
        cached = self._identity_map.get(User, user_id)
        if cached is not None:
            return cached

        user = User(
            id=user_id,
            name=str(row["name"]),
            email=str(row["email"]),
        )
        self._identity_map.add(User, user_id, user)
        return user

    def find_by_id(self, key: int) -> User | None:
        cached = self._identity_map.get(User, key)
        if cached is not None:
            return cached

        query = (
            select(self.table.id, self.table.name, self.table.email)
            .from_(self.table)
            .where(self.table.id == key)
        )
        row = self._execute(query).fetchone()
        if row is None:
            return None
        return self._row_to_user(cast("sqlite3.Row", row))

    def find_all(self) -> list[User]:
        query = (
            select(self.table.id, self.table.name, self.table.email)
            .from_(self.table)
            .order_by(self.table.id)
        )
        rows = self._execute(query).fetchall()
        return [self._row_to_user(cast("sqlite3.Row", row)) for row in rows]

    def insert(self, entity: User) -> None:
        if entity.id is not None:
            raise ValueError("New entities must not have an id yet")

        query = insert(self.table).values(name=entity.name, email=entity.email)
        cursor = self._execute(query)
        entity.id = cursor.lastrowid
        if entity.id is None:
            raise RuntimeError("SQLite did not return a primary key")
        self._identity_map.add(User, entity.id, entity)

    def update(self, entity: User) -> None:
        if entity.id is None:
            raise ValueError("Persistent entities must have an id")

        query = (
            update(self.table)
            .set(name=entity.name, email=entity.email)
            .where(self.table.id == entity.id)
        )
        self._execute(query)
        self._identity_map.add(User, entity.id, entity)

    def delete(self, entity: User) -> None:
        if entity.id is None:
            raise ValueError("Persistent entities must have an id")

        query = delete(self.table).where(self.table.id == entity.id)
        self._execute(query)
        self._identity_map.remove(User, entity.id)


class PostMapper:
    """A concrete mapper for posts."""

    table = Table("posts")

    def __init__(
        self,
        connection: sqlite3.Connection,
        identity_map: IdentityMap,
    ) -> None:
        self._connection = connection
        self._identity_map = identity_map

    def _execute(self, query: Any) -> sqlite3.Cursor:
        sql, params = query.compile()
        print(sql)
        print(params)
        return self._connection.execute(sql, params)

    def _row_to_post(self, row: sqlite3.Row) -> Post:
        post_id = int(row["id"])
        cached = self._identity_map.get(Post, post_id)
        if cached is not None:
            return cached

        post = Post(
            id=post_id,
            user_id=int(row["user_id"]),
            title=str(row["title"]),
            body=str(row["body"]),
        )
        self._identity_map.add(Post, post_id, post)
        return post

    def find_by_id(self, key: int) -> Post | None:
        cached = self._identity_map.get(Post, key)
        if cached is not None:
            return cached

        query = (
            select(
                self.table.id,
                self.table.user_id,
                self.table.title,
                self.table.body,
            )
            .from_(self.table)
            .where(self.table.id == key)
        )
        row = self._execute(query).fetchone()
        if row is None:
            return None
        return self._row_to_post(cast("sqlite3.Row", row))

    def find_all(self) -> list[Post]:
        query = (
            select(
                self.table.id,
                self.table.user_id,
                self.table.title,
                self.table.body,
            )
            .from_(self.table)
            .order_by(self.table.id)
        )
        rows = self._execute(query).fetchall()
        return [self._row_to_post(cast("sqlite3.Row", row)) for row in rows]

    def insert(self, entity: Post) -> None:
        if entity.id is not None:
            raise ValueError("New entities must not have an id yet")

        query = insert(self.table).values(
            user_id=entity.user_id,
            title=entity.title,
            body=entity.body,
        )
        cursor = self._execute(query)
        entity.id = cursor.lastrowid
        if entity.id is None:
            raise RuntimeError("SQLite did not return a primary key")
        self._identity_map.add(Post, entity.id, entity)

    def update(self, entity: Post) -> None:
        if entity.id is None:
            raise ValueError("Persistent entities must have an id")

        query = (
            update(self.table)
            .set(user_id=entity.user_id, title=entity.title, body=entity.body)
            .where(self.table.id == entity.id)
        )
        self._execute(query)
        self._identity_map.add(Post, entity.id, entity)

    def delete(self, entity: Post) -> None:
        if entity.id is None:
            raise ValueError("Persistent entities must have an id")

        query = delete(self.table).where(self.table.id == entity.id)
        self._execute(query)
        self._identity_map.remove(Post, entity.id)


class UnitOfWork:
    """Collect new/dirty/removed entities and flush them as one transaction."""

    def __init__(
        self,
        connection: sqlite3.Connection,
        registry: MapperRegistry,
    ) -> None:
        self._connection = connection
        self._registry = registry
        self._new: list[Any] = []
        self._dirty: list[Any] = []
        self._removed: list[Any] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: object,
    ) -> None:
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

    @staticmethod
    def _contains(bucket: list[Any], entity: Any) -> bool:
        return any(item is entity for item in bucket)

    @staticmethod
    def _remove(bucket: list[Any], entity: Any) -> None:
        bucket[:] = [item for item in bucket if item is not entity]

    def register_new(self, entity: Any) -> None:
        self._remove(self._dirty, entity)
        self._remove(self._removed, entity)
        if not self._contains(self._new, entity):
            self._new.append(entity)

    def register_dirty(self, entity: Any) -> None:
        self._remove(self._removed, entity)
        if not self._contains(self._new, entity) and not self._contains(
            self._dirty,
            entity,
        ):
            self._dirty.append(entity)

    def register_removed(self, entity: Any) -> None:
        self._remove(self._new, entity)
        self._remove(self._dirty, entity)
        if not self._contains(self._removed, entity):
            self._removed.append(entity)

    def commit(self) -> None:
        for entity in self._new:
            mapper = self._registry.mapper_for(type(entity))  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
            mapper.insert(entity)  # pyright: ignore[reportUnknownMemberType]

        for entity in self._dirty:
            mapper = self._registry.mapper_for(type(entity))  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
            mapper.update(entity)  # pyright: ignore[reportUnknownMemberType]

        for entity in self._removed:
            mapper = self._registry.mapper_for(type(entity))  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
            mapper.delete(entity)  # pyright: ignore[reportUnknownMemberType]

        self._connection.commit()
        self._clear()

    def rollback(self) -> None:
        self._connection.rollback()
        self._clear()

    def _clear(self) -> None:
        self._new.clear()
        self._dirty.clear()
        self._removed.clear()


def create_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE
        )
        """,
    )
    connection.execute(
        """
        CREATE TABLE posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            title TEXT NOT NULL,
            body TEXT NOT NULL
        )
        """,
    )


def main() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row

    create_schema(connection)

    identity_map = IdentityMap()
    registry = MapperRegistry(identity_map)
    user_mapper = UserMapper(connection, identity_map)
    post_mapper = PostMapper(connection, identity_map)
    registry.register(User, user_mapper)
    registry.register(Post, post_mapper)

    alice = User(id=None, name="Alice", email="alice@example.com")
    bob = User(id=None, name="Bob", email="bob@example.com")

    with UnitOfWork(connection, registry) as uow:
        uow.register_new(alice)
        uow.register_new(bob)

    assert alice.id is not None
    assert bob.id is not None

    post_1 = Post(
        id=None,
        user_id=alice.id,
        title="Hello SQLite",
        body="First post body",
    )
    post_2 = Post(
        id=None,
        user_id=alice.id,
        title="Identity Map",
        body="Second post body",
    )

    with UnitOfWork(connection, registry) as uow:
        uow.register_new(post_1)
        uow.register_new(post_2)

    first_load = registry.mapper_for(User).find_by_id(alice.id)
    second_load = registry.mapper_for(User).find_by_id(alice.id)
    print("identity map hit:", first_load is second_load)

    assert first_load is not None
    first_load.name = "Alice Cooper"

    with UnitOfWork(connection, registry) as uow:
        uow.register_dirty(first_load)

    bob_from_cache = registry.mapper_for(User).find_by_id(bob.id)
    assert bob_from_cache is not None

    with UnitOfWork(connection, registry) as uow:
        uow.register_removed(bob_from_cache)

    print("posts for Alice:")
    for post in registry.mapper_for(Post).find_all():
        if post.user_id == alice.id:
            print(post)

    print("final rows:")
    for user in registry.mapper_for(User).find_all():
        print(user)

    print("identity map:")
    print(identity_map.snapshot())


if __name__ == "__main__":
    main()
