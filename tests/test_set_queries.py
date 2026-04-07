"""Tests for SQL set operation support."""

from sql_fusion import Table, except_, intersect, select, union


def test_union_all_by_name_compiles_with_params() -> None:
    active_users = Table("active_users")
    archived_users = Table("archived_users")
    active_query = (
        select(active_users.id, active_users.name)
        .from_(active_users)
        .where_by(status="active")
    )
    archived_query = (
        select(archived_users.id, archived_users.name)
        .from_(archived_users)
        .where_by(status="inactive")
    )

    union_query = union(
        active_query,
        archived_query,
        all_=True,
        by_name=True,
    )
    query, params = union_query.compile()

    assert query == (
        'SELECT "a"."id", "a"."name" '
        'FROM "active_users" AS "a" '
        'WHERE "a"."status" = ? '
        "UNION ALL BY NAME "
        'SELECT "b"."id", "b"."name" '
        'FROM "archived_users" AS "b" '
        'WHERE "b"."status" = ?'
    )
    assert params == ("active", "inactive")


def test_intersect_all_compiles_with_params() -> None:
    users = Table("users")
    admins = Table("admins")
    users_query = select(users.id).from_(users).where_by(active=True)
    admins_query = select(admins.user_id).from_(admins).where_by(active=True)

    intersect_query = intersect(users_query, admins_query, all_=True)
    query, params = intersect_query.compile()

    assert query == (
        'SELECT "a"."id" FROM "users" AS "a" WHERE "a"."active" = ? '
        "INTERSECT ALL "
        'SELECT "b"."user_id" FROM "admins" AS "b" WHERE "b"."active" = ?'
    )
    assert params == (True, True)


def test_except_compiles_with_params() -> None:
    users = Table("users")
    banned_users = Table("banned_users")
    users_query = select(users.id).from_(users)
    banned_query = (
        select(banned_users.user_id)
        .from_(banned_users)
        .where_by(reason="bounced")
    )

    except_query = except_(users_query, banned_query)
    query, params = except_query.compile()

    assert query == (
        'SELECT "a"."id" FROM "users" AS "a" '
        "EXCEPT "
        'SELECT "b"."user_id" FROM "banned_users" AS "b" '
        'WHERE "b"."reason" = ?'
    )
    assert params == ("bounced",)
