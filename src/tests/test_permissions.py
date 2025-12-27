from __future__ import annotations

from dataclasses import dataclass

from pytest import mark, param

from utilities.permissions import Permissions


@dataclass(kw_only=True, slots=True)
class _Case[T]:
    perms: Permissions
    human_int: int
    text: str


_CASES: list[_Case] = [
    _Case(perms=Permissions(), human_int=0, text="u=,g=,o="),
    _Case(perms=Permissions(user_read=True), human_int=400, text="u=r,g=,o="),
    _Case(perms=Permissions(group_write=True), human_int=20, text="u=,g=w,o="),
    _Case(perms=Permissions(others_execute=True), human_int=1, text="u=,g=,o=x"),
    _Case(
        perms=Permissions(user_read=True, user_write=True),
        human_int=600,
        text="u=rw,g=,o=",
    ),
    _Case(
        perms=Permissions(user_read=True, group_execute=True),
        human_int=410,
        text="u=r,g=x,o=",
    ),
]


class TestPermissions:
    @mark.parametrize(
        ("perms", "expected"), [param(case.perms, case.human_int) for case in _CASES]
    )
    def test_human_int(self, *, perms: Permissions, expected: str) -> None:
        result = perms.human_int
        assert result == expected
        assert Permissions.from_int(result) == perms

    @mark.parametrize(
        ("perms", "expected"), [param(case.perms, case.text) for case in _CASES]
    )
    def test_to_and_from_str(self, *, perms: Permissions, expected: str) -> None:
        result = str(perms)
        assert result == expected
        assert Permissions.from_str(result) == perms
