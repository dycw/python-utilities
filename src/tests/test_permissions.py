from __future__ import annotations

from dataclasses import dataclass

from hypothesis import given
from pytest import mark, param

from utilities.hypothesis import permissions
from utilities.permissions import Permissions


@dataclass(kw_only=True, slots=True)
class _Case:
    perms: Permissions
    human_int: int
    octal: int
    text: str


_CASES: list[_Case] = [
    _Case(perms=Permissions(), human_int=0, octal=0o0, text="u=,g=,o="),
    _Case(
        perms=Permissions(user_read=True), human_int=400, octal=0o400, text="u=r,g=,o="
    ),
    _Case(
        perms=Permissions(group_write=True), human_int=20, octal=0o020, text="u=,g=w,o="
    ),
    _Case(
        perms=Permissions(others_execute=True),
        human_int=1,
        octal=0o001,
        text="u=,g=,o=x",
    ),
    _Case(
        perms=Permissions(user_read=True, user_write=True),
        human_int=600,
        octal=0o600,
        text="u=rw,g=,o=",
    ),
    _Case(
        perms=Permissions(user_read=True, group_execute=True),
        human_int=410,
        octal=0o410,
        text="u=r,g=x,o=",
    ),
]


class TestPermissions:
    @given(perms=permissions())
    def test_int(self, *, perms: Permissions) -> None:
        assert Permissions.from_int(int(perms)) == perms

    @mark.parametrize(
        ("perms", "expected"), [param(case.perms, case.human_int) for case in _CASES]
    )
    def test_int_examples(self, *, perms: Permissions, expected: str) -> None:
        result = int(perms)
        assert result == expected
        assert Permissions.from_int(result) == perms

    @given(perms=permissions())
    def test_octal(self, *, perms: Permissions) -> None:
        assert Permissions.from_octal(perms.octal) == perms

    @mark.parametrize(
        ("perms", "expected"), [param(case.perms, case.octal) for case in _CASES]
    )
    def test_octal_examples(self, *, perms: Permissions, expected: str) -> None:
        result = perms.octal
        assert result == expected
        assert Permissions.from_octal(result) == perms

    @given(perms=permissions())
    def test_text(self, *, perms: Permissions) -> None:
        assert Permissions.from_text(str(perms)) == perms

    @mark.parametrize(
        ("perms", "expected"), [param(case.perms, case.text) for case in _CASES]
    )
    def test_text_examples(self, *, perms: Permissions, expected: str) -> None:
        result = str(perms)
        assert result == expected
        assert Permissions.from_text(result) == perms
