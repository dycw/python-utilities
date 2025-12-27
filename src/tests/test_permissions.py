from __future__ import annotations

from dataclasses import dataclass

from hypothesis import given
from hypothesis.strategies import booleans
from pytest import mark, param, raises

from utilities.hypothesis import permissions, sentinels
from utilities.permissions import (
    Permissions,
    PermissionsFromIntError,
    PermissionsFromOctalError,
    PermissionsFromTextError,
)
from utilities.sentinel import Sentinel


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

    @given(
        perms=permissions(),
        user_read=booleans() | sentinels(),
        user_write=booleans() | sentinels(),
        user_execute=booleans() | sentinels(),
        group_read=booleans() | sentinels(),
        group_write=booleans() | sentinels(),
        group_execute=booleans() | sentinels(),
        others_read=booleans() | sentinels(),
        others_write=booleans() | sentinels(),
        others_execute=booleans() | sentinels(),
    )
    def test_main(
        self,
        *,
        perms: Permissions,
        user_read: bool | Sentinel,
        user_write: bool | Sentinel,
        user_execute: bool | Sentinel,
        group_read: bool | Sentinel,
        group_write: bool | Sentinel,
        group_execute: bool | Sentinel,
        others_read: bool | Sentinel,
        others_write: bool | Sentinel,
        others_execute: bool | Sentinel,
    ) -> None:
        result = perms.replace(
            user_read=user_read,
            user_write=user_write,
            user_execute=user_execute,
            group_read=group_read,
            group_write=group_write,
            group_execute=group_execute,
            others_read=others_read,
            others_write=others_write,
            others_execute=others_execute,
        )
        if not isinstance(user_read, Sentinel):
            assert result.user_read is user_read
        if not isinstance(user_write, Sentinel):
            assert result.user_write is user_write
        if not isinstance(user_execute, Sentinel):
            assert result.user_execute is user_execute
        if not isinstance(group_read, Sentinel):
            assert result.group_read is group_read
        if not isinstance(group_write, Sentinel):
            assert result.group_write is group_write
        if not isinstance(group_execute, Sentinel):
            assert result.group_execute is group_execute
        if not isinstance(others_read, Sentinel):
            assert result.others_read is others_read
        if not isinstance(others_write, Sentinel):
            assert result.others_write is others_write
        if not isinstance(others_execute, Sentinel):
            assert result.others_execute is others_execute

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

    def test_error_from_int(self) -> None:
        with raises(
            PermissionsFromIntError, match="Invalid integer for permissions; got 8"
        ):
            _ = Permissions.from_int(8)

    def test_error_from_octal(self) -> None:
        with raises(
            PermissionsFromOctalError, match="Invalid octal for permissions; got 0o7777"
        ):
            _ = Permissions.from_octal(0o7777)

    def test_error_from_text(self) -> None:
        with raises(
            PermissionsFromTextError,
            match="Invalid string for permissions; got 'u=xwr,g=,o='",
        ):
            _ = Permissions.from_text("u=xwr,g=,o=")
