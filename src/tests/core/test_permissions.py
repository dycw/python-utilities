from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import booleans
from pytest import mark, param, raises

from utilities.constants import Sentinel
from utilities.core import (
    Permissions,
    _PermissionsFromHumanIntDigitError,
    _PermissionsFromHumanIntRangeError,
    _PermissionsFromIntError,
    _PermissionsFromTextError,
)
from utilities.hypothesis import permissions, sentinels, temp_paths

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(kw_only=True, slots=True)
class _Case:
    perms: Permissions
    human_int: int
    int_: int
    text: str


_CASES: list[_Case] = [
    _Case(perms=Permissions(), human_int=0, int_=0o0, text="u=,g=,o="),
    _Case(
        perms=Permissions(user_read=True), human_int=400, int_=0o400, text="u=r,g=,o="
    ),
    _Case(
        perms=Permissions(group_write=True), human_int=20, int_=0o020, text="u=,g=w,o="
    ),
    _Case(
        perms=Permissions(others_execute=True),
        human_int=1,
        int_=0o001,
        text="u=,g=,o=x",
    ),
    _Case(
        perms=Permissions(user_read=True, user_write=True),
        human_int=600,
        int_=0o600,
        text="u=rw,g=,o=",
    ),
    _Case(
        perms=Permissions(user_read=True, group_execute=True),
        human_int=410,
        int_=0o410,
        text="u=r,g=x,o=",
    ),
]


class TestPermissions:
    @given(root=temp_paths(), perms=permissions())
    def test_from_path(self, *, root: Path, perms: Permissions) -> None:
        path = root / "file.txt"
        path.touch()
        path.chmod(int(perms))
        assert Permissions.from_path(path) == perms

    @given(perms=permissions())
    def test_human_int(self, *, perms: Permissions) -> None:
        assert Permissions.from_human_int(perms.human_int) == perms

    @mark.parametrize(
        ("perms", "expected"), [param(case.perms, case.human_int) for case in _CASES]
    )
    def test_human_int_examples(self, *, perms: Permissions, expected: str) -> None:
        result = perms.human_int
        assert result == expected
        assert Permissions.from_human_int(result) == perms

    @given(perms=permissions())
    def test_int(self, *, perms: Permissions) -> None:
        assert Permissions.from_int(int(perms)) == perms

    @mark.parametrize(
        ("perms", "expected"), [param(case.perms, case.int_) for case in _CASES]
    )
    def test_int_examples(self, *, perms: Permissions, expected: str) -> None:
        result = int(perms)
        assert result == expected
        assert Permissions.from_int(result) == perms

    @given(perms=permissions())
    def test_new_int(self, *, perms: Permissions) -> None:
        assert Permissions.new(int(perms)) == perms

    @given(perms=permissions())
    def test_new_perms(self, *, perms: Permissions) -> None:
        assert Permissions.new(perms) == perms

    @given(perms=permissions())
    def test_new_text(self, *, perms: Permissions) -> None:
        assert Permissions.new(str(perms)) == perms

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
    def test_replace(
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

    def test_error_from_human_int_digit(self) -> None:
        with raises(
            _PermissionsFromHumanIntDigitError,
            match="Invalid human integer for permissions; got digit 8 in 8",
        ):
            _ = Permissions.from_human_int(8)

    def test_error_from_human_int_range(self) -> None:
        with raises(
            _PermissionsFromHumanIntRangeError,
            match="Invalid human integer for permissions; got 7777",
        ):
            _ = Permissions.from_human_int(7777)

    def test_error_from_int(self) -> None:
        with raises(
            _PermissionsFromIntError,
            match="Invalid integer for permissions; got 4095 = 0o7777",
        ):
            _ = Permissions.from_int(0o7777)

    def test_error_from_text(self) -> None:
        with raises(
            _PermissionsFromTextError,
            match="Invalid string for permissions; got 'u=xwr,g=,o='",
        ):
            _ = Permissions.from_text("u=xwr,g=,o=")
