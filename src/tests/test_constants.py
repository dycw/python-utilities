from __future__ import annotations

from typing import assert_never

from pytest import mark, param

from utilities.constants import (
    EFFECTIVE_GROUP_NAME,
    EFFECTIVE_USER_NAME,
    IS_LINUX,
    IS_MAC,
    IS_NOT_LINUX,
    IS_NOT_MAC,
    IS_NOT_WINDOWS,
    IS_WINDOWS,
    MAX_PID,
    ROOT_GROUP_NAME,
    ROOT_USER_NAME,
)
from utilities.platform import SYSTEM
from utilities.types import System
from utilities.typing import get_args


class TestGroupName:
    @mark.parametrize(
        "group",
        [
            param(ROOT_GROUP_NAME, id="root"),
            param(EFFECTIVE_GROUP_NAME, id="effective"),
        ],
    )
    def test_main(self, *, group: str | None) -> None:
        match SYSTEM:
            case "windows":  # skipif-not-windows
                assert group is None
            case "mac" | "linux":  # skipif-windows
                assert isinstance(group, str)
            case never:
                assert_never(never)


class TestMaxPID:
    def test_main(self) -> None:
        match SYSTEM:
            case "windows":  # skipif-not-windows
                assert MAX_PID is None
            case "mac":  # skipif-not-macos
                assert isinstance(MAX_PID, int)
            case "linux":  # skipif-not-linux
                assert isinstance(MAX_PID, int)
            case never:
                assert_never(never)


class TestSystem:
    def test_main(self) -> None:
        assert SYSTEM in get_args(System)

    @mark.parametrize(
        "predicate",
        [
            param(IS_WINDOWS, id="IS_WINDOWS"),
            param(IS_MAC, id="IS_MAC"),
            param(IS_LINUX, id="IS_LINUX"),
            param(IS_NOT_WINDOWS, id="IS_NOT_WINDOWS"),
            param(IS_NOT_MAC, id="IS_NOT_MAC"),
            param(IS_NOT_LINUX, id="IS_NOT_LINUX"),
        ],
    )
    def test_predicates(self, *, predicate: bool) -> None:
        assert isinstance(predicate, bool)


class TestUserName:
    @mark.parametrize(
        "user",
        [param(ROOT_USER_NAME, id="root"), param(EFFECTIVE_USER_NAME, id="effective")],
    )
    def test_main(self, *, user: str | None) -> None:
        match SYSTEM:
            case "windows":  # skipif-not-windows
                assert user is None
            case "mac" | "linux":  # skipif-windows
                assert isinstance(user, str)
            case never:
                assert_never(never)
