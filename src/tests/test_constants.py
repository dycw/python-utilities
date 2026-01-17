from __future__ import annotations

from typing import assert_never

from pytest import mark, param

from utilities.constants import (
    EFFECTIVE_GROUP_NAME,
    EFFECTIVE_USER_NAME,
    ROOT_GROUP_NAME,
    ROOT_USER_NAME,
)
from utilities.platform import SYSTEM


class TestGroupName:
    @mark.parametrize(
        "group",
        [
            param(ROOT_GROUP_NAME, id="root"),
            param(EFFECTIVE_GROUP_NAME, id="effective"),
        ],
    )
    def test_constant(self, *, group: str | None) -> None:
        match SYSTEM:
            case "windows":  # skipif-not-windows
                assert group is None
            case "mac" | "linux":  # skipif-windows
                assert isinstance(group, str)
            case never:
                assert_never(never)


class TestUserName:
    @mark.parametrize(
        "user",
        [param(ROOT_USER_NAME, id="root"), param(EFFECTIVE_USER_NAME, id="effective")],
    )
    def test_constant(self, *, user: str | None) -> None:
        match SYSTEM:
            case "windows":  # skipif-not-windows
                assert user is None
            case "mac" | "linux":  # skipif-windows
                assert isinstance(user, str)
            case never:
                assert_never(never)
