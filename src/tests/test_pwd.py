from __future__ import annotations

from typing import assert_never

from pytest import mark, param

from utilities.platform import SYSTEM
from utilities.pwd import EFFECTIVE_USER_NAME, ROOT_USER_NAME, get_uid_name


class TestUserName:
    def test_function(self) -> None:
        user = get_uid_name(0)
        assert isinstance(user, str) or (user is None)

    @mark.parametrize("user", [param(ROOT_USER_NAME), param(EFFECTIVE_USER_NAME)])
    def test_constant(self, *, user: str | None) -> None:
        match SYSTEM:
            case "windows":
                assert user is None
            case "mac" | "linux":
                assert isinstance(user, str)
            case never:
                assert_never(never)
