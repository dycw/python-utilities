from __future__ import annotations

from typing import assert_never

from pytest import mark, param

from utilities.platform import SYSTEM
from utilities.pwd import EFFECTIVE_USER_NAME, ROOT_USER_NAME, get_uid_name


class TestUserName:
    def test_function(self) -> None:
        result = get_uid_name(0)
        assert isinstance(result, str) or (result is None)

    @mark.parametrize("user_name", [param(ROOT_USER_NAME), param(EFFECTIVE_USER_NAME)])
    def test_constant(self, *, user_name: str | None) -> None:
        match SYSTEM:
            case "windows":
                assert user_name is None
            case "mac" | "linux":
                assert isinstance(ROOT_USER_NAME, str)
            case never:
                assert_never(never)
