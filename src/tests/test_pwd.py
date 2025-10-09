from __future__ import annotations

from pytest import mark, param

from utilities.pwd import EFFECTIVE_USER_NAME, ROOT_USER_NAME, get_uid_name


class TestUserName:
    def test_function(self) -> None:
        assert isinstance(get_uid_name(0), str)

    @mark.parametrize("user_name", [param(ROOT_USER_NAME), param(EFFECTIVE_USER_NAME)])
    def test_constant(self, *, user_name: str) -> None:
        assert isinstance(user_name, str)
