from __future__ import annotations

from pytest import mark, param

from utilities.grp import EFFECTIVE_GROUP_NAME, ROOT_GROUP_NAME, get_gid_name


class TestGroupName:
    def test_function(self) -> None:
        assert isinstance(get_gid_name(0), str)

    @mark.parametrize(
        "group_name", [param(ROOT_GROUP_NAME), param(EFFECTIVE_GROUP_NAME)]
    )
    def test_constant(self, *, group_name: str) -> None:
        assert isinstance(group_name, str)
