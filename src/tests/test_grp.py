from __future__ import annotations

from typing import assert_never

from pytest import mark, param

from utilities.grp import EFFECTIVE_GROUP_NAME, ROOT_GROUP_NAME, get_gid_name
from utilities.platform import SYSTEM


class TestGroupName:
    def test_function(self) -> None:
        group = get_gid_name(0)
        assert isinstance(group, str) or (group is None)

    @mark.parametrize("group", [param(ROOT_GROUP_NAME), param(EFFECTIVE_GROUP_NAME)])
    def test_constant(self, *, group: str | None) -> None:
        match SYSTEM:
            case "windows":
                assert group is None
            case "mac" | "linux":
                assert isinstance(group, str)
            case never:
                assert_never(never)
