from __future__ import annotations

from utilities.grp import EFFECTIVE_GROUP_NAME


class TestEffectiveGroupName:
    def test_main(self) -> None:
        assert isinstance(EFFECTIVE_GROUP_NAME, str)
