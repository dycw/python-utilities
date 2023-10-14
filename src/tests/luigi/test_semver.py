from __future__ import annotations

from hypothesis import given
from semver import VersionInfo

from utilities.hypothesis import versions
from utilities.luigi import VersionParameter


class TestVersionParameter:
    @given(version=versions())
    def test_main(self, version: VersionInfo) -> None:
        param = VersionParameter()
        norm = param.normalize(version)
        assert param.parse(param.serialize(norm)) == norm
