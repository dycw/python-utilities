from hypothesis import given
from semver import VersionInfo

from utilities.hypothesis.semver import versions
from utilities.luigi.semver import VersionParameter


class TestVersionParameter:
    @given(version=versions())
    def test_main(self, version: VersionInfo) -> None:
        param = VersionParameter()
        norm = param.normalize(version)
        assert param.parse(param.serialize(norm)) == norm
