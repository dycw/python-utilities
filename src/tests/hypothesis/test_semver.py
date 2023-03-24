from hypothesis import assume, given
from hypothesis.errors import InvalidArgument
from hypothesis.strategies import DataObject, data
from pytest import raises
from semver import VersionInfo

from utilities.hypothesis import lists_fixed_length
from utilities.hypothesis.semver import version_infos


class TestVersionInfos:
    @given(data=data())
    def test_main(self, data: DataObject) -> None:
        version = data.draw(version_infos())
        assert isinstance(version, VersionInfo)

    @given(data=data())
    def test_min_version(self, data: DataObject) -> None:
        min_version = data.draw(version_infos())
        version = data.draw(version_infos(min_version=min_version))
        assert version >= min_version

    @given(data=data())
    def test_max_version(self, data: DataObject) -> None:
        max_version = data.draw(version_infos())
        version = data.draw(version_infos(max_version=max_version))
        assert version <= max_version

    @given(data=data())
    def test_min_and_max_version(self, data: DataObject) -> None:
        version1, version2 = data.draw(lists_fixed_length(version_infos(), 2))
        min_version = min(version1, version2)
        max_version = max(version1, version2)
        version = data.draw(
            version_infos(min_version=min_version, max_version=max_version)
        )
        assert min_version <= version <= max_version

    @given(data=data())
    def test_error(self, data: DataObject) -> None:
        version1, version2 = data.draw(lists_fixed_length(version_infos(), 2))
        _ = assume(version1 != version2)
        with raises(InvalidArgument):
            _ = data.draw(
                version_infos(
                    min_version=max(version1, version2),
                    max_version=min(version1, version2),
                )
            )
