from hypothesis import assume, given
from hypothesis.errors import InvalidArgument
from hypothesis.strategies import DataObject, data
from pytest import raises
from semver import Version

from utilities.hypothesis import lists_fixed_length
from utilities.hypothesis.semver import versions


class TestVersions:
    @given(data=data())
    def test_main(self, data: DataObject) -> None:
        version = data.draw(versions())
        assert isinstance(version, Version)

    @given(data=data())
    def test_min_version(self, data: DataObject) -> None:
        min_version = data.draw(versions())
        version = data.draw(versions(min_version=min_version))
        assert version >= min_version

    @given(data=data())
    def test_max_version(self, data: DataObject) -> None:
        max_version = data.draw(versions())
        version = data.draw(versions(max_version=max_version))
        assert version <= max_version

    @given(data=data())
    def test_min_and_max_version(self, data: DataObject) -> None:
        version1, version2 = data.draw(lists_fixed_length(versions(), 2))
        min_version = min(version1, version2)
        max_version = max(version1, version2)
        version = data.draw(versions(min_version=min_version, max_version=max_version))
        assert min_version <= version <= max_version

    @given(data=data())
    def test_error(self, data: DataObject) -> None:
        version1, version2 = data.draw(lists_fixed_length(versions(), 2))
        _ = assume(version1 != version2)
        with raises(InvalidArgument):
            _ = data.draw(
                versions(
                    min_version=max(version1, version2),
                    max_version=min(version1, version2),
                )
            )
