from hypothesis import given
from hypothesis.strategies import DataObject, data, sampled_from

from utilities.hypothesis.semver import version_infos
from utilities.semver import ensure_version_info


class TestEnsureVersionInfo:
    @given(data=data())
    def test_main(self, data: DataObject) -> None:
        version = data.draw(version_infos())
        maybe_version = data.draw(sampled_from([version, str(version)]))
        result = ensure_version_info(maybe_version)
        assert result == version
