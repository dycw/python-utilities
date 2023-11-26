from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import DataObject, data, sampled_from

from utilities.hypothesis import versions
from utilities.semver import ensure_version


class TestEnsureVersion:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        version = data.draw(versions())
        maybe_version = data.draw(sampled_from([version, str(version)]))
        result = ensure_version(maybe_version)
        assert result == version
