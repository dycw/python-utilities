from __future__ import annotations

from pytest import raises

from utilities._typed_settings.common import get_loaders
from utilities.typed_settings import GetLoadersError


class TestGetLoaders:
    def test_success(self) -> None:
        _ = get_loaders()

    def test_error(self) -> None:
        with raises(GetLoadersError):
            _ = get_loaders(appname="app_name")
