from pathlib import Path

from hypothesis import given
from sqlalchemy import Engine

from tests.typed_settings.test_typed_settings import (
    TestClickOptions as _TestClickOptions,
)
from tests.typed_settings.test_typed_settings import (
    TestLoadSettings as _TestLoadSettings,
)
from utilities.hypothesis import sqlite_engines, temp_paths, text_ascii
from utilities.pytest import skipif_windows
from utilities.sqlalchemy import serialize_engine

app_names = text_ascii(min_size=1).map(str.lower)


class TestLoadSettings:
    @skipif_windows  # writing \\ to file
    @given(
        default=sqlite_engines(),
        appname=app_names,
        root=temp_paths(),
        value=sqlite_engines(),
    )
    def test_main(
        self, *, default: Engine, root: Path, appname: str, value: Engine
    ) -> None:
        def equal(x: Engine, y: Engine, /) -> bool:
            return x.url == y.url

        _TestLoadSettings.run_test(
            Engine, default, root, appname, serialize_engine, value, equal
        )


class TestClickOptions:
    @skipif_windows  # writing \\ to file
    @given(
        default=sqlite_engines(),
        appname=app_names,
        root=temp_paths(),
        value=sqlite_engines(),
        cfg=sqlite_engines(),
    )
    def test_main(
        self, *, default: Engine, appname: str, root: Path, value: Engine, cfg: Engine
    ) -> None:
        _TestClickOptions.run_test(
            Engine, default, appname, serialize_engine, root, value, cfg
        )
