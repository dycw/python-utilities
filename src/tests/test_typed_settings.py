import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from operator import eq
from pathlib import Path
from typing import TypeVar

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    data,
    dates,
    datetimes,
    times,
    tuples,
)
from pytest import mark, param

from utilities.hypothesis import temp_paths, text_ascii, timedeltas_2w
from utilities.typed_settings import load_settings
from utilities.whenever import (
    serialize_date,
    serialize_local_datetime,
    serialize_time,
    serialize_timedelta,
)

app_names = text_ascii(min_size=1).map(str.lower)


_T = TypeVar("_T")


class TestExtendedTSConverter:
    @given(data=data(), root=temp_paths(), appname=text_ascii(min_size=1))
    @mark.parametrize(
        ("test_cls", "strategy", "serialize"),
        [
            param(dt.date, dates(), serialize_date),
            param(dt.datetime, datetimes(), serialize_local_datetime),
            param(dt.time, times(), serialize_time),
            param(dt.timedelta, timedeltas_2w(), serialize_timedelta),
        ],
    )
    def test_main(
        self,
        *,
        data: DataObject,
        root: Path,
        appname: str,
        test_cls: type[_T],
        strategy: SearchStrategy[_T],
        serialize: Callable[[_T], str],
    ) -> None:
        default, value = data.draw(tuples(strategy, strategy))
        self._run_test(test_cls, default, root, appname, serialize, value, eq)

    def _run_test(
        self,
        test_cls: type[_T],
        default: _T,
        root: Path,
        appname: str,
        serialize: Callable[[_T], str],
        value: _T,
        equal: Callable[[_T, _T], bool],
        /,
    ) -> None:
        @dataclass(frozen=True, kw_only=True, slots=True)
        class Settings:
            value: test_cls = default

        settings_default = load_settings(Settings)
        assert settings_default.value == default
        _ = hash(settings_default)
        file = Path(root, "file.toml")
        with file.open(mode="w") as fh:
            _ = fh.write(f'[{appname}]\nvalue = "{serialize(value)}"')
        settings_loaded = load_settings(Settings, appname=appname, config_files=[file])
        assert equal(settings_loaded.value, value)
