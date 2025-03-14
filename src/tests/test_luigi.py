from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast, override

from hypothesis import given
from hypothesis.strategies import DataObject, booleans, data, sampled_from, times
from luigi import BoolParameter, Parameter, Task

from utilities.hypothesis import namespace_mixins, temp_paths, zoned_datetimes
from utilities.luigi import (
    DateHourParameter,
    DateMinuteParameter,
    DateSecondParameter,
    ExternalFile,
    ExternalTask,
    PathTarget,
    TimeParameter,
    _ExternalTaskDummyTarget,
    build,
)
from utilities.whenever import serialize_time, serialize_zoned_datetime

if TYPE_CHECKING:
    import datetime as dt


class TestBuild:
    @given(namespace_mixin=namespace_mixins())
    def test_main(self, *, namespace_mixin: Any) -> None:
        class Example(namespace_mixin, Task): ...

        _ = build([Example()], local_scheduler=True)


class TestDateTimeParameter:
    @given(
        data=data(),
        param_cls=sampled_from([
            DateHourParameter,
            DateMinuteParameter,
            DateSecondParameter,
        ]),
        datetime=zoned_datetimes(),
    )
    def test_main(
        self, *, data: DataObject, param_cls: type[Parameter], datetime: dt.datetime
    ) -> None:
        param = param_cls()
        input_ = data.draw(sampled_from([datetime, serialize_zoned_datetime(datetime)]))
        norm = param.normalize(input_)
        assert param.parse(param.serialize(norm)) == norm


class TestExternalFile:
    @given(namespace_mixin=namespace_mixins(), root=temp_paths())
    def test_main(self, *, namespace_mixin: Any, root: Path) -> None:
        path = root.joinpath("file")

        class Example(namespace_mixin, ExternalFile): ...

        task = Example(path)
        assert not task.exists()
        path.touch()
        assert task.exists()


class TestExternalTask:
    @given(namespace_mixin=namespace_mixins(), is_complete=booleans())
    def test_main(self, *, namespace_mixin: Any, is_complete: bool) -> None:
        class Example(namespace_mixin, ExternalTask):
            is_complete: bool = cast("bool", BoolParameter())

            @override
            def exists(self) -> bool:
                return self.is_complete

        task = Example(is_complete=is_complete)
        result = task.exists()
        assert result is is_complete
        assert isinstance(task.output(), _ExternalTaskDummyTarget)
        result2 = task.output().exists()
        assert result2 is is_complete


class TestPathTarget:
    def test_main(self, *, tmp_path: Path) -> None:
        target = PathTarget(path := tmp_path.joinpath("file"))
        assert isinstance(target.path, Path)
        assert not target.exists()
        path.touch()
        assert target.exists()


class TestTimeParameter:
    @given(data=data(), time=times())
    def test_main(self, *, data: DataObject, time: dt.time) -> None:
        param = TimeParameter()
        input_ = data.draw(sampled_from([time, serialize_time(time)]))
        norm = param.normalize(input_)
        assert param.parse(param.serialize(norm)) == time
