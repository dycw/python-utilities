from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast, override

from hypothesis import given
from hypothesis.strategies import booleans
from luigi import BoolParameter, Task
from pytest import mark, param

from utilities.hypothesis import namespace_mixins, temp_paths, zoned_datetimes
from utilities.luigi import (
    ExternalFile,
    ExternalTask,
    PathTarget,
    ZonedDateTimeParameter,
    _ExternalTaskDummyTarget,
    build,
)

if TYPE_CHECKING:
    from whenever import ZonedDateTime


class TestBuild:
    @given(namespace_mixin=namespace_mixins())
    def test_main(self, *, namespace_mixin: Any) -> None:
        class Example(namespace_mixin, Task): ...

        _ = build([Example()], local_scheduler=True)


class TestDateTimeParameter:
    @given(datetime=zoned_datetimes())
    @mark.parametrize("type_", [param("datetime"), param("str")])
    def test_main(
        self, *, datetime: ZonedDateTime, type_: Literal["datetime", "str"]
    ) -> None:
        param = ZonedDateTimeParameter()
        match type_:
            case "datetime":
                input_ = datetime
            case "str":
                input_ = datetime.format_common_iso()
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
