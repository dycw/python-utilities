from __future__ import annotations

from pathlib import Path

from hypothesis import given
from hypothesis.strategies import integers
from pydantic import BaseModel
from pytest import raises

from utilities.hypothesis import temp_paths
from utilities.pydantic import HashableBaseModel, LoadModelError, load_model, save_model
from utilities.pytest import skipif_windows


class TestHashableBaseModel:
    @given(x=integers())
    def test_main(self, *, x: int) -> None:
        class Example(HashableBaseModel):
            x: int

        example = Example(x=x)
        assert isinstance(hash(example), int)


class TestSaveAndLoadModel:
    @given(x=integers(), root=temp_paths())
    def test_main(self, *, x: int, root: Path) -> None:
        path = Path(root, "model.json")

        class Example(BaseModel):
            x: int

        example = Example(x=x)
        save_model(example, path)
        loaded = load_model(Example, path)
        assert loaded == example

    @skipif_windows
    def test_load_model_error_dir(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("dir")
        path.mkdir()

        class Example(BaseModel):
            x: int

        with raises(
            LoadModelError,
            match=r"Unable to load .*; path '.*' must not be a directory\.",
        ):
            _ = load_model(Example, path)

    def test_load_model_error_file(self, *, tmp_path: Path) -> None:
        class Example(BaseModel):
            x: int

        with raises(LoadModelError, match=r"Unable to load .*; path '.*' must exist\."):
            _ = load_model(Example, tmp_path.joinpath("model.json"))
