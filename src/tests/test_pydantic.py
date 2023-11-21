from __future__ import annotations

from pathlib import Path

from hypothesis import given
from hypothesis.strategies import integers
from pydantic import BaseModel

from utilities.hypothesis import temp_paths
from utilities.pydantic import HashableBaseModel, load_model, save_model


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
        path = root.joinpath("model.gz")

        class Example(BaseModel):
            x: int

        example = Example(x=x)
        save_model(example, path)
        loaded = load_model(Example, path)
        assert loaded == example
