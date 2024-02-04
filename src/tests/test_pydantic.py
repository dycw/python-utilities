from __future__ import annotations

from frozendict import frozendict
from hypothesis import given
from hypothesis.strategies import dictionaries, integers
from pydantic import BaseModel
from pytest import mark

from utilities.hypothesis import temp_paths
from utilities.pydantic import (
    HashableBaseModel,
    PydanticFrozenDict,
    load_model,
    save_model,
)


class TestHashableBaseModel:
    @given(x=integers())
    def test_main(self, *, x: int) -> None:
        class Example(HashableBaseModel):
            x: int

        example = Example(x=x)
        assert isinstance(hash(example), int)


class TestPydanticFrozenDict:
    @mark.only
    @given(x=dictionaries(integers(-10, 10), integers(-10, 10)).map(frozendict))
    def test_main(self, *, x: frozendict[int, int]) -> None:
        class Example(BaseModel):
            mapping: PydanticFrozenDict[int, int]

        obj = Example(mapping=x)
        assert isinstance(obj.mapping, frozendict)
        assert obj.mapping == x
        assert obj.model_dump() == {"mapping": x}
        json = obj.model_dump_json()
        loaded = Example.model_validate_json(json)
        assert isinstance(loaded.mapping, frozendict)
        assert loaded.mapping == x


class TestSaveAndLoadModel:
    @given(x=integers(), root=temp_paths())
    def test_main(self, *, x: int, root: Path) -> None:
        path = ensure_path(root, "model.json")

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

        with pytest.raises(
            LoadModelError,
            match=r"Unable to load .*; path .* must not be a directory\.",
        ):
            _ = load_model(Example, path)

    def test_load_model_error_file(self, *, tmp_path: Path) -> None:
        class Example(BaseModel):
            x: int

        with pytest.raises(
            LoadModelError, match=r"Unable to load .*; path .* must exist\."
        ):
            _ = load_model(Example, tmp_path.joinpath("model.json"))
