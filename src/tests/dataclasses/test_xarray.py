from __future__ import annotations

from dataclasses import dataclass

from hypothesis import given
from hypothesis.strategies import none
from xarray import DataArray

from utilities.dataclasses import rename_data_arrays
from utilities.hypothesis import text_ascii


class TestRenameDataArrays:
    @given(name_array=text_ascii() | none(), name_other=text_ascii() | none())
    def test_main(self, *, name_array: str | None, name_other: str | None) -> None:
        @dataclass
        class Other:
            name: str | None

        @dataclass
        class Example:
            array: DataArray
            other: Other

            def __post_init__(self) -> None:
                rename_data_arrays(self)

        array = DataArray(name=name_array)
        other = Other(name=name_other)
        example = Example(array, other)
        assert example.array is not array
        assert example.other is other
        assert example.array.name == "array"
        assert example.other.name == name_other
