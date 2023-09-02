from __future__ import annotations

from attrs import define
from hypothesis import given
from xarray import DataArray

from utilities.dataclasses.xarray import rename_data_arrays
from utilities.hypothesis import hashables


class TestRenameDataArrays:
    @given(name_array=hashables(), name_other=hashables())
    def test_main(self, name_array: str | None, name_other: str | None) -> None:
        @define
        class Other:
            name: str | None

        @define
        class Example:
            array: DataArray
            other: Other

            def __attrs_post_init__(self) -> None:
                rename_data_arrays(self)

        array = DataArray(name=name_array)
        other = Other(name=name_other)
        example = Example(array, other)
        assert example.array is not array
        assert example.other is other
        assert example.array.name == "array"
        assert example.other.name == name_other
