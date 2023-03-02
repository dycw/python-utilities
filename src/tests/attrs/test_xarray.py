from typing import Optional

from attrs import define
from hypothesis import given
from hypothesis.strategies import none
from xarray import DataArray

from utilities.attrs.xarray import rename_data_arrays
from utilities.hypothesis import text_ascii


class TestRenameDataArrays:
    @given(name_array=text_ascii() | none(), name_other=text_ascii() | none())
    def test_main(self, name_array: Optional[str], name_other: Optional[str]) -> None:
        @define
        class Other:
            name: Optional[str]

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
