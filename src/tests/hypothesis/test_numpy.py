from typing import Optional

from hypothesis import given
from hypothesis.errors import InvalidArgument
from hypothesis.extra.numpy import array_shapes
from hypothesis.strategies import DataObject, booleans, data, floats, none
from numpy import iinfo, int64, isinf, isnan

from utilities.hypothesis import assume_does_not_raise
from utilities.hypothesis.numpy import (
    bool_arrays,
    float_arrays,
    int64s,
    int_arrays,
)
from utilities.hypothesis.typing import Shape


class TestBoolArrays:
    @given(data=data(), shape=array_shapes())
    def test_main(self, data: DataObject, shape: Shape) -> None:
        array = data.draw(bool_arrays(shape=shape))
        assert array.dtype == bool
        assert array.shape == shape


class TestFloatArrays:
    @given(
        data=data(),
        shape=array_shapes(),
        min_value=floats() | none(),
        max_value=floats() | none(),
        allow_nan=booleans() | none(),
        allow_infinity=booleans() | none(),
    )
    def test_main(
        self,
        data: DataObject,
        shape: Shape,
        min_value: Optional[float],
        max_value: Optional[float],
        allow_nan: Optional[bool],
        allow_infinity: Optional[bool],
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                float_arrays(
                    shape=shape,
                    min_value=min_value,
                    max_value=max_value,
                    allow_nan=allow_nan,
                    allow_infinity=allow_infinity,
                ),
            )
        assert array.dtype == float
        assert array.shape == shape
        if allow_nan is False:
            assert (~isnan(array)).all()
        if allow_infinity is False:
            assert (~isinf(array)).all()


class TestIntArrays:
    @given(
        data=data(),
        shape=array_shapes(),
        min_value=int64s() | none(),
        max_value=int64s() | none(),
    )
    def test_main(
        self,
        data: DataObject,
        shape: Shape,
        min_value: Optional[int],
        max_value: Optional[int],
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                int_arrays(
                    shape=shape,
                    min_value=min_value,
                    max_value=max_value,
                ),
            )
        assert array.dtype == int
        assert array.shape == shape


class TestInt64s:
    @given(x=int64s())
    def test_main(self, x: int) -> None:
        assert isinstance(x, int)
        info = iinfo(int64)
        assert info.min <= x <= info.max
