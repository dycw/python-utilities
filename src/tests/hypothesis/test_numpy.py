from typing import Any, Optional

from beartype import beartype
from hypothesis import given
from hypothesis.errors import InvalidArgument
from hypothesis.extra.numpy import array_shapes
from hypothesis.strategies import (
    DataObject,
    booleans,
    data,
    floats,
    integers,
    just,
    none,
)
from numpy import (
    iinfo,
    inf,
    int32,
    int64,
    isfinite,
    isinf,
    isnan,
    isnat,
    ravel,
    rint,
    uint32,
    uint64,
    zeros,
)

from utilities.hypothesis import assume_does_not_raise
from utilities.hypothesis.numpy import (
    bool_arrays,
    concatenated_arrays,
    datetime64_dtypes,
    datetime64_units,
    datetime64D_arrays,
    datetime64s,
    float_arrays,
    int32s,
    int64s,
    int_arrays,
    str_arrays,
    uint32s,
    uint64s,
)
from utilities.hypothesis.typing import Shape
from utilities.numpy import (
    Datetime64Unit,
    datetime64_dtype_to_unit,
    datetime64_to_int,
    datetime64D,
)


class TestBoolArrays:
    @given(data=data(), shape=array_shapes())
    @beartype
    def test_main(self, data: DataObject, shape: Shape) -> None:
        array = data.draw(bool_arrays(shape=shape))
        assert array.dtype == bool
        assert array.shape == shape


class TestConcatenatedArrays:
    @given(data=data(), m=integers(0, 10), n=integers(0, 10))
    @beartype
    def test_1d(self, data: DataObject, m: int, n: int) -> None:
        arrays = just(zeros(n, dtype=float))
        array = data.draw(concatenated_arrays(arrays, m, n))
        assert array.shape == (m, n)

    @given(data=data(), m=integers(0, 10), n=integers(0, 10), p=integers(0, 10))
    @beartype
    def test_2d(self, data: DataObject, m: int, n: int, p: int) -> None:
        arrays = just(zeros((n, p), dtype=float))
        array = data.draw(concatenated_arrays(arrays, m, (n, p)))
        assert array.shape == (m, n, p)


class TestDatetime64DArrays:
    @given(data=data(), shape=array_shapes())
    @beartype
    def test_main(self, data: DataObject, shape: Shape) -> None:
        array = data.draw(datetime64D_arrays(shape=shape))
        assert array.dtype == datetime64D
        assert array.shape == shape


class TestDatetime64s:
    @given(data=data(), unit=datetime64_units())
    @beartype
    def test_main(self, data: DataObject, unit: Datetime64Unit) -> None:
        min_value = data.draw(datetime64s(unit=unit) | none())
        max_value = data.draw(datetime64s(unit=unit) | none())
        with assume_does_not_raise(InvalidArgument):
            datetime = data.draw(
                datetime64s(min_value=min_value, max_value=max_value, unit=unit)
            )
        assert datetime64_dtype_to_unit(datetime.dtype) == unit
        assert not isnat(datetime)
        if min_value is not None:
            assert datetime >= min_value
        if max_value is not None:
            assert datetime <= max_value

    @given(data=data(), min_value=int64s() | none(), max_value=int64s() | none())
    def test_bounds_as_ints(
        self, data: DataObject, min_value: Optional[int], max_value: Optional[int]
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            datetime = data.draw(datetime64s(min_value=min_value, max_value=max_value))
        if min_value is not None:
            assert datetime64_to_int(datetime) >= min_value
        if max_value is not None:
            assert datetime64_to_int(datetime) <= max_value


class TestDatetime64DTypes:
    @given(dtype=datetime64_dtypes())
    def test_main(self, dtype: Any) -> None:
        _ = dtype


class TestDatetime64Units:
    @given(unit=datetime64_units())
    def test_main(self, unit: Datetime64Unit) -> None:
        _ = unit


class TestFloatArrays:
    @given(
        data=data(),
        shape=array_shapes(),
        min_value=floats() | none(),
        max_value=floats() | none(),
        allow_nan=booleans(),
        allow_inf=booleans(),
        allow_pos_inf=booleans(),
        allow_neg_inf=booleans(),
        integral=booleans(),
        unique=booleans(),
    )
    def test_main(
        self,
        data: DataObject,
        shape: Shape,
        min_value: Optional[float],
        max_value: Optional[float],
        allow_nan: bool,
        allow_inf: bool,
        allow_pos_inf: bool,
        allow_neg_inf: bool,
        integral: bool,
        unique: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                float_arrays(
                    shape=shape,
                    min_value=min_value,
                    max_value=max_value,
                    allow_nan=allow_nan,
                    allow_inf=allow_inf,
                    allow_pos_inf=allow_pos_inf,
                    allow_neg_inf=allow_neg_inf,
                    integral=integral,
                    unique=unique,
                )
            )
        assert array.dtype == float
        assert array.shape == shape
        if min_value is not None:
            assert ((isfinite(array) & (array >= min_value)) | ~isfinite(array)).all()
        if max_value is not None:
            assert ((isfinite(array) & (array <= max_value)) | ~isfinite(array)).all()
        if not allow_nan:
            assert (~isnan(array)).all()
        if not allow_inf:
            if not (allow_pos_inf or allow_neg_inf):
                assert (~isinf(array)).all()
            if not allow_pos_inf:
                assert (array != inf).all()
            if not allow_neg_inf:
                assert (array != -inf).all()
        if integral:
            assert ((array == rint(array)) | isnan(array)).all()
        if unique:
            flat = ravel(array)
            assert len(set(flat)) == len(flat)


class TestIntArrays:
    @given(
        data=data(),
        shape=array_shapes(),
        min_value=int64s() | none(),
        max_value=int64s() | none(),
        unique=booleans(),
    )
    def test_main(
        self,
        data: DataObject,
        shape: Shape,
        min_value: Optional[int],
        max_value: Optional[int],
        unique: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                int_arrays(
                    shape=shape, min_value=min_value, max_value=max_value, unique=unique
                )
            )
        assert array.dtype == int
        assert array.shape == shape
        if unique:
            flat = ravel(array)
            assert len(set(flat)) == len(flat)


class TestInt32s:
    @given(data=data(), min_value=int32s() | none(), max_value=int32s() | none())
    def test_main(
        self, data: DataObject, min_value: Optional[int], max_value: Optional[int]
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(int32s(min_value=min_value, max_value=max_value))
        info = iinfo(int32)
        assert info.min <= x <= info.max
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value


class TestInt64s:
    @given(data=data(), min_value=int64s() | none(), max_value=int64s() | none())
    def test_main(
        self, data: DataObject, min_value: Optional[int], max_value: Optional[int]
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(int64s(min_value=min_value, max_value=max_value))
        info = iinfo(int64)
        assert info.min <= x <= info.max
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value


class TestStrArrays:
    @given(
        data=data(),
        shape=array_shapes(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
        allow_none=booleans(),
        unique=booleans(),
    )
    def test_main(
        self,
        data: DataObject,
        shape: Shape,
        min_size: int,
        max_size: Optional[int],
        allow_none: bool,
        unique: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                str_arrays(
                    shape=shape,
                    min_size=min_size,
                    max_size=max_size,
                    allow_none=allow_none,
                    unique=unique,
                )
            )
        assert array.dtype == object
        assert array.shape == shape
        flat = ravel(array)
        flat_text = [i for i in flat if i is not None]
        assert all(len(t) >= min_size for t in flat_text)
        if max_size is not None:
            assert all(len(t) <= max_size for t in flat_text)
        if not allow_none:
            assert len(flat_text) == array.size
        if unique:
            flat = ravel(array)
            assert len(set(flat)) == len(flat)


class TestUInt32s:
    @given(data=data(), min_value=uint32s() | none(), max_value=uint32s() | none())
    def test_main(
        self, data: DataObject, min_value: Optional[int], max_value: Optional[int]
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(uint32s(min_value=min_value, max_value=max_value))
        info = iinfo(uint32)
        assert info.min <= x <= info.max
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value


class TestUInt64s:
    @given(data=data(), min_value=uint64s() | none(), max_value=uint64s() | none())
    def test_main(
        self, data: DataObject, min_value: Optional[int], max_value: Optional[int]
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(uint64s(min_value=min_value, max_value=max_value))
        info = iinfo(uint64)
        assert info.min <= x <= info.max
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value
