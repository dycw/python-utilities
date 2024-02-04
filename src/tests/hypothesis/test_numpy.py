from __future__ import annotations

import datetime as dt
from typing import Any, Literal

import numpy as np
from hypothesis import assume, given
from hypothesis.errors import InvalidArgument
from hypothesis.extra.numpy import array_shapes
from hypothesis.strategies import (
    DataObject,
    booleans,
    data,
    dates,
    floats,
    integers,
    just,
    none,
)
from numpy import (
    datetime64,
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
from numpy.testing import assert_equal
from pytest import raises

from utilities._hypothesis.numpy import (
    Shape,
    bool_arrays,
    concatenated_arrays,
    datetime64_arrays,
    datetime64_dtypes,
    datetime64_indexes,
    datetime64_kinds,
    datetime64_units,
    datetime64D_indexes,
    datetime64s,
    datetime64us_indexes,
    float_arrays,
    int32s,
    int64s,
    int_arrays,
    str_arrays,
    uint32s,
    uint64s,
)
from utilities.hypothesis import assume_does_not_raise, datetimes_utc
from utilities.numpy import (
    Datetime64Kind,
    Datetime64Unit,
    datetime64_dtype_to_unit,
    datetime64_to_date,
    datetime64_to_datetime,
    datetime64_to_int,
    datetime64_unit_to_kind,
    dt64D,
    dt64us,
)


class TestBoolArrays:
    @given(data=data(), shape=array_shapes())
    def test_main(self, *, data: DataObject, shape: Shape) -> None:
        array = data.draw(bool_arrays(shape=shape))
        assert array.dtype == bool
        assert array.shape == shape


class TestConcatenatedArrays:
    @given(data=data(), m=integers(0, 10), n=integers(0, 10))
    def test_1d(self, *, data: DataObject, m: int, n: int) -> None:
        arrays = just(zeros(n, dtype=float))
        array = data.draw(concatenated_arrays(arrays, m, n))
        assert array.shape == (m, n)

    @given(data=data(), m=integers(0, 10), n=integers(0, 10), p=integers(0, 10))
    def test_2d(self, *, data: DataObject, m: int, n: int, p: int) -> None:
        arrays = just(zeros((n, p), dtype=float))
        array = data.draw(concatenated_arrays(arrays, m, (n, p)))
        assert array.shape == (m, n, p)


class TestDatetime64Arrays:
    @given(
        data=data(),
        shape=array_shapes(),
        unit=datetime64_units() | none(),
        valid_dates=booleans(),
        valid_datetimes=booleans(),
        unique=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        shape: Shape,
        unit: Datetime64Unit | None,
        valid_dates: bool,
        valid_datetimes: bool,
        unique: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                datetime64_arrays(
                    shape=shape,
                    unit=unit,
                    valid_dates=valid_dates,
                    valid_datetimes=valid_datetimes,
                    unique=unique,
                )
            )
        assert array.shape == shape
        if unit is not None:
            assert datetime64_dtype_to_unit(array.dtype) == unit
        if valid_dates:
            _ = [datetime64_to_date(d) for d in ravel(array)]
        if valid_datetimes:
            _ = [datetime64_to_datetime(d) for d in ravel(array)]
        if unique:
            assert len(ravel(array)) == len(np.unique(ravel(array)))


class TestDatetime64DIndexes:
    @given(
        data=data(),
        n=integers(0, 10),
        valid_dates=booleans(),
        unique=booleans(),
        sort=booleans(),
    )
    def test_main(
        self, *, data: DataObject, n: int, valid_dates: bool, unique: bool, sort: bool
    ) -> None:
        index = data.draw(
            datetime64D_indexes(n=n, valid_dates=valid_dates, unique=unique, sort=sort)
        )
        assert index.dtype == dt64D
        assert len(index) == n
        if valid_dates:
            _ = [datetime64_to_date(d) for d in index]
        if unique:
            assert len(index) == len(np.unique(index))
        if sort:
            assert_equal(index, np.sort(index))


class TestDatetime64DTypes:
    @given(dtype=datetime64_dtypes())
    def test_main(self, *, dtype: Any) -> None:
        _ = dtype


class TestDatetime64Indexes:
    @given(
        data=data(),
        n=integers(0, 10),
        unit=datetime64_units() | none(),
        valid_dates=booleans(),
        valid_datetimes=booleans(),
        unique=booleans(),
        sort=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        n: int,
        unit: Datetime64Unit | None,
        valid_dates: bool,
        valid_datetimes: bool,
        unique: bool,
        sort: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            index = data.draw(
                datetime64_indexes(
                    n=n,
                    unit=unit,
                    valid_dates=valid_dates,
                    valid_datetimes=valid_datetimes,
                    unique=unique,
                    sort=sort,
                )
            )
        assert len(index) == n
        if unit is not None:
            assert datetime64_dtype_to_unit(index.dtype) == unit
        if valid_dates:
            _ = [datetime64_to_date(d) for d in index]
        if valid_datetimes:
            _ = [datetime64_to_datetime(d) for d in index]
        if unique:
            assert len(index) == len(np.unique(index))
        if sort:
            assert_equal(index, np.sort(index))


class TestDatetime64Kinds:
    @given(kind=datetime64_kinds())
    def test_main(self, *, kind: Datetime64Kind) -> None:
        _ = kind


class TestDatetime64Units:
    @given(data=data(), kind=datetime64_kinds() | none())
    def test_main(self, *, data: DataObject, kind: Datetime64Kind | None) -> None:
        unit = data.draw(datetime64_units(kind=kind))
        if kind is not None:
            assert datetime64_unit_to_kind(unit) == kind


class TestDatetime64usIndexes:
    @given(
        data=data(),
        n=integers(0, 10),
        valid_datetimes=booleans(),
        unique=booleans(),
        sort=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        n: int,
        valid_datetimes: bool,
        unique: bool,
        sort: bool,
    ) -> None:
        index = data.draw(
            datetime64us_indexes(
                n=n, valid_datetimes=valid_datetimes, unique=unique, sort=sort
            )
        )
        assert index.dtype == dt64us
        assert len(index) == n
        if valid_datetimes:
            _ = [datetime64_to_datetime(d) for d in index]
        if unique:
            assert len(index) == len(np.unique(index))
        if sort:
            assert_equal(index, np.sort(index))


class TestDatetime64s:
    @given(data=data(), unit=datetime64_units())
    def test_main(self, *, data: DataObject, unit: Datetime64Unit) -> None:
        min_value = data.draw(datetime64s(unit=unit) | int64s() | none())
        max_value = data.draw(datetime64s(unit=unit) | int64s() | none())
        with assume_does_not_raise(InvalidArgument):
            datetime = data.draw(
                datetime64s(min_value=min_value, max_value=max_value, unit=unit)
            )
        assert datetime64_dtype_to_unit(datetime.dtype) == unit
        assert not isnat(datetime)
        if min_value is not None:
            if isinstance(min_value, datetime64):
                assert datetime >= min_value
            else:
                assert datetime64_to_int(datetime) >= min_value
        if max_value is not None:
            if isinstance(max_value, datetime64):
                assert datetime <= max_value
            else:
                assert datetime64_to_int(datetime) <= max_value

    @given(
        data=data(),
        min_value=datetime64s(unit="D") | dates() | none(),
        max_value=datetime64s(unit="D") | dates() | none(),
        unit=just("D") | none(),
    )
    def test_valid_dates(
        self,
        *,
        data: DataObject,
        min_value: datetime64 | dt.date | None,
        max_value: datetime64 | dt.date | None,
        unit: Literal["D"] | None,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            datetime = data.draw(
                datetime64s(
                    min_value=min_value,
                    max_value=max_value,
                    unit=unit,
                    valid_dates=True,
                )
            )
        assert datetime64_dtype_to_unit(datetime.dtype) == "D"
        date = datetime64_to_date(datetime)
        if min_value is not None:
            if isinstance(min_value, datetime64):
                assert datetime >= min_value
            else:
                assert date >= min_value
        if max_value is not None:
            if isinstance(max_value, datetime64):
                assert datetime <= max_value
            else:
                assert date <= max_value

    @given(data=data(), unit=datetime64_units())
    def test_valid_dates_error(self, *, data: DataObject, unit: Datetime64Unit) -> None:
        _ = assume(unit != "D")
        with raises(InvalidArgument):
            _ = data.draw(datetime64s(unit=unit, valid_dates=True))

    @given(
        data=data(),
        min_value=datetime64s(unit="us") | datetimes_utc() | none(),
        max_value=datetime64s(unit="us") | datetimes_utc() | none(),
        unit=just("us") | none(),
    )
    def test_valid_datetimes(
        self,
        *,
        data: DataObject,
        min_value: datetime64 | dt.datetime | None,
        max_value: datetime64 | dt.datetime | None,
        unit: Literal["us"] | None,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            np_datetime = data.draw(
                datetime64s(
                    min_value=min_value,
                    max_value=max_value,
                    unit=unit,
                    valid_datetimes=True,
                )
            )
        assert datetime64_dtype_to_unit(np_datetime.dtype) == "us"
        py_datetime = datetime64_to_datetime(np_datetime)
        if min_value is not None:
            if isinstance(min_value, datetime64):
                assert np_datetime >= min_value
            else:
                assert py_datetime >= min_value
        if max_value is not None:
            if isinstance(max_value, datetime64):
                assert np_datetime <= max_value
            else:
                assert py_datetime <= max_value

    @given(data=data(), unit=datetime64_units())
    def test_valid_datetimes_error(
        self, *, data: DataObject, unit: Datetime64Unit
    ) -> None:
        _ = assume(unit != "us")
        with raises(InvalidArgument):
            _ = data.draw(datetime64s(unit=unit, valid_datetimes=True))


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
        *,
        data: DataObject,
        shape: Shape,
        min_value: float | None,
        max_value: float | None,
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
        *,
        data: DataObject,
        shape: Shape,
        min_value: int | None,
        max_value: int | None,
        unique: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                int_arrays(
                    shape=shape, min_value=min_value, max_value=max_value, unique=unique
                )
            )
        assert array.dtype == int64
        assert array.shape == shape
        if unique:
            flat = ravel(array)
            assert len(set(flat)) == len(flat)


class TestInt32s:
    @given(data=data(), min_value=int32s() | none(), max_value=int32s() | none())
    def test_main(
        self, *, data: DataObject, min_value: int | None, max_value: int | None
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
        self, *, data: DataObject, min_value: int | None, max_value: int | None
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
        *,
        data: DataObject,
        shape: Shape,
        min_size: int,
        max_size: int | None,
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
        self, *, data: DataObject, min_value: int | None, max_value: int | None
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
        self, *, data: DataObject, min_value: int | None, max_value: int | None
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(uint64s(min_value=min_value, max_value=max_value))
        info = iinfo(uint64)
        assert info.min <= x <= info.max
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value
