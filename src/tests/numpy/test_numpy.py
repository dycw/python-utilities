from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from contextlib import suppress
from typing import Annotated, Any, Literal

from beartype.door import die_if_unbearable
from beartype.roar import BeartypeDoorHintViolation
from hypothesis import Phase, assume, example, given, settings
from hypothesis.strategies import DataObject, data, dates, datetimes, integers, just
from numpy import (
    arange,
    array,
    concatenate,
    datetime64,
    empty,
    eye,
    full,
    inf,
    int64,
    isclose,
    median,
    nan,
    ndarray,
    ones,
    zeros,
    zeros_like,
)
from numpy.random import Generator
from numpy.testing import assert_equal
from numpy.typing import NDArray
from pytest import mark, param, raises

from utilities._hypothesis.numpy import datetime64s
from utilities.datetime import UTC
from utilities.hypothesis import (
    datetime64_dtypes,
    datetime64_units,
    datetimes_utc,
    float_arrays,
    int_arrays,
)
from utilities.numpy import (
    DEFAULT_RNG,
    AsIntError,
    Datetime64Kind,
    DateTime64ToDateError,
    DateTime64ToDateTimeError,
    Datetime64Unit,
    DatetimeToDatetime64Error,
    EmptyNumpyConcatenateError,
    FlatN0Error,
    GetFillValueError,
    NDArray0,
    NDArray1,
    NDArray2,
    NDArray3,
    NDArrayB,
    NDArrayB0,
    NDArrayB1,
    NDArrayB2,
    NDArrayB3,
    NDArrayD0,
    NDArrayD1,
    NDArrayD2,
    NDArrayD3,
    NDArrayDas,
    NDArrayDas0,
    NDArrayDas1,
    NDArrayDas2,
    NDArrayDas3,
    NDArrayDD,
    NDArrayDD0,
    NDArrayDD1,
    NDArrayDD2,
    NDArrayDD3,
    NDArrayDfs,
    NDArrayDfs0,
    NDArrayDfs1,
    NDArrayDfs2,
    NDArrayDfs3,
    NDArrayDh,
    NDArrayDh0,
    NDArrayDh1,
    NDArrayDh2,
    NDArrayDh3,
    NDArrayDM,
    NDArrayDm,
    NDArrayDM0,
    NDArrayDm0,
    NDArrayDM1,
    NDArrayDm1,
    NDArrayDM2,
    NDArrayDm2,
    NDArrayDM3,
    NDArrayDm3,
    NDArrayDms,
    NDArrayDms0,
    NDArrayDms1,
    NDArrayDms2,
    NDArrayDms3,
    NDArrayDns,
    NDArrayDns0,
    NDArrayDns1,
    NDArrayDns2,
    NDArrayDns3,
    NDArrayDps,
    NDArrayDps0,
    NDArrayDps1,
    NDArrayDps2,
    NDArrayDps3,
    NDArrayDs,
    NDArrayDs0,
    NDArrayDs1,
    NDArrayDs2,
    NDArrayDs3,
    NDArrayDus,
    NDArrayDus0,
    NDArrayDus1,
    NDArrayDus2,
    NDArrayDus3,
    NDArrayDW,
    NDArrayDW0,
    NDArrayDW1,
    NDArrayDW2,
    NDArrayDW3,
    NDArrayDY,
    NDArrayDY0,
    NDArrayDY1,
    NDArrayDY2,
    NDArrayDY3,
    NDArrayF,
    NDArrayF0,
    NDArrayF0Fin,
    NDArrayF0FinInt,
    NDArrayF0FinIntNan,
    NDArrayF0FinNan,
    NDArrayF0FinNeg,
    NDArrayF0FinNegNan,
    NDArrayF0FinNonNeg,
    NDArrayF0FinNonNegNan,
    NDArrayF0FinNonPos,
    NDArrayF0FinNonPosNan,
    NDArrayF0FinNonZr,
    NDArrayF0FinNonZrNan,
    NDArrayF0FinPos,
    NDArrayF0FinPosNan,
    NDArrayF0Int,
    NDArrayF0IntNan,
    NDArrayF0Neg,
    NDArrayF0NegNan,
    NDArrayF0NonNeg,
    NDArrayF0NonNegNan,
    NDArrayF0NonPos,
    NDArrayF0NonPosNan,
    NDArrayF0NonZr,
    NDArrayF0NonZrNan,
    NDArrayF0Pos,
    NDArrayF0PosNan,
    NDArrayF0Zr,
    NDArrayF0ZrFinNonMic,
    NDArrayF0ZrFinNonMicNan,
    NDArrayF0ZrNan,
    NDArrayF0ZrNonMic,
    NDArrayF0ZrNonMicNan,
    NDArrayF1,
    NDArrayF1Fin,
    NDArrayF1FinInt,
    NDArrayF1FinIntNan,
    NDArrayF1FinNan,
    NDArrayF1FinNeg,
    NDArrayF1FinNegNan,
    NDArrayF1FinNonNeg,
    NDArrayF1FinNonNegNan,
    NDArrayF1FinNonPos,
    NDArrayF1FinNonPosNan,
    NDArrayF1FinNonZr,
    NDArrayF1FinNonZrNan,
    NDArrayF1FinPos,
    NDArrayF1FinPosNan,
    NDArrayF1Int,
    NDArrayF1IntNan,
    NDArrayF1Neg,
    NDArrayF1NegNan,
    NDArrayF1NonNeg,
    NDArrayF1NonNegNan,
    NDArrayF1NonPos,
    NDArrayF1NonPosNan,
    NDArrayF1NonZr,
    NDArrayF1NonZrNan,
    NDArrayF1Pos,
    NDArrayF1PosNan,
    NDArrayF1Zr,
    NDArrayF1ZrFinNonMic,
    NDArrayF1ZrFinNonMicNan,
    NDArrayF1ZrNan,
    NDArrayF1ZrNonMic,
    NDArrayF1ZrNonMicNan,
    NDArrayF2,
    NDArrayF2Fin,
    NDArrayF2FinInt,
    NDArrayF2FinIntNan,
    NDArrayF2FinNan,
    NDArrayF2FinNeg,
    NDArrayF2FinNegNan,
    NDArrayF2FinNonNeg,
    NDArrayF2FinNonNegNan,
    NDArrayF2FinNonPos,
    NDArrayF2FinNonPosNan,
    NDArrayF2FinNonZr,
    NDArrayF2FinNonZrNan,
    NDArrayF2FinPos,
    NDArrayF2FinPosNan,
    NDArrayF2Int,
    NDArrayF2IntNan,
    NDArrayF2Neg,
    NDArrayF2NegNan,
    NDArrayF2NonNeg,
    NDArrayF2NonNegNan,
    NDArrayF2NonPos,
    NDArrayF2NonPosNan,
    NDArrayF2NonZr,
    NDArrayF2NonZrNan,
    NDArrayF2Pos,
    NDArrayF2PosNan,
    NDArrayF2Zr,
    NDArrayF2ZrFinNonMic,
    NDArrayF2ZrFinNonMicNan,
    NDArrayF2ZrNan,
    NDArrayF2ZrNonMic,
    NDArrayF2ZrNonMicNan,
    NDArrayF3,
    NDArrayF3Fin,
    NDArrayF3FinInt,
    NDArrayF3FinIntNan,
    NDArrayF3FinNan,
    NDArrayF3FinNeg,
    NDArrayF3FinNegNan,
    NDArrayF3FinNonNeg,
    NDArrayF3FinNonNegNan,
    NDArrayF3FinNonPos,
    NDArrayF3FinNonPosNan,
    NDArrayF3FinNonZr,
    NDArrayF3FinNonZrNan,
    NDArrayF3FinPos,
    NDArrayF3FinPosNan,
    NDArrayF3Int,
    NDArrayF3IntNan,
    NDArrayF3Neg,
    NDArrayF3NegNan,
    NDArrayF3NonNeg,
    NDArrayF3NonNegNan,
    NDArrayF3NonPos,
    NDArrayF3NonPosNan,
    NDArrayF3NonZr,
    NDArrayF3NonZrNan,
    NDArrayF3Pos,
    NDArrayF3PosNan,
    NDArrayF3Zr,
    NDArrayF3ZrFinNonMic,
    NDArrayF3ZrFinNonMicNan,
    NDArrayF3ZrNan,
    NDArrayF3ZrNonMic,
    NDArrayF3ZrNonMicNan,
    NDArrayFFin,
    NDArrayFFinInt,
    NDArrayFFinIntNan,
    NDArrayFFinNan,
    NDArrayFFinNeg,
    NDArrayFFinNegNan,
    NDArrayFFinNonNeg,
    NDArrayFFinNonNegNan,
    NDArrayFFinNonPos,
    NDArrayFFinNonPosNan,
    NDArrayFFinNonZr,
    NDArrayFFinNonZrNan,
    NDArrayFFinPos,
    NDArrayFFinPosNan,
    NDArrayFInt,
    NDArrayFIntNan,
    NDArrayFNeg,
    NDArrayFNegNan,
    NDArrayFNonNeg,
    NDArrayFNonNegNan,
    NDArrayFNonPos,
    NDArrayFNonPosNan,
    NDArrayFNonZr,
    NDArrayFNonZrNan,
    NDArrayFPos,
    NDArrayFPosNan,
    NDArrayFZr,
    NDArrayFZrFinNonMic,
    NDArrayFZrFinNonMicNan,
    NDArrayFZrNan,
    NDArrayFZrNonMic,
    NDArrayFZrNonMicNan,
    NDArrayI,
    NDArrayI0,
    NDArrayI0Neg,
    NDArrayI0NonNeg,
    NDArrayI0NonPos,
    NDArrayI0NonZr,
    NDArrayI0Pos,
    NDArrayI0Zr,
    NDArrayI1,
    NDArrayI1Neg,
    NDArrayI1NonNeg,
    NDArrayI1NonPos,
    NDArrayI1NonZr,
    NDArrayI1Pos,
    NDArrayI1Zr,
    NDArrayI2,
    NDArrayI2Neg,
    NDArrayI2NonNeg,
    NDArrayI2NonPos,
    NDArrayI2NonZr,
    NDArrayI2Pos,
    NDArrayI2Zr,
    NDArrayI3,
    NDArrayI3Neg,
    NDArrayI3NonNeg,
    NDArrayI3NonPos,
    NDArrayI3NonZr,
    NDArrayI3Pos,
    NDArrayI3Zr,
    NDArrayINeg,
    NDArrayINonNeg,
    NDArrayINonPos,
    NDArrayINonZr,
    NDArrayIPos,
    NDArrayIZr,
    NDArrayO,
    NDArrayO0,
    NDArrayO1,
    NDArrayO2,
    NDArrayO3,
    NDim0,
    NDim1,
    NDim2,
    NDim3,
    array_indexer,
    as_int,
    date_to_datetime64,
    datetime64_dtype_to_unit,
    datetime64_to_date,
    datetime64_to_datetime,
    datetime64_to_int,
    datetime64_unit_to_dtype,
    datetime64_unit_to_kind,
    datetime_to_datetime64,
    discretize,
    dt64as,
    dt64D,
    dt64fs,
    dt64h,
    dt64M,
    dt64m,
    dt64ms,
    dt64ns,
    dt64ps,
    dt64s,
    dt64us,
    dt64W,
    dt64Y,
    ffill_non_nan_slices,
    fillna,
    flatn0,
    get_fill_value,
    has_dtype,
    is_at_least,
    is_at_least_or_nan,
    is_at_most,
    is_at_most_or_nan,
    is_between,
    is_between_or_nan,
    is_empty,
    is_finite_and_integral,
    is_finite_and_integral_or_nan,
    is_finite_and_negative,
    is_finite_and_negative_or_nan,
    is_finite_and_non_negative,
    is_finite_and_non_negative_or_nan,
    is_finite_and_non_positive,
    is_finite_and_non_positive_or_nan,
    is_finite_and_non_zero,
    is_finite_and_non_zero_or_nan,
    is_finite_and_positive,
    is_finite_and_positive_or_nan,
    is_finite_or_nan,
    is_greater_than,
    is_greater_than_or_nan,
    is_integral,
    is_integral_or_nan,
    is_less_than,
    is_less_than_or_nan,
    is_negative,
    is_negative_or_nan,
    is_non_empty,
    is_non_negative,
    is_non_negative_or_nan,
    is_non_positive,
    is_non_positive_or_nan,
    is_non_singular,
    is_non_zero,
    is_non_zero_or_nan,
    is_positive,
    is_positive_or_nan,
    is_positive_semidefinite,
    is_symmetric,
    is_zero,
    is_zero_or_finite_and_non_micro,
    is_zero_or_finite_and_non_micro_or_nan,
    is_zero_or_nan,
    is_zero_or_non_micro,
    is_zero_or_non_micro_or_nan,
    maximum,
    minimum,
    redirect_empty_numpy_concatenate,
    shift_bool,
    year,
)
from utilities.zoneinfo import HONG_KONG


class TestAnnotations:
    @mark.parametrize(
        ("dtype", "hint"),
        [
            param(bool, NDArrayB),
            param(dt64Y, NDArrayDY),
            param(dt64M, NDArrayDM),
            param(dt64W, NDArrayDW),
            param(dt64D, NDArrayDD),
            param(dt64h, NDArrayDh),
            param(dt64m, NDArrayDm),
            param(dt64s, NDArrayDs),
            param(dt64ms, NDArrayDms),
            param(dt64us, NDArrayDus),
            param(dt64ns, NDArrayDns),
            param(dt64ps, NDArrayDps),
            param(dt64fs, NDArrayDfs),
            param(dt64as, NDArrayDas),
            param(float, NDArrayF),
            param(int64, NDArrayI),
            param(object, NDArrayO),
        ],
    )
    def test_dtype(self, *, dtype: Any, hint: Any) -> None:
        arr = empty(0, dtype=dtype)
        die_if_unbearable(arr, hint)

    @mark.parametrize(
        ("ndim", "hint"),
        [
            param(0, NDArray0),
            param(1, NDArray1),
            param(2, NDArray2),
            param(3, NDArray3),
        ],
    )
    def test_ndim(self, *, ndim: int, hint: Any) -> None:
        arr = empty(zeros(ndim, dtype=int), dtype=float)
        die_if_unbearable(arr, hint)

    @mark.parametrize(
        ("dtype", "ndim", "hint"),
        [
            # ndim 0
            param(bool, 0, NDArrayB0),
            param(dt64D, 0, NDArrayD0),
            param(dt64Y, 0, NDArrayDY0),
            param(dt64M, 0, NDArrayDM0),
            param(dt64W, 0, NDArrayDW0),
            param(dt64D, 0, NDArrayDD0),
            param(dt64h, 0, NDArrayDh0),
            param(dt64m, 0, NDArrayDm0),
            param(dt64s, 0, NDArrayDs0),
            param(dt64ms, 0, NDArrayDms0),
            param(dt64us, 0, NDArrayDus0),
            param(dt64ns, 0, NDArrayDns0),
            param(dt64ps, 0, NDArrayDps0),
            param(dt64fs, 0, NDArrayDfs0),
            param(dt64as, 0, NDArrayDas0),
            param(float, 0, NDArrayF0),
            param(int64, 0, NDArrayI0),
            param(object, 0, NDArrayO0),
            # ndim 1
            param(bool, 1, NDArrayB1),
            param(dt64D, 1, NDArrayD1),
            param(dt64Y, 1, NDArrayDY1),
            param(dt64M, 1, NDArrayDM1),
            param(dt64W, 1, NDArrayDW1),
            param(dt64D, 1, NDArrayDD1),
            param(dt64h, 1, NDArrayDh1),
            param(dt64m, 1, NDArrayDm1),
            param(dt64s, 1, NDArrayDs1),
            param(dt64ms, 1, NDArrayDms1),
            param(dt64us, 1, NDArrayDus1),
            param(dt64ns, 1, NDArrayDns1),
            param(dt64ps, 1, NDArrayDps1),
            param(dt64fs, 1, NDArrayDfs1),
            param(dt64as, 1, NDArrayDas1),
            param(float, 1, NDArrayF1),
            param(int64, 1, NDArrayI1),
            param(object, 1, NDArrayO1),
            # ndim 2
            param(bool, 2, NDArrayB2),
            param(dt64D, 2, NDArrayD2),
            param(dt64Y, 2, NDArrayDY2),
            param(dt64M, 2, NDArrayDM2),
            param(dt64W, 2, NDArrayDW2),
            param(dt64D, 2, NDArrayDD2),
            param(dt64h, 2, NDArrayDh2),
            param(dt64m, 2, NDArrayDm2),
            param(dt64s, 2, NDArrayDs2),
            param(dt64ms, 2, NDArrayDms2),
            param(dt64us, 2, NDArrayDus2),
            param(dt64ns, 2, NDArrayDns2),
            param(dt64ps, 2, NDArrayDps2),
            param(dt64fs, 2, NDArrayDfs2),
            param(dt64as, 2, NDArrayDas2),
            param(float, 2, NDArrayF2),
            param(int64, 2, NDArrayI2),
            param(object, 2, NDArrayO2),
            # ndim 3
            param(bool, 3, NDArrayB3),
            param(dt64D, 3, NDArrayD3),
            param(dt64Y, 3, NDArrayDY3),
            param(dt64M, 3, NDArrayDM3),
            param(dt64W, 3, NDArrayDW3),
            param(dt64D, 3, NDArrayDD3),
            param(dt64h, 3, NDArrayDh3),
            param(dt64m, 3, NDArrayDm3),
            param(dt64s, 3, NDArrayDs3),
            param(dt64ms, 3, NDArrayDms3),
            param(dt64us, 3, NDArrayDus3),
            param(dt64ns, 3, NDArrayDns3),
            param(dt64ps, 3, NDArrayDps3),
            param(dt64fs, 3, NDArrayDfs3),
            param(dt64as, 3, NDArrayDas3),
            param(float, 3, NDArrayF3),
            param(int64, 3, NDArrayI3),
            param(object, 3, NDArrayO3),
        ],
    )
    def test_compound(self, *, dtype: Any, ndim: int, hint: Any) -> None:
        arr = empty(zeros(ndim, dtype=int64), dtype=dtype)
        die_if_unbearable(arr, hint)

    @given(arr=int_arrays())
    @example(arr=array([], dtype=int))
    @mark.parametrize("dtype", [param(int), param(float)])
    @mark.parametrize(
        "hint",
        [
            param(NDArrayINeg),
            param(NDArrayINonNeg),
            param(NDArrayINonPos),
            param(NDArrayINonZr),
            param(NDArrayIPos),
            param(NDArrayIZr),
            param(NDArrayI0Neg),
            param(NDArrayI0NonNeg),
            param(NDArrayI0NonPos),
            param(NDArrayI0NonZr),
            param(NDArrayI0Pos),
            param(NDArrayI0Zr),
            param(NDArrayI1Neg),
            param(NDArrayI1NonNeg),
            param(NDArrayI1NonPos),
            param(NDArrayI1NonZr),
            param(NDArrayI1Pos),
            param(NDArrayI1Zr),
            param(NDArrayI2Neg),
            param(NDArrayI2NonNeg),
            param(NDArrayI2NonPos),
            param(NDArrayI2NonZr),
            param(NDArrayI2Pos),
            param(NDArrayI2Zr),
            param(NDArrayI3Neg),
            param(NDArrayI3NonNeg),
            param(NDArrayI3NonPos),
            param(NDArrayI3NonZr),
            param(NDArrayI3Pos),
            param(NDArrayI3Zr),
        ],
    )
    @settings(max_examples=1, phases={Phase.explicit, Phase.generate})
    def test_int_checks(self, *, arr: NDArrayI, dtype: Any, hint: Any) -> None:
        with suppress(BeartypeDoorHintViolation):
            die_if_unbearable(arr.astype(dtype), hint)

    @given(arr=float_arrays())
    @example(arr=array([], dtype=float))
    @example(arr=array([nan], dtype=float))
    @example(arr=array([nan, nan], dtype=float))
    @mark.parametrize(
        "hint",
        [
            param(NDArrayFFin),
            param(NDArrayFFinInt),
            param(NDArrayFFinIntNan),
            param(NDArrayFFinNeg),
            param(NDArrayFFinNegNan),
            param(NDArrayFFinNonNeg),
            param(NDArrayFFinNonNegNan),
            param(NDArrayFFinNonPos),
            param(NDArrayFFinNonPosNan),
            param(NDArrayFFinNonZr),
            param(NDArrayFFinNonZrNan),
            param(NDArrayFFinPos),
            param(NDArrayFFinPosNan),
            param(NDArrayFFinNan),
            param(NDArrayFInt),
            param(NDArrayFIntNan),
            param(NDArrayFNeg),
            param(NDArrayFNegNan),
            param(NDArrayFNonNeg),
            param(NDArrayFNonNegNan),
            param(NDArrayFNonPos),
            param(NDArrayFNonPosNan),
            param(NDArrayFNonZr),
            param(NDArrayFNonZrNan),
            param(NDArrayFPos),
            param(NDArrayFPosNan),
            param(NDArrayFZr),
            param(NDArrayFZrNonMic),
            param(NDArrayFZrNonMicNan),
            param(NDArrayFZrNan),
            param(NDArrayFZrFinNonMic),
            param(NDArrayFZrFinNonMicNan),
            param(NDArrayF0Fin),
            param(NDArrayF0FinInt),
            param(NDArrayF0FinIntNan),
            param(NDArrayF0FinNeg),
            param(NDArrayF0FinNegNan),
            param(NDArrayF0FinNonNeg),
            param(NDArrayF0FinNonNegNan),
            param(NDArrayF0FinNonPos),
            param(NDArrayF0FinNonPosNan),
            param(NDArrayF0FinNonZr),
            param(NDArrayF0FinNonZrNan),
            param(NDArrayF0FinPos),
            param(NDArrayF0FinPosNan),
            param(NDArrayF0FinNan),
            param(NDArrayF0Int),
            param(NDArrayF0IntNan),
            param(NDArrayF0Neg),
            param(NDArrayF0NegNan),
            param(NDArrayF0NonNeg),
            param(NDArrayF0NonNegNan),
            param(NDArrayF0NonPos),
            param(NDArrayF0NonPosNan),
            param(NDArrayF0NonZr),
            param(NDArrayF0NonZrNan),
            param(NDArrayF0Pos),
            param(NDArrayF0PosNan),
            param(NDArrayF0Zr),
            param(NDArrayF0ZrNonMic),
            param(NDArrayF0ZrNonMicNan),
            param(NDArrayF0ZrNan),
            param(NDArrayF0ZrFinNonMic),
            param(NDArrayF0ZrFinNonMicNan),
            param(NDArrayF1Fin),
            param(NDArrayF1FinInt),
            param(NDArrayF1FinIntNan),
            param(NDArrayF1FinNeg),
            param(NDArrayF1FinNegNan),
            param(NDArrayF1FinNonNeg),
            param(NDArrayF1FinNonNegNan),
            param(NDArrayF1FinNonPos),
            param(NDArrayF1FinNonPosNan),
            param(NDArrayF1FinNonZr),
            param(NDArrayF1FinNonZrNan),
            param(NDArrayF1FinPos),
            param(NDArrayF1FinPosNan),
            param(NDArrayF1FinNan),
            param(NDArrayF1Int),
            param(NDArrayF1IntNan),
            param(NDArrayF1Neg),
            param(NDArrayF1NegNan),
            param(NDArrayF1NonNeg),
            param(NDArrayF1NonNegNan),
            param(NDArrayF1NonPos),
            param(NDArrayF1NonPosNan),
            param(NDArrayF1NonZr),
            param(NDArrayF1NonZrNan),
            param(NDArrayF1Pos),
            param(NDArrayF1PosNan),
            param(NDArrayF1Zr),
            param(NDArrayF1ZrNonMic),
            param(NDArrayF1ZrNonMicNan),
            param(NDArrayF1ZrNan),
            param(NDArrayF1ZrFinNonMic),
            param(NDArrayF1ZrFinNonMicNan),
            param(NDArrayF2Fin),
            param(NDArrayF2FinInt),
            param(NDArrayF2FinIntNan),
            param(NDArrayF2FinNeg),
            param(NDArrayF2FinNegNan),
            param(NDArrayF2FinNonNeg),
            param(NDArrayF2FinNonNegNan),
            param(NDArrayF2FinNonPos),
            param(NDArrayF2FinNonPosNan),
            param(NDArrayF2FinNonZr),
            param(NDArrayF2FinNonZrNan),
            param(NDArrayF2FinPos),
            param(NDArrayF2FinPosNan),
            param(NDArrayF2FinNan),
            param(NDArrayF2Int),
            param(NDArrayF2IntNan),
            param(NDArrayF2Neg),
            param(NDArrayF2NegNan),
            param(NDArrayF2NonNeg),
            param(NDArrayF2NonNegNan),
            param(NDArrayF2NonPos),
            param(NDArrayF2NonPosNan),
            param(NDArrayF2NonZr),
            param(NDArrayF2NonZrNan),
            param(NDArrayF2Pos),
            param(NDArrayF2PosNan),
            param(NDArrayF2Zr),
            param(NDArrayF2ZrNonMic),
            param(NDArrayF2ZrNonMicNan),
            param(NDArrayF2ZrNan),
            param(NDArrayF2ZrFinNonMic),
            param(NDArrayF2ZrFinNonMicNan),
            param(NDArrayF3Fin),
            param(NDArrayF3FinInt),
            param(NDArrayF3FinIntNan),
            param(NDArrayF3FinNeg),
            param(NDArrayF3FinNegNan),
            param(NDArrayF3FinNonNeg),
            param(NDArrayF3FinNonNegNan),
            param(NDArrayF3FinNonPos),
            param(NDArrayF3FinNonPosNan),
            param(NDArrayF3FinNonZr),
            param(NDArrayF3FinNonZrNan),
            param(NDArrayF3FinPos),
            param(NDArrayF3FinPosNan),
            param(NDArrayF3FinNan),
            param(NDArrayF3Int),
            param(NDArrayF3IntNan),
            param(NDArrayF3Neg),
            param(NDArrayF3NegNan),
            param(NDArrayF3NonNeg),
            param(NDArrayF3NonNegNan),
            param(NDArrayF3NonPos),
            param(NDArrayF3NonPosNan),
            param(NDArrayF3NonZr),
            param(NDArrayF3NonZrNan),
            param(NDArrayF3Pos),
            param(NDArrayF3PosNan),
            param(NDArrayF3Zr),
            param(NDArrayF3ZrNonMic),
            param(NDArrayF3ZrNonMicNan),
            param(NDArrayF3ZrNan),
            param(NDArrayF3ZrFinNonMic),
            param(NDArrayF3ZrFinNonMicNan),
        ],
    )
    @settings(max_examples=1, phases={Phase.explicit, Phase.generate})
    def test_float_checks(self, *, arr: NDArrayF, hint: Any) -> None:
        with suppress(BeartypeDoorHintViolation):
            die_if_unbearable(arr, hint)


class TestArrayIndexer:
    @mark.parametrize(
        ("i", "ndim", "expected"),
        [
            param(0, 1, (0,)),
            param(0, 2, (slice(None), 0)),
            param(1, 2, (slice(None), 1)),
            param(0, 3, (slice(None), slice(None), 0)),
            param(1, 3, (slice(None), slice(None), 1)),
            param(2, 3, (slice(None), slice(None), 2)),
        ],
    )
    def test_main(
        self, *, i: int, ndim: int, expected: tuple[int | slice, ...]
    ) -> None:
        assert array_indexer(i, ndim) == expected

    @mark.parametrize(
        ("i", "ndim", "axis", "expected"),
        [
            param(0, 1, 0, (0,)),
            param(0, 2, 0, (0, slice(None))),
            param(0, 2, 1, (slice(None), 0)),
            param(1, 2, 0, (1, slice(None))),
            param(1, 2, 1, (slice(None), 1)),
            param(0, 3, 0, (0, slice(None), slice(None))),
            param(0, 3, 1, (slice(None), 0, slice(None))),
            param(0, 3, 2, (slice(None), slice(None), 0)),
            param(1, 3, 0, (1, slice(None), slice(None))),
            param(1, 3, 1, (slice(None), 1, slice(None))),
            param(1, 3, 2, (slice(None), slice(None), 1)),
            param(2, 3, 0, (2, slice(None), slice(None))),
            param(2, 3, 1, (slice(None), 2, slice(None))),
            param(2, 3, 2, (slice(None), slice(None), 2)),
        ],
    )
    def test_axis(
        self, *, i: int, ndim: int, axis: int, expected: tuple[int | slice, ...]
    ) -> None:
        assert array_indexer(i, ndim, axis=axis) == expected


class TestAsInt:
    @given(n=integers(-10, 10))
    def test_main(self, *, n: int) -> None:
        arr = array([n], dtype=float)
        result = as_int(arr)
        expected = array([n], dtype=int)
        assert_equal(result, expected)

    @given(n=integers(-10, 10))
    def test_nan_elements_filled(self, *, n: int) -> None:
        arr = array([nan], dtype=float)
        result = as_int(arr, nan=n)
        expected = array([n], dtype=int)
        assert_equal(result, expected)

    @given(n=integers(-10, 10))
    def test_inf_elements_filled(self, *, n: int) -> None:
        arr = array([inf], dtype=float)
        result = as_int(arr, inf=n)
        expected = array([n], dtype=int)
        assert_equal(result, expected)

    @mark.parametrize("value", [param(inf), param(nan), param(0.5)])
    def test_errors(self, *, value: float) -> None:
        arr = array([value], dtype=float)
        with raises(AsIntError):
            _ = as_int(arr)


class TestDateToDatetime64ns:
    def test_example(self) -> None:
        result = date_to_datetime64(dt.date(2000, 1, 1))
        assert result == datetime64("2000-01-01", "D")
        assert result.dtype == dt64D

    @given(date=dates())
    def test_main(self, *, date: dt.date) -> None:
        result = date_to_datetime64(date)
        assert result.dtype == dt64D


class TestDatetimeToDatetime64:
    @mark.parametrize("tzinfo", [param(UTC), param(None)])
    def test_example(self, *, tzinfo: dt.tzinfo) -> None:
        result = datetime_to_datetime64(
            dt.datetime(2000, 1, 1, 0, 0, 0, 123456, tzinfo=tzinfo)
        )
        assert result == datetime64("2000-01-01 00:00:00.123456", "us")
        assert result.dtype == dt64us

    @given(datetime=datetimes() | datetimes_utc())
    def test_main(self, *, datetime: dt.datetime) -> None:
        result = datetime_to_datetime64(datetime)
        assert result.dtype == dt64us

    @given(datetime=datetimes(timezones=just(HONG_KONG)))
    def test_error(self, *, datetime: dt.datetime) -> None:
        with raises(
            DatetimeToDatetime64Error, match=r"Timezone must be None or UTC; got .*\."
        ):
            _ = datetime_to_datetime64(datetime)


class TestDatetime64ToDate:
    def test_example(self) -> None:
        assert datetime64_to_date(datetime64("2000-01-01", "D")) == dt.date(2000, 1, 1)

    @given(date=dates())
    def test_round_trip(self, *, date: dt.date) -> None:
        assert datetime64_to_date(date_to_datetime64(date)) == date

    @mark.parametrize(
        ("datetime", "dtype", "error"),
        [
            param("10000-01-01", "D", DateTime64ToDateError),
            param("2000-01-01", "ns", NotImplementedError),
        ],
    )
    def test_error(self, *, datetime: str, dtype: str, error: type[Exception]) -> None:
        with raises(error):
            _ = datetime64_to_date(datetime64(datetime, dtype))


class TestDatetime64ToInt:
    def test_example(self) -> None:
        expected = 10957
        assert datetime64_to_int(datetime64("2000-01-01", "D")) == expected

    @given(datetime=datetime64s())
    def test_main(self, *, datetime: datetime64) -> None:
        _ = datetime64_to_int(datetime)

    @given(data=data(), unit=datetime64_units())
    def test_round_trip(self, *, data: DataObject, unit: Datetime64Unit) -> None:
        datetime = data.draw(datetime64s(unit=unit))
        result = datetime64(datetime64_to_int(datetime), unit)
        assert result == datetime


class TestDatetime64ToDatetime:
    def test_example_ms(self) -> None:
        assert datetime64_to_datetime(
            datetime64("2000-01-01 00:00:00.123", "ms")
        ) == dt.datetime(2000, 1, 1, 0, 0, 0, 123000, tzinfo=UTC)

    @mark.parametrize("dtype", [param("us"), param("ns")])
    def test_examples_us_ns(self, *, dtype: str) -> None:
        assert datetime64_to_datetime(
            datetime64("2000-01-01 00:00:00.123456", dtype)
        ) == dt.datetime(2000, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)

    @given(datetime=datetimes_utc())
    def test_round_trip(self, *, datetime: dt.datetime) -> None:
        assert datetime64_to_datetime(datetime_to_datetime64(datetime)) == datetime

    @mark.parametrize(
        ("datetime", "dtype", "error"),
        [
            param("0000-12-31", "ms", DateTime64ToDateTimeError),
            param("10000-01-01", "ms", DateTime64ToDateTimeError),
            param("1970-01-01 00:00:00.000000001", "ns", DateTime64ToDateTimeError),
            param("2000-01-01", "D", NotImplementedError),
        ],
    )
    def test_error(self, *, datetime: str, dtype: str, error: type[Exception]) -> None:
        with raises(error):
            _ = datetime64_to_datetime(datetime64(datetime, dtype))


class TestDatetime64DTypeToUnit:
    @mark.parametrize(
        ("dtype", "expected"),
        [param(dt64D, "D"), param(dt64Y, "Y"), param(dt64ns, "ns")],
    )
    def test_example(self, *, dtype: Any, expected: Datetime64Unit) -> None:
        assert datetime64_dtype_to_unit(dtype) == expected

    @given(dtype=datetime64_dtypes())
    def test_round_trip(self, *, dtype: Any) -> None:
        assert datetime64_unit_to_dtype(datetime64_dtype_to_unit(dtype)) == dtype


class TestDatetime64DUnitToDType:
    @mark.parametrize(
        ("unit", "expected"),
        [param("D", dt64D), param("Y", dt64Y), param("ns", dt64ns)],
    )
    def test_example(self, *, unit: Datetime64Unit, expected: Any) -> None:
        assert datetime64_unit_to_dtype(unit) == expected

    @given(unit=datetime64_units())
    def test_round_trip(self, *, unit: Datetime64Unit) -> None:
        assert datetime64_dtype_to_unit(datetime64_unit_to_dtype(unit)) == unit


class TestDatetime64DUnitToKind:
    @mark.parametrize(
        ("unit", "expected"),
        [param("D", "date"), param("Y", "date"), param("ns", "time")],
    )
    def test_example(self, *, unit: Datetime64Unit, expected: Datetime64Kind) -> None:
        assert datetime64_unit_to_kind(unit) == expected


class TestDefaultRng:
    def test_main(self) -> None:
        assert isinstance(DEFAULT_RNG, Generator)


class TestDiscretize:
    @given(arr=float_arrays(shape=integers(0, 10), min_value=-1.0, max_value=1.0))
    def test_1_bin(self, *, arr: NDArrayF1) -> None:
        result = discretize(arr, 1)
        expected = zeros_like(arr, dtype=float)
        assert_equal(result, expected)

    @given(
        arr=float_arrays(
            shape=integers(1, 10), min_value=-1.0, max_value=1.0, unique=True
        )
    )
    def test_2_bins(self, *, arr: NDArrayF1) -> None:
        _ = assume(len(arr) % 2 == 0)
        result = discretize(arr, 2)
        med = median(arr)
        is_below = (arr < med) & ~isclose(arr, med)
        assert isclose(result[is_below], 0.0).all()
        is_above = (arr > med) & ~isclose(arr, med)
        assert isclose(result[is_above], 1.0).all()

    @given(bins=integers(1, 10))
    def test_empty(self, *, bins: int) -> None:
        arr = array([], dtype=float)
        result = discretize(arr, bins)
        assert_equal(result, arr)

    @given(n=integers(0, 10), bins=integers(1, 10))
    def test_all_nan(self, *, n: int, bins: int) -> None:
        arr = full(n, nan, dtype=float)
        result = discretize(arr, bins)
        assert_equal(result, arr)

    @mark.parametrize(
        ("arr_v", "bins", "expected_v"),
        [
            param(
                [1.0, 2.0, 3.0, 4.0],
                [0.0, 0.25, 0.5, 0.75, 1.0],
                [0.0, 1.0, 2.0, 3.0],
                id="equally spaced",
            ),
            param(
                [1.0, 2.0, 3.0, 4.0],
                [0.0, 0.1, 0.9, 1.0],
                [0.0, 1.0, 1.0, 2.0],
                id="unequally spaced",
            ),
            param(
                [1.0, 2.0, 3.0],
                [0.0, 0.33, 1.0],
                [0.0, 1.0, 1.0],
                id="equally spaced 1 to 2",
            ),
            param(
                [1.0, 2.0, 3.0, nan],
                [0.0, 0.33, 1.0],
                [0.0, 1.0, 1.0, nan],
                id="with nan",
            ),
        ],
    )
    def test_bins_of_floats(
        self,
        *,
        arr_v: Sequence[float],
        bins: Sequence[float],
        expected_v: Sequence[float],
    ) -> None:
        arr = array(arr_v, dtype=float)
        result = discretize(arr, bins)
        expected = array(expected_v, dtype=float)
        assert_equal(result, expected)


class TestFFillNonNanSlices:
    @mark.parametrize(
        ("limit", "axis", "expected_v"),
        [
            param(
                None,
                0,
                [[0.1, nan, nan, 0.2], [0.1, nan, nan, 0.2], [0.3, nan, nan, nan]],
            ),
            param(None, 1, [[0.1, 0.1, 0.1, 0.2], 4 * [nan], [0.3, 0.3, 0.3, nan]]),
            param(
                1, 0, [[0.1, nan, nan, 0.2], [0.1, nan, nan, 0.2], [0.3, nan, nan, nan]]
            ),
            param(1, 1, [[0.1, 0.1, nan, 0.2], 4 * [nan], [0.3, 0.3, nan, nan]]),
        ],
    )
    def test_main(
        self, *, limit: int | None, axis: int, expected_v: Sequence[Sequence[float]]
    ) -> None:
        arr = array(
            [[0.1, nan, nan, 0.2], 4 * [nan], [0.3, nan, nan, nan]], dtype=float
        )
        result = ffill_non_nan_slices(arr, limit=limit, axis=axis)
        expected = array(expected_v, dtype=float)
        assert_equal(result, expected)

    @mark.parametrize(
        ("axis", "expected_v"),
        [
            param(0, [4 * [nan], [nan, 0.1, nan, nan], [nan, 0.1, nan, nan]]),
            param(1, [4 * [nan], [nan, 0.1, 0.1, 0.1], 4 * [nan]]),
        ],
    )
    def test_initial_all_nan(
        self, *, axis: int, expected_v: Sequence[Sequence[float]]
    ) -> None:
        arr = array([4 * [nan], [nan, 0.1, nan, nan], 4 * [nan]], dtype=float)
        result = ffill_non_nan_slices(arr, axis=axis)
        expected = array(expected_v, dtype=float)
        assert_equal(result, expected)


class TestFillNa:
    @mark.parametrize(
        ("init", "value", "expected_v"),
        [
            param(0.0, 0.0, 0.0),
            param(0.0, nan, 0.0),
            param(0.0, inf, 0.0),
            param(nan, 0.0, 0.0),
            param(nan, nan, nan),
            param(nan, inf, inf),
            param(inf, 0.0, inf),
            param(inf, nan, inf),
            param(inf, inf, inf),
        ],
    )
    def test_main(self, *, init: float, value: float, expected_v: float) -> None:
        arr = array([init], dtype=float)
        result = fillna(arr, value=value)
        expected = array([expected_v], dtype=float)
        assert_equal(result, expected)


class TestFlatN0:
    @given(data=data(), n=integers(1, 10))
    def test_main(self, *, data: DataObject, n: int) -> None:
        i = data.draw(integers(0, n - 1))
        arr = arange(n) == i
        result = flatn0(arr)
        assert result == i

    @mark.parametrize(
        "array", [param(zeros(0, dtype=bool)), param(ones(2, dtype=bool))]
    )
    def test_errors(self, *, array: NDArrayB1) -> None:
        with raises(FlatN0Error):
            _ = flatn0(array)


class TestGetFillValue:
    @mark.parametrize(
        "dtype",
        [
            param(bool),
            param(dt64D),
            param(dt64Y),
            param(dt64ns),
            param(float),
            param(int),
            param(object),
        ],
    )
    def test_main(self, *, dtype: Any) -> None:
        fill_value = get_fill_value(dtype)
        array = full(0, fill_value, dtype=dtype)
        assert has_dtype(array, dtype)

    def test_error(self) -> None:
        with raises(GetFillValueError):
            _ = get_fill_value(None)


class TestHasDtype:
    @mark.parametrize(("dtype", "expected"), [param(float, True), param(int, False)])
    @mark.parametrize("is_tuple", [param(True), param(False)])
    def test_main(self, *, dtype: Any, is_tuple: bool, expected: bool) -> None:
        against = (dtype,) if is_tuple else dtype
        result = has_dtype(array([], dtype=float), against)
        assert result is expected


class TestIsAtLeast:
    @mark.parametrize(
        ("x", "y", "equal_nan", "expected"),
        [
            param(0.0, -inf, False, True),
            param(0.0, -1.0, False, True),
            param(0.0, -1e-6, False, True),
            param(0.0, -1e-7, False, True),
            param(0.0, -1e-8, False, True),
            param(0.0, 0.0, False, True),
            param(0.0, 1e-8, False, True),
            param(0.0, 1e-7, False, False),
            param(0.0, 1e-6, False, False),
            param(0.0, 1.0, False, False),
            param(0.0, inf, False, False),
            param(0.0, nan, False, False),
            param(nan, nan, True, True),
        ],
    )
    def test_main(self, *, x: float, y: float, equal_nan: bool, expected: bool) -> None:
        assert is_at_least(x, y, equal_nan=equal_nan).item() is expected

    @mark.parametrize(
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
    )
    def test_nan(self, *, y: float) -> None:
        assert is_at_least_or_nan(nan, y)


class TestIsAtMost:
    @mark.parametrize(
        ("x", "y", "equal_nan", "expected"),
        [
            param(0.0, -inf, False, False),
            param(0.0, -1.0, False, False),
            param(0.0, -1e-6, False, False),
            param(0.0, -1e-7, False, False),
            param(0.0, -1e-8, False, True),
            param(0.0, 0.0, False, True),
            param(0.0, 1e-8, False, True),
            param(0.0, 1e-7, False, True),
            param(0.0, 1e-6, False, True),
            param(0.0, 1.0, False, True),
            param(0.0, inf, False, True),
            param(0.0, nan, False, False),
            param(nan, nan, True, True),
        ],
    )
    def test_main(self, *, x: float, y: float, equal_nan: bool, expected: bool) -> None:
        assert is_at_most(x, y, equal_nan=equal_nan).item() is expected

    @mark.parametrize(
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
    )
    def test_nan(self, *, y: float) -> None:
        assert is_at_most_or_nan(nan, y)


class TestIsBetween:
    @mark.parametrize(
        ("x", "low", "high", "equal_nan", "expected"),
        [
            param(0.0, -1.0, -1.0, False, False),
            param(0.0, -1.0, 0.0, False, True),
            param(0.0, -1.0, 1.0, False, True),
            param(0.0, 0.0, -1.0, False, False),
            param(0.0, 0.0, 0.0, False, True),
            param(0.0, 0.0, 1.0, False, True),
            param(0.0, 1.0, -1.0, False, False),
            param(0.0, 1.0, 0.0, False, False),
            param(0.0, 1.0, 1.0, False, False),
            param(nan, -1.0, 1.0, False, False),
        ],
    )
    def test_main(
        self, *, x: float, low: float, high: float, equal_nan: bool, expected: bool
    ) -> None:
        assert is_between(x, low, high, equal_nan=equal_nan).item() is expected

    @mark.parametrize(
        "low",
        [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)],
    )
    @mark.parametrize(
        "high",
        [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)],
    )
    def test_nan(self, *, low: float, high: float) -> None:
        assert is_between_or_nan(nan, low, high)


class TestIsEmptyAndIsNotEmpty:
    @mark.parametrize(
        ("shape", "expected"),
        [
            param(0, "empty"),
            param(1, "non-empty"),
            param(2, "non-empty"),
            param((), "empty"),
            param((0,), "empty"),
            param((1,), "non-empty"),
            param((2,), "non-empty"),
            param((0, 0), "empty"),
            param((0, 1), "empty"),
            param((0, 2), "empty"),
            param((1, 0), "empty"),
            param((1, 1), "non-empty"),
            param((1, 2), "non-empty"),
            param((2, 0), "empty"),
            param((2, 1), "non-empty"),
            param((2, 2), "non-empty"),
        ],
    )
    @mark.parametrize("kind", [param("shape"), param("array")])
    def test_main(
        self,
        *,
        shape: int | tuple[int, ...],
        kind: Literal["shape", "array"],
        expected: Literal["empty", "non-empty"],
    ) -> None:
        shape_or_array = shape if kind == "shape" else zeros(shape, dtype=float)
        assert is_empty(shape_or_array) is (expected == "empty")
        assert is_non_empty(shape_or_array) is (expected == "non-empty")


class TestIsFiniteAndIntegral:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-2.0, True),
            param(-1.5, False),
            param(-1.0, True),
            param(-0.5, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, False),
            param(1e-6, False),
            param(0.5, False),
            param(1.0, True),
            param(1.5, False),
            param(2.0, True),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_integral(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_integral_or_nan(nan)


class TestIsFiniteOrNan:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, True),
            param(0.0, True),
            param(1.0, True),
            param(inf, False),
            param(nan, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_or_nan(x).item() is expected


class TestIsFiniteAndNegative:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, False),
            param(1e-6, False),
            param(1.0, False),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_negative(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_negative_or_nan(nan)


class TestIsFiniteAndNonNegative:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_non_negative(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_non_negative_or_nan(nan)


class TestIsFiniteAndNonPositive:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, False),
            param(1e-6, False),
            param(1.0, False),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_non_positive(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_non_positive_or_nan(nan)


class TestIsFiniteAndNonZero:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_non_zero(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_non_zero_or_nan(nan)


class TestIsFiniteAndPositive:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_positive(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_positive_or_nan(nan)


class TestIsGreaterThan:
    @mark.parametrize(
        ("x", "y", "equal_nan", "expected"),
        [
            param(0.0, -inf, False, True),
            param(0.0, -1.0, False, True),
            param(0.0, -1e-6, False, True),
            param(0.0, -1e-7, False, True),
            param(0.0, -1e-8, False, False),
            param(0.0, 0.0, False, False),
            param(0.0, 1e-8, False, False),
            param(0.0, 1e-7, False, False),
            param(0.0, 1e-6, False, False),
            param(0.0, 1.0, False, False),
            param(0.0, inf, False, False),
            param(0.0, nan, False, False),
            param(nan, nan, True, True),
        ],
    )
    def test_main(self, *, x: float, y: float, equal_nan: bool, expected: bool) -> None:
        assert is_greater_than(x, y, equal_nan=equal_nan).item() is expected

    @mark.parametrize(
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
    )
    def test_nan(self, *, y: float) -> None:
        assert is_greater_than_or_nan(nan, y)


class TestIsIntegral:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, True),
            param(-2.0, True),
            param(-1.5, False),
            param(-1.0, True),
            param(-0.5, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, False),
            param(1e-6, False),
            param(0.5, False),
            param(1.0, True),
            param(1.5, False),
            param(2.0, True),
            param(inf, True),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_integral(x).item() is expected

    def test_nan(self) -> None:
        assert is_integral_or_nan(nan)


class TestIsLessThan:
    @mark.parametrize(
        ("x", "y", "equal_nan", "expected"),
        [
            param(0.0, -inf, False, False),
            param(0.0, -1.0, False, False),
            param(0.0, -1e-6, False, False),
            param(0.0, -1e-7, False, False),
            param(0.0, -1e-8, False, False),
            param(0.0, 0.0, False, False),
            param(0.0, 1e-8, False, False),
            param(0.0, 1e-7, False, True),
            param(0.0, 1e-6, False, True),
            param(0.0, 1.0, False, True),
            param(0.0, inf, False, True),
            param(0.0, nan, False, False),
            param(nan, nan, True, True),
        ],
    )
    def test_main(self, *, x: float, y: float, equal_nan: bool, expected: bool) -> None:
        assert is_less_than(x, y, equal_nan=equal_nan).item() is expected

    @mark.parametrize(
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
    )
    def test_nan(self, *, y: float) -> None:
        assert is_less_than_or_nan(nan, y)


class TestIsNegative:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, True),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, False),
            param(1e-6, False),
            param(1.0, False),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_negative(x).item() is expected

    def test_nan(self) -> None:
        assert is_negative_or_nan(nan)


class TestIsNonNegative:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, True),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_non_negative(x).item() is expected

    def test_nan(self) -> None:
        assert is_non_negative_or_nan(nan)


class TestIsNonPositive:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, True),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, False),
            param(1e-6, False),
            param(1.0, False),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_non_positive(x).item() is expected

    def test_nan(self) -> None:
        assert is_non_positive_or_nan(nan)


class TestIsNonSingular:
    @mark.parametrize(
        ("array", "expected"), [param(eye(2), True), param(ones((2, 2)), False)]
    )
    @mark.parametrize("dtype", [param(float), param(int)])
    def test_main(self, *, array: NDArrayF2, dtype: Any, expected: bool) -> None:
        assert is_non_singular(array.astype(dtype)) is expected

    def test_overflow(self) -> None:
        arr = array([[0.0, 0.0], [5e-323, 0.0]], dtype=float)
        assert not is_non_singular(arr)


class TestIsNonZero:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, True),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, True),
            param(nan, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_non_zero(x).item() is expected

    def test_nan(self) -> None:
        assert is_non_zero_or_nan(nan)


class TestIsPositive:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, True),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_positive(x).item() is expected

    def test_nan(self) -> None:
        assert is_positive_or_nan(nan)


class TestIsPositiveSemiDefinite:
    @mark.parametrize(
        ("array", "expected"),
        [
            param(eye(2), True),
            param(zeros((1, 2), dtype=float), False),
            param(arange(4).reshape((2, 2)), False),
        ],
    )
    @mark.parametrize("dtype", [param(float), param(int)])
    def test_main(
        self, *, array: NDArrayF2 | NDArrayI2, dtype: Any, expected: bool
    ) -> None:
        assert is_positive_semidefinite(array.astype(dtype)) is expected

    @given(array=float_arrays(shape=(2, 2), min_value=-1.0, max_value=1.0))
    def test_overflow(self, *, array: NDArrayF2) -> None:
        _ = is_positive_semidefinite(array)


class TestIsSymmetric:
    @mark.parametrize(
        ("array", "expected"),
        [
            param(eye(2), True),
            param(zeros((1, 2), dtype=float), False),
            param(arange(4).reshape((2, 2)), False),
        ],
    )
    @mark.parametrize("dtype", [param(float), param(int)])
    def test_main(
        self, *, array: NDArrayF2 | NDArrayI2, dtype: Any, expected: bool
    ) -> None:
        assert is_symmetric(array.astype(dtype)) is expected


class TestIsZero:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, False),
            param(1e-6, False),
            param(1.0, False),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_zero(x).item() is expected

    def test_is_zero_or_nan(self) -> None:
        assert is_zero_or_nan(nan)


class TestIsZeroOrFiniteAndMicro:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, True),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_zero_or_finite_and_non_micro(x).item() is expected

    def test_nan(self) -> None:
        assert is_zero_or_finite_and_non_micro_or_nan(nan)


class TestIsZeroOrNonMicro:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, True),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, True),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, True),
            param(nan, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_zero_or_non_micro(x).item() is expected

    def test_nan(self) -> None:
        assert is_zero_or_non_micro_or_nan(nan)


class TestMaximumMinimum:
    def test_maximum_floats(self) -> None:
        result = maximum(1.0, 2.0)
        assert isinstance(result, float)

    def test_maximum_arrays(self) -> None:
        result = maximum(array([1.0], dtype=float), array([2.0], dtype=float))
        assert isinstance(result, ndarray)

    def test_minimum_floats(self) -> None:
        result = minimum(1.0, 2.0)
        assert isinstance(result, float)

    def test_minimum_arrays(self) -> None:
        result = minimum(array([1.0], dtype=float), array([2.0], dtype=float))
        assert isinstance(result, ndarray)


class TestNDims:
    @mark.parametrize(
        ("ndim", "hint"),
        [param(0, NDim0), param(1, NDim1), param(2, NDim2), param(3, NDim3)],
    )
    def test_main(self, *, ndim: int, hint: Any) -> None:
        arr = empty(zeros(ndim, dtype=int), dtype=float)
        die_if_unbearable(arr, Annotated[NDArray[Any], hint])


class TestRedirectEmptyNumpyConcatenate:
    def test_main(self) -> None:
        with raises(EmptyNumpyConcatenateError), redirect_empty_numpy_concatenate():
            _ = concatenate([])


class TestShiftBool:
    @mark.parametrize(
        ("n", "expected_v"),
        [
            param(1, [None, True, False], id="n=1"),
            param(2, [None, None, True], id="n=2"),
            param(-1, [False, True, None], id="n=-1"),
            param(-2, [True, None, None], id="n=-2"),
        ],
    )
    @mark.parametrize("fill_value", [param(True), param(False)])
    def test_main(
        self, *, n: int, expected_v: Sequence[bool | None], fill_value: bool
    ) -> None:
        arr = array([True, False, True], dtype=bool)
        result = shift_bool(arr, n=n, fill_value=fill_value)
        expected = array(
            [fill_value if e is None else e for e in expected_v], dtype=bool
        )
        assert_equal(result, expected)


class TestYear:
    @given(date=dates())
    def test_scalar(self, *, date: dt.date) -> None:
        date64 = datetime64(date, "D")
        yr = year(date64)
        assert yr == date.year

    @given(date=dates())
    def test_array(self, *, date: dt.date) -> None:
        dates = array([date], dtype=dt64D)
        years = year(dates)
        assert years.item() == date.year
