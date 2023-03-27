from contextlib import suppress
from typing import Any

from beartype.door import die_if_unbearable
from beartype.roar import BeartypeDoorHintViolation
from hypothesis import Phase, example, given, settings
from numpy import array, empty, nan, zeros
from pytest import mark, param

from utilities.hypothesis.numpy import float_arrays
from utilities.numpy.typing import (
    NDArray0,
    NDArray1,
    NDArray2,
    NDArray3,
    NDArrayB,
    NDArrayB0,
    NDArrayB1,
    NDArrayB2,
    NDArrayB3,
    NDArrayDD,
    NDArrayDD0,
    NDArrayDD1,
    NDArrayDD2,
    NDArrayDD3,
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
    NDArrayDus,
    NDArrayDus0,
    NDArrayDus1,
    NDArrayDus2,
    NDArrayDus3,
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
    datetime64D,
    datetime64ms,
    datetime64ns,
    datetime64us,
    datetime64Y,
)


class TestHints:
    @mark.parametrize(
        ("dtype", "hint"),
        [
            param(bool, NDArrayB),
            param(datetime64D, NDArrayDD),
            param(datetime64Y, NDArrayDY),
            param(datetime64ms, NDArrayDms),
            param(datetime64ns, NDArrayDns),
            param(datetime64us, NDArrayDus),
            param(float, NDArrayF),
            param(int, NDArrayI),
            param(object, NDArrayO),
        ],
    )
    def test_dtype(self, dtype: Any, hint: Any) -> None:
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
    def test_ndim(self, ndim: int, hint: Any) -> None:
        arr = empty(zeros(ndim, dtype=int), dtype=float)
        die_if_unbearable(arr, hint)

    @mark.parametrize(
        ("dtype", "ndim", "hint"),
        [
            # ndim 0
            param(bool, 0, NDArrayB0),
            param(datetime64D, 0, NDArrayDD0),
            param(datetime64Y, 0, NDArrayDY0),
            param(datetime64ms, 0, NDArrayDms0),
            param(datetime64ns, 0, NDArrayDns0),
            param(datetime64us, 0, NDArrayDus0),
            param(float, 0, NDArrayF0),
            param(int, 0, NDArrayI0),
            param(object, 0, NDArrayO0),
            # ndim 1
            param(bool, 1, NDArrayB1),
            param(datetime64D, 1, NDArrayDD1),
            param(datetime64Y, 1, NDArrayDY1),
            param(datetime64ms, 1, NDArrayDms1),
            param(datetime64ns, 1, NDArrayDns1),
            param(datetime64us, 1, NDArrayDus1),
            param(float, 1, NDArrayF1),
            param(int, 1, NDArrayI1),
            param(object, 1, NDArrayO1),
            # ndim 2
            param(bool, 2, NDArrayB2),
            param(datetime64D, 2, NDArrayDD2),
            param(datetime64Y, 2, NDArrayDY2),
            param(datetime64ms, 2, NDArrayDms2),
            param(datetime64ns, 2, NDArrayDns2),
            param(datetime64us, 2, NDArrayDus2),
            param(float, 2, NDArrayF2),
            param(int, 2, NDArrayI2),
            param(object, 2, NDArrayO2),
            # ndim 3
            param(bool, 3, NDArrayB3),
            param(datetime64D, 3, NDArrayDD3),
            param(datetime64Y, 3, NDArrayDY3),
            param(datetime64ms, 3, NDArrayDms3),
            param(datetime64ns, 3, NDArrayDns3),
            param(datetime64us, 3, NDArrayDus3),
            param(float, 3, NDArrayF3),
            param(int, 3, NDArrayI3),
            param(object, 3, NDArrayO3),
        ],
    )
    def test_compound(self, dtype: Any, ndim: int, hint: Any) -> None:
        arr = empty(zeros(ndim, dtype=int), dtype=dtype)
        die_if_unbearable(arr, hint)

    @given(arr=float_arrays())
    @example(arr=array([], dtype=float))
    @example(arr=array([nan], dtype=float))
    @example(arr=array([nan, nan], dtype=float))
    @mark.parametrize(
        "hint",
        [
            param(NDArrayINeg),
            param(NDArrayINonNeg),
            param(NDArrayINonPos),
            param(NDArrayINonZr),
            param(NDArrayIPos),
            param(NDArrayIZr),
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
    def test_checks(self, arr: NDArrayF, hint: Any) -> None:
        with suppress(BeartypeDoorHintViolation):
            die_if_unbearable(arr, hint)
