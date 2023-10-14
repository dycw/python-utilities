from __future__ import annotations

from contextlib import suppress
from typing import Annotated
from typing import Any

from beartype.door import die_if_unbearable
from beartype.roar import BeartypeDoorHintViolation
from hypothesis import Phase
from hypothesis import example
from hypothesis import given
from hypothesis import settings
from numpy import array
from numpy import empty
from numpy import nan
from numpy import zeros
from numpy.typing import NDArray
from pytest import mark
from pytest import param

from utilities.hypothesis.numpy import float_arrays
from utilities.hypothesis.numpy import int_arrays
from utilities.numpy import NDArray0
from utilities.numpy import NDArray1
from utilities.numpy import NDArray2
from utilities.numpy import NDArray3
from utilities.numpy import NDArrayB
from utilities.numpy import NDArrayB0
from utilities.numpy import NDArrayB1
from utilities.numpy import NDArrayB2
from utilities.numpy import NDArrayB3
from utilities.numpy import NDArrayD0
from utilities.numpy import NDArrayD1
from utilities.numpy import NDArrayD2
from utilities.numpy import NDArrayD3
from utilities.numpy import NDArrayDas
from utilities.numpy import NDArrayDas0
from utilities.numpy import NDArrayDas1
from utilities.numpy import NDArrayDas2
from utilities.numpy import NDArrayDas3
from utilities.numpy import NDArrayDD
from utilities.numpy import NDArrayDD0
from utilities.numpy import NDArrayDD1
from utilities.numpy import NDArrayDD2
from utilities.numpy import NDArrayDD3
from utilities.numpy import NDArrayDfs
from utilities.numpy import NDArrayDfs0
from utilities.numpy import NDArrayDfs1
from utilities.numpy import NDArrayDfs2
from utilities.numpy import NDArrayDfs3
from utilities.numpy import NDArrayDh
from utilities.numpy import NDArrayDh0
from utilities.numpy import NDArrayDh1
from utilities.numpy import NDArrayDh2
from utilities.numpy import NDArrayDh3
from utilities.numpy import NDArrayDM
from utilities.numpy import NDArrayDm
from utilities.numpy import NDArrayDM0
from utilities.numpy import NDArrayDm0
from utilities.numpy import NDArrayDM1
from utilities.numpy import NDArrayDm1
from utilities.numpy import NDArrayDM2
from utilities.numpy import NDArrayDm2
from utilities.numpy import NDArrayDM3
from utilities.numpy import NDArrayDm3
from utilities.numpy import NDArrayDms
from utilities.numpy import NDArrayDms0
from utilities.numpy import NDArrayDms1
from utilities.numpy import NDArrayDms2
from utilities.numpy import NDArrayDms3
from utilities.numpy import NDArrayDns
from utilities.numpy import NDArrayDns0
from utilities.numpy import NDArrayDns1
from utilities.numpy import NDArrayDns2
from utilities.numpy import NDArrayDns3
from utilities.numpy import NDArrayDps
from utilities.numpy import NDArrayDps0
from utilities.numpy import NDArrayDps1
from utilities.numpy import NDArrayDps2
from utilities.numpy import NDArrayDps3
from utilities.numpy import NDArrayDs
from utilities.numpy import NDArrayDs0
from utilities.numpy import NDArrayDs1
from utilities.numpy import NDArrayDs2
from utilities.numpy import NDArrayDs3
from utilities.numpy import NDArrayDus
from utilities.numpy import NDArrayDus0
from utilities.numpy import NDArrayDus1
from utilities.numpy import NDArrayDus2
from utilities.numpy import NDArrayDus3
from utilities.numpy import NDArrayDW
from utilities.numpy import NDArrayDW0
from utilities.numpy import NDArrayDW1
from utilities.numpy import NDArrayDW2
from utilities.numpy import NDArrayDW3
from utilities.numpy import NDArrayDY
from utilities.numpy import NDArrayDY0
from utilities.numpy import NDArrayDY1
from utilities.numpy import NDArrayDY2
from utilities.numpy import NDArrayDY3
from utilities.numpy import NDArrayF
from utilities.numpy import NDArrayF0
from utilities.numpy import NDArrayF0Fin
from utilities.numpy import NDArrayF0FinInt
from utilities.numpy import NDArrayF0FinIntNan
from utilities.numpy import NDArrayF0FinNan
from utilities.numpy import NDArrayF0FinNeg
from utilities.numpy import NDArrayF0FinNegNan
from utilities.numpy import NDArrayF0FinNonNeg
from utilities.numpy import NDArrayF0FinNonNegNan
from utilities.numpy import NDArrayF0FinNonPos
from utilities.numpy import NDArrayF0FinNonPosNan
from utilities.numpy import NDArrayF0FinNonZr
from utilities.numpy import NDArrayF0FinNonZrNan
from utilities.numpy import NDArrayF0FinPos
from utilities.numpy import NDArrayF0FinPosNan
from utilities.numpy import NDArrayF0Int
from utilities.numpy import NDArrayF0IntNan
from utilities.numpy import NDArrayF0Neg
from utilities.numpy import NDArrayF0NegNan
from utilities.numpy import NDArrayF0NonNeg
from utilities.numpy import NDArrayF0NonNegNan
from utilities.numpy import NDArrayF0NonPos
from utilities.numpy import NDArrayF0NonPosNan
from utilities.numpy import NDArrayF0NonZr
from utilities.numpy import NDArrayF0NonZrNan
from utilities.numpy import NDArrayF0Pos
from utilities.numpy import NDArrayF0PosNan
from utilities.numpy import NDArrayF0Zr
from utilities.numpy import NDArrayF0ZrFinNonMic
from utilities.numpy import NDArrayF0ZrFinNonMicNan
from utilities.numpy import NDArrayF0ZrNan
from utilities.numpy import NDArrayF0ZrNonMic
from utilities.numpy import NDArrayF0ZrNonMicNan
from utilities.numpy import NDArrayF1
from utilities.numpy import NDArrayF1Fin
from utilities.numpy import NDArrayF1FinInt
from utilities.numpy import NDArrayF1FinIntNan
from utilities.numpy import NDArrayF1FinNan
from utilities.numpy import NDArrayF1FinNeg
from utilities.numpy import NDArrayF1FinNegNan
from utilities.numpy import NDArrayF1FinNonNeg
from utilities.numpy import NDArrayF1FinNonNegNan
from utilities.numpy import NDArrayF1FinNonPos
from utilities.numpy import NDArrayF1FinNonPosNan
from utilities.numpy import NDArrayF1FinNonZr
from utilities.numpy import NDArrayF1FinNonZrNan
from utilities.numpy import NDArrayF1FinPos
from utilities.numpy import NDArrayF1FinPosNan
from utilities.numpy import NDArrayF1Int
from utilities.numpy import NDArrayF1IntNan
from utilities.numpy import NDArrayF1Neg
from utilities.numpy import NDArrayF1NegNan
from utilities.numpy import NDArrayF1NonNeg
from utilities.numpy import NDArrayF1NonNegNan
from utilities.numpy import NDArrayF1NonPos
from utilities.numpy import NDArrayF1NonPosNan
from utilities.numpy import NDArrayF1NonZr
from utilities.numpy import NDArrayF1NonZrNan
from utilities.numpy import NDArrayF1Pos
from utilities.numpy import NDArrayF1PosNan
from utilities.numpy import NDArrayF1Zr
from utilities.numpy import NDArrayF1ZrFinNonMic
from utilities.numpy import NDArrayF1ZrFinNonMicNan
from utilities.numpy import NDArrayF1ZrNan
from utilities.numpy import NDArrayF1ZrNonMic
from utilities.numpy import NDArrayF1ZrNonMicNan
from utilities.numpy import NDArrayF2
from utilities.numpy import NDArrayF2Fin
from utilities.numpy import NDArrayF2FinInt
from utilities.numpy import NDArrayF2FinIntNan
from utilities.numpy import NDArrayF2FinNan
from utilities.numpy import NDArrayF2FinNeg
from utilities.numpy import NDArrayF2FinNegNan
from utilities.numpy import NDArrayF2FinNonNeg
from utilities.numpy import NDArrayF2FinNonNegNan
from utilities.numpy import NDArrayF2FinNonPos
from utilities.numpy import NDArrayF2FinNonPosNan
from utilities.numpy import NDArrayF2FinNonZr
from utilities.numpy import NDArrayF2FinNonZrNan
from utilities.numpy import NDArrayF2FinPos
from utilities.numpy import NDArrayF2FinPosNan
from utilities.numpy import NDArrayF2Int
from utilities.numpy import NDArrayF2IntNan
from utilities.numpy import NDArrayF2Neg
from utilities.numpy import NDArrayF2NegNan
from utilities.numpy import NDArrayF2NonNeg
from utilities.numpy import NDArrayF2NonNegNan
from utilities.numpy import NDArrayF2NonPos
from utilities.numpy import NDArrayF2NonPosNan
from utilities.numpy import NDArrayF2NonZr
from utilities.numpy import NDArrayF2NonZrNan
from utilities.numpy import NDArrayF2Pos
from utilities.numpy import NDArrayF2PosNan
from utilities.numpy import NDArrayF2Zr
from utilities.numpy import NDArrayF2ZrFinNonMic
from utilities.numpy import NDArrayF2ZrFinNonMicNan
from utilities.numpy import NDArrayF2ZrNan
from utilities.numpy import NDArrayF2ZrNonMic
from utilities.numpy import NDArrayF2ZrNonMicNan
from utilities.numpy import NDArrayF3
from utilities.numpy import NDArrayF3Fin
from utilities.numpy import NDArrayF3FinInt
from utilities.numpy import NDArrayF3FinIntNan
from utilities.numpy import NDArrayF3FinNan
from utilities.numpy import NDArrayF3FinNeg
from utilities.numpy import NDArrayF3FinNegNan
from utilities.numpy import NDArrayF3FinNonNeg
from utilities.numpy import NDArrayF3FinNonNegNan
from utilities.numpy import NDArrayF3FinNonPos
from utilities.numpy import NDArrayF3FinNonPosNan
from utilities.numpy import NDArrayF3FinNonZr
from utilities.numpy import NDArrayF3FinNonZrNan
from utilities.numpy import NDArrayF3FinPos
from utilities.numpy import NDArrayF3FinPosNan
from utilities.numpy import NDArrayF3Int
from utilities.numpy import NDArrayF3IntNan
from utilities.numpy import NDArrayF3Neg
from utilities.numpy import NDArrayF3NegNan
from utilities.numpy import NDArrayF3NonNeg
from utilities.numpy import NDArrayF3NonNegNan
from utilities.numpy import NDArrayF3NonPos
from utilities.numpy import NDArrayF3NonPosNan
from utilities.numpy import NDArrayF3NonZr
from utilities.numpy import NDArrayF3NonZrNan
from utilities.numpy import NDArrayF3Pos
from utilities.numpy import NDArrayF3PosNan
from utilities.numpy import NDArrayF3Zr
from utilities.numpy import NDArrayF3ZrFinNonMic
from utilities.numpy import NDArrayF3ZrFinNonMicNan
from utilities.numpy import NDArrayF3ZrNan
from utilities.numpy import NDArrayF3ZrNonMic
from utilities.numpy import NDArrayF3ZrNonMicNan
from utilities.numpy import NDArrayFFin
from utilities.numpy import NDArrayFFinInt
from utilities.numpy import NDArrayFFinIntNan
from utilities.numpy import NDArrayFFinNan
from utilities.numpy import NDArrayFFinNeg
from utilities.numpy import NDArrayFFinNegNan
from utilities.numpy import NDArrayFFinNonNeg
from utilities.numpy import NDArrayFFinNonNegNan
from utilities.numpy import NDArrayFFinNonPos
from utilities.numpy import NDArrayFFinNonPosNan
from utilities.numpy import NDArrayFFinNonZr
from utilities.numpy import NDArrayFFinNonZrNan
from utilities.numpy import NDArrayFFinPos
from utilities.numpy import NDArrayFFinPosNan
from utilities.numpy import NDArrayFInt
from utilities.numpy import NDArrayFIntNan
from utilities.numpy import NDArrayFNeg
from utilities.numpy import NDArrayFNegNan
from utilities.numpy import NDArrayFNonNeg
from utilities.numpy import NDArrayFNonNegNan
from utilities.numpy import NDArrayFNonPos
from utilities.numpy import NDArrayFNonPosNan
from utilities.numpy import NDArrayFNonZr
from utilities.numpy import NDArrayFNonZrNan
from utilities.numpy import NDArrayFPos
from utilities.numpy import NDArrayFPosNan
from utilities.numpy import NDArrayFZr
from utilities.numpy import NDArrayFZrFinNonMic
from utilities.numpy import NDArrayFZrFinNonMicNan
from utilities.numpy import NDArrayFZrNan
from utilities.numpy import NDArrayFZrNonMic
from utilities.numpy import NDArrayFZrNonMicNan
from utilities.numpy import NDArrayI
from utilities.numpy import NDArrayI0
from utilities.numpy import NDArrayI0Neg
from utilities.numpy import NDArrayI0NonNeg
from utilities.numpy import NDArrayI0NonPos
from utilities.numpy import NDArrayI0NonZr
from utilities.numpy import NDArrayI0Pos
from utilities.numpy import NDArrayI0Zr
from utilities.numpy import NDArrayI1
from utilities.numpy import NDArrayI1Neg
from utilities.numpy import NDArrayI1NonNeg
from utilities.numpy import NDArrayI1NonPos
from utilities.numpy import NDArrayI1NonZr
from utilities.numpy import NDArrayI1Pos
from utilities.numpy import NDArrayI1Zr
from utilities.numpy import NDArrayI2
from utilities.numpy import NDArrayI2Neg
from utilities.numpy import NDArrayI2NonNeg
from utilities.numpy import NDArrayI2NonPos
from utilities.numpy import NDArrayI2NonZr
from utilities.numpy import NDArrayI2Pos
from utilities.numpy import NDArrayI2Zr
from utilities.numpy import NDArrayI3
from utilities.numpy import NDArrayI3Neg
from utilities.numpy import NDArrayI3NonNeg
from utilities.numpy import NDArrayI3NonPos
from utilities.numpy import NDArrayI3NonZr
from utilities.numpy import NDArrayI3Pos
from utilities.numpy import NDArrayI3Zr
from utilities.numpy import NDArrayINeg
from utilities.numpy import NDArrayINonNeg
from utilities.numpy import NDArrayINonPos
from utilities.numpy import NDArrayINonZr
from utilities.numpy import NDArrayIPos
from utilities.numpy import NDArrayIZr
from utilities.numpy import NDArrayO
from utilities.numpy import NDArrayO0
from utilities.numpy import NDArrayO1
from utilities.numpy import NDArrayO2
from utilities.numpy import NDArrayO3
from utilities.numpy import NDim0
from utilities.numpy import NDim1
from utilities.numpy import NDim2
from utilities.numpy import NDim3
from utilities.numpy import datetime64as
from utilities.numpy import datetime64D
from utilities.numpy import datetime64fs
from utilities.numpy import datetime64h
from utilities.numpy import datetime64M
from utilities.numpy import datetime64m
from utilities.numpy import datetime64ms
from utilities.numpy import datetime64ns
from utilities.numpy import datetime64ps
from utilities.numpy import datetime64s
from utilities.numpy import datetime64us
from utilities.numpy import datetime64W
from utilities.numpy import datetime64Y


class TestNDims:
    @mark.parametrize(
        ("ndim", "hint"),
        [param(0, NDim0), param(1, NDim1), param(2, NDim2), param(3, NDim3)],
    )
    def test_main(self, ndim: int, hint: Any) -> None:
        arr = empty(zeros(ndim, dtype=int), dtype=float)
        die_if_unbearable(arr, Annotated[NDArray[Any], hint])


class TestHints:
    @mark.parametrize(
        ("dtype", "hint"),
        [
            param(bool, NDArrayB),
            param(datetime64Y, NDArrayDY),
            param(datetime64M, NDArrayDM),
            param(datetime64W, NDArrayDW),
            param(datetime64D, NDArrayDD),
            param(datetime64h, NDArrayDh),
            param(datetime64m, NDArrayDm),
            param(datetime64s, NDArrayDs),
            param(datetime64ms, NDArrayDms),
            param(datetime64us, NDArrayDus),
            param(datetime64ns, NDArrayDns),
            param(datetime64ps, NDArrayDps),
            param(datetime64fs, NDArrayDfs),
            param(datetime64as, NDArrayDas),
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
            param(datetime64D, 0, NDArrayD0),
            param(datetime64Y, 0, NDArrayDY0),
            param(datetime64M, 0, NDArrayDM0),
            param(datetime64W, 0, NDArrayDW0),
            param(datetime64D, 0, NDArrayDD0),
            param(datetime64h, 0, NDArrayDh0),
            param(datetime64m, 0, NDArrayDm0),
            param(datetime64s, 0, NDArrayDs0),
            param(datetime64ms, 0, NDArrayDms0),
            param(datetime64us, 0, NDArrayDus0),
            param(datetime64ns, 0, NDArrayDns0),
            param(datetime64ps, 0, NDArrayDps0),
            param(datetime64fs, 0, NDArrayDfs0),
            param(datetime64as, 0, NDArrayDas0),
            param(float, 0, NDArrayF0),
            param(int, 0, NDArrayI0),
            param(object, 0, NDArrayO0),
            # ndim 1
            param(bool, 1, NDArrayB1),
            param(datetime64D, 1, NDArrayD1),
            param(datetime64Y, 1, NDArrayDY1),
            param(datetime64M, 1, NDArrayDM1),
            param(datetime64W, 1, NDArrayDW1),
            param(datetime64D, 1, NDArrayDD1),
            param(datetime64h, 1, NDArrayDh1),
            param(datetime64m, 1, NDArrayDm1),
            param(datetime64s, 1, NDArrayDs1),
            param(datetime64ms, 1, NDArrayDms1),
            param(datetime64us, 1, NDArrayDus1),
            param(datetime64ns, 1, NDArrayDns1),
            param(datetime64ps, 1, NDArrayDps1),
            param(datetime64fs, 1, NDArrayDfs1),
            param(datetime64as, 1, NDArrayDas1),
            param(float, 1, NDArrayF1),
            param(int, 1, NDArrayI1),
            param(object, 1, NDArrayO1),
            # ndim 2
            param(bool, 2, NDArrayB2),
            param(datetime64D, 2, NDArrayD2),
            param(datetime64Y, 2, NDArrayDY2),
            param(datetime64M, 2, NDArrayDM2),
            param(datetime64W, 2, NDArrayDW2),
            param(datetime64D, 2, NDArrayDD2),
            param(datetime64h, 2, NDArrayDh2),
            param(datetime64m, 2, NDArrayDm2),
            param(datetime64s, 2, NDArrayDs2),
            param(datetime64ms, 2, NDArrayDms2),
            param(datetime64us, 2, NDArrayDus2),
            param(datetime64ns, 2, NDArrayDns2),
            param(datetime64ps, 2, NDArrayDps2),
            param(datetime64fs, 2, NDArrayDfs2),
            param(datetime64as, 2, NDArrayDas2),
            param(float, 2, NDArrayF2),
            param(int, 2, NDArrayI2),
            param(object, 2, NDArrayO2),
            # ndim 3
            param(bool, 3, NDArrayB3),
            param(datetime64D, 3, NDArrayD3),
            param(datetime64Y, 3, NDArrayDY3),
            param(datetime64M, 3, NDArrayDM3),
            param(datetime64W, 3, NDArrayDW3),
            param(datetime64D, 3, NDArrayDD3),
            param(datetime64h, 3, NDArrayDh3),
            param(datetime64m, 3, NDArrayDm3),
            param(datetime64s, 3, NDArrayDs3),
            param(datetime64ms, 3, NDArrayDms3),
            param(datetime64us, 3, NDArrayDus3),
            param(datetime64ns, 3, NDArrayDns3),
            param(datetime64ps, 3, NDArrayDps3),
            param(datetime64fs, 3, NDArrayDfs3),
            param(datetime64as, 3, NDArrayDas3),
            param(float, 3, NDArrayF3),
            param(int, 3, NDArrayI3),
            param(object, 3, NDArrayO3),
        ],
    )
    def test_compound(self, dtype: Any, ndim: int, hint: Any) -> None:
        arr = empty(zeros(ndim, dtype=int), dtype=dtype)
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
    def test_int_checks(self, arr: NDArrayI, dtype: Any, hint: Any) -> None:
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
    def test_float_checks(self, arr: NDArrayF, hint: Any) -> None:
        with suppress(BeartypeDoorHintViolation):
            die_if_unbearable(arr, hint)
