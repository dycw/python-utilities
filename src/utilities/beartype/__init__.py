from __future__ import annotations

from utilities.beartype.beartype import Dummy

__all__ = ["Dummy"]


try:
    from utilities.beartype.numpy import IsFinite
    from utilities.beartype.numpy import IsFiniteAndIntegral
    from utilities.beartype.numpy import IsFiniteAndIntegralOrNan
    from utilities.beartype.numpy import IsFiniteAndNegative
    from utilities.beartype.numpy import IsFiniteAndNegativeOrNan
    from utilities.beartype.numpy import IsFiniteAndNonNegative
    from utilities.beartype.numpy import IsFiniteAndNonNegativeOrNan
    from utilities.beartype.numpy import IsFiniteAndNonPositive
    from utilities.beartype.numpy import IsFiniteAndNonPositiveOrNan
    from utilities.beartype.numpy import IsFiniteAndNonZero
    from utilities.beartype.numpy import IsFiniteAndNonZeroOrNan
    from utilities.beartype.numpy import IsFiniteAndPositive
    from utilities.beartype.numpy import IsFiniteAndPositiveOrNan
    from utilities.beartype.numpy import IsFiniteOrNan
    from utilities.beartype.numpy import IsIntegral
    from utilities.beartype.numpy import IsIntegralOrNan
    from utilities.beartype.numpy import IsNegative
    from utilities.beartype.numpy import IsNegativeOrNan
    from utilities.beartype.numpy import IsNonNegative
    from utilities.beartype.numpy import IsNonNegativeOrNan
    from utilities.beartype.numpy import IsNonPositive
    from utilities.beartype.numpy import IsNonPositiveOrNan
    from utilities.beartype.numpy import IsNonZero
    from utilities.beartype.numpy import IsNonZeroOrNan
    from utilities.beartype.numpy import IsPositive
    from utilities.beartype.numpy import IsPositiveOrNan
    from utilities.beartype.numpy import IsZero
    from utilities.beartype.numpy import IsZeroOrFiniteAndNonMicro
    from utilities.beartype.numpy import IsZeroOrFiniteAndNonMicroOrNan
    from utilities.beartype.numpy import IsZeroOrNan
    from utilities.beartype.numpy import IsZeroOrNonMicro
    from utilities.beartype.numpy import IsZeroOrNonMicroOrNan
    from utilities.beartype.numpy import NDArray0
    from utilities.beartype.numpy import NDArray1
    from utilities.beartype.numpy import NDArray2
    from utilities.beartype.numpy import NDArray3
    from utilities.beartype.numpy import NDArrayB0
    from utilities.beartype.numpy import NDArrayB1
    from utilities.beartype.numpy import NDArrayB2
    from utilities.beartype.numpy import NDArrayB3
    from utilities.beartype.numpy import NDArrayD1
    from utilities.beartype.numpy import NDArrayD2
    from utilities.beartype.numpy import NDArrayD3
    from utilities.beartype.numpy import NDArrayDas0
    from utilities.beartype.numpy import NDArrayDas1
    from utilities.beartype.numpy import NDArrayDas2
    from utilities.beartype.numpy import NDArrayDas3
    from utilities.beartype.numpy import NDArrayDD0
    from utilities.beartype.numpy import NDArrayDD1
    from utilities.beartype.numpy import NDArrayDD2
    from utilities.beartype.numpy import NDArrayDD3
    from utilities.beartype.numpy import NDArrayDfs0
    from utilities.beartype.numpy import NDArrayDfs1
    from utilities.beartype.numpy import NDArrayDfs2
    from utilities.beartype.numpy import NDArrayDfs3
    from utilities.beartype.numpy import NDArrayDh0
    from utilities.beartype.numpy import NDArrayDh1
    from utilities.beartype.numpy import NDArrayDh2
    from utilities.beartype.numpy import NDArrayDh3
    from utilities.beartype.numpy import NDArrayDM0
    from utilities.beartype.numpy import NDArrayDm0
    from utilities.beartype.numpy import NDArrayDM1
    from utilities.beartype.numpy import NDArrayDm1
    from utilities.beartype.numpy import NDArrayDM2
    from utilities.beartype.numpy import NDArrayDm2
    from utilities.beartype.numpy import NDArrayDM3
    from utilities.beartype.numpy import NDArrayDm3
    from utilities.beartype.numpy import NDArrayDms0
    from utilities.beartype.numpy import NDArrayDms1
    from utilities.beartype.numpy import NDArrayDms2
    from utilities.beartype.numpy import NDArrayDms3
    from utilities.beartype.numpy import NDArrayDns0
    from utilities.beartype.numpy import NDArrayDns1
    from utilities.beartype.numpy import NDArrayDns2
    from utilities.beartype.numpy import NDArrayDns3
    from utilities.beartype.numpy import NDArrayDps0
    from utilities.beartype.numpy import NDArrayDps1
    from utilities.beartype.numpy import NDArrayDps2
    from utilities.beartype.numpy import NDArrayDps3
    from utilities.beartype.numpy import NDArrayDs0
    from utilities.beartype.numpy import NDArrayDs1
    from utilities.beartype.numpy import NDArrayDs2
    from utilities.beartype.numpy import NDArrayDs3
    from utilities.beartype.numpy import NDArrayDus0
    from utilities.beartype.numpy import NDArrayDus1
    from utilities.beartype.numpy import NDArrayDus2
    from utilities.beartype.numpy import NDArrayDus3
    from utilities.beartype.numpy import NDArrayDW0
    from utilities.beartype.numpy import NDArrayDW1
    from utilities.beartype.numpy import NDArrayDW2
    from utilities.beartype.numpy import NDArrayDW3
    from utilities.beartype.numpy import NDArrayDY0
    from utilities.beartype.numpy import NDArrayDY1
    from utilities.beartype.numpy import NDArrayDY2
    from utilities.beartype.numpy import NDArrayDY3
    from utilities.beartype.numpy import NDArrayF0
    from utilities.beartype.numpy import NDArrayF0Fin
    from utilities.beartype.numpy import NDArrayF0FinInt
    from utilities.beartype.numpy import NDArrayF0FinIntNan
    from utilities.beartype.numpy import NDArrayF0FinNan
    from utilities.beartype.numpy import NDArrayF0FinNeg
    from utilities.beartype.numpy import NDArrayF0FinNegNan
    from utilities.beartype.numpy import NDArrayF0FinNonNeg
    from utilities.beartype.numpy import NDArrayF0FinNonNegNan
    from utilities.beartype.numpy import NDArrayF0FinNonPos
    from utilities.beartype.numpy import NDArrayF0FinNonPosNan
    from utilities.beartype.numpy import NDArrayF0FinNonZr
    from utilities.beartype.numpy import NDArrayF0FinNonZrNan
    from utilities.beartype.numpy import NDArrayF0FinPos
    from utilities.beartype.numpy import NDArrayF0FinPosNan
    from utilities.beartype.numpy import NDArrayF0Int
    from utilities.beartype.numpy import NDArrayF0IntNan
    from utilities.beartype.numpy import NDArrayF0Neg
    from utilities.beartype.numpy import NDArrayF0NegNan
    from utilities.beartype.numpy import NDArrayF0NonNeg
    from utilities.beartype.numpy import NDArrayF0NonNegNan
    from utilities.beartype.numpy import NDArrayF0NonPos
    from utilities.beartype.numpy import NDArrayF0NonPosNan
    from utilities.beartype.numpy import NDArrayF0NonZr
    from utilities.beartype.numpy import NDArrayF0NonZrNan
    from utilities.beartype.numpy import NDArrayF0Pos
    from utilities.beartype.numpy import NDArrayF0PosNan
    from utilities.beartype.numpy import NDArrayF0Zr
    from utilities.beartype.numpy import NDArrayF0ZrFinNonMic
    from utilities.beartype.numpy import NDArrayF0ZrFinNonMicNan
    from utilities.beartype.numpy import NDArrayF0ZrNan
    from utilities.beartype.numpy import NDArrayF0ZrNonMic
    from utilities.beartype.numpy import NDArrayF0ZrNonMicNan
    from utilities.beartype.numpy import NDArrayF1
    from utilities.beartype.numpy import NDArrayF1Fin
    from utilities.beartype.numpy import NDArrayF1FinInt
    from utilities.beartype.numpy import NDArrayF1FinIntNan
    from utilities.beartype.numpy import NDArrayF1FinNan
    from utilities.beartype.numpy import NDArrayF1FinNeg
    from utilities.beartype.numpy import NDArrayF1FinNegNan
    from utilities.beartype.numpy import NDArrayF1FinNonNeg
    from utilities.beartype.numpy import NDArrayF1FinNonNegNan
    from utilities.beartype.numpy import NDArrayF1FinNonPos
    from utilities.beartype.numpy import NDArrayF1FinNonPosNan
    from utilities.beartype.numpy import NDArrayF1FinNonZr
    from utilities.beartype.numpy import NDArrayF1FinNonZrNan
    from utilities.beartype.numpy import NDArrayF1FinPos
    from utilities.beartype.numpy import NDArrayF1FinPosNan
    from utilities.beartype.numpy import NDArrayF1Int
    from utilities.beartype.numpy import NDArrayF1IntNan
    from utilities.beartype.numpy import NDArrayF1Neg
    from utilities.beartype.numpy import NDArrayF1NegNan
    from utilities.beartype.numpy import NDArrayF1NonNeg
    from utilities.beartype.numpy import NDArrayF1NonNegNan
    from utilities.beartype.numpy import NDArrayF1NonPos
    from utilities.beartype.numpy import NDArrayF1NonPosNan
    from utilities.beartype.numpy import NDArrayF1NonZr
    from utilities.beartype.numpy import NDArrayF1NonZrNan
    from utilities.beartype.numpy import NDArrayF1Pos
    from utilities.beartype.numpy import NDArrayF1PosNan
    from utilities.beartype.numpy import NDArrayF1Zr
    from utilities.beartype.numpy import NDArrayF1ZrFinNonMic
    from utilities.beartype.numpy import NDArrayF1ZrFinNonMicNan
    from utilities.beartype.numpy import NDArrayF1ZrNan
    from utilities.beartype.numpy import NDArrayF1ZrNonMic
    from utilities.beartype.numpy import NDArrayF1ZrNonMicNan
    from utilities.beartype.numpy import NDArrayF2
    from utilities.beartype.numpy import NDArrayF2Fin
    from utilities.beartype.numpy import NDArrayF2FinInt
    from utilities.beartype.numpy import NDArrayF2FinIntNan
    from utilities.beartype.numpy import NDArrayF2FinNan
    from utilities.beartype.numpy import NDArrayF2FinNeg
    from utilities.beartype.numpy import NDArrayF2FinNegNan
    from utilities.beartype.numpy import NDArrayF2FinNonNeg
    from utilities.beartype.numpy import NDArrayF2FinNonNegNan
    from utilities.beartype.numpy import NDArrayF2FinNonPos
    from utilities.beartype.numpy import NDArrayF2FinNonPosNan
    from utilities.beartype.numpy import NDArrayF2FinNonZr
    from utilities.beartype.numpy import NDArrayF2FinNonZrNan
    from utilities.beartype.numpy import NDArrayF2FinPos
    from utilities.beartype.numpy import NDArrayF2FinPosNan
    from utilities.beartype.numpy import NDArrayF2Int
    from utilities.beartype.numpy import NDArrayF2IntNan
    from utilities.beartype.numpy import NDArrayF2Neg
    from utilities.beartype.numpy import NDArrayF2NegNan
    from utilities.beartype.numpy import NDArrayF2NonNeg
    from utilities.beartype.numpy import NDArrayF2NonNegNan
    from utilities.beartype.numpy import NDArrayF2NonPos
    from utilities.beartype.numpy import NDArrayF2NonPosNan
    from utilities.beartype.numpy import NDArrayF2NonZr
    from utilities.beartype.numpy import NDArrayF2NonZrNan
    from utilities.beartype.numpy import NDArrayF2Pos
    from utilities.beartype.numpy import NDArrayF2PosNan
    from utilities.beartype.numpy import NDArrayF2Zr
    from utilities.beartype.numpy import NDArrayF2ZrFinNonMic
    from utilities.beartype.numpy import NDArrayF2ZrFinNonMicNan
    from utilities.beartype.numpy import NDArrayF2ZrNan
    from utilities.beartype.numpy import NDArrayF2ZrNonMic
    from utilities.beartype.numpy import NDArrayF2ZrNonMicNan
    from utilities.beartype.numpy import NDArrayF3
    from utilities.beartype.numpy import NDArrayF3FinInt
    from utilities.beartype.numpy import NDArrayF3FinIntNan
    from utilities.beartype.numpy import NDArrayF3FinNan
    from utilities.beartype.numpy import NDArrayF3FinNeg
    from utilities.beartype.numpy import NDArrayF3FinNegNan
    from utilities.beartype.numpy import NDArrayF3FinNonNeg
    from utilities.beartype.numpy import NDArrayF3FinNonNegNan
    from utilities.beartype.numpy import NDArrayF3FinNonPos
    from utilities.beartype.numpy import NDArrayF3FinNonPosNan
    from utilities.beartype.numpy import NDArrayF3FinNonZr
    from utilities.beartype.numpy import NDArrayF3FinNonZrNan
    from utilities.beartype.numpy import NDArrayF3FinPos
    from utilities.beartype.numpy import NDArrayF3FinPosNan
    from utilities.beartype.numpy import NDArrayF3Int
    from utilities.beartype.numpy import NDArrayF3IntNan
    from utilities.beartype.numpy import NDArrayF3Neg
    from utilities.beartype.numpy import NDArrayF3NegNan
    from utilities.beartype.numpy import NDArrayF3NonNeg
    from utilities.beartype.numpy import NDArrayF3NonNegNan
    from utilities.beartype.numpy import NDArrayF3NonPos
    from utilities.beartype.numpy import NDArrayF3NonPosNan
    from utilities.beartype.numpy import NDArrayF3NonZr
    from utilities.beartype.numpy import NDArrayF3NonZrNan
    from utilities.beartype.numpy import NDArrayF3Pos
    from utilities.beartype.numpy import NDArrayF3PosNan
    from utilities.beartype.numpy import NDArrayF3Zr
    from utilities.beartype.numpy import NDArrayF3ZrFinNonMic
    from utilities.beartype.numpy import NDArrayF3ZrFinNonMicNan
    from utilities.beartype.numpy import NDArrayF3ZrNan
    from utilities.beartype.numpy import NDArrayF3ZrNonMic
    from utilities.beartype.numpy import NDArrayF3ZrNonMicNan
    from utilities.beartype.numpy import NDArrayFFin
    from utilities.beartype.numpy import NDArrayFFinInt
    from utilities.beartype.numpy import NDArrayFFinIntNan
    from utilities.beartype.numpy import NDArrayFFinNan
    from utilities.beartype.numpy import NDArrayFFinNeg
    from utilities.beartype.numpy import NDArrayFFinNegNan
    from utilities.beartype.numpy import NDArrayFFinNonNeg
    from utilities.beartype.numpy import NDArrayFFinNonNegNan
    from utilities.beartype.numpy import NDArrayFFinNonPos
    from utilities.beartype.numpy import NDArrayFFinNonPosNan
    from utilities.beartype.numpy import NDArrayFFinNonZr
    from utilities.beartype.numpy import NDArrayFFinNonZrNan
    from utilities.beartype.numpy import NDArrayFFinPos
    from utilities.beartype.numpy import NDArrayFFinPosNan
    from utilities.beartype.numpy import NDArrayFInt
    from utilities.beartype.numpy import NDArrayFIntNan
    from utilities.beartype.numpy import NDArrayFNeg
    from utilities.beartype.numpy import NDArrayFNegNan
    from utilities.beartype.numpy import NDArrayFNonNeg
    from utilities.beartype.numpy import NDArrayFNonNegNan
    from utilities.beartype.numpy import NDArrayFNonPos
    from utilities.beartype.numpy import NDArrayFNonPosNan
    from utilities.beartype.numpy import NDArrayFNonZr
    from utilities.beartype.numpy import NDArrayFNonZrNan
    from utilities.beartype.numpy import NDArrayFPos
    from utilities.beartype.numpy import NDArrayFPosNan
    from utilities.beartype.numpy import NDArrayFZr
    from utilities.beartype.numpy import NDArrayFZrFinNonMic
    from utilities.beartype.numpy import NDArrayFZrFinNonMicNan
    from utilities.beartype.numpy import NDArrayFZrNan
    from utilities.beartype.numpy import NDArrayFZrNonMic
    from utilities.beartype.numpy import NDArrayFZrNonMicNan
    from utilities.beartype.numpy import NDArrayI0
    from utilities.beartype.numpy import NDArrayI0Neg
    from utilities.beartype.numpy import NDArrayI0NonNeg
    from utilities.beartype.numpy import NDArrayI0NonPos
    from utilities.beartype.numpy import NDArrayI0NonZr
    from utilities.beartype.numpy import NDArrayI0Pos
    from utilities.beartype.numpy import NDArrayI0Zr
    from utilities.beartype.numpy import NDArrayI1
    from utilities.beartype.numpy import NDArrayI1Neg
    from utilities.beartype.numpy import NDArrayI1NonNeg
    from utilities.beartype.numpy import NDArrayI1NonPos
    from utilities.beartype.numpy import NDArrayI1NonZr
    from utilities.beartype.numpy import NDArrayI1Pos
    from utilities.beartype.numpy import NDArrayI1Zr
    from utilities.beartype.numpy import NDArrayI2
    from utilities.beartype.numpy import NDArrayI2Neg
    from utilities.beartype.numpy import NDArrayI2NonNeg
    from utilities.beartype.numpy import NDArrayI2NonPos
    from utilities.beartype.numpy import NDArrayI2NonZr
    from utilities.beartype.numpy import NDArrayI2Pos
    from utilities.beartype.numpy import NDArrayI2Zr
    from utilities.beartype.numpy import NDArrayI3
    from utilities.beartype.numpy import NDArrayI3Neg
    from utilities.beartype.numpy import NDArrayI3NonNeg
    from utilities.beartype.numpy import NDArrayI3NonPos
    from utilities.beartype.numpy import NDArrayI3NonZr
    from utilities.beartype.numpy import NDArrayI3Pos
    from utilities.beartype.numpy import NDArrayI3Zr
    from utilities.beartype.numpy import NDArrayINeg
    from utilities.beartype.numpy import NDArrayINonNeg
    from utilities.beartype.numpy import NDArrayINonPos
    from utilities.beartype.numpy import NDArrayINonZr
    from utilities.beartype.numpy import NDArrayIPos
    from utilities.beartype.numpy import NDArrayIZr
    from utilities.beartype.numpy import NDArrayO0
    from utilities.beartype.numpy import NDArrayO1
    from utilities.beartype.numpy import NDArrayO2
    from utilities.beartype.numpy import NDArrayO3
    from utilities.beartype.numpy import NDim0
    from utilities.beartype.numpy import NDim1
    from utilities.beartype.numpy import NDim2
    from utilities.beartype.numpy import NDim3
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "IsFinite",
        "IsFiniteAndIntegral",
        "IsFiniteAndIntegralOrNan",
        "IsFiniteAndNegative",
        "IsFiniteAndNegativeOrNan",
        "IsFiniteAndNonNegative",
        "IsFiniteAndNonNegativeOrNan",
        "IsFiniteAndNonPositive",
        "IsFiniteAndNonPositiveOrNan",
        "IsFiniteAndNonZero",
        "IsFiniteAndNonZeroOrNan",
        "IsFiniteAndPositive",
        "IsFiniteAndPositiveOrNan",
        "IsFiniteOrNan",
        "IsIntegral",
        "IsIntegralOrNan",
        "IsNegative",
        "IsNegativeOrNan",
        "IsNonNegative",
        "IsNonNegativeOrNan",
        "IsNonPositive",
        "IsNonPositiveOrNan",
        "IsNonZero",
        "IsNonZeroOrNan",
        "IsPositive",
        "IsPositiveOrNan",
        "IsZero",
        "IsZeroOrFiniteAndNonMicro",
        "IsZeroOrFiniteAndNonMicroOrNan",
        "IsZeroOrNan",
        "IsZeroOrNonMicro",
        "IsZeroOrNonMicroOrNan",
        "NDArray0",
        "NDArray1",
        "NDArray2",
        "NDArray3",
        "NDArrayB0",
        "NDArrayB1",
        "NDArrayB2",
        "NDArrayB3",
        "NDArrayD1",
        "NDArrayD1",
        "NDArrayD1",
        "NDArrayD2",
        "NDArrayD3",
        "NDArrayDas0",
        "NDArrayDas1",
        "NDArrayDas2",
        "NDArrayDas3",
        "NDArrayDD0",
        "NDArrayDD1",
        "NDArrayDD2",
        "NDArrayDD3",
        "NDArrayDfs0",
        "NDArrayDfs1",
        "NDArrayDfs2",
        "NDArrayDfs3",
        "NDArrayDh0",
        "NDArrayDh1",
        "NDArrayDh2",
        "NDArrayDh3",
        "NDArrayDm0",
        "NDArrayDM0",
        "NDArrayDm1",
        "NDArrayDM1",
        "NDArrayDm2",
        "NDArrayDM2",
        "NDArrayDm3",
        "NDArrayDM3",
        "NDArrayDms0",
        "NDArrayDms1",
        "NDArrayDms2",
        "NDArrayDms3",
        "NDArrayDns0",
        "NDArrayDns1",
        "NDArrayDns2",
        "NDArrayDns3",
        "NDArrayDps0",
        "NDArrayDps1",
        "NDArrayDps2",
        "NDArrayDps3",
        "NDArrayDs0",
        "NDArrayDs1",
        "NDArrayDs2",
        "NDArrayDs3",
        "NDArrayDus0",
        "NDArrayDus1",
        "NDArrayDus2",
        "NDArrayDus3",
        "NDArrayDW0",
        "NDArrayDW1",
        "NDArrayDW2",
        "NDArrayDW3",
        "NDArrayDY0",
        "NDArrayDY1",
        "NDArrayDY2",
        "NDArrayDY3",
        "NDArrayF0",
        "NDArrayF0Fin",
        "NDArrayF0FinInt",
        "NDArrayF0FinIntNan",
        "NDArrayF0FinNan",
        "NDArrayF0FinNeg",
        "NDArrayF0FinNegNan",
        "NDArrayF0FinNonNeg",
        "NDArrayF0FinNonNegNan",
        "NDArrayF0FinNonPos",
        "NDArrayF0FinNonPosNan",
        "NDArrayF0FinNonZr",
        "NDArrayF0FinNonZrNan",
        "NDArrayF0FinPos",
        "NDArrayF0FinPosNan",
        "NDArrayF0Int",
        "NDArrayF0IntNan",
        "NDArrayF0Neg",
        "NDArrayF0NegNan",
        "NDArrayF0NonNeg",
        "NDArrayF0NonNegNan",
        "NDArrayF0NonPos",
        "NDArrayF0NonPosNan",
        "NDArrayF0NonZr",
        "NDArrayF0NonZrNan",
        "NDArrayF0Pos",
        "NDArrayF0PosNan",
        "NDArrayF0Zr",
        "NDArrayF0ZrFinNonMic",
        "NDArrayF0ZrFinNonMicNan",
        "NDArrayF0ZrNan",
        "NDArrayF0ZrNonMic",
        "NDArrayF0ZrNonMicNan",
        "NDArrayF1",
        "NDArrayF1Fin",
        "NDArrayF1FinInt",
        "NDArrayF1FinIntNan",
        "NDArrayF1FinNan",
        "NDArrayF1FinNeg",
        "NDArrayF1FinNegNan",
        "NDArrayF1FinNonNeg",
        "NDArrayF1FinNonNegNan",
        "NDArrayF1FinNonPos",
        "NDArrayF1FinNonPosNan",
        "NDArrayF1FinNonZr",
        "NDArrayF1FinNonZrNan",
        "NDArrayF1FinPos",
        "NDArrayF1FinPosNan",
        "NDArrayF1Int",
        "NDArrayF1IntNan",
        "NDArrayF1Neg",
        "NDArrayF1NegNan",
        "NDArrayF1NonNeg",
        "NDArrayF1NonNegNan",
        "NDArrayF1NonPos",
        "NDArrayF1NonPosNan",
        "NDArrayF1NonZr",
        "NDArrayF1NonZrNan",
        "NDArrayF1Pos",
        "NDArrayF1PosNan",
        "NDArrayF1Zr",
        "NDArrayF1ZrFinNonMic",
        "NDArrayF1ZrFinNonMicNan",
        "NDArrayF1ZrNan",
        "NDArrayF1ZrNonMic",
        "NDArrayF1ZrNonMicNan",
        "NDArrayF2",
        "NDArrayF2Fin",
        "NDArrayF2FinInt",
        "NDArrayF2FinIntNan",
        "NDArrayF2FinNan",
        "NDArrayF2FinNeg",
        "NDArrayF2FinNegNan",
        "NDArrayF2FinNonNeg",
        "NDArrayF2FinNonNegNan",
        "NDArrayF2FinNonPos",
        "NDArrayF2FinNonPosNan",
        "NDArrayF2FinNonZr",
        "NDArrayF2FinNonZrNan",
        "NDArrayF2FinPos",
        "NDArrayF2FinPosNan",
        "NDArrayF2Int",
        "NDArrayF2IntNan",
        "NDArrayF2Neg",
        "NDArrayF2NegNan",
        "NDArrayF2NonNeg",
        "NDArrayF2NonNegNan",
        "NDArrayF2NonPos",
        "NDArrayF2NonPosNan",
        "NDArrayF2NonZr",
        "NDArrayF2NonZrNan",
        "NDArrayF2Pos",
        "NDArrayF2PosNan",
        "NDArrayF2Zr",
        "NDArrayF2ZrFinNonMic",
        "NDArrayF2ZrFinNonMicNan",
        "NDArrayF2ZrNan",
        "NDArrayF2ZrNonMic",
        "NDArrayF2ZrNonMicNan",
        "NDArrayF3",
        "NDArrayF3FinInt",
        "NDArrayF3FinIntNan",
        "NDArrayF3FinNan",
        "NDArrayF3FinNeg",
        "NDArrayF3FinNegNan",
        "NDArrayF3FinNonNeg",
        "NDArrayF3FinNonNegNan",
        "NDArrayF3FinNonPos",
        "NDArrayF3FinNonPosNan",
        "NDArrayF3FinNonZr",
        "NDArrayF3FinNonZrNan",
        "NDArrayF3FinPos",
        "NDArrayF3FinPosNan",
        "NDArrayF3Int",
        "NDArrayF3IntNan",
        "NDArrayF3Neg",
        "NDArrayF3NegNan",
        "NDArrayF3NonNeg",
        "NDArrayF3NonNegNan",
        "NDArrayF3NonPos",
        "NDArrayF3NonPosNan",
        "NDArrayF3NonZr",
        "NDArrayF3NonZrNan",
        "NDArrayF3Pos",
        "NDArrayF3PosNan",
        "NDArrayF3Zr",
        "NDArrayF3ZrFinNonMic",
        "NDArrayF3ZrFinNonMicNan",
        "NDArrayF3ZrNan",
        "NDArrayF3ZrNonMic",
        "NDArrayF3ZrNonMicNan",
        "NDArrayFFin",
        "NDArrayFFinInt",
        "NDArrayFFinIntNan",
        "NDArrayFFinNan",
        "NDArrayFFinNeg",
        "NDArrayFFinNegNan",
        "NDArrayFFinNonNeg",
        "NDArrayFFinNonNegNan",
        "NDArrayFFinNonPos",
        "NDArrayFFinNonPosNan",
        "NDArrayFFinNonZr",
        "NDArrayFFinNonZrNan",
        "NDArrayFFinPos",
        "NDArrayFFinPosNan",
        "NDArrayFInt",
        "NDArrayFIntNan",
        "NDArrayFNeg",
        "NDArrayFNegNan",
        "NDArrayFNonNeg",
        "NDArrayFNonNegNan",
        "NDArrayFNonPos",
        "NDArrayFNonPosNan",
        "NDArrayFNonZr",
        "NDArrayFNonZrNan",
        "NDArrayFPos",
        "NDArrayFPosNan",
        "NDArrayFZr",
        "NDArrayFZrFinNonMic",
        "NDArrayFZrFinNonMicNan",
        "NDArrayFZrNan",
        "NDArrayFZrNonMic",
        "NDArrayFZrNonMicNan",
        "NDArrayI0",
        "NDArrayI0Neg",
        "NDArrayI0NonNeg",
        "NDArrayI0NonPos",
        "NDArrayI0NonZr",
        "NDArrayI0Pos",
        "NDArrayI0Zr",
        "NDArrayI1",
        "NDArrayI1Neg",
        "NDArrayI1NonNeg",
        "NDArrayI1NonPos",
        "NDArrayI1NonZr",
        "NDArrayI1Pos",
        "NDArrayI1Zr",
        "NDArrayI2",
        "NDArrayI2Neg",
        "NDArrayI2NonNeg",
        "NDArrayI2NonPos",
        "NDArrayI2NonZr",
        "NDArrayI2Pos",
        "NDArrayI2Zr",
        "NDArrayI3",
        "NDArrayI3Neg",
        "NDArrayI3NonNeg",
        "NDArrayI3NonPos",
        "NDArrayI3NonZr",
        "NDArrayI3Pos",
        "NDArrayI3Zr",
        "NDArrayINeg",
        "NDArrayINonNeg",
        "NDArrayINonPos",
        "NDArrayINonZr",
        "NDArrayIPos",
        "NDArrayIZr",
        "NDArrayO0",
        "NDArrayO1",
        "NDArrayO2",
        "NDArrayO3",
        "NDim0",
        "NDim1",
        "NDim2",
        "NDim3",
    ]
