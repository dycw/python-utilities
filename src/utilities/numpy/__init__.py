from __future__ import annotations

from utilities.numpy.numpy import DATE_MAX_AS_DATETIME64
from utilities.numpy.numpy import DATE_MAX_AS_INT
from utilities.numpy.numpy import DATE_MIN_AS_DATETIME64
from utilities.numpy.numpy import DATE_MIN_AS_INT
from utilities.numpy.numpy import DATETIME_MAX_AS_DATETIMETIME64
from utilities.numpy.numpy import DATETIME_MAX_AS_INT
from utilities.numpy.numpy import DATETIME_MIN_AS_DATETIMETIME64
from utilities.numpy.numpy import DATETIME_MIN_AS_INT
from utilities.numpy.numpy import DateOverflowError
from utilities.numpy.numpy import Datetime64Kind
from utilities.numpy.numpy import Datetime64Unit
from utilities.numpy.numpy import DTypeB
from utilities.numpy.numpy import DTypeDns
from utilities.numpy.numpy import DTypeF
from utilities.numpy.numpy import DTypeI
from utilities.numpy.numpy import DTypeO
from utilities.numpy.numpy import EmptyNumpyConcatenateError
from utilities.numpy.numpy import InfElementsError
from utilities.numpy.numpy import InvalidDTypeError
from utilities.numpy.numpy import IsFinite
from utilities.numpy.numpy import IsFiniteAndIntegral
from utilities.numpy.numpy import IsFiniteAndIntegralOrNan
from utilities.numpy.numpy import IsFiniteAndNegative
from utilities.numpy.numpy import IsFiniteAndNegativeOrNan
from utilities.numpy.numpy import IsFiniteAndNonNegative
from utilities.numpy.numpy import IsFiniteAndNonNegativeOrNan
from utilities.numpy.numpy import IsFiniteAndNonPositive
from utilities.numpy.numpy import IsFiniteAndNonPositiveOrNan
from utilities.numpy.numpy import IsFiniteAndNonZero
from utilities.numpy.numpy import IsFiniteAndNonZeroOrNan
from utilities.numpy.numpy import IsFiniteAndPositive
from utilities.numpy.numpy import IsFiniteAndPositiveOrNan
from utilities.numpy.numpy import IsFiniteOrNan
from utilities.numpy.numpy import IsIntegral
from utilities.numpy.numpy import IsIntegralOrNan
from utilities.numpy.numpy import IsNegative
from utilities.numpy.numpy import IsNegativeOrNan
from utilities.numpy.numpy import IsNonNegative
from utilities.numpy.numpy import IsNonNegativeOrNan
from utilities.numpy.numpy import IsNonPositive
from utilities.numpy.numpy import IsNonPositiveOrNan
from utilities.numpy.numpy import IsNonZero
from utilities.numpy.numpy import IsNonZeroOrNan
from utilities.numpy.numpy import IsPositive
from utilities.numpy.numpy import IsPositiveOrNan
from utilities.numpy.numpy import IsZero
from utilities.numpy.numpy import IsZeroOrFiniteAndNonMicro
from utilities.numpy.numpy import IsZeroOrFiniteAndNonMicroOrNan
from utilities.numpy.numpy import IsZeroOrNan
from utilities.numpy.numpy import IsZeroOrNonMicro
from utilities.numpy.numpy import IsZeroOrNonMicroOrNan
from utilities.numpy.numpy import LossOfNanosecondsError
from utilities.numpy.numpy import MultipleTrueElementsError
from utilities.numpy.numpy import NanElementsError
from utilities.numpy.numpy import NDArray0
from utilities.numpy.numpy import NDArray1
from utilities.numpy.numpy import NDArray2
from utilities.numpy.numpy import NDArray3
from utilities.numpy.numpy import NDArrayA
from utilities.numpy.numpy import NDArrayB
from utilities.numpy.numpy import NDArrayB0
from utilities.numpy.numpy import NDArrayB1
from utilities.numpy.numpy import NDArrayB2
from utilities.numpy.numpy import NDArrayB3
from utilities.numpy.numpy import NDArrayD
from utilities.numpy.numpy import NDArrayD0
from utilities.numpy.numpy import NDArrayD1
from utilities.numpy.numpy import NDArrayD2
from utilities.numpy.numpy import NDArrayD3
from utilities.numpy.numpy import NDArrayDas
from utilities.numpy.numpy import NDArrayDas0
from utilities.numpy.numpy import NDArrayDas1
from utilities.numpy.numpy import NDArrayDas2
from utilities.numpy.numpy import NDArrayDas3
from utilities.numpy.numpy import NDArrayDD
from utilities.numpy.numpy import NDArrayDD0
from utilities.numpy.numpy import NDArrayDD1
from utilities.numpy.numpy import NDArrayDD2
from utilities.numpy.numpy import NDArrayDD3
from utilities.numpy.numpy import NDArrayDfs
from utilities.numpy.numpy import NDArrayDfs0
from utilities.numpy.numpy import NDArrayDfs1
from utilities.numpy.numpy import NDArrayDfs2
from utilities.numpy.numpy import NDArrayDfs3
from utilities.numpy.numpy import NDArrayDh
from utilities.numpy.numpy import NDArrayDh0
from utilities.numpy.numpy import NDArrayDh1
from utilities.numpy.numpy import NDArrayDh2
from utilities.numpy.numpy import NDArrayDh3
from utilities.numpy.numpy import NDArrayDM
from utilities.numpy.numpy import NDArrayDm
from utilities.numpy.numpy import NDArrayDM0
from utilities.numpy.numpy import NDArrayDm0
from utilities.numpy.numpy import NDArrayDM1
from utilities.numpy.numpy import NDArrayDm1
from utilities.numpy.numpy import NDArrayDM2
from utilities.numpy.numpy import NDArrayDm2
from utilities.numpy.numpy import NDArrayDM3
from utilities.numpy.numpy import NDArrayDm3
from utilities.numpy.numpy import NDArrayDms
from utilities.numpy.numpy import NDArrayDms0
from utilities.numpy.numpy import NDArrayDms1
from utilities.numpy.numpy import NDArrayDms2
from utilities.numpy.numpy import NDArrayDms3
from utilities.numpy.numpy import NDArrayDns
from utilities.numpy.numpy import NDArrayDns0
from utilities.numpy.numpy import NDArrayDns1
from utilities.numpy.numpy import NDArrayDns2
from utilities.numpy.numpy import NDArrayDns3
from utilities.numpy.numpy import NDArrayDps
from utilities.numpy.numpy import NDArrayDps0
from utilities.numpy.numpy import NDArrayDps1
from utilities.numpy.numpy import NDArrayDps2
from utilities.numpy.numpy import NDArrayDps3
from utilities.numpy.numpy import NDArrayDs
from utilities.numpy.numpy import NDArrayDs0
from utilities.numpy.numpy import NDArrayDs1
from utilities.numpy.numpy import NDArrayDs2
from utilities.numpy.numpy import NDArrayDs3
from utilities.numpy.numpy import NDArrayDus
from utilities.numpy.numpy import NDArrayDus0
from utilities.numpy.numpy import NDArrayDus1
from utilities.numpy.numpy import NDArrayDus2
from utilities.numpy.numpy import NDArrayDus3
from utilities.numpy.numpy import NDArrayDW
from utilities.numpy.numpy import NDArrayDW0
from utilities.numpy.numpy import NDArrayDW1
from utilities.numpy.numpy import NDArrayDW2
from utilities.numpy.numpy import NDArrayDW3
from utilities.numpy.numpy import NDArrayDY
from utilities.numpy.numpy import NDArrayDY0
from utilities.numpy.numpy import NDArrayDY1
from utilities.numpy.numpy import NDArrayDY2
from utilities.numpy.numpy import NDArrayDY3
from utilities.numpy.numpy import NDArrayF
from utilities.numpy.numpy import NDArrayF0
from utilities.numpy.numpy import NDArrayF0Fin
from utilities.numpy.numpy import NDArrayF0FinInt
from utilities.numpy.numpy import NDArrayF0FinIntNan
from utilities.numpy.numpy import NDArrayF0FinNan
from utilities.numpy.numpy import NDArrayF0FinNeg
from utilities.numpy.numpy import NDArrayF0FinNegNan
from utilities.numpy.numpy import NDArrayF0FinNonNeg
from utilities.numpy.numpy import NDArrayF0FinNonNegNan
from utilities.numpy.numpy import NDArrayF0FinNonPos
from utilities.numpy.numpy import NDArrayF0FinNonPosNan
from utilities.numpy.numpy import NDArrayF0FinNonZr
from utilities.numpy.numpy import NDArrayF0FinNonZrNan
from utilities.numpy.numpy import NDArrayF0FinPos
from utilities.numpy.numpy import NDArrayF0FinPosNan
from utilities.numpy.numpy import NDArrayF0Int
from utilities.numpy.numpy import NDArrayF0IntNan
from utilities.numpy.numpy import NDArrayF0Neg
from utilities.numpy.numpy import NDArrayF0NegNan
from utilities.numpy.numpy import NDArrayF0NonNeg
from utilities.numpy.numpy import NDArrayF0NonNegNan
from utilities.numpy.numpy import NDArrayF0NonPos
from utilities.numpy.numpy import NDArrayF0NonPosNan
from utilities.numpy.numpy import NDArrayF0NonZr
from utilities.numpy.numpy import NDArrayF0NonZrNan
from utilities.numpy.numpy import NDArrayF0Pos
from utilities.numpy.numpy import NDArrayF0PosNan
from utilities.numpy.numpy import NDArrayF0Zr
from utilities.numpy.numpy import NDArrayF0ZrFinNonMic
from utilities.numpy.numpy import NDArrayF0ZrFinNonMicNan
from utilities.numpy.numpy import NDArrayF0ZrNan
from utilities.numpy.numpy import NDArrayF0ZrNonMic
from utilities.numpy.numpy import NDArrayF0ZrNonMicNan
from utilities.numpy.numpy import NDArrayF1
from utilities.numpy.numpy import NDArrayF1Fin
from utilities.numpy.numpy import NDArrayF1FinInt
from utilities.numpy.numpy import NDArrayF1FinIntNan
from utilities.numpy.numpy import NDArrayF1FinNan
from utilities.numpy.numpy import NDArrayF1FinNeg
from utilities.numpy.numpy import NDArrayF1FinNegNan
from utilities.numpy.numpy import NDArrayF1FinNonNeg
from utilities.numpy.numpy import NDArrayF1FinNonNegNan
from utilities.numpy.numpy import NDArrayF1FinNonPos
from utilities.numpy.numpy import NDArrayF1FinNonPosNan
from utilities.numpy.numpy import NDArrayF1FinNonZr
from utilities.numpy.numpy import NDArrayF1FinNonZrNan
from utilities.numpy.numpy import NDArrayF1FinPos
from utilities.numpy.numpy import NDArrayF1FinPosNan
from utilities.numpy.numpy import NDArrayF1Int
from utilities.numpy.numpy import NDArrayF1IntNan
from utilities.numpy.numpy import NDArrayF1Neg
from utilities.numpy.numpy import NDArrayF1NegNan
from utilities.numpy.numpy import NDArrayF1NonNeg
from utilities.numpy.numpy import NDArrayF1NonNegNan
from utilities.numpy.numpy import NDArrayF1NonPos
from utilities.numpy.numpy import NDArrayF1NonPosNan
from utilities.numpy.numpy import NDArrayF1NonZr
from utilities.numpy.numpy import NDArrayF1NonZrNan
from utilities.numpy.numpy import NDArrayF1Pos
from utilities.numpy.numpy import NDArrayF1PosNan
from utilities.numpy.numpy import NDArrayF1Zr
from utilities.numpy.numpy import NDArrayF1ZrFinNonMic
from utilities.numpy.numpy import NDArrayF1ZrFinNonMicNan
from utilities.numpy.numpy import NDArrayF1ZrNan
from utilities.numpy.numpy import NDArrayF1ZrNonMic
from utilities.numpy.numpy import NDArrayF1ZrNonMicNan
from utilities.numpy.numpy import NDArrayF2
from utilities.numpy.numpy import NDArrayF2Fin
from utilities.numpy.numpy import NDArrayF2FinInt
from utilities.numpy.numpy import NDArrayF2FinIntNan
from utilities.numpy.numpy import NDArrayF2FinNan
from utilities.numpy.numpy import NDArrayF2FinNeg
from utilities.numpy.numpy import NDArrayF2FinNegNan
from utilities.numpy.numpy import NDArrayF2FinNonNeg
from utilities.numpy.numpy import NDArrayF2FinNonNegNan
from utilities.numpy.numpy import NDArrayF2FinNonPos
from utilities.numpy.numpy import NDArrayF2FinNonPosNan
from utilities.numpy.numpy import NDArrayF2FinNonZr
from utilities.numpy.numpy import NDArrayF2FinNonZrNan
from utilities.numpy.numpy import NDArrayF2FinPos
from utilities.numpy.numpy import NDArrayF2FinPosNan
from utilities.numpy.numpy import NDArrayF2Int
from utilities.numpy.numpy import NDArrayF2IntNan
from utilities.numpy.numpy import NDArrayF2Neg
from utilities.numpy.numpy import NDArrayF2NegNan
from utilities.numpy.numpy import NDArrayF2NonNeg
from utilities.numpy.numpy import NDArrayF2NonNegNan
from utilities.numpy.numpy import NDArrayF2NonPos
from utilities.numpy.numpy import NDArrayF2NonPosNan
from utilities.numpy.numpy import NDArrayF2NonZr
from utilities.numpy.numpy import NDArrayF2NonZrNan
from utilities.numpy.numpy import NDArrayF2Pos
from utilities.numpy.numpy import NDArrayF2PosNan
from utilities.numpy.numpy import NDArrayF2Zr
from utilities.numpy.numpy import NDArrayF2ZrFinNonMic
from utilities.numpy.numpy import NDArrayF2ZrFinNonMicNan
from utilities.numpy.numpy import NDArrayF2ZrNan
from utilities.numpy.numpy import NDArrayF2ZrNonMic
from utilities.numpy.numpy import NDArrayF2ZrNonMicNan
from utilities.numpy.numpy import NDArrayF3
from utilities.numpy.numpy import NDArrayF3Fin
from utilities.numpy.numpy import NDArrayF3FinInt
from utilities.numpy.numpy import NDArrayF3FinIntNan
from utilities.numpy.numpy import NDArrayF3FinNan
from utilities.numpy.numpy import NDArrayF3FinNeg
from utilities.numpy.numpy import NDArrayF3FinNegNan
from utilities.numpy.numpy import NDArrayF3FinNonNeg
from utilities.numpy.numpy import NDArrayF3FinNonNegNan
from utilities.numpy.numpy import NDArrayF3FinNonPos
from utilities.numpy.numpy import NDArrayF3FinNonPosNan
from utilities.numpy.numpy import NDArrayF3FinNonZr
from utilities.numpy.numpy import NDArrayF3FinNonZrNan
from utilities.numpy.numpy import NDArrayF3FinPos
from utilities.numpy.numpy import NDArrayF3FinPosNan
from utilities.numpy.numpy import NDArrayF3Int
from utilities.numpy.numpy import NDArrayF3IntNan
from utilities.numpy.numpy import NDArrayF3Neg
from utilities.numpy.numpy import NDArrayF3NegNan
from utilities.numpy.numpy import NDArrayF3NonNeg
from utilities.numpy.numpy import NDArrayF3NonNegNan
from utilities.numpy.numpy import NDArrayF3NonPos
from utilities.numpy.numpy import NDArrayF3NonPosNan
from utilities.numpy.numpy import NDArrayF3NonZr
from utilities.numpy.numpy import NDArrayF3NonZrNan
from utilities.numpy.numpy import NDArrayF3Pos
from utilities.numpy.numpy import NDArrayF3PosNan
from utilities.numpy.numpy import NDArrayF3Zr
from utilities.numpy.numpy import NDArrayF3ZrFinNonMic
from utilities.numpy.numpy import NDArrayF3ZrFinNonMicNan
from utilities.numpy.numpy import NDArrayF3ZrNan
from utilities.numpy.numpy import NDArrayF3ZrNonMic
from utilities.numpy.numpy import NDArrayF3ZrNonMicNan
from utilities.numpy.numpy import NDArrayFFin
from utilities.numpy.numpy import NDArrayFFinInt
from utilities.numpy.numpy import NDArrayFFinIntNan
from utilities.numpy.numpy import NDArrayFFinNan
from utilities.numpy.numpy import NDArrayFFinNeg
from utilities.numpy.numpy import NDArrayFFinNegNan
from utilities.numpy.numpy import NDArrayFFinNonNeg
from utilities.numpy.numpy import NDArrayFFinNonNegNan
from utilities.numpy.numpy import NDArrayFFinNonPos
from utilities.numpy.numpy import NDArrayFFinNonPosNan
from utilities.numpy.numpy import NDArrayFFinNonZr
from utilities.numpy.numpy import NDArrayFFinNonZrNan
from utilities.numpy.numpy import NDArrayFFinPos
from utilities.numpy.numpy import NDArrayFFinPosNan
from utilities.numpy.numpy import NDArrayFInt
from utilities.numpy.numpy import NDArrayFIntNan
from utilities.numpy.numpy import NDArrayFNeg
from utilities.numpy.numpy import NDArrayFNegNan
from utilities.numpy.numpy import NDArrayFNonNeg
from utilities.numpy.numpy import NDArrayFNonNegNan
from utilities.numpy.numpy import NDArrayFNonPos
from utilities.numpy.numpy import NDArrayFNonPosNan
from utilities.numpy.numpy import NDArrayFNonZr
from utilities.numpy.numpy import NDArrayFNonZrNan
from utilities.numpy.numpy import NDArrayFPos
from utilities.numpy.numpy import NDArrayFPosNan
from utilities.numpy.numpy import NDArrayFZr
from utilities.numpy.numpy import NDArrayFZrFinNonMic
from utilities.numpy.numpy import NDArrayFZrFinNonMicNan
from utilities.numpy.numpy import NDArrayFZrNan
from utilities.numpy.numpy import NDArrayFZrNonMic
from utilities.numpy.numpy import NDArrayFZrNonMicNan
from utilities.numpy.numpy import NDArrayI
from utilities.numpy.numpy import NDArrayI0
from utilities.numpy.numpy import NDArrayI0Neg
from utilities.numpy.numpy import NDArrayI0NonNeg
from utilities.numpy.numpy import NDArrayI0NonPos
from utilities.numpy.numpy import NDArrayI0NonZr
from utilities.numpy.numpy import NDArrayI0Pos
from utilities.numpy.numpy import NDArrayI0Zr
from utilities.numpy.numpy import NDArrayI1
from utilities.numpy.numpy import NDArrayI1Neg
from utilities.numpy.numpy import NDArrayI1NonNeg
from utilities.numpy.numpy import NDArrayI1NonPos
from utilities.numpy.numpy import NDArrayI1NonZr
from utilities.numpy.numpy import NDArrayI1Pos
from utilities.numpy.numpy import NDArrayI1Zr
from utilities.numpy.numpy import NDArrayI2
from utilities.numpy.numpy import NDArrayI2Neg
from utilities.numpy.numpy import NDArrayI2NonNeg
from utilities.numpy.numpy import NDArrayI2NonPos
from utilities.numpy.numpy import NDArrayI2NonZr
from utilities.numpy.numpy import NDArrayI2Pos
from utilities.numpy.numpy import NDArrayI2Zr
from utilities.numpy.numpy import NDArrayI3
from utilities.numpy.numpy import NDArrayI3Neg
from utilities.numpy.numpy import NDArrayI3NonNeg
from utilities.numpy.numpy import NDArrayI3NonPos
from utilities.numpy.numpy import NDArrayI3NonZr
from utilities.numpy.numpy import NDArrayI3Pos
from utilities.numpy.numpy import NDArrayI3Zr
from utilities.numpy.numpy import NDArrayINeg
from utilities.numpy.numpy import NDArrayINonNeg
from utilities.numpy.numpy import NDArrayINonPos
from utilities.numpy.numpy import NDArrayINonZr
from utilities.numpy.numpy import NDArrayIPos
from utilities.numpy.numpy import NDArrayIZr
from utilities.numpy.numpy import NDArrayO
from utilities.numpy.numpy import NDArrayO0
from utilities.numpy.numpy import NDArrayO1
from utilities.numpy.numpy import NDArrayO2
from utilities.numpy.numpy import NDArrayO3
from utilities.numpy.numpy import NDim0
from utilities.numpy.numpy import NDim1
from utilities.numpy.numpy import NDim2
from utilities.numpy.numpy import NDim3
from utilities.numpy.numpy import NonIntegralElementsError
from utilities.numpy.numpy import NoTrueElementsError
from utilities.numpy.numpy import ZeroShiftError
from utilities.numpy.numpy import array_indexer
from utilities.numpy.numpy import as_int
from utilities.numpy.numpy import date_to_datetime64
from utilities.numpy.numpy import datetime64_dtype_to_unit
from utilities.numpy.numpy import datetime64_to_date
from utilities.numpy.numpy import datetime64_to_datetime
from utilities.numpy.numpy import datetime64_to_int
from utilities.numpy.numpy import datetime64_unit_to_dtype
from utilities.numpy.numpy import datetime64_unit_to_kind
from utilities.numpy.numpy import datetime64as
from utilities.numpy.numpy import datetime64D
from utilities.numpy.numpy import datetime64fs
from utilities.numpy.numpy import datetime64h
from utilities.numpy.numpy import datetime64M
from utilities.numpy.numpy import datetime64m
from utilities.numpy.numpy import datetime64ms
from utilities.numpy.numpy import datetime64ns
from utilities.numpy.numpy import datetime64ps
from utilities.numpy.numpy import datetime64s
from utilities.numpy.numpy import datetime64us
from utilities.numpy.numpy import datetime64W
from utilities.numpy.numpy import datetime64Y
from utilities.numpy.numpy import datetime_to_datetime64
from utilities.numpy.numpy import discretize
from utilities.numpy.numpy import ffill_non_nan_slices
from utilities.numpy.numpy import fillna
from utilities.numpy.numpy import flatn0
from utilities.numpy.numpy import get_fill_value
from utilities.numpy.numpy import has_dtype
from utilities.numpy.numpy import is_at_least
from utilities.numpy.numpy import is_at_least_or_nan
from utilities.numpy.numpy import is_at_most
from utilities.numpy.numpy import is_at_most_or_nan
from utilities.numpy.numpy import is_between
from utilities.numpy.numpy import is_between_or_nan
from utilities.numpy.numpy import is_empty
from utilities.numpy.numpy import is_finite_and_integral
from utilities.numpy.numpy import is_finite_and_integral_or_nan
from utilities.numpy.numpy import is_finite_and_negative
from utilities.numpy.numpy import is_finite_and_negative_or_nan
from utilities.numpy.numpy import is_finite_and_non_negative
from utilities.numpy.numpy import is_finite_and_non_negative_or_nan
from utilities.numpy.numpy import is_finite_and_non_positive
from utilities.numpy.numpy import is_finite_and_non_positive_or_nan
from utilities.numpy.numpy import is_finite_and_non_zero
from utilities.numpy.numpy import is_finite_and_non_zero_or_nan
from utilities.numpy.numpy import is_finite_and_positive
from utilities.numpy.numpy import is_finite_and_positive_or_nan
from utilities.numpy.numpy import is_finite_or_nan
from utilities.numpy.numpy import is_greater_than
from utilities.numpy.numpy import is_greater_than_or_nan
from utilities.numpy.numpy import is_integral
from utilities.numpy.numpy import is_integral_or_nan
from utilities.numpy.numpy import is_less_than
from utilities.numpy.numpy import is_less_than_or_nan
from utilities.numpy.numpy import is_negative
from utilities.numpy.numpy import is_negative_or_nan
from utilities.numpy.numpy import is_non_empty
from utilities.numpy.numpy import is_non_negative
from utilities.numpy.numpy import is_non_negative_or_nan
from utilities.numpy.numpy import is_non_positive
from utilities.numpy.numpy import is_non_positive_or_nan
from utilities.numpy.numpy import is_non_singular
from utilities.numpy.numpy import is_non_zero
from utilities.numpy.numpy import is_non_zero_or_nan
from utilities.numpy.numpy import is_positive
from utilities.numpy.numpy import is_positive_or_nan
from utilities.numpy.numpy import is_positive_semidefinite
from utilities.numpy.numpy import is_symmetric
from utilities.numpy.numpy import is_zero
from utilities.numpy.numpy import is_zero_or_finite_and_non_micro
from utilities.numpy.numpy import is_zero_or_finite_and_non_micro_or_nan
from utilities.numpy.numpy import is_zero_or_nan
from utilities.numpy.numpy import is_zero_or_non_micro
from utilities.numpy.numpy import is_zero_or_non_micro_or_nan
from utilities.numpy.numpy import maximum
from utilities.numpy.numpy import minimum
from utilities.numpy.numpy import redirect_to_empty_numpy_concatenate_error
from utilities.numpy.numpy import shift
from utilities.numpy.numpy import shift_bool
from utilities.numpy.numpy import year

__all__ = [
    "array_indexer",
    "as_int",
    "DATE_MAX_AS_DATETIME64",
    "DATE_MAX_AS_INT",
    "DATE_MIN_AS_DATETIME64",
    "DATE_MIN_AS_INT",
    "date_to_datetime64",
    "DateOverflowError",
    "DATETIME_MAX_AS_DATETIMETIME64",
    "DATETIME_MAX_AS_INT",
    "DATETIME_MIN_AS_DATETIMETIME64",
    "DATETIME_MIN_AS_INT",
    "datetime_to_datetime64",
    "datetime64_dtype_to_unit",
    "datetime64_to_date",
    "datetime64_to_datetime",
    "datetime64_to_int",
    "datetime64_unit_to_dtype",
    "datetime64_unit_to_kind",
    "datetime64as",
    "datetime64D",
    "datetime64fs",
    "datetime64h",
    "Datetime64Kind",
    "datetime64m",
    "datetime64M",
    "datetime64ms",
    "datetime64ns",
    "datetime64ps",
    "datetime64s",
    "Datetime64Unit",
    "datetime64us",
    "datetime64W",
    "datetime64Y",
    "discretize",
    "DTypeB",
    "DTypeDns",
    "DTypeF",
    "DTypeI",
    "DTypeO",
    "EmptyNumpyConcatenateError",
    "ffill_non_nan_slices",
    "fillna",
    "flatn0",
    "get_fill_value",
    "has_dtype",
    "InfElementsError",
    "InvalidDTypeError",
    "is_at_least_or_nan",
    "is_at_least",
    "is_at_most_or_nan",
    "is_at_most",
    "is_between_or_nan",
    "is_between",
    "is_empty",
    "is_finite_and_integral_or_nan",
    "is_finite_and_integral",
    "is_finite_and_negative_or_nan",
    "is_finite_and_negative",
    "is_finite_and_non_negative_or_nan",
    "is_finite_and_non_negative",
    "is_finite_and_non_positive_or_nan",
    "is_finite_and_non_positive",
    "is_finite_and_non_zero_or_nan",
    "is_finite_and_non_zero",
    "is_finite_and_positive_or_nan",
    "is_finite_and_positive",
    "is_finite_or_nan",
    "is_greater_than_or_nan",
    "is_greater_than",
    "is_integral_or_nan",
    "is_integral",
    "is_less_than_or_nan",
    "is_less_than",
    "is_negative_or_nan",
    "is_negative",
    "is_non_empty",
    "is_non_negative_or_nan",
    "is_non_negative",
    "is_non_positive_or_nan",
    "is_non_positive",
    "is_non_singular",
    "is_non_zero_or_nan",
    "is_non_zero",
    "is_positive_or_nan",
    "is_positive_semidefinite",
    "is_positive",
    "is_symmetric",
    "is_zero_or_finite_and_non_micro_or_nan",
    "is_zero_or_finite_and_non_micro",
    "is_zero_or_nan",
    "is_zero_or_non_micro_or_nan",
    "is_zero_or_non_micro",
    "is_zero",
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
    "LossOfNanosecondsError",
    "maximum",
    "minimum",
    "MultipleTrueElementsError",
    "NanElementsError",
    "NDArray0",
    "NDArray1",
    "NDArray2",
    "NDArray3",
    "NDArrayA",
    "NDArrayB",
    "NDArrayB0",
    "NDArrayB1",
    "NDArrayB2",
    "NDArrayB3",
    "NDArrayD",
    "NDArrayD0",
    "NDArrayD1",
    "NDArrayD2",
    "NDArrayD3",
    "NDArrayDas",
    "NDArrayDas0",
    "NDArrayDas1",
    "NDArrayDas2",
    "NDArrayDas3",
    "NDArrayDD",
    "NDArrayDD0",
    "NDArrayDD1",
    "NDArrayDD2",
    "NDArrayDD3",
    "NDArrayDfs",
    "NDArrayDfs0",
    "NDArrayDfs1",
    "NDArrayDfs2",
    "NDArrayDfs3",
    "NDArrayDh",
    "NDArrayDh0",
    "NDArrayDh1",
    "NDArrayDh2",
    "NDArrayDh3",
    "NDArrayDm",
    "NDArrayDM",
    "NDArrayDm0",
    "NDArrayDM0",
    "NDArrayDm1",
    "NDArrayDM1",
    "NDArrayDm2",
    "NDArrayDM2",
    "NDArrayDm3",
    "NDArrayDM3",
    "NDArrayDms",
    "NDArrayDms0",
    "NDArrayDms1",
    "NDArrayDms2",
    "NDArrayDms3",
    "NDArrayDns",
    "NDArrayDns0",
    "NDArrayDns1",
    "NDArrayDns2",
    "NDArrayDns3",
    "NDArrayDps",
    "NDArrayDps0",
    "NDArrayDps1",
    "NDArrayDps2",
    "NDArrayDps3",
    "NDArrayDs",
    "NDArrayDs0",
    "NDArrayDs1",
    "NDArrayDs2",
    "NDArrayDs3",
    "NDArrayDus",
    "NDArrayDus0",
    "NDArrayDus1",
    "NDArrayDus2",
    "NDArrayDus3",
    "NDArrayDW",
    "NDArrayDW0",
    "NDArrayDW1",
    "NDArrayDW2",
    "NDArrayDW3",
    "NDArrayDY",
    "NDArrayDY0",
    "NDArrayDY1",
    "NDArrayDY2",
    "NDArrayDY3",
    "NDArrayF",
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
    "NDArrayF3Fin",
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
    "NDArrayI",
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
    "NDArrayO",
    "NDArrayO0",
    "NDArrayO1",
    "NDArrayO2",
    "NDArrayO3",
    "NDim0",
    "NDim1",
    "NDim2",
    "NDim3",
    "NonIntegralElementsError",
    "NoTrueElementsError",
    "redirect_to_empty_numpy_concatenate_error",
    "shift_bool",
    "shift",
    "year",
    "ZeroShiftError",
]

try:
    from utilities.numpy.bottleneck import ZeroPercentageChangeSpanError
    from utilities.numpy.bottleneck import ffill
    from utilities.numpy.bottleneck import pct_change
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["ffill", "pct_change", "ZeroPercentageChangeSpanError"]


try:
    from utilities.numpy.numbagg import ewma
    from utilities.numpy.numbagg import exp_moving_sum
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["ewma", "exp_moving_sum"]
