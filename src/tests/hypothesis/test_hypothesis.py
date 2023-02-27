import datetime as dt
from math import inf
from math import isfinite
from math import isinf
from math import isnan
from pathlib import Path
from re import search
from typing import Optional

from hypothesis import Phase
from hypothesis import assume
from hypothesis import given
from hypothesis import settings
from hypothesis.errors import InvalidArgument
from hypothesis.strategies import DataObject
from hypothesis.strategies import DrawFn
from hypothesis.strategies import booleans
from hypothesis.strategies import composite
from hypothesis.strategies import data
from hypothesis.strategies import datetimes
from hypothesis.strategies import floats
from hypothesis.strategies import integers
from hypothesis.strategies import just
from hypothesis.strategies import none
from hypothesis.strategies import sets
from more_itertools import pairwise
from pytest import mark
from pytest import param
from pytest import raises

from utilities.datetime import UTC
from utilities.hypothesis import _MAX_EXAMPLES
from utilities.hypothesis import _NO_SHRINK
from utilities.hypothesis import assume_does_not_raise
from utilities.hypothesis import datetimes_utc
from utilities.hypothesis import floats_extra
from utilities.hypothesis import lists_fixed_length
from utilities.hypothesis import setup_hypothesis_profiles
from utilities.hypothesis import slices
from utilities.hypothesis import temp_dirs
from utilities.hypothesis import temp_paths
from utilities.hypothesis import text_ascii
from utilities.hypothesis import text_clean
from utilities.hypothesis import text_printable
from utilities.os import temp_environ
from utilities.pandas import TIMESTAMP_MAX_AS_DATETIME
from utilities.pandas import TIMESTAMP_MIN_AS_DATETIME
from utilities.tempfile import TemporaryDirectory


class TestAssumeDoesNotRaise:
    @given(x=booleans())
    def test_no_match_and_suppressed(self, x: bool) -> None:
        with assume_does_not_raise(ValueError):
            if x is True:
                msg = "x is True"
                raise ValueError(msg)
        assert x is False

    @given(x=booleans())
    def test_no_match_and_not_suppressed(self, x: bool) -> None:
        msg = "x is True"
        if x is True:
            with raises(ValueError, match=msg), assume_does_not_raise(
                RuntimeError,
            ):
                raise ValueError(msg)

    @given(x=booleans())
    def test_with_match_and_suppressed(self, x: bool) -> None:
        msg = "x is True"
        if x is True:
            with assume_does_not_raise(ValueError, match=msg):
                raise ValueError(msg)
        assert x is False

    @given(x=just(True))
    def test_with_match_and_not_suppressed(self, x: bool) -> None:
        msg = "x is True"
        if x is True:
            with raises(ValueError, match=msg), assume_does_not_raise(
                ValueError,
                match="wrong",
            ):
                raise ValueError(msg)


class TestDatetimesUTC:
    @given(
        data=data(),
        min_value=datetimes(
            min_value=TIMESTAMP_MIN_AS_DATETIME.replace(tzinfo=None),
        ),
        max_value=datetimes(
            max_value=TIMESTAMP_MAX_AS_DATETIME.replace(tzinfo=None),
        ),
    )
    def test_main(
        self,
        data: DataObject,
        min_value: dt.datetime,
        max_value: dt.datetime,
    ) -> None:
        min_value, max_value = (v.replace(tzinfo=UTC) for v in [min_value, max_value])
        _ = assume(min_value <= max_value)
        datetime = data.draw(
            datetimes_utc(min_value=min_value, max_value=max_value),
        )
        assert min_value <= datetime <= max_value


class TestFloatsExtra:
    @given(
        data=data(),
        min_value=floats() | none(),
        max_value=floats() | none(),
        allow_nan=booleans(),
        allow_inf=booleans(),
        allow_pos_inf=booleans(),
        allow_neg_inf=booleans(),
        integral=booleans(),
    )
    def test_main(
        self,
        data: DataObject,
        min_value: Optional[float],
        max_value: Optional[float],
        allow_nan: bool,
        allow_inf: bool,
        allow_pos_inf: bool,
        allow_neg_inf: bool,
        integral: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(
                floats_extra(
                    min_value=min_value,
                    max_value=max_value,
                    allow_nan=allow_nan,
                    allow_inf=allow_inf,
                    allow_pos_inf=allow_pos_inf,
                    allow_neg_inf=allow_neg_inf,
                    integral=integral,
                ),
            )
        if min_value is not None:
            assert (isfinite(x) and x >= min_value) or not isfinite(x)
        if max_value is not None:
            assert (isfinite(x) and x <= max_value) or not isfinite(x)
        if not allow_nan:
            assert not isnan(x)
        if not allow_inf:
            if not (allow_pos_inf or allow_neg_inf):
                assert not isinf(x)
            if not allow_pos_inf:
                assert x != inf
            if not allow_neg_inf:
                assert x != -inf
        if integral:
            assert (isfinite(x) and x == round(x)) or not isfinite(x)


class TestLiftDraw:
    @given(data=data(), x=booleans())
    def test_fixed(self, data: DataObject, x: bool) -> None:
        @composite
        def func(_draw: DrawFn, /) -> bool:
            _ = _draw(booleans())
            return x

        result = data.draw(func())
        assert result is x

    @given(data=data())
    def test_strategy(self, data: DataObject) -> None:
        @composite
        def func(_draw: DrawFn, /) -> bool:
            return _draw(booleans())

        result = data.draw(func())
        assert isinstance(result, bool)


class TestListsFixedLength:
    @given(data=data(), size=integers(1, 10))
    @mark.parametrize(
        "unique",
        [param(True, id="unique"), param(False, id="no unique")],
    )
    @mark.parametrize(
        "sorted_",
        [param(True, id="sorted"), param(False, id="no sorted")],
    )
    def test_main(
        self,
        data: DataObject,
        size: int,
        unique: bool,
        sorted_: bool,
    ) -> None:
        result = data.draw(
            lists_fixed_length(integers(), size, unique=unique, sorted=sorted_),
        )
        assert isinstance(result, list)
        assert len(result) == size
        if unique:
            assert len(set(result)) == len(result)
        if sorted_:
            assert sorted(result) == result


class TestSlices:
    @given(data=data(), iter_len=integers(0, 10))
    def test_main(self, data: DataObject, iter_len: int) -> None:
        slice_len = data.draw(integers(0, iter_len) | none())
        slice_ = data.draw(slices(iter_len, slice_len=slice_len))
        range_slice = range(iter_len)[slice_]
        assert all(i + 1 == j for i, j in pairwise(range_slice))
        if slice_len is not None:
            assert len(range_slice) == slice_len

    @given(data=data(), iter_len=integers(0, 10))
    def test_error(self, data: DataObject, iter_len: int) -> None:
        with raises(
            InvalidArgument,
            match=r"Slice length \d+ exceeds iterable length \d+",
        ):
            _ = data.draw(slices(iter_len, slice_len=iter_len + 1))


class TestSetupHypothesisProfiles:
    def test_main(self) -> None:
        setup_hypothesis_profiles()
        curr = settings()
        assert Phase.shrink in curr.phases
        assert curr.max_examples == 100

    def test_no_shrink(self) -> None:
        with temp_environ({_NO_SHRINK: "1"}):
            setup_hypothesis_profiles()
        assert Phase.shrink not in settings().phases

    @given(max_examples=integers(1, 100))
    def test_max_examples(self, max_examples: int) -> None:
        with temp_environ({_MAX_EXAMPLES: str(max_examples)}):
            setup_hypothesis_profiles()
        assert settings().max_examples == max_examples


class TestTempDirs:
    @given(temp_dir=temp_dirs())
    def test_main(self, temp_dir: TemporaryDirectory) -> None:
        _test_temp_path(temp_dir.name)

    @given(
        temp_dir=temp_dirs(),
        contents=sets(text_ascii(min_size=1), max_size=10),
    )
    def test_writing_files(
        self,
        temp_dir: TemporaryDirectory,
        contents: set[str],
    ) -> None:
        _test_writing_to_temp_path(temp_dir.name, contents)


class TestTempPaths:
    @given(temp_path=temp_paths())
    def test_main(self, temp_path: Path) -> None:
        _test_temp_path(temp_path)

    @given(
        temp_path=temp_paths(),
        contents=sets(text_ascii(min_size=1), max_size=10),
    )
    def test_writing_files(self, temp_path: Path, contents: set[str]) -> None:
        _test_writing_to_temp_path(temp_path, contents)


def _test_temp_path(path: Path, /) -> None:
    assert path.is_dir()
    assert len(set(path.iterdir())) == 0


def _test_writing_to_temp_path(path: Path, contents: set[str], /) -> None:
    assert len(set(path.iterdir())) == 0
    for content in contents:
        path.joinpath(content).touch()
    assert len(set(path.iterdir())) == len(contents)


class TestTextAscii:
    @given(
        data=data(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
        disallow_na=booleans(),
    )
    def test_main(
        self,
        data: DataObject,
        min_size: int,
        max_size: Optional[int],
        disallow_na: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(
                text_ascii(
                    min_size=min_size,
                    max_size=max_size,
                    disallow_na=disallow_na,
                ),
            )
        assert search("^[A-Za-z]*$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size
        if disallow_na:
            assert text != "NA"


class TestTextClean:
    @given(
        data=data(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
        disallow_na=booleans(),
    )
    def test_main(
        self,
        data: DataObject,
        min_size: int,
        max_size: Optional[int],
        disallow_na: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(
                text_clean(
                    min_size=min_size,
                    max_size=max_size,
                    disallow_na=disallow_na,
                ),
            )
        assert search("^\\S[^\\r\\n]*$|^$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size
        if disallow_na:
            assert text != "NA"


class TestTextPrintable:
    @given(
        data=data(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
        disallow_na=booleans(),
    )
    def test_main(
        self,
        data: DataObject,
        min_size: int,
        max_size: Optional[int],
        disallow_na: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(
                text_printable(
                    min_size=min_size,
                    max_size=max_size,
                    disallow_na=disallow_na,
                ),
            )
        assert search(
            r"^[0-9A-Za-z!\"#$%&'()*+,-./:;<=>?@\[\\\]^_`{|}~\s]*$",
            text,
        )
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size
        if disallow_na:
            assert text != "NA"
