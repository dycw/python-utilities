from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from reprlib import repr as repr_
from typing import TYPE_CHECKING, assert_never, override

from utilities.types import CopyOrMove, PathLike, SupportsRichComparison

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Iterable
    from pathlib import Path
    from re import Pattern


###############################################################################
#### builtins #################################################################
###############################################################################


@dataclass(kw_only=True, slots=True)
class MinNullableError[T: SupportsRichComparison](Exception):
    iterable: Iterable[T | None]

    @override
    def __str__(self) -> str:
        return f"Minimum of {repr_(self.iterable)} is undefined"


@dataclass(kw_only=True, slots=True)
class MaxNullableError[T: SupportsRichComparison](Exception):
    iterable: Iterable[T | None]

    @override
    def __str__(self) -> str:
        return f"Maximum of {repr_(self.iterable)} is undefined"


###############################################################################
#### compression ##############################################################
###############################################################################


def _compress_error_msg(srcs: Iterable[PathLike], dest: PathLike, /) -> str:
    return f"Cannot compress source(s) {[repr(str(s)) for s in srcs]} since destination {str(dest)!r} already exists"


@dataclass(kw_only=True, slots=True)
class CompressBZ2Error(Exception):
    srcs: list[Path]
    dest: Path

    @override
    def __str__(self) -> str:
        return _compress_error_msg(self.srcs, self.dest)


@dataclass(kw_only=True, slots=True)
class CompressGzipError(Exception):
    srcs: list[Path]
    dest: Path

    @override
    def __str__(self) -> str:
        return _compress_error_msg(self.srcs, self.dest)


@dataclass(kw_only=True, slots=True)
class CompressLZMAError(Exception):
    srcs: list[Path]
    dest: Path

    @override
    def __str__(self) -> str:
        return _compress_error_msg(self.srcs, self.dest)


@dataclass(kw_only=True, slots=True)
class CompressZipError(Exception):
    srcs: list[Path]
    dest: Path

    @override
    def __str__(self) -> str:
        return _compress_error_msg(self.srcs, self.dest)


@dataclass(kw_only=True, slots=True)
class CompressFilesError(Exception):
    srcs: list[Path]
    dest: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


##


def _yield_uncompressed_file_not_found_error_msg(path: PathLike, /) -> str:
    return f"Cannot uncompress {str(path)!r} since it does not exist"


def _yield_uncompressed_file_is_a_directory_error_msg(path: PathLike, /) -> str:
    return f"Cannot uncompress {str(path)!r} since it is a directory"


@dataclass(kw_only=True, slots=True)
class YieldBZ2Error(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class YieldBZ2FileNotFoundError(YieldBZ2Error):
    path: Path

    @override
    def __str__(self) -> str:
        return _yield_uncompressed_file_not_found_error_msg(self.path)


@dataclass(kw_only=True, slots=True)
class YieldBZ2IsADirectoryError(YieldBZ2Error):
    path: Path

    @override
    def __str__(self) -> str:
        return _yield_uncompressed_file_is_a_directory_error_msg(self.path)


@dataclass(kw_only=True, slots=True)
class YieldGzipError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class YieldGzipFileNotFoundError(YieldGzipError):
    path: Path

    @override
    def __str__(self) -> str:
        return _yield_uncompressed_file_not_found_error_msg(self.path)


@dataclass(kw_only=True, slots=True)
class YieldGzipIsADirectoryError(YieldGzipError):
    path: Path

    @override
    def __str__(self) -> str:
        return _yield_uncompressed_file_is_a_directory_error_msg(self.path)


@dataclass(kw_only=True, slots=True)
class YieldLZMAError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class YieldLZMAFileNotFoundError(YieldLZMAError):
    path: Path

    @override
    def __str__(self) -> str:
        return _yield_uncompressed_file_not_found_error_msg(self.path)


@dataclass(kw_only=True, slots=True)
class YieldLZMAIsADirectoryError(YieldLZMAError):
    path: Path

    @override
    def __str__(self) -> str:
        return _yield_uncompressed_file_is_a_directory_error_msg(self.path)


@dataclass(kw_only=True, slots=True)
class YieldZipError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class YieldZipFileNotFoundError(YieldZipError):
    path: Path

    @override
    def __str__(self) -> str:
        return _yield_uncompressed_file_not_found_error_msg(self.path)


@dataclass(kw_only=True, slots=True)
class YieldZipIsADirectoryError(YieldZipError):
    path: Path

    @override
    def __str__(self) -> str:
        return _yield_uncompressed_file_is_a_directory_error_msg(self.path)


@dataclass(kw_only=True, slots=True)
class YieldUncompressedError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class YieldUncompressedFileNotFoundError(YieldUncompressedError):
    path: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class YieldUncompressedIsADirectoryError(YieldUncompressedError):
    path: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


###############################################################################
#### itertools ################################################################
###############################################################################


@dataclass(kw_only=True, slots=True)
class OneError[T](Exception):
    iterables: tuple[Iterable[T], ...]


@dataclass(kw_only=True, slots=True)
class OneEmptyError[T](OneError[T]):
    @override
    def __str__(self) -> str:
        return f"Iterable(s) {repr_(self.iterables)} must not be empty"


@dataclass(kw_only=True, slots=True)
class OneNonUniqueError[T](OneError):
    first: T
    second: T

    @override
    def __str__(self) -> str:
        return f"Iterable(s) {repr_(self.iterables)} must contain exactly one item; got {repr_(self.first)}, {repr_(self.second)} and perhaps more"


##


@dataclass(kw_only=True, slots=True)
class OneStrError(Exception):
    iterable: Iterable[str]
    text: str
    head: bool = False
    case_sensitive: bool = False


@dataclass(kw_only=True, slots=True)
class OneStrEmptyError(OneStrError):
    @override
    def __str__(self) -> str:
        head = f"Iterable {repr_(self.iterable)} does not contain"
        match self.head, self.case_sensitive:
            case False, True:
                tail = repr(self.text)
            case False, False:
                tail = f"{self.text!r} (modulo case)"
            case True, True:
                tail = f"any string starting with {self.text!r}"
            case True, False:
                tail = f"any string starting with {self.text!r} (modulo case)"
            case never:
                assert_never(never)
        return f"{head} {tail}"


@dataclass(kw_only=True, slots=True)
class OneStrNonUniqueError(OneStrError):
    first: str
    second: str

    @override
    def __str__(self) -> str:
        head = f"Iterable {repr_(self.iterable)} must contain"
        match self.head, self.case_sensitive:
            case False, True:
                mid = f"{self.text!r} exactly once"
            case False, False:
                mid = f"{self.text!r} exactly once (modulo case)"
            case True, True:
                mid = f"exactly one string starting with {self.text!r}"
            case True, False:
                mid = f"exactly one string starting with {self.text!r} (modulo case)"
            case never:
                assert_never(never)
        return f"{head} {mid}; got {self.first!r}, {self.second!r} and perhaps more"


###############################################################################
#### os #######################################################################
###############################################################################


def _copy_or_move_source_not_found_error_msg(src: PathLike, /) -> str:
    return f"Source {str(src)!r} does not exist"


def _copy_or_move_dest_already_exists_error_msg(
    mode: CopyOrMove, src: PathLike, dest: PathLike, /
) -> str:
    return f"Cannot {mode} source {str(src)!r} since destination {str(dest)!r} already exists"


@dataclass(kw_only=True, slots=True)
class CopyError(Exception):
    src: Path


@dataclass(kw_only=True, slots=True)
class CopySourceNotFoundError(CopyError):
    @override
    def __str__(self) -> str:
        return _copy_or_move_source_not_found_error_msg(self.src)


@dataclass(kw_only=True, slots=True)
class CopyDestinationExistsError(CopyError):
    dest: Path

    @override
    def __str__(self) -> str:
        return _copy_or_move_dest_already_exists_error_msg("copy", self.src, self.dest)


@dataclass(kw_only=True, slots=True)
class MoveError(Exception):
    src: Path


@dataclass(kw_only=True, slots=True)
class MoveSourceNotFoundError(MoveError):
    @override
    def __str__(self) -> str:
        return _copy_or_move_source_not_found_error_msg(self.src)


@dataclass(kw_only=True, slots=True)
class MoveDestinationExistsError(MoveError):
    dest: Path

    @override
    def __str__(self) -> str:
        return _copy_or_move_dest_already_exists_error_msg("move", self.src, self.dest)


@dataclass(kw_only=True, slots=True)
class CopyOrMoveError(Exception):
    src: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class CopyOrMoveSourceNotFoundError(CopyOrMoveError):
    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class CopyOrMoveDestinationExistsError(CopyOrMoveError):
    dest: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


##


@dataclass(kw_only=True, slots=True)
class GetEnvError(Exception):
    key: str
    case_sensitive: bool = False

    @override
    def __str__(self) -> str:
        desc = f"No environment variable {str(self.key)!r}"
        return desc if self.case_sensitive else f"{desc} (modulo case)"


###############################################################################
#### pathlib ##################################################################
###############################################################################


@dataclass(kw_only=True, slots=True)
class FileOrDirError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class FileOrDirMissingError(FileOrDirError):
    @override
    def __str__(self) -> str:
        return f"Path does not exist: {str(self.path)!r}"


@dataclass(kw_only=True, slots=True)
class FileOrDirTypeError(FileOrDirError):
    @override
    def __str__(self) -> str:
        return f"Path is neither a file nor a directory: {str(self.path)!r}"


##


@dataclass(kw_only=True, slots=True)
class ReadTextIfExistingFileError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        return _read_file_is_a_directory_error(self.path)


###############################################################################
#### permissions ##############################################################
###############################################################################


@dataclass(kw_only=True, slots=True)
class PermissionsError(Exception):
    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class PermissionsFromHumanIntError(PermissionsError):
    n: int

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class PermissionsFromHumanIntRangeError(PermissionsFromHumanIntError):
    @override
    def __str__(self) -> str:
        return f"Invalid human integer for permissions; got {self.n}"


@dataclass(kw_only=True, slots=True)
class PermissionsFromHumanIntDigitError(PermissionsFromHumanIntError):
    digit: int

    @override
    def __str__(self) -> str:
        return (
            f"Invalid human integer for permissions; got digit {self.digit} in {self.n}"
        )


@dataclass(kw_only=True, slots=True)
class PermissionsFromIntError(PermissionsError):
    n: int

    @override
    def __str__(self) -> str:
        return f"Invalid integer for permissions; got {self.n} = {oct(self.n)}"


@dataclass(kw_only=True, slots=True)
class PermissionsFromTextError(PermissionsError):
    text: str

    @override
    def __str__(self) -> str:
        return f"Invalid string for permissions; got {self.text!r}"


###############################################################################
#### re #######################################################################
###############################################################################


@dataclass(kw_only=True, slots=True)
class ExtractGroupError(Exception):
    pattern: Pattern[str]
    text: str

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class ExtractGroupMultipleCaptureGroupsError(ExtractGroupError):
    @override
    def __str__(self) -> str:
        return f"Pattern {str(self.pattern)!r} must contain exactly one capture group; it had multiple"


@dataclass(kw_only=True, slots=True)
class ExtractGroupMultipleMatchesError(ExtractGroupError):
    matches: list[str]

    @override
    def __str__(self) -> str:
        return f"Pattern {str(self.pattern)!r} must match against {self.text!r} exactly once; matches were {', '.join([repr(str(m)) for m in self.matches])}"


@dataclass(kw_only=True, slots=True)
class ExtractGroupNoCaptureGroupsError(ExtractGroupError):
    @override
    def __str__(self) -> str:
        return f"Pattern {str(self.pattern)!r} must contain exactly one capture group; it had none"


@dataclass(kw_only=True, slots=True)
class ExtractGroupNoMatchesError(ExtractGroupError):
    @override
    def __str__(self) -> str:
        return f"Pattern {str(self.pattern)!r} must match against {self.text!r}"


##


@dataclass(kw_only=True, slots=True)
class ExtractGroupsError(Exception):
    pattern: Pattern[str]
    text: str

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class ExtractGroupsMultipleMatchesError(ExtractGroupsError):
    matches: list[str]

    @override
    def __str__(self) -> str:
        return f"Pattern {str(self.pattern)!r} must match against {self.text!r} exactly once; matches were {', '.join(repr(str(m)) for m in self.matches)}"


@dataclass(kw_only=True, slots=True)
class ExtractGroupsNoCaptureGroupsError(ExtractGroupsError):
    @override
    def __str__(self) -> str:
        return f"Pattern {str(self.pattern)!r} must contain at least one capture group"


@dataclass(kw_only=True, slots=True)
class ExtractGroupsNoMatchesError(ExtractGroupsError):
    @override
    def __str__(self) -> str:
        return f"Pattern {str(self.pattern)!r} must match against {self.text!r}"


###############################################################################
#### readers/writers ##########################################################
###############################################################################


def _read_file_file_not_found_error(path: PathLike, /) -> str:
    return f"Cannot read from {str(path)!r} since it does not exist"


def _read_file_is_a_directory_error(path: PathLike, /) -> str:
    return f"Cannot read from {str(path)!r} since it is a directory"


def _write_file_error(path: PathLike, /) -> str:
    return f"Cannot write to {str(path)!r} since it already exists"


@dataclass(kw_only=True, slots=True)
class ReadBytesError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class ReadBytesFileNotFoundError(ReadBytesError):
    path: Path

    @override
    def __str__(self) -> str:
        return _read_file_file_not_found_error(self.path)


@dataclass(kw_only=True, slots=True)
class ReadBytesIsADirectoryError(ReadBytesError):
    path: Path

    @override
    def __str__(self) -> str:
        return _read_file_is_a_directory_error(self.path)


@dataclass(kw_only=True, slots=True)
class WriteBytesError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        return _write_file_error(self.path)


@dataclass(kw_only=True, slots=True)
class ReadPickleError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class ReadPickleFileNotFoundError(ReadPickleError):
    path: Path

    @override
    def __str__(self) -> str:
        return _read_file_file_not_found_error(self.path)


@dataclass(kw_only=True, slots=True)
class ReadPickleIsADirectoryError(ReadPickleError):
    path: Path

    @override
    def __str__(self) -> str:
        return _read_file_is_a_directory_error(self.path)


@dataclass(kw_only=True, slots=True)
class WritePickleError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        return _write_file_error(self.path)


@dataclass(kw_only=True, slots=True)
class ReadTextError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class ReadTextFileNotFoundError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        return _read_file_file_not_found_error(self.path)


@dataclass(kw_only=True, slots=True)
class ReadTextIsADirectoryError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        return _read_file_is_a_directory_error(self.path)


@dataclass(kw_only=True, slots=True)
class WriteTextError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        return _write_file_error(self.path)


###############################################################################
#### shutil ###################################################################
###############################################################################


@dataclass(kw_only=True, slots=True)
class WhichError(Exception):
    cmd: str

    @override
    def __str__(self) -> str:
        return f"{self.cmd!r} not found"


###############################################################################
#### text #####################################################################
###############################################################################


@dataclass(kw_only=True, slots=True)
class SubstituteError(Exception):
    key: str

    @override
    def __str__(self) -> str:
        return f"Missing key: {self.key!r}"


###############################################################################
#### whenever #################################################################
###############################################################################


@dataclass(kw_only=True, slots=True)
class DeltaComponentsError(Exception):
    years: int = 0
    months: int = 0
    days: int = 0

    @override
    def __str__(self) -> str:
        return f"Years, months and days must have the same sign; got {self.years}, {self.months} and {self.days}"


##


@dataclass(kw_only=True, slots=True)
class NumYearsError(Exception):
    months: int = 0
    weeks: int = 0
    days: int = 0
    hours: int = 0
    minutes: int = 0
    seconds: int = 0
    milliseconds: int = 0
    microseconds: int = 0
    nanoseconds: int = 0

    @override
    def __str__(self) -> str:
        return f"Delta must not contain months ({self.months}), weeks ({self.weeks}), days ({self.days}), hours ({self.hours}), minutes ({self.minutes}), seconds ({self.seconds}), milliseconds ({self.milliseconds}), microseconds ({self.microseconds}) or nanoseconds ({self.nanoseconds})"


@dataclass(kw_only=True, slots=True)
class NumMonthsError(Exception):
    weeks: int = 0
    days: int = 0
    hours: int = 0
    minutes: int = 0
    seconds: int = 0
    milliseconds: int = 0
    microseconds: int = 0
    nanoseconds: int = 0

    @override
    def __str__(self) -> str:
        return f"Delta must not contain weeks ({self.weeks}), days ({self.days}), hours ({self.hours}), minutes ({self.minutes}), seconds ({self.seconds}), milliseconds ({self.milliseconds}), microseconds ({self.microseconds}) or nanoseconds ({self.nanoseconds})"


@dataclass(kw_only=True, slots=True)
class NumWeeksError(Exception):
    years: int = 0
    months: int = 0
    days: int = 0
    hours: int = 0
    minutes: int = 0
    seconds: int = 0
    milliseconds: int = 0
    microseconds: int = 0
    nanoseconds: int = 0

    @override
    def __str__(self) -> str:
        return f"Delta must not contain years ({self.years}), months ({self.months}), days ({self.days}), hours ({self.hours}), minutes ({self.minutes}), seconds ({self.seconds}), milliseconds ({self.milliseconds}), microseconds ({self.microseconds}) or nanoseconds ({self.nanoseconds})"


@dataclass(kw_only=True, slots=True)
class NumDaysError(Exception):
    years: int = 0
    months: int = 0
    hours: int = 0
    minutes: int = 0
    seconds: int = 0
    milliseconds: int = 0
    microseconds: int = 0
    nanoseconds: int = 0

    @override
    def __str__(self) -> str:
        return f"Delta must not contain years ({self.years}), months ({self.months}), hours ({self.hours}), minutes ({self.minutes}), seconds ({self.seconds}), milliseconds ({self.milliseconds}), microseconds ({self.microseconds}) or nanoseconds ({self.nanoseconds})"


@dataclass(kw_only=True, slots=True)
class NumHoursError(Exception):
    years: int = 0
    months: int = 0
    minutes: int = 0
    seconds: int = 0
    milliseconds: int = 0
    microseconds: int = 0
    nanoseconds: int = 0

    @override
    def __str__(self) -> str:
        return f"Delta must not contain years ({self.years}), months ({self.months}), minutes ({self.minutes}), seconds ({self.seconds}), milliseconds ({self.milliseconds}), microseconds ({self.microseconds}) or nanoseconds ({self.nanoseconds})"


@dataclass(kw_only=True, slots=True)
class NumMinutesError(Exception):
    years: int = 0
    months: int = 0
    seconds: int = 0
    milliseconds: int = 0
    microseconds: int = 0
    nanoseconds: int = 0

    @override
    def __str__(self) -> str:
        return f"Delta must not contain years ({self.years}), months ({self.months}), seconds ({self.seconds}), milliseconds ({self.milliseconds}), microseconds ({self.microseconds}) or nanoseconds ({self.nanoseconds})"


@dataclass(kw_only=True, slots=True)
class NumSecondsError(Exception):
    years: int = 0
    months: int = 0
    milliseconds: int = 0
    microseconds: int = 0
    nanoseconds: int = 0

    @override
    def __str__(self) -> str:
        return f"Delta must not contain years ({self.years}), months ({self.months}), milliseconds ({self.milliseconds}), microseconds ({self.microseconds}) or nanoseconds ({self.nanoseconds})"


@dataclass(kw_only=True, slots=True)
class NumMilliSecondsError(Exception):
    years: int = 0
    months: int = 0
    microseconds: int = 0
    nanoseconds: int = 0

    @override
    def __str__(self) -> str:
        return f"Delta must not contain years ({self.years}), months ({self.months}), microseconds ({self.microseconds}) or nanoseconds ({self.nanoseconds})"


@dataclass(kw_only=True, slots=True)
class NumMicroSecondsError(Exception):
    years: int = 0
    months: int = 0
    nanoseconds: int = 0

    @override
    def __str__(self) -> str:
        return f"Delta must not contain years ({self.years}), months ({self.months}) or nanoseconds ({self.nanoseconds})"


@dataclass(kw_only=True, slots=True)
class NumNanoSecondsError(Exception):
    years: int = 0
    months: int = 0

    @override
    def __str__(self) -> str:
        return f"Delta must not contain years ({self.years}) or months ({self.months})"


###############################################################################
#### writers ##################################################################
###############################################################################


@dataclass(kw_only=True, slots=True)
class YieldWritePathError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        return f"Cannot write to {str(self.path)!r} since it already exists"


###############################################################################
#### zoneinfo #################################################################
###############################################################################


@dataclass(kw_only=True, slots=True)
class ToTimeZoneNameError(Exception):
    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class ToTimeZoneNameInvalidKeyError(ToTimeZoneNameError):
    time_zone: str

    @override
    def __str__(self) -> str:
        return f"Invalid time-zone: {self.time_zone!r}"


@dataclass(kw_only=True, slots=True)
class ToTimeZoneNameInvalidTZInfoError(ToTimeZoneNameError):
    time_zone: dt.tzinfo

    @override
    def __str__(self) -> str:
        return f"Invalid time-zone: {self.time_zone}"


@dataclass(kw_only=True, slots=True)
class ToTimeZoneNamePlainDateTimeError(ToTimeZoneNameError):
    date_time: dt.datetime

    @override
    def __str__(self) -> str:
        return f"Plain date-time: {self.date_time}"


##


@dataclass(kw_only=True, slots=True)
class ToZoneInfoError(Exception):
    @override
    def __str__(self) -> str:
        raise NotImplementedError  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class ToZoneInfoInvalidTZInfoError(ToZoneInfoError):
    time_zone: dt.tzinfo

    @override
    def __str__(self) -> str:
        return f"Invalid time-zone: {self.time_zone}"


@dataclass(kw_only=True, slots=True)
class ToZoneInfoPlainDateTimeError(ToZoneInfoError):
    date_time: dt.datetime

    @override
    def __str__(self) -> str:
        return f"Plain date-time: {self.date_time}"


__all__ = [
    "CompressBZ2Error",
    "CompressFilesError",
    "CompressGzipError",
    "CompressLZMAError",
    "CompressZipError",
    "CopyDestinationExistsError",
    "CopyError",
    "CopyOrMoveDestinationExistsError",
    "CopyOrMoveSourceNotFoundError",
    "CopySourceNotFoundError",
    "DeltaComponentsError",
    "ExtractGroupError",
    "ExtractGroupMultipleCaptureGroupsError",
    "ExtractGroupMultipleMatchesError",
    "ExtractGroupNoCaptureGroupsError",
    "ExtractGroupNoMatchesError",
    "ExtractGroupsError",
    "ExtractGroupsMultipleMatchesError",
    "ExtractGroupsNoCaptureGroupsError",
    "ExtractGroupsNoMatchesError",
    "FileOrDirError",
    "FileOrDirMissingError",
    "FileOrDirTypeError",
    "GetEnvError",
    "MaxNullableError",
    "MinNullableError",
    "MoveDestinationExistsError",
    "MoveError",
    "MoveSourceNotFoundError",
    "NumDaysError",
    "NumHoursError",
    "NumMicroSecondsError",
    "NumMilliSecondsError",
    "NumMinutesError",
    "NumMonthsError",
    "NumNanoSecondsError",
    "NumSecondsError",
    "NumWeeksError",
    "NumYearsError",
    "OneEmptyError",
    "OneError",
    "OneNonUniqueError",
    "PermissionsError",
    "PermissionsFromHumanIntDigitError",
    "PermissionsFromHumanIntRangeError",
    "PermissionsFromIntError",
    "PermissionsFromIntError",
    "PermissionsFromTextError",
    "ReadBytesError",
    "ReadBytesFileNotFoundError",
    "ReadBytesIsADirectoryError",
    "ReadPickleError",
    "ReadPickleFileNotFoundError",
    "ReadPickleIsADirectoryError",
    "ReadTextError",
    "ReadTextFileNotFoundError",
    "ReadTextIfExistingFileError",
    "ReadTextIsADirectoryError",
    "SubstituteError",
    "ToTimeZoneNameError",
    "ToTimeZoneNameError",
    "ToTimeZoneNameInvalidKeyError",
    "ToTimeZoneNameInvalidTZInfoError",
    "ToTimeZoneNamePlainDateTimeError",
    "ToZoneInfoError",
    "ToZoneInfoPlainDateTimeError",
    "WhichError",
    "WriteBytesError",
    "WritePickleError",
    "WriteTextError",
    "YieldBZ2Error",
    "YieldBZ2FileNotFoundError",
    "YieldBZ2IsADirectoryError",
    "YieldGzipError",
    "YieldGzipFileNotFoundError",
    "YieldGzipIsADirectoryError",
    "YieldLZMAError",
    "YieldLZMAFileNotFoundError",
    "YieldLZMAIsADirectoryError",
    "YieldUncompressedError",
    "YieldUncompressedFileNotFoundError",
    "YieldUncompressedIsADirectoryError",
    "YieldWritePathError",
    "YieldZipError",
    "YieldZipFileNotFoundError",
    "YieldZipIsADirectoryError",
]
