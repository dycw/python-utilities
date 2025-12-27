from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from operator import or_
from pathlib import Path
from stat import (
    S_IMODE,
    S_IRGRP,
    S_IROTH,
    S_IRUSR,
    S_IWGRP,
    S_IWOTH,
    S_IWUSR,
    S_IXGRP,
    S_IXOTH,
    S_IXUSR,
)
from typing import TYPE_CHECKING, Literal, Self, assert_never, override

from utilities.dataclasses import replace_non_sentinel
from utilities.re import ExtractGroupsError, extract_groups
from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    from utilities.types import PathLike


type PermissionsLike = Permissions | int | str


##


def ensure_perms(perms: PermissionsLike, /) -> Permissions:
    """Ensure a set of file permissions."""
    match perms:
        case Permissions():
            return perms
        case int():
            return Permissions.from_int(perms)
        case str():
            return Permissions.from_text(perms)
        case never:
            assert_never(never)



##



@dataclass(order=True, unsafe_hash=True, kw_only=True, slots=True)
class Permissions:
    """A set of file permissions."""

    user_read: bool = False
    user_write: bool = False
    user_execute: bool = False
    group_read: bool = False
    group_write: bool = False
    group_execute: bool = False
    others_read: bool = False
    others_write: bool = False
    others_execute: bool = False

    def __int__(self) -> int:
        flags: list[int] = [
            S_IRUSR if self.user_read else 0,
            S_IWUSR if self.user_write else 0,
            S_IXUSR if self.user_execute else 0,
            S_IRGRP if self.group_read else 0,
            S_IWGRP if self.group_write else 0,
            S_IXGRP if self.group_execute else 0,
            S_IROTH if self.others_read else 0,
            S_IWOTH if self.others_write else 0,
            S_IXOTH if self.others_execute else 0,
        ]
        return reduce(or_, flags)

    @override
    def __repr__(self) -> str:
        return ",".join([
            self._repr_parts(
                "u",
                read=self.user_read,
                write=self.user_write,
                execute=self.user_execute,
            ),
            self._repr_parts(
                "g",
                read=self.group_read,
                write=self.group_write,
                execute=self.group_execute,
            ),
            self._repr_parts(
                "o",
                read=self.others_read,
                write=self.others_write,
                execute=self.others_execute,
            ),
        ])

    def _repr_parts(
        self,
        prefix: Literal["u", "g", "o"],
        /,
        *,
        read: bool = False,
        write: bool = False,
        execute: bool = False,
    ) -> str:
        parts: list[str] = [
            "r" if read else "",
            "w" if write else "",
            "x" if execute else "",
        ]
        return f"{prefix}={''.join(parts)}"

    @override
    def __str__(self) -> str:
        return repr(self)

    @classmethod
    def from_human_int(cls, n: int, /) -> Self:
        if not (0 <= n <= 777):
            raise PermissionsFromHumanIntRangeError(n=n)
        user_read, user_write, user_execute = cls._from_human_int(n, (n // 100) % 10)
        group_read, group_write, group_execute = cls._from_human_int(n, (n // 10) % 10)
        others_read, others_write, others_execute = cls._from_human_int(n, n % 10)
        return cls(
            user_read=user_read,
            user_write=user_write,
            user_execute=user_execute,
            group_read=group_read,
            group_write=group_write,
            group_execute=group_execute,
            others_read=others_read,
            others_write=others_write,
            others_execute=others_execute,
        )

    @classmethod
    def _from_human_int(cls, n: int, digit: int, /) -> tuple[bool, bool, bool]:
        if not (0 <= digit <= 7):
            raise PermissionsFromHumanIntDigitError(n=n, digit=digit)
        return bool(4 & digit), bool(2 & digit), bool(1 & digit)

    @classmethod
    def from_int(cls, n: int, /) -> Self:
        if 0o0 <= n <= 0o777:
            return cls(
                user_read=bool(n & S_IRUSR),
                user_write=bool(n & S_IWUSR),
                user_execute=bool(n & S_IXUSR),
                group_read=bool(n & S_IRGRP),
                group_write=bool(n & S_IWGRP),
                group_execute=bool(n & S_IXGRP),
                others_read=bool(n & S_IROTH),
                others_write=bool(n & S_IWOTH),
                others_execute=bool(n & S_IXOTH),
            )
        raise PermissionsFromIntError(n=n)

    @classmethod
    def from_path(cls, path: PathLike, /) -> Self:
        return cls.from_int(S_IMODE(Path(path).stat().st_mode))

    @classmethod
    def from_text(cls, text: str, /) -> Self:
        try:
            user, group, others = extract_groups(
                r"^u=(r?w?x?),g=(r?w?x?),o=(r?w?x?)$", text
            )
        except ExtractGroupsError:
            raise PermissionsFromTextError(text=text) from None
        user_read, user_write, user_execute = cls._from_text_part(user)
        group_read, group_write, group_execute = cls._from_text_part(group)
        others_read, others_write, others_execute = cls._from_text_part(others)
        return cls(
            user_read=user_read,
            user_write=user_write,
            user_execute=user_execute,
            group_read=group_read,
            group_write=group_write,
            group_execute=group_execute,
            others_read=others_read,
            others_write=others_write,
            others_execute=others_execute,
        )

    @classmethod
    def _from_text_part(cls, text: str, /) -> tuple[bool, bool, bool]:
        read, write, execute = extract_groups("^(r?)(w?)(x?)$", text)
        return read != "", write != "", execute != ""

    @property
    def human_int(self) -> int:
        return (
            100
            * self._human_int(
                read=self.user_read, write=self.user_write, execute=self.user_execute
            )
            + 10
            * self._human_int(
                read=self.group_read, write=self.group_write, execute=self.group_execute
            )
            + self._human_int(
                read=self.others_read,
                write=self.others_write,
                execute=self.others_execute,
            )
        )

    def _human_int(
        self, *, read: bool = False, write: bool = False, execute: bool = False
    ) -> int:
        return (4 if read else 0) + (2 if write else 0) + (1 if execute else 0)

    def replace(
        self,
        *,
        user_read: bool | Sentinel = sentinel,
        user_write: bool | Sentinel = sentinel,
        user_execute: bool | Sentinel = sentinel,
        group_read: bool | Sentinel = sentinel,
        group_write: bool | Sentinel = sentinel,
        group_execute: bool | Sentinel = sentinel,
        others_read: bool | Sentinel = sentinel,
        others_write: bool | Sentinel = sentinel,
        others_execute: bool | Sentinel = sentinel,
    ) -> Self:
        return replace_non_sentinel(
            self,
            user_read=user_read,
            user_write=user_write,
            user_execute=user_execute,
            group_read=group_read,
            group_write=group_write,
            group_execute=group_execute,
            others_read=others_read,
            others_write=others_write,
            others_execute=others_execute,
        )


@dataclass(kw_only=True, slots=True)
class PermissionsError(Exception): ...


@dataclass(kw_only=True, slots=True)
class PermissionsFromHumanIntError(PermissionsError):
    n: int


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


@dataclass(kw_only=True, slots=True)
class PermissionsFromIntRangeError(PermissionsFromIntError):
    @override
    def __str__(self) -> str:
<<<<<<< HEAD
        return f"Invalid integer for permissions; got {self.n} = {oct(self.n)}"
||||||| parent of b8f6c85d (2025-12-27 17:21:37 (Sat)  > DW-Mac  > derekwan)
        return f"Invalid integer for permissions; got {self.n}"


@dataclass(kw_only=True, slots=True)
class PermissionsFromOctalError(PermissionsError):
    n: int

    @override
    def __str__(self) -> str:
        return f"Invalid octal for permissions; got {oct(self.n)}"
=======
        return f"Invalid integer for permissions; got {self.n}"


@dataclass(kw_only=True, slots=True)
class PermissionsFromIntDigitError(PermissionsFromIntError):
    digit: int

    @override
    def __str__(self) -> str:
        return f"Invalid integer for permissions; got digit {self.digit} in {self.n}"


@dataclass(kw_only=True, slots=True)
class PermissionsFromOctalError(PermissionsError):
    n: int

    @override
    def __str__(self) -> str:
        return f"Invalid octal for permissions; got {oct(self.n)}"
>>>>>>> b8f6c85d (2025-12-27 17:21:37 (Sat)  > DW-Mac  > derekwan)


@dataclass(kw_only=True, slots=True)
class PermissionsFromTextError(PermissionsError):
    text: str

    @override
    def __str__(self) -> str:
        return f"Invalid string for permissions; got {self.text!r}"


__all__ = [
    "Permissions",
    "PermissionsError",
<<<<<<< HEAD
    "PermissionsFromHumanIntDigitError",
    "PermissionsFromHumanIntError",
||||||| parent of b8f6c85d (2025-12-27 17:21:37 (Sat)  > DW-Mac  > derekwan)
=======
    "PermissionsFromIntDigitError",
>>>>>>> b8f6c85d (2025-12-27 17:21:37 (Sat)  > DW-Mac  > derekwan)
    "PermissionsFromIntError",
    "PermissionsFromTextError",
<<<<<<< HEAD
    "ensure_perms",
||||||| parent of b8f6c85d (2025-12-27 17:21:37 (Sat)  > DW-Mac  > derekwan)
=======
    "PermissionsLike",
    "ensure_perms",
>>>>>>> b8f6c85d (2025-12-27 17:21:37 (Sat)  > DW-Mac  > derekwan)
]
