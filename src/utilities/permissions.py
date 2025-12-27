from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from functools import reduce
from operator import or_
from stat import (
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
from typing import Literal, Self, override

from utilities.dataclasses import replace_non_sentinel
from utilities.functions import ensure_member
from utilities.re import (
    ExtractGroupError,
    ExtractGroupsError,
    extract_group,
    extract_groups,
)
from utilities.sentinel import Sentinel, sentinel
from utilities.typing import get_args

type _ZeroToSeven = Literal[0, 1, 2, 3, 4, 5, 6, 7]
_ZERO_TO_SEVEN: list[_ZeroToSeven] = list(get_args(_ZeroToSeven.__value__))


@dataclass(order=True, unsafe_hash=True, kw_only=True, slots=True)
class Permissions:
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
        return (
            100
            * self._int(
                read=self.user_read, write=self.user_write, execute=self.user_execute
            )
            + 10
            * self._int(
                read=self.group_read, write=self.group_write, execute=self.group_execute
            )
            + self._int(
                read=self.others_read,
                write=self.others_write,
                execute=self.others_execute,
            )
        )

    def _int(
        self, *, read: bool = False, write: bool = False, execute: bool = False
    ) -> _ZeroToSeven:
        return (4 if read else 0) + (2 if write else 0) + (1 if execute else 0)

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
        parts: list[str] = []
        if read:
            parts.append("r")
        if write:
            parts.append("w")
        if execute:
            parts.append("x")
        return f"{prefix}={''.join(parts)}"

    @override
    def __str__(self) -> str:
        return repr(self)

    @classmethod
    def from_int(cls, n: int, /) -> Self:
        with suppress(ExtractGroupsError):
            user, group, others = extract_groups(r"^([0-7])([0-7])([0-7])$", str(n))
            user_read, user_write, user_execute = cls._from_int(
                ensure_member(int(user), _ZERO_TO_SEVEN)
            )
            group_read, group_write, group_execute = cls._from_int(
                ensure_member(int(group), _ZERO_TO_SEVEN)
            )
            others_read, others_write, others_execute = cls._from_int(
                ensure_member(int(others), _ZERO_TO_SEVEN)
            )
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
        with suppress(ExtractGroupsError):
            group, others = extract_groups(r"^([0-7])([0-7])$", str(n))
            group_read, group_write, group_execute = cls._from_int(
                ensure_member(int(group), _ZERO_TO_SEVEN)
            )
            others_read, others_write, others_execute = cls._from_int(
                ensure_member(int(others), _ZERO_TO_SEVEN)
            )
            return cls(
                group_read=group_read,
                group_write=group_write,
                group_execute=group_execute,
                others_read=others_read,
                others_write=others_write,
                others_execute=others_execute,
            )
        with suppress(ExtractGroupError):
            others = extract_group(r"^([0-7])$", str(n))
            others_read, others_write, others_execute = cls._from_int(
                ensure_member(int(others), _ZERO_TO_SEVEN)
            )
            return cls(
                others_read=others_read,
                others_write=others_write,
                others_execute=others_execute,
            )
        return cls()

    @classmethod
    def _from_int(cls, n: _ZeroToSeven, /) -> tuple[bool, bool, bool]:
        return bool(4 & n), bool(2 & n), bool(1 & n)

    @classmethod
    def from_octal(cls, n: int, /) -> Self:
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

    @classmethod
    def from_text(cls, text: str, /) -> Self:
        try:
            user, group, others = extract_groups(
                r"^u=(r?w?x?),g=(r?w?x?),o=(r?w?x?)$", text
            )
        except ExtractGroupsError:
            raise PermissionsFromStrError(text=text) from None
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
    def octal(self) -> int:
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
class PermissionsFromIntError(PermissionsError):
    n: int

    @override
    def __str__(self) -> str:
        return f"Invalid integer for permissions; got {self.n}"


@dataclass(kw_only=True, slots=True)
class PermissionsFromStrError(PermissionsError):
    text: str

    @override
    def __str__(self) -> str:
        return f"Invalid string for permissions; got {self.text!r}"


__all__ = [
    "Permissions",
    "PermissionsError",
    "PermissionsFromIntError",
    "PermissionsFromStrError",
]
