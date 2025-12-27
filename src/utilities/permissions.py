from __future__ import annotations

from dataclasses import dataclass
from re import search
from typing import Literal, Self, override

from utilities.dataclasses import replace_non_sentinel
from utilities.functions import ensure_member
from utilities.re import ExtractGroupsError, extract_groups
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

    def __int__(self) -> int:
        return (
            100
            * self._to_int_part(
                read=self.user_read, write=self.user_write, execute=self.user_execute
            )
            + 10
            * self._to_int_part(
                read=self.group_read, write=self.group_write, execute=self.group_execute
            )
            + self._to_int_part(
                read=self.others_read,
                write=self.others_write,
                execute=self.others_execute,
            )
        )

    def _to_int_part(
        self, *, read: bool = False, write: bool = False, execute: bool = False
    ) -> _ZeroToSeven:
        return (4 if read else 0) + (2 if write else 0) + (1 if execute else 0)

    @override
    def __str__(self) -> str:
        return repr(self)

    @classmethod
    def from_int(cls, n: int, /) -> Self:
        try:
            user, group, others = extract_groups(r"^([0-7])([0-7])([0-7])$", str(n))
        except ExtractGroupsError:
            raise PermissionsFromIntError(n=n) from None
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

    @classmethod
    def _from_int(cls, n: _ZeroToSeven, /) -> tuple[bool, bool, bool]:
        return bool(4 & n), bool(2 & n), bool(1 & n)

    @classmethod
    def from_str(cls, text: str, /) -> Self:
        try:
            user, group, others = extract_groups(
                r"^u=([r?w?x?]),g=([r?w?x?]),o=([r?w?x?])$", text
            )
        except ExtractGroupsError:
            raise PermissionsFromStrError(text=text) from None
        user_read, user_write, user_execute = cls._from_str(user)
        group_read, group_write, group_execute = cls._from_str(group)
        others_read, others_write, others_execute = cls._from_str(others)
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
    def _from_str(cls, text: str, /) -> tuple[bool, bool, bool]:
        read, write, execute = extract_groups("^(r?)(w?)(x?)$", text)
        return read != "", write != "", execute != ""

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
        return f"Invalid string for permissions; got {self.text}"


__all__ = [
    "Permissions",
    "PermissionsError",
    "PermissionsFromIntError",
    "PermissionsFromStrError",
]
