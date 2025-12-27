from __future__ import annotations

from dataclasses import dataclass
from re import search
from typing import Literal, Self, override

from utilities.dataclasses import replace_non_sentinel
from utilities.sentinel import Sentinel, sentinel


@dataclass(order=True, unsafe_hash=True, kw_only=True, slots=True)
class Permissions:
    user_read: bool = False
    user_write: bool = False
    user_execute: bool = False
    group_read: bool = False
    group_write: bool = False
    group_execute: bool = False
    other_read: bool = False
    other_write: bool = False
    other_execute: bool = False

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
                read=self.other_read,
                write=self.other_write,
                execute=self.other_execute,
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
                read=self.other_read, write=self.other_write, execute=self.other_execute
            )
        )

    def _to_int_part(
        self, *, read: bool = False, write: bool = False, execute: bool = False
    ) -> Literal[0, 1, 2, 3, 4, 5, 6, 7]:
        return (4 if read else 0) + (2 if write else 0) + (1 if execute else 0)

    @override
    def __str__(self) -> str:
        return repr(self)

    @classmethod
    def from_int(cls, n: int, /) -> Self:
        as_str = str(n)
        if not search(r"^[0-7]{3}$", as_str):
            raise PermissionsFromIntError(n=n)
        user_read, user_write, user_execute = cls._from_int_part(n // 100)
        group_read, group_write, group_execute = cls._from_int_part((n // 10) % 10)
        other_read, other_write, other_execute = cls._from_int_part(n % 10)
        return cls(
            user_read=user_read,
            user_write=user_write,
            user_execute=user_execute,
            group_read=group_read,
            group_write=group_write,
            group_execute=group_execute,
            other_read=other_read,
            other_write=other_write,
            other_execute=other_execute,
        )

    @classmethod
    def _from_int_part(cls, n: int, /) -> tuple[bool, bool, bool]:
        return bool(4 & n), bool(2 & n), bool(1 & n)

    def replace(
        self,
        *,
        user_read: bool | Sentinel = sentinel,
        user_write: bool | Sentinel = sentinel,
        user_execute: bool | Sentinel = sentinel,
        group_read: bool | Sentinel = sentinel,
        group_write: bool | Sentinel = sentinel,
        group_execute: bool | Sentinel = sentinel,
        other_read: bool | Sentinel = sentinel,
        other_write: bool | Sentinel = sentinel,
        other_execute: bool | Sentinel = sentinel,
    ) -> Self:
        return replace_non_sentinel(
            self,
            user_read=user_read,
            user_write=user_write,
            user_execute=user_execute,
            group_read=group_read,
            group_write=group_write,
            group_execute=group_execute,
            other_read=other_read,
            other_write=other_write,
            other_execute=other_execute,
        )


@dataclass(kw_only=True, slots=True)
class PermissionsError(Exception): ...


@dataclass(kw_only=True, slots=True)
class PermissionsFromIntError(PermissionsError):
    n: int

    @override
    def __str__(self) -> str:
        return f"Invalid integer for permissions; got {self.n}"


__all__ = ["Permissions", "PermissionsError", "PermissionsFromIntError"]
