from __future__ import annotations

import datetime as dt
from asyncio import Event
from collections.abc import Awaitable, Callable, Coroutine, Hashable, Iterable, Mapping
from enum import Enum
from logging import Logger
from pathlib import Path
from random import Random
from types import TracebackType
from typing import (
    Any,
    ClassVar,
    Literal,
    Protocol,
    TypeVar,
    overload,
    runtime_checkable,
)
from zoneinfo import ZoneInfo

_T_co = TypeVar("_T_co", covariant=True)
_T_contra = TypeVar("_T_contra", contravariant=True)


# basic
type OpenMode = Literal[
    "r",
    "w",
    "x",
    "a",
    "rb",
    "wb",
    "xb",
    "ab",
    "r+",
    "w+",
    "x+",
    "a+",
    "rb+",
    "wb+",
    "xb+",
    "ab+",
    "r+b",
    "w+b",
    "x+b",
    "a+b",
]
type MaybeCallable[_T] = _T | Callable[[], _T]
type MaybeStr[_T] = _T | str
type MaybeType[_T] = _T | type[_T]
type StrMapping = Mapping[str, Any]
type StrStrMapping = Mapping[str, str]
type TypeLike[_T] = type[_T] | tuple[type[_T], ...]
type TupleOrStrMapping = tuple[Any, ...] | StrMapping


# asyncio
type Coroutine1[_T] = Coroutine[Any, Any, _T]
type MaybeAwaitable[_T] = _T | Awaitable[_T]
type MaybeCallableEvent = MaybeCallable[Event]
type MaybeCoroutine1[_T] = _T | Coroutine1[_T]


# callable
TCallable = TypeVar("TCallable", bound=Callable[..., Any])
TCallable1 = TypeVar("TCallable1", bound=Callable[..., Any])
TCallable2 = TypeVar("TCallable2", bound=Callable[..., Any])
TCallableMaybeCoroutine1None = TypeVar(
    "TCallableMaybeCoroutine1None", bound=Callable[..., MaybeCoroutine1[None]]
)


# concurrent
type Parallelism = Literal["processes", "threads"]


# dataclasses
@runtime_checkable
class Dataclass(Protocol):
    """Protocol for `dataclass` classes."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


TDataclass = TypeVar("TDataclass", bound=Dataclass)


# datetime
type DateLike = MaybeStr[dt.date]
type DateTimeLike = MaybeStr[dt.datetime]
type DateOrDateTime = dt.date | dt.datetime
type Duration = Number | dt.timedelta
type DurationLike = MaybeStr[Duration]
type MaybeCallableDate = MaybeCallable[dt.date]
type MaybeCallableDateTime = MaybeCallable[dt.datetime]
type TimeLike = MaybeStr[dt.time]
type TimeDeltaLike = MaybeStr[dt.timedelta]
type WeekDay = Literal["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


# enum
type EnumLike[_TEnum: Enum] = MaybeStr[_TEnum]
TEnum = TypeVar("TEnum", bound=Enum)


# exceptions
TBaseException = TypeVar("TBaseException", bound=BaseException)


# hashable
THashable = TypeVar("THashable", bound=Hashable)
THashable1 = TypeVar("THashable1", bound=Hashable)
THashable2 = TypeVar("THashable2", bound=Hashable)


# iterables
type MaybeIterable[_T] = _T | Iterable[_T]
type IterableHashable[_THashable: Hashable] = (
    tuple[_THashable, ...] | frozenset[_THashable]
)
type MaybeIterableHashable[_THashable: Hashable] = (
    _THashable | IterableHashable[_THashable]
)


# logging
type LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
type LoggerOrName = MaybeStr[Logger]


# math
type Number = int | float
type RoundMode = Literal[
    "standard",
    "floor",
    "ceil",
    "toward-zero",
    "away-zero",
    "standard-tie-floor",
    "standard-tie-ceil",
    "standard-tie-toward-zero",
    "standard-tie-away-zero",
]
type Sign = Literal[-1, 0, 1]


# operator


@runtime_checkable
class SupportsAbs(Protocol[_T_co]):
    def __abs__(self) -> _T_co: ...  # pragma: no cover


TSupportsAbs = TypeVar("TSupportsAbs", bound=SupportsAbs)


@runtime_checkable
class SupportsAdd(Protocol[_T_contra, _T_co]):
    def __add__(self, x: _T_contra, /) -> _T_co: ...  # pragma: no cover


TSupportsAdd = TypeVar("TSupportsAdd", bound=SupportsAdd)


@runtime_checkable
class SupportsBytes(Protocol):
    def __bytes__(self) -> bytes: ...  # pragma: no cover


TSupportsBytes = TypeVar("TSupportsBytes", bound=SupportsBytes)


@runtime_checkable
class SupportsComplex(Protocol):
    def __complex__(self) -> complex: ...  # pragma: no cover


TSupportsComplex = TypeVar("TSupportsComplex", bound=SupportsComplex)


@runtime_checkable
class SupportsFloat(Protocol):
    def __float__(self) -> float: ...  # pragma: no cover


TSupportsFloat = TypeVar("TSupportsFloat", bound=SupportsFloat)


@runtime_checkable
class SupportsGT(Protocol[_T_contra]):
    def __gt__(self, other: _T_contra, /) -> bool: ...  # pragma: no cover


TSupportsGT = TypeVar("TSupportsGT", bound=SupportsGT)


@runtime_checkable
class SupportsIndex(Protocol):
    def __index__(self) -> int: ...  # pragma: no cover


TSupportsIndex = TypeVar("TSupportsIndex", bound=SupportsIndex)


@runtime_checkable
class SupportsInt(Protocol):
    def __int__(self) -> int: ...  # pragma: no cover


TSupportsInt = TypeVar("TSupportsInt", bound=SupportsInt)


@runtime_checkable
class SupportsLT(Protocol[_T_contra]):
    def __lt__(self, other: _T_contra, /) -> bool: ...  # pragma: no cover


TSupportsLT = TypeVar("TSupportsLT", bound=SupportsLT)


SupportsRichComparison = SupportsLT[Any] | SupportsGT[Any]
TSupportsRichComparison = TypeVar(
    "TSupportsRichComparison", bound=SupportsRichComparison
)


@runtime_checkable
class SupportsRound(Protocol[_T_co]):
    @overload
    def __round__(self) -> int: ...
    @overload
    def __round__(self, ndigits: int, /) -> _T_co: ...


# parse
type ParseObjectExtra = Mapping[Any, Callable[[str], Any]]
type SerializeObjectExtra = Mapping[Any, Callable[[Any], str]]


# pathlib
type PathLike = MaybeStr[Path]
type PathLikeOrCallable = PathLike | Callable[[], PathLike]


# random
type Seed = int | float | str | bytes | bytearray | Random


# traceback
type ExcInfo = tuple[type[BaseException], BaseException, TracebackType]
type OptExcInfo = ExcInfo | tuple[None, None, None]


# zoneinfo
# fmt: off
type TimeZone = Literal[
    "Africa/Abidjan", "Africa/Accra", "Africa/Addis_Ababa", "Africa/Algiers", "Africa/Asmara", "Africa/Asmera", "Africa/Bamako", "Africa/Bangui", "Africa/Banjul", "Africa/Bissau", "Africa/Blantyre", "Africa/Brazzaville", "Africa/Bujumbura", "Africa/Cairo", "Africa/Casablanca", "Africa/Ceuta", "Africa/Conakry", "Africa/Dakar", "Africa/Dar_es_Salaam", "Africa/Djibouti", "Africa/Douala", "Africa/El_Aaiun", "Africa/Freetown", "Africa/Gaborone", "Africa/Harare", "Africa/Johannesburg", "Africa/Juba", "Africa/Kampala", "Africa/Khartoum", "Africa/Kigali", "Africa/Kinshasa", "Africa/Lagos", "Africa/Libreville", "Africa/Lome", "Africa/Luanda", "Africa/Lubumbashi", "Africa/Lusaka", "Africa/Malabo", "Africa/Maputo", "Africa/Maseru", "Africa/Mbabane", "Africa/Mogadishu", "Africa/Monrovia", "Africa/Nairobi", "Africa/Ndjamena", "Africa/Niamey", "Africa/Nouakchott", "Africa/Ouagadougou", "Africa/Porto-Novo", "Africa/Sao_Tome", "Africa/Timbuktu", "Africa/Tripoli", "Africa/Tunis", "Africa/Windhoek", "America/Adak", "America/Anchorage", "America/Anguilla", "America/Antigua", "America/Araguaina", "America/Argentina/Buenos_Aires", "America/Argentina/Catamarca", "America/Argentina/ComodRivadavia", "America/Argentina/Cordoba", "America/Argentina/Jujuy", "America/Argentina/La_Rioja", "America/Argentina/Mendoza", "America/Argentina/Rio_Gallegos", "America/Argentina/Salta", "America/Argentina/San_Juan", "America/Argentina/San_Luis", "America/Argentina/Tucuman", "America/Argentina/Ushuaia", "America/Aruba", "America/Asuncion", "America/Atikokan", "America/Atka", "America/Bahia", "America/Bahia_Banderas", "America/Barbados", "America/Belem", "America/Belize", "America/Blanc-Sablon", "America/Boa_Vista", "America/Bogota", "America/Boise", "America/Buenos_Aires", "America/Cambridge_Bay", "America/Campo_Grande", "America/Cancun", "America/Caracas", "America/Catamarca", "America/Cayenne", "America/Cayman", "America/Chicago", "America/Chihuahua", "America/Ciudad_Juarez", "America/Coral_Harbour", "America/Cordoba", "America/Costa_Rica", "America/Coyhaique", "America/Creston", "America/Cuiaba", "America/Curacao", "America/Danmarkshavn", "America/Dawson", "America/Dawson_Creek", "America/Denver", "America/Detroit", "America/Dominica", "America/Edmonton", "America/Eirunepe", "America/El_Salvador", "America/Ensenada", "America/Fort_Nelson", "America/Fort_Wayne", "America/Fortaleza", "America/Glace_Bay", "America/Godthab", "America/Goose_Bay", "America/Grand_Turk", "America/Grenada", "America/Guadeloupe", "America/Guatemala", "America/Guayaquil", "America/Guyana", "America/Halifax", "America/Havana", "America/Hermosillo", "America/Indiana/Indianapolis", "America/Indiana/Knox", "America/Indiana/Marengo", "America/Indiana/Petersburg", "America/Indiana/Tell_City", "America/Indiana/Vevay", "America/Indiana/Vincennes", "America/Indiana/Winamac", "America/Indianapolis", "America/Inuvik", "America/Iqaluit", "America/Jamaica", "America/Jujuy", "America/Juneau", "America/Kentucky/Louisville", "America/Kentucky/Monticello", "America/Knox_IN", "America/Kralendijk", "America/La_Paz", "America/Lima", "America/Los_Angeles", "America/Louisville", "America/Lower_Princes", "America/Maceio", "America/Managua", "America/Manaus", "America/Marigot", "America/Martinique", "America/Matamoros", "America/Mazatlan", "America/Mendoza", "America/Menominee", "America/Merida", "America/Metlakatla", "America/Mexico_City", "America/Miquelon", "America/Moncton", "America/Monterrey", "America/Montevideo", "America/Montreal", "America/Montserrat", "America/Nassau", "America/New_York", "America/Nipigon", "America/Nome", "America/Noronha", "America/North_Dakota/Beulah", "America/North_Dakota/Center", "America/North_Dakota/New_Salem", "America/Nuuk", "America/Ojinaga", "America/Panama", "America/Pangnirtung", "America/Paramaribo", "America/Phoenix", "America/Port-au-Prince", "America/Port_of_Spain", "America/Porto_Acre", "America/Porto_Velho", "America/Puerto_Rico", "America/Punta_Arenas", "America/Rainy_River", "America/Rankin_Inlet", "America/Recife", "America/Regina", "America/Resolute", "America/Rio_Branco", "America/Rosario", "America/Santa_Isabel", "America/Santarem", "America/Santiago", "America/Santo_Domingo", "America/Sao_Paulo", "America/Scoresbysund", "America/Shiprock", "America/Sitka", "America/St_Barthelemy", "America/St_Johns", "America/St_Kitts", "America/St_Lucia", "America/St_Thomas", "America/St_Vincent", "America/Swift_Current", "America/Tegucigalpa", "America/Thule", "America/Thunder_Bay", "America/Tijuana", "America/Toronto", "America/Tortola", "America/Vancouver", "America/Virgin", "America/Whitehorse", "America/Winnipeg", "America/Yakutat", "America/Yellowknife", "Antarctica/Casey", "Antarctica/Davis", "Antarctica/DumontDUrville", "Antarctica/Macquarie", "Antarctica/Mawson", "Antarctica/McMurdo", "Antarctica/Palmer", "Antarctica/Rothera", "Antarctica/South_Pole", "Antarctica/Syowa", "Antarctica/Troll", "Antarctica/Vostok", "Arctic/Longyearbyen", "Asia/Aden", "Asia/Almaty", "Asia/Amman", "Asia/Anadyr", "Asia/Aqtau", "Asia/Aqtobe", "Asia/Ashgabat", "Asia/Ashkhabad", "Asia/Atyrau", "Asia/Baghdad", "Asia/Bahrain", "Asia/Baku", "Asia/Bangkok", "Asia/Barnaul", "Asia/Beirut", "Asia/Bishkek", "Asia/Brunei", "Asia/Calcutta", "Asia/Chita", "Asia/Choibalsan", "Asia/Chongqing", "Asia/Chungking", "Asia/Colombo", "Asia/Dacca", "Asia/Damascus", "Asia/Dhaka", "Asia/Dili", "Asia/Dubai", "Asia/Dushanbe", "Asia/Famagusta", "Asia/Gaza", "Asia/Harbin", "Asia/Hebron", "Asia/Ho_Chi_Minh", "Asia/Hong_Kong", "Asia/Hovd", "Asia/Irkutsk", "Asia/Istanbul", "Asia/Jakarta", "Asia/Jayapura", "Asia/Jerusalem", "Asia/Kabul", "Asia/Kamchatka", "Asia/Karachi", "Asia/Kashgar", "Asia/Kathmandu", "Asia/Katmandu", "Asia/Khandyga", "Asia/Kolkata", "Asia/Krasnoyarsk", "Asia/Kuala_Lumpur", "Asia/Kuching", "Asia/Kuwait", "Asia/Macao", "Asia/Macau", "Asia/Magadan", "Asia/Makassar", "Asia/Manila", "Asia/Muscat", "Asia/Nicosia", "Asia/Novokuznetsk", "Asia/Novosibirsk", "Asia/Omsk", "Asia/Oral", "Asia/Phnom_Penh", "Asia/Pontianak", "Asia/Pyongyang", "Asia/Qatar", "Asia/Qostanay", "Asia/Qyzylorda", "Asia/Rangoon", "Asia/Riyadh", "Asia/Saigon", "Asia/Sakhalin", "Asia/Samarkand", "Asia/Seoul", "Asia/Shanghai", "Asia/Singapore", "Asia/Srednekolymsk", "Asia/Taipei", "Asia/Tashkent", "Asia/Tbilisi", "Asia/Tehran", "Asia/Tel_Aviv", "Asia/Thimbu", "Asia/Thimphu", "Asia/Tokyo", "Asia/Tomsk", "Asia/Ujung_Pandang", "Asia/Ulaanbaatar", "Asia/Ulan_Bator", "Asia/Urumqi", "Asia/Ust-Nera", "Asia/Vientiane", "Asia/Vladivostok", "Asia/Yakutsk", "Asia/Yangon", "Asia/Yekaterinburg", "Asia/Yerevan", "Atlantic/Azores", "Atlantic/Bermuda", "Atlantic/Canary", "Atlantic/Cape_Verde", "Atlantic/Faeroe", "Atlantic/Faroe", "Atlantic/Jan_Mayen", "Atlantic/Madeira", "Atlantic/Reykjavik", "Atlantic/South_Georgia", "Atlantic/St_Helena", "Atlantic/Stanley", "Australia/ACT", "Australia/Adelaide", "Australia/Brisbane", "Australia/Broken_Hill", "Australia/Canberra", "Australia/Currie", "Australia/Darwin", "Australia/Eucla", "Australia/Hobart", "Australia/LHI", "Australia/Lindeman", "Australia/Lord_Howe", "Australia/Melbourne", "Australia/NSW", "Australia/North", "Australia/Perth", "Australia/Queensland", "Australia/South", "Australia/Sydney", "Australia/Tasmania", "Australia/Victoria", "Australia/West", "Australia/Yancowinna", "Brazil/Acre", "Brazil/DeNoronha", "Brazil/East", "Brazil/West", "CET", "CST6CDT", "Canada/Atlantic", "Canada/Central", "Canada/Eastern", "Canada/Mountain", "Canada/Newfoundland", "Canada/Pacific", "Canada/Saskatchewan", "Canada/Yukon", "Chile/Continental", "Chile/EasterIsland", "Cuba", "EET", "EST", "EST5EDT", "Egypt", "Eire", "Etc/GMT", "Etc/GMT+0", "Etc/GMT+1", "Etc/GMT+10", "Etc/GMT+11", "Etc/GMT+12", "Etc/GMT+2", "Etc/GMT+3", "Etc/GMT+4", "Etc/GMT+5", "Etc/GMT+6", "Etc/GMT+7", "Etc/GMT+8", "Etc/GMT+9", "Etc/GMT-0", "Etc/GMT-1", "Etc/GMT-10", "Etc/GMT-11", "Etc/GMT-12", "Etc/GMT-13", "Etc/GMT-14", "Etc/GMT-2", "Etc/GMT-3", "Etc/GMT-4", "Etc/GMT-5", "Etc/GMT-6", "Etc/GMT-7", "Etc/GMT-8", "Etc/GMT-9", "Etc/GMT0", "Etc/Greenwich", "Etc/UCT", "Etc/UTC", "Etc/Universal", "Etc/Zulu", "Europe/Amsterdam", "Europe/Andorra", "Europe/Astrakhan", "Europe/Athens", "Europe/Belfast", "Europe/Belgrade", "Europe/Berlin", "Europe/Bratislava", "Europe/Brussels", "Europe/Bucharest", "Europe/Budapest", "Europe/Busingen", "Europe/Chisinau", "Europe/Copenhagen", "Europe/Dublin", "Europe/Gibraltar", "Europe/Guernsey", "Europe/Helsinki", "Europe/Isle_of_Man", "Europe/Istanbul", "Europe/Jersey", "Europe/Kaliningrad", "Europe/Kiev", "Europe/Kirov", "Europe/Kyiv", "Europe/Lisbon", "Europe/Ljubljana", "Europe/London", "Europe/Luxembourg", "Europe/Madrid", "Europe/Malta", "Europe/Mariehamn", "Europe/Minsk", "Europe/Monaco", "Europe/Moscow", "Europe/Nicosia", "Europe/Oslo", "Europe/Paris", "Europe/Podgorica", "Europe/Prague", "Europe/Riga", "Europe/Rome", "Europe/Samara", "Europe/San_Marino", "Europe/Sarajevo", "Europe/Saratov", "Europe/Simferopol", "Europe/Skopje", "Europe/Sofia", "Europe/Stockholm", "Europe/Tallinn", "Europe/Tirane", "Europe/Tiraspol", "Europe/Ulyanovsk", "Europe/Uzhgorod", "Europe/Vaduz", "Europe/Vatican", "Europe/Vienna", "Europe/Vilnius", "Europe/Volgograd", "Europe/Warsaw", "Europe/Zagreb", "Europe/Zaporozhye", "Europe/Zurich", "Factory", "GB", "GB-Eire", "GMT", "GMT+0", "GMT-0", "GMT0", "Greenwich", "HST", "Hongkong", "Iceland", "Indian/Antananarivo", "Indian/Chagos", "Indian/Christmas", "Indian/Cocos", "Indian/Comoro", "Indian/Kerguelen", "Indian/Mahe", "Indian/Maldives", "Indian/Mauritius", "Indian/Mayotte", "Indian/Reunion", "Iran", "Israel", "Jamaica", "Japan", "Kwajalein", "Libya", "MET", "MST", "MST7MDT", "Mexico/BajaNorte", "Mexico/BajaSur", "Mexico/General", "NZ", "NZ-CHAT", "Navajo", "PRC", "PST8PDT", "Pacific/Apia", "Pacific/Auckland", "Pacific/Bougainville", "Pacific/Chatham", "Pacific/Chuuk", "Pacific/Easter", "Pacific/Efate", "Pacific/Enderbury", "Pacific/Fakaofo", "Pacific/Fiji", "Pacific/Funafuti", "Pacific/Galapagos", "Pacific/Gambier", "Pacific/Guadalcanal", "Pacific/Guam", "Pacific/Honolulu", "Pacific/Johnston", "Pacific/Kanton", "Pacific/Kiritimati", "Pacific/Kosrae", "Pacific/Kwajalein", "Pacific/Majuro", "Pacific/Marquesas", "Pacific/Midway", "Pacific/Nauru", "Pacific/Niue", "Pacific/Norfolk", "Pacific/Noumea", "Pacific/Pago_Pago", "Pacific/Palau", "Pacific/Pitcairn", "Pacific/Pohnpei", "Pacific/Ponape", "Pacific/Port_Moresby", "Pacific/Rarotonga", "Pacific/Saipan", "Pacific/Samoa", "Pacific/Tahiti", "Pacific/Tarawa", "Pacific/Tongatapu", "Pacific/Truk", "Pacific/Wake", "Pacific/Wallis", "Pacific/Yap", "Poland", "Portugal", "ROC", "ROK", "Singapore", "Turkey", "UCT", "US/Alaska", "US/Aleutian", "US/Arizona", "US/Central", "US/East-Indiana", "US/Eastern", "US/Hawaii", "US/Indiana-Starke", "US/Michigan", "US/Mountain", "US/Pacific", "US/Samoa", "UTC", "Universal", "W-SU", "WET", "Zulu"
]
# fmt: on
type TimeZoneLike = ZoneInfo | Literal["local"] | TimeZone | dt.tzinfo | dt.datetime


__all__ = [
    "Coroutine1",
    "Dataclass",
    "DateLike",
    "DateOrDateTime",
    "DateTimeLike",
    "Duration",
    "DurationLike",
    "EnumLike",
    "ExcInfo",
    "IterableHashable",
    "LogLevel",
    "LoggerOrName",
    "MaybeAwaitable",
    "MaybeCallable",
    "MaybeCallableDate",
    "MaybeCallableDateTime",
    "MaybeCallableEvent",
    "MaybeCoroutine1",
    "MaybeIterable",
    "MaybeIterableHashable",
    "MaybeStr",
    "MaybeType",
    "Number",
    "OpenMode",
    "OptExcInfo",
    "Parallelism",
    "ParseObjectExtra",
    "PathLike",
    "PathLikeOrCallable",
    "RoundMode",
    "Seed",
    "SerializeObjectExtra",
    "Sign",
    "StrMapping",
    "StrStrMapping",
    "SupportsAbs",
    "SupportsAdd",
    "SupportsBytes",
    "SupportsComplex",
    "SupportsFloat",
    "SupportsGT",
    "SupportsInt",
    "SupportsInt",
    "SupportsLT",
    "SupportsRichComparison",
    "SupportsRound",
    "TBaseException",
    "TCallable",
    "TCallable1",
    "TCallable2",
    "TCallableMaybeCoroutine1None",
    "TDataclass",
    "TEnum",
    "THashable",
    "THashable1",
    "THashable2",
    "TSupportsAbs",
    "TSupportsAdd",
    "TSupportsBytes",
    "TSupportsComplex",
    "TSupportsGT",
    "TSupportsIndex",
    "TSupportsInt",
    "TSupportsLT",
    "TSupportsRichComparison",
    "TimeDeltaLike",
    "TimeLike",
    "TimeZone",
    "TimeZoneLike",
    "TupleOrStrMapping",
    "TypeLike",
    "WeekDay",
]
