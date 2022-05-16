import datetime as dt
from contextlib import suppress
from typing import Any
from typing import Optional

from click import Context
from click import ParamType
from click import Parameter


class Date(ParamType):
    """A date-valued `click` parameter."""

    name = "date"

    def convert(
        self, value: Any, param: Optional[Parameter], ctx: Optional[Context]
    ) -> dt.date:
        with suppress(ValueError):
            return dt.date.fromisoformat(value)
        with suppress(ValueError):
            return dt.datetime.strptime(value, "%Y%m%d").date()
        self.fail(f"Unable to parse {value}", param, ctx)
