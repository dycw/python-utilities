from __future__ import annotations

from os import getenv
from typing import TypedDict
from typing import cast

from hypothesis import Verbosity
from hypothesis import settings


class Kwargs(TypedDict, total=False):
    deadline: None
    print_blob: bool
    report_multiple_bugs: bool


kwargs = cast(
    Kwargs,
    {"deadline": None, "print_blob": True, "report_multiple_bugs": False},
)
settings.register_profile("default", max_examples=100, **kwargs)
settings.register_profile("dev", max_examples=10, **kwargs)
settings.register_profile("ci", max_examples=1000, **kwargs)
settings.register_profile(
    "debug", max_examples=10, verbosity=Verbosity.verbose, **kwargs
)
settings.load_profile(getenv("HYPOTHESIS_PROFILE", "default"))
