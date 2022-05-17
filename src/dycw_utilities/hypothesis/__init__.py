from collections.abc import Iterator
from contextlib import contextmanager
from os import getenv
from re import search
from typing import Optional

from hypothesis import Verbosity
from hypothesis import assume
from hypothesis import settings
from typeguard import typeguard_ignore

from dycw_utilities.text import ensure_str


@typeguard_ignore
@contextmanager
def assume_does_not_raise(
    *exceptions: type[Exception], match: Optional[str] = None
) -> Iterator[None]:
    """Assume a set of exceptions are not raised. Optionally filter on the
    string representation of the exception.
    """

    try:
        yield
    except exceptions as caught:
        if match is None:
            _ = assume(False)
        else:
            (msg,) = caught.args
            if search(match, ensure_str(msg)):
                _ = assume(False)
            else:
                raise


def setup_hypothesis_profiles() -> None:
    """Set up the hypothesis profiles."""

    kwargs = {
        "deadline": None,
        "print_blob": True,
        "report_multiple_bugs": False,
    }
    settings.register_profile("default", max_examples=100, **kwargs)
    settings.register_profile("dev", max_examples=10, **kwargs)
    settings.register_profile("ci", max_examples=1000, **kwargs)
    settings.register_profile(
        "debug", max_examples=10, verbosity=Verbosity.verbose, **kwargs
    )
    settings.load_profile(getenv("HYPOTHESIS_PROFILE", "default"))
