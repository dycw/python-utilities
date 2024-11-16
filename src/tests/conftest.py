from __future__ import annotations

from os import environ

from pytest import mark

from utilities.platform import IS_NOT_LINUX, IS_WINDOWS

FLAKY = mark.flaky(reruns=5, reruns_delay=1)
SKIPIF_CI = mark.skipif("CI" in environ, reason="Skipped for CI")
SKIPIF_CI_AND_WINDOWS = mark.skipif(
    ("CI" in environ) and IS_WINDOWS, reason="Skipped for CI/Windows"
)
SKIPIF_CI_AND_NOT_LINUX = mark.skipif(
    ("CI" in environ) and IS_NOT_LINUX, reason="Skipped for CI/non-Linux"
)


# hypothesis


try:
    from utilities.hypothesis import setup_hypothesis_profiles
except ModuleNotFoundError:
    pass
else:
    setup_hypothesis_profiles()
