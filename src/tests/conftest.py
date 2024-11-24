from __future__ import annotations

from os import environ

from pytest import mark

from utilities.platform import IS_NOT_LINUX, IS_WINDOWS

FLAKY = mark.flaky(reruns=5, reruns_delay=1)
IS_CI = "CI" in environ
SKIPIF_CI = mark.skipif(IS_CI, reason="Skipped for CI")
IS_CI_AND_WINDOWS = IS_CI and IS_WINDOWS
SKIPIF_CI_AND_WINDOWS = mark.skipif(IS_CI_AND_WINDOWS, reason="Skipped for CI/Windows")
SKIPIF_CI_AND_NOT_LINUX = mark.skipif(
    IS_CI and IS_NOT_LINUX, reason="Skipped for CI/non-Linux"
)


# hypothesis


try:
    from utilities.hypothesis import setup_hypothesis_profiles
except ModuleNotFoundError:
    pass
else:
    setup_hypothesis_profiles()
