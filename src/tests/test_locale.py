from __future__ import annotations

from locale import LC_CTYPE, LC_TIME, setlocale

from pytest import mark, param

from utilities.locale import atof, get_locale_for_platform, override_locale
from utilities.platform import SYSTEM, System


class TestAToF:
    @mark.parametrize(
        ("text", "expected"),
        [
            param("0.00", 0.0),
            param("1.23", 1.23),
            param("12.34", 12.34),
            param("123.45", 123.45),
            param("1,234.56", 1234.56),
        ],
    )
    def test_main(self, *, text: str, expected: float) -> None:
        plat_locale = get_locale_for_platform("en_US")
        assert atof(text, locale=plat_locale) == expected


class TestGetLocaleForPlatform:
    def test_main(self) -> None:
        plat_locale = get_locale_for_platform("en_US")
        _ = setlocale(LC_CTYPE, locale=plat_locale)


class TestOverrideLocale:
    @mark.skipif(condition=SYSTEM is System.windows, reason="non-Windows only")
    def test_main(self) -> None:
        plat_locale = get_locale_for_platform("en_US")
        with override_locale(locale=plat_locale):
            assert LC_TIME in {2, 5}
