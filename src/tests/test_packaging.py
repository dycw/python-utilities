from __future__ import annotations

from pytest import mark, param, raises

from utilities.iterables import one
from utilities.packaging import Requirement, _CustomRequirement, _CustomSpecifierSet


class TestCustomRequirement:
    def test_main(self) -> None:
        req = _CustomRequirement("package>=1.2.3, <2")
        assert isinstance(req.specifier, _CustomSpecifierSet)

    def test_drop_existing(self) -> None:
        req = _CustomRequirement("package>=1.2.3, <2").drop("<")
        expected = _CustomRequirement("package>=1.2.3")
        assert req == expected

    def test_drop_missing(self) -> None:
        req = _CustomRequirement("package>=1.2.3")
        with raises(KeyError):
            _ = req.drop("<")

    def test_replace_existing(self) -> None:
        req = _CustomRequirement("package>=1.2.3, <1.3").replace(">=", "1.2.4")
        expected = _CustomRequirement("package>=1.2.4, <1.3")
        assert req == expected

    def test_replace_new(self) -> None:
        req = _CustomRequirement("package>=1.2.3").replace("<", "1.3")
        expected = _CustomRequirement("package>=1.2.3, <1.3")
        assert req == expected

    def test_replace_remove(self) -> None:
        req = _CustomRequirement("package>=1.2.3, <1.3").replace("<", None)
        expected = _CustomRequirement("package>=1.2.3")
        assert req == expected

    @mark.parametrize(
        ("req", "expected"),
        [
            param("package", "package"),
            param("package>=1.2.3", "package>=1.2.3"),
            param("package<1.3", "package<1.3"),
            param("package>=1.2.3,<1.3", "package>=1.2.3, <1.3"),
            param("package<1.3,>=1.2.3", "package>=1.2.3, <1.3"),
        ],
    )
    def test_str(self, *, req: str, expected: str) -> None:
        requirement = _CustomRequirement(req)
        assert str(requirement) == expected


class TestRequirement:
    def test_drop_existing(self) -> None:
        req = Requirement("package>=1.2.3, <1.3").drop("<")
        expected = Requirement("package>=1.2.3")
        assert req == expected

    def test_drop_missing(self) -> None:
        req = Requirement("package>=1.2.3")
        with raises(KeyError):
            _ = req.drop("<")

    def test_extra(self) -> None:
        extra = "extra"
        req = Requirement(f"package[{extra}]")
        assert req.extras == [extra]

    def test_get(self) -> None:
        req = Requirement("package>=1.2.3, <1.3")
        assert req.get(">=") == "1.2.3"
        assert req.get("<") == "1.3"
        assert req.get(">") is None

    def test_get_item(self) -> None:
        req = Requirement("package>=1.2.3, <1.3")
        assert req[">="] == "1.2.3"
        assert req["<"] == "1.3"
        with raises(KeyError):
            _ = req[">"]

    def test_marker(self) -> None:
        req = Requirement('package; python_version >= "3"')
        assert req.marker is not None
        variable, op, value = one(req.marker)
        assert str(variable) == "python_version"
        assert str(op) == ">="
        assert str(value) == "3"

    def test_name(self) -> None:
        req = Requirement("package")
        assert req.name == "package"

    def test_replace_existing(self) -> None:
        req = Requirement("package>=1.2.3, <1.3").replace(">=", "1.2.4")
        expected = Requirement("package>=1.2.4, <1.3")
        assert req == expected

    def test_replace_new(self) -> None:
        req = Requirement("package>=1.2.3").replace("<", "1.3")
        expected = Requirement("package>=1.2.3, <1.3")
        assert req == expected

    def test_replace_remove(self) -> None:
        req = Requirement("package>=1.2.3, <1.3").replace("<", None)
        expected = Requirement("package>=1.2.3")
        assert req == expected

    def test_specifier(self) -> None:
        req = Requirement("package>=1.2.3, <1.3")
        assert req.specifier == ">=1.2.3,<1.3"

    def test_specifier_set(self) -> None:
        req = Requirement("package>=1.2.3, <1.3")
        assert req.specifier_set == ">=1.2.3,<1.3"

    @mark.parametrize(
        ("req", "expected"),
        [
            param("package", "package"),
            param("package>=1.2.3", "package>=1.2.3"),
            param("package[extra]>=1.2.3", "package[extra]>=1.2.3"),
            param("package[extra1,extra2]>=1.2.3", "package[extra1,extra2]>=1.2.3"),
            param("package@https://www.github.com", "package@ https://www.github.com"),
            param("package;python_version>='3'", 'package; python_version >= "3"'),
            param(
                "package[extra1,extra2]@https://www.github.com>=1.2.3,<1.3;python_version>='3'",
                "package[extra1,extra2]@ https://www.github.com>=1.2.3,<1.3;python_version>='3'",
            ),
        ],
    )
    def test_str(self, *, req: str, expected: str) -> None:
        requirement = Requirement(req)
        assert str(requirement) == expected

    def test_url(self) -> None:
        url = "https://www.github.com"
        req = Requirement(f"package@{url}")
        assert req.url == url


class TestCustomSpecifierSet:
    def test_drop_existing(self) -> None:
        set_ = _CustomSpecifierSet(">=1.2.3, <1.3").drop("<")
        assert set_[">="] == "1.2.3"
        with raises(KeyError):
            set_["<"]

    def test_drop_missing(self) -> None:
        set_ = _CustomSpecifierSet(">=1.2.3")
        with raises(KeyError):
            _ = set_.drop("<")

    def test_get(self) -> None:
        set_ = _CustomSpecifierSet(">=1.2.3, <1.3")
        assert set_.get(">=") == "1.2.3"
        assert set_.get("<") == "1.3"
        assert set_.get(">") is None

    def test_get_item(self) -> None:
        set_ = _CustomSpecifierSet(">=1.2.3, <1.3")
        assert set_[">="] == "1.2.3"
        assert set_["<"] == "1.3"
        with raises(KeyError):
            _ = set_[">"]

    def test_replace_existing(self) -> None:
        set_ = _CustomSpecifierSet(">=1.2.3, <1.3").replace(">=", "1.2.4")
        expected = _CustomSpecifierSet(">=1.2.4, <1.3")
        assert set_ == expected

    def test_replace_new(self) -> None:
        set_ = _CustomSpecifierSet(">=1.2.3").replace("<", "1.3")
        expected = _CustomSpecifierSet(">=1.2.3, <1.3")
        assert set_ == expected

    def test_replace_remove(self) -> None:
        set_ = _CustomSpecifierSet(">=1.2.3, <1.3").replace("<", None)
        expected = _CustomSpecifierSet(">=1.2.3")
        assert set_ == expected

    @mark.parametrize(
        ("specifier", "expected"),
        [
            param("", ""),
            param(">=1.2.3", ">=1.2.3"),
            param("<1.3", "<1.3"),
            param(">=1.2.3,<1.3", ">=1.2.3, <1.3"),
            param("<1.3,>=1.2.3", ">=1.2.3, <1.3"),
        ],
    )
    def test_str(self, *, specifier: str, expected: str) -> None:
        set_ = _CustomSpecifierSet(specifier)
        assert str(set_) == expected
