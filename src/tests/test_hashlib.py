from typing import Any

from pytest import mark, param

from utilities.hashlib import md5_hash


class TestMD5Hash:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(b"", "d41d8cd98f00b204e9800998ecf8427e"),
            param(b"bytes", "4b3a6218bb3e3a7303e8a171a60fcf92"),
            param("", "d41d8cd98f00b204e9800998ecf8427e"),
            param("text", "1cb251ec0d568de6a929b520c4aed8d1"),
            param(None, "7d1e55650014e21b4568ed3e3d1fc531"),
        ],
    )
    def test_main(self, obj: Any, expected: str) -> None:
        assert md5_hash(obj) == expected
