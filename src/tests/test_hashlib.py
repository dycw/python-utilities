from __future__ import annotations

from typing import Any

from hypothesis import given

from tests.test_objects.objects import objects
from utilities.hashlib import md5_hash


class TestMD5Hash:
    @given(obj=objects(parsable=True))
    def test_main(self, *, obj: Any) -> None:
        _ = md5_hash(obj)
