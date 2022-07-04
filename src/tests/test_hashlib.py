from utilities.hashlib import md5_hash


class TestMD5Hash:
    def test_main(self) -> None:
        assert md5_hash("") == "d41d8cd98f00b204e9800998ecf8427e"
