from __future__ import annotations

from pathlib import Path
from smtplib import SMTPServerDisconnected

from pytest import raises

from utilities.email import send_email


class TestSendEmail:
    def test_main(self) -> None:
        with raises(SMTPServerDisconnected):
            send_email("no-reply@test.com", ["user@test.com"])

    def test_subject(self) -> None:
        with raises(SMTPServerDisconnected):
            send_email("no-reply@test.com", ["user@test.com"], subject="Subject")

    def test_contents_str(self) -> None:
        with raises(SMTPServerDisconnected):
            send_email(
                "no-reply@test.com",
                ["user@test.com"],
                subject="Subject",
                contents="contents",
            )

    def test_attachment(self, tmp_path: Path) -> None:
        file = Path(tmp_path, "file")
        file.touch()
        with raises(SMTPServerDisconnected):
            send_email(
                "no-reply@test.com",
                ["user@test.com"],
                subject="Subject",
                attachments=[file],
            )
