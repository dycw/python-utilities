from __future__ import annotations

from smtplib import SMTPServerDisconnected

from pytest import raises

from utilities.airium import yield_airium
from utilities.email import send_email


class TestSendEmail:
    def test_contents_airium(self) -> None:
        with yield_airium() as airium:
            _ = airium
        with raises(SMTPServerDisconnected):
            send_email(
                "no-reply@test.com",
                ["user@test.com"],
                subject="Subject",
                contents=airium,
            )
