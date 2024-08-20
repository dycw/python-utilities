from __future__ import annotations

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from smtplib import SMTP
from typing import TYPE_CHECKING

from utilities.iterables import always_iterable

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from utilities.types import PathLike


def send_email(
    from_: str,
    to: str | Sequence[str],
    /,
    *,
    subject: str | None = None,
    contents: str | None = None,
    subtype: str = "plain",
    host: str = "",
    port: int = 0,
    attachments: Iterable[PathLike] | None = None,
) -> None:
    """Send an email."""
    message = MIMEMultipart()
    message["From"] = from_
    message["To"] = ",".join(always_iterable(to))
    if subject is not None:
        message["Subject"] = subject
    if contents is not None:
        text = MIMEText(contents, subtype)
        message.attach(text)
    if attachments is not None:
        for attachment in attachments:
            _add_attachment(attachment, message)
    with SMTP(host=host, port=port) as smtp:
        _ = smtp.send_message(message)


def _add_attachment(path: PathLike, message: MIMEMultipart, /) -> None:
    """Add an attachment to an email."""
    path = Path(path)
    name = path.name
    with path.open(mode="rb") as fh:
        part = MIMEApplication(fh.read(), Name=name)
    part["Content-Disposition"] = f"attachment; filename{name}"
    message.attach(part)


class SendEmailError(Exception): ...


__all__ = ["SendEmailError", "send_email"]
