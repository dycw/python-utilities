from __future__ import annotations

from collections.abc import Callable, Iterable
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from smtplib import SMTP
from typing import Any

from utilities.errors import redirect_error
from utilities.pathlib import PathLike
from utilities.pytest import is_pytest
from utilities.types import IterableStrs


def send_email(
    from_: str,
    to: IterableStrs,
    /,
    *,
    subject: str | None = None,
    contents: Any = None,
    subtype: str = "plain",
    host: str = "",
    port: int = 0,
    attachments: Iterable[PathLike] | None = None,
    disable: Callable[[], bool] | None = is_pytest,
) -> None:
    """Send an email."""
    if (disable is not None) and disable():
        return
    message = MIMEMultipart()
    message["From"] = from_
    message["To"] = ",".join(to)
    if subject is not None:
        message["Subject"] = subject
    if contents is not None:
        if isinstance(contents, str):
            text = MIMEText(contents, subtype)
        else:
            with redirect_error(ModuleNotFoundError, SendEmailError(f"{contents=}")):
                from airium import Airium

                if not isinstance(contents, Airium):
                    raise SendEmailError(contents)
                text = MIMEText(str(contents), "html")
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


class SendEmailError(Exception):
    ...


__all__ = ["SendEmailError", "is_pytest", "send_email"]
