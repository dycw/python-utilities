from collections.abc import Callable, Iterable
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from smtplib import SMTP
from typing import Any, Optional

from beartype import beartype

from utilities.beartype import IterableStrs
from utilities.pathlib import PathLike
from utilities.pytest import is_pytest


@beartype
def send_email(
    from_: str,
    to: IterableStrs,
    /,
    *,
    subject: Optional[str] = None,
    contents: Any = None,
    subtype: str = "plain",
    host: str = "",
    port: int = 0,
    attachments: Optional[Iterable[PathLike]] = None,
    disable: Optional[Callable[[], bool]] = is_pytest,
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
            try:
                from airium import Airium
            except ModuleNotFoundError:  # pragma: no cover
                raise InvalidContentsError(contents) from None
            else:
                if not isinstance(contents, Airium):
                    raise InvalidContentsError(contents)
                text = MIMEText(str(contents), "html")
        message.attach(text)
    if attachments is not None:
        for attachment in attachments:
            _add_attachment(attachment, message)
    with SMTP(host=host, port=port) as smtp:
        _ = smtp.send_message(message)


@beartype
def _add_attachment(path: PathLike, message: MIMEMultipart, /) -> None:
    """Add an attachment to an email."""
    path = Path(path)
    name = path.name
    with path.open(mode="rb") as fh:
        part = MIMEApplication(fh.read(), Name=name)
    part["Content-Disposition"] = f"attachment; filename{name}"
    message.attach(part)


class InvalidContentsError(TypeError):
    """Raised when an invalid set of contents is encountered."""
