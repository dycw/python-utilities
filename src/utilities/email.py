from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP
from typing import Any

from beartype import beartype

from utilities.beartype import IterableStrs


@beartype
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
) -> None:
    """Send an email."""

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
                raise InvalidContents(contents)
            else:
                if isinstance(contents, Airium):
                    text = MIMEText(str(contents), "html")
                else:
                    raise InvalidContents(contents)
        message.attach(text)
    with SMTP(host=host, port=port) as smtp:
        _ = smtp.send_message(message)


class InvalidContents(TypeError):
    ...
