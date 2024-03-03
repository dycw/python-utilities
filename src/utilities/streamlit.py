from __future__ import annotations

from hmac import compare_digest
from typing import TYPE_CHECKING, NoReturn

from streamlit import (
    error,
    form,
    form_submit_button,
    secrets,
    session_state,
    text_input,
)
from streamlit import stop as _stop

if TYPE_CHECKING:
    from collections.abc import Callable

_USERNAME = "username"
_PASSWORD = "password"  # noqa: S105
_PASSWORD_CORRECT = "password_correct"  # noqa: S105


def ensure_logged_in(
    *,
    skip: bool = False,
    before_form: Callable[..., None] | None = None,
    after_form: Callable[..., None] | None = None,
) -> None:
    """Ensure the user is logged in."""

    if not (skip or _check_password(before_form=before_form, after_form=after_form)):
        _stop()


def _check_password(
    *,
    before_form: Callable[..., None] | None = None,
    after_form: Callable[..., None] | None = None,
) -> bool:
    """Returns `True` if the user had a correct password."""
    if session_state.get("password_correct", False):
        return True
    if before_form is not None:
        before_form()
    with form("Credentials"):
        _ = text_input("Username", key=_USERNAME)
        _ = text_input("Password", type="password", key=_PASSWORD)
        _ = form_submit_button("Log in", on_click=_password_entered)
    if after_form is not None:
        after_form()
    if _PASSWORD_CORRECT in session_state:
        _ = error("Username/password combination invalid or incorrect")
    return False


def _password_entered() -> None:
    """Checks whether a password entered by the user is correct."""
    if (session_state[_USERNAME] in secrets["passwords"]) and compare_digest(
        session_state[_PASSWORD], secrets.passwords[session_state[_USERNAME]]
    ):
        session_state[_PASSWORD_CORRECT] = True
        del session_state[_PASSWORD]
        del session_state[_USERNAME]
    else:
        session_state[_PASSWORD_CORRECT] = False


def stop() -> NoReturn:
    """Wrapper around `stop`."""

    _stop()
    msg = "Failed to stop the `streamlit` document"
    raise RuntimeError(msg)


__all__ = ["ensure_logged_in", "stop"]
