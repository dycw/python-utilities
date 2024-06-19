from __future__ import annotations

from hmac import compare_digest
from typing import TYPE_CHECKING, Literal

from streamlit import (
    button,
    empty,
    error,
    form,
    form_submit_button,
    markdown,
    secrets,
    session_state,
    stop,
    text_input,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from streamlit.runtime.state import WidgetArgs, WidgetCallback, WidgetKwargs
    from streamlit.type_util import Key


def centered_button(
    label: str,
    /,
    *,
    key: Key | None = None,
    help: str | None = None,  # noqa: A002
    on_click: WidgetCallback | None = None,
    args: WidgetArgs | None = None,
    kwargs: WidgetKwargs | None = None,
    type: Literal["primary", "secondary"] = "secondary",  # noqa: A002
    disabled: bool = False,
    use_container_width: bool = False,
) -> bool:
    """Create a centered button."""
    style = r"<style>.row-widget.stButton {text-align: center;}</style>"
    _ = markdown(style, unsafe_allow_html=True)
    with empty():
        return button(
            label,
            key=key,
            help=help,
            on_click=on_click,
            args=args,
            kwargs=kwargs,
            type=type,
            disabled=disabled,
            use_container_width=use_container_width,
        )


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
        stop()


def _check_password(
    *,
    before_form: Callable[..., None] | None = None,
    after_form: Callable[..., None] | None = None,
) -> bool:
    """Return `True` if the user had a correct password."""
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
    """Check whether a password entered by the user is correct."""
    if (session_state[_USERNAME] in secrets["passwords"]) and compare_digest(
        session_state[_PASSWORD], secrets.passwords[session_state[_USERNAME]]
    ):
        session_state[_PASSWORD_CORRECT] = True
        del session_state[_PASSWORD]
        del session_state[_USERNAME]
    else:
        session_state[_PASSWORD_CORRECT] = False


__all__ = ["ensure_logged_in"]
