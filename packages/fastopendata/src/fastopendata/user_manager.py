import uuid

from fastapi import Request
from fastapi_users import BaseUserManager, UUIDIDMixin

from .db import User

SECRET = "SECRET"


class UserManager(UUIDIDMixin, BaseUserManager[User, uuid.UUID]):
    """My custom user manager for FastOpenData."""

    reset_password_token_secret = SECRET
    verification_token_secret = SECRET

    async def on_after_register(
        self, user: User, request: Request | None = None
    ) -> None:
        """Docstring here."""

    async def on_after_forgot_password(
        self, user: User, token: str, request: Request | None = None
    ) -> None:
        """Docstring here."""

    async def on_after_request_verify(
        self, user: User, token: str, request: Request | None = None
    ) -> None:
        """Docstring here."""


async def get_user_manager(user_db=Depends(get_user_db)):
    yield UserManager(user_db)
