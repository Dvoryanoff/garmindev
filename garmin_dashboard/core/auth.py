from __future__ import annotations

import hashlib
import hmac
import secrets
from datetime import datetime, timedelta

from .config import SESSION_TTL_DAYS


def hash_password(password: str, salt: str | None = None) -> str:
    salt = salt or secrets.token_hex(16)
    derived = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        200_000,
    ).hex()
    return f"{salt}${derived}"


def verify_password(password: str, stored_hash: str) -> bool:
    if "$" not in stored_hash:
        return False
    salt, expected = stored_hash.split("$", 1)
    actual = hash_password(password, salt=salt).split("$", 1)[1]
    return hmac.compare_digest(actual, expected)


def new_session_token() -> str:
    return secrets.token_urlsafe(32)


def session_expiry(days: int = SESSION_TTL_DAYS) -> str:
    return (datetime.now() + timedelta(days=days)).isoformat(timespec="seconds")
