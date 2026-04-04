from __future__ import annotations

import smtplib
from email.message import EmailMessage
from pathlib import Path

from .config import (
    PROJECT_ROOT,
    SMTP_FROM_EMAIL,
    SMTP_HOST,
    SMTP_PASSWORD,
    SMTP_PORT,
    SMTP_USE_SSL,
    SMTP_USE_TLS,
    SMTP_USERNAME,
)


def mail_is_configured() -> bool:
    return bool(SMTP_HOST and SMTP_USERNAME and SMTP_PASSWORD and SMTP_FROM_EMAIL)


def write_outbox(*, to_email: str, subject: str, body: str) -> Path:
    outbox_dir = PROJECT_ROOT / "runtime" / "mail_outbox"
    outbox_dir.mkdir(parents=True, exist_ok=True)
    file_path = outbox_dir / f"{to_email.replace('@', '_at_')}_{subject[:32].replace(' ', '_')}.txt"
    file_path.write_text(f"TO: {to_email}\nSUBJECT: {subject}\n\n{body}\n", encoding="utf-8")
    return file_path


def send_email(*, to_email: str, subject: str, body: str) -> dict:
    outbox_path = write_outbox(to_email=to_email, subject=subject, body=body)
    if not mail_is_configured():
        return {"sent": False, "mode": "outbox", "path": str(outbox_path)}

    message = EmailMessage()
    message["From"] = SMTP_FROM_EMAIL
    message["To"] = to_email
    message["Subject"] = subject
    message.set_content(body)

    if SMTP_USE_SSL:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=20) as smtp:
            smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
            smtp.send_message(message)
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as smtp:
            if SMTP_USE_TLS:
                smtp.starttls()
            smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
            smtp.send_message(message)
    return {"sent": True, "mode": "smtp", "path": str(outbox_path)}
