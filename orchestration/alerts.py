"""Failure notification stubs (swap ``send_alert`` for a Slack webhook in prod)."""

from __future__ import annotations

import structlog

log = structlog.get_logger(__name__)


def send_alert(subject: str, body: str) -> None:
    """Print an alert to stdout. Replace with a Slack/email integration in prod."""
    log.error("ALERT", subject=subject, body=body)
    print(f"[ALERT] {subject}\n{body}")
