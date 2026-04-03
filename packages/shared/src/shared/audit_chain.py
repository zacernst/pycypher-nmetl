"""Chained audit log with HMAC integrity verification.

Provides an append-only audit log where each record is linked to its
predecessor via an HMAC chain, making tampering detectable.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import time
from dataclasses import dataclass, field
from typing import Any


def _resolve_hmac_key() -> bytes:
    """Resolve the HMAC key from environment or generate an ephemeral one.

    Checks ``PYCYPHER_AUDIT_HMAC_KEY`` environment variable first.  If unset,
    generates a random 32-byte key for the current process lifetime (suitable
    for development; logs a warning to remind operators to configure a
    persistent key for production).
    """
    env_key = os.environ.get("PYCYPHER_AUDIT_HMAC_KEY", "").strip()
    if env_key:
        return env_key.encode("utf-8")
    import logging

    logging.getLogger("shared.audit_chain").warning(
        "PYCYPHER_AUDIT_HMAC_KEY not set — using ephemeral random key. "
        "Set this environment variable for persistent audit chain verification.",
    )
    return secrets.token_bytes(32)


_DEFAULT_KEY: bytes = _resolve_hmac_key()


@dataclass
class AuditRecord:
    """A single audit log entry with HMAC chaining."""

    sequence: int
    timestamp: float
    event: str
    severity: str = "info"
    source: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    previous_hmac: str = ""
    hmac_signature: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a JSON-compatible dict."""
        return {
            "sequence": self.sequence,
            "timestamp": self.timestamp,
            "event": self.event,
            "severity": self.severity,
            "source": self.source,
            "metadata": self.metadata,
            "previous_hmac": self.previous_hmac,
            "hmac_signature": self.hmac_signature,
        }

    def _payload(self) -> str:
        """Build the canonical string used for HMAC computation."""
        return (
            f"{self.sequence}|{self.timestamp}|{self.event}|"
            f"{self.severity}|{self.source}|{self.previous_hmac}"
        )


class ChainedAuditLog:
    """Append-only audit log with HMAC chain integrity."""

    def __init__(self, key: bytes = _DEFAULT_KEY) -> None:
        self._key = key
        self._records: list[AuditRecord] = []

    @property
    def length(self) -> int:
        """Number of records in the chain."""
        return len(self._records)

    def append(
        self,
        event: str,
        severity: str = "info",
        source: str = "",
        **metadata: Any,
    ) -> AuditRecord:
        """Append a new record to the chain."""
        prev_hmac = self._records[-1].hmac_signature if self._records else ""
        record = AuditRecord(
            sequence=len(self._records),
            timestamp=time.time(),
            event=event,
            severity=severity,
            source=source,
            metadata=metadata,
            previous_hmac=prev_hmac,
        )
        sig = hmac.new(
            self._key,
            record._payload().encode(),
            hashlib.sha256,
        ).hexdigest()
        record.hmac_signature = sig
        self._records.append(record)
        return record

    def verify(self) -> tuple[bool, list[str]]:
        """Verify the integrity of the full chain.

        Returns (valid, violations) where violations is a list of
        human-readable descriptions of any broken links.
        """
        violations: list[str] = []
        for i, record in enumerate(self._records):
            expected_prev = self._records[i - 1].hmac_signature if i > 0 else ""
            if record.previous_hmac != expected_prev:
                violations.append(
                    f"Record {i}: previous_hmac mismatch"
                )
            expected_sig = hmac.new(
                self._key,
                record._payload().encode(),
                hashlib.sha256,
            ).hexdigest()
            if record.hmac_signature != expected_sig:
                violations.append(
                    f"Record {i}: HMAC signature mismatch"
                )
        return (len(violations) == 0, violations)

    def tail(self, n: int = 10) -> list[AuditRecord]:
        """Return the last *n* records."""
        return self._records[-n:]
