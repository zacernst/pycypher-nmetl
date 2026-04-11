"""Chained audit log with HMAC integrity verification.

Provides an append-only audit log where each record is linked to its
predecessor via an HMAC chain, making tampering detectable.

Supports configurable rotation to prevent unbounded memory growth in
long-running processes.  Set ``PYCYPHER_AUDIT_MAX_RECORDS`` to control the
maximum number of records kept in-memory before automatic rotation.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import time
from dataclasses import dataclass, field
from typing import Any

#: Default max records before automatic rotation (0 = unlimited).
_DEFAULT_MAX_RECORDS: int = int(
    os.environ.get("PYCYPHER_AUDIT_MAX_RECORDS", "0")
)


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
class RotationRecord:
    """Summary of a completed chain rotation.

    Preserves the cryptographic boundary so that archived segments can
    still be verified against the live chain.
    """

    rotation_index: int
    rotated_at: float
    record_count: int
    first_sequence: int
    last_sequence: int
    last_hmac: str


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
    """Append-only audit log with HMAC chain integrity.

    Parameters
    ----------
    key:
        HMAC signing key.
    max_records:
        Maximum records to keep in-memory.  When *append* would exceed
        this limit the oldest records are rotated out and a
        :class:`RotationRecord` is saved.  ``0`` (the default) disables
        automatic rotation.
    """

    def __init__(
        self,
        key: bytes = _DEFAULT_KEY,
        max_records: int = _DEFAULT_MAX_RECORDS,
    ) -> None:
        self._key = key
        self._records: list[AuditRecord] = []
        self._max_records: int = max(max_records, 0)
        self._rotations: list[RotationRecord] = []
        self._global_sequence: int = 0

    @property
    def length(self) -> int:
        """Number of records in the current (active) chain segment."""
        return len(self._records)

    @property
    def total_appended(self) -> int:
        """Total records appended since creation, including rotated ones."""
        return self._global_sequence

    @property
    def max_records(self) -> int:
        """Configured rotation threshold (0 = unlimited)."""
        return self._max_records

    @property
    def rotation_count(self) -> int:
        """Number of rotations that have occurred."""
        return len(self._rotations)

    @property
    def rotations(self) -> list[RotationRecord]:
        """History of rotation events."""
        return list(self._rotations)

    def append(
        self,
        event: str,
        severity: str = "info",
        source: str = "",
        **metadata: Any,
    ) -> AuditRecord:
        """Append a new record to the chain.

        If *max_records* is set and the chain has reached that limit,
        automatic rotation occurs **before** the new record is appended.
        """
        if self._max_records and len(self._records) >= self._max_records:
            self.rotate()

        if self._records:
            prev_hmac = self._records[-1].hmac_signature
        elif self._rotations:
            prev_hmac = self._rotations[-1].last_hmac
        else:
            prev_hmac = ""
        record = AuditRecord(
            sequence=self._global_sequence,
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
        self._global_sequence += 1
        return record

    def rotate(self) -> RotationRecord | None:
        """Manually rotate the current chain segment.

        Returns the :class:`RotationRecord` describing the rotated
        segment, or ``None`` if the chain was already empty.

        The last HMAC of the rotated segment is preserved so that
        the next appended record can chain from it, maintaining
        cryptographic continuity across rotation boundaries.
        """
        if not self._records:
            return None

        rotation = RotationRecord(
            rotation_index=len(self._rotations),
            rotated_at=time.time(),
            record_count=len(self._records),
            first_sequence=self._records[0].sequence,
            last_sequence=self._records[-1].sequence,
            last_hmac=self._records[-1].hmac_signature,
        )
        self._rotations.append(rotation)
        self._records.clear()
        return rotation

    def verify(self) -> tuple[bool, list[str]]:
        """Verify the integrity of the current chain segment.

        Returns (valid, violations) where violations is a list of
        human-readable descriptions of any broken links.
        """
        violations: list[str] = []
        for i, record in enumerate(self._records):
            if i == 0:
                # First record after rotation chains from the rotated
                # segment's last HMAC (or "" for a fresh chain).
                if self._rotations:
                    expected_prev = self._rotations[-1].last_hmac
                else:
                    expected_prev = ""
            else:
                expected_prev = self._records[i - 1].hmac_signature
            if record.previous_hmac != expected_prev:
                violations.append(
                    f"Record {record.sequence}: previous_hmac mismatch"
                )
            expected_sig = hmac.new(
                self._key,
                record._payload().encode(),
                hashlib.sha256,
            ).hexdigest()
            if record.hmac_signature != expected_sig:
                violations.append(
                    f"Record {record.sequence}: HMAC signature mismatch"
                )
        return (len(violations) == 0, violations)

    def tail(self, n: int = 10) -> list[AuditRecord]:
        """Return the last *n* records."""
        return self._records[-n:]
