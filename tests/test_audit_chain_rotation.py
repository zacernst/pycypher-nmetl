"""Tests for ChainedAuditLog rotation to prevent unbounded memory growth."""

from __future__ import annotations

import pytest

from shared.audit_chain import AuditRecord, ChainedAuditLog, RotationRecord


# ---------------------------------------------------------------------------
# Basic rotation behaviour
# ---------------------------------------------------------------------------


class TestRotationBasics:
    """Core rotation mechanics."""

    def test_no_rotation_when_disabled(self):
        """max_records=0 means unlimited growth (backward compat)."""
        log = ChainedAuditLog(max_records=0)
        for i in range(200):
            log.append(f"event-{i}")
        assert log.length == 200
        assert log.rotation_count == 0

    def test_auto_rotation_at_threshold(self):
        """Records are rotated when max_records is reached."""
        log = ChainedAuditLog(max_records=10)
        for i in range(25):
            log.append(f"event-{i}")
        # 25 records with max 10: should rotate at least twice
        assert log.rotation_count >= 2
        assert log.length <= 10

    def test_manual_rotate(self):
        """rotate() clears the active segment and returns a summary."""
        log = ChainedAuditLog()
        log.append("first")
        log.append("second")
        assert log.length == 2

        rotation = log.rotate()
        assert rotation is not None
        assert isinstance(rotation, RotationRecord)
        assert rotation.record_count == 2
        assert rotation.rotation_index == 0
        assert log.length == 0
        assert log.rotation_count == 1

    def test_rotate_empty_returns_none(self):
        """Rotating an empty chain is a no-op."""
        log = ChainedAuditLog()
        assert log.rotate() is None
        assert log.rotation_count == 0

    def test_total_appended_tracks_across_rotations(self):
        """total_appended counts all records, including rotated ones."""
        log = ChainedAuditLog(max_records=5)
        for i in range(17):
            log.append(f"event-{i}")
        assert log.total_appended == 17

    def test_max_records_property(self):
        log = ChainedAuditLog(max_records=42)
        assert log.max_records == 42

    def test_negative_max_records_treated_as_zero(self):
        log = ChainedAuditLog(max_records=-5)
        assert log.max_records == 0


# ---------------------------------------------------------------------------
# Cryptographic integrity across rotations
# ---------------------------------------------------------------------------


class TestRotationCryptoIntegrity:
    """HMAC chain integrity must be preserved across rotation boundaries."""

    def test_verify_after_rotation(self):
        """Chain verifies cleanly after rotation."""
        log = ChainedAuditLog(max_records=5)
        for i in range(12):
            log.append(f"event-{i}")
        valid, violations = log.verify()
        assert valid, f"Violations: {violations}"

    def test_chain_continuity_across_rotation(self):
        """First record after rotation chains from the rotated segment's last HMAC."""
        log = ChainedAuditLog()
        log.append("before-rotation")
        last_hmac = log.tail(1)[0].hmac_signature

        log.rotate()
        log.append("after-rotation")

        first_after = log.tail(1)[0]
        assert first_after.previous_hmac == last_hmac

    def test_rotation_record_preserves_last_hmac(self):
        """RotationRecord.last_hmac matches the last record's signature."""
        log = ChainedAuditLog()
        log.append("a")
        rec = log.append("b")
        expected_hmac = rec.hmac_signature

        rotation = log.rotate()
        assert rotation is not None
        assert rotation.last_hmac == expected_hmac

    def test_verify_with_multiple_rotations(self):
        """Chain integrity holds across many rotation cycles."""
        log = ChainedAuditLog(max_records=3)
        for i in range(30):
            log.append(f"event-{i}")
        valid, violations = log.verify()
        assert valid, f"Violations: {violations}"
        assert log.rotation_count >= 9

    def test_tampered_record_detected_after_rotation(self):
        """Tampering is still detected in post-rotation segments."""
        log = ChainedAuditLog(max_records=5)
        for i in range(8):
            log.append(f"event-{i}")
        # Tamper with the current segment
        if log._records:
            log._records[0].event = "TAMPERED"
        valid, violations = log.verify()
        assert not valid
        assert len(violations) >= 1


# ---------------------------------------------------------------------------
# Sequence numbering
# ---------------------------------------------------------------------------


class TestGlobalSequence:
    """Sequence numbers must be globally unique and monotonic."""

    def test_sequence_continues_after_rotation(self):
        """Sequence numbers don't reset on rotation."""
        log = ChainedAuditLog(max_records=3)
        sequences = []
        for i in range(9):
            rec = log.append(f"event-{i}")
            sequences.append(rec.sequence)
        assert sequences == list(range(9))

    def test_sequence_with_manual_rotation(self):
        """Manual rotation doesn't reset sequence."""
        log = ChainedAuditLog()
        log.append("a")
        log.append("b")
        log.rotate()
        rec = log.append("c")
        assert rec.sequence == 2


# ---------------------------------------------------------------------------
# Rotation history
# ---------------------------------------------------------------------------


class TestRotationHistory:
    """Rotation records track audit chain lifecycle."""

    def test_rotation_history_grows(self):
        """Each rotation adds a RotationRecord."""
        log = ChainedAuditLog(max_records=2)
        for i in range(10):
            log.append(f"event-{i}")
        rotations = log.rotations
        assert len(rotations) >= 4
        for i, r in enumerate(rotations):
            assert r.rotation_index == i
            assert r.record_count > 0
            assert r.rotated_at > 0
            assert r.last_hmac != ""

    def test_rotation_sequence_ranges(self):
        """Rotation records track first/last sequence of each segment."""
        log = ChainedAuditLog(max_records=3)
        for i in range(9):
            log.append(f"event-{i}")
        rotations = log.rotations
        # First rotation: sequences 0-2
        assert rotations[0].first_sequence == 0
        assert rotations[0].last_sequence == 2
        # Second rotation: sequences 3-5
        assert rotations[1].first_sequence == 3
        assert rotations[1].last_sequence == 5

    def test_rotations_returns_copy(self):
        """rotations property returns a copy, not the internal list."""
        log = ChainedAuditLog()
        log.append("a")
        log.rotate()
        rotations = log.rotations
        rotations.clear()
        assert log.rotation_count == 1


# ---------------------------------------------------------------------------
# Backward compatibility
# ---------------------------------------------------------------------------


class TestBackwardCompatibility:
    """Existing API must work unchanged."""

    def test_default_no_rotation(self):
        """Default constructor has no rotation limit."""
        log = ChainedAuditLog()
        assert log.max_records == 0

    def test_append_returns_record(self):
        log = ChainedAuditLog()
        rec = log.append("test", severity="warning", source="unit-test", extra="data")
        assert isinstance(rec, AuditRecord)
        assert rec.event == "test"
        assert rec.severity == "warning"
        assert rec.source == "unit-test"
        assert rec.metadata == {"extra": "data"}

    def test_tail_works(self):
        log = ChainedAuditLog()
        for i in range(5):
            log.append(f"event-{i}")
        tail = log.tail(3)
        assert len(tail) == 3
        assert tail[-1].event == "event-4"

    def test_verify_clean_chain(self):
        log = ChainedAuditLog()
        for i in range(10):
            log.append(f"event-{i}")
        valid, violations = log.verify()
        assert valid
        assert violations == []

    def test_length_property(self):
        log = ChainedAuditLog()
        assert log.length == 0
        log.append("a")
        assert log.length == 1

    def test_to_dict(self):
        log = ChainedAuditLog()
        rec = log.append("test")
        d = rec.to_dict()
        assert d["event"] == "test"
        assert "hmac_signature" in d
        assert "previous_hmac" in d
