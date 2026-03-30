"""Tests for interdimensional communication protocols.

Tests cover:
- MessageType enum values
- DimensionalMessage construction and frozen immutability
- DimensionalMessageBus: subscribe/publish, drain, message filtering,
  convenience publishers, stats, log trimming, handler error resilience
"""

from __future__ import annotations

import pytest
from fastopendata.multiverse.protocols import (
    DimensionalMessage,
    DimensionalMessageBus,
    MessageType,
)

# ---------------------------------------------------------------------------
# MessageType
# ---------------------------------------------------------------------------


class TestMessageType:
    def test_all_types_present(self) -> None:
        expected = {
            "cost_update",
            "cardinality_hint",
            "early_termination",
            "coherence_offer",
            "decoherence_alert",
            "plan_revision",
        }
        assert {mt.value for mt in MessageType} == expected


# ---------------------------------------------------------------------------
# DimensionalMessage
# ---------------------------------------------------------------------------


class TestDimensionalMessage:
    def test_defaults(self) -> None:
        msg = DimensionalMessage()
        assert len(msg.message_id) == 12
        assert msg.message_type == MessageType.COST_UPDATE
        assert msg.source_universe_id == ""
        assert msg.target_universe_id == "*"
        assert msg.payload == {}
        assert msg.priority == 5
        assert isinstance(msg.timestamp, float)

    def test_frozen(self) -> None:
        msg = DimensionalMessage()
        with pytest.raises(AttributeError):
            msg.priority = 1  # type: ignore[misc]

    def test_custom_fields(self) -> None:
        msg = DimensionalMessage(
            message_type=MessageType.CARDINALITY_HINT,
            source_universe_id="u1",
            target_universe_id="u2",
            payload={"rows": 500},
            priority=1,
        )
        assert msg.message_type == MessageType.CARDINALITY_HINT
        assert msg.source_universe_id == "u1"
        assert msg.target_universe_id == "u2"
        assert msg.payload["rows"] == 500
        assert msg.priority == 1


# ---------------------------------------------------------------------------
# DimensionalMessageBus
# ---------------------------------------------------------------------------


class TestDimensionalMessageBus:
    def test_publish_and_drain(self) -> None:
        bus = DimensionalMessageBus()
        msg = DimensionalMessage(source_universe_id="u1")
        bus.publish(msg)
        drained = bus.drain()
        assert len(drained) == 1
        assert drained[0] is msg

    def test_drain_clears_pending(self) -> None:
        bus = DimensionalMessageBus()
        bus.publish(DimensionalMessage())
        bus.drain()
        assert bus.drain() == []

    def test_drain_sorted_by_priority_then_timestamp(self) -> None:
        bus = DimensionalMessageBus()
        low = DimensionalMessage(priority=10, source_universe_id="low")
        high = DimensionalMessage(priority=1, source_universe_id="high")
        bus.publish(low)
        bus.publish(high)
        drained = bus.drain()
        assert drained[0].source_universe_id == "high"
        assert drained[1].source_universe_id == "low"

    def test_subscribe_and_publish(self) -> None:
        bus = DimensionalMessageBus()
        received: list[DimensionalMessage] = []
        bus.subscribe(MessageType.COST_UPDATE, received.append)
        msg = DimensionalMessage(message_type=MessageType.COST_UPDATE)
        bus.publish(msg)
        assert len(received) == 1
        assert received[0] is msg

    def test_subscribe_filters_by_type(self) -> None:
        bus = DimensionalMessageBus()
        received: list[DimensionalMessage] = []
        bus.subscribe(MessageType.COST_UPDATE, received.append)
        bus.publish(DimensionalMessage(message_type=MessageType.CARDINALITY_HINT))
        assert len(received) == 0

    def test_multiple_subscribers(self) -> None:
        bus = DimensionalMessageBus()
        r1: list[DimensionalMessage] = []
        r2: list[DimensionalMessage] = []
        bus.subscribe(MessageType.COST_UPDATE, r1.append)
        bus.subscribe(MessageType.COST_UPDATE, r2.append)
        bus.publish(DimensionalMessage(message_type=MessageType.COST_UPDATE))
        assert len(r1) == 1
        assert len(r2) == 1

    def test_handler_error_does_not_break_bus(self) -> None:
        bus = DimensionalMessageBus()

        def bad_handler(msg: DimensionalMessage) -> None:
            raise ValueError("handler error")

        received: list[DimensionalMessage] = []
        bus.subscribe(MessageType.COST_UPDATE, bad_handler)
        bus.subscribe(MessageType.COST_UPDATE, received.append)
        # Should not raise; second handler still called
        bus.publish(DimensionalMessage(message_type=MessageType.COST_UPDATE))
        # The second handler should still receive the message
        assert len(received) == 1

    def test_messages_for_universe_targeted(self) -> None:
        bus = DimensionalMessageBus()
        bus.publish(DimensionalMessage(target_universe_id="u1"))
        bus.publish(DimensionalMessage(target_universe_id="u2"))
        bus.publish(DimensionalMessage(target_universe_id="*"))
        msgs = bus.messages_for_universe("u1")
        assert len(msgs) == 2  # targeted + broadcast

    def test_messages_for_universe_broadcast(self) -> None:
        bus = DimensionalMessageBus()
        bus.publish(DimensionalMessage(target_universe_id="*"))
        msgs = bus.messages_for_universe("any_id")
        assert len(msgs) == 1

    def test_publish_cost_update(self) -> None:
        bus = DimensionalMessageBus()
        bus.publish_cost_update("u1", "frag1", 42.5)
        drained = bus.drain()
        assert len(drained) == 1
        msg = drained[0]
        assert msg.message_type == MessageType.COST_UPDATE
        assert msg.source_universe_id == "u1"
        assert msg.payload["fragment_fingerprint"] == "frag1"
        assert msg.payload["actual_cost"] == 42.5
        assert msg.priority == 3

    def test_publish_cardinality_hint(self) -> None:
        bus = DimensionalMessageBus()
        bus.publish_cardinality_hint("u1", "scan:Person", 1000, estimated_rows=500)
        drained = bus.drain()
        assert len(drained) == 1
        msg = drained[0]
        assert msg.message_type == MessageType.CARDINALITY_HINT
        assert msg.payload["actual_rows"] == 1000
        assert msg.payload["estimated_rows"] == 500
        assert msg.payload["estimation_error"] == pytest.approx(1.0)
        assert msg.priority == 2

    def test_publish_cardinality_hint_zero_estimate(self) -> None:
        bus = DimensionalMessageBus()
        bus.publish_cardinality_hint("u1", "op", 100, estimated_rows=0)
        msg = bus.drain()[0]
        assert msg.payload["estimation_error"] == 0.0

    def test_publish_coherence_offer(self) -> None:
        bus = DimensionalMessageBus()
        bus.publish_coherence_offer("u1", "frag1", "100 rows")
        drained = bus.drain()
        assert len(drained) == 1
        msg = drained[0]
        assert msg.message_type == MessageType.COHERENCE_OFFER
        assert msg.payload["fragment_fingerprint"] == "frag1"
        assert msg.payload["result_summary"] == "100 rows"
        assert msg.priority == 1

    def test_stats(self) -> None:
        bus = DimensionalMessageBus()
        received: list[DimensionalMessage] = []
        bus.subscribe(MessageType.COST_UPDATE, received.append)
        bus.publish(DimensionalMessage(message_type=MessageType.COST_UPDATE))
        bus.publish(DimensionalMessage(message_type=MessageType.CARDINALITY_HINT))
        stats = bus.stats
        assert stats["total_published"] == 2
        assert stats["total_delivered"] == 1  # only COST_UPDATE has subscriber
        assert stats["log_size"] == 2
        assert stats["by_type"]["cost_update"] == 1
        assert stats["by_type"]["cardinality_hint"] == 1

    def test_log_trimming(self) -> None:
        bus = DimensionalMessageBus(max_log_size=5)
        for i in range(10):
            bus.publish(DimensionalMessage(source_universe_id=str(i)))
        stats = bus.stats
        assert stats["log_size"] == 5
        # Should retain the most recent messages
        msgs = bus.messages_for_universe("*")
        sources = [m.source_universe_id for m in msgs]
        assert "9" in sources
        assert "0" not in sources
