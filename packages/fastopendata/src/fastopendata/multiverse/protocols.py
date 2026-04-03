"""Interdimensional communication protocols for optimization insight sharing.

When universes execute in parallel, they may discover information that
is valuable to other branches — e.g., a universe discovers that a
particular scan is extremely cheap, or that a join produces far fewer
rows than estimated. This module provides a message bus for sharing
such insights across the "dimensional boundary."

Architecture
------------

::

    DimensionalMessageBus
    ├── publish()                 — send a message to all subscribers
    ├── subscribe()               — register a callback for a message type
    ├── drain()                   — collect all pending messages
    └── _message_log              — ordered log for replay/debugging

    DimensionalMessage
    ├── message_type              — category of insight
    ├── source_universe_id        — which branch discovered this
    ├── payload                   — the actual insight data
    └── timestamp                 — when the message was published

Interdimensional Communication Analogy
---------------------------------------

In many-worlds QM, branches cannot directly communicate — but the
multiverse executor has a privileged position outside all branches
(the "meta-observer") and can relay information. The message bus
acts as this meta-observer channel:

- **COST_UPDATE**: A universe reports its actual execution cost for
  a fragment, enabling other universes to revise their estimates.
- **CARDINALITY_HINT**: A universe reports actual row counts from
  a scan or join, helping others avoid expensive operations.
- **EARLY_TERMINATION**: A universe signals it should be pruned
  (e.g., it exceeded a resource limit).
- **COHERENCE_OFFER**: A universe offers to share a computed
  fragment result with others.

.. versionadded:: 0.0.30
"""

from __future__ import annotations

import enum
import logging
import threading
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

_logger = logging.getLogger(__name__)


class MessageType(enum.Enum):
    """Categories of interdimensional messages.

    COST_UPDATE:        Actual execution cost for a fragment.
    CARDINALITY_HINT:   Actual row count from a scan or join.
    EARLY_TERMINATION:  Request to prune a universe.
    COHERENCE_OFFER:    Offer to share a computed fragment.
    DECOHERENCE_ALERT:  Warning that a universe is diverging.
    PLAN_REVISION:      Suggestion to revise an execution plan.
    """

    COST_UPDATE = "cost_update"
    CARDINALITY_HINT = "cardinality_hint"
    EARLY_TERMINATION = "early_termination"
    COHERENCE_OFFER = "coherence_offer"
    DECOHERENCE_ALERT = "decoherence_alert"
    PLAN_REVISION = "plan_revision"


@dataclass(frozen=True)
class DimensionalMessage:
    """A message passed between execution universes.

    Attributes
    ----------
    message_id : str
        Globally unique message identifier.
    message_type : MessageType
        Category of insight being communicated.
    source_universe_id : str
        Universe that originated the message.
    target_universe_id : str
        Intended recipient (``"*"`` for broadcast).
    payload : dict[str, Any]
        The insight data.
    timestamp : float
        When the message was published (epoch seconds).
    priority : int
        Message priority (lower = higher priority). Affects
        processing order when multiple messages are pending.

    """

    message_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    message_type: MessageType = MessageType.COST_UPDATE
    source_universe_id: str = ""
    target_universe_id: str = "*"
    payload: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    priority: int = 5


# Type alias for message handlers
MessageHandler = Callable[[DimensionalMessage], None]


class DimensionalMessageBus:
    """Thread-safe message bus for interdimensional communication.

    Provides pub/sub messaging between parallel execution universes.
    Messages are delivered asynchronously — publishers do not block
    waiting for subscribers to process.

    Parameters
    ----------
    max_log_size : int
        Maximum number of messages retained in the replay log
        (default: 10000).

    """

    def __init__(self, *, max_log_size: int = 10_000) -> None:
        self._subscribers: dict[MessageType, list[MessageHandler]] = (
            defaultdict(list)
        )
        self._message_log: list[DimensionalMessage] = []
        self._pending: list[DimensionalMessage] = []
        self._lock = threading.Lock()
        self._max_log_size = max_log_size
        self._total_published = 0
        self._total_delivered = 0

    def subscribe(
        self,
        message_type: MessageType,
        handler: MessageHandler,
    ) -> None:
        """Register a handler for a specific message type.

        Parameters
        ----------
        message_type:
            The type of message to listen for.
        handler:
            Callback invoked when a matching message is published.

        """
        with self._lock:
            self._subscribers[message_type].append(handler)

    def publish(self, message: DimensionalMessage) -> None:
        """Publish a message to all subscribers of its type.

        Messages are also appended to the replay log and pending
        queue for drain-based consumption.

        Parameters
        ----------
        message:
            The interdimensional message to publish.

        """
        with self._lock:
            self._total_published += 1
            self._message_log.append(message)
            self._pending.append(message)

            # Trim log if needed
            if len(self._message_log) > self._max_log_size:
                self._message_log = self._message_log[-self._max_log_size :]

            handlers = list(self._subscribers.get(message.message_type, []))

        # Deliver outside the lock to avoid deadlocks in handler code.
        # The _total_delivered counter is updated under the lock after
        # delivery to ensure thread-safe accounting.
        delivered = 0
        for handler in handlers:
            try:
                handler(message)
                delivered += 1
            except Exception:
                _logger.exception(
                    "Handler failed for message %s from universe %s",
                    message.message_id,
                    message.source_universe_id,
                )
        if delivered:
            with self._lock:
                self._total_delivered += delivered

    def drain(self) -> list[DimensionalMessage]:
        """Collect and return all pending messages.

        Messages are returned in publication order and cleared from
        the pending queue.

        Returns
        -------
        list[DimensionalMessage]
            Pending messages, sorted by priority then timestamp.

        """
        with self._lock:
            messages = sorted(
                self._pending,
                key=lambda m: (m.priority, m.timestamp),
            )
            self._pending = []
        return messages

    def messages_for_universe(
        self, universe_id: str
    ) -> list[DimensionalMessage]:
        """Return all logged messages targeted at a specific universe.

        Parameters
        ----------
        universe_id:
            The target universe (also includes broadcast messages).

        Returns
        -------
        list[DimensionalMessage]
            Messages targeted at this universe or broadcast.

        """
        with self._lock:
            return [
                m
                for m in self._message_log
                if m.target_universe_id in (universe_id, "*")
            ]

    def publish_cost_update(
        self,
        source_universe_id: str,
        fragment_fingerprint: str,
        actual_cost: float,
    ) -> None:
        """Convenience: publish a cost update message.

        Parameters
        ----------
        source_universe_id:
            Universe that measured the cost.
        fragment_fingerprint:
            Which fragment's cost was measured.
        actual_cost:
            The observed cost value.

        """
        self.publish(
            DimensionalMessage(
                message_type=MessageType.COST_UPDATE,
                source_universe_id=source_universe_id,
                payload={
                    "fragment_fingerprint": fragment_fingerprint,
                    "actual_cost": actual_cost,
                },
                priority=3,
            ),
        )

    def publish_cardinality_hint(
        self,
        source_universe_id: str,
        operation: str,
        actual_rows: int,
        estimated_rows: int = 0,
    ) -> None:
        """Convenience: publish a cardinality hint message.

        Parameters
        ----------
        source_universe_id:
            Universe that measured the cardinality.
        operation:
            Which operation produced the row count.
        actual_rows:
            The observed row count.
        estimated_rows:
            The original estimate (for computing estimation error).

        """
        self.publish(
            DimensionalMessage(
                message_type=MessageType.CARDINALITY_HINT,
                source_universe_id=source_universe_id,
                payload={
                    "operation": operation,
                    "actual_rows": actual_rows,
                    "estimated_rows": estimated_rows,
                    "estimation_error": (
                        abs(actual_rows - estimated_rows)
                        / max(1, estimated_rows)
                        if estimated_rows > 0
                        else 0.0
                    ),
                },
                priority=2,
            ),
        )

    def publish_coherence_offer(
        self,
        source_universe_id: str,
        fragment_fingerprint: str,
        result_summary: str = "",
    ) -> None:
        """Convenience: publish a coherence offer message.

        Parameters
        ----------
        source_universe_id:
            Universe offering the cached result.
        fragment_fingerprint:
            Which fragment is being offered.
        result_summary:
            Human-readable summary of the result.

        """
        self.publish(
            DimensionalMessage(
                message_type=MessageType.COHERENCE_OFFER,
                source_universe_id=source_universe_id,
                payload={
                    "fragment_fingerprint": fragment_fingerprint,
                    "result_summary": result_summary,
                },
                priority=1,
            ),
        )

    @property
    def stats(self) -> dict[str, Any]:
        """Return message bus statistics.

        Returns
        -------
        dict[str, Any]
            Statistics including total published, delivered, pending,
            and per-type counts.

        """
        with self._lock:
            type_counts: dict[str, int] = defaultdict(int)
            for msg in self._message_log:
                type_counts[msg.message_type.value] += 1

            return {
                "total_published": self._total_published,
                "total_delivered": self._total_delivered,
                "log_size": len(self._message_log),
                "pending": len(self._pending),
                "by_type": dict(type_counts),
            }
