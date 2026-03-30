"""Multiverse-inspired parallel query execution framework.

Applies the many-worlds interpretation of quantum mechanics as an
architectural metaphor for speculative parallel query execution:

- **Universes** represent distinct execution strategies (join orders,
  scan methods, index choices) explored concurrently.
- **Superposition** maintains multiple candidate plans simultaneously,
  deferring commitment until empirical cost data is available.
- **Coherence** identifies shared sub-computations across universes,
  allowing memoized intermediate results to be reused (quantum
  entanglement of shared plan fragments).
- **Collapse** selects the optimal result by measuring actual execution
  cost, latency, or cardinality — the "quantum measurement" that
  reduces the multiverse to a single observed outcome.

Modules
-------

:mod:`~fastopendata.multiverse.core`
    Universe and Multiverse data structures, branch tracking, and
    decoherence detection.

:mod:`~fastopendata.multiverse.planner`
    Many-worlds plan enumeration with coherence-based deduplication
    of shared sub-plans.

:mod:`~fastopendata.multiverse.executor`
    Parallel speculative execution across universes with adaptive
    collapse strategies.

:mod:`~fastopendata.multiverse.protocols`
    Interdimensional communication protocols for sharing optimization
    insights between parallel execution branches.

.. versionadded:: 0.0.30
"""

from fastopendata.multiverse.core import (
    BranchPoint,
    CollapseResult,
    MultiverseState,
    Universe,
    UniverseStatus,
)
from fastopendata.multiverse.executor import (
    CollapseStrategy,
    LatencyCollapseStrategy,
    MultiverseExecutor,
    QualityCollapseStrategy,
)
from fastopendata.multiverse.planner import (
    CoherenceGraph,
    MultiversePlanner,
    PlanVariant,
)
from fastopendata.multiverse.protocols import (
    DimensionalMessage,
    DimensionalMessageBus,
    MessageType,
)

__all__ = [
    # Core
    "BranchPoint",
    "CollapseResult",
    "MultiverseState",
    "Universe",
    "UniverseStatus",
    # Planner
    "CoherenceGraph",
    "MultiversePlanner",
    "PlanVariant",
    # Executor
    "CollapseStrategy",
    "LatencyCollapseStrategy",
    "MultiverseExecutor",
    "QualityCollapseStrategy",
    # Protocols
    "DimensionalMessage",
    "DimensionalMessageBus",
    "MessageType",
]
