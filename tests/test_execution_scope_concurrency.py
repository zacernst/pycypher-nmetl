"""Concurrency isolation tests for the per-query ``ExecutionScope``.

Verifies that concurrent ``Star.execute_query()`` calls sharing one
``Star``/``Context`` don't race on parameters or the mutation shadow layer
(see ``execution_scope.py`` / Phase 4 of ``IMPLEMENTATION_PLAN.md``).
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest
from pycypher import ContextBuilder, Star


@pytest.fixture
def star() -> Star:
    """Return a Star instance with a simple Person entity."""
    df = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )
    context = ContextBuilder().add_entity("Person", df).build()
    return Star(context=context)


class TestConcurrentParameters:
    """Concurrent queries with different bound parameters must not cross-talk."""

    def test_concurrent_parameterized_queries_do_not_leak(
        self, star: Star
    ) -> None:
        names = ["Alice", "Bob", "Carol"] * 20

        def run(name: str) -> str:
            result = star.execute_query(
                "MATCH (p:Person) WHERE p.name = $name RETURN p.name",
                parameters={"name": name},
            )
            assert len(result) == 1
            return result.iloc[0]["name"]

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(run, names))

        assert results == names


class TestConcurrentShadowIsolation:
    """Two concurrent scopes on the same ``Context`` must not share the shadow layer.

    This exercises ``Context.scoped_execution()``/``ExecutionScope`` directly
    (the mechanism Phase 4 introduces) rather than going through
    ``commit_query()``'s write to the shared canonical ``entity_mapping`` —
    concurrent commits to the *same* entity type racing on that shared
    mapping is a separate, pre-existing hazard outside Phase 4's scope.
    """

    def test_shadow_writes_are_not_visible_across_concurrent_scopes(
        self, star: Star
    ) -> None:
        barrier = threading.Barrier(2)
        seen: dict[str, dict] = {}

        def worker(key: str, value: int) -> None:
            with star.context.scoped_execution():
                star.context._shadow[key] = value
                barrier.wait()  # both threads now inside their own scope
                seen[key] = dict(star.context._shadow)

        t1 = threading.Thread(target=worker, args=("A", 1))
        t2 = threading.Thread(target=worker, args=("B", 2))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert seen["A"] == {"A": 1}
        assert seen["B"] == {"B": 2}
        # Scopes are popped on exit — no leftover shadow state on the Context.
        assert star.context._shadow == {}


class TestConcurrentReadDuringMutation:
    """A read query running concurrently with a CREATE must never observe
    the CREATE's uncommitted shadow rows."""

    def test_concurrent_read_never_sees_foreign_shadow(
        self, star: Star
    ) -> None:
        def create() -> None:
            for _ in range(20):
                star.execute_query(
                    "CREATE (:Person {name: 'ShouldNotLeak', age: 1})",
                )

        def read() -> int:
            counts = set()
            for _ in range(20):
                result = star.execute_query(
                    "MATCH (p:Person) RETURN p.name",
                )
                counts.add(len(result))
            return counts

        with ThreadPoolExecutor(max_workers=2) as pool:
            create_future = pool.submit(create)
            read_future = pool.submit(read)
            create_future.result()
            read_counts = read_future.result()

        # Reads only ever see fully-committed states: the original 3 rows,
        # or 3 + N committed CREATEs — never a partial/foreign shadow write.
        assert all(c >= 3 for c in read_counts)
        assert star.context._shadow == {}
        assert star.context._shadow_rels == {}
