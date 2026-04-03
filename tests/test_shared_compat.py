"""Tests for shared.compat — API surface snapshot and diff utilities.

Covers snapshot_api_surface, save_snapshot, load_snapshot, diff_surfaces,
check_neo4j_compat, and all data classes (SymbolInfo, APISurface, SurfaceDiff).
Targets 0% → 90%+ coverage.
"""

from __future__ import annotations

import json
from pathlib import Path

from shared.compat import (
    NEO4J_COMPAT_NOTES,
    APISurface,
    SurfaceDiff,
    SymbolInfo,
    check_neo4j_compat,
    diff_surfaces,
    load_snapshot,
    save_snapshot,
    snapshot_api_surface,
)

# ---------------------------------------------------------------------------
# SymbolInfo
# ---------------------------------------------------------------------------


class TestSymbolInfo:
    """Tests for SymbolInfo dataclass."""

    def test_to_dict_with_signature(self) -> None:
        sym = SymbolInfo(
            name="foo",
            kind="function",
            signature="() -> None",
            module="mod",
        )
        d = sym.to_dict()
        assert d == {
            "name": "foo",
            "kind": "function",
            "signature": "() -> None",
            "module": "mod",
        }

    def test_to_dict_without_signature(self) -> None:
        sym = SymbolInfo(name="X", kind="constant")
        d = sym.to_dict()
        assert d == {"name": "X", "kind": "constant"}
        assert "signature" not in d
        assert "module" not in d

    def test_from_dict_roundtrip(self) -> None:
        sym = SymbolInfo(
            name="Bar",
            kind="class",
            signature="(x: int)",
            module="pkg.bar",
        )
        restored = SymbolInfo.from_dict(sym.to_dict())
        assert restored.name == sym.name
        assert restored.kind == sym.kind
        assert restored.signature == sym.signature
        assert restored.module == sym.module

    def test_from_dict_minimal(self) -> None:
        sym = SymbolInfo.from_dict({"name": "C", "kind": "constant"})
        assert sym.signature is None
        assert sym.module == ""


# ---------------------------------------------------------------------------
# APISurface
# ---------------------------------------------------------------------------


class TestAPISurface:
    """Tests for APISurface dataclass."""

    def test_to_dict(self) -> None:
        surface = APISurface(
            module_name="mymod",
            version="1.0",
            symbols={"A": SymbolInfo(name="A", kind="class")},
        )
        d = surface.to_dict()
        assert d["module_name"] == "mymod"
        assert d["version"] == "1.0"
        assert "A" in d["symbols"]

    def test_from_dict_roundtrip(self) -> None:
        surface = APISurface(
            module_name="pkg",
            version="2.0",
            symbols={
                "foo": SymbolInfo(
                    name="foo", kind="function", signature="() -> int"
                ),
                "Bar": SymbolInfo(name="Bar", kind="class"),
            },
        )
        restored = APISurface.from_dict(surface.to_dict())
        assert restored.module_name == "pkg"
        assert restored.version == "2.0"
        assert len(restored.symbols) == 2
        assert restored.symbols["foo"].signature == "() -> int"

    def test_from_dict_empty_symbols(self) -> None:
        surface = APISurface.from_dict({"module_name": "m", "version": "0"})
        assert surface.symbols == {}


# ---------------------------------------------------------------------------
# snapshot_api_surface
# ---------------------------------------------------------------------------


class TestSnapshotApiSurface:
    """Tests for snapshot_api_surface()."""

    def test_snapshot_pycypher(self) -> None:
        surface = snapshot_api_surface("pycypher")
        assert surface.module_name == "pycypher"
        assert surface.version == "0.0.19"
        assert len(surface.symbols) > 0
        assert "Star" in surface.symbols
        assert surface.symbols["Star"].kind == "class"

    def test_snapshot_captures_exceptions(self) -> None:
        surface = snapshot_api_surface("pycypher")
        assert surface.symbols["SecurityError"].kind == "exception"

    def test_snapshot_captures_constants(self) -> None:
        surface = snapshot_api_surface("pycypher")
        assert surface.symbols["ID_COLUMN"].kind == "constant"

    def test_snapshot_captures_functions(self) -> None:
        surface = snapshot_api_surface("pycypher")
        assert surface.symbols["get_cache_stats"].kind == "function"
        assert surface.symbols["get_cache_stats"].signature is not None


# ---------------------------------------------------------------------------
# save_snapshot / load_snapshot
# ---------------------------------------------------------------------------


class TestSnapshotPersistence:
    """Tests for save_snapshot() and load_snapshot()."""

    def test_save_and_load_roundtrip(self, tmp_path: Path) -> None:
        surface = APISurface(
            module_name="test",
            version="0.1",
            symbols={
                "Foo": SymbolInfo(name="Foo", kind="class", signature="()"),
            },
        )
        path = tmp_path / "snapshot.json"
        save_snapshot(surface, path)

        loaded = load_snapshot(path)
        assert loaded.module_name == "test"
        assert loaded.version == "0.1"
        assert "Foo" in loaded.symbols
        assert loaded.symbols["Foo"].kind == "class"

    def test_save_creates_valid_json(self, tmp_path: Path) -> None:
        surface = APISurface(module_name="m", version="1", symbols={})
        path = tmp_path / "out.json"
        save_snapshot(surface, path)
        data = json.loads(path.read_text())
        assert data["module_name"] == "m"


# ---------------------------------------------------------------------------
# diff_surfaces
# ---------------------------------------------------------------------------


class TestDiffSurfaces:
    """Tests for diff_surfaces()."""

    def _make_surface(
        self,
        version: str,
        symbols: dict[str, SymbolInfo],
    ) -> APISurface:
        return APISurface(module_name="pkg", version=version, symbols=symbols)

    def test_no_changes(self) -> None:
        sym = {"A": SymbolInfo(name="A", kind="class")}
        diff = diff_surfaces(
            self._make_surface("1.0", sym),
            self._make_surface("1.1", dict(sym)),
        )
        assert diff.added == []
        assert diff.removed == []
        assert diff.changed == []
        assert not diff.has_breaking_changes

    def test_added_symbol(self) -> None:
        old = self._make_surface(
            "1.0", {"A": SymbolInfo(name="A", kind="class")}
        )
        new = self._make_surface(
            "1.1",
            {
                "A": SymbolInfo(name="A", kind="class"),
                "B": SymbolInfo(name="B", kind="function"),
            },
        )
        diff = diff_surfaces(old, new)
        assert len(diff.added) == 1
        assert diff.added[0].name == "B"
        assert not diff.has_breaking_changes

    def test_removed_symbol(self) -> None:
        old = self._make_surface(
            "1.0",
            {
                "A": SymbolInfo(name="A", kind="class"),
                "B": SymbolInfo(name="B", kind="function"),
            },
        )
        new = self._make_surface(
            "1.1", {"A": SymbolInfo(name="A", kind="class")}
        )
        diff = diff_surfaces(old, new)
        assert len(diff.removed) == 1
        assert diff.removed[0].name == "B"
        assert diff.has_breaking_changes

    def test_changed_signature(self) -> None:
        old = self._make_surface(
            "1.0",
            {
                "foo": SymbolInfo(
                    name="foo", kind="function", signature="(x: int)"
                ),
            },
        )
        new = self._make_surface(
            "1.1",
            {
                "foo": SymbolInfo(
                    name="foo", kind="function", signature="(x: str)"
                ),
            },
        )
        diff = diff_surfaces(old, new)
        assert len(diff.changed) == 1
        assert diff.changed[0][0].signature == "(x: int)"
        assert diff.changed[0][1].signature == "(x: str)"
        assert diff.has_breaking_changes

    def test_signature_none_not_flagged_as_change(self) -> None:
        """Constants with no signature should not be flagged as changed."""
        old = self._make_surface(
            "1.0",
            {
                "X": SymbolInfo(name="X", kind="constant", signature=None),
            },
        )
        new = self._make_surface(
            "1.1",
            {
                "X": SymbolInfo(name="X", kind="constant", signature=None),
            },
        )
        diff = diff_surfaces(old, new)
        assert diff.changed == []

    def test_summary_no_breaking(self) -> None:
        diff = SurfaceDiff(old_version="1.0", new_version="1.1")
        summary = diff.summary()
        assert "No breaking changes" in summary

    def test_summary_with_removals(self) -> None:
        diff = SurfaceDiff(
            old_version="1.0",
            new_version="1.1",
            removed=[SymbolInfo(name="gone", kind="function")],
        )
        summary = diff.summary()
        assert "BREAKING" in summary
        assert "gone" in summary

    def test_summary_with_additions(self) -> None:
        diff = SurfaceDiff(
            old_version="1.0",
            new_version="1.1",
            added=[SymbolInfo(name="new_fn", kind="function")],
        )
        summary = diff.summary()
        assert "new_fn" in summary

    def test_summary_with_changes(self) -> None:
        diff = SurfaceDiff(
            old_version="1.0",
            new_version="1.1",
            changed=[
                (
                    SymbolInfo(name="fn", kind="function", signature="(a)"),
                    SymbolInfo(name="fn", kind="function", signature="(a, b)"),
                ),
            ],
        )
        summary = diff.summary()
        assert "Signature changes" in summary


# ---------------------------------------------------------------------------
# check_neo4j_compat
# ---------------------------------------------------------------------------


class TestCheckNeo4jCompat:
    """Tests for check_neo4j_compat()."""

    def test_exact_match(self) -> None:
        result = check_neo4j_compat("CREATE")
        assert result is not None
        assert result["supported"] is True

    def test_substring_match(self) -> None:
        result = check_neo4j_compat("detach")
        assert result is not None
        assert result["feature"] == "DETACH DELETE"
        assert result["supported"] is False

    def test_no_match(self) -> None:
        result = check_neo4j_compat("nonexistent_feature_xyz")
        assert result is None

    def test_case_insensitive(self) -> None:
        result = check_neo4j_compat("optional match")
        assert result is not None

    def test_neo4j_compat_notes_not_empty(self) -> None:
        assert len(NEO4J_COMPAT_NOTES) > 10


# ---------------------------------------------------------------------------
# Integration: baseline validation
# ---------------------------------------------------------------------------


class TestBaselineValidation:
    """Validate that api_baseline.json matches the live API surface."""

    def test_baseline_symbols_match_all(self) -> None:
        """Every symbol in __all__ is present in the baseline."""
        import pycypher

        baseline = load_snapshot("api_baseline.json")
        all_set = set(pycypher.__all__)
        baseline_set = set(baseline.symbols.keys())
        assert all_set == baseline_set, (
            f"Mismatch: in __all__ not in baseline: {all_set - baseline_set}, "
            f"in baseline not in __all__: {baseline_set - all_set}"
        )

    def test_baseline_symbol_count(self) -> None:
        baseline = load_snapshot("api_baseline.json")
        assert len(baseline.symbols) >= 37

    def test_no_symbols_removed_from_baseline(self) -> None:
        """Live snapshot should not have fewer symbols than baseline."""
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            live = snapshot_api_surface("pycypher")
        baseline = load_snapshot("api_baseline.json")
        diff = diff_surfaces(baseline, live)
        assert not diff.removed, (
            f"Symbols removed: {[s.name for s in diff.removed]}"
        )
