"""Tests for fastopendata.processing.compress_wikidata module.

Covers:
- _filtered_lines() generator: pre-filtering, JSON parsing, English field extraction
- Malformed JSON handling
- Sitelinks removal
- Multilingual field pruning

Note: The module-level Progress block executes on import, so we mock
sys.stdin and rich.progress.Progress to test _filtered_lines() in isolation.
"""

from __future__ import annotations

import json
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest


def _make_entity(
    *,
    latitude: float = 40.0,
    longitude: float = -74.0,
    descriptions: dict | None = None,
    labels: dict | None = None,
    aliases: dict | None = None,
    sitelinks: dict | None = None,
    extra: dict | None = None,
) -> dict:
    """Build a minimal Wikidata entity dict with coordinate claims."""
    entity: dict = {
        "id": "Q42",
        "type": "item",
        "claims": {
            "P625": [
                {
                    "mainsnak": {
                        "datavalue": {
                            "value": {
                                "latitude": latitude,
                                "longitude": longitude,
                            }
                        }
                    }
                }
            ]
        },
    }
    if descriptions is not None:
        entity["descriptions"] = descriptions
    if labels is not None:
        entity["labels"] = labels
    if aliases is not None:
        entity["aliases"] = aliases
    if sitelinks is not None:
        entity["sitelinks"] = sitelinks
    if extra:
        entity.update(extra)
    return entity


@pytest.mark.integration
class TestFilteredLines:
    """Test the _filtered_lines() generator logic."""

    def _run_filtered_lines(self, lines: list[str]) -> list[tuple[dict, int]]:
        """Import and run _filtered_lines with mocked stdin and Progress."""
        import importlib

        stdin_mock = StringIO("\n".join(lines) + "\n")

        with (
            patch("sys.stdin", stdin_mock),
            patch(
                "fastopendata.processing.compress_wikidata.Progress"
            ) as mock_progress,
        ):
            mock_progress.return_value.__enter__ = MagicMock(
                return_value=mock_progress
            )
            mock_progress.return_value.__exit__ = MagicMock(return_value=False)
            mock_progress.get_default_columns = MagicMock(return_value=[])
            mock_progress.add_task = MagicMock(return_value=0)

            # Force re-import to reset _total_lines and re-execute module
            import fastopendata.processing.compress_wikidata as mod

            # Reset the global counter
            mod._total_lines = 0

            # Patch stdin for the generator
            results = []
            gen_stdin = StringIO("\n".join(lines) + "\n")
            with patch("sys.stdin", gen_stdin):
                for entity, line_num in mod._filtered_lines():
                    results.append((entity, line_num))
            return results

    def test_entity_with_latitude_passes_filter(self) -> None:
        entity = _make_entity(
            descriptions={"en": {"value": "A place"}},
            labels={"en": {"value": "Place"}},
        )
        line = json.dumps(entity)
        results = self._run_filtered_lines([line])

        assert len(results) == 1
        parsed, line_num = results[0]
        assert parsed["id"] == "Q42"
        assert line_num == 1

    def test_entity_without_latitude_filtered_out(self) -> None:
        """Lines without 'latitude' substring are skipped before JSON parsing."""
        entity = {"id": "Q99", "type": "item", "claims": {}}
        line = json.dumps(entity)
        results = self._run_filtered_lines([line])

        assert len(results) == 0

    def test_short_lines_skipped(self) -> None:
        """Lines shorter than 3 chars are skipped."""
        results = self._run_filtered_lines(["", "{}", "x"])
        assert len(results) == 0

    def test_english_descriptions_kept(self) -> None:
        entity = _make_entity(
            descriptions={"en": {"value": "English desc"}, "fr": {"value": "French"}},
        )
        line = json.dumps(entity)
        results = self._run_filtered_lines([line])

        assert len(results) == 1
        # English facet is kept directly
        assert results[0][0]["descriptions"] == {"value": "English desc"}

    def test_non_english_only_descriptions_removed(self) -> None:
        entity = _make_entity(
            descriptions={"fr": {"value": "French only"}},
        )
        line = json.dumps(entity)
        results = self._run_filtered_lines([line])

        assert len(results) == 1
        assert "descriptions" not in results[0][0]

    def test_labels_english_extraction(self) -> None:
        entity = _make_entity(
            labels={"en": {"value": "English label"}, "de": {"value": "German"}},
        )
        line = json.dumps(entity)
        results = self._run_filtered_lines([line])

        assert len(results) == 1
        assert results[0][0]["labels"] == {"value": "English label"}

    def test_aliases_english_extraction(self) -> None:
        entity = _make_entity(
            aliases={"en": [{"value": "Alias1"}], "ja": [{"value": "Japanese"}]},
        )
        line = json.dumps(entity)
        results = self._run_filtered_lines([line])

        assert len(results) == 1
        assert results[0][0]["aliases"] == [{"value": "Alias1"}]

    def test_sitelinks_removed(self) -> None:
        entity = _make_entity(
            descriptions={"en": {"value": "Test"}},
            sitelinks={"enwiki": {"title": "Test"}},
        )
        line = json.dumps(entity)
        results = self._run_filtered_lines([line])

        assert len(results) == 1
        assert "sitelinks" not in results[0][0]

    def test_malformed_json_skipped(self) -> None:
        """Malformed JSON lines are skipped with a warning."""
        good = _make_entity(descriptions={"en": {"value": "Good"}})
        lines = [
            '{"latitude": broken json',
            json.dumps(good),
        ]
        results = self._run_filtered_lines(lines)

        assert len(results) == 1
        assert results[0][0]["id"] == "Q42"

    def test_trailing_comma_stripped(self) -> None:
        """Wikidata dumps have trailing commas; they should be stripped."""
        entity = _make_entity(descriptions={"en": {"value": "Test"}})
        line = json.dumps(entity) + ","
        results = self._run_filtered_lines([line])

        assert len(results) == 1

    def test_multiple_entities_counted(self) -> None:
        entity1 = _make_entity(
            descriptions={"en": {"value": "First"}},
            extra={"id": "Q1"},
        )
        entity2 = _make_entity(
            descriptions={"en": {"value": "Second"}},
            extra={"id": "Q2"},
        )
        # Interleave with a non-matching line
        lines = [
            json.dumps(entity1),
            '{"id": "Q3", "no_coords": true}',
            json.dumps(entity2),
        ]
        results = self._run_filtered_lines(lines)

        assert len(results) == 2
        assert results[0][0]["id"] == "Q1"
        assert results[1][0]["id"] == "Q2"
        # Line numbers should reflect actual position
        assert results[0][1] == 1
        assert results[1][1] == 3
