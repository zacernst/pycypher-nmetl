r"""Filter and slim a Wikidata JSON dump to geolocated entities only.

Reads newline-delimited JSON from stdin (after bzip2 decompression) and
writes one JSON object per line to stdout, keeping only entities that:
  - contain the string "latitude" (fast pre-filter before JSON parsing)
  - have at least one English description, label, or alias

Non-English variants of descriptions/labels/aliases are discarded; the
sitelinks block is removed entirely.  The result is re-compressed by the
caller.

Usage (full pipeline):
    wget -O - https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2 \\
      | bunzip2 -c \\
      | uv run python compress_wikidata.py \\
      | bzip2 -c \\
      > $DATA_DIR/wikidata_compressed.json.bz2

The script reports progress via a rich progress bar written to stderr.
The total line count (98,631,069,547) is an approximation from the
2024 full dump; adjust if running against a different vintage.
"""

import json
import sys
from collections.abc import Generator
from typing import Any

import rich  # noqa: F401  (imported for side-effects: enables rich tracebacks)
from rich.progress import MofNCompleteColumn, Progress, SpinnerColumn

_APPROX_TOTAL_LINES = 98_631_069_547

_total_lines = 0


def _filtered_lines() -> Generator[tuple[dict[str, Any], int]]:
    """Yield (entity_dict, line_number) for geolocated entities."""
    global _total_lines
    for raw in sys.stdin:
        _total_lines += 1
        # Cheap string check before paying for JSON parsing.
        if len(raw) < 3 or "latitude" not in raw:
            continue
        entity: dict[str, Any] = json.loads(raw.rstrip(",\n"))
        # Keep only the English facets of multilingual fields.
        for field in ("descriptions", "labels", "aliases"):
            if field in entity and "en" in entity[field]:
                entity[field] = entity[field]["en"]
            else:
                entity.pop(field, None)
        entity.pop("sitelinks", None)
        yield entity, _total_lines


with Progress(
    SpinnerColumn(),
    *Progress.get_default_columns(),
    MofNCompleteColumn(),
) as progress:
    task = progress.add_task(
        "Filtering Wikidata...", total=_APPROX_TOTAL_LINES
    )
    for entity, line_number in _filtered_lines():
        sys.stdout.write(json.dumps(entity) + "\n")
        progress.update(task, completed=line_number)
