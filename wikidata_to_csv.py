#!/usr/bin/env python3
"""
Convert Wikidata JSON lines (from bz2) to fully flattened CSV format.

Produces two CSV files:
  1. items.csv - Main item metadata (id, label, description)
  2. snaks.csv - Fully flattened snaks (one row per snak, no JSON parsing needed)
"""

import bz2
import json
import csv
import sys
from pathlib import Path
from typing import Any, Optional
from collections import defaultdict


def extract_label(labels: Any) -> Optional[str]:
    """Extract the first label (usually English) or any available label."""
    if isinstance(labels, dict):
        return labels.get("value")
    return None


def extract_description(descriptions: Any) -> Optional[str]:
    """Extract the first description (usually English) or any available."""
    if isinstance(descriptions, dict):
        return descriptions.get("value")
    return None


def flatten_snak_value(value_obj: Any) -> str:
    """Extract a string representation of a snak value."""
    if isinstance(value_obj, dict):
        # For entityid types, extract the ID
        if "id" in value_obj:
            return value_obj["id"]
        # For quantity/time/etc, extract key fields
        if "amount" in value_obj:
            return value_obj["amount"]
        if "time" in value_obj:
            return value_obj["time"]
        # Fallback: JSON encode
        return json.dumps(value_obj)
    return str(value_obj) if value_obj else ""


def flatten_claim(item_id: str, property_id: str, claim: dict) -> list[dict]:
    """
    Flatten a single claim into one row per snak.

    Each snak (mainsnak + qualifiers) becomes a separate CSV row.
    Returns a list of dicts, one per snak.
    """
    rows = []
    rank = claim.get("rank", "normal")
    claim_id = claim.get("id", "")

    # Add row for mainsnak
    mainsnak = claim.get("mainsnak", {})
    if mainsnak:
        datavalue = mainsnak.get("datavalue", {})
        value_obj = datavalue.get("value", "")
        value_str = flatten_snak_value(value_obj)

        rows.append({
            "item_id": item_id,
            "claim_property": property_id,
            "claim_id": claim_id,
            "rank": rank,
            "snak_property": mainsnak.get("property", ""),
            "snak_value": value_str,
            "snak_datatype": mainsnak.get("datatype", ""),
            "snak_snaktype": mainsnak.get("snaktype", "value"),
            "snak_hash": mainsnak.get("hash", ""),
            "snak_type": "mainsnak",
        })

    # Add rows for each qualifier snak
    qualifiers = claim.get("qualifiers", {})
    for qualifier_property, snak_list in qualifiers.items():
        for qualifier_snak in snak_list:
            datavalue = qualifier_snak.get("datavalue", {})
            value_obj = datavalue.get("value", "")
            value_str = flatten_snak_value(value_obj)

            rows.append({
                "item_id": item_id,
                "claim_property": property_id,
                "claim_id": claim_id,
                "rank": rank,
                "snak_property": qualifier_snak.get("property", ""),
                "snak_value": value_str,
                "snak_datatype": qualifier_snak.get("datatype", ""),
                "snak_snaktype": qualifier_snak.get("snaktype", "value"),
                "snak_hash": qualifier_snak.get("hash", ""),
                "snak_type": "qualifier",
            })

    return rows


def process_wikidata_bz2(input_file: Path, output_dir: Path, max_items: Optional[int] = None):
    """
    Read Wikidata JSON lines from bz2 and write to CSV files.

    Args:
        input_file: Path to .bz2 file
        output_dir: Directory to write CSV files to
        max_items: Max items to process (None = all)
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    items_file = output_dir / "items.csv"
    claims_file = output_dir / "claims.csv"

    items_data = []
    claims_data = []

    item_count = 0
    claim_count = 0
    error_count = 0

    print(f"Reading from {input_file}...", file=sys.stderr)

    with bz2.open(input_file, "rt", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            if max_items and item_count >= max_items:
                break

            try:
                blob = json.loads(line.strip())

                # Only process items
                if blob.get("type") != "item":
                    continue

                item_id = blob.get("id")
                label = extract_label(blob.get("labels"))
                description = extract_description(blob.get("descriptions"))

                # Add to items
                items_data.append({
                    "id": item_id,
                    "label": label or "",
                    "description": description or "",
                })

                # Process claims
                claims_dict = blob.get("claims", {})
                for property_id, claim_list in claims_dict.items():
                    for claim in claim_list:
                        flattened = flatten_claim(item_id, property_id, claim)
                        claims_data.extend(flattened)
                        claim_count += len(flattened)

                item_count += 1

                if item_count % 1000 == 0:
                    print(f"  Processed {item_count} items, {claim_count} snaks...", file=sys.stderr)

            except json.JSONDecodeError as e:
                error_count += 1
                if error_count <= 5:
                    print(f"  Line {line_num}: JSON error: {e}", file=sys.stderr)
            except Exception as e:
                error_count += 1
                if error_count <= 5:
                    print(f"  Line {line_num}: {type(e).__name__}: {e}", file=sys.stderr)

    # Write items CSV
    print(f"\nWriting {len(items_data)} items to {items_file}...", file=sys.stderr)
    with open(items_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "label", "description"])
        writer.writeheader()
        writer.writerows(items_data)

    # Write snaks CSV (one row per snak)
    print(f"Writing {len(claims_data)} snaks to {claims_file}...", file=sys.stderr)
    if claims_data:
        fieldnames = ["item_id", "claim_property", "claim_id", "rank",
                      "snak_property", "snak_value", "snak_datatype", "snak_snaktype",
                      "snak_hash", "snak_type"]
        with open(claims_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(claims_data)

    print(f"\nDone! Processed {item_count} items, {claim_count} snaks", file=sys.stderr)
    if error_count:
        print(f"Errors: {error_count} (show first 5 above)", file=sys.stderr)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to wikidata_compressed.json.bz2",
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=Path,
        default=Path("wikidata_csv"),
        help="Output directory for CSV files (default: wikidata_csv)",
    )
    parser.add_argument(
        "-n", "--max-items",
        type=int,
        help="Max items to process (default: all)",
    )

    args = parser.parse_args()

    if not args.input_file.exists():
        print(f"Error: {args.input_file} not found", file=sys.stderr)
        sys.exit(1)

    process_wikidata_bz2(args.input_file, args.output_dir, args.max_items)
