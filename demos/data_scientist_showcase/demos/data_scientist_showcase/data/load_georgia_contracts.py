"""Load and process Georgia federal contract data for showcase demos.

Reads raw FY2025 contract CSVs from the fastopendata package, filters to
Georgia (FIPS 13), and selects a useful subset of columns for graph-based
analysis in the demonstration scripts.

The raw data lives at::

    packages/fastopendata/FY2025_All_Contracts_Full_20260307_*.csv

This module produces a trimmed DataFrame with the most interesting columns
for demonstrating real-world data messiness, geographic analysis, and
agency–vendor relationship graphs.
"""

from __future__ import annotations

import csv
import sys
from glob import glob
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GEORGIA_FIPS = "13"

# Raw data location (relative to project root)
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_FASTOPENDATA_DIR = _PROJECT_ROOT / "packages" / "fastopendata"

# Columns used for state filtering
_RECIPIENT_STATE_FIPS = "prime_award_transaction_recipient_state_fips_code"
_POP_STATE_FIPS = "prime_award_transaction_place_of_performance_state_fips_code"

# Columns to keep for demos (subset of the 297 available)
DEMO_COLUMNS = [
    # Identifiers
    "contract_transaction_unique_key",
    "award_id_piid",
    # Financial
    "federal_action_obligation",
    "total_dollars_obligated",
    # Dates
    "action_date",
    "action_date_fiscal_year",
    "period_of_performance_start_date",
    "period_of_performance_current_end_date",
    # Agency
    "awarding_agency_code",
    "awarding_agency_name",
    "awarding_sub_agency_name",
    "funding_agency_name",
    # Recipient / vendor
    "recipient_name",
    "recipient_city_name",
    "recipient_county_name",
    "recipient_state_name",
    _RECIPIENT_STATE_FIPS,
    "recipient_zip_4_code",
    # Place of performance
    "primary_place_of_performance_city_name",
    "primary_place_of_performance_county_name",
    "primary_place_of_performance_state_name",
    _POP_STATE_FIPS,
    "prime_award_transaction_place_of_performance_county_fips_code",
    # Classification
    "naics_code",
    "naics_description",
    "product_or_service_code_description",
    "transaction_description",
]

# Georgia county FIPS range (13001–13321, odd numbers only for GA)
GA_COUNTY_FIPS_MIN = "13001"
GA_COUNTY_FIPS_MAX = "13321"


# ---------------------------------------------------------------------------
# Core loader
# ---------------------------------------------------------------------------

def load_georgia_contracts(
    *,
    data_dir: Path | None = None,
    contract_glob: str = "FY2025_All_Contracts_Full_*.csv",
    max_rows: int | None = None,
    columns: list[str] | None = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """Load Georgia contract records from the raw FY2025 CSVs.

    Streams through the raw CSV files, filtering to rows where either the
    recipient state or the place of performance is Georgia (FIPS 13).

    Parameters
    ----------
    data_dir:
        Directory containing the raw CSV files.  Defaults to the
        ``packages/fastopendata/`` directory in the project root.
    contract_glob:
        Glob pattern for input files within *data_dir*.
    max_rows:
        If set, stop after collecting this many matching rows.
        Useful for quick demos that don't need the full ~34K rows.
    columns:
        Columns to keep.  Defaults to :data:`DEMO_COLUMNS`.
    verbose:
        Print progress messages.

    Returns
    -------
    pd.DataFrame
        Georgia contract records with the selected columns.
    """
    if data_dir is None:
        data_dir = _FASTOPENDATA_DIR
    if columns is None:
        columns = list(DEMO_COLUMNS)

    pattern = str(data_dir / contract_glob)
    input_files = sorted(glob(pattern))

    if not input_files:
        print(f"ERROR: No files matched {pattern}", file=sys.stderr)
        print(
            "Hint: ensure the raw FY2025 contract CSVs are in "
            f"{data_dir}",
            file=sys.stderr,
        )
        return pd.DataFrame(columns=columns)

    if verbose:
        print(f"Loading Georgia contracts from {len(input_files)} files...")

    rows: list[dict[str, str]] = []
    total_scanned = 0

    for filepath in input_files:
        file_rows = 0
        fname = Path(filepath).name
        if verbose:
            print(f"  Scanning {fname}...", end=" ", flush=True)

        with open(filepath, newline="", buffering=1_048_576) as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_scanned += 1
                recipient_fips = row.get(_RECIPIENT_STATE_FIPS, "")
                pop_fips = row.get(_POP_STATE_FIPS, "")

                if recipient_fips == GEORGIA_FIPS or pop_fips == GEORGIA_FIPS:
                    # Keep only requested columns (handle missing gracefully)
                    trimmed = {col: row.get(col, "") for col in columns}
                    rows.append(trimmed)
                    file_rows += 1

                    if max_rows is not None and len(rows) >= max_rows:
                        break

        if verbose:
            print(f"{file_rows} Georgia rows")

        if max_rows is not None and len(rows) >= max_rows:
            break

    if verbose:
        print(f"Total: {len(rows)} Georgia rows from {total_scanned:,} scanned")

    df = pd.DataFrame(rows, columns=columns)

    # Coerce numeric columns
    for col in ["federal_action_obligation", "total_dollars_obligated"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def load_georgia_sample(n: int = 500, *, seed: int = 42) -> pd.DataFrame:
    """Load a small reproducible sample of Georgia contracts.

    Loads all Georgia records, then samples *n* rows deterministically.
    Suitable for quick demos where full data isn't needed.
    """
    full = load_georgia_contracts(verbose=False)
    if len(full) == 0:
        return full
    if len(full) <= n:
        return full
    return full.sample(n=n, random_state=seed).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Graph-oriented views (for PyCypher demos)
# ---------------------------------------------------------------------------

def contracts_as_graph(
    df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Convert a contracts DataFrame into entity/relationship DataFrames.

    Creates a graph with:
    - **Agency** entities (awarding agencies)
    - **Vendor** entities (recipients)
    - **AWARDED_TO** relationships (agency → vendor, with contract details)

    Returns a dict suitable for ``ContextBuilder.from_dict()``.
    """
    # Build Agency entities from unique awarding agencies
    agencies = (
        df[["awarding_agency_code", "awarding_agency_name"]]
        .drop_duplicates(subset=["awarding_agency_code"])
        .reset_index(drop=True)
    )
    agencies = agencies.rename(columns={
        "awarding_agency_code": "__ID__",
        "awarding_agency_name": "name",
    })

    # Build Vendor entities from unique recipients
    vendors = (
        df[["recipient_name", "recipient_city_name", "recipient_state_name"]]
        .drop_duplicates(subset=["recipient_name"])
        .reset_index(drop=True)
    )
    vendors.insert(0, "__ID__", range(100_001, 100_001 + len(vendors)))
    vendors = vendors.rename(columns={
        "recipient_name": "name",
        "recipient_city_name": "city",
        "recipient_state_name": "state",
    })

    # Build a vendor name → ID lookup
    vendor_id_map = dict(zip(vendors["name"], vendors["__ID__"]))

    # Build AWARDED_TO relationships (agency → vendor)
    rels = df[
        ["awarding_agency_code", "recipient_name",
         "federal_action_obligation", "action_date", "naics_description"]
    ].copy()
    rels = rels.rename(columns={
        "awarding_agency_code": "__SOURCE__",
        "federal_action_obligation": "obligation",
        "action_date": "date",
        "naics_description": "sector",
    })
    rels["__TARGET__"] = rels["recipient_name"].map(vendor_id_map)
    rels = rels.drop(columns=["recipient_name"])
    rels.insert(0, "__ID__", range(200_001, 200_001 + len(rels)))

    return {
        "Agency": agencies,
        "Vendor": vendors,
        "AWARDED_TO": rels,
    }


# ---------------------------------------------------------------------------
# CLI entry point (for standalone extraction)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract Georgia contracts to CSV for demos",
    )
    parser.add_argument(
        "-o", "--output",
        default="contracts_sample_ga.csv",
        help="Output CSV filename (default: contracts_sample_ga.csv)",
    )
    parser.add_argument(
        "-n", "--max-rows",
        type=int,
        default=None,
        help="Maximum rows to extract (default: all)",
    )
    args = parser.parse_args()

    df = load_georgia_contracts(max_rows=args.max_rows)
    out_path = Path(__file__).parent / args.output
    df.to_csv(out_path, index=False)
    print(f"\nWrote {len(df)} rows to {out_path}")
