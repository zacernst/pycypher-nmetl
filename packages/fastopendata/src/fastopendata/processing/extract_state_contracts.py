"""Extract state-specific subsets from FY2025 federal contract CSV files.

Filters the split contract CSVs by recipient state FIPS code and/or place of
performance state FIPS code, producing a single merged output CSV containing
all rows where the specified state appears in either field.

Usage:
    STATE_FIPS=13 DATA_DIR=raw_data uv run python extract_state_contracts.py

Environment variables:
    STATE_FIPS  — 2-digit FIPS code (e.g. "13" for Georgia)
    DATA_DIR    — directory containing the source CSV files (default: raw_data)
    CONTRACT_GLOB — glob pattern for input files (default: FY2025_All_Contracts_Full_*.csv)
"""

from __future__ import annotations

import csv
import os
import re
import sys
from glob import glob
from pathlib import Path

_FIPS_RE = re.compile(r"^\d{2}\Z")

# Column names used for state filtering
RECIPIENT_STATE_FIPS_COL = "prime_award_transaction_recipient_state_fips_code"
POP_STATE_FIPS_COL = "prime_award_transaction_place_of_performance_state_fips_code"


def extract_state_contracts(
    state_fips: str,
    data_dir: Path,
    contract_glob: str = "FY2025_All_Contracts_Full_*.csv",
    output_filename: str | None = None,
) -> Path:
    """Filter contract CSVs to rows matching a state FIPS code.

    A row is included if the recipient state FIPS *or* the place of
    performance state FIPS matches ``state_fips``.

    Parameters
    ----------
    state_fips:
        Two-digit state FIPS code (e.g. ``"13"`` for Georgia).
    data_dir:
        Directory containing the source CSV files.
    contract_glob:
        Glob pattern to find input CSV files within *data_dir*.
    output_filename:
        Name of the output file. Defaults to
        ``contracts_state_{fips}.csv``.

    Returns
    -------
    Path to the written output file inside *data_dir*.
    """
    if not _FIPS_RE.match(state_fips):
        raise ValueError(
            f"Invalid state FIPS code {state_fips!r}: must be exactly "
            f"two digits (e.g. '13' for Georgia, '06' for California)"
        )

    if output_filename is None:
        output_filename = f"contracts_state_{state_fips}.csv"

    output_path = data_dir / output_filename

    # Find all contract CSV files inside data_dir
    pattern = str(data_dir / contract_glob)
    input_files = sorted(glob(pattern))

    if not input_files:
        raise FileNotFoundError(
            f"No files matched pattern {pattern}"
        )

    print(f"Extracting state FIPS {state_fips} from {len(input_files)} files")
    print(f"Output: {output_path}")

    header: list[str] | None = None
    recipient_idx: int = -1
    pop_idx: int = -1
    total_rows = 0

    with open(output_path, "w", newline="", buffering=1_048_576) as out_f:
        writer = csv.writer(out_f)

        for filepath in input_files:
            file_rows = 0
            print(f"  Processing {Path(filepath).name}...", end=" ", flush=True)

            with open(filepath, newline="", buffering=1_048_576) as in_f:
                reader = csv.reader(in_f)
                file_header = next(reader)

                if header is None:
                    header = file_header
                    writer.writerow(header)
                    recipient_idx = header.index(RECIPIENT_STATE_FIPS_COL)
                    pop_idx = header.index(POP_STATE_FIPS_COL)

                for row in reader:
                    if row[recipient_idx] == state_fips or row[pop_idx] == state_fips:
                        writer.writerow(row)
                        file_rows += 1

            total_rows += file_rows
            print(f"{file_rows} rows")

    print(f"Total: {total_rows} rows written to {output_path}")
    return output_path


def main() -> None:
    state_fips = os.environ.get("STATE_FIPS")
    if not state_fips:
        print("ERROR: STATE_FIPS environment variable required", file=sys.stderr)
        sys.exit(1)

    data_dir = Path(os.environ.get("DATA_DIR", "raw_data"))
    contract_glob = os.environ.get(
        "CONTRACT_GLOB", "FY2025_All_Contracts_Full_*.csv"
    )

    try:
        extract_state_contracts(
            state_fips=state_fips,
            data_dir=data_dir,
            contract_glob=contract_glob,
        )
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
