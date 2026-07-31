"""
Extract and combine the USAspending FY2025 "All Contracts Full" archive.

The zip contains one or more numbered CSV parts (e.g.
``FY2025_All_Contracts_Full_<date>_1.csv``, ``..._2.csv``, ...) whose
filenames embed whatever date USAspending stamped on that release. Since
the zip is downloaded under a fixed local name (see
``rule download_fy2025_contracts`` / ``resolve_fy2025_contracts_filename.py``),
those part filenames aren't known ahead of time -- this script discovers
them from the zip's own member list, so it doesn't need to guess a date.

Parts are concatenated into a single combined CSV: the header row is taken
from the first part only, and each subsequent part's header row is skipped
(matching what the per-state sample rule already does with
``head -1`` / ``tail -n +2``). The extracted per-part CSVs are removed after
a successful combine so a re-run doesn't leave multi-gigabyte duplicates
alongside the combined file.

Usage
-----
    uv run python -m fastopendata.processing.extract_fy2025_contracts \\
        <input_zip> <output_csv>
"""

from __future__ import annotations

import shutil
import sys
import zipfile
from pathlib import Path


def extract_and_combine(zip_path: Path, output_csv: Path) -> Path:
    extract_dir = zip_path.parent

    with zipfile.ZipFile(zip_path) as zf:
        part_names = [name for name in zf.namelist() if name.endswith(".csv")]
        if not part_names:
            raise RuntimeError(f"No CSV members found in {zip_path}")
        zf.extractall(extract_dir, members=part_names)

    part_paths = sorted(extract_dir / name for name in part_names)

    with output_csv.open("wb") as out_f:
        for i, part_path in enumerate(part_paths):
            with part_path.open("rb") as part_f:
                header = part_f.readline()
                if i == 0:
                    out_f.write(header)
                shutil.copyfileobj(part_f, out_f)

    for part_path in part_paths:
        part_path.unlink()

    return output_csv


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "usage: extract_fy2025_contracts.py <input_zip> <output_csv>",
            file=sys.stderr,
        )
        sys.exit(1)

    extract_and_combine(Path(sys.argv[1]), Path(sys.argv[2]))
