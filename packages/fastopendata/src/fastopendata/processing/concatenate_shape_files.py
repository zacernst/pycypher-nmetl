"""Concatenate a set of shapefiles into a single output shapefile.

Used to merge the per-state TIGER/Line block-group shapefiles
(tl_2024_*_bg.shp) into one national file: $DATA_DIR/combined_block_groups.shp.

Columns retained: BLKGRPCE, GEOID, geometry.

Usage:
    DATA_DIR=/path/to/raw_data uv run python concatenate_shape_files.py
"""

from pathlib import Path

import pyogrio
from shared.logger import LOGGER


def concatenate_shapefiles(
    file_list: list[str],
    file_directory: str | Path,
    output_file: str | Path,
    columns: list[str] | None = None,
) -> None:
    """Read *file_list* from *file_directory* and write a merged shapefile.

    Reads one shapefile at a time and appends to the output to avoid loading
    all data into memory simultaneously.

    Args:
        file_list: Basenames of the shapefiles to merge.
        file_directory: Directory that contains them.
        output_file: Absolute path for the merged output shapefile.
        columns: Optional list of columns to retain (geometry is always kept).

    """
    if not file_list:
        msg = (
            f"No shapefiles to concatenate in {file_directory}. "
            "Check that upstream extraction completed successfully."
        )
        raise FileNotFoundError(msg)

    LOGGER.info("Concatenating %d shapefiles...", len(file_list))
    succeeded = 0
    failed: list[tuple[str, str]] = []
    output_path = Path(output_file)

    for i, name in enumerate(file_list):
        path = Path(file_directory) / name
        LOGGER.info("Reading %s (%d/%d)...", path, i + 1, len(file_list))
        try:
            frame = (
                pyogrio.read_dataframe(path, columns=columns)
                if columns
                else pyogrio.read_dataframe(path)
            )
        except Exception as exc:
            LOGGER.warning(
                "Skipping corrupt or unreadable shapefile %s: %s", path, exc
            )
            failed.append((name, str(exc)))
            continue

        if succeeded == 0:
            # First file — write fresh, establishing schema
            pyogrio.write_dataframe(frame, output_path)
        else:
            # Subsequent files — append to existing output
            pyogrio.write_dataframe(frame, output_path, append=True)
        succeeded += 1
        del frame  # Free memory immediately

    if succeeded == 0:
        msg = (
            f"All {len(file_list)} shapefiles failed to read. "
            f"First failure: {failed[0][0]}: {failed[0][1]}"
        )
        raise RuntimeError(msg)

    if failed:
        LOGGER.warning(
            "%d of %d shapefiles skipped due to errors",
            len(failed),
            len(file_list),
        )

    LOGGER.info("Done — merged %d shapefiles to %s.", succeeded, output_path)


if __name__ == "__main__":
    from fastopendata.config import config

    data_dir = config.data_path
    shapefiles = [f.name for f in data_dir.iterdir() if f.suffix == ".shp" and
                  "bg" in f.name]
    concatenate_shapefiles(
        shapefiles,
        data_dir,
        data_dir / "combined_shape_files.shp",
        columns=["BLKGRPCE", "GEOID", "geometry"],
    )
