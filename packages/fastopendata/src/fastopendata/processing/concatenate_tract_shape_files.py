from shared.logger import LOGGER

from fastopendata.config import config
from fastopendata.processing.concatenate_shape_files import (
    concatenate_shapefiles,
)

if __name__ == "__main__":
    # Use centralized configuration (supports DATA_DIR environment override)
    data_dir = config.data_path

    LOGGER.info("Using data directory: %s", data_dir)

    shape_files = [
        f.name
        for f in data_dir.iterdir()
        if "_tract" in f.name.lower() and f.suffix == ".shp"
    ]
    LOGGER.info("Found %d tract shapefiles.", len(shape_files))

    output_path = config.get_dataset_path(dataset_name="combined_tiger_tracts")
    LOGGER.info("Output file: %s", output_path)

    concatenate_shapefiles(
        shape_files,
        data_dir,
        output_path,
        columns=["GEOID", "geometry"],
    )
