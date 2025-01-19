"""Load configuration from toml file and make available."""

from pathlib import Path

import toml

config_file = Path(__file__).parent.parent / "config" / "config.toml"
with open(config_file, "r", encoding="utf8") as f:
    config = toml.load(f)

for key, value in config["global"].items():
    globals()[key] = value
