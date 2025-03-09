"""Load configuration from toml file and make available."""

import os
from pathlib import Path

import toml

CWD = Path(os.getcwd())
SRC_BASE_DIR = Path(__file__).parent.parent.parent
MONOREPO_BASE_DIR = SRC_BASE_DIR.parent.parent

config_file = Path(__file__).parent / "config" / "config.toml"
with open(config_file, "r", encoding="utf8") as f:
    config = toml.load(f)

for key, value in config["global"].items():
    globals()[key] = value

globals()["SRC_BASE_DIR"] = SRC_BASE_DIR
globals()["MONOREPO_BASE_DIR"] = MONOREPO_BASE_DIR
globals()["CWD"] = CWD
