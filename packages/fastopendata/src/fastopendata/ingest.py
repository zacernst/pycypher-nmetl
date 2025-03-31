"""Create data for FastOpenData"""
# pylint: disable=invalid-name,missing-function-docstring,disallowed-name,protected-access,unused-argument,unused-import,redefined-outer-name,too-many-lines

import json

from nmetl.configuration import load_session_config
from nmetl.data_asset import DataAsset
from nmetl.data_source import NewColumn
from nmetl.trigger import VariableAttribute
from pycypher.logger import LOGGER

LOGGER.setLevel("INFO")
INGEST_CONFIG_PATH = (
    "/Users/zernst/git/pycypher-nmetl/packages/"
    "fastopendata/src/fastopendata/ingest.yaml"
)
session = load_session_config(INGEST_CONFIG_PATH)

with open(
    "/Users/zernst/git/pycypher-nmetl/packages/fastopendata/src/"
    "fastopendata/acs_pums_2023_data_dictionary.json",
    "r",
    encoding="utf-8",
) as f:
    session.register_data_asset(
        DataAsset(name="acs_pums_2023_data_dictionary", obj=json.load(f))
    )


@session.new_column("state_county_tract_puma")
def state_county_tract(STATEFP, COUNTYFP, TRACTCE) -> NewColumn["tract_fips"]:
    out = STATEFP + COUNTYFP + TRACTCE
    return out


@session.new_column("state_county_tract_puma")
def state_county(STATEFP, COUNTYFP) -> NewColumn["county_fips"]:
    out = STATEFP + COUNTYFP
    return out


@session.trigger(
    "MATCH (i:PSAM_2023_Individual) WITH i.MIL AS military RETURN military"
)
def active_duty(military) -> VariableAttribute["i", "active_duty"]:
    return military == "1"


@session.trigger(
    "MATCH (i:PSAM_2023_Individual) WITH i.MIL AS military RETURN military"
)
def active_duty_in_past(
    military,
) -> VariableAttribute["i", "active_duty_in_past"]:
    return military == "2"


@session.trigger(
    "MATCH (i:PSAM_2023_Individual) WITH i.MIL AS military RETURN military"
)
def reserves_national_guard(
    military,
) -> VariableAttribute["i", "reserves_national_guard"]:
    return military == "3"


@session.trigger(
    "MATCH (i:PSAM_2023_Individual) WITH i.MIL AS military RETURN military"
)
def never_served_in_military(
    military,
) -> VariableAttribute["i", "never_served_in_military"]:
    """Never served in the military"""
    return military == "3"


@session.trigger(
    "MATCH (i:PSAM_2023_Individual) WITH i.MIL AS military RETURN military"
)
def some_military(military) -> VariableAttribute["i", "some_military"]:
    """Currently serving or served in the military previously"""
    return military in ["1", "2", "3"]


@session.trigger(
    "MATCH (i:PSAM_2023_Individual) WITH i.MLPA AS military RETURN military"
)
def military_after_sept_11(
    military,
) -> VariableAttribute["i", "military_after_sept_11"]:
    """Served in the military after September 11, 2001"""
    return military == "1"


@session.trigger(
    "MATCH (i:PSAM_2023_Individual) WITH i.DEYE AS eye RETURN eye"
)
def vision_difficulty(
    eye, acs_pums_2023_data_dictionary
) -> VariableAttribute["i", "vision_difficulty"]:
    """Has vision difficulty"""
    return acs_pums_2023_data_dictionary["DEYE"][eye]


@session.trigger(
    "MATCH (i:PSAM_2023_Individual) WITH i.DPHY AS physical RETURN physical"
)
def physical_difficulty(
    physical, acs_pums_2023_data_dictionary
) -> VariableAttribute["i", "physical_difficulty"]:
    """Has physical difficulty"""
    return acs_pums_2023_data_dictionary["DPHY"][physical]




LOGGER.setLevel("INFO")
session()
# session.start_threads()
