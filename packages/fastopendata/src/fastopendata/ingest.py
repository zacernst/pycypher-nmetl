'''Create data for FastOpenData'''

from nmetl.configuration import load_session_config
from nmetl.data_source import NewColumn
from pycypher.logger import LOGGER

LOGGER.setLevel("INFO")
INGEST_CONFIG_PATH = "/Users/zernst/git/pycypher-nmetl/packages/fastopendata/src/fastopendata/ingest.yaml"
session = load_session_config(INGEST_CONFIG_PATH)

@session.new_column("state_county_tract_puma")
def state_county_tract_puma(STATEFP, COUNTYFP, TRACTCE) -> NewColumn["tract_fips"]:
    out = STATEFP + COUNTYFP + TRACTCE
    return out


@session.new_column("state_county_tract_puma")
def state_county(STATEFP, COUNTYFP) -> NewColumn["county_fips"]:
    out = STATEFP + COUNTYFP
    return out

session()