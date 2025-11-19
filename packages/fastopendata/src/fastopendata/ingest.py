# pyrefly: ignore-all-errors
"""Create data for FastOpenData"""
# pylint: disable=invalid-name,missing-function-docstring,disallowed-name,protected-access,unused-argument,unused-import,redefined-outer-name,too-many-lines

import base64
import functools
import json
import pickle
from typing import TYPE_CHECKING, LiteralString

from nmetl.data_asset import DataAsset
from nmetl.data_source import NewColumn
from nmetl.session import Session
from nmetl.trigger import VariableAttribute
from nmetl.worker_context import WorkerContext
from pycypher.fact_collection import FactCollection
from pycypher.fact_collection.foundationdb import FoundationDBFactCollection
from shared.logger import LOGGER

SOURCE_DIR = (
    "/app/packages/fastopendata/"
)

INGEST_CONFIG_PATH: LiteralString = f"packages/fastopendata/ingest.yaml"
PUMS_DATA_DICTIONARY_PATH: LiteralString = f"packages/fastopendata/data_assets/acs_pums_2023_data_dictionary.json"

if __name__ == "__main__":

    # def setup_worker_data(session: Session):
    #     """Setup worker data without Dask."""
    #     
    #     # Broadcast data assets to worker context
    #     for asset_name, data_asset in session.data_assets.items():
    #         WorkerContext.set_global_data(asset_name, data_asset.obj)
    #     
    #     # Broadcast trigger dictionary
    #     import dill
    #     trigger_dict_bytes = dill.dumps(session.trigger_dict)
    #     WorkerContext.set_global_data('trigger_dict', trigger_dict_bytes)

    # Use thread manager instead

    session = Session(session_config_file="packages/fastopendata/sample_session_config.toml")
    LOGGER.setLevel(session.configuration.logging_level.value)
    LOGGER.info('Loaded session...')
    
    
    # Setup worker data
    # setup_worker_data(session)

    fact_collection_kwargs: dict[str, str | int | float | bool] = {}

    fact_collection: FactCollection = FoundationDBFactCollection(
        foundationdb_cluster_file=session.configuration.foundationdb_cluster_file)

    with open(
        PUMS_DATA_DICTIONARY_PATH,
        "r",
        encoding="utf-8",
    ) as f:
        acs_pums_2023_data_dictionary: DataAsset = DataAsset(
            name="acs_pums_2023_data_dictionary", obj=json.load(f)
        )


    session.register_data_asset(acs_pums_2023_data_dictionary)
    #def send_data_assets(session) -> None:
    #    for data_asset in session.data_assets:
    #        thread_manager.register_callback(
    #            functools.partial(send_data_asset, data_asset=data_asset)
    #        )

    @session.new_column("state_county_tract_puma")
    def state_county_tract(
        STATEFP, COUNTYFP, TRACTCE
    ) -> NewColumn["tract_fips"]:
        '''FIPS code for the tract, including county and state'''
        out = STATEFP + COUNTYFP + TRACTCE
        LOGGER.debug('state_county_tract')
        return out

    @session.new_column("state_county_tract_puma")
    def state_county(STATEFP, COUNTYFP) -> NewColumn["county_fips"]:
        '''FIPS code for the county, including state'''
        out = STATEFP + COUNTYFP
        return out

    # @session.new_column("united_states_nodes")
    # def get_osm_tags(encoded_tags) -> NewColumn["tags"]:
    #     '''All the OSM tags for the point'''
    #     out = pickle.loads(base64.b64decode(encoded_tags))
    #     return out

    @session.trigger(
        "MATCH (i:PSAM_2023_Individual) WITH i.MIL AS military RETURN military"
    )
    def active_duty(military) -> VariableAttribute["i", "active_duty"]:
        '''Actively serving in the military'''
        LOGGER.warning('In active_duty trigger: %s', military)
        return military == "1"

    @session.trigger(
        "MATCH (i:PSAM_2023_Individual) WITH i.STATE AS state RETURN state"
    )
    def psam_state_name(
        state, acs_pums_2023_data_dictionary
    ) -> VariableAttribute["i", "state_name"]:
        '''State name'''
        lookup: str | None = acs_pums_2023_data_dictionary["STATE"].get(
            state, None
        )
        LOGGER.debug('>>>>> psam_state_name')
        return lookup

    @session.trigger(
        "MATCH (i:PSAM_2023_Individual) WITH i.PUMA AS puma RETURN puma"
    )
    def puma_fips(
        puma
    ) -> VariableAttribute["i", "puma_fips"]:
        '''FIPS code for PUMA (public use microdata area)'''
        return puma

    @session.trigger(
        "MATCH (t:Tract)-[r:in]->(c:County) WITH COLLECT(t.tract_fips) AS tracts RETURN tracts"
    )
    def number_of_tracts(tracts) -> VariableAttribute["c", "num_tracts"]:
        '''Count the number of tracts in the county'''
        LOGGER.warning('Calculating the number of tracts')
        return len(tracts)

    @session.trigger(
        "MATCH (i:PSAM_2023_Individual) WITH i.puma_fips "
        "AS puma_fips, i.state_fips AS state_fips "
        "RETURN puma_fips, state_fips"
    )
    def state_puma_fips(puma_fips, state_fips) -> VariableAttribute["i", "state_puma_fips"]:
        '''FIPS code for PUMA including two-digit state FIPS code'''
        LOGGER.debug('in state_puma_fips')
        return state_fips + puma_fips
    
    # trigger_dict: bytes = dill.dumps(session.trigger_dict)
    # thread_manager.register_callback(
    #     functools.partial(
    #         send_trigger_dict_to_worker, trigger_dict=trigger_dict
    #     )
    # )
    # start_http_server(8000)
    session.start_threads()
    # session.attribute_table()

    session.block_until_finished()
