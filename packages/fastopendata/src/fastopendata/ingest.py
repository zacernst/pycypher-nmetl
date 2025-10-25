# pyrefly: ignore-all-errors
"""Create data for FastOpenData"""
# pylint: disable=invalid-name,missing-function-docstring,disallowed-name,protected-access,unused-argument,unused-import,redefined-outer-name,too-many-lines

import base64
import functools
import json
import pickle
from typing import TYPE_CHECKING, LiteralString

import dask
import dill
from dask.distributed import Client, LocalCluster, Worker
from nmetl.configuration import load_session_config
from nmetl.data_asset import DataAsset
from nmetl.data_source import NewColumn
from nmetl.session import Session
from nmetl.trigger import VariableAttribute
from prometheus_client import start_http_server
from pycypher.fact_collection import FactCollection
from pycypher.fact_collection.foundationdb import FoundationDBFactCollection
from shared.logger import LOGGER

LOGGER.setLevel("WARNING")


def send_trigger_dict_to_worker(dask_worker, trigger_dict) -> None:
    dask_worker.trigger_dict = dill.loads(trigger_dict)


SOURCE_DIR = (
    "/Users/zernst/git/pycypher-nmetl/packages/fastopendata/src/fastopendata/"
)

INGEST_CONFIG_PATH: LiteralString = f"{SOURCE_DIR}/ingest.yaml"
PUMS_DATA_DICTIONARY_PATH: LiteralString = f"{SOURCE_DIR}/acs_pums_2023_data_dictionary.json"

dask.config.set(
    task_serialization="dill",
)
dask.config.set(multiprocessing_context="fork")
dask.config.set({"distributed.worker.multiprocessing-method": "fork"})
if __name__ == "__main__":
    import dill

    def worker_setup(dask_worker: Worker) -> None:
        from pycypher.fact_collection.foundationdb import \
            FoundationDBFactCollection

        dask_worker.fact_collection = FoundationDBFactCollection()

    # LOCAL_CLUSTER = LocalCluster(processes=True)
    cluster = LocalCluster(n_workers=10, threads_per_worker=2, processes=True)
    DASK_CLIENT: Client = Client(cluster)
    fact_collection_kwargs: dict[str, str | int | float | bool] = {}

    fact_collection: FactCollection = FoundationDBFactCollection(
        **fact_collection_kwargs
    )

    DASK_CLIENT.register_worker_callbacks(worker_setup)

    session: Session = load_session_config(
        INGEST_CONFIG_PATH,
        dask_client=DASK_CLIENT,
    )
    # session.fact_collection.clear()

    with open(
        PUMS_DATA_DICTIONARY_PATH,
        "r",
        encoding="utf-8",
    ) as f:
        acs_pums_2023_data_dictionary: DataAsset = DataAsset(
            name="acs_pums_2023_data_dictionary", obj=json.load(f)
        )

    def send_data_asset(dask_worker: Worker, data_asset=None) -> None:
        if not hasattr(dask_worker, "data_assets"):
            dask_worker.data_assets: dict[str, Any] = {}
        LOGGER.info("Broadcasting data asset: %s", data_asset.name)
        dask_worker.data_assets[data_asset.name] = data_asset.obj

    session.register_data_asset(acs_pums_2023_data_dictionary)

    def send_data_assets(session) -> None:
        for data_asset in session.data_assets:
            DASK_CLIENT.register_worker_callbacks(
                functools.partial(send_data_asset, data_asset=data_asset)
            )

    @session.new_column("state_county_tract_puma")
    def state_county_tract(
        STATEFP, COUNTYFP, TRACTCE
    ) -> NewColumn["tract_fips"]:
        '''FIPS code for the tract, including county and state'''
        out = STATEFP + COUNTYFP + TRACTCE
        return out

    @session.new_column("state_county_tract_puma")
    def state_county(STATEFP, COUNTYFP) -> NewColumn["county_fips"]:
        '''FIPS code for the county, including state'''
        out = STATEFP + COUNTYFP
        return out

    @session.new_column("united_states_nodes")
    def get_osm_tags(encoded_tags) -> NewColumn["tags"]:
        '''All the OSM tags for the point'''
        out = pickle.loads(base64.b64decode(encoded_tags))
        return out

    @session.trigger(
        "MATCH (i:PSAM_2023_Individual) WITH i.MIL AS military RETURN military"
    )
    def active_duty(military) -> VariableAttribute["i", "active_duty"]:
        '''Actively serving in the military'''
        LOGGER.warning('In active_duty trigger: %s', military)
        return military == "1"

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.MIL AS military RETURN military"
    # )
    # def active_duty_in_past(
    #     military,
    # ) -> VariableAttribute["i", "active_duty_in_past"]:
    #     '''Has served in the military in the past'''
    #     return military == "2"

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.MIL AS military RETURN military"
    # )
    # def reserves_national_guard(
    #     military,
    # ) -> VariableAttribute["i", "reserves_national_guard"]:
    #     '''Actively serving in the reserves or the national guard'''
    #     return military == "3"

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.MIL AS military RETURN military"
    # )
    # def never_served_in_military(
    #     military,
    # ) -> VariableAttribute["i", "never_served_in_military"]:
    #     """Never served in the military"""
    #     return military == "3"

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.MIL AS military RETURN military"
    # )
    # def some_military(military) -> VariableAttribute["i", "some_military"]:
    #     """Currently serving or served in the military previously"""
    #     return military in ["1", "2", "3"]

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.MLPA AS military RETURN military"
    # )
    # def military_after_sept_11(
    #     military,
    # ) -> VariableAttribute["i", "military_after_sept_11"]:
    #     """Served in the military after September 11, 2001"""
    #     return military == "1"

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.DEYE AS eye RETURN eye"
    # )
    # def vision_difficulty(
    #     eye, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "vision_difficulty"]:
    #     """Has vision difficulty"""
    #     return acs_pums_2023_data_dictionary["DEYE"].get(eye, None)

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.DPHY AS physical RETURN physical"
    # )
    # def physical_difficulty(
    #     physical, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "physical_difficulty"]:
    #     """Has some kind of physical difficulty"""
    #     return acs_pums_2023_data_dictionary["DPHY"].get(physical, None)

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.ENG AS english RETURN english"
    # )
    # def english_very_well(
    #     english, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "speaks_english_very_well"]:
    #     '''Speaks english very well'''
    #     return english == "1"

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.ENG AS english RETURN english"
    # )
    # def english_well(
    #     english, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "speaks_english_well"]:
    #     '''Speaks english well, but not very well'''
    #     return english == "2"

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.ENG AS english RETURN english"
    # )
    # def english_not_well(
    #     english, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "speaks_english_not_well"]:
    #     '''Speaks some english, but not well'''
    #     return english == "3"

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.ENG AS english RETURN english"
    # )
    # def english_not_at_all(
    #     english, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "speaks_english_not_at_all"]:
    #     '''Speaks no english'''
    #     return english == "4"

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_mgr(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_management"]:
    #     '''Occupation in management'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out: bool = lookup[:3] == "MGR"
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_bus(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_business"]:
    #     '''Occupation in business'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out: bool = lookup[:3] == "BUS"
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_fin(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_financial"]:
    #     '''Occupation in the financial sector'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out: bool = lookup[:3] == "FIN"
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_cmm(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_computer"]:
    #     '''Occupation in computer industry'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out: bool = lookup[:3] == "CMM"
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_eng(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_engineering"]:
    #     '''Occupation in engineering'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out: bool = lookup[:3] == "ENG"
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_sci(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_science"]:
    #     '''Occupation in science research'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out: bool = lookup[:3] == "SCI"
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_cms(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_counseling"]:
    #     '''Occupation in counseling'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out: bool = lookup[:3] == "CMS"
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_lgl(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_legal"]:
    #     '''Occupation in the legal profession'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out: bool = lookup[:3] == "LGL"
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_edu(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_education"]:
    #     '''Occupation in education'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out: bool = lookup[:3] == "EDU"
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_ent(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_entertainment"]:
    #     '''Occupation in the entertainment industry'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out: bool = lookup[:3] == "ENT"
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_med(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_medicine"]:
    #     '''Occupation in medicine'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out = lookup[:3] == "MED"
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_hls(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_health_services"]:
    #     '''Occupation in health services'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out: bool = lookup[:3] == "HLS"
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_prt(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_protection"]:
    #     '''Occupation in personal protection industry'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out: bool = lookup[:3] == "PRT"
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_eat(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_food_service"]:
    #     '''Occupation in food service'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out: bool = lookup[:3] == "EAT"
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_cln(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_cleaning"]:
    #     '''Occupation as a cleaner'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out = lookup[:3] == "CLN"
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_prs(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_personal_service"]:
    #     '''Occupation in personal service'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out = lookup[:3] == "PRS"
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_sal(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_sales"]:
    #     '''Occupation in sales'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out = lookup[:3] == "SAL"
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_off(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_office_work"]:
    #     '''Occupation in office work'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out: bool = lookup[:3] == "OFF"
    #         return out
    #     else:
    #         return False
    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_con(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_construction"]:
    #     '''Occupation in construction'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out: bool = lookup[:3] == "CON"
    #         return out
    #     else:
    #         return False
    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_ext(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_extraction"]:
    #     '''Occupation in mineral or other natural resource extraction'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out: bool = lookup[:3] == "EXT"
    #         return out
    #     else:
    #         return False
    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_rpr(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_repair"]:
    #     '''Occupation in machinery repair'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out = lookup[:3] == "RPR"
    #         return out
    #     else:
    #         return False
    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_prd(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_production"]:
    #     '''Occupation in production'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out = lookup[:3] == "PRD"
    #         return out
    #     else:
    #         return False
    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_trn(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_transportation"]:
    #     '''Occupation in transportation'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out = lookup[:3] == "TRN"
    #         return out
    #     else:
    #         return False
    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_mil(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_military"]:
    #     '''Occupation in the military'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out = lookup[:3] == "MIL"
    #         return out
    #     else:
    #         return False
    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.SOCP AS occupation RETURN occupation"
    # )
    # def occupation_sal(
    #     occupation, acs_pums_2023_data_dictionary
    # ) -> VariableAttribute["i", "occupation_unemployed"]:
    #     '''Not currently employed'''
    #     lookup: str | None = acs_pums_2023_data_dictionary["SOCP"].get(
    #         occupation, None
    #     )
    #     if isinstance(lookup, str):
    #         out: bool = lookup.startswith('Unemployed')
    #         return out
    #     else:
    #         return False

    # @session.trigger(
    #     "MATCH (i:PSAM_2023_Individual) WITH i.STATE AS state RETURN state"
    # )
    # def psam_state_fips(
    #     state
    # ) -> VariableAttribute["i", "state_fips"]:
    #     '''State FIPS code (two digits)'''
    #     return state

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
        "MATCH (i:PSAM_2023_Individual) WITH i.puma_fips "
        "AS puma_fips, i.state_fips AS state_fips "
        "RETURN puma_fips, state_fips"
    )
    def state_puma_fips(puma_fips, state_fips) -> VariableAttribute["i", "state_puma_fips"]:
        '''FIPS code for PUMA including two-digit state FIPS code'''
        return state_fips + puma_fips
    
    # @session.trigger(
    #     "MATCH (i:Tract)-[r:in]->(c:County) WITH COLLECT(i.tract_fips) AS tract_list RETURN tract_list"
    # )
    # def relationship_trigger(tract_list) -> VariableAttribute["c", "number_of_tracts"]:
    #     '''Number of tracts in the county'''
    #     return len(tract_list)


    trigger_dict: bytes = dill.dumps(session.trigger_dict)
    DASK_CLIENT.register_worker_callbacks(
        functools.partial(
            send_trigger_dict_to_worker, trigger_dict=trigger_dict
        )
    )
    # start_http_server(8000)
    session.start_threads()
    session.attribute_table()

    session.block_until_finished()
