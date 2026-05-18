"""Derive graph relationships from FIPS code joins across data sources.

All relationships in the fastopendata graph are **computed** at ingestion
time by joining DataFrames on shared FIPS codes. No relationship data
exists in the source files — edges are derived from column matches.

Each function takes entity DataFrames as input and returns an edge
DataFrame with ``__SOURCE__`` and ``__TARGET__`` columns suitable for
:meth:`GraphPipeline.add_relationship_dataframe`.
"""

from __future__ import annotations

import pandas as pd


def derive_tract_state_relationships(
    tracts_df: pd.DataFrame,
    states_df: pd.DataFrame,
) -> pd.DataFrame:
    """Create IN_STATE edges: CensusTract → State via STATEFP match.

    Parameters
    ----------
    tracts_df:
        Census tract DataFrame with ``GEOID`` and ``STATEFP`` columns.
    states_df:
        State DataFrame with ``STATEFP`` column.

    Returns
    -------
    DataFrame with ``__SOURCE__`` (tract GEOID) and ``__TARGET__`` (state STATEFP).
    """
    valid_states = set(states_df["STATEFP"])
    edges = tracts_df[["GEOID", "STATEFP"]].copy()
    edges = edges[edges["STATEFP"].isin(valid_states)]
    edges = edges.rename(columns={"GEOID": "__SOURCE__", "STATEFP": "__TARGET__"})
    return edges.reset_index(drop=True)


def derive_tract_puma_relationships(
    crosswalk_df: pd.DataFrame,
    *,
    state_fips: str | None = None,
) -> pd.DataFrame:
    """Create MAPS_TO_PUMA edges: CensusTract → Puma via crosswalk.

    Parameters
    ----------
    crosswalk_df:
        Tract-PUMA crosswalk DataFrame with ``STATEFP``, ``COUNTYFP``,
        ``TRACTCE``, and ``PUMA5CE`` columns. All columns must be string type.
    state_fips:
        Optional state FIPS filter. If provided, only crosswalk rows for
        that state are included.

    Returns
    -------
    DataFrame with ``__SOURCE__`` (tract GEOID) and ``__TARGET__`` (PUMA code).
    """
    df = crosswalk_df.copy()

    if state_fips is not None:
        df = df[df["STATEFP"] == state_fips]

    # Build tract GEOID: STATEFP + COUNTYFP + TRACTCE
    df["__SOURCE__"] = df["STATEFP"] + df["COUNTYFP"] + df["TRACTCE"]
    df["__TARGET__"] = df["PUMA5CE"]

    return df[["__SOURCE__", "__TARGET__"]].reset_index(drop=True)


def derive_tract_block_group_relationships(
    tracts_df: pd.DataFrame,
    block_groups_df: pd.DataFrame,
) -> pd.DataFrame:
    """Create CONTAINS_BLOCK_GROUP edges: CensusTract → BlockGroup.

    Block groups belong to tracts. A block group's GEOID is the tract GEOID
    plus a 1-digit block group code (12 digits total vs 11 for tracts).
    The relationship is derived by matching the first 11 characters of the
    block group GEOID to the tract GEOID.

    Parameters
    ----------
    tracts_df:
        Census tract DataFrame with ``GEOID`` column (11-digit).
    block_groups_df:
        Block group DataFrame with ``GEOID`` column (12-digit).

    Returns
    -------
    DataFrame with ``__SOURCE__`` (tract GEOID) and ``__TARGET__`` (block group GEOID).
    """
    valid_tracts = set(tracts_df["GEOID"])
    bg = block_groups_df[["GEOID"]].copy()
    bg["tract_geoid"] = bg["GEOID"].str[:11]
    bg = bg[bg["tract_geoid"].isin(valid_tracts)]
    edges = bg.rename(columns={"tract_geoid": "__SOURCE__", "GEOID": "__TARGET__"})
    return edges[["__SOURCE__", "__TARGET__"]].reset_index(drop=True)


def derive_contract_state_relationships(
    contracts_df: pd.DataFrame,
    states_df: pd.DataFrame,
    *,
    fips_column: str = "prime_award_transaction_place_of_performance_state_fips_code",
) -> pd.DataFrame:
    """Create contract → State edges via FIPS code match.

    Parameters
    ----------
    contracts_df:
        Contracts DataFrame with ``contract_transaction_unique_key`` and
        the specified FIPS column.
    states_df:
        State DataFrame with ``STATEFP`` column.
    fips_column:
        Column in contracts_df containing the state FIPS code to join on.
        Defaults to place-of-performance FIPS. Use
        ``"prime_award_transaction_recipient_state_fips_code"`` for
        recipient-state relationships.

    Returns
    -------
    DataFrame with ``__SOURCE__`` (contract key) and ``__TARGET__`` (state STATEFP).
    """
    valid_states = set(states_df["STATEFP"])
    edges = contracts_df[["contract_transaction_unique_key", fips_column]].copy()
    edges = edges.dropna(subset=[fips_column])
    edges = edges[edges[fips_column].isin(valid_states)]
    edges = edges.rename(
        columns={
            "contract_transaction_unique_key": "__SOURCE__",
            fips_column: "__TARGET__",
        }
    )
    return edges[["__SOURCE__", "__TARGET__"]].reset_index(drop=True)


def derive_person_puma_relationships(
    persons_df: pd.DataFrame,
    pumas_df: pd.DataFrame,
) -> pd.DataFrame:
    """Create LIVES_IN_PUMA edges: Person → Puma via PUMA+STATE match.

    Each ACS PUMS person record carries a ``PUMA`` code (numeric, up to 5
    digits) and a ``STATE`` FIPS code (numeric, up to 2 digits). PUMAs from
    TIGER shapefiles use ``STATEFP`` (zero-padded 2-char string) and
    ``PUMACE20`` (zero-padded 5-char string). This function normalizes the
    types on both sides so the join succeeds regardless of how each frame
    was loaded.

    Parameters
    ----------
    persons_df:
        ACS person DataFrame with ``PUMA``, ``STATE``, and ``SERIALNO``
        columns. ``SERIALNO`` is used as the source identifier.
    pumas_df:
        PUMA DataFrame with ``PUMACE20`` and ``STATEFP`` columns. If a
        ``GEOID`` (or ``GEOID20``) column is present, it is used as the
        target identifier; otherwise ``PUMACE20`` is used.

    Returns
    -------
    DataFrame with ``__SOURCE__`` (person ``SERIALNO``) and ``__TARGET__``
    (PUMA ``GEOID``/``PUMACE20``). If any of the required columns are
    missing on either input frame, an empty edge DataFrame is returned.
    """
    required_person_cols = {"SERIALNO", "PUMA", "STATE"}
    required_puma_cols = {"PUMACE20", "STATEFP"}
    if not required_person_cols.issubset(persons_df.columns):
        return pd.DataFrame(columns=["__SOURCE__", "__TARGET__"])
    if not required_puma_cols.issubset(pumas_df.columns):
        return pd.DataFrame(columns=["__SOURCE__", "__TARGET__"])

    if persons_df.empty or pumas_df.empty:
        return pd.DataFrame(columns=["__SOURCE__", "__TARGET__"])

    # Normalize PUMA/STATE on persons: pad PUMA to 5 chars, STATE to 2 chars.
    persons = persons_df[["SERIALNO", "PUMA", "STATE"]].copy()
    persons = persons.dropna(subset=["PUMA", "STATE", "SERIALNO"])
    persons["_puma_key"] = (
        persons["PUMA"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(5)
    )
    persons["_state_key"] = (
        persons["STATE"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(2)
    )

    # Normalize on PUMA frame: pick the best target id column.
    target_col = "GEOID" if "GEOID" in pumas_df.columns else (
        "GEOID20" if "GEOID20" in pumas_df.columns else "PUMACE20"
    )
    if target_col == "PUMACE20":
        pumas = pumas_df[["PUMACE20", "STATEFP"]].copy()
        pumas["__TARGET__"] = pumas["PUMACE20"].astype(str)
    else:
        pumas = pumas_df[["PUMACE20", "STATEFP", target_col]].copy()
        pumas = pumas.rename(columns={target_col: "__TARGET__"})
    pumas["_puma_key"] = pumas["PUMACE20"].astype(str).str.zfill(5)
    pumas["_state_key"] = pumas["STATEFP"].astype(str).str.zfill(2)

    edges = persons.merge(
        pumas[["_puma_key", "_state_key", "__TARGET__"]],
        on=["_puma_key", "_state_key"],
        how="inner",
    )
    edges = edges.rename(columns={"SERIALNO": "__SOURCE__"})
    return edges[["__SOURCE__", "__TARGET__"]].reset_index(drop=True)


def derive_county_state_relationships(
    county_df: pd.DataFrame,
    states_df: pd.DataFrame,
    *,
    county_fips_col: str = "county_fips",
) -> pd.DataFrame:
    """Create COUNTY_IN_STATE edges: county → State via FIPS prefix match.

    A county FIPS is a 5-digit code whose first 2 digits ARE the state FIPS
    (e.g. ``"13089"`` is DeKalb County, GA — state ``"13"``). This function
    extracts the prefix from each county row, joins to the State entity,
    and drops counties whose state isn't represented in ``states_df``.

    Type normalization: county FIPS may be an int from pandas auto-inference
    (which loses leading zeros) or a zero-padded string. Inputs are coerced
    to zero-padded 5-char strings before slicing.

    Parameters
    ----------
    county_df:
        DataFrame containing one row per county. Must have ``county_fips_col``
        as the source identifier (also used for slicing).
    states_df:
        State DataFrame with a ``STATEFP`` column.
    county_fips_col:
        Name of the county-FIPS column on ``county_df``. Defaults to
        ``"county_fips"`` (CJARS convention).

    Returns
    -------
    DataFrame with ``__SOURCE__`` (county FIPS, zero-padded 5-char) and
    ``__TARGET__`` (state STATEFP). If the required columns are missing on
    either input, an empty edge DataFrame is returned.
    """
    if county_fips_col not in county_df.columns:
        return pd.DataFrame(columns=["__SOURCE__", "__TARGET__"])
    if "STATEFP" not in states_df.columns:
        return pd.DataFrame(columns=["__SOURCE__", "__TARGET__"])
    if county_df.empty or states_df.empty:
        return pd.DataFrame(columns=["__SOURCE__", "__TARGET__"])

    # Normalize county FIPS to zero-padded 5-char string. Strips a trailing
    # ``.0`` if pandas read the column as float.
    county_keys = (
        county_df[county_fips_col]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(5)
    )
    state_prefix = county_keys.str[:2]
    valid_states = set(states_df["STATEFP"].astype(str).str.zfill(2))

    edges = pd.DataFrame({
        "__SOURCE__": county_keys,
        "__TARGET__": state_prefix,
    })
    edges = edges[edges["__TARGET__"].isin(valid_states)]
    return edges.reset_index(drop=True)
