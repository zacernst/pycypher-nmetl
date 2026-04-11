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
