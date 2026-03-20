"""FastOpenData API — stub implementation.

Serves U.S. Census, geographic, and OSM open datasets via a REST API
backed by the fastopendata package and the pycypher query engine.

All endpoints are stubbed; replace the placeholder responses with real
data access as the ingestion pipeline matures.

Run locally (from the monorepo root):
    uv run uvicorn fastopendata.api:app --reload --port 8000

Or via Docker:
    make fod-api-up
    curl http://localhost:8093/health
"""

from __future__ import annotations

import pycypher
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(
    title="FastOpenData API",
    description=(
        "REST API for U.S. Census, geographic, and OSM open datasets, "
        "queryable via Cypher through the pycypher engine."
    ),
    version="0.0.1",
)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str
    api_version: str
    pycypher_version: str


class DatasetInfo(BaseModel):
    name: str = Field(description="Short identifier used in API paths")
    description: str
    format: str = Field(description="Primary file format of the raw data")
    source: str = Field(description="Originating organisation or project")
    approx_size: str = Field(description="Approximate download size")


class DatasetListResponse(BaseModel):
    datasets: list[DatasetInfo]


class CypherQueryRequest(BaseModel):
    query: str = Field(
        description="Cypher query to execute against the graph",
        examples=["MATCH (n:Person) RETURN n.name LIMIT 10"],
    )
    parameters: dict[str, str | int | float | bool] = Field(
        default_factory=dict,
        description="Named parameters referenced by the query",
    )


class CypherQueryResponse(BaseModel):
    query: str
    rows: list[dict[str, str | int | float | bool | None]]
    row_count: int


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/", response_model=HealthResponse)
async def root() -> HealthResponse:
    """Root endpoint — confirms the service is alive."""
    return HealthResponse(
        status="ok",
        api_version=app.version,
        pycypher_version=pycypher.__version__,
    )


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Health check — suitable for container liveness/readiness probes."""
    return HealthResponse(
        status="ok",
        api_version=app.version,
        pycypher_version=pycypher.__version__,
    )


@app.get("/datasets", response_model=DatasetListResponse)
async def list_datasets() -> DatasetListResponse:
    """Return metadata for all datasets in the fastopendata catalogue."""
    return DatasetListResponse(
        datasets=[
            DatasetInfo(
                name="acs_pums_1yr_persons",
                description="ACS PUMS 2023 — 1-year person microdata",
                format="CSV",
                source="U.S. Census Bureau",
                approx_size="~300 MB",
            ),
            DatasetInfo(
                name="acs_pums_5yr_persons",
                description="ACS PUMS 2023 — 5-year person microdata",
                format="CSV",
                source="U.S. Census Bureau",
                approx_size="~1.5 GB",
            ),
            DatasetInfo(
                name="acs_pums_1yr_housing",
                description="ACS PUMS 2023 — 1-year housing unit microdata",
                format="CSV",
                source="U.S. Census Bureau",
                approx_size="~200 MB",
            ),
            DatasetInfo(
                name="acs_pums_5yr_housing",
                description="ACS PUMS 2023 — 5-year housing unit microdata",
                format="CSV",
                source="U.S. Census Bureau",
                approx_size="~1 GB",
            ),
            DatasetInfo(
                name="sipp_2023_puf",
                description="SIPP 2023 public use file (longitudinal panel)",
                format="CSV",
                source="U.S. Census Bureau",
                approx_size="~500 MB",
            ),
            DatasetInfo(
                name="sipp_2023_weights",
                description="SIPP 2023 replicate weights for variance estimation",
                format="CSV",
                source="U.S. Census Bureau",
                approx_size="~200 MB",
            ),
            DatasetInfo(
                name="ahs_2023",
                description="American Housing Survey 2023 national PUF",
                format="CSV",
                source="U.S. Census Bureau",
                approx_size="~100 MB",
            ),
            DatasetInfo(
                name="cjars_2022_county",
                description="CJARS 2022 county-level job offers",
                format="CSV",
                source="U.S. Census Bureau",
                approx_size="<10 MB",
            ),
            DatasetInfo(
                name="tiger_puma_2024",
                description="TIGER/Line 2024 PUMA boundary shapefiles",
                format="Shapefile",
                source="U.S. Census Bureau",
                approx_size="~300 MB",
            ),
            DatasetInfo(
                name="tiger_block_groups_2024",
                description="TIGER/Line 2024 census block group boundaries",
                format="Shapefile",
                source="U.S. Census Bureau",
                approx_size="~500 MB",
            ),
            DatasetInfo(
                name="tiger_tracts_2025",
                description="TIGER/Line 2025 census tract boundaries (per state)",
                format="Shapefile",
                source="U.S. Census Bureau",
                approx_size="~1 GB",
            ),
            DatasetInfo(
                name="tiger_addr_2025",
                description="TIGER/Line 2025 address range features (per state)",
                format="Shapefile",
                source="U.S. Census Bureau",
                approx_size="~10 GB",
            ),
            DatasetInfo(
                name="osm_us_nodes",
                description="OpenStreetMap U.S. point node extract",
                format="CSV",
                source="Geofabrik / OpenStreetMap contributors (ODbL 1.0)",
                approx_size="~10 GB raw",
            ),
            DatasetInfo(
                name="wikidata_us_geopoints",
                description="Wikidata U.S. geographic point entities",
                format="Newline-delimited JSON",
                source="Wikimedia Foundation (CC0 1.0)",
                approx_size="~5 GB filtered",
            ),
        ],
    )


@app.get("/datasets/{name}", response_model=DatasetInfo)
async def get_dataset(name: str) -> DatasetInfo:
    """Return metadata for a single dataset by name."""
    result = await list_datasets()
    for dataset in result.datasets:
        if dataset.name == name:
            return dataset
    raise HTTPException(status_code=404, detail=f"Dataset '{name}' not found.")


@app.post("/query", response_model=CypherQueryResponse)
async def run_cypher_query(
    request: CypherQueryRequest,
) -> CypherQueryResponse:
    """Execute a Cypher query against the fastopendata graph.

    Stub — pycypher parsing is wired in but execution against loaded data
    is not yet implemented.  Returns an empty result set.
    """
    # Validate the query before returning; surface any semantic errors.
    errors = pycypher.validate_query(request.query)
    if errors:
        raise HTTPException(
            status_code=422,
            detail=[
                {"severity": e.severity.value, "message": e.message}
                for e in errors
            ],
        )

    # TODO: execute against loaded fastopendata graph.
    return CypherQueryResponse(
        query=request.query,
        rows=[],
        row_count=0,
    )
