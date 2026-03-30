"""FastOpenData API — Cypher query execution over open datasets.

Serves U.S. Census, geographic, and OSM open datasets via a REST API
backed by the fastopendata package and the pycypher query engine.

Run locally (from the monorepo root):
    uv run uvicorn fastopendata.api:app --reload --port 8000

Or via Docker:
    make fod-api-up
    curl http://localhost:8093/health
"""

from __future__ import annotations

import datetime
import logging
import math
import time
from decimal import Decimal
from typing import Any
from uuid import UUID

import pandas as pd
import pycypher
from fastapi import FastAPI, HTTPException, Request, Response
from pycypher.exc_execution import (
    MissingParameterError,
    QueryComplexityError,
    QueryMemoryBudgetError,
    QueryTimeoutError,
)
from pycypher.exc_type_errors import GraphTypeNotFoundError
from pycypher.exc_variable_errors import VariableNotFoundError
from pycypher.star import Star
from pydantic import BaseModel, Field
from shared.audit_chain import ChainedAuditLog

from .analytics.collector import MetricsCollector, QueryStatus
from .analytics.engine import AnalyticsEngine
from .analytics.regression import RegressionDetector
from .config import config

_logger = logging.getLogger(__name__)

app = FastAPI(
    title=config.api_title,
    description=config.api_description,
    version=config.api_version,
)


# ---------------------------------------------------------------------------
# Security headers middleware
# ---------------------------------------------------------------------------


@app.middleware("http")
async def security_headers_middleware(
    request: Request,
    call_next: Any,
) -> Response:
    """Add standard security headers to every response."""
    response: Response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Content-Security-Policy"] = "default-src 'none'"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    return response


# ---------------------------------------------------------------------------
# Query engine — singleton Star instance
# ---------------------------------------------------------------------------

_star: Star | None = None


def get_star() -> Star:
    """Return the shared Star instance, creating it lazily if needed."""
    global _star
    if _star is None:
        try:
            _star = Star()
        except Exception as exc:
            msg = (
                f"Failed to initialize query engine: {exc}. "
                "Check that data files are loaded and configuration is correct."
            )
            raise RuntimeError(msg) from exc
    return _star


def set_star(star: Star) -> None:
    """Replace the shared Star instance (used for testing and data loading)."""
    global _star
    _star = star


# ---------------------------------------------------------------------------
# Query performance analytics — singleton collector + engine
# ---------------------------------------------------------------------------

_metrics_collector = MetricsCollector()
_analytics_engine = AnalyticsEngine(_metrics_collector)
_regression_detector = RegressionDetector(_metrics_collector)


def get_metrics_collector() -> MetricsCollector:
    """Return the global metrics collector."""
    return _metrics_collector


def get_analytics_engine() -> AnalyticsEngine:
    """Return the global analytics engine."""
    return _analytics_engine


def get_regression_detector() -> RegressionDetector:
    """Return the global regression detector."""
    return _regression_detector


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
    datasets = []
    for dataset_name, dataset_config in config.datasets.items():
        datasets.append(
            DatasetInfo(
                name=dataset_name,
                description=dataset_config.description,
                format=dataset_config.format,
                source=dataset_config.source,
                approx_size=dataset_config.approx_size,
            ),
        )

    return DatasetListResponse(datasets=datasets)


@app.get("/datasets/{name}", response_model=DatasetInfo)
async def get_dataset(name: str) -> DatasetInfo:
    """Return metadata for a single dataset by name."""
    try:
        dataset_config = config.get_dataset(name)
    except KeyError as exc:
        raise HTTPException(
            status_code=404,
            detail=f"Dataset '{name}' not found.",
        ) from exc
    return DatasetInfo(
        name=name,
        description=dataset_config.description,
        format=dataset_config.format,
        source=dataset_config.source,
        approx_size=dataset_config.approx_size,
    )


def _sanitize_value(val: Any) -> str | int | float | bool | None:
    """Coerce a DataFrame cell to a JSON-safe scalar."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    if isinstance(val, bool | int | str):
        return val
    if isinstance(val, float):
        return val
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, UUID):
        return str(val)
    if isinstance(val, datetime.datetime):
        return val.isoformat()
    if isinstance(val, datetime.date):
        return val.isoformat()
    # Fallback — stringify complex objects
    return str(val)


def _record_error_and_raise(
    query: str,
    t_start: float,
    parse_ms: float,
    exc: Exception,
    *,
    status_code: int,
    detail: str | list[dict[str, str]],
    status: QueryStatus = QueryStatus.ERROR,
    log_traceback: bool = False,
) -> None:
    """Record a query error metric and raise an HTTPException.

    Consolidates the repeated record-error-then-raise pattern used by
    the ``/query`` endpoint.
    """
    total_ms = (time.monotonic() - t_start) * 1000
    _metrics_collector.record_error(
        query,
        total_ms,
        str(exc),
        status=status,
        metadata={"parse_ms": parse_ms},
    )
    if log_traceback:
        _logger.exception("Unexpected query execution failure: %s", exc)
    raise HTTPException(status_code=status_code, detail=detail) from exc


# Exception-type → (status_code, detail_template, optional extra kwargs)
_QUERY_ERROR_MAP: list[
    tuple[type[Exception] | tuple[type[Exception], ...], int, str | None]
] = [
    (
        (GraphTypeNotFoundError, VariableNotFoundError),
        422,
        "Query references unknown type or variable: {}",
    ),
    (MissingParameterError, 422, "Missing query parameter: {}"),
    (QueryTimeoutError, 504, "Query timed out: {}"),
    (
        (QueryMemoryBudgetError, QueryComplexityError),
        413,
        "Query exceeds resource limits: {}",
    ),
]


@app.post("/query", response_model=CypherQueryResponse)
async def run_cypher_query(
    request: CypherQueryRequest,
) -> CypherQueryResponse:
    """Execute a Cypher query against the fastopendata graph."""
    t_start = time.monotonic()

    # Validate syntax before execution.
    t_parse_start = time.monotonic()
    errors = pycypher.validate_query(request.query)
    t_parse_end = time.monotonic()
    parse_ms = (t_parse_end - t_parse_start) * 1000

    if errors:
        detail = [{"severity": e.severity.value, "message": e.message} for e in errors]
        _record_error_and_raise(
            request.query,
            t_start,
            parse_ms,
            ValueError("Validation failed"),
            status_code=422,
            detail=detail,
        )

    star = get_star()
    t_exec_start = time.monotonic()
    try:
        result: pd.DataFrame = star.execute_query(
            request.query,
            parameters=request.parameters or None,
        )
    except (
        GraphTypeNotFoundError,
        VariableNotFoundError,
        MissingParameterError,
        QueryTimeoutError,
        QueryMemoryBudgetError,
        QueryComplexityError,
    ) as exc:
        for exc_types, code, template in _QUERY_ERROR_MAP:
            if isinstance(exc, exc_types):
                _record_error_and_raise(
                    request.query,
                    t_start,
                    parse_ms,
                    exc,
                    status_code=code,
                    detail=template.format(exc),
                    status=QueryStatus.TIMEOUT
                    if isinstance(exc, QueryTimeoutError)
                    else QueryStatus.ERROR,
                )
        # Unreachable, but satisfies type checker
        raise  # pragma: no cover
    except Exception as exc:
        _record_error_and_raise(
            request.query,
            t_start,
            parse_ms,
            exc,
            status_code=400,
            detail="Query execution failed",
            log_traceback=True,
        )

    exec_ms = (time.monotonic() - t_exec_start) * 1000

    rows: list[dict[str, str | int | float | bool | None]] = [
        {col: _sanitize_value(row[col]) for col in result.columns}
        for _, row in result.iterrows()
    ]

    total_ms = (time.monotonic() - t_start) * 1000
    _metrics_collector.record_success(
        request.query,
        total_ms=total_ms,
        row_count=len(rows),
        parse_ms=parse_ms,
        exec_ms=exec_ms,
    )

    return CypherQueryResponse(
        query=request.query,
        rows=rows,
        row_count=len(rows),
    )


# ---------------------------------------------------------------------------
# Audit chain
# ---------------------------------------------------------------------------

_audit_log = ChainedAuditLog()


def get_audit_log() -> ChainedAuditLog:
    """Return the global audit log instance."""
    return _audit_log


class AuditVerifyResponse(BaseModel):
    valid: bool
    record_count: int
    violations: list[str]


class AuditTailResponse(BaseModel):
    records: list[dict[str, Any]]


@app.get("/audit/verify", response_model=AuditVerifyResponse)
async def audit_verify() -> AuditVerifyResponse:
    """Verify the integrity of the audit chain."""
    log = get_audit_log()
    valid, violations = log.verify()
    return AuditVerifyResponse(
        valid=valid,
        record_count=log.length,
        violations=list(violations),
    )


@app.get("/audit/tail", response_model=AuditTailResponse)
async def audit_tail(n: int = 10) -> AuditTailResponse:
    """Return the last *n* audit records."""
    log = get_audit_log()
    records = log.tail(n)
    return AuditTailResponse(records=[r.to_dict() for r in records])


# ---------------------------------------------------------------------------
# Performance analytics endpoints
# ---------------------------------------------------------------------------


class AnalyticsSummaryResponse(BaseModel):
    total_queries: int
    error_rate: float
    latency: dict[str, Any]
    bottlenecks: list[dict[str, Any]]
    trends: list[dict[str, Any]]
    slowest_queries: list[dict[str, Any]]
    recommendations: list[str]


class AnalyticsOverviewResponse(BaseModel):
    total_queries: int
    total_errors: int
    error_rate: float
    uptime_seconds: float
    queries_per_second: float


class RecentMetricsResponse(BaseModel):
    metrics: list[dict[str, Any]]


@app.get("/analytics/overview", response_model=AnalyticsOverviewResponse)
async def analytics_overview() -> AnalyticsOverviewResponse:
    """Return high-level query performance counters."""
    collector = get_metrics_collector()
    return AnalyticsOverviewResponse(
        total_queries=collector.total_queries,
        total_errors=collector.total_errors,
        error_rate=round(collector.error_rate, 4),
        uptime_seconds=round(collector.uptime_seconds, 2),
        queries_per_second=round(collector.queries_per_second, 4),
    )


@app.get("/analytics/summary", response_model=AnalyticsSummaryResponse)
async def analytics_summary(last_n: int | None = None) -> AnalyticsSummaryResponse:
    """Return full performance analysis with bottlenecks and recommendations.

    Parameters
    ----------
    last_n : int | None
        If provided, only analyze the most recent *last_n* queries.

    """
    engine = get_analytics_engine()
    result = engine.summary(last_n=last_n)
    data = result.to_dict()
    return AnalyticsSummaryResponse(**data)


@app.get("/analytics/recent", response_model=RecentMetricsResponse)
async def analytics_recent(n: int = 20) -> RecentMetricsResponse:
    """Return the *n* most recent query metrics."""
    collector = get_metrics_collector()
    recent = collector.recent(n)
    return RecentMetricsResponse(
        metrics=[m.to_dict() for m in recent],
    )


class RegressionResponse(BaseModel):
    regressions: list[dict[str, Any]]
    total_tracked_fingerprints: int


@app.get("/analytics/regressions", response_model=RegressionResponse)
async def analytics_regressions() -> RegressionResponse:
    """Check for query performance regressions.

    Ingests recent metrics and returns any detected regressions
    where queries are running significantly slower than their
    historical baseline.
    """
    detector = get_regression_detector()
    detector.ingest()
    return RegressionResponse(
        regressions=[a.to_dict() for a in detector.alerts],
        total_tracked_fingerprints=detector.tracked_fingerprint_count,
    )
