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

import collections
import datetime
import hashlib
import logging
import math
import os
import secrets
import time
from decimal import Decimal
from typing import Any
from uuid import UUID

import pandas as pd
import pycypher
from fastapi import FastAPI, HTTPException, Request, Response
from pycypher.exceptions import (
    GraphTypeNotFoundError,
    MissingParameterError,
    QueryComplexityError,
    QueryMemoryBudgetError,
    QueryTimeoutError,
    VariableNotFoundError,
)
from pycypher.star import Star
from pydantic import BaseModel, Field
from shared.audit_chain import ChainedAuditLog

from .analytics.collector import MetricsCollector, QueryStatus
from .analytics.engine import AnalyticsEngine
from .analytics.regression import RegressionDetector
from .config import config

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Production mode configuration
# ---------------------------------------------------------------------------

# When true, generic error messages are returned for unexpected exceptions
# and internal details (exception types, stack traces) are suppressed.
# Set PYCYPHER_PRODUCTION=1 to enable.
_PRODUCTION_MODE: bool = os.environ.get("PYCYPHER_PRODUCTION", "").strip() in (
    "1",
    "true",
    "yes",
)

# ---------------------------------------------------------------------------
# Rate limiting configuration
# ---------------------------------------------------------------------------

# Maximum requests per window per client IP.
# Override with PYCYPHER_RATE_LIMIT_MAX (default: 100 requests).
_RATE_LIMIT_MAX: int = int(
    os.environ.get("PYCYPHER_RATE_LIMIT_MAX", "100"),
)
# Window size in seconds.
# Override with PYCYPHER_RATE_LIMIT_WINDOW (default: 60 seconds).
_RATE_LIMIT_WINDOW: float = float(
    os.environ.get("PYCYPHER_RATE_LIMIT_WINDOW", "60"),
)

# ---------------------------------------------------------------------------
# Request body size configuration
# ---------------------------------------------------------------------------

# Maximum allowed request body size in bytes.
# Override with PYCYPHER_MAX_BODY_BYTES (default: 1 MiB).
_MAX_BODY_BYTES: int = int(
    os.environ.get("PYCYPHER_MAX_BODY_BYTES", str(1024 * 1024)),
)


# ---------------------------------------------------------------------------
# Rate limiter implementation (token bucket per IP)
# ---------------------------------------------------------------------------


class _TokenBucket:
    """Simple token-bucket rate limiter keyed by client IP.

    Each IP gets *max_tokens* tokens that refill at a rate of
    *max_tokens / window_seconds* per second.  A request consumes one
    token; when no tokens remain the request is rejected.
    """

    def __init__(self, max_tokens: int, window_seconds: float) -> None:
        self.max_tokens = max_tokens
        self.refill_rate = max_tokens / window_seconds if window_seconds > 0 else max_tokens
        # {ip: (tokens_remaining, last_refill_time)}
        self._buckets: dict[str, tuple[float, float]] = {}
        # Track IPs in insertion order for eviction
        self._order: collections.deque[str] = collections.deque()
        self._max_ips: int = 10_000  # cap memory usage

    def allow(self, ip: str) -> bool:
        """Return True if the request from *ip* is allowed."""
        now = time.monotonic()
        if ip in self._buckets:
            tokens, last = self._buckets[ip]
            elapsed = now - last
            tokens = min(self.max_tokens, tokens + elapsed * self.refill_rate)
        else:
            tokens = float(self.max_tokens)
            self._order.append(ip)
            # Evict oldest if too many IPs tracked
            if len(self._order) > self._max_ips:
                oldest = self._order.popleft()
                self._buckets.pop(oldest, None)

        if tokens >= 1.0:
            self._buckets[ip] = (tokens - 1.0, now)
            return True

        self._buckets[ip] = (tokens, now)
        return False


_rate_limiter = _TokenBucket(_RATE_LIMIT_MAX, _RATE_LIMIT_WINDOW)


def get_rate_limiter() -> _TokenBucket:
    """Return the global rate limiter (for testing)."""
    return _rate_limiter


def is_production_mode() -> bool:
    """Return True if production mode is active."""
    return _PRODUCTION_MODE


def get_max_body_bytes() -> int:
    """Return the configured max body size in bytes."""
    return _MAX_BODY_BYTES


# ---------------------------------------------------------------------------
# API key authentication
# ---------------------------------------------------------------------------

# Enable API key requirement.  Set PYCYPHER_API_AUTH=1 to enforce.
_API_AUTH_ENABLED: bool = os.environ.get("PYCYPHER_API_AUTH", "").strip() in (
    "1",
    "true",
    "yes",
)

# Admin key for managing API keys (create/revoke).
# Must be set when auth is enabled.
_ADMIN_API_KEY: str = os.environ.get("PYCYPHER_ADMIN_KEY", "")

# Paths that never require authentication (health checks, docs).
_AUTH_EXEMPT_PATHS: frozenset[str] = frozenset({
    "/",
    "/health",
    "/docs",
    "/openapi.json",
    "/redoc",
})


class ApiKeyStore:
    """In-memory API key store with usage tracking.

    Keys are stored as SHA-256 hashes so raw tokens are never retained
    in memory after validation.  Each key has a label, creation time,
    active status, and per-key request counter.
    """

    def __init__(self) -> None:
        # {hash: {"label": str, "created": float, "active": bool, "requests": int}}
        self._keys: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _hash(key: str) -> str:
        """Return the SHA-256 hex digest of a raw API key."""
        return hashlib.sha256(key.encode()).hexdigest()

    def create_key(self, label: str = "") -> str:
        """Generate a new API key and register it.

        Returns the raw key (shown once; stored only as hash).
        """
        raw_key = f"pyc_{secrets.token_urlsafe(32)}"
        h = self._hash(raw_key)
        self._keys[h] = {
            "label": label,
            "created": time.time(),
            "active": True,
            "requests": 0,
        }
        return raw_key

    def validate(self, raw_key: str) -> bool:
        """Return True if the key is valid and active."""
        h = self._hash(raw_key)
        entry = self._keys.get(h)
        if entry is None or not entry["active"]:
            return False
        entry["requests"] += 1
        return True

    def revoke(self, raw_key: str) -> bool:
        """Revoke a key. Returns True if the key existed."""
        h = self._hash(raw_key)
        entry = self._keys.get(h)
        if entry is None:
            return False
        entry["active"] = False
        return True

    def list_keys(self) -> list[dict[str, Any]]:
        """Return metadata for all keys (no raw keys or hashes exposed)."""
        result = []
        for h, entry in self._keys.items():
            result.append({
                "key_prefix": h[:8] + "...",
                "label": entry["label"],
                "created": entry["created"],
                "active": entry["active"],
                "requests": entry["requests"],
            })
        return result

    @property
    def active_count(self) -> int:
        """Number of currently active keys."""
        return sum(1 for e in self._keys.values() if e["active"])


_api_key_store = ApiKeyStore()

# If an admin key is configured, register it as a valid API key.
if _ADMIN_API_KEY:
    _api_key_store.create_key("admin-bootstrap")
    # Also accept the admin key directly for auth
    _api_key_store._keys[ApiKeyStore._hash(_ADMIN_API_KEY)] = {
        "label": "admin",
        "created": time.time(),
        "active": True,
        "requests": 0,
    }


def get_api_key_store() -> ApiKeyStore:
    """Return the global API key store (for testing)."""
    return _api_key_store


def is_api_auth_enabled() -> bool:
    """Return True if API key authentication is enabled."""
    return _API_AUTH_ENABLED


app = FastAPI(
    title=config.api_title,
    description=config.api_description,
    version=config.api_version,
)


# ---------------------------------------------------------------------------
# Rate limiting middleware
# ---------------------------------------------------------------------------


@app.middleware("http")
async def rate_limit_middleware(
    request: Request,
    call_next: Any,
) -> Response:
    """Enforce per-IP rate limiting using a token bucket algorithm.

    Returns HTTP 429 when the client exceeds the configured request rate.
    Rate limit headers (X-RateLimit-Limit, X-RateLimit-Remaining) are
    included in every response for client visibility.
    """
    client_ip = request.client.host if request.client else "unknown"
    if not _rate_limiter.allow(client_ip):
        return Response(
            content='{"detail":"Rate limit exceeded. Try again later."}',
            status_code=429,
            media_type="application/json",
            headers={
                "Retry-After": str(int(_RATE_LIMIT_WINDOW)),
                "X-RateLimit-Limit": str(_RATE_LIMIT_MAX),
            },
        )
    response: Response = await call_next(request)
    response.headers["X-RateLimit-Limit"] = str(_RATE_LIMIT_MAX)
    return response


# ---------------------------------------------------------------------------
# Request body size limit middleware
# ---------------------------------------------------------------------------


@app.middleware("http")
async def body_size_limit_middleware(
    request: Request,
    call_next: Any,
) -> Response:
    """Reject requests whose Content-Length exceeds the configured maximum.

    This prevents excessively large payloads from consuming memory before
    they reach the application layer.  Only applies to requests that
    declare a Content-Length header.
    """
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            length = int(content_length)
        except ValueError:
            return Response(
                content='{"detail":"Invalid Content-Length header."}',
                status_code=400,
                media_type="application/json",
            )
        if length > _MAX_BODY_BYTES:
            return Response(
                content=(
                    '{"detail":"Request body too large. '
                    f'Maximum allowed: {_MAX_BODY_BYTES} bytes."}}'
                ),
                status_code=413,
                media_type="application/json",
            )
    return await call_next(request)


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
    response.headers["Permissions-Policy"] = (
        "camera=(), microphone=(), geolocation=()"
    )
    return response


# ---------------------------------------------------------------------------
# API key authentication middleware
# ---------------------------------------------------------------------------


@app.middleware("http")
async def api_key_auth_middleware(
    request: Request,
    call_next: Any,
) -> Response:
    """Enforce API key authentication when enabled.

    Checks the ``Authorization: Bearer <key>`` header against the
    key store.  Exempt paths (health, docs) are always allowed.
    When auth is disabled (default), all requests pass through.
    """
    if not _API_AUTH_ENABLED:
        return await call_next(request)

    # Skip auth for exempt paths
    if request.url.path in _AUTH_EXEMPT_PATHS:
        return await call_next(request)

    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Bearer "):
        return Response(
            content='{"detail":"Missing or invalid Authorization header. '
            'Use: Authorization: Bearer <api_key>"}',
            status_code=401,
            media_type="application/json",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = auth_header[7:]  # Strip "Bearer " prefix
    if not _api_key_store.validate(token):
        return Response(
            content='{"detail":"Invalid or revoked API key."}',
            status_code=403,
            media_type="application/json",
        )

    return await call_next(request)


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
        detail = [
            {"severity": e.severity.value, "message": e.message}
            for e in errors
        ]
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
                # In production mode, omit exception details from
                # the response to prevent information disclosure.
                if _PRODUCTION_MODE:
                    detail = template.format("(details omitted)")
                else:
                    detail = template.format(exc)
                _record_error_and_raise(
                    request.query,
                    t_start,
                    parse_ms,
                    exc,
                    status_code=code,
                    detail=detail,
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
async def analytics_summary(
    last_n: int | None = None,
) -> AnalyticsSummaryResponse:
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


# ---------------------------------------------------------------------------
# API key management endpoints
# ---------------------------------------------------------------------------


def _require_admin(request: Request) -> None:
    """Raise HTTPException 403 if the request does not carry the admin key."""
    if not _ADMIN_API_KEY:
        raise HTTPException(
            status_code=403,
            detail="Admin key not configured. Set PYCYPHER_ADMIN_KEY.",
        )
    auth = request.headers.get("authorization", "")
    if auth != f"Bearer {_ADMIN_API_KEY}":
        raise HTTPException(status_code=403, detail="Admin access required.")


class CreateKeyRequest(BaseModel):
    label: str = Field(
        default="",
        description="Human-readable label for the API key",
    )


class CreateKeyResponse(BaseModel):
    key: str = Field(description="The raw API key (shown once)")
    label: str
    message: str


class RevokeKeyRequest(BaseModel):
    key: str = Field(description="The raw API key to revoke")


class RevokeKeyResponse(BaseModel):
    revoked: bool
    message: str


class ListKeysResponse(BaseModel):
    keys: list[dict[str, Any]]
    active_count: int
    auth_enabled: bool


@app.post("/admin/keys", response_model=CreateKeyResponse)
async def create_api_key(
    request: Request,
    body: CreateKeyRequest,
) -> CreateKeyResponse:
    """Create a new API key. Requires admin authorization."""
    _require_admin(request)
    raw_key = _api_key_store.create_key(label=body.label)
    return CreateKeyResponse(
        key=raw_key,
        label=body.label,
        message="API key created. Store it securely — it cannot be retrieved again.",
    )


@app.post("/admin/keys/revoke", response_model=RevokeKeyResponse)
async def revoke_api_key(
    request: Request,
    body: RevokeKeyRequest,
) -> RevokeKeyResponse:
    """Revoke an existing API key. Requires admin authorization."""
    _require_admin(request)
    revoked = _api_key_store.revoke(body.key)
    if revoked:
        return RevokeKeyResponse(revoked=True, message="API key revoked.")
    return RevokeKeyResponse(
        revoked=False,
        message="Key not found or already revoked.",
    )


@app.get("/admin/keys", response_model=ListKeysResponse)
async def list_api_keys(request: Request) -> ListKeysResponse:
    """List all API keys (metadata only). Requires admin authorization."""
    _require_admin(request)
    return ListKeysResponse(
        keys=_api_key_store.list_keys(),
        active_count=_api_key_store.active_count,
        auth_enabled=_API_AUTH_ENABLED,
    )
