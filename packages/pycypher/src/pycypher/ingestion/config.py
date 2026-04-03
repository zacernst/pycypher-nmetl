"""Pydantic models for the pycypher ETL pipeline YAML configuration file.

The top-level model is :class:`PipelineConfig`, loaded from disk via
:func:`load_pipeline_config`.  Every public field that is not required
for the system to function is typed ``Optional`` and defaults to ``None``
or an empty collection.

Environment variables
---------------------
String values in the YAML file may contain ``${VAR_NAME}`` placeholders.
:func:`load_pipeline_config` substitutes them from the process environment
before Pydantic validation.  Unresolved variables (env var not set) are
left as-is; an error is raised only when the value is actually used (e.g.
when a source is loaded or an output is written).

Supported URI schemes
---------------------
The ``uri`` field on source and output records is validated at config-load
time.  Accepted forms:

* ``file:///path/to/file.{csv,parquet,json}`` — local files
* ``/path/to/file.{csv,parquet,json}`` — bare filesystem paths
* ``s3://``, ``gs://``, ``abfss://``, ``http://``, ``https://`` — cloud /
  remote files, with a ``.csv``, ``.parquet``, or ``.json`` extension
* ``postgresql://``, ``postgres://``, ``mysql://``, ``sqlite://``,
  ``duckdb://`` — SQL databases (a ``query`` field is also required)

Any URI with an unrecognised scheme + extension combination, or a SQL-scheme
URI without a ``query``, raises a ``pydantic.ValidationError`` immediately.
"""

from __future__ import annotations

import os
import re
import warnings
from enum import StrEnum
from pathlib import Path
from typing import Any, NamedTuple
from urllib.parse import urlparse

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator
from shared.logger import LOGGER as _logger

from pycypher.exceptions import SecurityError
from pycypher.ingestion.data_sources import _SQL_SCHEMES, _SUPPORTED_EXTENSIONS

# ---------------------------------------------------------------------------
# URI validation helpers — constants imported from data_sources (single source
# of truth; adding a new format only requires updating data_sources.py)
# ---------------------------------------------------------------------------

_EXT_LIST = ", ".join(sorted(_SUPPORTED_EXTENSIONS))
_SQL_LIST = ", ".join(f"{s}://" for s in sorted(_SQL_SCHEMES))


# ---------------------------------------------------------------------------
# Shared URI validators
# ---------------------------------------------------------------------------


def _check_source_uri(uri: str, query: str | None) -> None:
    """Validate a data-source URI for syntactic correctness and supported type.

    Raises ``ValueError`` if:

    * *uri* is empty or whitespace-only.
    * *uri* has a SQL scheme but no *query* is provided.
    * *uri* has neither a recognised SQL scheme nor a supported file extension.

    Args:
        uri: The URI string to validate.
        query: The optional SQL query accompanying this source.

    Raises:
        ValueError: On any of the conditions above.

    """
    if not uri or not uri.strip():
        msg = (
            "Source 'uri' must not be empty. "
            "Provide a file path (e.g. 'data/people.csv') or a SQL-scheme URI "
            f"({_SQL_LIST})."
        )
        raise ValueError(msg)

    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()

    if scheme in _SQL_SCHEMES:
        if query is None:
            msg = (
                f"Source URI {uri!r} uses a SQL scheme ({scheme}://) but no "
                "'query' field was provided. SQL sources require an explicit query."
            )
            raise ValueError(
                msg,
            )
        return  # no extension check for SQL URIs

    path_lower = parsed.path.lower()
    if not any(path_lower.endswith(ext) for ext in _SUPPORTED_EXTENSIONS):
        msg = (
            f"Source URI {uri!r} has an unrecognised file extension. "
            f"Supported extensions: {_EXT_LIST}. "
            f"For database sources use a SQL-scheme URI ({_SQL_LIST})."
        )
        raise ValueError(
            msg,
        )


def _check_output_uri(uri: str) -> None:
    """Validate an output sink URI for syntactic correctness and supported type.

    Output URIs must resolve to a writable file with a supported extension.
    SQL-scheme URIs are not accepted as output sinks.

    Args:
        uri: The URI string to validate.

    Raises:
        ValueError: If *uri* is empty, or has an unsupported file extension.

    """
    if not uri or not uri.strip():
        msg = (
            "Output 'uri' must not be empty. "
            f"Provide a file path with a supported extension ({_EXT_LIST})."
        )
        raise ValueError(msg)

    parsed = urlparse(uri)
    path_lower = parsed.path.lower()
    if not any(path_lower.endswith(ext) for ext in _SUPPORTED_EXTENSIONS):
        msg = (
            f"Output URI {uri!r} has an unrecognised file extension. "
            f"Supported extensions: {_EXT_LIST}."
        )
        raise ValueError(
            msg,
        )


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class ErrorHandlingPolicy(StrEnum):
    """Policy applied when a source fails to load or a query fails.

    Attributes:
        FAIL: Abort the pipeline immediately (default).
        WARN: Log a warning and continue.
        SKIP: Silently skip the failing item and continue.

    """

    FAIL = "fail"
    WARN = "warn"
    SKIP = "skip"


class SkippedSource(NamedTuple):
    """Record of a data source that was suppressed by an error policy.

    Returned by :meth:`PipelineConfig.load_with_sources` so callers can
    programmatically inspect which sources failed and why, rather than
    relying solely on log output.

    Attributes:
        source_id: The ``id`` of the source that failed.
        error: The exception that caused the failure.
        policy: The error handling policy that suppressed the failure.

    """

    source_id: str
    error: Exception
    policy: ErrorHandlingPolicy


class OutputFormat(StrEnum):
    """Explicit output serialisation format.

    When absent, the format is inferred from the ``uri`` file extension.

    Attributes:
        CSV: Comma-separated values.
        PARQUET: Apache Parquet columnar format.
        JSON: JSON (newline-delimited records).

    """

    CSV = "csv"
    PARQUET = "parquet"
    JSON = "json"


# ---------------------------------------------------------------------------
# Project metadata
# ---------------------------------------------------------------------------


class ProjectConfig(BaseModel):
    """Human-readable project metadata.

    Attributes:
        name: Short identifier for the pipeline (used in logs and artefacts).
        description: Optional longer description of the pipeline's purpose.

    """

    name: str
    description: str | None = None


# ---------------------------------------------------------------------------
# Source configurations
# ---------------------------------------------------------------------------


class EntitySourceConfig(BaseModel):
    """Configuration for a single entity (node) data source.

    Attributes:
        id: Unique logical identifier for this source within the config file.
        uri: URI of the data source.  See module docstring for supported
            schemes.
        entity_type: The Cypher entity label assigned to rows from this
            source (e.g. ``"Person"``).
        id_col: Column in the source whose values become ``__ID__``.  When
            absent, sequential integers are auto-generated.
        query: Optional DuckDB SQL applied after loading the source.  The
            loaded data is available as the virtual table ``source``; when
            omitted ``SELECT * FROM source`` is used.
        schema_hints: Optional mapping of column name → Arrow/DuckDB type
            string (e.g. ``{"zip_code": "VARCHAR", "age": "INTEGER"}``).
            Applied before the data enters the pipeline to prevent type
            inference surprises.
        on_error: What to do if this source cannot be loaded.  Defaults to
            the pipeline-level policy (``fail`` when unspecified).

    """

    id: str
    uri: str
    entity_type: str
    id_col: str | None = None
    query: str | None = None
    schema_hints: dict[str, str] | None = None
    on_error: ErrorHandlingPolicy | None = None

    @model_validator(mode="after")
    def check_uri(self) -> EntitySourceConfig:
        """Validate that *uri* is syntactically correct and of a supported type."""
        _check_source_uri(self.uri, self.query)
        return self


class RelationshipSourceConfig(BaseModel):
    """Configuration for a single relationship (edge) data source.

    ``source_col`` and ``target_col`` are required — there is no sensible
    default for relationship endpoints.

    Attributes:
        id: Unique logical identifier for this source.
        uri: URI of the data source.
        relationship_type: The Cypher relationship type label (e.g.
            ``"WORKS_FOR"``).
        source_col: Column whose values are the source node IDs
            (mapped to ``__SOURCE__``).
        target_col: Column whose values are the target node IDs
            (mapped to ``__TARGET__``).
        id_col: Column to use as ``__ID__`` for the relationship itself.
            Auto-generated when absent.
        query: Optional DuckDB SQL applied after loading.
        schema_hints: Optional column-type overrides.
        on_error: What to do if this source cannot be loaded.

    """

    id: str
    uri: str
    relationship_type: str
    source_col: str
    target_col: str
    id_col: str | None = None
    query: str | None = None
    schema_hints: dict[str, str] | None = None
    on_error: ErrorHandlingPolicy | None = None

    @model_validator(mode="after")
    def check_uri(self) -> RelationshipSourceConfig:
        """Validate that *uri* is syntactically correct and of a supported type."""
        _check_source_uri(self.uri, self.query)
        return self


class SourcesConfig(BaseModel):
    """Container for all entity and relationship source definitions.

    Source ``id`` values must be unique across both the entity and relationship
    lists — all sources share the same logical namespace.

    Attributes:
        entities: Zero or more entity source configurations.
        relationships: Zero or more relationship source configurations.

    """

    entities: list[EntitySourceConfig] = Field(default_factory=list)
    relationships: list[RelationshipSourceConfig] = Field(default_factory=list)

    def get_source_by_id(
        self,
        source_id: str,
    ) -> EntitySourceConfig | RelationshipSourceConfig | None:
        """Look up a source by its unique ``id``.

        Searches both the entity and relationship source lists.  The id
        namespace is shared across both lists (enforced by
        :meth:`check_unique_source_ids`), so at most one source can match.

        Args:
            source_id: The ``id`` value to look up.

        Returns:
            The matching :class:`EntitySourceConfig` or
            :class:`RelationshipSourceConfig`, or ``None`` if no source with
            that id exists.

        """
        for source in [*self.entities, *self.relationships]:
            if source.id == source_id:
                return source
        return None

    @model_validator(mode="after")
    def check_unique_source_ids(self) -> SourcesConfig:
        """Enforce that every source ``id`` is unique across entities and relationships.

        Raises:
            ValueError: If any ``id`` appears more than once, listing the
                duplicates.

        """
        all_ids = [s.id for s in self.entities] + [
            s.id for s in self.relationships
        ]
        seen: set[str] = set()
        duplicates: set[str] = set()
        for sid in all_ids:
            if sid in seen:
                duplicates.add(sid)
            seen.add(sid)
        if duplicates:
            dup_list = ", ".join(sorted(repr(d) for d in duplicates))
            msg = (
                f"Duplicate source id(s) found: {dup_list}. "
                "Every source must have a unique 'id'."
            )
            raise ValueError(
                msg,
            )
        return self


# ---------------------------------------------------------------------------
# Function registration
# ---------------------------------------------------------------------------


# Modules that must never be importable via user config to prevent
# arbitrary code execution, file-system access, or network exfiltration.
_BLOCKED_IMPORT_MODULES: frozenset[str] = frozenset(
    {
        "builtins",
        "code",
        "codeop",
        "compileall",
        "ctypes",
        "importlib",
        "io",
        "multiprocessing",
        "os",
        "pathlib",
        "pickle",
        "shutil",
        "signal",
        "socket",
        "subprocess",
        "sys",
        "tempfile",
        "webbrowser",
    },
)

_DOTTED_IMPORT_PATH = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$",
)


def _validate_import_path(path: str) -> str:
    """Reject import paths that target dangerous stdlib modules."""
    if not _DOTTED_IMPORT_PATH.match(path):
        msg = f"Invalid import path: {path!r}. Must be a dotted Python identifier."
        raise ValueError(msg)
    top_level = path.split(".")[0]
    if top_level in _BLOCKED_IMPORT_MODULES:
        msg = (
            f"Import path {path!r} is blocked: module {top_level!r} is not "
            "allowed for security reasons."
        )
        raise ValueError(msg)
    return path


class FunctionConfig(BaseModel):
    """Register one or more Python callables as Cypher functions.

    Two mutually exclusive forms are supported:

    **Module form** — register specific functions (or all public ones) from a
    Python module::

        module: "mypackage.string_utils"
        names:
          - normalize_name
          - parse_phone_number
        # OR: names: "*"  (registers all public callables)

    **Callable form** — register a single function by its fully-qualified
    dotted import path::

        callable: "mypackage.string_utils.normalize_name"

    Attributes:
        module: Dotted import path of the module to register from.
        names: List of function names within ``module`` to register, or
            ``"*"`` to register all public callables.  Required when
            ``module`` is specified.
        callable: Fully-qualified dotted path to a single callable.
            Mutually exclusive with ``module``.

    """

    module: str | None = None
    names: list[str] | str | None = None  # list of names or "*"
    callable: str | None = None

    @field_validator("module", mode="after")
    @classmethod
    def validate_module_path(cls, v: str | None) -> str | None:
        """Validate module import path against blocked list."""
        if v is not None:
            return _validate_import_path(v)
        return v

    @field_validator("callable", mode="after")
    @classmethod
    def validate_callable_path(cls, v: str | None) -> str | None:
        """Validate callable import path against blocked list."""
        if v is not None:
            return _validate_import_path(v)
        return v

    @model_validator(mode="after")
    def check_exclusive_forms(self) -> FunctionConfig:
        """Enforce mutual exclusivity of module-form vs. callable-form."""
        has_module = self.module is not None
        has_callable = self.callable is not None
        if has_module and has_callable:
            msg = "Specify either 'module' (with 'names') or 'callable', not both."
            raise ValueError(
                msg,
            )
        if not has_module and not has_callable:
            msg = "Specify either 'module' (with 'names') or 'callable'."
            raise ValueError(
                msg,
            )
        if has_module and self.names is None:
            msg = (
                "'names' is required when 'module' is specified. "
                'Use names: "*" to register all public callables.'
            )
            raise ValueError(
                msg,
            )
        return self


# ---------------------------------------------------------------------------
# Query configuration
# ---------------------------------------------------------------------------


class QueryConfig(BaseModel):
    """A named Cypher query defined either by file reference or inline text.

    Exactly one of ``source`` or ``inline`` must be provided.

    Attributes:
        id: Unique identifier for this query within the pipeline.
        description: Optional human-readable description.
        source: Path to a ``.cypher`` file containing the query.  Relative
            paths are resolved from the directory containing the config file.
        inline: Literal Cypher query text.  Intended as a convenience escape
            hatch for short, stable queries; prefer ``source`` for anything
            non-trivial.

    """

    id: str
    description: str | None = None
    source: str | None = None
    inline: str | None = None

    @model_validator(mode="after")
    def check_query_source(self) -> QueryConfig:
        """Enforce that exactly one of source or inline is provided."""
        has_source = self.source is not None
        has_inline = self.inline is not None
        if has_source and has_inline:
            msg = f"Query {self.id!r}: specify either 'source' or 'inline', not both."
            raise ValueError(
                msg,
            )
        if not has_source and not has_inline:
            msg = (
                f"Query {self.id!r}: specify either 'source' (path to .cypher "
                "file) or 'inline' (Cypher text)."
            )
            raise ValueError(
                msg,
            )
        return self


# ---------------------------------------------------------------------------
# Output (sink) configuration
# ---------------------------------------------------------------------------


class OutputConfig(BaseModel):
    """Sink configuration for writing a query's result.

    Attributes:
        query_id: The ``id`` of the :class:`QueryConfig` whose result is
            written to this sink.
        uri: Destination URI.  Format is inferred from the file extension
            when ``format`` is absent.
        format: Explicit output format.  When ``None``, inferred from the
            URI extension (e.g. ``.parquet`` → ``parquet``).

    """

    query_id: str
    uri: str
    format: OutputFormat | None = None

    @model_validator(mode="after")
    def check_uri(self) -> OutputConfig:
        """Validate that *uri* is syntactically correct and of a supported type."""
        _check_output_uri(self.uri)
        return self


# ---------------------------------------------------------------------------
# Config schema versioning
# ---------------------------------------------------------------------------

#: Versions that the current code can fully load without any migration.
SUPPORTED_CONFIG_VERSIONS: frozenset[str] = frozenset({"1.0"})

#: The latest schema version — used as the default for new configs.
CURRENT_CONFIG_VERSION: str = "1.0"

_VERSION_RE = re.compile(r"^\d+\.\d+$")


# ---------------------------------------------------------------------------
# Top-level pipeline configuration
# ---------------------------------------------------------------------------


class PipelineConfig(BaseModel):
    """Root configuration model for a pycypher ETL pipeline.

    Attributes:
        version: Configuration schema version.  Must match a version in
            :data:`SUPPORTED_CONFIG_VERSIONS`.  Defaults to ``"1.0"``.
        project: Optional project metadata (name, description).
        sources: Entity and relationship source definitions.
        functions: Python callables to register as Cypher functions.
        queries: Named Cypher queries.  Execution order is automatically
            inferred from the entity/relationship types referenced in each
            query's parsed AST.
        output: Sink configurations mapping query results to output URIs.

    """

    version: str = "1.0"
    project: ProjectConfig | None = None
    sources: SourcesConfig = Field(default_factory=SourcesConfig)
    functions: list[FunctionConfig] = Field(default_factory=list)
    queries: list[QueryConfig] = Field(default_factory=list)
    output: list[OutputConfig] = Field(default_factory=list)

    @field_validator("version")
    @classmethod
    def check_version(cls, v: str) -> str:
        """Validate that *version* is a supported config schema version.

        Raises :class:`ValueError` for unknown versions with actionable
        guidance, and emits a :class:`FutureWarning` for versions that look
        newer than the current code supports (e.g. ``"2.0"`` when only
        ``"1.0"`` is supported).
        """
        if not _VERSION_RE.match(v):
            msg = (
                f"Invalid config version format: {v!r}. "
                "Expected '<major>.<minor>' (e.g. '1.0')."
            )
            raise ValueError(msg)

        if v in SUPPORTED_CONFIG_VERSIONS:
            return v

        # Parse major.minor for a more helpful message.
        major, minor = v.split(".")
        current_major, _ = CURRENT_CONFIG_VERSION.split(".")

        if int(major) > int(current_major):
            msg = (
                f"Config version {v!r} is newer than this version of pycypher "
                f"supports (supported: {', '.join(sorted(SUPPORTED_CONFIG_VERSIONS))}). "
                "Please upgrade pycypher, or downgrade your config file."
            )
            raise ValueError(msg)

        # Same major but unknown minor — warn but allow (forward-compatible
        # within a major version by convention).
        warnings.warn(
            f"Config version {v!r} is not explicitly supported "
            f"(supported: {', '.join(sorted(SUPPORTED_CONFIG_VERSIONS))}). "
            "Loading will proceed but some fields may be ignored.",
            FutureWarning,
            stacklevel=2,
        )
        return v

    def get_source_by_id(
        self,
        source_id: str,
    ) -> EntitySourceConfig | RelationshipSourceConfig | None:
        """Look up a data source by its unique ``id``.

        Convenience passthrough to :meth:`SourcesConfig.get_source_by_id`.

        Args:
            source_id: The ``id`` value to look up.

        Returns:
            The matching source config, or ``None`` if not found.

        """
        return self.sources.get_source_by_id(source_id)

    @classmethod
    def load_with_sources(
        cls,
        path: str | Path,
    ) -> tuple[PipelineConfig, dict[str, Any], list[SkippedSource]]:
        """Load a config file and eagerly read all data sources into Arrow tables.

        This is the single entry point for pipelines that want both a validated
        config and pre-loaded Arrow tables in one call.

        Args:
            path: Path to the YAML pipeline configuration file.

        Returns:
            A ``(config, sources, skipped)`` tuple where *config* is a
            validated :class:`PipelineConfig`, *sources* is a ``dict``
            mapping each source ``id`` to its ``pa.Table``, and *skipped*
            is a list of :class:`SkippedSource` records for any sources
            suppressed by a WARN or SKIP error policy.

        Raises:
            FileNotFoundError: If *path* does not exist.
            yaml.YAMLError: If the file contains invalid YAML.
            pydantic.ValidationError: If the configuration is structurally
                invalid.

        """
        from pycypher.ingestion.data_sources import data_source_from_uri

        config = load_pipeline_config(path)
        result: dict[str, Any] = {}
        skipped: list[SkippedSource] = []
        for source in [
            *config.sources.entities,
            *config.sources.relationships,
        ]:
            try:
                ds = data_source_from_uri(source.uri, query=source.query)
                result[source.id] = ds.read()
            except Exception as exc:  # noqa: BLE001 — error policy dispatch; SecurityError always re-raised
                # SecurityError must always propagate — error policies must
                # never suppress security violations (path traversal, injection, etc.).
                if isinstance(exc, SecurityError):
                    raise
                policy = source.on_error or ErrorHandlingPolicy.FAIL
                if policy is ErrorHandlingPolicy.FAIL:
                    raise
                skipped.append(
                    SkippedSource(
                        source_id=source.id,
                        error=exc,
                        policy=policy,
                    )
                )
                if policy is ErrorHandlingPolicy.WARN:
                    _logger.warning(
                        f"Source '{source.id}' failed to load and will be skipped: {exc}",
                    )
                else:
                    _logger.debug(
                        f"Source '{source.id}' skipped due to error (on_error=skip): {exc}",
                    )
        return config, result, skipped


# ---------------------------------------------------------------------------
# YAML loader with environment-variable substitution
# ---------------------------------------------------------------------------

_ENV_VAR_RE = re.compile(r"\$\{([^}]+)\}")

# Environment variables whose values must never leak into pipeline configs.
# These are blocked to prevent credential exfiltration when config files
# are shared or logged.  Note: database connection variables (DB_PASSWORD,
# PGPASSWORD, etc.) are intentionally NOT blocked because they are a
# primary use case for env var substitution in pipeline YAML configs.
_BLOCKED_ENV_VARS: frozenset[str] = frozenset(
    {
        # Cloud provider credentials — rarely needed in pipeline URI strings
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AZURE_CLIENT_SECRET",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "GCP_SERVICE_ACCOUNT_KEY",
        # API keys and tokens — not relevant to data pipeline configs
        "API_SECRET",
        "SECRET_KEY",
        "PRIVATE_KEY",
        "GITHUB_TOKEN",
        "GITLAB_TOKEN",
        "NPM_TOKEN",
        "PYPI_TOKEN",
        # SSH/TLS keys
        "SSH_PRIVATE_KEY",
        "SSL_KEY",
        "TLS_KEY",
    },
)

# Prefixes that signal a variable likely contains non-pipeline secrets.
_BLOCKED_ENV_PREFIXES: tuple[str, ...] = (
    "PRIVATE_",
    "CREDENTIAL_",
)


def _substitute_env_vars(value: Any) -> Any:
    """Recursively replace ``${VAR}`` placeholders with environment values.

    Blocks known-sensitive environment variables (credentials, API keys,
    cloud secrets) to prevent accidental leakage when config files are
    shared or logged.  Unresolved variables (env var not set or blocked)
    are left as ``${VAR}`` strings.

    Args:
        value: Any Python value from a parsed YAML document.

    Returns:
        The value with all resolvable, non-sensitive ``${VAR}``
        placeholders replaced.

    """
    if isinstance(value, str):
        return _ENV_VAR_RE.sub(_safe_env_replace, value)
    if isinstance(value, dict):
        return {k: _substitute_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_substitute_env_vars(item) for item in value]
    return value


def _safe_env_replace(match: re.Match[str]) -> str:
    """Replace a single ``${VAR}`` match, blocking sensitive variables.

    Args:
        match: Regex match object with group(1) being the variable name.

    Returns:
        The environment value, or the original ``${VAR}`` placeholder if
        the variable is blocked or not set.

    """
    var_name = match.group(1)
    upper = var_name.upper()

    if upper in _BLOCKED_ENV_VARS or upper.startswith(_BLOCKED_ENV_PREFIXES):
        _logger.warning(
            "Blocked expansion of sensitive env var ${%s} in pipeline config",
            var_name,
        )
        return match.group(0)  # leave as-is

    return os.environ.get(var_name, match.group(0))


def load_pipeline_config(path: str | Path) -> PipelineConfig:
    """Load and validate a pipeline configuration from a YAML file.

    Processing steps:

    1. Read and parse the YAML file.
    2. Substitute ``${VAR}`` placeholders from the process environment.
    3. Validate the resulting dict against :class:`PipelineConfig`.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        A fully validated :class:`PipelineConfig` instance.

    Raises:
        FileNotFoundError: If *path* does not exist.
        yaml.YAMLError: If the file contains invalid YAML.
        pydantic.ValidationError: If the configuration is structurally
            invalid (missing required fields, constraint violations, etc.).

    """
    path = Path(path)
    raw: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
    resolved = _substitute_env_vars(raw or {})
    return PipelineConfig.model_validate(resolved)
