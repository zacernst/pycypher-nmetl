"""Unit tests for the PipelineConfig Pydantic model and YAML loader.

Covers:
- Direct model construction for every sub-model
- Optional field defaults
- Validation errors for constraint violations
- load_pipeline_config() round-trips against all fixture files
- Environment-variable substitution
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from pycypher.ingestion.config import (
    CURRENT_CONFIG_VERSION,
    SUPPORTED_CONFIG_VERSIONS,
    EntitySourceConfig,
    ErrorHandlingPolicy,
    FunctionConfig,
    OutputConfig,
    OutputFormat,
    PipelineConfig,
    ProjectConfig,
    QueryConfig,
    RelationshipSourceConfig,
    SourcesConfig,
    _substitute_env_vars,
    load_pipeline_config,
)
from pydantic import ValidationError

FIXTURES = Path(__file__).parent / "fixtures" / "configs"
INVALID = FIXTURES / "invalid"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load(name: str) -> PipelineConfig:
    """Load a fixture config by filename."""
    return load_pipeline_config(FIXTURES / name)


def _load_invalid(name: str) -> None:
    """Attempt to load an invalid fixture; expected to raise ValidationError."""
    load_pipeline_config(INVALID / name)


# ===========================================================================
# TestProjectConfig
# ===========================================================================


class TestProjectConfig:
    def test_name_required(self) -> None:
        with pytest.raises(ValidationError, match="name"):
            ProjectConfig.model_validate({})

    def test_description_optional(self) -> None:
        p = ProjectConfig(name="my_pipeline")
        assert p.description is None

    def test_full(self) -> None:
        p = ProjectConfig(name="my_pipeline", description="A test pipeline")
        assert p.name == "my_pipeline"
        assert p.description == "A test pipeline"


# ===========================================================================
# TestEntitySourceConfig
# ===========================================================================


class TestEntitySourceConfig:
    def test_required_fields(self) -> None:
        e = EntitySourceConfig(
            id="persons",
            uri="file:///data/persons.csv",
            entity_type="Person",
        )
        assert e.id == "persons"
        assert e.uri == "file:///data/persons.csv"
        assert e.entity_type == "Person"

    def test_optional_fields_default_to_none(self) -> None:
        e = EntitySourceConfig(
            id="persons",
            uri="file:///data/persons.csv",
            entity_type="Person",
        )
        assert e.id_col is None
        assert e.query is None
        assert e.schema_hints is None
        assert e.on_error is None

    def test_all_fields(self) -> None:
        e = EntitySourceConfig(
            id="persons",
            uri="s3://bucket/persons.parquet",
            entity_type="Person",
            id_col="person_id",
            query="SELECT * FROM source WHERE active = true",
            schema_hints={"person_id": "INTEGER", "name": "VARCHAR"},
            on_error=ErrorHandlingPolicy.WARN,
        )
        assert e.id_col == "person_id"
        assert e.schema_hints == {"person_id": "INTEGER", "name": "VARCHAR"}
        assert e.on_error == ErrorHandlingPolicy.WARN

    def test_on_error_string_coercion(self) -> None:
        """String values are coerced to the enum."""
        e = EntitySourceConfig(
            id="x",
            uri="file:///x.csv",
            entity_type="X",
            on_error="skip",
        )
        assert e.on_error is ErrorHandlingPolicy.SKIP

    def test_missing_required_fields(self) -> None:
        with pytest.raises(ValidationError):
            EntitySourceConfig(
                id="x",
                uri="file:///x.csv",
            )  # missing entity_type


# ===========================================================================
# TestRelationshipSourceConfig
# ===========================================================================


class TestRelationshipSourceConfig:
    def test_required_fields(self) -> None:
        r = RelationshipSourceConfig(
            id="employment",
            uri="file:///data/employment.parquet",
            relationship_type="WORKS_FOR",
            source_col="person_id",
            target_col="company_id",
        )
        assert r.source_col == "person_id"
        assert r.target_col == "company_id"

    def test_optional_fields_default_to_none(self) -> None:
        r = RelationshipSourceConfig(
            id="e",
            uri="file:///e.parquet",
            relationship_type="KNOWS",
            source_col="a",
            target_col="b",
        )
        assert r.id_col is None
        assert r.query is None
        assert r.schema_hints is None
        assert r.on_error is None

    def test_source_col_required(self) -> None:
        with pytest.raises(ValidationError):
            RelationshipSourceConfig(
                id="e",
                uri="file:///e.parquet",
                relationship_type="KNOWS",
                target_col="b",
            )

    def test_target_col_required(self) -> None:
        with pytest.raises(ValidationError):
            RelationshipSourceConfig(
                id="e",
                uri="file:///e.parquet",
                relationship_type="KNOWS",
                source_col="a",
            )

    def test_all_optional_fields(self) -> None:
        r = RelationshipSourceConfig(
            id="e",
            uri="file:///e.parquet",
            relationship_type="KNOWS",
            source_col="a",
            target_col="b",
            id_col="edge_id",
            query="SELECT * FROM source",
            schema_hints={"a": "INTEGER"},
            on_error=ErrorHandlingPolicy.FAIL,
        )
        assert r.id_col == "edge_id"
        assert r.on_error is ErrorHandlingPolicy.FAIL


# ===========================================================================
# TestSourcesConfig
# ===========================================================================


class TestSourcesConfig:
    def test_empty_by_default(self) -> None:
        s = SourcesConfig()
        assert s.entities == []
        assert s.relationships == []

    def test_entities_populated(self) -> None:
        entity = EntitySourceConfig(
            id="p",
            uri="file:///p.csv",
            entity_type="Person",
        )
        s = SourcesConfig(entities=[entity])
        assert len(s.entities) == 1
        assert s.entities[0].entity_type == "Person"


# ===========================================================================
# TestFunctionConfig
# ===========================================================================


class TestFunctionConfig:
    def test_module_form_list(self) -> None:
        f = FunctionConfig(
            module="mypackage.utils",
            names=["func_a", "func_b"],
        )
        assert f.module == "mypackage.utils"
        assert f.names == ["func_a", "func_b"]
        assert f.callable is None

    def test_module_form_wildcard(self) -> None:
        f = FunctionConfig(module="mypackage.utils", names="*")
        assert f.names == "*"

    def test_callable_form(self) -> None:
        f = FunctionConfig(callable="mypackage.utils.my_func")
        assert f.callable == "mypackage.utils.my_func"
        assert f.module is None
        assert f.names is None

    def test_both_module_and_callable_raises(self) -> None:
        with pytest.raises(ValidationError, match="not both"):
            FunctionConfig(
                module="mypackage.utils",
                names=["f"],
                callable="mypackage.utils.f",
            )

    def test_neither_raises(self) -> None:
        with pytest.raises(ValidationError, match="either"):
            FunctionConfig()

    def test_module_without_names_raises(self) -> None:
        with pytest.raises(ValidationError, match="'names' is required"):
            FunctionConfig(module="mypackage.utils")


# ===========================================================================
# TestQueryConfig
# ===========================================================================


class TestQueryConfig:
    def test_inline_form(self) -> None:
        q = QueryConfig(id="q1", inline="MATCH (n) RETURN n")
        assert q.inline == "MATCH (n) RETURN n"
        assert q.source is None

    def test_source_form(self) -> None:
        q = QueryConfig(id="q1", source="queries/q1.cypher")
        assert q.source == "queries/q1.cypher"
        assert q.inline is None

    def test_description_optional(self) -> None:
        q = QueryConfig(id="q1", inline="MATCH (n) RETURN n")
        assert q.description is None

    def test_description_set(self) -> None:
        q = QueryConfig(
            id="q1",
            inline="MATCH (n) RETURN n",
            description="All nodes",
        )
        assert q.description == "All nodes"

    def test_both_source_and_inline_raises(self) -> None:
        with pytest.raises(ValidationError, match="not both"):
            QueryConfig(
                id="q1",
                source="queries/q1.cypher",
                inline="MATCH (n) RETURN n",
            )

    def test_neither_source_nor_inline_raises(self) -> None:
        with pytest.raises(ValidationError, match="either"):
            QueryConfig(id="q1")

    def test_error_message_includes_query_id(self) -> None:
        with pytest.raises(ValidationError, match="'q_named'"):
            QueryConfig(id="q_named")


# ===========================================================================
# TestOutputConfig
# ===========================================================================


class TestOutputConfig:
    def test_required_fields(self) -> None:
        o = OutputConfig(query_id="q1", uri="file:///output/result.csv")
        assert o.query_id == "q1"
        assert o.uri == "file:///output/result.csv"

    def test_format_optional(self) -> None:
        o = OutputConfig(query_id="q1", uri="file:///output/result.csv")
        assert o.format is None

    def test_format_set(self) -> None:
        o = OutputConfig(
            query_id="q1",
            uri="file:///output/result.csv",
            format=OutputFormat.CSV,
        )
        assert o.format is OutputFormat.CSV

    def test_format_string_coercion(self) -> None:
        o = OutputConfig(
            query_id="q1",
            uri="file:///r.parquet",
            format="parquet",
        )
        assert o.format is OutputFormat.PARQUET


# ===========================================================================
# TestPipelineConfig
# ===========================================================================


class TestPipelineConfig:
    def test_defaults(self) -> None:
        cfg = PipelineConfig()
        assert cfg.version == "1.0"
        assert cfg.project is None
        assert cfg.sources.entities == []
        assert cfg.sources.relationships == []
        assert cfg.functions == []
        assert cfg.queries == []
        assert cfg.output == []

    def test_version_default(self) -> None:
        cfg = PipelineConfig()
        assert cfg.version == "1.0"

    def test_supported_version_accepted(self) -> None:
        for v in SUPPORTED_CONFIG_VERSIONS:
            cfg = PipelineConfig(version=v)
            assert cfg.version == v

    def test_newer_major_version_rejected(self) -> None:
        with pytest.raises(ValidationError, match="newer than this version"):
            PipelineConfig(version="2.0")

    def test_unknown_minor_version_warns(self) -> None:
        with pytest.warns(FutureWarning, match="not explicitly supported"):
            cfg = PipelineConfig(version="1.1")
        assert cfg.version == "1.1"

    def test_invalid_version_format_rejected(self) -> None:
        with pytest.raises(ValidationError, match="Invalid config version format"):
            PipelineConfig(version="latest")

    def test_invalid_version_format_no_dot(self) -> None:
        with pytest.raises(ValidationError, match="Invalid config version format"):
            PipelineConfig(version="1")

    def test_version_constants_consistent(self) -> None:
        assert CURRENT_CONFIG_VERSION in SUPPORTED_CONFIG_VERSIONS


# ===========================================================================
# TestEnvVarSubstitution
# ===========================================================================


class TestEnvVarSubstitution:
    def test_string_substitution(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("MY_VAR", "hello")
        result = _substitute_env_vars("${MY_VAR} world")
        assert result == "hello world"

    def test_unset_var_left_as_placeholder(self) -> None:
        # Make sure the var is not set
        os.environ.pop("UNSET_TEST_VAR_XYZ", None)
        result = _substitute_env_vars("${UNSET_TEST_VAR_XYZ}/path")
        assert result == "${UNSET_TEST_VAR_XYZ}/path"

    def test_nested_dict_substitution(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DB_HOST", "localhost")
        monkeypatch.setenv("DB_PORT", "5432")
        data = {"uri": "postgresql://user@${DB_HOST}:${DB_PORT}/db"}
        result = _substitute_env_vars(data)
        assert result == {"uri": "postgresql://user@localhost:5432/db"}

    def test_nested_list_substitution(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("BUCKET", "my-bucket")
        data = ["s3://${BUCKET}/a.parquet", "s3://${BUCKET}/b.parquet"]
        result = _substitute_env_vars(data)
        assert result == [
            "s3://my-bucket/a.parquet",
            "s3://my-bucket/b.parquet",
        ]

    def test_non_string_values_pass_through(self) -> None:
        assert _substitute_env_vars(42) == 42
        assert _substitute_env_vars(3.14) == 3.14
        assert _substitute_env_vars(True) is True
        assert _substitute_env_vars(None) is None

    def test_multiple_vars_in_one_string(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("USER", "alice")
        monkeypatch.setenv("PASS", "secret")
        result = _substitute_env_vars("postgresql://${USER}:${PASS}@host/db")
        assert result == "postgresql://alice:secret@host/db"


# ===========================================================================
# TestLoadPipelineConfig — fixture round-trips
# ===========================================================================


class TestLoadPipelineConfigMinimal:
    def test_loads_without_error(self) -> None:
        cfg = _load("minimal.yaml")
        assert isinstance(cfg, PipelineConfig)

    def test_entity_type(self) -> None:
        cfg = _load("minimal.yaml")
        assert cfg.sources.entities[0].entity_type == "Person"

    def test_no_project(self) -> None:
        cfg = _load("minimal.yaml")
        assert cfg.project is None

    def test_no_functions(self) -> None:
        cfg = _load("minimal.yaml")
        assert cfg.functions == []

    def test_no_queries(self) -> None:
        cfg = _load("minimal.yaml")
        assert cfg.queries == []

    def test_no_output(self) -> None:
        cfg = _load("minimal.yaml")
        assert cfg.output == []


class TestLoadPipelineConfigFullPipeline:
    def test_loads_without_error(self) -> None:
        cfg = _load("full_pipeline.yaml")
        assert isinstance(cfg, PipelineConfig)

    def test_project_metadata(self) -> None:
        cfg = _load("full_pipeline.yaml")
        assert cfg.project is not None
        assert cfg.project.name == "customer_graph"
        assert cfg.project.description is not None

    def test_entity_count(self) -> None:
        cfg = _load("full_pipeline.yaml")
        assert len(cfg.sources.entities) == 3

    def test_relationship_count(self) -> None:
        cfg = _load("full_pipeline.yaml")
        assert len(cfg.sources.relationships) == 2

    def test_entity_with_query(self) -> None:
        cfg = _load("full_pipeline.yaml")
        persons = next(e for e in cfg.sources.entities if e.id == "persons")
        assert persons.query is not None
        assert "active = true" in persons.query

    def test_entity_with_schema_hints(self) -> None:
        cfg = _load("full_pipeline.yaml")
        persons = next(e for e in cfg.sources.entities if e.id == "persons")
        assert persons.schema_hints == {
            "person_id": "INTEGER",
            "name": "VARCHAR",
            "age": "INTEGER",
        }

    def test_entity_on_error(self) -> None:
        cfg = _load("full_pipeline.yaml")
        persons = next(e for e in cfg.sources.entities if e.id == "persons")
        assert persons.on_error is ErrorHandlingPolicy.FAIL

    def test_relationship_cols(self) -> None:
        cfg = _load("full_pipeline.yaml")
        emp = next(r for r in cfg.sources.relationships if r.id == "employment")
        assert emp.source_col == "person_id"
        assert emp.target_col == "company_id"
        assert emp.id_col == "employment_id"

    def test_function_module_form(self) -> None:
        cfg = _load("full_pipeline.yaml")
        f = next(fn for fn in cfg.functions if fn.module == "mypackage.string_utils")
        assert f.names == ["normalize_name", "parse_phone_number"]

    def test_function_wildcard_form(self) -> None:
        cfg = _load("full_pipeline.yaml")
        f = next(fn for fn in cfg.functions if fn.module == "mypackage.math_utils")
        assert f.names == "*"

    def test_function_callable_form(self) -> None:
        cfg = _load("full_pipeline.yaml")
        f = next(fn for fn in cfg.functions if fn.callable is not None)
        assert f.callable == "mypackage.formatters.format_currency"

    def test_query_external_source(self) -> None:
        cfg = _load("full_pipeline.yaml")
        q = next(q for q in cfg.queries if q.id == "customer_summary")
        assert q.source == "cypher/customer_summary.cypher"
        assert q.inline is None

    def test_query_inline(self) -> None:
        cfg = _load("full_pipeline.yaml")
        q = next(q for q in cfg.queries if q.id == "active_employees")
        assert q.inline is not None
        assert "WORKS_FOR" in q.inline
        assert q.source is None

    def test_output_count(self) -> None:
        cfg = _load("full_pipeline.yaml")
        assert len(cfg.output) == 2

    def test_output_format_csv(self) -> None:
        cfg = _load("full_pipeline.yaml")
        csv_out = next(o for o in cfg.output if o.format == OutputFormat.CSV)
        assert "employees.csv" in csv_out.uri


class TestLoadPipelineConfigEntitiesOnly:
    def test_loads_without_error(self) -> None:
        cfg = _load("entities_only.yaml")
        assert len(cfg.sources.entities) == 2
        assert cfg.sources.relationships == []


class TestLoadPipelineConfigRelationshipsOnly:
    def test_loads_without_error(self) -> None:
        cfg = _load("relationships_only.yaml")
        assert cfg.sources.entities == []
        assert len(cfg.sources.relationships) == 1

    def test_relationship_fields(self) -> None:
        cfg = _load("relationships_only.yaml")
        r = cfg.sources.relationships[0]
        assert r.relationship_type == "WORKS_FOR"
        assert r.source_col == "from_id"
        assert r.target_col == "to_id"


class TestLoadPipelineConfigWithInlineQuery:
    def test_inline_query_loaded(self) -> None:
        cfg = _load("with_inline_query.yaml")
        assert len(cfg.queries) == 1
        q = cfg.queries[0]
        assert q.id == "all_persons"
        assert q.inline is not None
        assert "MATCH" in q.inline
        assert q.source is None

    def test_description_set(self) -> None:
        cfg = _load("with_inline_query.yaml")
        assert cfg.queries[0].description == "Return all person names"


class TestLoadPipelineConfigWithExternalQuery:
    def test_external_query_loaded(self) -> None:
        cfg = _load("with_external_query.yaml")
        q = cfg.queries[0]
        assert q.source == "cypher/customer_summary.cypher"
        assert q.inline is None


class TestLoadPipelineConfigWithFunctions:
    def test_module_form(self) -> None:
        cfg = _load("with_functions_module.yaml")
        assert len(cfg.functions) == 1
        f = cfg.functions[0]
        assert f.module == "mypackage.string_utils"
        assert f.names == ["normalize_name", "parse_phone_number"]

    def test_callable_form(self) -> None:
        cfg = _load("with_functions_callable.yaml")
        assert len(cfg.functions) == 1
        f = cfg.functions[0]
        assert f.callable == "mypackage.formatters.format_currency"
        assert f.module is None

    def test_wildcard_register(self) -> None:
        cfg = _load("with_functions_wildcard.yaml")
        f = cfg.functions[0]
        assert f.names == "*"


class TestLoadPipelineConfigWithOutput:
    def test_output_count(self) -> None:
        cfg = _load("with_output.yaml")
        assert len(cfg.output) == 3

    def test_output_formats(self) -> None:
        cfg = _load("with_output.yaml")
        formats = {o.format for o in cfg.output}
        assert OutputFormat.CSV in formats
        assert OutputFormat.JSON in formats

    def test_output_without_explicit_format(self) -> None:
        cfg = _load("with_output.yaml")
        no_format = next(o for o in cfg.output if o.format is None)
        assert "parquet" in no_format.uri


class TestLoadPipelineConfigWithSchemaHints:
    def test_entity_schema_hints(self) -> None:
        cfg = _load("with_schema_hints.yaml")
        persons = cfg.sources.entities[0]
        assert persons.schema_hints is not None
        assert persons.schema_hints["zip_code"] == "VARCHAR"
        assert persons.schema_hints["age"] == "INTEGER"

    def test_relationship_schema_hints(self) -> None:
        cfg = _load("with_schema_hints.yaml")
        emp = cfg.sources.relationships[0]
        assert emp.schema_hints is not None
        assert emp.schema_hints["start_date"] == "DATE"


class TestLoadPipelineConfigWithOnError:
    def test_fail_policy(self) -> None:
        cfg = _load("with_on_error.yaml")
        required = next(e for e in cfg.sources.entities if e.id == "required_persons")
        assert required.on_error is ErrorHandlingPolicy.FAIL

    def test_skip_policy(self) -> None:
        cfg = _load("with_on_error.yaml")
        optional = next(
            e for e in cfg.sources.entities if e.id == "optional_enrichment"
        )
        assert optional.on_error is ErrorHandlingPolicy.SKIP

    def test_warn_policy(self) -> None:
        cfg = _load("with_on_error.yaml")
        advisory = next(e for e in cfg.sources.entities if e.id == "advisory_data")
        assert advisory.on_error is ErrorHandlingPolicy.WARN

    def test_relationship_warn_policy(self) -> None:
        cfg = _load("with_on_error.yaml")
        emp = cfg.sources.relationships[0]
        assert emp.on_error is ErrorHandlingPolicy.WARN


class TestLoadPipelineConfigWithEnvVars:
    def test_resolved_vars_substituted(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PIPELINE_NAME", "prod_pipeline")
        monkeypatch.setenv("ENVIRONMENT", "production")
        monkeypatch.setenv("DB_USER", "admin")
        monkeypatch.setenv("DB_PASSWORD", "s3cret")
        monkeypatch.setenv("DB_HOST", "db.example.com")
        monkeypatch.setenv("DB_NAME", "crm")
        monkeypatch.setenv("TENANT_ID", "tenant42")
        monkeypatch.setenv("S3_BUCKET", "my-bucket")
        monkeypatch.setenv("OUTPUT_BUCKET", "results")
        cfg = _load("with_env_vars.yaml")
        assert cfg.project is not None
        assert cfg.project.name == "prod_pipeline"
        entity = cfg.sources.entities[0]
        assert "admin" in entity.uri
        assert "s3cret" in entity.uri

    def test_unresolved_vars_left_as_placeholders(self) -> None:
        # Strip any vars that might be set
        for var in (
            "PIPELINE_NAME",
            "ENVIRONMENT",
            "DB_USER",
            "DB_PASSWORD",
            "DB_HOST",
            "DB_NAME",
            "TENANT_ID",
            "S3_BUCKET",
            "OUTPUT_BUCKET",
        ):
            os.environ.pop(var, None)
        cfg = _load("with_env_vars.yaml")
        # Unresolved vars remain as ${VAR} — the config still parses
        assert "${" in cfg.sources.entities[0].uri


class TestLoadPipelineConfigWithMultipleQueries:
    def test_query_count(self) -> None:
        cfg = _load("with_pipeline_order.yaml")
        assert len(cfg.queries) == 3

    def test_query_ids(self) -> None:
        cfg = _load("with_pipeline_order.yaml")
        ids = [q.id for q in cfg.queries]
        assert ids == ["enrich_persons", "enrich_companies", "combined_report"]


# ===========================================================================
# TestInvalidConfigs — expect ValidationError
# ===========================================================================


class TestInvalidConfigs:
    def test_query_both_sources(self) -> None:
        with pytest.raises(ValidationError, match="not both"):
            _load_invalid("query_both_sources.yaml")

    def test_query_no_source(self) -> None:
        with pytest.raises(ValidationError, match="either"):
            _load_invalid("query_no_source.yaml")

    def test_function_both(self) -> None:
        with pytest.raises(ValidationError, match="not both"):
            _load_invalid("function_both.yaml")

    def test_function_neither(self) -> None:
        with pytest.raises(ValidationError, match="either"):
            _load_invalid("function_neither.yaml")

    def test_function_module_no_names(self) -> None:
        with pytest.raises(ValidationError, match="'names' is required"):
            _load_invalid("function_module_no_register.yaml")


# ===========================================================================
# TestLoadPipelineConfigEdgeCases
# ===========================================================================


class TestLoadPipelineConfigEdgeCases:
    def test_nonexistent_file_raises(self) -> None:
        with pytest.raises(FileNotFoundError):
            load_pipeline_config("/nonexistent/path/config.yaml")

    def test_empty_config_parses_to_defaults(self, tmp_path: Path) -> None:
        """An empty YAML file (or just '{}') is valid — all fields have defaults."""
        empty = tmp_path / "empty.yaml"
        empty.write_text("{}", encoding="utf-8")
        cfg = load_pipeline_config(empty)
        assert cfg.version == "1.0"
        assert cfg.project is None
        assert cfg.sources.entities == []

    def test_version_string_preserved(self, tmp_path: Path) -> None:
        f = tmp_path / "v.yaml"
        f.write_text('version: "1.0"\n', encoding="utf-8")
        cfg = load_pipeline_config(f)
        assert cfg.version == "1.0"

    def test_unsupported_version_rejected_from_file(self, tmp_path: Path) -> None:
        f = tmp_path / "v2.yaml"
        f.write_text('version: "2.0"\n', encoding="utf-8")
        with pytest.raises(ValidationError, match="newer than this version"):
            load_pipeline_config(f)

    def test_sources_key_absent_gives_empty_sources(
        self,
        tmp_path: Path,
    ) -> None:
        f = tmp_path / "nosources.yaml"
        f.write_text("version: '1.0'\n", encoding="utf-8")
        cfg = load_pipeline_config(f)
        assert cfg.sources.entities == []
        assert cfg.sources.relationships == []


# ===========================================================================
# TestUriValidation — EntitySourceConfig, RelationshipSourceConfig, OutputConfig
# ===========================================================================


class TestEntitySourceUriValidation:
    """URI validation on EntitySourceConfig."""

    # --- valid URIs ---

    def test_bare_csv_path(self) -> None:
        cfg = EntitySourceConfig(id="s", uri="/data/f.csv", entity_type="X")
        assert cfg.uri == "/data/f.csv"

    def test_bare_parquet_path(self) -> None:
        cfg = EntitySourceConfig(
            id="s",
            uri="/data/f.parquet",
            entity_type="X",
        )
        assert cfg.uri == "/data/f.parquet"

    def test_bare_json_path(self) -> None:
        cfg = EntitySourceConfig(id="s", uri="/data/f.json", entity_type="X")
        assert cfg.uri == "/data/f.json"

    def test_file_uri_csv(self) -> None:
        cfg = EntitySourceConfig(
            id="s",
            uri="file:///data/f.csv",
            entity_type="X",
        )
        assert cfg.uri == "file:///data/f.csv"

    def test_s3_parquet(self) -> None:
        cfg = EntitySourceConfig(
            id="s",
            uri="s3://bucket/prefix/f.parquet",
            entity_type="X",
        )
        assert cfg.uri == "s3://bucket/prefix/f.parquet"

    def test_https_json(self) -> None:
        cfg = EntitySourceConfig(
            id="s",
            uri="https://example.com/data.json",
            entity_type="X",
        )
        assert cfg.uri == "https://example.com/data.json"

    def test_sql_uri_with_query(self) -> None:
        cfg = EntitySourceConfig(
            id="s",
            uri="postgresql://user:pass@host/db",
            entity_type="X",
            query="SELECT * FROM t",
        )
        assert cfg.uri == "postgresql://user:pass@host/db"

    def test_postgres_alias_with_query(self) -> None:
        cfg = EntitySourceConfig(
            id="s",
            uri="postgres://host/db",
            entity_type="X",
            query="SELECT 1",
        )
        assert cfg.uri == "postgres://host/db"

    def test_duckdb_uri_with_query(self) -> None:
        cfg = EntitySourceConfig(
            id="s",
            uri="duckdb:///path/to/db.duckdb",
            entity_type="X",
            query="SELECT * FROM t",
        )
        assert cfg.uri == "duckdb:///path/to/db.duckdb"

    # --- invalid URIs ---

    def test_empty_uri_raises(self) -> None:
        with pytest.raises(ValidationError, match="must not be empty"):
            EntitySourceConfig(id="s", uri="", entity_type="X")

    def test_whitespace_uri_raises(self) -> None:
        with pytest.raises(ValidationError, match="must not be empty"):
            EntitySourceConfig(id="s", uri="   ", entity_type="X")

    def test_unsupported_extension_raises(self) -> None:
        with pytest.raises(
            ValidationError,
            match="unrecognised file extension",
        ):
            EntitySourceConfig(id="s", uri="/data/f.xlsx", entity_type="X")

    def test_no_extension_raises(self) -> None:
        with pytest.raises(
            ValidationError,
            match="unrecognised file extension",
        ):
            EntitySourceConfig(id="s", uri="/data/file", entity_type="X")

    def test_sql_without_query_raises(self) -> None:
        with pytest.raises(ValidationError, match="SQL scheme"):
            EntitySourceConfig(
                id="s",
                uri="postgresql://host/db",
                entity_type="X",
            )

    def test_mysql_without_query_raises(self) -> None:
        with pytest.raises(ValidationError, match="SQL scheme"):
            EntitySourceConfig(id="s", uri="mysql://host/db", entity_type="X")

    def test_sqlite_without_query_raises(self) -> None:
        with pytest.raises(ValidationError, match="SQL scheme"):
            EntitySourceConfig(
                id="s",
                uri="sqlite:///path/to/db.sqlite",
                entity_type="X",
            )


class TestRelationshipSourceUriValidation:
    """URI validation on RelationshipSourceConfig."""

    _REQUIRED = {
        "id": "r",
        "uri": "PLACEHOLDER",
        "relationship_type": "REL",
        "source_col": "src",
        "target_col": "tgt",
    }

    def _cfg(
        self,
        uri: str,
        query: str | None = None,
    ) -> RelationshipSourceConfig:
        return RelationshipSourceConfig(
            **{
                **self._REQUIRED,
                "uri": uri,
                **({"query": query} if query else {}),
            },
        )

    def test_valid_csv(self) -> None:
        cfg = self._cfg("/data/edges.csv")
        assert cfg.uri == "/data/edges.csv"

    def test_valid_parquet_s3(self) -> None:
        cfg = self._cfg("s3://bucket/edges.parquet")
        assert cfg.uri == "s3://bucket/edges.parquet"

    def test_valid_sql_with_query(self) -> None:
        cfg = self._cfg("postgresql://host/db", query="SELECT src, tgt FROM t")
        assert cfg.uri == "postgresql://host/db"

    def test_empty_uri_raises(self) -> None:
        with pytest.raises(ValidationError, match="must not be empty"):
            self._cfg("")

    def test_bad_extension_raises(self) -> None:
        with pytest.raises(
            ValidationError,
            match="unrecognised file extension",
        ):
            self._cfg("/data/edges.tsv")

    def test_sql_without_query_raises(self) -> None:
        with pytest.raises(ValidationError, match="SQL scheme"):
            self._cfg("postgresql://host/db")


class TestOutputUriValidation:
    """URI validation on OutputConfig."""

    def test_valid_csv_output(self) -> None:
        cfg = OutputConfig(query_id="q", uri="/out/result.csv")
        assert cfg.uri == "/out/result.csv"

    def test_valid_parquet_output(self) -> None:
        cfg = OutputConfig(query_id="q", uri="s3://bucket/result.parquet")
        assert cfg.uri == "s3://bucket/result.parquet"

    def test_valid_json_output(self) -> None:
        cfg = OutputConfig(query_id="q", uri="file:///out/result.json")
        assert cfg.uri == "file:///out/result.json"

    def test_empty_uri_raises(self) -> None:
        with pytest.raises(ValidationError, match="must not be empty"):
            OutputConfig(query_id="q", uri="")

    def test_bad_extension_raises(self) -> None:
        with pytest.raises(
            ValidationError,
            match="unrecognised file extension",
        ):
            OutputConfig(query_id="q", uri="/out/result.xlsx")

    def test_no_extension_raises(self) -> None:
        with pytest.raises(
            ValidationError,
            match="unrecognised file extension",
        ):
            OutputConfig(query_id="q", uri="/out/result")

    def test_sql_scheme_rejected_for_output(self) -> None:
        """SQL-scheme URIs are not valid output sinks."""
        with pytest.raises(
            ValidationError,
            match="unrecognised file extension",
        ):
            OutputConfig(query_id="q", uri="postgresql://host/db")


# ===========================================================================
# TestSourceIdUniqueness
# ===========================================================================


class TestSourceIdUniqueness:
    def test_unique_ids_accepted(self) -> None:
        cfg = SourcesConfig(
            entities=[
                EntitySourceConfig(id="a", uri="/f.csv", entity_type="A"),
                EntitySourceConfig(id="b", uri="/g.csv", entity_type="B"),
            ],
        )
        assert len(cfg.entities) == 2

    def test_duplicate_entity_ids_raises(self) -> None:
        with pytest.raises(ValidationError, match="Duplicate source id"):
            SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="same",
                        uri="/f.csv",
                        entity_type="A",
                    ),
                    EntitySourceConfig(
                        id="same",
                        uri="/g.csv",
                        entity_type="B",
                    ),
                ],
            )

    def test_duplicate_relationship_ids_raises(self) -> None:
        with pytest.raises(ValidationError, match="Duplicate source id"):
            SourcesConfig(
                relationships=[
                    RelationshipSourceConfig(
                        id="same",
                        uri="/e.csv",
                        relationship_type="R",
                        source_col="s",
                        target_col="t",
                    ),
                    RelationshipSourceConfig(
                        id="same",
                        uri="/f.csv",
                        relationship_type="S",
                        source_col="s",
                        target_col="t",
                    ),
                ],
            )

    def test_duplicate_across_entity_and_relationship_raises(self) -> None:
        with pytest.raises(ValidationError, match="Duplicate source id"):
            SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="shared",
                        uri="/f.csv",
                        entity_type="A",
                    ),
                ],
                relationships=[
                    RelationshipSourceConfig(
                        id="shared",
                        uri="/e.csv",
                        relationship_type="R",
                        source_col="s",
                        target_col="t",
                    ),
                ],
            )

    def test_error_message_names_duplicate(self) -> None:
        with pytest.raises(ValidationError, match="'persons'"):
            SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="persons",
                        uri="/f.csv",
                        entity_type="A",
                    ),
                    EntitySourceConfig(
                        id="persons",
                        uri="/g.csv",
                        entity_type="B",
                    ),
                ],
            )

    def test_fixture_duplicate_entity_ids(self) -> None:
        with pytest.raises(ValidationError, match="Duplicate source id"):
            _load_invalid("duplicate_source_ids.yaml")

    def test_fixture_duplicate_cross_list_ids(self) -> None:
        with pytest.raises(ValidationError, match="Duplicate source id"):
            _load_invalid("duplicate_source_ids_cross.yaml")

    def test_single_source_always_valid(self) -> None:
        cfg = SourcesConfig(
            entities=[
                EntitySourceConfig(id="only", uri="/f.csv", entity_type="X"),
            ],
        )
        assert cfg.entities[0].id == "only"

    def test_empty_sources_valid(self) -> None:
        cfg = SourcesConfig()
        assert cfg.entities == []
        assert cfg.relationships == []


# ===========================================================================
# TestLoadWithSources
# ===========================================================================

FIXTURES_DATA = Path(__file__).parent / "fixtures" / "data"


class TestLoadWithSources:
    """Tests for PipelineConfig.load_with_sources()."""

    def _write_yaml(self, tmp_path: Path, content: str) -> Path:
        p = tmp_path / "config.yaml"
        p.write_text(content, encoding="utf-8")
        return p

    def test_returns_tuple_of_config_and_dict(self, tmp_path: Path) -> None:
        yaml_content = f"""\
sources:
  entities:
    - id: persons
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        result = PipelineConfig.load_with_sources(cfg_path)
        assert isinstance(result, tuple)
        assert len(result) == 3
        config, sources, skipped = result
        assert isinstance(config, PipelineConfig)
        assert isinstance(sources, dict)

    def test_entity_source_ids_as_keys(self, tmp_path: Path) -> None:
        yaml_content = f"""\
sources:
  entities:
    - id: persons
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
    - id: companies
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Company
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        _, sources, _ = PipelineConfig.load_with_sources(cfg_path)
        assert "persons" in sources
        assert "companies" in sources

    def test_relationship_source_ids_as_keys(self, tmp_path: Path) -> None:
        yaml_content = f"""\
sources:
  relationships:
    - id: employment
      uri: "{FIXTURES_DATA / "sample.csv"}"
      relationship_type: WORKS_FOR
      source_col: id
      target_col: id
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        _, sources, _ = PipelineConfig.load_with_sources(cfg_path)
        assert "employment" in sources

    def test_sources_are_arrow_tables(self, tmp_path: Path) -> None:
        yaml_content = f"""\
sources:
  entities:
    - id: persons
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        _, sources, _ = PipelineConfig.load_with_sources(cfg_path)
        assert isinstance(sources["persons"], pa.Table)

    def test_arrow_table_has_correct_row_count(self, tmp_path: Path) -> None:
        yaml_content = f"""\
sources:
  entities:
    - id: persons
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        _, sources, _ = PipelineConfig.load_with_sources(cfg_path)
        assert sources["persons"].num_rows == 2

    def test_empty_sources_returns_empty_dict(self, tmp_path: Path) -> None:
        yaml_content = "sources: {}\n"
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        config, sources, _ = PipelineConfig.load_with_sources(cfg_path)
        assert isinstance(config, PipelineConfig)
        assert sources == {}

    def test_nonexistent_path_raises_file_not_found(self) -> None:
        with pytest.raises(FileNotFoundError):
            PipelineConfig.load_with_sources("/nonexistent/path/config.yaml")

    def test_read_called_once_per_source(self, tmp_path: Path) -> None:
        preset = pa.table({"id": [1], "name": ["Alice"]})
        mock_ds = MagicMock()
        mock_ds.read.return_value = preset

        yaml_content = f"""\
sources:
  entities:
    - id: persons
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
    - id: companies
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Company
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        with patch(
            "pycypher.ingestion.data_sources.data_source_from_uri",
            return_value=mock_ds,
        ):
            _, sources, _ = PipelineConfig.load_with_sources(cfg_path)

        assert mock_ds.read.call_count == 2
        assert sources["persons"] is preset
        assert sources["companies"] is preset

    def test_correct_uri_passed_to_factory(self, tmp_path: Path) -> None:
        preset = pa.table({"id": [1]})
        calls: list[tuple] = []

        def fake_factory(uri: str, **kwargs: object) -> MagicMock:
            calls.append((uri, kwargs))
            m = MagicMock()
            m.read.return_value = preset
            return m

        yaml_content = f"""\
sources:
  entities:
    - id: persons
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        with patch(
            "pycypher.ingestion.data_sources.data_source_from_uri",
            side_effect=fake_factory,
        ):
            PipelineConfig.load_with_sources(cfg_path)

        assert len(calls) == 1
        assert str(FIXTURES_DATA / "sample.csv") in calls[0][0]


# ---------------------------------------------------------------------------
# load_with_sources() error-handling policy
# ---------------------------------------------------------------------------


class TestLoadWithSourcesErrorPolicy:
    """on_error policy is honoured by load_with_sources().

    - FAIL (default): exception from ds.read() propagates immediately.
    - WARN: source is omitted from the result dict; no exception raised.
    - SKIP: same as WARN but truly silent (no log side-effects tested here).
    """

    def _write_yaml(self, tmp_path: Path, content: str) -> Path:
        p = tmp_path / "config.yaml"
        p.write_text(content, encoding="utf-8")
        return p

    def _make_failing_factory(self, exc: Exception):
        """Return a factory function whose .read() always raises *exc*."""

        def factory(uri: str, **kwargs: object):
            m = MagicMock()
            m.read.side_effect = exc
            return m

        return factory

    # -- FAIL policy ---------------------------------------------------------

    def test_fail_policy_propagates_exception(self, tmp_path: Path) -> None:
        """on_error: fail — exception from read() propagates immediately."""
        yaml_content = f"""\
sources:
  entities:
    - id: broken
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
      on_error: fail
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        boom = RuntimeError("disk error")
        with (
            patch(
                "pycypher.ingestion.data_sources.data_source_from_uri",
                side_effect=self._make_failing_factory(boom),
            ),
            pytest.raises(RuntimeError, match="disk error"),
        ):
            PipelineConfig.load_with_sources(cfg_path)

    def test_default_policy_propagates_exception(self, tmp_path: Path) -> None:
        """No on_error field → default is FAIL; exception must propagate."""
        yaml_content = f"""\
sources:
  entities:
    - id: broken
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        boom = RuntimeError("default fail")
        with (
            patch(
                "pycypher.ingestion.data_sources.data_source_from_uri",
                side_effect=self._make_failing_factory(boom),
            ),
            pytest.raises(RuntimeError, match="default fail"),
        ):
            PipelineConfig.load_with_sources(cfg_path)

    # -- SKIP policy ---------------------------------------------------------

    def test_skip_policy_omits_failed_source(self, tmp_path: Path) -> None:
        """on_error: skip — failed source absent from result dict."""
        yaml_content = f"""\
sources:
  entities:
    - id: broken
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
      on_error: skip
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        with patch(
            "pycypher.ingestion.data_sources.data_source_from_uri",
            side_effect=self._make_failing_factory(RuntimeError("oops")),
        ):
            _, sources, _ = PipelineConfig.load_with_sources(cfg_path)
        assert "broken" not in sources

    def test_skip_policy_other_sources_still_loaded(
        self,
        tmp_path: Path,
    ) -> None:
        """on_error: skip on one source does not prevent others from loading."""
        good_table = pa.table({"id": [1], "name": ["Alice"]})

        call_count = [0]

        def smart_factory(uri: str, **kwargs: object):
            call_count[0] += 1
            m = MagicMock()
            if "broken" in uri or call_count[0] == 1:
                # First call is the broken source
                m.read.side_effect = RuntimeError("oops")
            else:
                m.read.return_value = good_table
            return m

        yaml_content = f"""\
sources:
  entities:
    - id: broken
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
      on_error: skip
    - id: good
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Company
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        with patch(
            "pycypher.ingestion.data_sources.data_source_from_uri",
            side_effect=smart_factory,
        ):
            _, sources, _ = PipelineConfig.load_with_sources(cfg_path)
        assert "broken" not in sources
        assert "good" in sources

    def test_skip_policy_does_not_raise(self, tmp_path: Path) -> None:
        """on_error: skip — no exception escapes load_with_sources()."""
        yaml_content = f"""\
sources:
  entities:
    - id: broken
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
      on_error: skip
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        with patch(
            "pycypher.ingestion.data_sources.data_source_from_uri",
            side_effect=self._make_failing_factory(ValueError("bad uri")),
        ):
            config, sources, _ = PipelineConfig.load_with_sources(
                cfg_path,
            )  # must not raise
        assert isinstance(config, PipelineConfig)
        assert isinstance(sources, dict)

    # -- WARN policy ---------------------------------------------------------

    def test_warn_policy_omits_failed_source(self, tmp_path: Path) -> None:
        """on_error: warn — failed source absent from result dict."""
        yaml_content = f"""\
sources:
  entities:
    - id: broken
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
      on_error: warn
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        with patch(
            "pycypher.ingestion.data_sources.data_source_from_uri",
            side_effect=self._make_failing_factory(RuntimeError("warn me")),
        ):
            _, sources, _ = PipelineConfig.load_with_sources(cfg_path)
        assert "broken" not in sources

    def test_warn_policy_does_not_raise(self, tmp_path: Path) -> None:
        """on_error: warn — no exception escapes load_with_sources()."""
        yaml_content = f"""\
sources:
  entities:
    - id: broken
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
      on_error: warn
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        with patch(
            "pycypher.ingestion.data_sources.data_source_from_uri",
            side_effect=self._make_failing_factory(RuntimeError("warn me")),
        ):
            config, sources, _ = PipelineConfig.load_with_sources(cfg_path)
        assert isinstance(config, PipelineConfig)

    def test_warn_policy_emits_log_warning(self, tmp_path: Path) -> None:
        """on_error: warn — a warning is logged with the source id."""
        yaml_content = f"""\
sources:
  entities:
    - id: my_source
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
      on_error: warn
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        with (
            patch(
                "pycypher.ingestion.data_sources.data_source_from_uri",
                side_effect=self._make_failing_factory(RuntimeError("warn me")),
            ),
            self._assert_log_warning("my_source"),
        ):
            PipelineConfig.load_with_sources(cfg_path)

    @staticmethod
    def _assert_log_warning(source_id: str):
        """Context manager: assert a warning mentioning *source_id* is logged."""
        import logging

        class _Catcher(logging.Handler):
            def __init__(self):
                super().__init__()
                self.records: list[logging.LogRecord] = []

            def emit(self, record):
                self.records.append(record)

        handler = _Catcher()
        handler.setLevel(logging.WARNING)
        logger = logging.getLogger("shared.logger")
        logger.addHandler(handler)

        class _CM:
            def __enter__(self):
                return handler

            def __exit__(self, *_):
                logger.removeHandler(handler)
                found = any(
                    source_id in r.getMessage()
                    for r in handler.records
                    if r.levelno >= logging.WARNING
                )
                if not found:
                    raise AssertionError(
                        f"No WARNING log containing '{source_id}' was emitted. "
                        f"Records: {[r.getMessage() for r in handler.records]}",
                    )

        return _CM()

    # -- SecurityError always propagates regardless of policy ----------------

    def test_security_error_propagates_despite_skip_policy(
        self,
        tmp_path: Path,
    ) -> None:
        """SecurityError must always propagate, even with on_error: skip."""
        from pycypher.exceptions import SecurityError

        yaml_content = f"""\
sources:
  entities:
    - id: malicious
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
      on_error: skip
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        with (
            patch(
                "pycypher.ingestion.data_sources.data_source_from_uri",
                side_effect=self._make_failing_factory(
                    SecurityError("path traversal detected"),
                ),
            ),
            pytest.raises(SecurityError, match="path traversal"),
        ):
            PipelineConfig.load_with_sources(cfg_path)

    def test_security_error_propagates_despite_warn_policy(
        self,
        tmp_path: Path,
    ) -> None:
        """SecurityError must always propagate, even with on_error: warn."""
        from pycypher.exceptions import SecurityError

        yaml_content = f"""\
sources:
  entities:
    - id: malicious
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
      on_error: warn
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        with (
            patch(
                "pycypher.ingestion.data_sources.data_source_from_uri",
                side_effect=self._make_failing_factory(
                    SecurityError("SQL injection attempt"),
                ),
            ),
            pytest.raises(SecurityError, match="SQL injection"),
        ):
            PipelineConfig.load_with_sources(cfg_path)

    # -- Audit trail (skipped list) ------------------------------------------

    def test_skip_policy_populates_skipped_audit_trail(
        self,
        tmp_path: Path,
    ) -> None:
        """on_error: skip — skipped source appears in audit trail."""
        from pycypher.ingestion.config import SkippedSource

        yaml_content = f"""\
sources:
  entities:
    - id: broken
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
      on_error: skip
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        with patch(
            "pycypher.ingestion.data_sources.data_source_from_uri",
            side_effect=self._make_failing_factory(RuntimeError("disk full")),
        ):
            _, sources, skipped = PipelineConfig.load_with_sources(cfg_path)
        assert "broken" not in sources
        assert len(skipped) == 1
        assert isinstance(skipped[0], SkippedSource)
        assert skipped[0].source_id == "broken"
        assert isinstance(skipped[0].error, RuntimeError)
        assert "disk full" in str(skipped[0].error)
        assert skipped[0].policy == ErrorHandlingPolicy.SKIP

    def test_warn_policy_populates_skipped_audit_trail(
        self,
        tmp_path: Path,
    ) -> None:
        """on_error: warn — warned source appears in audit trail."""
        yaml_content = f"""\
sources:
  entities:
    - id: flaky
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
      on_error: warn
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        with patch(
            "pycypher.ingestion.data_sources.data_source_from_uri",
            side_effect=self._make_failing_factory(RuntimeError("timeout")),
        ):
            _, _, skipped = PipelineConfig.load_with_sources(cfg_path)
        assert len(skipped) == 1
        assert skipped[0].source_id == "flaky"
        assert skipped[0].policy == ErrorHandlingPolicy.WARN

    def test_fail_policy_does_not_populate_skipped_audit_trail(
        self,
        tmp_path: Path,
    ) -> None:
        """on_error: fail — no audit trail entry (exception propagates)."""
        yaml_content = f"""\
sources:
  entities:
    - id: critical
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
      on_error: fail
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        with (
            patch(
                "pycypher.ingestion.data_sources.data_source_from_uri",
                side_effect=self._make_failing_factory(RuntimeError("fatal")),
            ),
            pytest.raises(RuntimeError, match="fatal"),
        ):
            PipelineConfig.load_with_sources(cfg_path)

    def test_successful_load_returns_empty_skipped_list(
        self,
        tmp_path: Path,
    ) -> None:
        """All sources succeed — skipped list is empty."""
        good_table = pa.table({"id": [1]})

        def factory(uri: str, **kwargs: object):
            m = MagicMock()
            m.read.return_value = good_table
            return m

        yaml_content = f"""\
sources:
  entities:
    - id: good
      uri: "{FIXTURES_DATA / "sample.csv"}"
      entity_type: Person
"""
        cfg_path = self._write_yaml(tmp_path, yaml_content)
        with patch(
            "pycypher.ingestion.data_sources.data_source_from_uri",
            side_effect=factory,
        ):
            _, _, skipped = PipelineConfig.load_with_sources(cfg_path)
        assert skipped == []


# ---------------------------------------------------------------------------
# SourcesConfig.get_source_by_id / PipelineConfig.get_source_by_id
# ---------------------------------------------------------------------------


class TestGetSourceById:
    """SourcesConfig.get_source_by_id() and the PipelineConfig passthrough."""

    @pytest.fixture
    def sources(self) -> SourcesConfig:
        return SourcesConfig(
            entities=[
                EntitySourceConfig(
                    id="persons",
                    uri="s3://b/p.csv",
                    entity_type="Person",
                ),
                EntitySourceConfig(
                    id="companies",
                    uri="s3://b/c.parquet",
                    entity_type="Company",
                ),
            ],
            relationships=[
                RelationshipSourceConfig(
                    id="knows",
                    uri="s3://b/k.csv",
                    relationship_type="KNOWS",
                    source_entity_type="Person",
                    target_entity_type="Person",
                    source_col="src_id",
                    target_col="tgt_id",
                ),
            ],
        )

    def test_find_entity_source_by_id(self, sources: SourcesConfig) -> None:
        """get_source_by_id returns the correct EntitySourceConfig."""
        result = sources.get_source_by_id("persons")
        assert result is not None
        assert isinstance(result, EntitySourceConfig)
        assert result.id == "persons"

    def test_find_relationship_source_by_id(
        self,
        sources: SourcesConfig,
    ) -> None:
        """get_source_by_id returns the correct RelationshipSourceConfig."""
        result = sources.get_source_by_id("knows")
        assert result is not None
        assert isinstance(result, RelationshipSourceConfig)
        assert result.id == "knows"

    def test_missing_id_returns_none(self, sources: SourcesConfig) -> None:
        """get_source_by_id returns None when the id does not exist."""
        assert sources.get_source_by_id("nonexistent") is None

    def test_pipeline_config_passthrough(self, sources: SourcesConfig) -> None:
        """PipelineConfig.get_source_by_id() delegates to sources."""
        config = PipelineConfig(sources=sources)
        result = config.get_source_by_id("companies")
        assert result is not None
        assert result.id == "companies"

    def test_pipeline_config_missing_returns_none(
        self,
        sources: SourcesConfig,
    ) -> None:
        """PipelineConfig.get_source_by_id() returns None for missing id."""
        config = PipelineConfig(sources=sources)
        assert config.get_source_by_id("missing") is None

    def test_second_entity_found(self, sources: SourcesConfig) -> None:
        """Second entity source is correctly retrieved."""
        result = sources.get_source_by_id("companies")
        assert result is not None
        assert result.entity_type == "Company"  # type: ignore[union-attr]

    def test_empty_sources_returns_none(self) -> None:
        """Empty SourcesConfig always returns None."""
        empty = SourcesConfig()
        assert empty.get_source_by_id("anything") is None
