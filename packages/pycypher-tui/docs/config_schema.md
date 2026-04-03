# Pipeline Configuration Schema

YAML configuration format for PyCypher ETL pipelines managed by the TUI.

**Schema version:** `1.0`
**Defined in:** `pycypher.ingestion.config.PipelineConfig` (Pydantic BaseModel)

## Example

```yaml
version: "1.0"

project:
  name: my-pipeline
  description: Load people and companies, find who works where

sources:
  entities:
    - id: people
      uri: data/people.csv
      entity_type: Person
      id_col: person_id
      schema_hints:
        age: INTEGER
        zip_code: VARCHAR

    - id: companies
      uri: data/companies.parquet
      entity_type: Company

  relationships:
    - id: employment
      uri: data/works_at.csv
      relationship_type: WORKS_FOR
      source_col: person_id
      target_col: company_id

queries:
  - id: find_employees
    description: Find all employees at each company
    inline: |
      MATCH (p:Person)-[:WORKS_FOR]->(c:Company)
      RETURN p.name, c.name

  - id: complex_query
    source: queries/complex.cypher

output:
  - query_id: find_employees
    uri: output/employees.parquet
```

## Top-Level Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `version` | `str` | No | `"1.0"` | Config schema version. Must be `"1.0"`. |
| `project` | object | No | `null` | Project metadata |
| `sources` | object | No | empty | Entity and relationship data sources |
| `queries` | list | No | `[]` | Named Cypher queries |
| `output` | list | No | `[]` | Output sink configurations |

## `project`

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | `str` | **Yes** | Short identifier (used in TUI header, logs) |
| `description` | `str` | No | Longer description of the pipeline |

## `sources.entities[]`

Each entry defines a node data source.

| Field | Type | Required | Description |
|---|---|---|---|
| `id` | `str` | **Yes** | Unique source identifier (shared namespace with relationships) |
| `uri` | `str` | **Yes** | Data source URI (file path, URL, or connection string) |
| `entity_type` | `str` | **Yes** | Cypher node label assigned to rows (e.g., `Person`) |
| `id_col` | `str` | No | Column to use as `__ID__`. Auto-generated if absent. |
| `query` | `str` | No | DuckDB SQL applied after loading. Data available as `source` table. |
| `schema_hints` | `dict[str, str]` | No | Column-name → type overrides (e.g., `{"zip": "VARCHAR"}`) |
| `on_error` | `str` | No | Error policy: `"fail"`, `"skip"`, or `"warn"`. Defaults to pipeline-level. |

## `sources.relationships[]`

Each entry defines an edge data source.

| Field | Type | Required | Description |
|---|---|---|---|
| `id` | `str` | **Yes** | Unique source identifier (shared namespace with entities) |
| `uri` | `str` | **Yes** | Data source URI |
| `relationship_type` | `str` | **Yes** | Cypher relationship type label (e.g., `WORKS_FOR`) |
| `source_col` | `str` | **Yes** | Column for source node IDs (mapped to `__SOURCE__`) |
| `target_col` | `str` | **Yes** | Column for target node IDs (mapped to `__TARGET__`) |
| `id_col` | `str` | No | Column for relationship `__ID__`. Auto-generated if absent. |
| `query` | `str` | No | DuckDB SQL applied after loading |
| `schema_hints` | `dict[str, str]` | No | Column-name → type overrides |
| `on_error` | `str` | No | Error policy |

## `queries[]`

Each entry defines a named Cypher query. Exactly one of `source` or `inline` must be provided.

| Field | Type | Required | Description |
|---|---|---|---|
| `id` | `str` | **Yes** | Unique query identifier |
| `description` | `str` | No | Human-readable description |
| `source` | `str` | One of | Path to a `.cypher` file (relative to config directory) |
| `inline` | `str` | One of | Literal Cypher query text |

**Validation:** Providing both `source` and `inline` or neither is an error.

## `output[]`

Each entry maps a query result to an output destination.

| Field | Type | Required | Description |
|---|---|---|---|
| `query_id` | `str` | **Yes** | The `id` of the query whose result to write |
| `uri` | `str` | **Yes** | Destination URI (format inferred from extension) |
| `format` | `str` | No | Explicit output format (e.g., `"parquet"`, `"csv"`) |

## Validation Rules

The TUI validates configs via `ConfigManager.validate()` → `ValidationResult`:

1. **Source IDs must be unique** across both entities and relationships (shared namespace)
2. **Query must have exactly one of** `source` or `inline`
3. **URIs are validated** for supported schemes and syntax
4. **Config version** must be `"1.0"` (the only supported version)

Validation results are shown:
- In the **PipelineOverviewScreen** bottom bar (green check / red error count / amber warning count)
- In **PipelineTestingScreen** dry run diagnostics (per-step validation)
- Via `:w` save (validates before writing)

## TUI CRUD Operations

The TUI modifies this config through `ConfigManager` methods:

| Operation | Method | TUI Trigger |
|---|---|---|
| Add entity source | `add_entity_source(id, uri, entity_type, ...)` | `a` on DataSources/EntityTables screen |
| Update entity source | `update_entity_source(id, **kwargs)` | Edit dialog |
| Remove entity source | `remove_entity_source(id)` | `dd` with confirmation |
| Add relationship source | `add_relationship_source(id, uri, type, src, tgt, ...)` | `a` on Relationships screen |
| Update relationship source | `update_relationship_source(id, **kwargs)` | Edit dialog |
| Remove relationship source | `remove_relationship_source(id)` | `dd` with confirmation |
| Add query | `add_query(id, inline=..., source=...)` | Ctrl+S in QueryEditor |
| Remove query | `remove_query(id)` | `dd` with confirmation |
| Add output | `add_output(query_id, uri, format=...)` | `a` on Outputs section |
| Remove output | `remove_output(query_id, uri)` | `dd` with confirmation |

All mutations support **undo/redo** (`u` / `Ctrl+R` or `:u` / `:redo`).

## Save Behavior

`:w` performs an atomic save:
1. Backs up existing file to `<name>.yaml.bak`
2. Writes to a temp file in the same directory
3. Atomic rename over the target (POSIX `os.replace()`)
4. Marks config as clean (disables "unsaved changes" prompt on `:q`)
