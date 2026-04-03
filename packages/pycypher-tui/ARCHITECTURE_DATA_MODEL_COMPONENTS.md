# TUI Data Model Component Architecture

## 1. Component Hierarchy

### Overview

The data model visualization layer extends the existing `VimNavigableScreen[T]` pattern
with three new component categories: **Browsers** (read-only exploration), **Editors**
(configuration mutation), and **Mappers** (visual relationship rendering).

```
PyCypherTUI (app.py)
  └── Screens
       ├── PipelineOverviewScreen          [existing - entry point]
       │
       ├── Entity Browsing
       │   ├── EntityBrowserScreen          [new - unified entity explorer]
       │   │   ├── EntityListPanel           (left: entity types + counts)
       │   │   └── EntityDetailPanel         (right: properties, sources, indexes)
       │   └── EntityEditorScreen           [new - entity source editing]
       │       ├── SourceFieldEditor         (URI, ID col, schema hints)
       │       └── SchemaPreviewWidget       (live column introspection)
       │
       ├── Relationship Browsing
       │   ├── RelationshipBrowserScreen    [new - relationship explorer]
       │   │   ├── RelationshipListPanel     (left: rel types + endpoints)
       │   │   └── RelationshipDetailPanel   (right: columns, mappings, validation)
       │   └── RelationshipEditorScreen     [new - relationship source editing]
       │       ├── EndpointSelector          (source/target entity picker)
       │       └── ColumnMappingWidget       [existing - reused]
       │
       ├── Data Source Mapping
       │   ├── DataSourceMapperScreen       [new - source-to-model mapping]
       │   │   ├── SourceListPanel           (left: all sources by type)
       │   │   └── MappingVisualization      (right: source → entity/rel mapping)
       │   └── DataSourceInspectorScreen    [new - schema introspection]
       │       ├── SchemaTreeWidget          (column hierarchy)
       │       └── DataPreviewDialog         [existing - reused]
       │
       └── Model Visualization
            └── DataModelScreen              [existing - enhanced]
                ├── GraphTopologyWidget      [new - entity-relationship graph]
                ├── ModelStatisticsPanel      [new - cardinality/index stats]
                └── SchemaEvolutionWidget    [new - schema change tracking]
```

### Base Classes

All new screens extend `VimNavigableScreen[T]` to inherit:
- VIM `j/k` cursor navigation
- `/pattern` search and highlighting
- Detail panel updates on cursor move
- Consistent key bindings across all browsers

New editors extend a new `VimEditableScreen[T]` base (extends `VimNavigableScreen[T]`)
adding:
- INSERT mode field editing with validation
- Dirty-state tracking via ConfigManager
- Undo/redo integration
- Save confirmation on exit

---

## 2. Data Flow Patterns

### Read Path (Browsing)

```
PipelineConfig ──┐
                 ├──→ DataModelAdapter ──→ Screen.load_items() ──→ UI
GraphIndexManager┘         │
                           ├── .entity_types()      → list[EntityViewModel]
                           ├── .relationship_types() → list[RelationshipViewModel]
                           ├── .source_mappings()    → list[SourceMappingViewModel]
                           └── .model_statistics()   → ModelStatsViewModel
```

**DataModelAdapter** is a facade that joins PipelineConfig (static definitions)
with GraphIndexManager (runtime indexes) to produce view models. Screens never
access pycypher internals directly.

### Write Path (Editing)

```
User Input ──→ Screen._on_edit() ──→ ConfigManager.update_*()
                                          │
                                          ├── Validates via CachedValidator
                                          ├── Updates PipelineConfig
                                          ├── Pushes to undo stack
                                          └── Posts ConfigChanged message
                                                │
                                                └── All open screens refresh
```

All mutations go through ConfigManager (existing). The adapter layer refreshes
its cache on `ConfigChanged` messages from the Textual message bus.

### Async Introspection Path

```
DataSourceInspectorScreen
    │
    ├── worker: DataSourceIntrospector.introspect(uri)
    │       └── returns: IntrospectionResult(columns, types, sample_rows, stats)
    │
    └── on_worker_complete:
            └── SchemaTreeWidget.update(result)
```

Heavy I/O (file reads, DB connections) runs in Textual workers to keep the UI
responsive. Results are cached by URI in the adapter.

---

## 3. Interface Definitions

### DataModelAdapter (new: `adapters/data_model.py`)

Central adapter between pycypher data structures and TUI view models.

```python
class DataModelAdapter:
    """Facade joining PipelineConfig + GraphIndexManager for TUI consumption."""

    def __init__(self, config_manager: ConfigManager,
                 index_manager: GraphIndexManager | None = None) -> None: ...

    # Entity access
    def entity_types(self) -> list[EntityViewModel]: ...
    def entity_detail(self, entity_type: str) -> EntityDetailViewModel: ...

    # Relationship access
    def relationship_types(self) -> list[RelationshipViewModel]: ...
    def relationship_detail(self, rel_type: str) -> RelationshipDetailViewModel: ...

    # Source mapping
    def source_mappings(self) -> list[SourceMappingViewModel]: ...
    def unmapped_sources(self) -> list[SourceViewModel]: ...

    # Statistics (requires GraphIndexManager)
    def model_statistics(self) -> ModelStatsViewModel: ...

    # Cache management
    def refresh(self) -> None: ...
```

### View Models (new: `adapters/view_models.py`)

Immutable dataclasses consumed by screens. Decoupled from pycypher internals.

```python
@dataclass(frozen=True)
class EntityViewModel:
    entity_type: str
    source_count: int
    property_names: tuple[str, ...]
    id_column: str
    has_index: bool
    row_count: int | None  # None if index unavailable

@dataclass(frozen=True)
class EntityDetailViewModel:
    entity_type: str
    sources: tuple[EntitySourceViewModel, ...]
    properties: tuple[PropertyViewModel, ...]
    index_stats: IndexStatsViewModel | None
    validation_issues: tuple[ValidationIssue, ...]

@dataclass(frozen=True)
class RelationshipViewModel:
    relationship_type: str
    source_entity: str
    target_entity: str
    source_count: int
    column_mappings: tuple[ColumnMappingViewModel, ...]
    validation_status: str  # "valid" | "warning" | "error"

@dataclass(frozen=True)
class RelationshipDetailViewModel:
    relationship_type: str
    sources: tuple[RelSourceViewModel, ...]
    column_mappings: tuple[ColumnMappingViewModel, ...]
    adjacency_stats: AdjacencyStatsViewModel | None
    validation_issues: tuple[ValidationIssue, ...]

@dataclass(frozen=True)
class SourceMappingViewModel:
    source_id: str
    uri: str
    maps_to: str  # entity_type or relationship_type
    mapping_type: str  # "entity" | "relationship"
    status: str  # "connected" | "orphaned" | "error"

@dataclass(frozen=True)
class ModelStatsViewModel:
    entity_type_count: int
    relationship_type_count: int
    total_entity_rows: int | None
    total_relationship_rows: int | None
    index_count: int
    index_memory_bytes: int
```

### Screen Protocols

Each browser screen implements this contract via `VimNavigableScreen[T]`:

```python
# EntityBrowserScreen(VimNavigableScreen[EntityViewModel])
def load_items(self) -> list[EntityViewModel]:
    return self.adapter.entity_types()

def update_detail(self, item: EntityViewModel) -> None:
    detail = self.adapter.entity_detail(item.entity_type)
    self.detail_panel.update_item(detail)
```

---

## 4. Component Responsibility Matrix

| Component                  | Responsibility                          | Reads From           | Writes To       |
|----------------------------|-----------------------------------------|----------------------|-----------------|
| EntityBrowserScreen        | List/detail entity type exploration      | DataModelAdapter     | —               |
| EntityEditorScreen         | Edit entity source configuration         | DataModelAdapter     | ConfigManager   |
| RelationshipBrowserScreen  | List/detail relationship exploration     | DataModelAdapter     | —               |
| RelationshipEditorScreen   | Edit relationship source configuration   | DataModelAdapter     | ConfigManager   |
| DataSourceMapperScreen     | Visualize source→model mappings          | DataModelAdapter     | —               |
| DataSourceInspectorScreen  | Schema introspection and preview         | DataSourceIntrospector | —             |
| GraphTopologyWidget        | Entity-relationship graph rendering      | DataModelAdapter     | —               |
| ModelStatisticsPanel       | Cardinality and index statistics          | DataModelAdapter     | —               |
| SchemaTreeWidget           | Hierarchical column display              | IntrospectionResult  | —               |
| EndpointSelector           | Entity type picker for relationships     | DataModelAdapter     | ConfigManager   |
| SchemaPreviewWidget        | Live column preview during editing       | DataSourceIntrospector | —             |
| DataModelAdapter           | Facade joining config + runtime indexes  | ConfigManager, GraphIndexManager | — |

---

## 5. Navigation Flow

### Screen Navigation Graph

```
PipelineOverviewScreen
    │
    ├── [e] Entity Types ──→ EntityBrowserScreen
    │                            │
    │                            ├── [Enter] ──→ EntityEditorScreen
    │                            │                   └── [Escape/q] ──→ back
    │                            └── [Escape/q] ──→ back
    │
    ├── [r] Relationships ──→ RelationshipBrowserScreen
    │                            │
    │                            ├── [Enter] ──→ RelationshipEditorScreen
    │                            │                   └── [Escape/q] ──→ back
    │                            └── [Escape/q] ──→ back
    │
    ├── [s] Data Sources ──→ DataSourceMapperScreen
    │                            │
    │                            ├── [Enter] ──→ DataSourceInspectorScreen
    │                            │                   └── [Escape/q] ──→ back
    │                            └── [Escape/q] ──→ back
    │
    ├── [m] Data Model ──→ DataModelScreen [enhanced]
    │                            │
    │                            ├── [Tab] cycle: Graph → Schema → Statistics
    │                            └── [Escape/q] ──→ back
    │
    └── [existing screens: queries, outputs, lineage, templates, testing]
```

### Navigation Patterns

1. **Drill-down:** Overview → Browser → Editor (3-level max depth)
2. **Breadcrumb trail:** Each screen reports `breadcrumb_text` (e.g., "Pipeline > Entities > Customer")
3. **Back navigation:** `Escape` or `q` in NORMAL mode pops to parent screen
4. **Cross-reference jumps:** `gd` (go-to-definition) from relationship → entity browser at referenced type
5. **Tab cycling:** Within DataModelScreen, `Tab` switches between sub-views

### Key Binding Conventions

| Key | Context | Action |
|-----|---------|--------|
| `j/k` | All browsers | Navigate list items |
| `Enter` | Browser | Drill into selected item / open editor |
| `Escape`/`q` | All screens | Back to parent |
| `a` | Browser | Add new item |
| `dd` | Browser | Delete selected item |
| `e` | Overview | Jump to entity browser |
| `r` | Overview | Jump to relationship browser |
| `s` | Overview | Jump to data source mapper |
| `m` | Overview | Jump to data model view |
| `gd` | Relationship browser | Jump to referenced entity |
| `p` | Source browser | Preview data sample |
| `Tab` | Model screen | Cycle sub-views |
| `/` | All screens | Search/filter |
| `:w` | Editors | Save changes |
| `u` | Editors | Undo |
| `Ctrl+r` | Editors | Redo |

---

## 6. Integration Points with Existing Infrastructure

### Reused Components

| Existing Component | Reuse In | How |
|---|---|---|
| `VimNavigableScreen[T]` | All new browsers | Direct subclass |
| `BaseListItem[T]` | All list panels | Subclass with custom `compose()` |
| `BaseDetailPanel` | All detail panels | Subclass with custom `update_item()` |
| `ColumnMappingWidget` | RelationshipEditorScreen | Embed directly |
| `DataPreviewDialog` | DataSourceInspectorScreen | Mount on `p` keypress |
| `ConfirmDialog` / `InputDialog` | All editors | CRUD confirmation |
| `ConfigManager` | All editors | Mutation + undo/redo |
| `CachedValidator` | All editors | Real-time validation feedback |
| `HelpRegistry` | All new screens | Register help topics |

### New Integration Seams

1. **DataModelAdapter ↔ ConfigManager:** Adapter subscribes to `ConfigChanged` messages
   to refresh its cache. No polling.

2. **DataModelAdapter ↔ GraphIndexManager:** Optional dependency. When available
   (after data loading), adapter provides statistics. When unavailable, stats fields
   return `None` and UI shows "Load data to see statistics."

3. **Screen registration in app.py:** New screens register in `_navigate_to_section()`
   method (existing pattern from PipelineOverviewScreen's section routing).

4. **Help topics:** Each new screen registers topics via `HelpRegistry.register()`
   following the existing `"screens.<name>"` namespace pattern.

### File Layout

```
packages/pycypher-tui/src/pycypher_tui/
├── adapters/                    [new directory]
│   ├── __init__.py
│   ├── data_model.py            DataModelAdapter
│   └── view_models.py           All ViewModel dataclasses
├── screens/
│   ├── entity_browser.py        [new] EntityBrowserScreen
│   ├── entity_editor.py         [new] EntityEditorScreen
│   ├── relationship_browser.py  [new] RelationshipBrowserScreen
│   ├── relationship_editor.py   [new] RelationshipEditorScreen
│   ├── data_source_mapper.py    [new] DataSourceMapperScreen
│   ├── data_source_inspector.py [new] DataSourceInspectorScreen
│   └── ... (existing screens unchanged)
├── widgets/
│   ├── graph_topology.py        [new] GraphTopologyWidget
│   ├── model_statistics.py      [new] ModelStatisticsPanel
│   ├── schema_tree.py           [new] SchemaTreeWidget
│   ├── endpoint_selector.py     [new] EndpointSelector
│   ├── schema_preview.py        [new] SchemaPreviewWidget
│   └── ... (existing widgets unchanged)
└── ... (existing files unchanged)
```

---

## 7. Design Decisions

| Decision | Rationale |
|---|---|
| Adapter facade between screens and pycypher | Screens stay decoupled from internal data structures. Adapter can be tested independently. Changes to pycypher internals only require adapter updates. |
| Frozen dataclass view models | Immutable data prevents accidental mutation from UI code. Cheap to create, safe to pass around. |
| Optional GraphIndexManager dependency | TUI must work in config-only mode (no data loaded). Statistics degrade gracefully to "unavailable." |
| 3-level max navigation depth | Keeps mental model simple. Overview → Browse → Edit covers all workflows. |
| Reuse VimNavigableScreen[T] for browsers | Consistent UX. No new navigation patterns to learn. Existing test infrastructure applies. |
| New VimEditableScreen[T] for editors | Editing needs INSERT mode field handling, dirty tracking, and save confirmation — distinct from browsing. |
| Cross-reference via `gd` | Familiar VIM idiom. Enables fast navigation between related entities without returning to overview. |
