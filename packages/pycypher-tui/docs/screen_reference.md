# Screen Reference

Complete reference for all TUI screens: their purpose, data models, and screen-specific keys.

## Screen Architecture Overview

The TUI has two categories of screens:

1. **VimNavigableScreen subclasses** — List-detail screens mounted as `Widget` instances inside `#main-content`. These share cursor management, search, delete confirmation, and yank-to-register via the base class.

2. **Textual Screen subclasses** — Full-screen editors pushed onto the Textual screen stack. These manage their own key routing independently of the base class.

```
PyCypherTUI (App)
  |
  +-- #main-content (Container)
  |     +-- PipelineOverviewScreen (VimNavigableScreen[SectionInfo])
  |     +-- DataSourcesScreen (VimNavigableScreen[SourceItem])
  |     +-- EntityTablesScreen (VimNavigableScreen[EntityTableInfo])
  |     +-- RelationshipScreen (VimNavigableScreen[RelationshipItem])
  |     +-- TemplateBrowserScreen (VimNavigableScreen[TemplateSummary])
  |     +-- PipelineTestingScreen (VimNavigableScreen[ExecutionStep])
  |
  +-- Screen stack (push/pop)
        +-- QueryEditorScreen (Screen) -- pushed on top, not a VimNavigableScreen
        +-- HelpScreen (ModalScreen) -- modal overlay
```

---

## PipelineOverviewScreen

**Module:** `pycypher_tui.screens.pipeline_overview`
**Base:** `VimNavigableScreen[SectionInfo]`
**Role:** Central dashboard — the first screen users see. Shows all pipeline sections with status and item counts.

### Data Model: SectionInfo

| Field | Type | Description |
|---|---|---|
| `key` | `str` | Section identifier (`entity_sources`, `relationship_sources`, `queries`, `outputs`) |
| `label` | `str` | Display name (e.g., "Entity Sources") |
| `icon` | `str` | Section icon (e.g., `[E]`, `[R]`, `[Q]`, `[O]`) |
| `item_count` | `int` | Number of items in this section |
| `status` | `str` | `"empty"`, `"configured"`, or `"error"` |
| `details` | `list[str]` | First 3 items as summary lines |

### Screen-Specific Keys

| Key | Action | Override Reason |
|---|---|---|
| `i` | Edit current section (posts `ActionRequested`) | Overrides NormalMode INSERT transition |
| `u` | Undo config change | Overrides NormalMode (already handles `u` but this goes directly to ConfigManager) |
| `Ctrl+R` | Redo config change | Direct ConfigManager access |
| `1`-`4` | Jump to section by number and drill in | Number keys for quick section access |

### Messages

| Message | When Posted |
|---|---|
| `SectionSelected(section_key)` | User presses Enter/l or number key on a section |
| `ActionRequested(section_key, action)` | User requests edit/add/delete on a section |

### Layout

Standard two-column list+detail plus a bottom validation summary bar showing pipeline validation status (valid/errors/warnings) with color-coded indicators.

---

## DataSourcesScreen

**Module:** `pycypher_tui.screens.data_sources`
**Base:** `VimNavigableScreen[SourceItem]`
**Role:** Manage entity and relationship data sources with Tab filtering.

### Screen-Specific Keys

| Key | Action |
|---|---|
| `Tab` | Cycle filter: All → Entity → Relationship → All |

### Add Flow

3-step chained `InputDialog` sequence:
1. Source ID
2. URI (file path or connection string)
3. Entity type / relationship type

---

## EntityTablesScreen

**Module:** `pycypher_tui.screens.entity_tables`
**Base:** `VimNavigableScreen[EntityTableInfo]`
**Role:** View and manage entity table configurations with column mappings.

### Add Flow

3-step chained `InputDialog` sequence:
1. Source ID
2. URI
3. Entity type

---

## RelationshipScreen

**Module:** `pycypher_tui.screens.relationships`
**Base:** `VimNavigableScreen[RelationshipItem]`
**Role:** Manage relationship data sources with validation status display.

### Add Flow

5-step chained `InputDialog` sequence:
1. Source ID
2. URI
3. Relationship type
4. Source column
5. Target column

---

## TemplateBrowserScreen

**Module:** `pycypher_tui.screens.template_browser`
**Base:** `VimNavigableScreen[TemplateSummary]`
**Role:** Browse and instantiate pre-built pipeline templates. Read-only — `on_add()` and `on_delete()` are no-ops.

---

## PipelineTestingScreen

**Module:** `pycypher_tui.screens.pipeline_testing`
**Base:** `VimNavigableScreen[ExecutionStep]`
**Role:** Dry run execution, execution plan visualization, and error diagnosis.

### Data Models

**ExecutionStep:**

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Step name (e.g., "Load users", "Execute query_1") |
| `description` | `str` | What this step does |
| `step_type` | `str` | `"validate"`, `"load"`, `"query"`, or `"output"` |
| `status` | `StepStatus` | PENDING, RUNNING, SUCCESS, WARNING, ERROR, SKIPPED |
| `duration_ms` | `float` | Execution time |
| `row_count` | `int` | Rows processed |
| `error_message` | `str \| None` | Error details if failed |
| `warnings` | `list[str]` | Warning messages |

**DiagnosticEntry:**

| Field | Type | Description |
|---|---|---|
| `severity` | `str` | `"error"`, `"warning"`, `"info"` |
| `category` | `str` | `"syntax"`, `"data"`, `"config"`, `"runtime"` |
| `message` | `str` | Diagnostic message |
| `suggestion` | `str` | Fix suggestion |
| `location` | `str` | Where in config (e.g., `query.query_1`) |

### Screen-Specific Keys

| Key | Action |
|---|---|
| `r` | Run dry execution |
| `q` | Close screen (pop from screen stack) |

### Dry Run Behavior

`run_dry_execution()` validates each step without actually loading data:
- **validate**: Runs `ConfigManager.validate()`, captures errors/warnings
- **load**: Confirms source exists in config (always succeeds)
- **query**: Parses inline Cypher via `pycypher.validate_query()`, reports syntax errors
- **output**: Confirms output config exists (always succeeds)

Results are displayed with timing, color-coded status, and diagnostics in the detail panel.

---

## QueryEditorScreen

**Module:** `pycypher_tui.screens.query_editor`
**Base:** `Screen` (Textual Screen, **NOT** VimNavigableScreen)
**Role:** Full-featured Cypher query editor with syntax awareness.

**Important:** This is the only screen that does NOT extend VimNavigableScreen. It is pushed onto the Textual screen stack (not mounted as a widget) and manages its own key routing through a `CypherEditor` widget.

### Architecture Difference

| Aspect | VimNavigableScreen subclasses | QueryEditorScreen |
|---|---|---|
| Base class | `Widget` | `Screen` |
| Mounting | Inside `#main-content` | Pushed onto screen stack |
| Key routing | ModeManager via App + screen overrides | Direct `on_key()` → `CypherEditor.handle_key()` |
| VIM features | List navigation (j/k/gg/G), search, yank | Full text editing (w/b/e, ci/ca, dd, yy, f/t) |
| Mode display | App status bar | Own `#editor-mode-label` widget |

### Keys

| Key | Mode | Action |
|---|---|---|
| `Ctrl+Enter` | Any | Execute query (validate syntax via pycypher) |
| `Ctrl+S` | Any | Save query to pipeline config |
| `q` | Normal | Close editor |
| `i`/`a`/`o` | Normal | Enter insert mode |
| `Escape` | Insert | Return to normal mode |
| All VIM motions | Normal | Handled by CypherEditor (w/b/e, f/t, gg/G, dd, etc.) |

### Messages

| Message | When Posted |
|---|---|
| `QueryExecuted(result)` | After query validation/execution |
| `EditorClosed(query_text, query_id)` | When editor is closed |

### Query Save Behavior

- **Existing query** (`query_id` provided): Removes and re-adds with updated inline text
- **New query** (no `query_id`): Auto-generates ID (`query_1`, `query_2`, ...) and saves

### CypherEditor Widget

The actual editing is handled by `CypherEditor` (`widgets/query_editor.py`), which provides:
- `EditorBuffer` with line-based text storage and cursor management
- `SyntaxToken` highlighting for Cypher keywords
- `QueryHistory` for previously executed queries
- Own normal/insert mode tracking (independent of App's ModeManager)
