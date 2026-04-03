# VimNavigableScreen API Reference

Base class for all list-detail screens with VIM-style navigation.

**Module:** `pycypher_tui.screens.base`

## Overview

`VimNavigableScreen[T]` is a generic abstract `Widget` (not a Textual `Screen`) that provides:

- Cursor-managed list navigation (j/k/gg/G/Ctrl+F/Ctrl+B)
- Two-column layout (scrollable list panel + detail panel)
- ModeManager integration for key routing
- Search with `/pattern`, `n`/`N` cycling
- Delete confirmation via `ConfirmDialog`
- Yank-to-register via `RegisterFile`
- Header with title + breadcrumb, footer with key hints

Screens subclass `VimNavigableScreen[T]` with a concrete item type `T` (e.g., `SourceItem`, `EntityTableInfo`, `RelationshipItem`, `TemplateSummary`).

## App-Level Integration

`VimNavigableScreen` instances are mounted inside the `PyCypherTUI` app, which owns:

- **`ModeManager`** (`self.app.mode_manager`) -- coordinates modal input
- **`RegisterFile`** (`self.app.register_file`) -- named registers for yank/paste
- **`CachedValidator`** (`self.app.validator`) -- config validation
- **`ConfigManager`** (`self._config_manager`) -- CRUD, undo/redo, atomic save

The app routes unhandled keys through ModeManager as a fallback and executes ex-commands (`:w`, `:q`, `:e`, `:help`, `:s`, `:u`, `:redo`, `:noh`, `:reg`).

## Constructor

```python
def __init__(self, config_manager: ConfigManager, **kwargs) -> None
```

All screens receive a `ConfigManager` for loading/saving pipeline configuration. `ConfigManager` provides:

- CRUD: `add_entity_source()`, `update_entity_source()`, `remove_entity_source()`, and relationship equivalents
- Undo/redo: `undo()`, `redo()`, `can_undo()`, `can_redo()`
- Persistence: `save(path)` with atomic writes and `.bak` backups
- Validation: `validate()` returning structured `ValidationResult`
- State: `is_dirty()`, `is_empty()`, `get_config()`

## Required Abstract Methods

Subclasses **must** implement all of the following:

### Display Configuration

| Property | Return Type | Description |
|---|---|---|
| `screen_title` | `str` | Title in screen header (e.g., "Data Sources") |
| `breadcrumb_text` | `str` | Breadcrumb path (e.g., "Pipeline > Data Sources") |
| `footer_hints` | `str` | Key hints shown in footer bar |

### Data Operations

| Method | Signature | Description |
|---|---|---|
| `load_items()` | `-> list[T]` | Load items from `ConfigManager`. Called by `refresh_from_config()`. |
| `get_item_id(item)` | `(T) -> str` | Return a unique CSS-safe identifier for an item. Used for widget IDs and search. |

### Widget Factory

| Method | Signature | Description |
|---|---|---|
| `create_list_item(item, item_id)` | `(T, str) -> BaseListItem` | Create a list item widget. `item_id` is the CSS ID. |
| `create_detail_panel()` | `-> BaseDetailPanel` | Create the right-side detail panel widget. |
| `update_detail_panel(item)` | `(T | None) -> None` | Update detail panel to show `item` (or clear if `None`). |

### CRUD Callbacks

| Method | Signature | Description |
|---|---|---|
| `on_edit(item)` | `(T) -> None` | Handle Enter/l on current item. Typically posts an `Edit*` message. |
| `on_add()` | `-> None` | Handle `a` key. Opens add dialog flow. |
| `on_delete(item)` | `(T) -> None` | Handle confirmed delete. Called after `ConfirmDialog` approval. |

## Optional Overrides

| Method/Property | Default | Description |
|---|---|---|
| `handle_extra_key(key)` | Returns `False` | Handle screen-specific keys (e.g., `Tab` for filter toggle). Return `True` if handled. |
| `get_item_search_text(item)` | Returns `get_item_id(item)` | Searchable text for `/pattern` matching. Override for multi-field search. |
| `_screen_override_keys` | `frozenset()` | Additional keys intercepted before ModeManager (e.g., `Tab`). |
| `list_panel_id` | `"list-panel"` | CSS ID for the list scroll container. |
| `detail_panel_id` | `"detail-panel"` | CSS ID for the detail panel. |
| `empty_list_message` | `"No items configured.\nPress 'a' to add."` | Message when item list is empty. |

## Built-in Features

### Cursor Management

```python
# Properties
item_count: int          # Number of loaded items
current_item: T | None   # Item at cursor position
items: list[T]           # All loaded items

# Methods (internal, used by key dispatch)
_move_cursor(delta: int)  # Move cursor by delta, clamped to bounds
_jump_to(index: int)      # Jump to absolute index
```

### Search

```python
apply_search(pattern: str)  # Apply regex search, jump to first match
search_next()               # Jump to next match (n key)
search_prev()               # Jump to previous match (N key)
search_status: str          # Human-readable: "/pattern [2/5]"
```

### Delete Confirmation

```python
_confirm_and_delete(item: T)  # Opens ConfirmDialog, calls on_delete() if confirmed
```

Automatically used when ModeManager dispatches `edit:delete_line` (from `dd`).

### Register (Yank)

```python
_yank_current_item()  # Stores item ID in app's RegisterFile (unnamed + yank registers)
```

### Refresh

```python
refresh_from_config()  # Reloads items via load_items(), re-renders list, updates detail panel
```

## Key Routing Flow

When a key is pressed, `on_key()` follows this priority:

1. **Pending multi-key sequences** in ModeManager (e.g., second `g` in `gg`) -- forward unconditionally
2. **Screen override keys** (`_SCREEN_OVERRIDE_KEYS | _screen_override_keys`) -- handle directly without ModeManager
3. **ModeManager routing** -- produces a command string or mode transition
4. **`handle_extra_key()`** fallback -- for anything ModeManager didn't handle

Default screen override keys: `a` (add), `Ctrl+F` (page down), `Ctrl+B` (page up), `n` (search next), `N` (search prev).

### Command Dispatch

ModeManager returns command strings in `category:action` format. `_dispatch_command()` translates them:

| Command | Action |
|---|---|
| `navigate:down` | `_move_cursor(1)` |
| `navigate:up` | `_move_cursor(-1)` |
| `navigate:left` | Post `NavigateBack` message |
| `navigate:right` | `on_edit(current_item)` |
| `navigate:first` | `_jump_to(0)` |
| `navigate:last` | `_jump_to(item_count - 1)` |
| `action:confirm` | `on_edit(current_item)` |
| `edit:delete_line` | `_confirm_and_delete(current_item)` |
| `clipboard:yank` | `_yank_current_item()` |
| `clipboard:paste` | No-op for list screens |
| `ex:/pattern` | `apply_search(pattern)` |
| `search:next` | `search_next()` |
| `search:prev` | `search_prev()` |

### Legacy Fallback

When no ModeManager is available (unit tests), `_handle_key_fallback()` provides direct key handling with the same j/k/gg/dd/Enter/h semantics.

## Supporting Classes

### BaseListItem[T]

Focusable list item widget with `focused` reactive property. Sets `.item-focused` CSS class when focused.

### BaseDetailPanel

Right-side detail panel with consistent styling. Subclasses override display methods. Constructor accepts `empty_message` for the no-selection state.

## Implementation Example

```python
class DataSourcesScreen(VimNavigableScreen[SourceItem]):
    @property
    def screen_title(self) -> str:
        return "Data Sources"

    @property
    def breadcrumb_text(self) -> str:
        return "Pipeline > Data Sources"

    @property
    def footer_hints(self) -> str:
        return " j/k:navigate  a:add  Enter:edit  dd:delete  Tab:filter  h:back"

    def load_items(self) -> list[SourceItem]:
        config = self.config_manager.get_config()
        return self._extract_sources(config)

    def create_list_item(self, item, item_id):
        return SourceListItem(item, id=item_id)

    def create_detail_panel(self):
        return SourceDetailPanel(id=self.detail_panel_id)

    def update_detail_panel(self, item):
        detail = self.query_one(f"#{self.detail_panel_id}", SourceDetailPanel)
        detail.update_source(item)

    def get_item_id(self, item):
        return item.source_id

    def on_edit(self, item):
        self.post_message(self.EditSource(item.source_id, item.source_type))

    def on_add(self):
        self._add_source_step1("entity")

    def on_delete(self, item):
        self.config_manager.remove_entity_source(item.source_id)
        self.refresh_from_config()

    # Screen-specific: Tab key for filter cycling
    def handle_extra_key(self, key):
        if key == "tab":
            self._cycle_filter()
            return True
        return False
```

## Existing Screens

| Screen | Item Type | Custom Keys | Notes |
|---|---|---|---|
| `DataSourcesScreen` | `SourceItem` | `Tab` (filter: all/entity/relationship) | Full CRUD with 3-step add dialog |
| `EntityTablesScreen` | `EntityTableInfo` | -- | Full CRUD with 3-step add dialog |
| `RelationshipScreen` | `RelationshipItem` | -- | Full CRUD with 5-step add dialog, validation status |
| `TemplateBrowserScreen` | `TemplateSummary` | -- | Read-only (add/delete are no-ops) |
