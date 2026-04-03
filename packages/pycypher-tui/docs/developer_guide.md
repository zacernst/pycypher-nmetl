# Developer Guide

How to extend the PyCypher TUI: adding screens, keys, commands, and tests.

## Adding a New List-Detail Screen

All list-detail screens extend `VimNavigableScreen[T]` where `T` is your data model type.

### Step 1: Define your data model

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class MyItem:
    id: str
    name: str
    status: str
```

### Step 2: Create list item and detail panel widgets

```python
from pycypher_tui.screens.base import BaseListItem, BaseDetailPanel

class MyListItem(BaseListItem[MyItem]):
    CSS = """
    MyListItem { width: 100%; height: 1; padding: 0 2; }
    MyListItem.item-focused { background: #283457; }
    """
    def __init__(self, item: MyItem, **kwargs):
        super().__init__(**kwargs)
        self.item = item

    def compose(self) -> ComposeResult:
        yield Label(f"  {self.item.name} [{self.item.status}]")

class MyDetailPanel(BaseDetailPanel):
    def update_item(self, item: MyItem | None) -> None:
        self.remove_children()
        if item is None:
            self.mount(Label("(nothing selected)", classes="detail-title"))
            return
        self.mount(Label(f"  {item.name}", classes="detail-title"))
        self.mount(Label(f"  Status: {item.status}", classes="detail-row"))
```

### Step 3: Implement the screen

These are the **required** abstract methods:

```python
from pycypher_tui.screens.base import VimNavigableScreen

class MyScreen(VimNavigableScreen[MyItem]):

    # --- Display properties (required) ---

    @property
    def screen_title(self) -> str:
        return "My Screen"

    @property
    def breadcrumb_text(self) -> str:
        return "Pipeline > My Section"

    @property
    def footer_hints(self) -> str:
        return " j/k:navigate  Enter:edit  a:add  dd:delete"

    # --- Data loading (required) ---

    def load_items(self) -> list[MyItem]:
        """Return items from ConfigManager or other source."""
        return [...]

    # --- Widget factories (required) ---

    def create_list_item(self, item: MyItem, item_id: str) -> BaseListItem:
        return MyListItem(item, id=item_id)

    def create_detail_panel(self) -> BaseDetailPanel:
        return MyDetailPanel(id=self.detail_panel_id)

    def update_detail_panel(self, item: MyItem | None) -> None:
        detail = self.query_one(f"#{self.detail_panel_id}", MyDetailPanel)
        detail.update_item(item)

    def get_item_id(self, item: MyItem) -> str:
        return item.id  # Must be CSS-safe (no spaces)

    # --- CRUD callbacks (required) ---

    def on_edit(self, item: MyItem) -> None:
        """Called on Enter/l. Open edit dialog or drill-down."""
        pass

    def on_add(self) -> None:
        """Called on 'a'. Open InputDialog chain."""
        pass

    def on_delete(self, item: MyItem) -> None:
        """Called after 'dd' confirmation. Remove from config."""
        self._config_manager.remove_my_item(item.id)
        self.refresh_from_config()
```

### Step 4: Optional overrides

```python
    # Search text (default uses get_item_id)
    def get_item_search_text(self, item: MyItem) -> str:
        return f"{item.id} {item.name} {item.status}"

    # Empty state message
    @property
    def empty_list_message(self) -> str:
        return "No items yet. Press 'a' to add one."

    # Screen-specific keys that should NOT go through ModeManager
    @property
    def _screen_override_keys(self) -> frozenset[str]:
        return frozenset({"tab", "r"})

    def handle_extra_key(self, key: str) -> bool:
        match key:
            case "tab":
                self._cycle_filter()
                return True
            case _:
                return False
```

### Step 5: Register in the app

Mount your screen in `PyCypherTUI.compose()` inside `#main-content` and wire it to the navigation system (e.g., respond to `PipelineOverviewScreen.SectionSelected`).

---

## Adding a New Key Binding

See [Key Routing Architecture](key_routing_architecture.md) for the full dual-path design.

### New NormalMode key (e.g., a new motion)

1. Add to `NormalMode.handle_key()` in `modes/normal.py`, returning a `KeyResult` with a command string
2. Handle the command in `VimNavigableScreen._dispatch_command()` if screen-relevant
3. The app's `_dispatch_to_content()` automatically forwards unrecognized commands

### New screen-specific key (e.g., Tab for filter)

1. Override `_screen_override_keys` to include the key name
2. Handle it in `handle_extra_key()` — this runs BEFORE ModeManager sees the key

### New ex-command (e.g., `:mycommand`)

1. Add a case to `PyCypherTUI._execute_ex_command()` in `app.py`

---

## Adding a New Dialog

Use `InputDialog` for text input and `ConfirmDialog` for yes/no:

```python
from pycypher_tui.widgets.dialog import InputDialog, ConfirmDialog

# Single input
def on_add(self) -> None:
    dialog = InputDialog(
        title="Add Item",
        prompt="Enter item name:",
        callback=self._handle_add_name,
    )
    self.app.push_screen(dialog)

def _handle_add_name(self, name: str) -> None:
    # Chain to next dialog or commit
    self._config_manager.add_item(name)
    self.refresh_from_config()
```

For multi-step flows, chain callbacks — each dialog's callback opens the next dialog. See `DataSourcesScreen.on_add()` (3 steps) or `RelationshipScreen.on_add()` (5 steps) for examples.

---

## Writing Tests

### Unit tests (no Textual app)

Test screen logic by instantiating the screen with a `ConfigManager`:

```python
from pycypher_tui.screens.data_sources import DataSourcesScreen
from pycypher_tui.config.pipeline import ConfigManager

def test_load_items():
    cm = ConfigManager()
    cm.add_entity_source("src1", "file.csv", "Person")
    screen = DataSourcesScreen(config_manager=cm)
    items = screen.load_items()
    assert len(items) == 1
    assert items[0].source_id == "src1"
```

### Behavioral pilot tests (with Textual app)

Use `textual.testing.app_test` for end-to-end key interaction:

```python
import pytest
from pycypher_tui.app import PyCypherTUI

@pytest.mark.asyncio
async def test_navigation():
    async with PyCypherTUI().run_test() as pilot:
        await pilot.press("j")  # Move down
        await pilot.press("k")  # Move up
        await pilot.press("enter")  # Drill in
```

### Mode tests (no app needed)

Test modes directly:

```python
from pycypher_tui.modes.manager import ModeManager

def test_normal_to_insert():
    mm = ModeManager()
    result = mm.handle_key("i")
    assert result.transition_to == ModeType.INSERT
```
