# PyCypher TUI

VIM-style terminal interface for PyCypher ETL pipeline configuration.

## Quick Start

```bash
# Run the TUI
pycypher-tui

# Open a config file directly
pycypher-tui my_pipeline.yaml
```

Use `j`/`k` to navigate, `Enter` to drill in, `h` to go back, `a` to add, `dd` to delete. Press `:help` for the built-in help system.

## Documentation

- [VimNavigableScreen API](docs/vim_navigable_screen_api.md) -- Base class for list-detail screens
- [Modal System Reference](docs/modal_system_reference.md) -- VIM mode architecture (NORMAL/INSERT/VISUAL/COMMAND)
- [Keybinding Reference](docs/keybinding_reference.md) -- Complete keybinding cheat sheet
- [Key Routing Architecture](docs/key_routing_architecture.md) -- How VIM key events flow through the dual-path system
- [Screen Reference](docs/screen_reference.md) -- All screens: Overview, DataSources, Entities, Relationships, Testing, QueryEditor
- [Config Schema](docs/config_schema.md) -- YAML pipeline configuration format
- [Developer Guide](docs/developer_guide.md) -- How to add screens, keys, commands, and tests

## Architecture

The TUI is built on [Textual](https://textual.textualize.io/) with a VIM-style modal system:

- **`PyCypherTUI`** (app.py) -- Main app owning ModeManager, RegisterFile, and ConfigManager
- **`VimNavigableScreen[T]`** (screens/base.py) -- Generic base for all list-detail screens
- **`ModeManager`** (modes/manager.py) -- Coordinates NORMAL/INSERT/VISUAL/COMMAND modes
- **`ConfigManager`** (config/pipeline.py) -- CRUD, undo/redo, atomic save with `.bak` backups
