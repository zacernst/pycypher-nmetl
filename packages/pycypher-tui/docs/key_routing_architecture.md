# Key Routing Architecture

How VIM key events flow from Textual through ModeManager to screen actions.

**Critical context:** VimNavigableScreen instances are mounted as **widgets** inside `#main-content`, not pushed as Textual screens. This means key events bubble from child widgets up to the App, and both `VimNavigableScreen.on_key()` and `App.on_key()` can fire on the same key press.

## Dual-Path Design

The system uses two complementary routing paths to avoid double-handling:

### Path A: App-Driven (primary path for most keys)

```
User presses key
  -> VimNavigableScreen.on_key()
       ModeManager not in NORMAL? -> return (let App handle)
       Key not a screen override?  -> route through ModeManager
       event.stop() if handled     -> prevents App from seeing it
  -> App.on_key() (only fires if event was NOT stopped)
       -> ModeManager.handle_key()
       -> _execute_command(result.command)
            -> "ex:*" commands: _execute_ex_command() (save, quit, help, etc.)
            -> "command:search": set CommandMode prefix to /
            -> all other commands: _dispatch_to_content()
                 -> VimNavigableScreen._dispatch_command()
       -> if ModeManager didn't handle: _forward_key_to_content()
            -> VimNavigableScreen._handle_screen_key()
```

### Path B: Screen-Driven (override keys + pending sequences)

```
User presses key
  -> VimNavigableScreen.on_key()
       Pending multi-key sequence (gg, dd in progress)?
         -> Forward to ModeManager unconditionally
         -> _dispatch_command() on result
         -> event.stop()
       Screen override key (a, Ctrl+F/B, n, N)?
         -> _handle_screen_key() directly (no ModeManager)
         -> event.stop()
       Unit test (no ModeManager)?
         -> _handle_key_fallback() with legacy j/k/gg/dd handling
```

## Why This Prevents Double-Handling

1. **VimNavigableScreen fires first** (child widget in DOM order)
2. If the screen handles the key, it calls `event.stop()` -> App never sees it
3. If the screen doesn't handle it (or returns early for non-NORMAL modes), the event bubbles to the App
4. The App routes through ModeManager and dispatches commands back to the screen via `_dispatch_to_content()`

## Key Categories and Their Paths

| Key | Handled By | Path |
|---|---|---|
| `j`/`k`/`h`/`l`/`G` | VimNavigableScreen.on_key() -> ModeManager -> _dispatch_command() | Screen stops event |
| `gg`/`dd` (pending) | VimNavigableScreen.on_key() -> ModeManager (pending handler) | Screen stops event |
| `a` (add item) | VimNavigableScreen._handle_screen_key() | Screen override, no ModeManager |
| `Ctrl+F`/`Ctrl+B` | VimNavigableScreen._handle_screen_key() | Screen override, no ModeManager |
| `n`/`N` (search) | VimNavigableScreen._handle_screen_key() | Screen override, no ModeManager |
| `Tab` (screen-specific) | VimNavigableScreen.handle_extra_key() via _handle_screen_key() | Screen override |
| `i`/`v`/`:`/`/` (mode switch) | VimNavigableScreen.on_key() -> ModeManager (transition) | Screen stops event |
| `Escape` (in NORMAL) | App.on_key() -> ModeManager -> _dispatch_to_content("navigate:left") | App special case |
| `:w`/`:q`/`:help` (ex-cmds) | App._execute_ex_command() after CommandMode returns | App handles directly |
| INSERT/VISUAL/COMMAND keys | App.on_key() -> ModeManager | Screen returns early (non-NORMAL) |

## Screen Override Keys

These keys have different meanings in list screens vs VIM text editing, so VimNavigableScreen intercepts them **before** ModeManager:

| Key | Screen Meaning | NormalMode Would Do |
|---|---|---|
| `a` | Add new item | "Append after cursor" -> INSERT mode |
| `Ctrl+F` | Page down (5 items) | (no binding) |
| `Ctrl+B` | Page up (5 items) | (no binding) |
| `n` | Next search match | (no binding) |
| `N` | Previous search match | (no binding) |

Subclasses can extend via the `_screen_override_keys` property (e.g., DataSourcesScreen adds `Tab`).

## Adding New Key Bindings

**For a new screen-specific key** (e.g., `Tab` for filter):
1. Override `_screen_override_keys` to include the key
2. Handle it in `handle_extra_key()`

**For a new NormalMode key** (e.g., a new motion):
1. Add to `NormalMode.handle_key()` returning a command string
2. Add dispatch in `VimNavigableScreen._dispatch_command()` if screen-relevant
3. App's `_dispatch_to_content()` automatically forwards it

**For a new ex-command** (e.g., `:newcmd`):
1. Add to `App._execute_ex_command()` match statement

## Historical Context

The original 14 behavioral test failures were caused by both VimNavigableScreen.on_key() and App.on_key() independently calling ModeManager.handle_key() on the same key press without proper event stopping. The fix established the current dual-path architecture where the screen handles keys first and stops propagation, with the App serving as a fallback and ex-command executor.
