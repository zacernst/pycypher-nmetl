# Modal System Reference

VIM-style modal input system for the PyCypher TUI.

**Module:** `pycypher_tui.modes`

## Architecture

```
ModeManager (coordinator)
  |
  +-- NormalMode    (navigation, commands, multi-key sequences)
  +-- InsertMode    (text input passthrough)
  +-- VisualMode    (selection operations)
  +-- CommandMode   (ex-command line with history)
```

The `ModeManager` holds instances of all four modes and handles transitions between them. It is owned by `PyCypherTUI` (the Textual `App`) and accessed by screens via `self.app.mode_manager`.

**Key routing:** The app's `on_key()` handles non-NORMAL modes entirely and acts as a fallback for NORMAL mode keys that screens don't consume. Ex-commands (`:w`, `:q`, `:e`, `:help`, `:s`, etc.) are executed by `PyCypherTUI._execute_ex_command()` after CommandMode returns them as `ex:` prefixed command strings.

## ModeType Enum

```python
class ModeType(Enum):
    NORMAL  = auto()   # Default mode - navigation and commands
    INSERT  = auto()   # Text editing mode
    VISUAL  = auto()   # Selection mode
    COMMAND = auto()   # Ex-command line (:)
```

## KeyResult Protocol

Every `handle_key()` call returns a `KeyResult`:

```python
@dataclass
class KeyResult:
    handled: bool = False                    # Was the key consumed?
    transition_to: ModeType | None = None    # Request mode change
    command: str | None = None               # Command string to execute
    text_input: str | None = None            # Text to insert (INSERT mode)
    pending: bool = False                    # More keys needed (e.g., gg, dd)
```

## ModeManager

### Key Methods

| Method | Description |
|---|---|
| `handle_key(key: str) -> KeyResult` | Route key to current mode; execute transitions automatically |
| `transition_to(mode_type, trigger_key)` | Switch modes; calls `on_exit()` then `on_enter()` |
| `add_listener(callback)` | Register `(old_mode, new_mode)` callback for UI updates |
| `get_mode(mode_type) -> BaseMode` | Access a specific mode instance |

### Properties

| Property | Type | Description |
|---|---|---|
| `current_mode` | `BaseMode` | Active mode instance |
| `current_type` | `ModeType` | Active mode type |
| `display_name` | `str` | Mode name for status bar |
| `style_color` | `str` | CSS color for mode indicator |

### Transition Flow

```
1. handle_key(key) called on ModeManager
2. ModeManager delegates to current_mode.handle_key(key)
3. If result.transition_to is set:
   a. old_mode.on_exit()    -- clears pending keys
   b. current_type updated
   c. new_mode.on_enter()   -- clears pending keys
   d. Listeners notified with (old_type, new_type)
4. KeyResult returned to caller (screen)
```

## Mode Details

### Normal Mode

Default mode. Navigation, commands, and multi-key sequences.

**Display:** "NORMAL" in blue (`#7aa2f7`)

**Single-key bindings:**

| Key | Result |
|---|---|
| `h`/`left` | `command="navigate:left"` |
| `j`/`down` | `command="navigate:down"` |
| `k`/`up` | `command="navigate:up"` |
| `l`/`right` | `command="navigate:right"` |
| `w` | `command="motion:word_forward"` |
| `b` | `command="motion:word_backward"` |
| `e` | `command="motion:word_end"` |
| `G` | `command="navigate:last"` |
| `i` | `transition_to=INSERT` |
| `a` | `transition_to=INSERT, command="cursor:after"` |
| `o` | `transition_to=INSERT, command="line:new_below"` |
| `v` | `transition_to=VISUAL` |
| `:` (colon) | `transition_to=COMMAND` |
| `/` (slash) | `transition_to=COMMAND, command="command:search"` |
| `y` | `command="clipboard:yank"` |
| `p` | `command="clipboard:paste"` |
| `u` | `command="edit:undo"` |
| `Ctrl+R` | `command="edit:redo"` |
| `Enter` | `command="action:confirm"` |
| `Escape` | Clears pending keys |

**Multi-key sequences (pending state):**

| Sequence | Result |
|---|---|
| `gg` | `command="navigate:first"` |
| `dd` | `command="edit:delete_line"` |
| `cc` | `command="edit:change_line", transition_to=INSERT` |
| `f{char}` | `command="motion:find_char:{char}"` |
| `t{char}` | `command="motion:till_char:{char}"` |
| `F{char}` | `command="motion:find_char_back:{char}"` |
| `T{char}` | `command="motion:till_char_back:{char}"` |
| `ci{char}` | `command="textobj:change_i:{char}", transition_to=INSERT` |
| `ca{char}` | `command="textobj:change_a:{char}", transition_to=INSERT` |
| `q{reg}` | `command="macro:toggle_record:{reg}"` |
| `@{reg}` | `command="macro:play:{reg}"` |
| `@@` | `command="macro:replay_last"` |
| `"{reg}` | `command="register:select:{reg}"` |

### Insert Mode

Text input mode. All printable characters pass through as `text_input`.

**Display:** "INSERT" in green (`#9ece6a`)

| Key | Result |
|---|---|
| `Escape` | `transition_to=NORMAL` |
| `Enter` | `text_input="\n"` |
| `Tab` | `text_input="\t"` |
| `Backspace` | `command="edit:backspace"` |
| `Delete` | `command="edit:delete"` |
| Printable char | `text_input=char` |

### Visual Mode

Selection mode. Directional keys extend the selection.

**Display:** "VISUAL" in purple (`#bb9af7`)

| Key | Result |
|---|---|
| `Escape`/`v` | `transition_to=NORMAL, command="selection:clear"` |
| `h`/`left` | `command="selection:extend_left"` |
| `j`/`down` | `command="selection:extend_down"` |
| `k`/`up` | `command="selection:extend_up"` |
| `l`/`right` | `command="selection:extend_right"` |
| `y` | `transition_to=NORMAL, command="selection:yank"` |
| `d` | `transition_to=NORMAL, command="selection:delete"` |

### Command Mode

Ex-command line. Entered with `:` or `/`.

**Display:** "COMMAND" in amber (`#e0af68`)

| Key | Result |
|---|---|
| `Escape` | `transition_to=NORMAL` (clears buffer) |
| `Enter` | `transition_to=NORMAL, command="ex:{prefix}{buffer}"` |
| `Backspace` | Removes last char; exits to NORMAL if buffer empty |
| `Up` | Recall previous command from history |
| `Down` | Recall next command from history |
| Printable char | Appends to buffer |

The `prefix` is `:` for ex-commands or `/` for search. Commands are returned as `ex:/pattern` or `ex::w`.

**CommandHistory:** Stores up to 100 entries, deduplicates consecutive identical commands, supports up/down recall.

## Command String Conventions

Commands follow the `category:action` format:

| Category | Actions | Produced By |
|---|---|---|
| `navigate` | `left`, `right`, `up`, `down`, `first`, `last` | Normal mode |
| `motion` | `word_forward`, `word_backward`, `word_end`, `find_char:{c}`, `till_char:{c}`, `find_char_back:{c}`, `till_char_back:{c}` | Normal mode |
| `edit` | `delete_line`, `change_line`, `undo`, `redo`, `backspace`, `delete` | Normal/Insert modes |
| `clipboard` | `yank`, `paste` | Normal mode |
| `action` | `confirm` | Normal mode (Enter) |
| `selection` | `clear`, `extend_left/right/up/down`, `yank`, `delete` | Visual mode |
| `textobj` | `change_i:{c}`, `change_a:{c}` | Normal mode (ci/ca) |
| `macro` | `toggle_record:{reg}`, `play:{reg}`, `replay_last` | Normal mode |
| `register` | `select:{reg}` | Normal mode |
| `ex` | `:{cmd}`, `/{pattern}` | Command mode |
| `cursor` | `after` | Normal mode (a key) |
| `line` | `new_below` | Normal mode (o key) |
| `command` | `search` | Normal mode (/ key) |

## Supporting Modules

### RegisterFile (`modes/registers.py`)

Named register storage (a-z, 0-9, special registers).

| Register | Purpose |
|---|---|
| `"` (unnamed) | Default yank/paste target |
| `0` | Last yank content |
| `+` | System clipboard proxy |
| `a`-`z` | Named user registers |

Key methods: `yank(content, register)`, `paste(register)`, `append(name, content)`, `list_nonempty()`.

### MacroRecorder (`modes/registers.py`)

Records and replays key sequences stored in named registers.

| Method | Description |
|---|---|
| `start_recording(register)` | Begin recording keys into register |
| `stop_recording()` | Save recorded keys, return register name |
| `record_key(key)` | Record a key during macro recording |
| `get_macro(register)` | Get key sequence as `list[str]` |
| `get_last_macro()` | Get last played macro for `@@` |

Macros are stored as pipe-separated key strings in RegisterFile.

### Motions (`modes/motions.py`)

Word movement and character find operations.

| Function | VIM Key | Description |
|---|---|---|
| `word_forward(text, pos)` | `w` | Start of next word |
| `word_backward(text, pos)` | `b` | Start of previous word |
| `word_end(text, pos)` | `e` | End of current/next word |
| `find_char_forward(text, pos, char)` | `f{c}` | Next occurrence of char |
| `till_char_forward(text, pos, char)` | `t{c}` | Just before next char |
| `find_char_backward(text, pos, char)` | `F{c}` | Previous occurrence of char |
| `till_char_backward(text, pos, char)` | `T{c}` | Just after previous char |

Text objects for `ci`/`ca` operations:

| Function | VIM Key | Description |
|---|---|---|
| `find_inner_pair(text, pos, char)` | `ci(` etc. | Range inside delimiters |
| `find_around_pair(text, pos, char)` | `ca(` etc. | Range including delimiters |
| `find_inner_word(text, pos)` | `ciw` | Word at cursor |
| `find_around_word(text, pos)` | `caw` | Word + surrounding whitespace |

Supported delimiter pairs: `()`, `[]`, `{}`, `<>`, `""`, `''`, `` `` ``.

### Search/Replace (`modes/search_replace.py`)

VIM-style `:s` command parser and executor.

| Command | Description |
|---|---|
| `:s/pat/rep/` | Replace first match on current line |
| `:s/pat/rep/g` | Replace all on current line |
| `:%s/pat/rep/g` | Replace all in buffer |
| `:%s/pat/rep/gc` | Replace all with confirmation |
| `:%s/pat/rep/gi` | Case-insensitive replace all |

Separator can be any non-alphanumeric character (not just `/`).

## Transition Diagram

```
           i / a / o
  NORMAL ──────────> INSERT
    |  ^               |
    |  |  Escape       | Escape
    |  +───────────────+
    |
    | v
    +──────────> VISUAL
    |  ^           |
    |  | Esc/v/y/d |
    |  +-----------+
    |
    | : / /
    +──────────> COMMAND
       ^           |
       | Esc/Enter |
       +-----------+
```
