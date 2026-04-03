# Keybinding Quick Reference

Complete keybinding reference for the PyCypher TUI.

## Mode Indicator

The status bar shows the current mode:

| Mode | Color | Indicator |
|---|---|---|
| NORMAL | Blue | Default navigation mode |
| INSERT | Green | Text editing mode |
| VISUAL | Purple | Selection mode |
| COMMAND | Amber | Ex-command line |

## Normal Mode (default)

### Navigation

| Key | Action |
|---|---|
| `j` / `Down` | Move cursor down |
| `k` / `Up` | Move cursor up |
| `h` / `Left` | Navigate back (previous screen) |
| `l` / `Right` | Drill into item (same as Enter) |
| `gg` | Jump to first item |
| `G` | Jump to last item |
| `Ctrl+F` | Page down (5 items) |
| `Ctrl+B` | Page up (5 items) |

### Word Motions (query editor)

| Key | Action |
|---|---|
| `w` | Jump to start of next word |
| `b` | Jump to start of previous word |
| `e` | Jump to end of current/next word |

### Character Find (query editor)

| Key | Action |
|---|---|
| `f{char}` | Jump to next occurrence of char |
| `t{char}` | Jump to just before next char |
| `F{char}` | Jump to previous occurrence of char |
| `T{char}` | Jump to just after previous char |

### Editing

| Key | Action |
|---|---|
| `a` | Add new item (list screens) |
| `dd` | Delete current item (with confirmation) |
| `y` | Yank (copy) current item to register |
| `p` | Paste from register |
| `u` | Undo |
| `Ctrl+R` | Redo |

### Text Objects (query editor)

| Key | Action |
|---|---|
| `ci(` | Change inside parentheses |
| `ci[` | Change inside brackets |
| `ci{` | Change inside braces |
| `ci"` | Change inside double quotes |
| `ci'` | Change inside single quotes |
| `ca(` | Change around parentheses (including delimiters) |
| `ca[` | Change around brackets |
| `cc` | Change entire line |

### Search

| Key | Action |
|---|---|
| `/pattern` | Search forward (enters command mode) |
| `n` | Jump to next search match |
| `N` | Jump to previous search match |

### Mode Switching

| Key | Action |
|---|---|
| `i` | Enter INSERT mode (before cursor) |
| `a` | Enter INSERT mode (after cursor) * |
| `o` | Enter INSERT mode (new line below) |
| `v` | Enter VISUAL mode |
| `:` | Enter COMMAND mode |
| `/` | Enter COMMAND mode with search prefix |

\* On list screens, `a` triggers "add item" instead of INSERT mode.

### Macros

| Key | Action |
|---|---|
| `q{a-z}` | Start recording macro into register |
| `q` | Stop recording (while recording) |
| `@{a-z}` | Play macro from register |
| `@@` | Replay last played macro |

### Registers

| Key | Action |
|---|---|
| `"{a-z}` | Select register for next yank/paste |

### Other

| Key | Action |
|---|---|
| `Enter` | Confirm / activate / edit current item |
| `Escape` | Cancel pending key sequence |

## Insert Mode

| Key | Action |
|---|---|
| `Escape` | Return to NORMAL mode |
| `Backspace` | Delete character before cursor |
| `Delete` | Delete character at cursor |
| `Enter` | Insert newline |
| `Tab` | Insert tab / accept autocomplete |
| Any printable | Insert character |

## Visual Mode

| Key | Action |
|---|---|
| `h`/`j`/`k`/`l` | Extend selection in direction |
| `y` | Yank selection, return to NORMAL |
| `d` | Delete selection, return to NORMAL |
| `v` | Toggle off, return to NORMAL |
| `Escape` | Cancel selection, return to NORMAL |

## Command Mode

Enter with `:` from NORMAL mode.

### Built-in Ex-Commands (wired in app.py)

| Command | Action |
|---|---|
| `:w` / `:write` | Save configuration (atomic write + .bak backup) |
| `:q` / `:quit` | Quit (prompts if unsaved changes) |
| `:q!` / `:quit!` | Force quit without saving |
| `:wq` | Save and quit |
| `:e <file>` | Open pipeline config file |
| `:help` | Show help index |
| `:help <topic>` | Show help for topic |
| `:u` / `:undo` | Undo last config change |
| `:redo` | Redo last undone change |
| `:registers` / `:reg` | Show register contents |
| `:nohlsearch` / `:noh` | Clear search highlighting |

### Help Topics

`:help keys`, `:help normal`, `:help insert`, `:help visual`, `:help command`, `:help config`, `:help cypher`, `:help quickref`, `:help tutorial`, `:help screens`

### Search/Replace

| Command | Action |
|---|---|
| `:s/pat/rep/` | Replace first match on current line |
| `:s/pat/rep/g` | Replace all matches on current line |
| `:%s/pat/rep/g` | Replace all matches in buffer |
| `:%s/pat/rep/gc` | Replace all with confirmation |
| `:%s/pat/rep/gi` | Case-insensitive replace all |

### Command Mode Navigation

| Key | Action |
|---|---|
| `Up` | Previous command from history |
| `Down` | Next command from history |
| `Backspace` | Delete last character (exit if empty) |
| `Escape` | Cancel and return to NORMAL |
| `Enter` | Execute command and return to NORMAL |

## Screen-Specific Keys

These keys are intercepted by screens before reaching ModeManager:

### All List Screens

| Key | Action |
|---|---|
| `a` | Add new item (overrides NORMAL mode's "append after cursor") |
| `Ctrl+F` | Page down (5 items) |
| `Ctrl+B` | Page up (5 items) |
| `n` | Next search match |
| `N` | Previous search match |

### Data Sources Screen

| Key | Action |
|---|---|
| `Tab` | Cycle filter: All -> Entity -> Relationship -> All |

### Help Screen

| Key | Action |
|---|---|
| `q` / `Escape` | Close help |
| `j` / `k` | Scroll down / up |
| `gg` / `G` | Scroll to top / bottom |
| `n` / `p` | Next / previous help topic |
| `Ctrl+O` | Go back in topic history |

### Dialog Keybindings

#### Confirm Dialog

| Key | Action |
|---|---|
| `y` / `Enter` | Confirm (yes) |
| `n` / `Escape` / `q` | Cancel (no) |

#### Input Dialog

| Key | Action |
|---|---|
| `Enter` | Submit input value |
| `Escape` | Cancel |

#### General Dialog

| Key | Action |
|---|---|
| `Escape` / `q` | Close |
| `Enter` | OK / confirm |

## Troubleshooting: Keys Feel Unresponsive

If keys seem to do nothing, check these common causes:

### 1. Wrong Mode

The most common cause. Check the mode indicator in the status bar:

| If you're in... | `j`/`k` will... | Fix |
|---|---|---|
| **NORMAL** (blue) | Navigate up/down as expected | — |
| **INSERT** (green) | Type the letter `j`/`k` into a buffer | Press `Escape` to return to NORMAL |
| **COMMAND** (amber) | Append to the command line | Press `Escape` to cancel, or `Enter` to execute |
| **VISUAL** (purple) | Extend the selection | Press `Escape` or `v` to return to NORMAL |

### 2. Pending Key Sequence

Some keys start a multi-key sequence. If you've pressed the first key of a sequence, the system is waiting for more input:

| First Key | Waiting For | What Happens If Wrong Key |
|---|---|---|
| `g` | `g` (to make `gg` = jump to first) | Pending state silently cancels |
| `d` | `d` (to make `dd` = delete line) | Pending state silently cancels |
| `c` | `c` or `i`/`a` + char (for `cc`, `ci(`, etc.) | Pending state silently cancels |
| `f`/`t`/`F`/`T` | Any character (find/till) | Character consumed, may jump nowhere if not found |
| `q` | Register name `a`-`z` (macro record) | Pending state silently cancels |
| `@` | Register name or `@` (macro play) | Pending state silently cancels |
| `"` | Register name (register select) | Pending state silently cancels |

**Known limitation:** There is no visual indicator that a pending sequence is in progress. Press `Escape` to cancel any pending state.

### 3. Screen-Specific Key Overrides

Some keys behave differently depending on which screen you're on:

| Key | On List Screens | On QueryEditorScreen | In NormalMode (VIM standard) |
|---|---|---|---|
| `a` | **Add new item** | Enter INSERT after cursor | Enter INSERT after cursor |
| `i` | Enter INSERT mode | Enter INSERT before cursor | Enter INSERT before cursor |
| `i` (PipelineOverview only) | **Edit current section** | — | Enter INSERT before cursor |
| `n`/`N` | **Next/prev search match** | (editor handles) | (no binding) |
| `r` (PipelineTesting only) | **Run dry execution** | — | (no binding) |
| `q` (PipelineTesting only) | **Close screen** | **Close editor** | (no binding) |

If a key works on one screen but not another, it may be a screen-specific override. See [Screen Reference](screen_reference.md) for per-screen key tables.
