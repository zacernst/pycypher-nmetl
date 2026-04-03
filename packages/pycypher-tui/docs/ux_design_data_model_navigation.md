# UX Design: Data Model Navigation and Editing

**Author:** Don (UX/API Specialist)
**Status:** Design specification for Task #3
**Depends on:** Task #2 (TUI Architecture Design)

---

## 1. Current State Analysis

The existing TUI has 8 screens with VIM-style navigation and a mature modal system. Key gaps identified for data model exploration and ETL configuration editing:

| Gap | Current State | Impact |
|-----|--------------|--------|
| DataModelScreen is read-only | `on_add()`/`on_delete()` are no-ops | Users cannot create or remove entity/relationship types from model view |
| Flat list layout | Entity/relationship types shown as flat list | No visual graph structure; hard to understand connections |
| Lineage tab incomplete | Shows "coming soon" placeholder | Users cannot trace data flow through pipeline |
| Sequential modal add flow | Relationship add requires 5 separate dialogs | Slow, error-prone, no way to review before confirming |
| No detail panel tab navigation | No keyboard shortcut to switch tabs | Mouse-dependent for tab switching in detail panel |
| No filter/group in data model | All types shown in single flat list | Hard to navigate in large models |
| No inline property editing | Must navigate to sub-screen to edit | Context loss when editing simple properties |

---

## 2. Screen Layout Mockups

### 2.1 Enhanced Data Model Screen (Graph View)

```
+------------------------------------------------------------------------+
| Pipeline > Data Model                            [NORMAL] config.yaml  |
+------------------------------------------------------------------------+
|                                                                        |
|  ENTITY TYPES                    | Overview | Attributes | Validation  |
|  ================================|          |            |             |
|                                  |----------------------------------------|
|  > (O) Person    [2 sources]     | Entity: Person                     |
|    (O) Company   [1 source ]     |                                     |
|    (O) Product   [3 sources]     | Type:    entity                     |
|                                  | Sources: 2 data sources             |
|  RELATIONSHIP TYPES              |   person_csv                        |
|  ================================|   person_api                        |
|                                  |                                     |
|    [->] KNOWS     [1 source ]    | Connections:                        |
|    [->] WORKS_AT  [1 source ]    |   -[:KNOWS]-> (person_id->friend_id)|
|    [->] PURCHASED [2 sources]    |   <-[:WORKS_AT]- (employee_id)      |
|                                  |                                     |
+------------------------------------------------------------------------+
|  Graph: 3 entities  3 relationships  6 connections                     |
+------------------------------------------------------------------------+
|  j/k:navigate  Enter:drill-down  e:edit  a:add  Tab:switch-tab  h:back|
+------------------------------------------------------------------------+
```

Key changes from current:
- **Grouped sections**: Entity types and relationship types in labeled groups with separators
- **Tab key**: Cycles through detail panel tabs (Overview, Attributes, Validation, Statistics, Lineage)
- **`e` key**: Opens inline edit for the selected type's properties
- **`a` key**: Add new entity/relationship type (context-aware based on cursor position)

### 2.2 Enhanced Data Model Screen (ASCII Graph Layout)

Activated via `g` key to toggle between list and graph view:

```
+------------------------------------------------------------------------+
| Pipeline > Data Model [Graph View]               [NORMAL] config.yaml  |
+------------------------------------------------------------------------+
|                                                                        |
|        +--------+    KNOWS     +--------+                              |
|        | Person |------------->| Person |                              |
|        +--------+              +--------+                              |
|            |                                                           |
|            | WORKS_AT                                                   |
|            v                                                           |
|        +---------+                                                     |
|        | Company |                                                     |
|        +---------+                                                     |
|            ^                                                           |
|            | PURCHASED                                                 |
|            |                                                           |
|        +---------+                                                     |
|        | Product |                                                     |
|        +---------+                                                     |
|                                                                        |
+------------------------------------------------------------------------+
|  3 entities  3 relationships  |  hjkl:pan  +/-:zoom  Enter:select      |
+------------------------------------------------------------------------+
```

Implementation note: The graph layout uses a simple hierarchical layout algorithm. For models with >10 entity types, fall back to the grouped list view with a notification.

### 2.3 Unified Add/Edit Form (replaces sequential dialogs)

```
+------------------------------------------------------------------------+
|                    Add Relationship Source                              |
+------------------------------------------------------------------------+
|                                                                        |
|  Source ID:          [follows_csv_____________]                         |
|  URI:                [data/follows.csv________]                         |
|  Relationship Type:  [FOLLOWS_________________]                         |
|  Source Column:      [from_id_________________]                         |
|  Target Column:      [to_id___________________]                         |
|  ID Column:          [(optional)______________]                         |
|  Error Policy:       [ FAIL  v ]                                       |
|                                                                        |
|  +-- Validation ----------------------------------------+              |
|  | [ok] Source ID is unique                              |              |
|  | [ok] URI format valid (CSV file)                      |              |
|  | [!!] File not found at data/follows.csv               |              |
|  +------------------------------------------------------+              |
|                                                                        |
|  Tab:next-field  Shift+Tab:prev-field  Enter:confirm  Escape:cancel    |
+------------------------------------------------------------------------+
```

Key improvements:
- **Single form** replaces 5 sequential modal dialogs
- **Live validation** as user types (debounced 300ms)
- **Tab/Shift+Tab** to move between fields
- **Validation panel** shows issues before submission
- **Escape** cancels without losing work (with confirmation if fields modified)

### 2.4 Overview Dashboard Enhancement

```
+------------------------------------------------------------------------+
| Pipeline > Overview                              [NORMAL] config.yaml  |
+------------------------------------------------------------------------+
|                                                                        |
|  Pipeline: My ETL Pipeline                                             |
|  ==========================================                            |
|                                                                        |
|  +-- Model Summary -----+  +-- Health ----------+                      |
|  | 3 Entity Types       |  | [ok] Config valid  |                      |
|  | 3 Relationship Types |  | [ok] Sources found |                      |
|  | 6 Data Sources       |  | [!!] 1 warning     |                      |
|  | 4 Queries            |  +--------------------+                      |
|  | 2 Outputs            |                                              |
|  +-----------------------+                                             |
|                                                                        |
|  > [1] Entity Sources    .............. 3 items  [configured]          |
|    [2] Relationship Sources ........... 3 items  [configured]          |
|    [3] Queries ........................ 4 items  [configured]          |
|    [4] Outputs ........................ 2 items  [configured]          |
|    [5] Data Model ..................... 6 types   [ok]                 |
|    [6] Query Lineage .................. 9 nodes  [ok]                 |
|    [7] Pipeline Testing ............... idle                           |
|    [8] Query Editor                                                    |
|                                                                        |
+------------------------------------------------------------------------+
|  j/k:navigate  Enter:open  1-8:jump  /:search  :e file:open  :w:save  |
+------------------------------------------------------------------------+
```

Changes:
- **Model summary** card at top showing counts
- **Health indicator** card with validation status
- **Numbered sections** (1-8) for quick jump via number keys

---

## 3. User Workflow Diagrams

### 3.1 Data Model Exploration Flow

```
Pipeline Overview
    |
    |-- Enter on "Data Model" (or press 5)
    v
Data Model Screen (List View)
    |
    |-- j/k: navigate between entity/relationship types
    |-- /pattern: search types by name
    |-- g: toggle graph view
    |-- Tab: cycle detail panel tabs
    |
    |-- Enter/l on entity type
    |   v
    |   Entity Tables Screen (filtered to that type)
    |       |-- Enter: edit entity source properties
    |       |-- p: preview source data
    |       |-- h: back to Data Model
    |
    |-- Enter/l on relationship type
    |   v
    |   Relationship Screen (filtered to that type)
    |       |-- Enter: edit relationship mapping
    |       |-- p: preview source data
    |       |-- v: toggle validation detail
    |       |-- h: back to Data Model
    |
    |-- h/Escape: back to Pipeline Overview
```

### 3.2 Configuration Editing Flow

```
Any List Screen (Entity/Relationship/Query/Output)
    |
    |-- a: add new item
    |   v
    |   Unified Form Dialog
    |       |-- Tab/Shift+Tab: navigate fields
    |       |-- type in fields (INSERT mode in each field)
    |       |-- live validation feedback
    |       |-- Enter: confirm (if validation passes)
    |       |-- Escape: cancel (confirm if dirty)
    |       v
    |   Item added, cursor moves to new item
    |
    |-- Enter/l: edit selected item
    |   v
    |   Unified Form Dialog (pre-populated)
    |       |-- same navigation as add
    |       v
    |   Item updated, detail panel refreshes
    |
    |-- dd: delete selected item
    |   v
    |   Confirm Dialog ("Delete 'source_id'? (y/n)")
    |       |-- y: delete, cursor adjusts
    |       |-- n/Escape: cancel
    |
    |-- :w: save all changes to YAML
    |-- u: undo last change
    |-- Ctrl+R: redo
```

### 3.3 Relationship Column Mapping Flow

```
Relationship Screen
    |-- select relationship
    |-- Enter/l: edit
    |   v
    Relationship Edit Form
        |-- Tab to Source Column field
        |-- If source data available:
        |   Column picker dropdown shows available columns
        |   Filtered as you type
        |-- Tab to Target Column field
        |   Same column picker
        |
        |-- Validation panel shows:
        |   - Column exists in source data? [ok/XX]
        |   - Column type compatible?       [ok/!!]
        |   - Referential integrity?        [ok/!!]
        |
        |-- Enter: confirm mapping
        v
    Mapping saved, validation refreshes
```

---

## 4. Keyboard Shortcut Scheme

### 4.1 Global Keys (all screens)

| Key | Action | Notes |
|-----|--------|-------|
| `:w` | Save config | Atomic write with .bak backup |
| `:q` | Quit | Prompts if unsaved changes |
| `:e <file>` | Open config file | |
| `:help [topic]` | Show help | |
| `u` | Undo | |
| `Ctrl+R` | Redo | |
| `?` | Quick help overlay | Context-sensitive for current screen |

### 4.2 List Navigation (all VimNavigableScreen)

| Key | Action | Notes |
|-----|--------|-------|
| `j`/`k` | Move down/up | |
| `gg` | First item | |
| `G` | Last item | |
| `Ctrl+F`/`Ctrl+B` | Page down/up | 5 items per page |
| `/pattern` | Search | Regex supported |
| `n`/`N` | Next/prev match | |
| `Enter`/`l` | Drill down / edit | |
| `h`/`Escape` | Navigate back | |
| `a` | Add new item | |
| `dd` | Delete item | With confirmation |
| `y` | Yank (copy) | To register |
| `p` | Paste from register | |

### 4.3 New Keys for Enhanced Data Model

| Key | Action | Screen | Notes |
|-----|--------|--------|-------|
| `Tab` | Next detail tab | DataModelScreen | Cycles Overview > Attributes > Validation > Statistics > Lineage |
| `Shift+Tab` | Previous detail tab | DataModelScreen | Reverse cycle |
| `g` | Toggle graph view | DataModelScreen | Switch between list and ASCII graph |
| `e` | Edit properties | DataModelScreen | Opens edit form for selected type |
| `f` | Filter types | DataModelScreen | Toggle: All > Entities > Relationships |
| `1`-`5` | Jump to tab | DataModelScreen | 1=Overview, 2=Attributes, 3=Validation, 4=Statistics, 5=Lineage |

### 4.4 Form Dialog Keys

| Key | Action | Notes |
|-----|--------|-------|
| `Tab` | Next field | |
| `Shift+Tab` | Previous field | |
| `Enter` | Submit form | Only if validation passes |
| `Escape` | Cancel | Confirms if fields modified |
| `Ctrl+U` | Clear field | Clears current input field |

---

## 5. Responsive Layout Specifications

### 5.1 Terminal Size Breakpoints

| Width | Layout | Detail Panel | Notes |
|-------|--------|-------------|-------|
| >= 120 cols | Two-column (2fr + 1fr) | Full detail panel | Optimal layout |
| 80-119 cols | Two-column (3fr + 2fr) | Condensed detail | Narrower right panel |
| 60-79 cols | Single column | Toggle with `Tab` | Detail replaces list on toggle |
| < 60 cols | Single column | Detail as popup | Minimal mode |

### 5.2 Height Breakpoints

| Height | Adjustment |
|--------|-----------|
| >= 30 rows | Full layout with summary cards on overview |
| 20-29 rows | Collapse summary cards to single line |
| < 20 rows | Hide footer hints, collapse headers |

### 5.3 Implementation Strategy

The existing `VimNavigableScreen` uses Textual's CSS layout with `2fr + 1fr` for the two-column split. Responsive adjustments should use Textual's `on_resize` event:

```python
def on_resize(self, event: events.Resize) -> None:
    if event.size.width < 80:
        self._switch_to_single_column()
    else:
        self._switch_to_two_column()
```

---

## 6. Visual Representation of Data Relationships

### 6.1 Entity-Relationship Notation in List View

Current format is plain text. Enhanced format uses Cypher-inspired notation:

```
Entity Types:
  (O) Person     [2 sources]  -- KNOWS, WORKS_AT
  (O) Company    [1 source ]  <- WORKS_AT
  (O) Product    [3 sources]  <- PURCHASED

Relationships:
  [->] KNOWS      (Person)-[:KNOWS]->(Person)         [1 source]
  [->] WORKS_AT   (Person)-[:WORKS_AT]->(Company)     [1 source]
  [->] PURCHASED  (Person)-[:PURCHASED]->(Product)    [2 sources]
```

### 6.2 Connection Summary in Detail Panel

For entity types, show incoming and outgoing relationships:

```
Connections:
  Outgoing:
    -[:KNOWS]->(Person)        via person_id -> friend_id
    -[:WORKS_AT]->(Company)    via employee_id -> company_id
  Incoming:
    <-[:PURCHASED]-(Person)    via product_id <- purchased_id
```

### 6.3 ASCII Graph Layout Algorithm

For the graph toggle view (`g` key):

1. **Layout**: Use a layered (Sugiyama-style) approach:
   - Layer 0: Entity types with no incoming relationships
   - Layer N: Entity types whose incoming edges come from Layer N-1
   - Self-referential edges shown as loops
2. **Rendering**: Box-drawing characters for nodes, line-drawing for edges
3. **Constraints**: Max 10 entity types for graph view; beyond that, show grouped list with notification
4. **Interaction**: `hjkl` to pan viewport, `Enter` to select focused node

---

## 7. Error Handling and Validation UX Patterns

### 7.1 Validation Feedback Levels

| Level | Icon | Color | Behavior |
|-------|------|-------|----------|
| Pass | `[ok]` | Green (#9ece6a) | No action needed |
| Warning | `[!!]` | Amber (#e0af68) | Allows save, shows in summary |
| Error | `[XX]` | Red (#f7768e) | Blocks form submission, shows fix suggestion |
| Info | `[ii]` | Blue (#7aa2f7) | Informational, no action needed |

### 7.2 Inline Validation in Forms

Validation runs on each field as the user types (debounced 300ms):

```
Source ID:    [follows csv___________]
              [XX] Source IDs cannot contain spaces. Use underscores.

URI:          [data/follows.csv______]
              [!!] File not found. Will be created on first write.
```

### 7.3 Validation Summary Bar

Bottom of each screen shows aggregate validation status:

```
 3 relationships: 2 valid, 1 with warnings
```

Color-coded: green if all pass, amber if warnings, red if errors.

### 7.4 Error Recovery Patterns

| Error | Recovery UX |
|-------|------------|
| File not found | Warning with "file will be created" or "check path" suggestion |
| Duplicate ID | Error with auto-suggestion (append `_2`) |
| Missing required field | Error with field label highlighted, cursor jumps to field |
| Column not in source | Error with available columns listed as suggestions |
| Invalid URI scheme | Error with list of supported schemes |
| Parse error in query | Error with line/column indicator, syntax hint |

### 7.5 Undo/Redo Feedback

After undo/redo, show a brief notification:

```
 Undone: removed entity source 'person_csv'     (Ctrl+R to redo)
```

Notification disappears after 3 seconds or on next key press.

---

## 8. Interaction Pattern Summary

### 8.1 Navigation Metaphor

The TUI follows a **hub-and-spoke** model:
- **Hub**: Pipeline Overview (always accessible via `h` from any screen)
- **Spokes**: Entity, Relationship, Data Model, Query Lineage, Testing, Editor
- **Drill-down**: From Data Model into Entity/Relationship screens filtered by type

### 8.2 Edit Metaphor

All editing follows the **view-then-edit** pattern:
1. Navigate to item in list view
2. Press `Enter`/`l` to open edit form
3. Modify fields with live validation
4. `Enter` to confirm, `Escape` to cancel
5. `:w` to persist to YAML file

### 8.3 Discoverability

- **Footer hints**: Every screen shows available keys in the bottom bar
- **`?` key**: Context-sensitive help overlay
- **`:help`**: Full help system with topic navigation
- **Command palette**: `:` opens ex-command line with tab completion

---

## 9. Implementation Priority

| Priority | Feature | Effort | Impact |
|----------|---------|--------|--------|
| P0 | Unified form dialog (replace sequential modals) | Medium | High - fixes worst UX pain point |
| P0 | Detail panel tab keyboard navigation | Low | High - removes mouse dependency |
| P1 | Data model grouped list view with entity/rel sections | Low | Medium - better organization |
| P1 | Responsive single-column mode for narrow terminals | Medium | Medium - accessibility |
| P1 | Data model filter toggle (All/Entities/Relationships) | Low | Medium - navigation efficiency |
| P2 | ASCII graph view toggle | High | Medium - visual understanding |
| P2 | Enhanced overview dashboard with summary cards | Medium | Low-Medium - nice-to-have |
| P2 | Inline validation with field-level feedback | Medium | Medium - error prevention |
| P3 | Lineage tab full implementation | High | Medium - depends on lineage tracking |
| P3 | Column picker dropdown in relationship edit | Medium | Low - convenience |
