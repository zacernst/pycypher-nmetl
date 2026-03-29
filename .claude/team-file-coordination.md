# Multi-Agent File Coordination Protocol

## Purpose

When multiple agents work concurrently on the same codebase, file edit
collisions cause wasted effort and merge conflicts. This protocol
establishes lightweight coordination to prevent contention on shared
infrastructure files.

## Core Infrastructure Files (Serial Access Required)

These files are imported by 10+ modules and affect the entire codebase.
**Only one agent should edit these at a time.**

| File | Importers | Why It's Critical |
|------|-----------|-------------------|
| `packages/pycypher/src/pycypher/exceptions.py` | 14+ | Exception hierarchy; changes affect all error handling |
| `packages/pycypher/src/pycypher/ast_models.py` | 14+ | Core AST schema; changes affect all parsing/validation |
| `packages/pycypher/src/pycypher/star.py` | 8+ | Central query engine; touches all architectural layers |
| `packages/pycypher/src/pycypher/__init__.py` | All | Public API surface; re-exports from 3 hub modules |
| `packages/pycypher/src/pycypher/relational_models.py` | 9+ | Data model; changes affect all query execution |
| `packages/shared/src/shared/metrics.py` | 8+ | Metrics singleton; changes affect all instrumentation |

## Safe Zones (Parallel Work OK)

These areas have minimal cross-dependencies and can be edited
concurrently without coordination:

- **CLI commands** (`cli/*.py` except `cli/common.py`)
- **Individual evaluators** (`*_evaluator.py`) — each is self-contained
- **Scalar functions** (`scalar_functions/*.py`)
- **Examples** (`examples/*.py`)
- **Tests** (`tests/*.py`) — different test files rarely conflict
- **Documentation** (`docs/**/*.rst`)
- **Ingestion modules** (`ingestion/*.py` except `config.py`)
- **Sinks** (`sinks/*.py`)

## Coordination Protocol

### Before Starting a Task

1. **Check the task description** for file paths mentioned
2. **If the task touches a core infrastructure file**, send a message:

   ```
   To: team-lead
   "Claiming [file] for task #N, estimated duration: [short/medium/long]"
   ```

3. **If another agent has claimed the file**, wait or pick a different task

### Duration Estimates

- **Short** (< 5 minutes): Adding a method, fixing a bug
- **Medium** (5-15 minutes): Adding a class, refactoring a section
- **Long** (15+ minutes): Major restructuring, adding protocol methods

### When Collisions Happen

If you get a "File has been modified since read" error:

1. **Stop editing that file immediately**
2. **Re-read the file** to see what changed
3. **Assess**: Can your changes be applied on top of the new state?
4. **If yes**: Re-read and re-apply your edit
5. **If no**: Message the team lead about the conflict

### Task Assignment Rules

The team lead should:

1. **Check file overlap** before assigning tasks to different agents
2. **Never assign two tasks that modify the same core file** simultaneously
3. **Sequence dependent tasks** — if Task A modifies exceptions.py and
   Task B depends on those changes, assign B after A completes
4. **Use `blocks`/`blockedBy`** in the task system for file dependencies

## File Dependency Map

```
ast_models.py ──────┐
                     ├──> star.py ──> __init__.py
relational_models.py┘        │
                             │
exceptions.py ───────────────┘
                             │
metrics.py ──────────────────┘
```

Changes flow **downstream**: modifying `ast_models.py` may require
corresponding updates in `star.py` and then `__init__.py`. These
should be assigned to the **same agent** or sequenced with blockers.

## Anti-Patterns

- **3+ agents editing `exceptions.py`** — we experienced this; assign
  all exception work to one agent per round
- **Adding exports to `__init__.py` without checking** — another agent
  may be modifying the same import block
- **Modifying `star.py` dispatch logic** while another agent adds
  clause handlers — these are the same code region
- **Adding metrics instrumentation** across files while another agent
  restructures the metrics module
