"""Interactive Cypher REPL for PyCypher.

Provides an interactive read-eval-print loop for exploring graph data
with Cypher queries.  Designed to feel like familiar SQL consoles
(psql, sqlite3, mycli) but for Cypher.

Features:

- Readline-based input with history persistence
- Dot-commands for schema inspection and session control
- Automatic query timing and row counts
- EXPLAIN/PROFILE commands for execution plan analysis
- Multi-line query support (terminate with ``;``)
- Tab completion for dot-commands

Usage::

    from pycypher.repl import CypherRepl

    repl = CypherRepl(entity_specs=["Person=people.csv"])
    repl.run()

Or via CLI::

    nmetl repl --entity Person=people.csv
"""

from __future__ import annotations

import cmd
import difflib
import logging
import os
import readline
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click

if TYPE_CHECKING:
    import pandas as pd

_logger = logging.getLogger(__name__)

# History file location
_HISTORY_DIR = Path.home() / ".pycypher"
_HISTORY_FILE = _HISTORY_DIR / "repl_history"
_MAX_HISTORY = 1000


def _ensure_history() -> None:
    """Create history directory and load history file if it exists."""
    _HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    if _HISTORY_FILE.exists():
        try:
            readline.read_history_file(str(_HISTORY_FILE))
        except OSError:
            _logger.debug(
                "Could not read REPL history file %s",
                _HISTORY_FILE,
                exc_info=True,
            )
    readline.set_history_length(_MAX_HISTORY)


def _save_history() -> None:
    """Save readline history to disk."""
    try:
        readline.write_history_file(str(_HISTORY_FILE))
    except OSError:
        _logger.debug(
            "Could not save REPL history file %s",
            _HISTORY_FILE,
            exc_info=True,
        )


class CypherRepl(cmd.Cmd):
    """Interactive Cypher query REPL.

    Args:
        entity_specs: Entity source specs (``"Label=path.csv"``).
        rel_specs: Relationship source specs (``"REL=path.csv:src:tgt"``).
        default_id_col: Default ID column name.
        prompt_str: Custom prompt string.

    """

    intro = (
        "PyCypher Interactive Shell\n"
        "Type Cypher queries or .help for commands.  "
        "End queries with ';' or press Enter.\n"
    )
    prompt = "cypher> "
    _multiline_prompt = "    .> "

    _DOT_COMMANDS: list[str] = [
        "help",
        "load",
        "schema",
        "tables",
        "functions",
        "metrics",
        "history",
        "search",
        "format",
        "template",
        "batch",
        "examples",
        "suggest",
        "hints",
        "clear",
        "quit",
        "exit",
    ]

    def __init__(
        self,
        *,
        entity_specs: list[str] | None = None,
        rel_specs: list[str] | None = None,
        default_id_col: str | None = None,
        prompt_str: str | None = None,
    ) -> None:
        super().__init__()
        self._entity_specs = entity_specs or []
        self._rel_specs = rel_specs or []
        self._default_id_col = default_id_col
        self._star: Any = None
        self._context: Any = None
        self._query_count = 0
        self._total_time_ms = 0.0
        self._multiline_buffer: list[str] = []
        self._output_format: str = "table"  # table, csv, json
        self._templates: dict[str, str] = {}
        if prompt_str:
            self.prompt = prompt_str

        _ensure_history()

    def preloop(self) -> None:
        """Build the execution context from entity/rel specs."""
        if not self._entity_specs and not self._rel_specs:
            click.echo(
                "No data sources loaded.  Use .load to add sources "
                "or restart with --entity/--rel flags.",
            )
            return

        self._build_context()

    def postloop(self) -> None:
        """Save history on exit."""
        _save_history()
        if self._query_count > 0:
            click.echo(
                f"\nSession: {self._query_count} queries, "
                f"{self._total_time_ms:.0f}ms total",
            )

    def _build_context(self) -> None:
        """Build Star execution context from specs."""
        from pycypher.ingestion.context_builder import ContextBuilder
        from pycypher.star import Star

        n_sources = len(self._entity_specs) + len(self._rel_specs)
        click.echo(f"Loading {n_sources} data source(s) …")
        t0 = time.perf_counter()

        builder = ContextBuilder()
        loaded = 0

        for i, spec in enumerate(self._entity_specs, 1):
            label, path, id_col = _parse_entity_spec(spec)
            effective_id = id_col or self._default_id_col
            try:
                builder.add_entity(label, path, id_col=effective_id)
                loaded += 1
                click.echo(f"  [{i}/{n_sources}] entity {label} <- {path}")
            except Exception as exc:  # noqa: BLE001 — REPL: display error and continue loading
                from pycypher.exceptions import sanitize_error_message

                click.echo(
                    f"  [{i}/{n_sources}] FAILED entity {label}: {sanitize_error_message(exc)}"
                )

        for j, spec in enumerate(self._rel_specs, 1):
            idx = len(self._entity_specs) + j
            rel_type, path, src_col, tgt_col = _parse_rel_spec(spec)
            try:
                builder.add_relationship(
                    rel_type,
                    path,
                    source_col=src_col,
                    target_col=tgt_col,
                )
                loaded += 1
                click.echo(
                    f"  [{idx}/{n_sources}] relationship {rel_type} <- {path}",
                )
            except Exception as exc:  # noqa: BLE001 — REPL: display error and continue loading
                from pycypher.exceptions import sanitize_error_message

                click.echo(
                    f"  [{idx}/{n_sources}] FAILED relationship {rel_type}: {sanitize_error_message(exc)}",
                )

        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        try:
            self._context = builder.build()
            self._star = Star(context=self._context)
            click.echo(
                f"  Context ready ({loaded}/{n_sources} loaded, {elapsed_ms:.0f}ms).\n"
            )
        except (ValueError, RuntimeError) as exc:
            click.echo(f"  Error building context: {exc}\n")

    def default(self, line: str) -> None:
        """Handle Cypher queries, multi-line input, and unknown dot-commands."""
        stripped = line.strip()

        # Handle empty lines
        if not stripped:
            return

        # "Did you mean?" for mistyped dot-commands
        if stripped.startswith("."):
            cmd_name = stripped[1:].split()[0] if stripped[1:].split() else ""
            matches = difflib.get_close_matches(
                cmd_name, self._DOT_COMMANDS, n=3, cutoff=0.5
            )
            if matches:
                suggestion = ", ".join(f".{m}" for m in matches)
                click.echo(
                    f"Unknown command '.{cmd_name}'. Did you mean: {suggestion}?"
                )
            else:
                click.echo(
                    f"Unknown command '.{cmd_name}'. Type .help for available commands."
                )
            return

        # Accumulate multi-line input
        if self._multiline_buffer:
            self._multiline_buffer.append(line)
            if stripped.endswith(";"):
                query = " ".join(self._multiline_buffer).strip()
                if query.endswith(";"):
                    query = query[:-1].strip()
                self._multiline_buffer.clear()
                self.prompt = "cypher> "
                self._execute_query(query)
            return

        # Check for multi-line continuation
        if not stripped.endswith(";") and _looks_incomplete(stripped):
            self._multiline_buffer.append(line)
            self.prompt = self._multiline_prompt
            return

        # Single-line query
        query = stripped
        if query.endswith(";"):
            query = query[:-1].strip()
        if query:
            self._execute_query(query)

    def _execute_query(self, query: str) -> None:
        """Execute a Cypher query and print results."""
        if self._star is None:
            click.echo(
                "No data context loaded.  "
                "Use .load to add data sources, e.g.:\n"
                "  .load entity Person=people.csv\n"
                "  .load rel KNOWS=knows.csv:src:tgt\n"
                "Or restart with --entity/--rel flags.",
            )
            return

        upper = query.upper().strip()

        # Handle EXPLAIN prefix
        if upper.startswith("EXPLAIN "):
            self._explain_query(query[8:].strip())
            return

        # Handle PROFILE prefix
        if upper.startswith("PROFILE "):
            self._profile_query(query[8:].strip())
            return

        t0 = time.perf_counter()
        try:
            result = self._star.execute_query(query)
        except Exception as exc:  # noqa: BLE001 — REPL: display error to user
            from pycypher.exceptions import sanitize_error_message

            click.echo(f"Error: {sanitize_error_message(exc)}")
            return
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        self._query_count += 1
        self._total_time_ms += elapsed_ms

        _display_result(result, fmt=self._output_format)
        click.echo(
            f"{len(result)} row(s)  ({elapsed_ms:.1f}ms)",
        )

    def _explain_query(self, query: str) -> None:
        """Show execution plan without running the query."""
        try:
            from pycypher.ast_converter import ASTConverter
            from pycypher.grammar_parser import GrammarParser

            parser = GrammarParser()

            # Phase 1: Parse to Lark tree
            t0 = time.perf_counter()
            parse_tree = parser.parse(query)
            parse_ms = (time.perf_counter() - t0) * 1000.0

            # Phase 2: Convert to typed AST
            t1 = time.perf_counter()
            raw_ast = parser.transformer.transform(parse_tree)
            converter = ASTConverter()
            typed_ast = converter.convert(raw_ast)
            convert_ms = (time.perf_counter() - t1) * 1000.0

            click.echo(f"\nParse: {parse_ms:.1f}ms  Convert: {convert_ms:.1f}ms")
            click.echo(f"Root: {type(typed_ast).__name__}")
            click.echo(f"\n{typed_ast.pretty()}")

            # Show semantic validation
            from pycypher.semantic_validator import SemanticValidator

            validator = SemanticValidator()
            errors = validator.validate(typed_ast)
            if errors:
                click.echo("\nValidation errors:")
                for err in errors:
                    click.echo(f"  - {err}")
            else:
                click.echo("\nValidation: OK")

        except Exception as exc:  # noqa: BLE001 — REPL: display error to user
            from pycypher.exceptions import sanitize_error_message

            click.echo(f"Error: {sanitize_error_message(exc)}")

    def _profile_query(self, query: str) -> None:
        """Execute query with profiling and show detailed breakdown."""
        if self._star is None:
            click.echo("No data context loaded.")
            return

        try:
            from pycypher.query_profiler import QueryProfiler

            profiler = QueryProfiler(star=self._star)
            report = profiler.profile(query)

            _display_result(
                self._star.execute_query(query)
                if report.row_count == 0
                else None,
            )
            click.echo(f"\n{report}")

            self._query_count += 1
            self._total_time_ms += report.total_time_ms

        except Exception as exc:  # noqa: BLE001 — REPL: display error to user
            from pycypher.exceptions import sanitize_error_message

            click.echo(f"Error: {sanitize_error_message(exc)}")

    # -----------------------------------------------------------------
    # Dot-commands
    # -----------------------------------------------------------------

    def do_help(self, arg: str) -> None:
        """Show available commands."""
        click.echo(
            "\nCommands:\n"
            "  .help                  Show this help\n"
            "  .load                  Load entity or relationship data sources\n"
            "  .schema                Show loaded entity types and relationships\n"
            "  .tables                Show entity and relationship table details\n"
            "  .functions             List available Cypher functions\n"
            "  .examples              Show query examples for loaded schema\n"
            "  .metrics               Show session query metrics\n"
            "  .history               Show recent query history\n"
            "  .search <keyword>      Search history for matching queries\n"
            "  .format <table|csv|json>  Set output format\n"
            "  .template save|list|run|delete  Manage query templates\n"
            "  .batch <file>          Run queries from a file\n"
            "  .suggest <query>       Show similar query suggestions\n"
            "  .hints <query>         Show performance hints for a query\n"
            "  .clear                 Clear the screen\n"
            "  .quit / .exit          Exit the REPL\n"
            "\n"
            "Query prefixes:\n"
            "  EXPLAIN <query>  Show execution plan without running\n"
            "  PROFILE <query>  Run with detailed timing breakdown\n"
            "\n"
            "End multi-line queries with ';'\n",
        )

    # Map dot-commands to methods
    def do_schema(self, arg: str) -> None:
        """Show loaded schema."""
        if self._context is None:
            click.echo("No context loaded.")
            return

        em = self._context.entity_mapping
        rm = self._context.relationship_mapping

        click.echo("\nEntity types:")
        for name, table in em.mapping.items():
            props = [c for c in table.column_names if c != "__ID__"]
            click.echo(f"  :{name}  ({len(props)} properties)")
            for p in props:
                click.echo(f"    .{p}")

        if rm.mapping:
            click.echo("\nRelationship types:")
            for name, table in rm.mapping.items():
                props = [
                    c
                    for c in table.column_names
                    if c not in {"__ID__", "__SOURCE__", "__TARGET__"}
                ]
                src = getattr(table, "source_entity_type", "?")
                tgt = getattr(table, "target_entity_type", "?")
                click.echo(
                    f"  [:{name}]  ({src})->({tgt})  "
                    f"({len(props)} properties)",
                )
                for p in props:
                    click.echo(f"    .{p}")
        click.echo()

    def do_tables(self, arg: str) -> None:
        """Show entity and relationship table row counts."""
        if self._context is None:
            click.echo("No context loaded.")
            return

        em = self._context.entity_mapping
        rm = self._context.relationship_mapping

        click.echo("\nEntity tables:")
        for name, table in em.mapping.items():
            df = table.source_obj
            click.echo(f"  {name}: {len(df)} rows, {len(df.columns)} cols")

        if rm.mapping:
            click.echo("\nRelationship tables:")
            for name, table in rm.mapping.items():
                df = table.source_obj
                click.echo(
                    f"  {name}: {len(df)} rows, {len(df.columns)} cols",
                )
        click.echo()

    def do_functions(self, arg: str) -> None:
        """List available scalar functions."""
        from pycypher.scalar_functions import ScalarFunctionRegistry

        registry = ScalarFunctionRegistry.get_instance()
        funcs = sorted(registry._functions.keys())
        click.echo(f"\n{len(funcs)} available functions:")
        # Print in columns
        cols = 4
        for i in range(0, len(funcs), cols):
            row = funcs[i : i + cols]
            click.echo(
                "  " + "  ".join(f"{f:<20}" for f in row),
            )
        click.echo()

    def do_metrics(self, arg: str) -> None:
        """Show session query metrics."""
        from shared.metrics import QUERY_METRICS

        snap = QUERY_METRICS.snapshot()
        click.echo(f"\nHealth: {snap.health_status()}")
        click.echo(snap.summary())
        click.echo()

    def do_history(self, arg: str) -> None:
        """Show recent query history."""
        n = readline.get_current_history_length()
        start = max(1, n - 20)
        click.echo(f"\nRecent history ({n} total):")
        for i in range(start, n + 1):
            item = readline.get_history_item(i)
            if item:
                click.echo(f"  {i}: {item}")
        click.echo()

    def do_search(self, arg: str) -> None:
        """Search command history for a keyword.

        Usage::

            .search MATCH
            .search person

        """
        keyword = arg.strip()
        if not keyword:
            click.echo("Usage: .search <keyword>")
            return

        n = readline.get_current_history_length()
        matches: list[tuple[int, str]] = []
        lower_kw = keyword.lower()
        for i in range(1, n + 1):
            item = readline.get_history_item(i)
            if item and lower_kw in item.lower():
                matches.append((i, item))

        if not matches:
            click.echo(f"No history entries matching '{keyword}'.")
            return

        click.echo(f"\n{len(matches)} match(es) for '{keyword}':")
        for idx, item in matches[-20:]:  # show last 20
            click.echo(f"  {idx}: {item}")
        click.echo()

    def do_format(self, arg: str) -> None:
        """Set output format: table (default), csv, or json.

        Usage::

            .format table
            .format csv
            .format json

        """
        fmt = arg.strip().lower()
        if fmt in ("table", "csv", "json"):
            self._output_format = fmt
            click.echo(f"Output format set to: {fmt}")
        elif not fmt:
            click.echo(f"Current format: {self._output_format}")
            click.echo("Usage: .format <table|csv|json>")
        else:
            click.echo(
                f"Unknown format '{fmt}'. Choose: table, csv, json"
            )

    def do_template(self, arg: str) -> None:
        """Save, list, or run query templates with $parameter substitution.

        Usage::

            .template save find_by_name MATCH (p:Person {name: $name}) RETURN p
            .template list
            .template run find_by_name name=Alice

        """
        parts = arg.strip().split(None, 1)
        if not parts:
            click.echo(
                "Usage:\n"
                "  .template save <name> <query with $params>\n"
                "  .template list\n"
                "  .template run <name> param1=val1 param2=val2"
            )
            return

        action = parts[0].lower()
        rest = parts[1] if len(parts) > 1 else ""

        if action == "save":
            save_parts = rest.split(None, 1)
            if len(save_parts) < 2:
                click.echo("Usage: .template save <name> <query>")
                return
            name, query = save_parts
            self._templates[name] = query
            click.echo(f"Template '{name}' saved.")

        elif action == "list":
            if not self._templates:
                click.echo("No templates saved. Use .template save <name> <query>")
                return
            click.echo(f"\n{len(self._templates)} template(s):")
            for name, query in self._templates.items():
                display = query if len(query) <= 60 else query[:57] + "..."
                click.echo(f"  {name}: {display}")
            click.echo()

        elif action == "run":
            run_parts = rest.split()
            if not run_parts:
                click.echo("Usage: .template run <name> param1=val1 ...")
                return
            name = run_parts[0]
            if name not in self._templates:
                available = ", ".join(self._templates) if self._templates else "(none)"
                click.echo(
                    f"No template '{name}'. Available: {available}"
                )
                return
            query = self._templates[name]
            for param in run_parts[1:]:
                if "=" in param:
                    key, val = param.split("=", 1)
                    query = query.replace(f"${key}", val)
            # Warn about unsubstituted parameters
            import re

            remaining = re.findall(r"\$\w+", query)
            if remaining:
                click.echo(
                    f"Warning: unsubstituted parameters: {', '.join(remaining)}"
                )
            click.echo(f"Running: {query}")
            self._execute_query(query)

        elif action == "delete":
            name = rest.strip()
            if name in self._templates:
                del self._templates[name]
                click.echo(f"Template '{name}' deleted.")
            else:
                click.echo(f"No template '{name}'.")

        else:
            click.echo(
                f"Unknown template action '{action}'. Use: save, list, run, delete"
            )

    def do_batch(self, arg: str) -> None:
        """Run queries from a file, one per line (lines starting with -- are skipped).

        Usage::

            .batch queries.cypher

        """
        path = arg.strip()
        if not path:
            click.echo("Usage: .batch <file.cypher>")
            return

        filepath = Path(path)
        if not filepath.exists():
            click.echo(f"File not found: {path}")
            return

        lines = filepath.read_text().splitlines()
        queries = [
            ln.strip()
            for ln in lines
            if ln.strip() and not ln.strip().startswith("--")
        ]

        if not queries:
            click.echo("No queries found in file.")
            return

        click.echo(f"Running {len(queries)} queries from {path}...")
        for i, q in enumerate(queries, 1):
            if q.endswith(";"):
                q = q[:-1].strip()
            click.echo(f"\n[{i}/{len(queries)}] {q}")
            self._execute_query(q)
        click.echo(f"\nBatch complete: {len(queries)} queries.")

    def do_examples(self, arg: str) -> None:
        """Show contextual query examples based on loaded schema.

        If data is loaded, examples use actual entity/relationship labels.
        Otherwise, shows generic Cypher patterns.
        """
        if self._context is not None:
            entities = list(self._context.entity_mapping.mapping.keys())
            rels = list(self._context.relationship_mapping.mapping.keys())
        else:
            entities = []
            rels = []

        click.echo("\nQuery Examples:")

        if entities:
            e = entities[0]
            # Get a property name
            table = self._context.entity_mapping.mapping[e]
            props = [c for c in table.column_names if c != "__ID__"]
            p = props[0] if props else "name"

            click.echo(f"\n  -- Find all {e} nodes")
            click.echo(f"  MATCH (n:{e}) RETURN n.{p}")
            click.echo(f"\n  -- Count {e} nodes")
            click.echo(f"  MATCH (n:{e}) RETURN count(n) AS total")
            click.echo(f"\n  -- Filter by property")
            click.echo(f"  MATCH (n:{e}) WHERE n.{p} IS NOT NULL RETURN n.{p}")
            click.echo(f"\n  -- Create a new {e}")
            click.echo(f"  CREATE (:{e} {{{p}: 'value'}})")

            if len(entities) > 1:
                e2 = entities[1]
                click.echo(f"\n  -- Query multiple types")
                click.echo(
                    f"  MATCH (a:{e}), (b:{e2}) RETURN a, b LIMIT 10"
                )

            if rels:
                r = rels[0]
                click.echo(f"\n  -- Follow relationships")
                click.echo(
                    f"  MATCH (n:{e})-[r:{r}]->(m) RETURN n, r, m LIMIT 10"
                )
        else:
            click.echo("\n  -- Basic node query")
            click.echo("  MATCH (n:Label) RETURN n.property")
            click.echo("\n  -- Filter with WHERE")
            click.echo("  MATCH (n:Label) WHERE n.age > 30 RETURN n.name")
            click.echo("\n  -- Relationship traversal")
            click.echo("  MATCH (a)-[r:REL]->(b) RETURN a, r, b")
            click.echo("\n  -- Aggregation")
            click.echo("  MATCH (n:Label) RETURN n.type, count(n) AS cnt")
            click.echo("\n  -- Create nodes")
            click.echo("  CREATE (:Label {name: 'value', age: 30})")

        click.echo(
            "\n  Tip: Use EXPLAIN <query> to see the execution plan"
            " without running."
        )
        click.echo()

    def do_suggest(self, arg: str) -> None:
        """Show similar query suggestions for a given query.

        Usage::

            .suggest MATCH (p:Person) RETURN p.name

        """
        query = arg.strip()
        if not query:
            click.echo("Usage: .suggest <partial-or-full-query>")
            return

        try:
            from pycypher.cli_intelligence import (
                QuerySuggestionEngine,
                format_suggestions,
            )

            engine = QuerySuggestionEngine.with_common_patterns()
            suggestions = engine.suggest(query)
            output = format_suggestions(suggestions)
            if output:
                click.echo(output)
            else:
                click.echo("No similar query patterns found.")
        except Exception as exc:  # noqa: BLE001 — REPL: display error to user
            click.echo(f"Suggestion error: {exc}")

    def do_hints(self, arg: str) -> None:
        """Show performance hints for a query.

        Usage::

            .hints MATCH (p:Person) RETURN *

        """
        query = arg.strip()
        if not query:
            click.echo("Usage: .hints <query>")
            return

        try:
            from pycypher.cli_intelligence import (
                PerformanceHintEngine,
                format_hints,
            )

            engine = PerformanceHintEngine()
            hints = engine.analyze(query)
            output = format_hints(hints)
            if output:
                click.echo(output)
            else:
                click.echo("No performance issues detected.")
        except Exception as exc:  # noqa: BLE001 — REPL: display error to user
            click.echo(f"Hint analysis error: {exc}")

    def do_load(self, arg: str) -> None:
        """Load entity or relationship data sources mid-session.

        Usage::

            .load entity Person=people.csv
            .load entity Person=people.csv:id_col
            .load rel KNOWS=knows.csv:src:tgt

        After loading, the execution context is rebuilt automatically.
        """
        parts = arg.strip().split(None, 1)
        if len(parts) < 2:
            click.echo(
                "Usage:\n"
                "  .load entity Label=path.csv[:id_col]\n"
                "  .load rel REL=path.csv:src_col:tgt_col",
            )
            return

        kind, spec = parts[0].lower(), parts[1]

        if kind in ("entity", "e"):
            try:
                _parse_entity_spec(spec)  # validate before appending
            except ValueError as exc:
                click.echo(f"Error: {exc}")
                return
            self._entity_specs.append(spec)
            click.echo(f"Added entity spec: {spec}")

        elif kind in ("rel", "relationship", "r"):
            try:
                _parse_rel_spec(spec)  # validate before appending
            except ValueError as exc:
                click.echo(f"Error: {exc}")
                return
            self._rel_specs.append(spec)
            click.echo(f"Added relationship spec: {spec}")

        else:
            click.echo(
                f"Unknown load type {kind!r}. Use 'entity' or 'rel'.",
            )
            return

        # Rebuild context with all specs
        self._build_context()

    def do_clear(self, arg: str) -> None:
        """Clear the screen."""
        click.clear()

    def do_quit(self, arg: str) -> bool:
        """Exit the REPL."""
        return True

    def do_exit(self, arg: str) -> bool:
        """Exit the REPL."""
        return True

    def do_EOF(self, arg: str) -> bool:
        """Handle Ctrl-D."""
        click.echo()
        return True

    # Map .command syntax to do_command methods
    def parseline(
        self,
        line: str,
    ) -> tuple[str | None, str | None, str]:
        """Override to support dot-command syntax (.help, .schema, etc.)."""
        stripped = line.strip()
        if stripped.startswith("."):
            # Convert .command to command for cmd.Cmd dispatch
            rest = stripped[1:]
            parts = rest.split(None, 1)
            command = parts[0] if parts else ""
            arg = parts[1] if len(parts) > 1 else ""
            return command, arg, line
        return super().parseline(line)

    # Cypher keywords for tab completion
    _CYPHER_KEYWORDS: list[str] = [
        "MATCH",
        "OPTIONAL MATCH",
        "RETURN",
        "WHERE",
        "WITH",
        "ORDER BY",
        "LIMIT",
        "SKIP",
        "UNWIND",
        "UNION",
        "UNION ALL",
        "CREATE",
        "MERGE",
        "DELETE",
        "DETACH DELETE",
        "SET",
        "REMOVE",
        "FOREACH",
        "CALL",
        "EXPLAIN",
        "PROFILE",
        "AS",
        "AND",
        "OR",
        "NOT",
        "IN",
        "IS NULL",
        "IS NOT NULL",
        "STARTS WITH",
        "ENDS WITH",
        "CONTAINS",
        "EXISTS",
        "DISTINCT",
        "ASC",
        "DESC",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "TRUE",
        "FALSE",
        "NULL",
    ]

    def completenames(
        self,
        text: str,
        *ignored: object,
    ) -> list[str]:
        """Tab-complete dot-commands, Cypher keywords, functions, and labels."""
        if text.startswith("."):
            # Complete dot-commands
            commands = [f".{c}" for c in self._DOT_COMMANDS]
            return [c for c in commands if c.startswith(text)]

        results: list[str] = []

        # Complete Cypher keywords (case-insensitive match)
        upper = text.upper()
        results.extend(kw for kw in self._CYPHER_KEYWORDS if kw.startswith(upper))

        # Complete function names
        try:
            from pycypher.scalar_functions import ScalarFunctionRegistry

            registry = ScalarFunctionRegistry.get_instance()
            lower = text.lower()
            results.extend(
                f"{fn}("
                for fn in sorted(registry._functions.keys())
                if fn.lower().startswith(lower)
            )
        except Exception:  # noqa: BLE001 — best-effort tab completion
            _logger.debug("Tab completion failed for %r", text, exc_info=True)

        # Complete entity/relationship type labels (after : in patterns)
        if self._context is not None:
            for label in self._context.entity_mapping.mapping:
                if label.upper().startswith(upper) or label.startswith(text):
                    results.append(label)
            for label in self._context.relationship_mapping.mapping:
                if label.upper().startswith(upper) or label.startswith(text):
                    results.append(label)

        if results:
            return results
        return super().completenames(text, *ignored)

    def completedefault(
        self,
        text: str,
        line: str,
        begidx: int,
        endidx: int,
    ) -> list[str]:
        """Tab-complete property names after a dot (e.g., ``p.`` → ``p.name``)."""
        if self._context is None or "." not in text:
            return []

        prefix, _, partial = text.rpartition(".")
        if not prefix:
            return []

        # Collect all property names from all entity and relationship types
        properties: set[str] = set()
        for table in self._context.entity_mapping.mapping.values():
            for col in table.column_names:
                if col != "__ID__":
                    properties.add(col)
        for table in self._context.relationship_mapping.mapping.values():
            for col in table.column_names:
                if col not in {"__ID__", "__SOURCE__", "__TARGET__"}:
                    properties.add(col)

        return [
            f"{prefix}.{p}"
            for p in sorted(properties)
            if p.lower().startswith(partial.lower())
        ]

    def emptyline(self) -> bool:
        """Do nothing on empty line (don't repeat last command)."""
        return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_entity_spec(spec: str) -> tuple[str, str, str | None]:
    """Parse ``Label=path.csv`` or ``Label=path.csv:id_col``."""
    if "=" not in spec:
        msg = f"expected 'Label=path', got {spec!r}"
        raise ValueError(msg)
    label, rest = spec.split("=", 1)
    parts = rest.split(":")
    path = parts[0]
    id_col = parts[1] if len(parts) > 1 else None
    return label.strip(), path.strip(), id_col


def _parse_rel_spec(spec: str) -> tuple[str, str, str, str]:
    """Parse ``REL=path.csv:src_col:tgt_col``."""
    if "=" not in spec:
        msg = f"expected 'REL=path:src:tgt', got {spec!r}"
        raise ValueError(msg)
    rel_type, rest = spec.split("=", 1)
    parts = rest.split(":")
    if len(parts) < 3:
        msg = (
            f"relationship spec needs 'REL=path:src_col:tgt_col', got {spec!r}"
        )
        raise ValueError(msg)
    return (
        rel_type.strip(),
        parts[0].strip(),
        parts[1].strip(),
        parts[2].strip(),
    )


def _looks_incomplete(line: str) -> bool:
    """Heuristic: does this line look like an incomplete Cypher query?"""
    upper = line.upper().strip()
    # Lines starting with keywords that expect continuation
    _CONTINUING = {"MATCH", "WITH", "UNWIND", "OPTIONAL", "WHERE", "ORDER"}
    # If it ends with a keyword that expects more, it's incomplete
    last_word = upper.split()[-1] if upper.split() else ""
    if last_word in _CONTINUING:
        return True
    # If it starts with MATCH but has no RETURN, likely incomplete
    if "MATCH" in upper and "RETURN" not in upper:
        return True
    return False


def _display_result(
    result: pd.DataFrame | None, *, fmt: str = "table"
) -> None:
    """Display a query result DataFrame in the chosen format."""
    if result is None:
        return

    import pandas as pd

    if not isinstance(result, pd.DataFrame):
        click.echo(f"Result: {result}")
        return

    if result.empty:
        click.echo("(no rows returned)")
        return

    try:
        max_rows = int(os.environ.get("PYCYPHER_REPL_MAX_ROWS", "50"))
    except (ValueError, TypeError):
        max_rows = 50

    display_df = result.head(max_rows) if len(result) > max_rows else result

    if fmt == "csv":
        click.echo(display_df.to_csv(index=False))
    elif fmt == "json":
        click.echo(display_df.to_json(orient="records", indent=2))
    else:
        click.echo(display_df.to_string(index=False))

    if len(result) > max_rows:
        click.echo(
            f"... ({len(result) - max_rows} more rows, set PYCYPHER_REPL_MAX_ROWS to show more)",
        )
