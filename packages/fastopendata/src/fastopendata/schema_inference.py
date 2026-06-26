"""
Infer graph schema from fod_input_configs.yaml.template.

Extracts node labels, relationship types with source/target labels, and
computed properties with inferred types (those written by Cypher SET
statements) without touching any data files.

Type inference priority for each property
-----------------------------------------
1. Explicit ``property_types`` block on the query definition in the template
2. Cypher expression analysis (COUNT → int, AVG/toFloat/division → float,
   comparison → bool)
3. Property name conventions (*_count* → int, pct_* → float, over_* → bool,
   *_description → str, *_estimate* / num_* → int, *_avg → float)
4. Default: float

Usage
-----
As a module::

    from fastopendata.schema_inference import infer_schema
    schema = infer_schema()
    # schema["node_types"]["PUMA"]["pop_estimate_1yr"] == "int"

As a CLI::

    uv run python -m fastopendata.schema_inference
    uv run python -m fastopendata.schema_inference path/to/template.yaml
"""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path

import yaml

_TEMPLATE_DEFAULT = (
    Path(__file__).parent.parent.parent / "fod_input_configs.yaml.template"
)


# ---------------------------------------------------------------------------
# Template loading
# ---------------------------------------------------------------------------

def _load_template(path: Path) -> dict:
    text = path.read_text()
    text = re.sub(r"\$\{[^}]+\}", "PLACEHOLDER", text)
    return yaml.safe_load(text)


# ---------------------------------------------------------------------------
# Entity / relationship helpers
# ---------------------------------------------------------------------------

def _build_id_col_maps(entities: list[dict]) -> tuple[dict, dict]:
    """
    Returns:
      by_uri_col : (uri_basename, id_col) -> entity_type
      by_col     : id_col -> entity_type  (last definition wins)
    """
    by_uri_col: dict[tuple[str, str], str] = {}
    by_col: dict[str, str] = {}
    for e in entities:
        etype = e["entity_type"]
        id_col = e.get("id_col")
        if not id_col:
            continue
        uri_base = Path(e.get("uri", "")).name
        by_uri_col[(uri_base, id_col)] = etype
        by_col[id_col] = etype
    return by_uri_col, by_col


# ---------------------------------------------------------------------------
# Cypher parsing helpers
# ---------------------------------------------------------------------------

def _var_label_map(cypher: str) -> dict[str, str]:
    """Return {variable: label} for every (var:Label) in a Cypher string."""
    return {m.group(1): m.group(2) for m in re.finditer(r"\((\w+):(\w+)\)", cypher)}


def _alias_expr_map(cypher: str) -> dict[str, str]:
    """Return {alias: expression} for every EXPR AS alias in a Cypher string."""
    alias_map: dict[str, str] = {}
    for m in re.finditer(
        r"([\w().,'\[\] ]+?)\s+AS\s+(\w+)", cypher, re.IGNORECASE
    ):
        alias_map[m.group(2)] = m.group(1).strip()
    return alias_map


def _extract_rel_triples(
    cypher: str, var_map: dict[str, str]
) -> set[tuple[str, str, str]]:
    """Extract (src_label, rel_type, tgt_label) from MATCH patterns."""
    triples: set[tuple[str, str, str]] = set()
    for m in re.finditer(r"\((\w+)\)-\[:(\w+)\]->\((\w+)\)", cypher):
        src, rel, tgt = m.group(1), m.group(2), m.group(3)
        if src in var_map and tgt in var_map:
            triples.add((var_map[src], rel, var_map[tgt]))
    for m in re.finditer(r"\((\w+)\)<-\[:(\w+)\]-\((\w+)\)", cypher):
        tgt, rel, src = m.group(1), m.group(2), m.group(3)
        if src in var_map and tgt in var_map:
            triples.add((var_map[src], rel, var_map[tgt]))
    return triples


# ---------------------------------------------------------------------------
# Type inference
# ---------------------------------------------------------------------------

def _infer_type(prop: str, rhs: str, alias_map: dict[str, str]) -> str:
    """
    Return a Python type string ('int', 'float', 'str', 'bool') for a Cypher
    SET assignment ``var.prop = rhs``.
    """
    # Resolve alias chain to the originating expression
    expr = rhs.strip()
    seen: set[str] = set()
    while expr in alias_map and expr not in seen:
        seen.add(expr)
        expr = alias_map[expr].strip()

    eu = expr.upper()

    # --- expression-based rules ---
    if re.search(r"\bCOUNT\s*\(", eu):
        return "int"
    if re.search(r"\bAVG\s*\(", eu):
        return "float"
    if re.search(r"\bTOFLOAT\s*\(", eu):
        return "float"
    if "/" in expr and re.search(r"\bTOFLOAT\b|\bFLOAT\b", eu):
        return "float"
    # Bare comparison produces a boolean
    if re.search(r"\s[><=!]+\s", eu) and not re.search(r"\b(SET|WHERE)\b", eu):
        return "bool"
    if re.search(r"\bSUM\s*\(", eu):
        # SUM of _count properties is int; SUM of floats stays float
        inner = re.search(r"\bSUM\s*\(([^)]+)\)", eu)
        if inner and re.search(r"_COUNT|_ESTIMATE", inner.group(1)):
            return "int"
        return "int"  # default SUM → int for this config

    # --- property name conventions ---
    p = prop.lower()
    if re.search(r"_count(?:_|$)", p):
        return "int"
    if "_estimate" in p:
        return "int"
    if p.startswith("num_"):
        return "int"
    if p.startswith("pct_") or p.endswith("_avg"):
        return "float"
    if p.startswith("over_"):
        return "bool"
    if p.endswith("_description"):
        return "str"

    return "float"


def _extract_set_props(
    cypher: str,
    var_map: dict[str, str],
    alias_map: dict[str, str],
    type_overrides: dict[str, str],
    description: str = "",
) -> list[tuple[str, str, str, str]]:
    """
    Return [(node_label, property_name, type_str, description), ...] for every
    left-hand side assignment in SET clauses.
    """
    results = []
    for m in re.finditer(
        r"\bSET\b(.*?)(?=\bWHERE\b|\bWITH\b|\bMATCH\b|\bRETURN\b|$)",
        cypher,
        re.IGNORECASE,
    ):
        set_clause = m.group(1)
        # Split on comma-separated assignments; match var.prop = <rhs>
        for pm in re.finditer(r"\b(\w+)\.(\w+)\s*=(?!=)\s*([^,]+)", set_clause):
            var, prop, rhs = pm.group(1), pm.group(2), pm.group(3).strip()
            if var not in var_map:
                continue
            label = var_map[var]
            if prop in type_overrides:
                ptype = type_overrides[prop]
            else:
                ptype = _infer_type(prop, rhs, alias_map)
            results.append((label, prop, ptype, description))
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def infer_schema(template_path: Path | None = None) -> dict:
    """
    Parse the pipeline config template and return a schema dict with keys:

    ``node_types``
        ``{label: {property_name: type_str}}`` — computed properties only
        (those written by Cypher SET statements; raw source columns excluded).
        Type strings are ``'int'``, ``'float'``, ``'str'``, or ``'bool'``.

    ``relationship_types``
        ``{rel_type: [{"from": src, "to": tgt}, ...]}``
    """
    if template_path is None:
        template_path = _TEMPLATE_DEFAULT

    data = _load_template(template_path)
    entities: list[dict] = data["sources"]["entities"]
    relationships: list[dict] = data["sources"].get("relationships", [])
    queries: list[dict] = data.get("queries", [])

    # node labels — ensure every entity_type exists as a key
    node_props: dict[str, dict[str, dict]] = defaultdict(dict)
    for e in entities:
        node_props[e["entity_type"]]

    # relationship source/target resolution
    by_uri_col, by_col = _build_id_col_maps(entities)
    rel_pairs: dict[str, set[tuple[str, str]]] = defaultdict(set)

    for r in relationships:
        rel_type = r["relationship_type"]
        src_label = r.get("source_entity_type")
        tgt_label = r.get("target_entity_type")
        if not src_label or not tgt_label:
            uri_base = Path(r.get("uri", "")).name
            src_col, tgt_col = r.get("source_col"), r.get("target_col")
            if not src_label:
                src_label = by_uri_col.get((uri_base, src_col)) or by_col.get(src_col)
            if not tgt_label:
                tgt_label = by_uri_col.get((uri_base, tgt_col)) or by_col.get(tgt_col)
        if src_label and tgt_label:
            rel_pairs[rel_type].add((src_label, tgt_label))

    # Cypher query parsing
    for q in queries:
        inline = q.get("inline", "")
        if not inline:
            continue
        type_overrides: dict[str, str] = q.get("property_types", {})
        description: str = q.get("description", "")
        var_map = _var_label_map(inline)
        alias_map = _alias_expr_map(inline)

        for src, rel, tgt in _extract_rel_triples(inline, var_map):
            rel_pairs[rel].add((src, tgt))

        for label, prop, ptype, desc in _extract_set_props(
            inline, var_map, alias_map, type_overrides, description
        ):
            node_props[label][prop] = {"type": ptype, "description": desc}

    return {
        "node_types": {
            label: dict(sorted(props.items()))
            for label, props in sorted(node_props.items())
        },
        "relationship_types": {
            rel_type: sorted(
                [{"from": src, "to": tgt} for src, tgt in pairs],
                key=lambda x: (x["from"], x["to"]),
            )
            for rel_type, pairs in sorted(rel_pairs.items())
        },
    }


def main() -> None:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    print(json.dumps(infer_schema(path), indent=2))


if __name__ == "__main__":
    main()
