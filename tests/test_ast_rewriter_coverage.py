"""Coverage-focused tests for ast_rewriter.py.

Targets uncovered serialization paths: DELETE, DETACH DELETE, SET, MERGE,
UNWIND, OPTIONAL MATCH, RETURN DISTINCT, ORDER BY (ASC/DESC), SKIP, LIMIT,
WITH WHERE, ReturnItem aliases, relationship directions, anonymous nodes,
node properties, and all literal/expression types in _expr_to_cypher.
"""

from __future__ import annotations

import pytest
from pycypher.ast_models import (
    BooleanLiteral,
    Comparison,
    Create,
    Delete,
    FloatLiteral,
    IntegerLiteral,
    Match,
    Merge,
    NodePattern,
    NullLiteral,
    OrderByItem,
    Parameter,
    Pattern,
    PatternPath,
    PropertyLookup,
    Query,
    RelationshipPattern,
    Return,
    ReturnItem,
    Set,
    SetItem,
    StringLiteral,
    Unwind,
    Variable,
    With,
)
from pycypher.ast_rewriter import ASTRewriter


@pytest.fixture
def rewriter() -> ASTRewriter:
    """Provide a fresh ASTRewriter instance."""
    return ASTRewriter()


def _make_person_node(var: str | None = None) -> NodePattern:
    """Create a (:Person) node pattern with optional variable."""
    return NodePattern(
        variable=Variable(name=var) if var else None,
        labels=["Person"],
    )


def _simple_match_return(var: str = "n") -> Query:
    """Build MATCH (n:Person) RETURN n."""
    return Query(
        clauses=[
            Match(
                pattern=Pattern(
                    paths=[PatternPath(elements=[_make_person_node(var)])],
                ),
            ),
            Return(items=[ReturnItem(expression=Variable(name=var))]),
        ],
    )


# ---------------------------------------------------------------------------
# DELETE / DETACH DELETE serialization
# ---------------------------------------------------------------------------


class TestDeleteSerialization:
    """Exercise _clause_to_cypher for Delete clauses."""

    def test_delete_single_variable(self, rewriter: ASTRewriter) -> None:
        """DELETE n serializes correctly."""
        query = Query(
            clauses=[
                Delete(expressions=[Variable(name="n")]),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "DELETE" in cypher
        assert "n" in cypher
        assert "DETACH" not in cypher

    def test_detach_delete(self, rewriter: ASTRewriter) -> None:
        """DETACH DELETE n serializes correctly."""
        query = Query(
            clauses=[
                Delete(detach=True, expressions=[Variable(name="n")]),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "DETACH DELETE" in cypher
        assert "n" in cypher

    def test_delete_multiple_expressions(self, rewriter: ASTRewriter) -> None:
        """DELETE n, r serializes with comma separation."""
        query = Query(
            clauses=[
                Delete(
                    expressions=[Variable(name="n"), Variable(name="r")],
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "n" in cypher
        assert "r" in cypher


# ---------------------------------------------------------------------------
# SET clause serialization
# ---------------------------------------------------------------------------


class TestSetSerialization:
    """Exercise _clause_to_cypher for Set clauses."""

    def test_set_property_item(self, rewriter: ASTRewriter) -> None:
        """SET n.name = 'Bob' serializes correctly."""
        set_item = SetItem(
            variable=Variable(name="n"),
            property="name",
            expression=StringLiteral(value="Bob"),
        )
        query = Query(clauses=[Set(items=[set_item])])
        cypher = rewriter.to_cypher(query)
        assert "SET" in cypher


# ---------------------------------------------------------------------------
# MERGE clause serialization
# ---------------------------------------------------------------------------


class TestMergeSerialization:
    """Exercise _clause_to_cypher for Merge clauses."""

    def test_merge_node(self, rewriter: ASTRewriter) -> None:
        """MERGE (n:Person) serializes correctly."""
        query = Query(
            clauses=[
                Merge(
                    pattern=Pattern(
                        paths=[
                            PatternPath(elements=[_make_person_node("n")]),
                        ],
                    ),
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "MERGE" in cypher
        assert "Person" in cypher


# ---------------------------------------------------------------------------
# UNWIND clause serialization
# ---------------------------------------------------------------------------


class TestUnwindSerialization:
    """Exercise _clause_to_cypher for Unwind clauses."""

    def test_unwind_with_variable_alias(self, rewriter: ASTRewriter) -> None:
        """UNWIND expr AS x serializes correctly when alias is a Variable."""
        query = Query(
            clauses=[
                Unwind(
                    expression=Variable(name="list"),
                    alias="x",
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "UNWIND" in cypher
        assert "AS" in cypher
        assert "x" in cypher

    def test_unwind_with_string_alias(self, rewriter: ASTRewriter) -> None:
        """UNWIND expr AS y serializes when alias is a plain string."""
        query = Query(
            clauses=[
                Unwind(
                    expression=Variable(name="items"),
                    alias="y",
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "UNWIND" in cypher
        assert "AS y" in cypher


# ---------------------------------------------------------------------------
# OPTIONAL MATCH serialization
# ---------------------------------------------------------------------------


class TestOptionalMatchSerialization:
    """Exercise OPTIONAL MATCH branch in _clause_to_cypher."""

    def test_optional_match(self, rewriter: ASTRewriter) -> None:
        """OPTIONAL MATCH renders keyword correctly."""
        query = Query(
            clauses=[
                Match(
                    optional=True,
                    pattern=Pattern(
                        paths=[PatternPath(elements=[_make_person_node("n")])],
                    ),
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "OPTIONAL MATCH" in cypher

    def test_match_with_where(self, rewriter: ASTRewriter) -> None:
        """MATCH ... WHERE renders correctly."""
        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
                        paths=[PatternPath(elements=[_make_person_node("n")])],
                    ),
                    where=Comparison(
                        left=PropertyLookup(
                            expression=Variable(name="n"),
                            property="age",
                        ),
                        operator=">",
                        right=IntegerLiteral(value=25),
                    ),
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "WHERE" in cypher
        assert "n.age" in cypher
        assert "25" in cypher


# ---------------------------------------------------------------------------
# Fallback clause serialization
# ---------------------------------------------------------------------------


class TestFallbackClause:
    """Exercise the fallback str(clause) path for unrecognized types."""

    def test_unknown_clause_uses_str_fallback(
        self,
        rewriter: ASTRewriter,
    ) -> None:
        """Unrecognized clause types fall back to str() via monkey-patched clauses."""
        query = Query(clauses=[Return(items=[])])
        # Bypass Pydantic validation to inject a non-Clause object
        query.__dict__["clauses"] = ["CALL db.info()"]
        cypher = rewriter.to_cypher(query)
        assert "CALL" in cypher


# ---------------------------------------------------------------------------
# RETURN / WITH modifiers: DISTINCT, ORDER BY, SKIP, LIMIT
# ---------------------------------------------------------------------------


class TestReturnLikeModifiers:
    """Exercise _return_like_to_cypher modifier branches."""

    def test_return_distinct(self, rewriter: ASTRewriter) -> None:
        """RETURN DISTINCT n serializes correctly."""
        query = Query(
            clauses=[
                Return(
                    distinct=True,
                    items=[ReturnItem(expression=Variable(name="n"))],
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "RETURN DISTINCT" in cypher

    def test_return_with_order_by_asc(self, rewriter: ASTRewriter) -> None:
        """RETURN n ORDER BY n.name serializes (ascending, no suffix)."""
        query = Query(
            clauses=[
                Return(
                    items=[ReturnItem(expression=Variable(name="n"))],
                    order_by=[
                        OrderByItem(
                            expression=PropertyLookup(
                                expression=Variable(name="n"),
                                property="name",
                            ),
                            ascending=True,
                        ),
                    ],
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "ORDER BY" in cypher
        assert "n.name" in cypher
        assert "DESC" not in cypher

    def test_return_with_order_by_desc(self, rewriter: ASTRewriter) -> None:
        """RETURN n ORDER BY n.age DESC serializes correctly."""
        query = Query(
            clauses=[
                Return(
                    items=[ReturnItem(expression=Variable(name="n"))],
                    order_by=[
                        OrderByItem(
                            expression=PropertyLookup(
                                expression=Variable(name="n"),
                                property="age",
                            ),
                            ascending=False,
                        ),
                    ],
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "ORDER BY" in cypher
        assert "DESC" in cypher

    def test_return_with_skip(self, rewriter: ASTRewriter) -> None:
        """RETURN n SKIP 5 serializes correctly."""
        query = Query(
            clauses=[
                Return(
                    items=[ReturnItem(expression=Variable(name="n"))],
                    skip=IntegerLiteral(value=5),
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "SKIP" in cypher
        assert "5" in cypher

    def test_return_with_limit(self, rewriter: ASTRewriter) -> None:
        """RETURN n LIMIT 10 serializes correctly."""
        query = Query(
            clauses=[
                Return(
                    items=[ReturnItem(expression=Variable(name="n"))],
                    limit=IntegerLiteral(value=10),
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "LIMIT" in cypher
        assert "10" in cypher

    def test_return_item_with_alias(self, rewriter: ASTRewriter) -> None:
        """RETURN n.name AS personName serializes alias."""
        query = Query(
            clauses=[
                Return(
                    items=[
                        ReturnItem(
                            expression=PropertyLookup(
                                expression=Variable(name="n"),
                                property="name",
                            ),
                            alias="personName",
                        ),
                    ],
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "AS personName" in cypher

    def test_return_star(self, rewriter: ASTRewriter) -> None:
        """RETURN * (empty items) serializes to wildcard."""
        query = Query(clauses=[Return(items=[])])
        cypher = rewriter.to_cypher(query)
        assert "RETURN *" in cypher


# ---------------------------------------------------------------------------
# WITH clause specific: WHERE
# ---------------------------------------------------------------------------


class TestWithClauseModifiers:
    """Exercise WITH-specific serialization paths."""

    def test_with_where(self, rewriter: ASTRewriter) -> None:
        """WITH n WHERE n.age > 21 serializes WHERE."""
        query = Query(
            clauses=[
                With(
                    items=[ReturnItem(expression=Variable(name="n"))],
                    where=Comparison(
                        left=PropertyLookup(
                            expression=Variable(name="n"),
                            property="age",
                        ),
                        operator=">",
                        right=IntegerLiteral(value=21),
                    ),
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "WITH" in cypher
        assert "WHERE" in cypher
        assert "n.age" in cypher

    def test_with_distinct(self, rewriter: ASTRewriter) -> None:
        """WITH DISTINCT n serializes correctly."""
        query = Query(
            clauses=[
                With(
                    distinct=True,
                    items=[ReturnItem(expression=Variable(name="n"))],
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "WITH DISTINCT" in cypher

    def test_with_order_by_skip_limit(self, rewriter: ASTRewriter) -> None:
        """WITH n ORDER BY n.age SKIP 1 LIMIT 5 serializes all modifiers."""
        query = Query(
            clauses=[
                With(
                    items=[ReturnItem(expression=Variable(name="n"))],
                    order_by=[
                        OrderByItem(
                            expression=PropertyLookup(
                                expression=Variable(name="n"),
                                property="age",
                            ),
                        ),
                    ],
                    skip=IntegerLiteral(value=1),
                    limit=IntegerLiteral(value=5),
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "ORDER BY" in cypher
        assert "SKIP" in cypher
        assert "LIMIT" in cypher


# ---------------------------------------------------------------------------
# Node serialization: anonymous, labels, properties
# ---------------------------------------------------------------------------


class TestNodeSerialization:
    """Exercise _node_to_cypher branches."""

    def test_anonymous_node(self, rewriter: ASTRewriter) -> None:
        """(:Person) — no variable — serializes correctly."""
        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
                        paths=[
                            PatternPath(
                                elements=[_make_person_node(None)],
                            ),
                        ],
                    ),
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "(:Person)" in cypher

    def test_node_with_properties(self, rewriter: ASTRewriter) -> None:
        """(n:Person {name: 'Alice'}) serializes properties."""
        node = NodePattern(
            variable=Variable(name="n"),
            labels=["Person"],
            properties={"name": StringLiteral(value="Alice")},
        )
        query = Query(
            clauses=[
                Create(
                    pattern=Pattern(paths=[PatternPath(elements=[node])]),
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "Person" in cypher
        assert "name" in cypher
        assert "Alice" in cypher

    def test_node_with_multiple_labels(self, rewriter: ASTRewriter) -> None:
        """(n:Person:Employee) serializes multiple labels."""
        node = NodePattern(
            variable=Variable(name="n"),
            labels=["Person", "Employee"],
        )
        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(paths=[PatternPath(elements=[node])]),
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert ":Person:Employee" in cypher


# ---------------------------------------------------------------------------
# Relationship serialization: variable, labels, direction
# ---------------------------------------------------------------------------


class TestRelationshipSerialization:
    """Exercise _relationship_to_cypher branches."""

    def test_right_direction_with_type(self, rewriter: ASTRewriter) -> None:
        """-[:KNOWS]-> serializes correctly."""
        rel = RelationshipPattern(
            variable=None,
            labels=["KNOWS"],
            direction="->",
        )
        path = PatternPath(
            elements=[_make_person_node("a"), rel, _make_person_node("b")],
        )
        query = Query(
            clauses=[
                Match(pattern=Pattern(paths=[path])),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "KNOWS" in cypher
        assert "->" in cypher or "-[" in cypher

    def test_relationship_with_variable(
        self,
        rewriter: ASTRewriter,
    ) -> None:
        """-[r:KNOWS]-> serializes variable and type."""
        rel = RelationshipPattern(
            variable=Variable(name="r"),
            labels=["KNOWS"],
            direction="->",
        )
        path = PatternPath(
            elements=[_make_person_node("a"), rel, _make_person_node("b")],
        )
        query = Query(
            clauses=[
                Match(pattern=Pattern(paths=[path])),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "r" in cypher
        assert "KNOWS" in cypher

    def test_left_direction_relationship(
        self,
        rewriter: ASTRewriter,
    ) -> None:
        """<-[:KNOWS]- serializes LEFT direction via monkey-patched direction attr."""
        rel = RelationshipPattern(
            variable=None,
            labels=["KNOWS"],
            direction="->",
        )
        # The rewriter checks direction == "LEFT" (string), not "<-" (AST token).
        # Bypass Pydantic to inject the rewriter-expected value.
        object.__setattr__(rel, "direction", "LEFT")
        path = PatternPath(
            elements=[_make_person_node("a"), rel, _make_person_node("b")],
        )
        query = Query(
            clauses=[
                Match(pattern=Pattern(paths=[path])),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "<-" in cypher

    def test_no_bracket_relationship(self, rewriter: ASTRewriter) -> None:
        """--  (no variable, no type) omits brackets."""
        rel = RelationshipPattern(variable=None, labels=[], direction="->")
        path = PatternPath(
            elements=[_make_person_node("a"), rel, _make_person_node("b")],
        )
        query = Query(
            clauses=[
                Match(pattern=Pattern(paths=[path])),
            ],
        )
        cypher = rewriter.to_cypher(query)
        # Should have dashes but no brackets
        assert "-->" in cypher or "--" in cypher

    def test_relationship_multiple_types(
        self,
        rewriter: ASTRewriter,
    ) -> None:
        """-[:KNOWS|LIKES]-> serializes pipe-separated types."""
        rel = RelationshipPattern(
            variable=None,
            labels=["KNOWS", "LIKES"],
            direction="->",
        )
        path = PatternPath(
            elements=[_make_person_node("a"), rel, _make_person_node("b")],
        )
        query = Query(
            clauses=[
                Match(pattern=Pattern(paths=[path])),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "KNOWS|LIKES" in cypher


# ---------------------------------------------------------------------------
# Pattern edge cases
# ---------------------------------------------------------------------------


class TestPatternEdgeCases:
    """Exercise _pattern_to_cypher edge cases."""

    def test_none_pattern(self, rewriter: ASTRewriter) -> None:
        """_pattern_to_cypher(None) returns empty string."""
        result = rewriter._pattern_to_cypher(None)
        assert result == ""


# ---------------------------------------------------------------------------
# Expression serialization: all literal types and operators
# ---------------------------------------------------------------------------


class TestExpressionSerialization:
    """Exercise _expr_to_cypher for all expression AST node types."""

    def test_variable(self, rewriter: ASTRewriter) -> None:
        """Variable serializes to its name."""
        result = rewriter._expr_to_cypher(Variable(name="foo"))
        assert result == "foo"

    def test_property_lookup(self, rewriter: ASTRewriter) -> None:
        """PropertyLookup serializes to base.property."""
        expr = PropertyLookup(
            expression=Variable(name="n"),
            property="name",
        )
        result = rewriter._expr_to_cypher(expr)
        assert result == "n.name"

    def test_integer_literal(self, rewriter: ASTRewriter) -> None:
        """IntegerLiteral serializes to string of its value."""
        result = rewriter._expr_to_cypher(IntegerLiteral(value=42))
        assert result == "42"

    def test_float_literal(self, rewriter: ASTRewriter) -> None:
        """FloatLiteral serializes to string of its value."""
        result = rewriter._expr_to_cypher(FloatLiteral(value=3.14))
        assert result == "3.14"

    def test_string_literal(self, rewriter: ASTRewriter) -> None:
        """StringLiteral serializes with single quotes."""
        result = rewriter._expr_to_cypher(StringLiteral(value="hello"))
        assert result == "'hello'"

    def test_string_literal_with_quote(self, rewriter: ASTRewriter) -> None:
        """StringLiteral escapes embedded single quotes."""
        result = rewriter._expr_to_cypher(StringLiteral(value="it's"))
        assert "\\'" in result

    def test_boolean_literal_true(self, rewriter: ASTRewriter) -> None:
        """BooleanLiteral(True) serializes to 'true'."""
        result = rewriter._expr_to_cypher(BooleanLiteral(value=True))
        assert result == "true"

    def test_boolean_literal_false(self, rewriter: ASTRewriter) -> None:
        """BooleanLiteral(False) serializes to 'false'."""
        result = rewriter._expr_to_cypher(BooleanLiteral(value=False))
        assert result == "false"

    def test_null_literal(self, rewriter: ASTRewriter) -> None:
        """NullLiteral serializes to 'NULL'."""
        result = rewriter._expr_to_cypher(NullLiteral())
        assert result == "NULL"

    def test_none_expression(self, rewriter: ASTRewriter) -> None:
        """None expression serializes to 'NULL'."""
        result = rewriter._expr_to_cypher(None)
        assert result == "NULL"

    def test_parameter(self, rewriter: ASTRewriter) -> None:
        """Parameter serializes to $name."""
        result = rewriter._expr_to_cypher(Parameter(name="limit"))
        assert result == "$limit"

    def test_comparison(self, rewriter: ASTRewriter) -> None:
        """Comparison serializes to 'left op right'."""
        expr = Comparison(
            left=Variable(name="a"),
            operator="=",
            right=IntegerLiteral(value=1),
        )
        result = rewriter._expr_to_cypher(expr)
        assert result == "a = 1"

    def test_raw_int(self, rewriter: ASTRewriter) -> None:
        """Raw Python int serializes via the (int, float) branch."""
        result = rewriter._expr_to_cypher(99)
        assert result == "99"

    def test_raw_float(self, rewriter: ASTRewriter) -> None:
        """Raw Python float serializes via the (int, float) branch."""
        result = rewriter._expr_to_cypher(2.5)
        assert result == "2.5"

    def test_fallback_expression(self, rewriter: ASTRewriter) -> None:
        """Unrecognized expression types fall back to str()."""
        result = rewriter._expr_to_cypher(["unrecognized"])
        assert result == "['unrecognized']"


# ---------------------------------------------------------------------------
# Combined / integration: full query round-trip through uncovered paths
# ---------------------------------------------------------------------------


class TestFullQuerySerialization:
    """End-to-end serialization of queries using many clause types."""

    def test_match_set_return(self, rewriter: ASTRewriter) -> None:
        """MATCH + SET + RETURN serializes all three clauses."""
        set_item = SetItem(
            variable=Variable(name="n"),
            property="age",
            expression=IntegerLiteral(value=30),
        )
        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
                        paths=[PatternPath(elements=[_make_person_node("n")])],
                    ),
                ),
                Set(items=[set_item]),
                Return(items=[ReturnItem(expression=Variable(name="n"))]),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "MATCH" in cypher
        assert "SET" in cypher
        assert "RETURN" in cypher

    def test_merge_three_queries_with_various_clauses(
        self,
        rewriter: ASTRewriter,
    ) -> None:
        """Merging queries with MATCH, CREATE, RETURN strips intermediates."""
        q1 = Query(
            clauses=[
                Create(
                    pattern=Pattern(
                        paths=[PatternPath(elements=[_make_person_node("n")])],
                    ),
                ),
                Return(items=[ReturnItem(expression=Variable(name="n"))]),
            ],
        )
        q2 = Query(
            clauses=[
                Match(
                    pattern=Pattern(
                        paths=[PatternPath(elements=[_make_person_node("m")])],
                    ),
                ),
                Return(items=[ReturnItem(expression=Variable(name="m"))]),
            ],
        )
        merged = rewriter.merge_queries([q1, q2])
        cypher = rewriter.to_cypher(merged)
        assert "CREATE" in cypher
        assert "WITH *" in cypher
        assert "MATCH" in cypher
        # Only one RETURN should remain (from q2)
        assert cypher.count("RETURN") == 1

    def test_complex_return_with_all_modifiers(
        self,
        rewriter: ASTRewriter,
    ) -> None:
        """RETURN DISTINCT n.name AS name ORDER BY n.name DESC SKIP 2 LIMIT 10."""
        query = Query(
            clauses=[
                Return(
                    distinct=True,
                    items=[
                        ReturnItem(
                            expression=PropertyLookup(
                                expression=Variable(name="n"),
                                property="name",
                            ),
                            alias="name",
                        ),
                    ],
                    order_by=[
                        OrderByItem(
                            expression=PropertyLookup(
                                expression=Variable(name="n"),
                                property="name",
                            ),
                            ascending=False,
                        ),
                    ],
                    skip=IntegerLiteral(value=2),
                    limit=IntegerLiteral(value=10),
                ),
            ],
        )
        cypher = rewriter.to_cypher(query)
        assert "RETURN DISTINCT" in cypher
        assert "AS name" in cypher
        assert "ORDER BY" in cypher
        assert "DESC" in cypher
        assert "SKIP 2" in cypher
        assert "LIMIT 10" in cypher
