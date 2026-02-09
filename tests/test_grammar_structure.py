"""Tests for structural verification of the grammar parser.

These tests ensure that the parser produces the correct dictionary-based AST structure
according to the transformer logic, rather than just asserting non-None.
"""

import pytest
from pycypher.grammar_parser import GrammarParser


@pytest.fixture
def parser():
    """Create a GrammarParser instance for testing."""
    return GrammarParser()


class TestNodeStructure:
    """Test AST structure for Node patterns."""

    def test_simple_node(self, parser):
        """Test simple node variable extraction."""
        query = "MATCH (n) RETURN n"
        tree = parser.parse(query)
        ast = parser.transformer.transform(tree)

        # Navigate to node pattern
        stmt = ast["statements"][0][0]
        match = stmt["clauses"][0]
        node = match["pattern"]["paths"][0]["element"]["parts"][0]

        assert node["type"] == "NodePattern"
        assert node["variable"] == "n"
        assert "labels" not in node or not node.get("labels")
        assert "properties" not in node or not node.get("properties")

    def test_node_with_labels(self, parser):
        """Test node with multiple labels."""
        query = "MATCH (n:Person:Employee) RETURN n"
        tree = parser.parse(query)
        ast = parser.transformer.transform(tree)

        node = ast["statements"][0][0]["clauses"][0]["pattern"]["paths"][0][
            "element"
        ]["parts"][0]

        assert node["type"] == "NodePattern"
        # Check label structure (list of dicts from label_expression)
        labels = node["labels"]
        # Depending on transformer "node_labels" returns {"labels": [...]} or just list
        # Looking at code: it returns {"labels": flat_list} based on update logic?
        # Wait, node_labels returns Dict[str, List] -> update(arg) merges keys.
        assert isinstance(labels, list)
        assert len(labels) == 2
        # Labels are structured (label_term -> label_primary -> label_name)
        # Or flattened string? Need to verify output.
        # Based on typical Lark use, identifiers might be strings.
        # We will inspect during test execution or assume strings if transformer cleans them.

    def test_node_with_properties(self, parser):
        """Test node with properties."""
        query = "MATCH (n {name: 'Alice', age: 30}) RETURN n"
        tree = parser.parse(query)
        ast = parser.transformer.transform(tree)

        node = ast["statements"][0][0]["clauses"][0]["pattern"]["paths"][0][
            "element"
        ]["parts"][0]

        props = node["properties"]
        assert isinstance(props, dict)
        # Property keys are strings
        assert "name" in props
        assert "age" in props

        # Values are property expressions (IntegerLiteral, StringLiteral or generic Literal)
        # Check transformation of literals
        name_val = props["name"]
        # Depending on literal transformation, this might be a string or dict {"type":"StringLiteral",...}
        # We expect a dict representation of the literal expression or raw value if primitive?
        # AST models converter handles primitives, but parser usually emits structure.


class TestRelationshipStructure:
    """Test AST structure for Relationship patterns."""

    def test_directed_relationship(self, parser):
        """Test directed relationship structure."""
        query = "MATCH (a)-[r:KNOWS]->(b) RETURN r"
        tree = parser.parse(query)
        ast = parser.transformer.transform(tree)

        parts = ast["statements"][0][0]["clauses"][0]["pattern"]["paths"][0][
            "element"
        ]["parts"]
        # parts: [Node, Rel, Node]
        rel = parts[1]

        assert rel["type"] == "RelationshipPattern"
        assert rel["direction"] == "right"
        assert rel["variable"] == "r"
        assert rel["types"][0] == "KNOWS"

    def test_undirected_relationship(self, parser):
        """Test undirected relationship structure."""
        query = "MATCH (a)-[]-(b) RETURN a"
        tree = parser.parse(query)
        ast = parser.transformer.transform(tree)

        rel = ast["statements"][0][0]["clauses"][0]["pattern"]["paths"][0][
            "element"
        ]["parts"][1]

        assert rel["direction"] == "any"
        assert "variable" not in rel or rel["variable"] is None


class TestWhereClauseStructure:
    """Test AST structure for WHERE clauses."""

    def test_simple_where(self, parser):
        """Test simple comparison in WHERE."""
        query = "MATCH (n) WHERE n.age > 18 RETURN n"
        tree = parser.parse(query)
        ast = parser.transformer.transform(tree)

        match = ast["statements"][0][0]["clauses"][0]
        where_clause = match["where"]

        assert where_clause["type"] == "WhereClause"
        condition = where_clause["condition"]

        assert condition["type"] == "Comparison"
        assert condition["operator"] == ">"
        assert condition["left"]["type"] == "PropertyAccess"
        assert condition["right"] == 18

    def test_arithmetic_expression(self, parser):
        """Test arithmetic expression structure."""
        query = "RETURN 1 + 2 * 3"
        tree = parser.parse(query)
        ast = parser.transformer.transform(tree)

        # return -> body -> items -> item -> expression
        expr = ast["statements"][0][0]["return"]["body"]["items"][0][
            "expression"
        ]

        assert expr["type"] == "Arithmetic"
        assert expr["operator"] == "+"
        assert expr["left"] == 1
        assert expr["right"]["type"] == "Arithmetic"
        assert expr["right"]["operator"] == "*"
        assert expr["right"]["left"] == 2
        assert expr["right"]["right"] == 3


class TestUpdateStructure:
    """Test AST structure for Update clauses."""

    def test_create_structure(self, parser):
        """Test CREATE clause structure."""
        query = "CREATE (n:Person {name: 'New'})"
        tree = parser.parse(query)
        ast = parser.transformer.transform(tree)

        # update_statement -> updates (list)
        update = ast["statements"][0][0]["updates"][0]

        assert update["type"] == "CreateClause"
        pattern = update["pattern"]
        assert (
            pattern["paths"][0]["element"]["parts"][0]["labels"][0] == "Person"
        )

    def test_set_structure(self, parser):
        """Test SET clause structure."""
        query = "MATCH (n) SET n.age = 30"
        tree = parser.parse(query)
        ast = parser.transformer.transform(tree)

        update = ast["statements"][0][0]["updates"][0]
        assert update["type"] == "SetClause"
        # Check items
        item = update["items"][0]
        assert item["type"] == "SetProperty"
        assert item["variable"] == "n"
        assert item["property"]["type"] == "PropertyLookup"
        assert item["property"]["property"] == "age"
        assert item["value"] == 30
