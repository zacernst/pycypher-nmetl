"""All the tests."""
# pylint: disable=invalid-name,missing-function-docstring,disallowed-name,protected-access,unused-argument,unused-import,redefined-outer-name,too-many-lines

import collections
import datetime
import filecmp
import logging
import pathlib
import queue
import subprocess
import threading
import time
from typing import Callable, Iterable
from unittest.mock import Mock, patch

import networkx as nx
import pytest
import rich  # pylint: disable=unused-import
from fixtures import (
    data_asset_1,
    empty_session,
    fact_collection_0,
    fact_collection_1,
    fact_collection_2,
    fact_collection_3,
    fact_collection_4,
    fact_collection_5,
    fact_collection_6,
    fact_collection_7,
    fact_collection_squares_circles,
    fixture_0_data_source_mapping_list,
    fixture_data_source_0,
    networkx_graph,
    patched_uuid,
    populated_session,
    raw_data_processor,
    session_with_aggregation_fixture,
    session_with_city_state_fixture,
    session_with_data_asset,
    session_with_three_triggers,
    session_with_trigger,
    session_with_trigger_using_data_asset,
    session_with_two_triggers,
    shapes_session,
    squares_csv_data_source,
)
from nmetl.configuration import (  # pylint: disable=unused-import
    MONOREPO_BASE_DIR,
    TYPE_DISPATCH_DICT,
    DataSourceMappingConfig,
    SessionConfig,
    load_session_config,
)
from nmetl.data_source import (
    CSVDataSource,
    DataSource,
    DataSourceMapping,
    NewColumn,
)
from nmetl.exceptions import (
    BadTriggerReturnAnnotationError,
    UnknownDataSourceError,
)
from nmetl.helpers import QueueGenerator, ensure_uri
from nmetl.message_types import EndOfData, RawDatum
from nmetl.session import NewColumnConfig, Session
from nmetl.data_asset import DataAsset
from nmetl.trigger import (
    NodeRelationship,
    NodeRelationshipTrigger,
    VariableAttribute,
    VariableAttributeTrigger,
)
from nmetl.writer import CSVTableWriter, ParquetTableWriter, TableWriter
from pycypher.cypher_parser import CypherParser
from pycypher.exceptions import (  # pylint: disable=unused-import
    InvalidCastError,
    WrongCypherTypeError,
)
from pycypher.fact import (  # We might get rid of this class entirely
    FactCollection,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactNodeRelatedToNode,
    FactRelationshipHasAttributeWithValue,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
)
from pycypher.logger import LOGGER
from pycypher.node_classes import (
    Addition,
    Aggregation,
    Alias,
    AliasedName,
    And,
    Collect,
    Collection,
    Cypher,
    Distinct,
    Division,
    Equals,
    Evaluable,
    GreaterThan,
    LessThan,
    Literal,
    Mapping,
    MappingSet,
    Match,
    Multiplication,
    Node,
    NodeNameLabel,
    Not,
    ObjectAsSeries,
    ObjectAttributeLookup,
    Or,
    Predicate,
    Projection,
    Query,
    Relationship,
    RelationshipChain,
    RelationshipChainList,
    RelationshipLeftRight,
    RelationshipRightLeft,
    Return,
    Size,
    Subtraction,
    Where,
    WithClause,
)
from pycypher.query import NullResult, QueryNodeLabel, QueryValueOfNodeAttribute
from pycypher.shims.networkx_cypher import NetworkX
from pycypher.solver import (
    ConstraintNodeHasAttributeWithValue,
    ConstraintNodeHasLabel,
    ConstraintRelationshipHasLabel,
    ConstraintRelationshipHasSourceNode,
    ConstraintRelationshipHasTargetNode,
    ConstraintVariableRefersToSpecificObject,
    IsTrue,
)
from pycypher.tree_mixin import TreeMixin
from pytest_unordered import unordered

TEST_DATA_DIRECTORY = pathlib.Path(__file__).parent / "test_data"


def test_parse_match_with_one_node_only_and_with_return():
    cypher_string = (
        "MATCH (s:Square) WITH s.side_length AS side_length RETURN side_length"
    )
    cypher = CypherParser(cypher_string)
    assert cypher


def test_trigger_in_queue_processor(
    fixture_data_source_0, empty_session, fixture_0_data_source_mapping_list
):
    @empty_session.trigger(
        "MATCH (n:Person {age: 25}) WITH n.Identifier AS person_name RETURN person_name"
    )
    def test_function(person_name) -> VariableAttribute["n", "thingy"]:  # pylint: disable=unused-argument  # type:ignore:w
        return True

    fixture_data_source_0.attach_mapping(fixture_0_data_source_mapping_list)
    empty_session.attach_data_source(fixture_data_source_0)
    empty_session.start_threads()
    empty_session.block_until_finished()


def test_parameter_not_present_in_cypher_with_aliases(empty_session):
    with pytest.raises(BadTriggerReturnAnnotationError):

        @empty_session.trigger("MATCH (n:Thingy) RETURN n.foo AS whatever")
        def test_function(n) -> VariableAttribute["n", "thingy"]:  # pylint: disable=unused-argument  # type:ignore
            return 1


def test_fact_collection_has_facts(fact_collection_0: FactCollection):
    assert fact_collection_0


def test_fact_collection_del_item(fact_collection_0: FactCollection):
    first_fact = fact_collection_0[0]
    assert first_fact in fact_collection_0
    del fact_collection_0[0]
    assert first_fact not in fact_collection_0


def test_fact_collection_set_item(fact_collection_0: FactCollection):
    fact = FactNodeHasLabel("3", "Thing")
    fact_collection_0[0] = fact
    assert fact_collection_0[0] == fact


def test_fact_collection_get_item(fact_collection_0: FactCollection):
    fact = fact_collection_0[0]
    assert isinstance(fact, FactNodeHasLabel)


def test_fact_collection_insert(fact_collection_0: FactCollection):
    fact = FactNodeHasLabel("3", "Thing")
    assert fact not in fact_collection_0
    fact_collection_0.insert(0, fact)
    assert fact in fact_collection_0


def test_fact_collection_iadd(fact_collection_0: FactCollection):
    fact = FactNodeHasLabel("3", "Thing")
    assert fact not in fact_collection_0
    fact_collection_0 += fact
    assert fact in fact_collection_0


def test_fact_collection_append(fact_collection_0: FactCollection):
    fact = FactNodeHasLabel("3", "Thing")
    assert fact not in fact_collection_0
    fact_collection_0.append(fact)
    assert fact in fact_collection_0


def test_can_parse_simple_cypher():
    obj = CypherParser("MATCH (n) RETURN n.foo")
    assert isinstance(obj, CypherParser)


def test_parser_builds_cypher_object():
    obj = CypherParser("MATCH (n) RETURN n.foo")
    assert isinstance(obj.parse_tree, Cypher)


def test_parser_creates_simple_node_object():
    obj = CypherParser("MATCH (n) RETURN n.foo")
    assert isinstance(obj.parse_tree.cypher, Query)


def test_parser_parses_complicated_query():
    query = (
        """MATCH (n:Thing {key1: "value", key2: 5})-[r]->(m:OtherThing {key3: "hithere"}) """
        """WHERE n.key = 2, n.foo = 3 """
        """RETURN n.foobar, n.baz"""
    )
    obj = CypherParser(query)
    assert isinstance(obj.parse_tree, Cypher)


def test_parser_handles_node_label():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) RETURN n.foobar"""
    obj = CypherParser(query)
    assert (
        obj.parse_tree.cypher.match_clause.pattern.relationships[0]
        .steps[0]
        .node_name_label.label
        == "Thingy"
    )


def test_parser_handles_where_clause():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(obj.parse_tree.cypher.match_clause.where_clause, Where)


def test_parser_handles_where_clause_predicate():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(obj.parse_tree.cypher.match_clause.where_clause.predicate, Equals)


def test_parser_handles_where_clause_predicate_lookup():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parse_tree.cypher.match_clause.where_clause.predicate.left_side,
        ObjectAttributeLookup,
    )


def test_parser_handles_where_clause_predicate_literal():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parse_tree.cypher.match_clause.where_clause.predicate.right_side,
        Literal,
    )


def test_parser_generates_alias_in_return_statement():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar AS myfoobar"""
    obj = CypherParser(query)
    assert isinstance(obj.parse_tree.cypher.return_clause.projection.lookups[0], Alias)


def test_parser_generates_alias_with_correct_name_in_return_statement():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar AS myfoobar"""
    obj = CypherParser(query)
    assert obj.parse_tree.cypher.return_clause.projection.lookups[0].alias == "myfoobar"


def test_node_has_label_equality():
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("1", "Thing")
    assert fact1 == fact2


def test_node_has_label_inequality():
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("2", "Thing")
    assert fact1 != fact2


def test_node_has_attribute_with_value_equality():
    fact1 = FactNodeHasAttributeWithValue("1", "key", 2)
    fact2 = FactNodeHasAttributeWithValue("1", "key", 2)
    assert fact1 == fact2


def test_node_has_attribute_with_value_inequality():
    fact1 = FactNodeHasAttributeWithValue("1", "key", 2)
    fact2 = FactNodeHasAttributeWithValue("1", "key", 3)
    assert fact1 != fact2


def test_node_has_related_node_equality():
    fact1 = FactNodeRelatedToNode("1", "2", "MyRelationship")
    fact2 = FactNodeRelatedToNode("1", "2", "MyRelationship")
    assert fact1 == fact2


def test_node_has_related_node_inequality():
    fact1 = FactNodeRelatedToNode("1", "2", "MyRelationship")
    fact2 = FactNodeRelatedToNode("1", "2", "MyOtherRelationship")
    assert fact1 != fact2


def test_aggregate_constraints_node_label():
    cypher = "MATCH (m:Thing) RETURN m.foobar"
    result = CypherParser(cypher)
    constraints = result.parse_tree.cypher.match_clause.constraints
    assert len(constraints) == 1


def test_aggregate_constraints_node_and_mapping():
    cypher = "MATCH (m:Thing {key: 2}) RETURN m.foobar"
    result = CypherParser(cypher)
    constraints = result.parse_tree.cypher.match_clause.constraints
    assert len(constraints) == 2


def test_parse_anonymous_node_no_label_no_mapping_gets_variable():
    with patch("uuid.uuid4", patched_uuid) as _:
        cypher = "MATCH () RETURN m.foobar"
        result = CypherParser(cypher)
        assert (
            result.parse_tree.cypher.match_clause.pattern.relationships[0]
            .steps[0]
            .node_name_label.name
            == "SOME_HEX"
        )


def test_parse_anonymous_node_with_label_no_mapping_gets_variable():
    with patch("uuid.uuid4", patched_uuid) as _:
        cypher = "MATCH (:Thing) RETURN m.foobar"
        result = CypherParser(cypher)
        assert (
            result.parse_tree.cypher.match_clause.pattern.relationships[0]
            .steps[0]
            .node_name_label.name
            == "SOME_HEX"
        )


def test_parse_anonymous_node_with_label_no_mapping_has_right_label():
    with patch("uuid.uuid4", patched_uuid) as _:
        cypher = "MATCH (:Thing) RETURN m.foobar"
        result = CypherParser(cypher)
        assert (
            result.parse_tree.cypher.match_clause.pattern.relationships[0]
            .steps[0]
            .node_name_label.label
            == "Thing"
        )


def test_source_node_constraint_from_left_right_relationship():
    with patch("uuid.uuid4", patched_uuid) as _:
        cypher = "MATCH (n:Thing)-[:Relationship]->(m:Other) RETURN n.foobar"
        result = CypherParser(cypher)
        assert (
            ConstraintRelationshipHasSourceNode("n", "SOME_HEX")
            in result.parse_tree.cypher.match_clause.constraints
        )


def test_source_node_constraint_from_left_right_relationship_with_label():
    cypher = "MATCH (n:Thing)-[r:Relationship]->(m:Other) RETURN n.foobar"
    result = CypherParser(cypher)
    assert (
        ConstraintRelationshipHasSourceNode("n", "r")
        in result.parse_tree.cypher.match_clause.constraints
    )


def test_target_node_constraint_from_left_right_relationship():
    with patch("uuid.uuid4", patched_uuid) as _:
        cypher = "MATCH (n:Thing)-[:Relationship]->(m:Other) RETURN n.foobar"
        result = CypherParser(cypher)
        assert (
            ConstraintRelationshipHasTargetNode("m", "SOME_HEX")
            in result.parse_tree.cypher.match_clause.constraints
        )


def test_target_node_constraint_from_left_right_relationship_with_label():
    cypher = "MATCH (n:Thing)-[r:Relationship]->(m:Other) RETURN n.foobar"
    result = CypherParser(cypher)
    assert (
        ConstraintRelationshipHasTargetNode("m", "r")
        in result.parse_tree.cypher.match_clause.constraints
    )


def test_constraint_node_has_label():
    cypher = "MATCH (n:Thing) RETURN n.foobar"
    result = CypherParser(cypher)
    assert (
        ConstraintNodeHasLabel("n", "Thing")
        in result.parse_tree.cypher.match_clause.constraints
    )


def test_constraint_relationship_has_label():
    with patch("uuid.uuid4", patched_uuid) as _:
        cypher = "MATCH (n:Thing)-[:Relationship]->(m:Other) RETURN n.foobar"
        result = CypherParser(cypher)
        assert (
            ConstraintRelationshipHasLabel("SOME_HEX", "Relationship")
            in result.parse_tree.cypher.match_clause.constraints
        )


def test_constraint_relationship_has_source_node():
    with patch("uuid.uuid4", patched_uuid) as _:
        cypher = "MATCH (n:Thing)-[:Relationship]->(m:Other) RETURN n.foobar"
        result = CypherParser(cypher)
        assert (
            ConstraintRelationshipHasSourceNode("n", "SOME_HEX")
            in result.parse_tree.cypher.match_clause.constraints
        )


def test_constraint_relationship_has_target_node():
    with patch("uuid.uuid4", patched_uuid) as _:
        cypher = "MATCH (n:Thing)-[:Relationship]->(m:Other) RETURN n.foobar"
        result = CypherParser(cypher)
        assert (
            ConstraintRelationshipHasTargetNode("m", "SOME_HEX")
            in result.parse_tree.cypher.match_clause.constraints
        )


def test_find_solution_node_has_label(fact_collection_0: FactCollection):
    cypher = "MATCH (n:Thing) RETURN n.foobar"
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_0)
    expected = [{"n": "1"}]
    assert solutions == expected


def test_find_solution_node_has_wrong_label(fact_collection_0: FactCollection):
    cypher = "MATCH (n:WrongLabel) RETURN n.foobar"
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_0)
    assert not solutions


def test_find_solution_node_with_relationship(
    fact_collection_0: FactCollection,
):
    # Hash variable for relationship not being added to variable list
    cypher = "MATCH (n:Thing)-[r:MyRelationship]->(m:OtherThing) RETURN n.foobar"
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_0)
    expected = [{"n": "1", "m": "2", "r": "relationship_123"}]
    assert solutions == expected


def test_find_solution_node_with_relationship_nonexistant(
    fact_collection_0: FactCollection,
):
    # Hash variable for relationship not being added to variable list
    cypher = (
        "MATCH (n:Thing)-[r:NotExistingRelationship]->(m:OtherThing) RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_0)
    expected = []
    assert solutions == expected


def test_find_solution_node_with_attribute_value(
    fact_collection_0: FactCollection,
):
    cypher = "MATCH (n:Thing {key: 2}) RETURN n.foobar"
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_0)
    expected = [{"n": "1"}]
    assert solutions == expected


def test_find_no_solution_node_with_wrong_attribute_value(
    fact_collection_0: FactCollection,
):
    cypher = "MATCH (n:Thing {key: 123}) RETURN n.foobar"
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_0)
    expected = []
    assert solutions == expected


def test_find_solution_node_with_attribute_and_relationship(
    fact_collection_0: FactCollection,
):
    cypher = (
        "MATCH (n:Thing {key: 2})-[r:MyRelationship]->(m:OtherThing) RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_0)
    expected = [{"n": "1", "m": "2", "r": "relationship_123"}]
    assert solutions == expected


def test_find_no_solution_node_with_wrong_attribute_and_relationship(
    fact_collection_0: FactCollection,
):
    cypher = (
        "MATCH (n:Thing {key: 3})-[r:MyRelationship]->(m:OtherThing) RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_0)
    expected = []
    assert solutions == expected


def test_find_no_solution_node_with_wrong_attribute_type_and_relationship(
    fact_collection_0: FactCollection,
):
    cypher = (
        'MATCH (n:Thing {key: "3"})-[r:MyRelationship]->(m:OtherThing) RETURN n.foobar'
    )
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_0)
    expected = []
    assert solutions == expected


def test_find_solution_node_with_attribute_type_and_relationship_target_node_attribute(
    fact_collection_0: FactCollection,
):
    cypher = "MATCH (n:Thing {key: 2})-[r:MyRelationship]->(m:OtherThing {key: 5}) RETURN n.foobar"
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_0)
    expected = [{"n": "1", "m": "2", "r": "relationship_123"}]
    assert solutions == expected


def test_find_no_solution_node_with_attribute_type_and_wrong_relationship_target_node_attribute(
    fact_collection_0: FactCollection,
):
    cypher = (
        "MATCH (n:Thing {key: 2})-[r:NoRelationshipLikeMeExists]->(m:OtherThing {key: 5}) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_0)
    expected = []
    assert solutions == expected


def test_find_two_solutions_node_has_label(fact_collection_1: FactCollection):
    cypher = "MATCH (n:Thing) RETURN n.foobar"
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_1)
    expected = [{"n": "1"}, {"n": "2"}]
    assert solutions == unordered(expected)


def test_constraints_from_relationship_chain():
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->"
        "(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    constraint1 = ConstraintRelationshipHasSourceNode(
        source_node_name="n", relationship_name="r"
    )
    constraint2 = ConstraintRelationshipHasTargetNode(
        target_node_name="m", relationship_name="r"
    )
    constraint3 = ConstraintRelationshipHasSourceNode(
        source_node_name="m", relationship_name="s"
    )
    constraint4 = ConstraintRelationshipHasTargetNode(
        target_node_name="o", relationship_name="s"
    )
    constraint5 = ConstraintRelationshipHasLabel(
        relationship_name="r", label="MyRelationship"
    )
    constraint6 = ConstraintRelationshipHasLabel(
        relationship_name="s", label="OtherRelationship"
    )
    constraint7 = ConstraintNodeHasLabel(variable="n", label="Thing")
    constraint8 = ConstraintNodeHasLabel(variable="m", label="MiddleThing")
    constraint9 = ConstraintNodeHasLabel(variable="o", label="OtherThing")
    assert constraint1 in result.parse_tree.cypher.match_clause.constraints
    assert constraint2 in result.parse_tree.cypher.match_clause.constraints
    assert constraint3 in result.parse_tree.cypher.match_clause.constraints
    assert constraint4 in result.parse_tree.cypher.match_clause.constraints
    assert constraint5 in result.parse_tree.cypher.match_clause.constraints
    assert constraint6 in result.parse_tree.cypher.match_clause.constraints
    assert constraint7 in result.parse_tree.cypher.match_clause.constraints
    assert constraint8 in result.parse_tree.cypher.match_clause.constraints
    assert constraint9 in result.parse_tree.cypher.match_clause.constraints


def test_constraints_from_relationship_pair():
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing), "
        "(m)-[s:OtherRelationship]->(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    constraint1 = ConstraintRelationshipHasSourceNode(
        source_node_name="n", relationship_name="r"
    )
    constraint2 = ConstraintRelationshipHasTargetNode(
        target_node_name="m", relationship_name="r"
    )
    constraint3 = ConstraintRelationshipHasSourceNode(
        source_node_name="m", relationship_name="s"
    )
    constraint4 = ConstraintRelationshipHasTargetNode(
        target_node_name="o", relationship_name="s"
    )
    constraint5 = ConstraintRelationshipHasLabel(
        relationship_name="r", label="MyRelationship"
    )
    constraint6 = ConstraintRelationshipHasLabel(
        relationship_name="s", label="OtherRelationship"
    )
    constraint7 = ConstraintNodeHasLabel(variable="n", label="Thing")
    constraint8 = ConstraintNodeHasLabel(variable="m", label="MiddleThing")
    constraint9 = ConstraintNodeHasLabel(variable="o", label="OtherThing")
    assert constraint1 in result.parse_tree.cypher.match_clause.constraints
    assert constraint2 in result.parse_tree.cypher.match_clause.constraints
    assert constraint3 in result.parse_tree.cypher.match_clause.constraints
    assert constraint4 in result.parse_tree.cypher.match_clause.constraints
    assert constraint5 in result.parse_tree.cypher.match_clause.constraints
    assert constraint6 in result.parse_tree.cypher.match_clause.constraints
    assert constraint7 in result.parse_tree.cypher.match_clause.constraints
    assert constraint8 in result.parse_tree.cypher.match_clause.constraints
    assert constraint9 in result.parse_tree.cypher.match_clause.constraints
    assert len(result.parse_tree.cypher.match_clause.constraints) == 9


def test_find_solution_relationship_chain_two_forks(
    fact_collection_2: FactCollection,
):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->"
        "(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_2)
    expected = [
        {
            "m": "2",
            "r": "relationship_1",
            "s": "relationship_2",
            "n": "1",
            "o": "3",
        }
    ]
    assert solutions == expected


def test_find_solution_relationship_chain_fork(
    fact_collection_3: FactCollection,
):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->"
        "(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_3)
    expected = [
        {
            "m": "2",
            "r": "relationship_1",
            "s": "relationship_2",
            "n": "1",
            "o": "3",
        },
        {
            "m": "2",
            "r": "relationship_3",
            "s": "relationship_2",
            "n": "4",
            "o": "3",
        },
    ]
    assert solutions == unordered(expected)


def test_find_solution_relationship_chain_fork_2(
    fact_collection_4: FactCollection,
):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->"
        "(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_4)
    expected = [
        {
            "m": "2",
            "r": "relationship_3",
            "s": "relationship_4",
            "n": "4",
            "o": "5",
        },
        {
            "m": "2",
            "r": "relationship_3",
            "s": "relationship_2",
            "n": "4",
            "o": "3",
        },
        {
            "m": "2",
            "r": "relationship_1",
            "s": "relationship_4",
            "n": "1",
            "o": "5",
        },
        {
            "m": "2",
            "r": "relationship_1",
            "s": "relationship_2",
            "n": "1",
            "o": "3",
        },
    ]
    assert solutions == unordered(expected)


def test_constraint_relationship_chain_with_node_attribute():
    cypher = (
        "MATCH (n:Thing {foo: 2})-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    constraint1 = ConstraintRelationshipHasSourceNode(
        source_node_name="n", relationship_name="r"
    )
    constraint2 = ConstraintRelationshipHasTargetNode(
        target_node_name="m", relationship_name="r"
    )
    constraint3 = ConstraintRelationshipHasSourceNode(
        source_node_name="m", relationship_name="s"
    )
    constraint4 = ConstraintRelationshipHasTargetNode(
        target_node_name="o", relationship_name="s"
    )
    constraint5 = ConstraintRelationshipHasLabel(
        relationship_name="r", label="MyRelationship"
    )
    constraint6 = ConstraintRelationshipHasLabel(
        relationship_name="s", label="OtherRelationship"
    )
    constraint7 = ConstraintNodeHasLabel(variable="n", label="Thing")
    constraint8 = ConstraintNodeHasLabel(variable="m", label="MiddleThing")
    constraint9 = ConstraintNodeHasLabel(variable="o", label="OtherThing")
    constraint10 = ConstraintNodeHasAttributeWithValue(
        variable="n", attribute="foo", value=Literal(2)
    )
    assert constraint1 in result.parse_tree.cypher.match_clause.constraints
    assert constraint2 in result.parse_tree.cypher.match_clause.constraints
    assert constraint3 in result.parse_tree.cypher.match_clause.constraints
    assert constraint4 in result.parse_tree.cypher.match_clause.constraints
    assert constraint5 in result.parse_tree.cypher.match_clause.constraints
    assert constraint6 in result.parse_tree.cypher.match_clause.constraints
    assert constraint7 in result.parse_tree.cypher.match_clause.constraints
    assert constraint8 in result.parse_tree.cypher.match_clause.constraints
    assert constraint9 in result.parse_tree.cypher.match_clause.constraints
    assert constraint10 in result.parse_tree.cypher.match_clause.constraints


def test_find_no_solution_relationship_chain_fork_missing_node_attribute(
    fact_collection_4: FactCollection,
):
    cypher = (
        "MATCH (n:Thing {foo: 2})-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_4)
    assert not solutions


def test_find_two_solutions_relationship_chain_fork_require_node_attribute_value(
    fact_collection_5: FactCollection,
):
    cypher = (
        "MATCH (n:Thing {foo: 2})-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_5)
    expected = [
        {
            "m": "2",
            "r": "relationship_3",
            "s": "relationship_4",
            "n": "4",
            "o": "5",
        },
        {
            "m": "2",
            "r": "relationship_3",
            "s": "relationship_2",
            "n": "4",
            "o": "3",
        },
    ]
    assert solutions == unordered(expected)


def test_find_no_solutions_relationship_chain_fork_node_attribute_value_wrong_type(
    fact_collection_5: FactCollection,
):
    cypher = (
        'MATCH (n:Thing {foo: "2"})-[r:MyRelationship]->(m:MiddleThing)-'
        "[s:OtherRelationship]->(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_5)
    assert not solutions


def test_find_two_solutions_relationship_chain_fork_red_herring_node(
    fact_collection_6: FactCollection,
):
    cypher = (
        "MATCH (n:Thing {foo: 2})-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_6)
    expected = [
        {
            "m": "2",
            "r": "relationship_3",
            "s": "relationship_4",
            "n": "4",
            "o": "5",
        },
        {
            "m": "2",
            "r": "relationship_3",
            "s": "relationship_2",
            "n": "4",
            "o": "3",
        },
    ]
    assert solutions == unordered(expected)


def test_find_no_solutions_relationship_chain_fork_node_attribute_value_wrong_type_red_herring_node(
    fact_collection_6: FactCollection,
):
    cypher = (
        """MATCH (n:Thing {foo: "2"})-[r:MyRelationship]->(m:MiddleThing)-"""
        """[s:OtherRelationship]->(o:OtherThing) """
        """RETURN n.foobar"""
    )
    result = CypherParser(cypher)
    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_6)
    assert not solutions


def test_networkx_shim_initialization(networkx_graph):
    nxshim = NetworkX(networkx_graph)
    assert nxshim.graph
    assert isinstance(nxshim.graph, nx.DiGraph)


def test_nx_graph_to_fact_collection(networkx_graph):
    nxshim = NetworkX(networkx_graph)
    fact_collection = nxshim.make_fact_collection()
    assert fact_collection.facts


def test_parser_creates_with_clause():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar, m.baz AS qux RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(obj.parse_tree.cypher.match_clause.with_clause, WithClause)


def test_parser_creates_with_clause_object_as_series():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar, m.baz AS qux RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parse_tree.cypher.match_clause.with_clause.lookups,
        ObjectAsSeries,
    )


def test_parser_creates_with_clause_object_as_series_members():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar, m.baz AS qux RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parse_tree.cypher.match_clause.with_clause.lookups.lookups,
        list,
    )
    assert len(obj.parse_tree.cypher.match_clause.with_clause.lookups.lookups) == 2


def test_parser_creates_with_clause_object_as_series_members_are_alias():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar, m.baz AS qux RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parse_tree.cypher.match_clause.with_clause.lookups.lookups[0],
        Alias,
    )
    assert isinstance(
        obj.parse_tree.cypher.match_clause.with_clause.lookups.lookups[1],
        Alias,
    )


def test_parser_creates_with_clause_object_alias_has_lookup():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar, m.baz AS qux RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parse_tree.cypher.match_clause.with_clause.lookups.lookups[0].reference,
        ObjectAttributeLookup,
    )
    assert isinstance(
        obj.parse_tree.cypher.match_clause.with_clause.lookups.lookups[0].alias,
        str,
    )


def test_parser_creates_with_clause_object_alias_correct_value():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar, m.baz AS qux RETURN n.foobar"""
    obj = CypherParser(query)
    assert (
        obj.parse_tree.cypher.match_clause.with_clause.lookups.lookups[0].alias == "bar"
    )


def test_parser_creates_with_clause_single_element():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(obj.parse_tree.cypher.match_clause.with_clause, WithClause)


def test_parser_handles_collect_aggregation_in_return():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert obj.parse_tree.cypher.return_clause.projection.lookups[0].alias == "whatever"


def test_parser_handles_collect_in_aggregation_in_return():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert isinstance(
        obj.parse_tree.cypher.return_clause.projection.lookups[0].reference.aggregation,
        Collect,
    )


def test_parser_handles_collect_in_aggregation_in_with_clause():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH COLLECT(n.foo) AS bar """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert isinstance(
        obj.parse_tree.cypher.return_clause.projection.lookups[0].reference.aggregation,
        Collect,
    )


def test_parser_handles_collect_in_aggregation_in_with_clause_node_only():
    query = (
        """MATCH (n:Thingy) """
        """WITH COLLECT(n.foo) AS bar """
        """WHERE n.whatever = "thing" """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert isinstance(
        obj.parse_tree.cypher.return_clause.projection.lookups[0].reference.aggregation,
        Collect,
    )


def test_parser_handles_collect_in_aggregation_in_return_twice():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """RETURN COLLECT(n.foobar) AS whatever, m.whatever AS bazqux"""
    )
    obj = CypherParser(query)
    assert isinstance(
        obj.parse_tree.cypher.return_clause.projection.lookups[0].reference.aggregation,
        Collect,
    )
    assert isinstance(
        obj.parse_tree.cypher.return_clause.projection.lookups[1].reference,
        ObjectAttributeLookup,
    )


def test_parser_handles_with_where_clause_where_class():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """WHERE n.whatever = "thing" """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert isinstance(obj.parse_tree.cypher.match_clause.where_clause, Where)


def test_parser_handles_with_where_clause_with_class():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """WHERE n.whatever = "thing" """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert isinstance(obj.parse_tree.cypher.match_clause.with_clause, WithClause)


def test_nodes_have_parent():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """WHERE n.whatever = "thing" """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert all(
        node.parent is not None
        for node in obj.parse_tree.walk()
        if isinstance(node, TreeMixin)
    )


def test_child_of_parent_is_self():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar WHERE n.whatever = "thing" """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert all(
        node in list(node.parent.children)
        for node in obj.parse_tree.walk()
        if isinstance(node, TreeMixin)
    )


def test_root_node_defined_everywhere():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """WHERE n.whatever = "thing" """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert all(
        node.root is obj.parse_tree
        for node in obj.parse_tree.walk()
        if isinstance(node, TreeMixin)
    )


def test_node_in_match_clause_has_match_clause_enclosing():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """WHERE n.whatever = "thing" """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    match_node = obj.parse_tree.cypher.match_clause
    assert all(
        node.enclosing_class(Match) is match_node
        for node in match_node.walk()
        if isinstance(node, TreeMixin)
    )


def test_error_on_no_enclosing_class():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """WHERE n.whatever = "thing" """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    with pytest.raises(ValueError):
        obj.parse_tree.cypher.return_clause.enclosing_class(Match)


def test_evaluate_literal_string():
    literal = Literal("thing")
    assert literal.evaluate(None) == "thing"


def test_evaluate_literal_int():
    literal = Literal(5)
    assert literal.evaluate(None) == 5


def test_evaluate_literal_float():
    literal = Literal(5.0)
    assert literal.evaluate(None) == 5.0


def test_evaluate_literal_bool():
    literal = Literal(True)
    assert literal.evaluate(None) is True


def test_evaluate_literal_none():
    literal = Literal(None)
    assert literal.evaluate(None) is None


def test_literals_are_evaluable():
    literal = Literal("thing")
    assert isinstance(literal, Evaluable)


def test_evaluate_literals_evaluate_equal_strings():
    literal1 = Literal("thing")
    literal2 = Literal("thing")
    assert Equals(literal1, literal2).evaluate(None)


def test_evaluate_literals_evaluate_equal_integer():
    literal1 = Literal(5)
    literal2 = Literal(5)
    assert Equals(literal1, literal2).evaluate(None)


def test_evaluate_literals_evaluate_not_equal_strings():
    literal1 = Literal("thing")
    literal2 = Literal("thingy")
    assert not Equals(literal1, literal2).evaluate(None)


def test_evaluate_literals_evaluate_greater_than_integer():
    literal1 = Literal(6)
    literal2 = Literal(5)
    assert GreaterThan(literal1, literal2).evaluate(None)


def test_evaluate_literals_evaluate_not_greater_than_integer():
    literal1 = Literal(6)
    literal2 = Literal(5)
    assert not GreaterThan(literal2, literal1).evaluate(None)


def test_evaluate_literals_evaluate_less_than_integer():
    literal1 = Literal(6)
    literal2 = Literal(5)
    assert not LessThan(literal1, literal2).evaluate(None)


def test_evaluate_literals_evaluate_not_less_than_integer():
    literal1 = Literal(6)
    literal2 = Literal(5)
    assert LessThan(literal2, literal1).evaluate(None)


def test_evaluate_addition_integers():
    literal1 = Literal(6)
    literal2 = Literal(5)
    assert Addition(literal1, literal2).evaluate(None) == 11


def test_evaluate_division_integers():
    literal1 = Literal(6)
    literal2 = Literal(3)
    assert Division(literal1, literal2).evaluate(None) == 2


def test_evaluating_division_returns_float():
    literal1 = Literal(6)
    literal2 = Literal(3)
    assert isinstance(Division(literal1, literal2).evaluate(None), float)


def test_evaluate_subtraction_integers():
    literal1 = Literal(6)
    literal2 = Literal(5)
    assert Subtraction(literal1, literal2).evaluate(None) == 1


def test_cannot_evaluate_addition_strings():
    literal1 = Literal("thing")
    literal2 = Literal("thing")
    with pytest.raises(Exception):
        Addition(literal1, literal2).evaluate(None)


def test_evaluate_nested_addition():
    literal1 = Literal(6)
    literal2 = Literal(5)
    literal3 = Literal(4)
    assert Addition(Addition(literal1, literal2), literal3).evaluate(None) == 15


def test_cannot_evaluate_addition_strings_right_side():
    literal1 = Literal(1)
    literal2 = Literal("thing")
    with pytest.raises(Exception):
        Addition(literal1, literal2).evaluate(None)


def test_evaluate_nested_subtraction():
    literal1 = Literal(6)
    literal2 = Literal(5)
    literal3 = Literal(4)
    assert Subtraction(Subtraction(literal1, literal2), literal3).evaluate(None) == -3


def test_evaluate_nested_addition_multiplication():
    literal1 = Literal(6)
    literal2 = Literal(5)
    literal3 = Literal(4)
    assert Addition(Multiplication(literal1, literal2), literal3).evaluate(None) == 34


def test_cannot_evaluate_addition_strings_left_side():
    literal1 = Literal("thing")
    literal2 = Literal(1)
    with pytest.raises(Exception):
        Addition(literal1, literal2).evaluate(None)


def test_refuse_to_divide_by_zero():
    literal1 = Literal(1)
    literal2 = Literal(0)
    with pytest.raises(WrongCypherTypeError):
        Division(literal1, literal2).evaluate(None)


def test_refuse_to_divide_by_zero_both():
    literal1 = Literal(0)
    literal2 = Literal(0)
    with pytest.raises(WrongCypherTypeError):
        Division(literal1, literal2).evaluate(None)


def test_evaluate_boolean_and_both_true():
    literal1 = Literal(True)
    literal2 = Literal(True)
    assert And(literal1, literal2).evaluate(None)


def test_evaluate_boolean_and_both_false():
    literal1 = Literal(False)
    literal2 = Literal(False)
    assert not And(literal1, literal2).evaluate(None)


def test_evaluate_boolean_and_one_false():
    literal1 = Literal(False)
    literal2 = Literal(True)
    assert not And(literal1, literal2).evaluate(None)


def test_evaluate_boolean_or_both_true():
    literal1 = Literal(True)
    literal2 = Literal(True)
    assert Or(literal1, literal2).evaluate(None)


def test_evaluate_boolean_or_one_true():
    literal1 = Literal(False)
    literal2 = Literal(True)
    assert Or(literal1, literal2).evaluate(None)


def test_evaluate_boolean_or_both_false():
    literal1 = Literal(False)
    literal2 = Literal(False)
    assert not Or(literal1, literal2).evaluate(None)


def test_evaluate_boolean_not_true():
    literal = Literal(True)
    assert not Not(literal).evaluate(None)


def test_evaluate_boolean_not_false():
    literal = Literal(False)
    assert Not(literal).evaluate(None)


def test_double_negation():
    literal = Literal(True)
    assert Not(Not(literal)).evaluate(None)


def test_evaluate_boolean_not_not_true():
    literal = Literal(False)
    assert not Not(Not(literal)).evaluate(None)


def test_evaluate_boolean_and_both_true_negated():
    literal1 = Literal(True)
    literal2 = Literal(True)
    assert not Not(And(literal1, literal2)).evaluate(None)


def test_evaluate_boolean_and_both_false_negated():
    literal1 = Literal(False)
    literal2 = Literal(False)
    assert Not(And(literal1, literal2)).evaluate(None)


def test_evaluate_boolean_and_one_false_negated():
    literal1 = Literal(False)
    literal2 = Literal(True)
    assert Not(And(literal1, literal2)).evaluate(None)


def test_evaluate_boolean_or_both_true_negated():
    literal1 = Literal(True)
    literal2 = Literal(True)
    assert not Not(Or(literal1, literal2)).evaluate(None)


def test_evaluate_boolean_or_one_true_negated():
    literal1 = Literal(False)
    literal2 = Literal(True)
    assert not Not(Or(literal1, literal2)).evaluate(None)


def test_evaluate_boolean_or_both_false_negated():
    literal1 = Literal(False)
    literal2 = Literal(False)
    assert Not(Or(literal1, literal2)).evaluate(None)


def test_evaluate_demorgan_law_both_true():
    literal1 = Literal(True)
    literal2 = Literal(True)
    assert Equals(
        Not(And(literal1, literal2)), Or(Not(literal1), Not(literal2))
    ).evaluate(None)


def test_evaluate_demorgan_law_left_true():
    literal1 = Literal(True)
    literal2 = Literal(False)
    assert Equals(
        Not(And(literal1, literal2)), Or(Not(literal1), Not(literal2))
    ).evaluate(None)


def test_evaluate_demorgan_law_right_true():
    literal1 = Literal(False)
    literal2 = Literal(True)
    assert Equals(
        Not(And(literal1, literal2)), Or(Not(literal1), Not(literal2))
    ).evaluate(None)


def test_evaluate_demorgan_law_both_false():
    literal1 = Literal(False)
    literal2 = Literal(False)
    assert Equals(
        Not(And(literal1, literal2)), Or(Not(literal1), Not(literal2))
    ).evaluate(None)


def test_enumerate_fact_types(fact_collection_6):
    facts = [fact for fact in fact_collection_6.node_has_label_facts()]
    assert len(facts) == 6


def test_query_node_has_attribute_with_value(fact_collection_6):
    query = QueryValueOfNodeAttribute(node_id="4", attribute="foo")
    value = fact_collection_6.query(query)
    assert value.evaluate(fact_collection_6) == 2


def test_query_node_label(fact_collection_6):
    query = QueryNodeLabel(node_id="4")
    value = fact_collection_6.query(query)
    assert value == "Thing"


def test_query_nonexistent_node_label(fact_collection_6):
    query = QueryNodeLabel(node_id="idontexist")
    assert isinstance(fact_collection_6.query(query), NullResult)


def test_query_node_has_non_existent_attribute(fact_collection_6):
    query = QueryValueOfNodeAttribute(node_id="4", attribute="bar")
    assert isinstance(fact_collection_6.query(query), NullResult)


def test_query_non_existent_node_has_attribute_raises_error(fact_collection_6):
    query = QueryValueOfNodeAttribute(node_id="idontexist", attribute="foo")
    assert isinstance(fact_collection_6.query(query), NullResult)


def test_object_attribute_lookup_evaluate(fact_collection_6):
    lookup = ObjectAttributeLookup(object_name="n", attribute="foo")
    assert lookup.evaluate(fact_collection_6, projection={"n": "4"}) == 2


def test_object_attribute_lookup_non_existent_object_raises_error(
    fact_collection_6,
):
    result = ObjectAttributeLookup(object_name="n", attribute="foo").evaluate(
        fact_collection_6, projection={"n": "idontexist"}
    )
    assert isinstance(result, NullResult)


def test_object_attribute_lookup_in_addition(fact_collection_6):
    lookup = ObjectAttributeLookup(object_name="n", attribute="foo")
    literal = Literal(3)
    assert (
        Addition(lookup, literal).evaluate(fact_collection_6, projection={"n": "4"})
        == 5
    )


def test_object_attribute_lookup_greater_than(fact_collection_6):
    lookup = ObjectAttributeLookup(object_name="n", attribute="foo")
    literal = Literal(1)
    assert GreaterThan(lookup, literal).evaluate(
        fact_collection_6, projection={"n": "4"}
    )


def test_object_attribute_lookup_greater_than_false(fact_collection_6):
    lookup = ObjectAttributeLookup(object_name="n", attribute="foo")
    literal = Literal(10)
    assert Not(GreaterThan(lookup, literal)).evaluate(
        fact_collection_6, projection={"n": "4"}
    )


def test_object_attribute_lookup_greater_than_double_negation(
    fact_collection_6,
):
    lookup = ObjectAttributeLookup(object_name="n", attribute="foo")
    literal = Literal(10)
    assert not Not(Not(GreaterThan(lookup, literal))).evaluate(
        fact_collection_6, projection={"n": "4"}
    )


def test_nonexistent_attribute_nested_evaluation_returns_null_result(
    fact_collection_6,
):
    lookup = ObjectAttributeLookup(object_name="n", attribute="idontexist")
    literal = Literal(10)
    result = Not(Not(GreaterThan(lookup, literal))).evaluate(
        fact_collection_6, projection={"n": "4"}
    )
    assert isinstance(result, NullResult)


def test_collect_aggregated_variables_in_with_clause():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH COLLECT(n.foo) AS thingy, m.qux AS bar """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert obj.parse_tree.cypher.match_clause.with_clause.aggregated_variables == ["n"]


def test_collect_all_variables_in_with_clause():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH COLLECT(n.foo) AS thingy, m.qux AS bar """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert sorted(obj.parse_tree.cypher.match_clause.with_clause.all_variables) == [
        "m",
        "n",
    ]


def test_collect_non_aggregated_variables_in_with_clause():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH COLLECT(n.foo) AS thingy, m.qux AS bar """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert obj.parse_tree.cypher.match_clause.with_clause.non_aggregated_variables == [
        "m"
    ]


def test_unique_non_aggregated_variable_solutions_one_aggregation():
    solutions = [
        {"n": "1", "m": "2", "o": "3"},
        {"n": "2", "m": "2", "o": "3"},
        {"n": "3", "m": "2", "o": "3"},
    ]
    non_aggregated_variables = ["m", "o"]
    unique_solutions = WithClause._unique_non_aggregated_variable_solutions(
        solutions, non_aggregated_variables
    )
    assert unique_solutions == [{"m": "2", "o": "3"}]


def test_unique_non_aggregated_variable_solutions_one_aggregation_complex():
    solutions = [
        {"n": "1", "m": "2", "o": "3"},
        {"n": "2", "m": "2", "o": "3"},
        {"n": "3", "m": "2", "o": "3"},
        {"n": "4", "m": "1", "o": "3"},
        {"n": "5", "m": "1", "o": "3"},
    ]
    non_aggregated_variables = ["m", "o"]
    unique_solutions = WithClause._unique_non_aggregated_variable_solutions(
        solutions, non_aggregated_variables
    )
    assert unique_solutions == [
        {"m": "2", "o": "3"},
        {"m": "1", "o": "3"},
    ]


def test_unique_non_aggregated_variable_solutions_two_aggregations_complex():
    solutions = [
        {"n": "1", "m": "2", "o": "3"},
        {"n": "2", "m": "2", "o": "3"},
        {"n": "3", "m": "2", "o": "3"},
        {"n": "4", "m": "1", "o": "3"},
        {"n": "5", "m": "1", "o": "3"},
    ]
    non_aggregated_variables = ["m"]
    unique_solutions = WithClause._unique_non_aggregated_variable_solutions(
        solutions, non_aggregated_variables
    )
    assert unique_solutions == [
        {"m": "2"},
        {"m": "1"},
    ]


def test_transform_solutions_by_aggregations():
    solutions = [
        {"n": "1", "m": "2", "o": "3"},
        {"n": "2", "m": "2", "o": "3"},
        {"n": "3", "m": "2", "o": "3"},
        {"n": "4", "m": "1", "o": "3"},
        {"n": "5", "m": "1", "o": "3"},
    ]
    aggregated_variables = ["n"]
    non_aggregated_variables = ["m", "o"]
    transformed_solutions = WithClause._transform_solutions_by_aggregations(
        solutions, aggregated_variables, non_aggregated_variables
    )
    assert transformed_solutions == [
        {
            "n": [
                "1",
                "2",
                "3",
            ],
            "m": "2",
            "o": "3",
        },
        {
            "n": [
                "4",
                "5",
            ],
            "m": "1",
            "o": "3",
        },
    ]


def test_transform_solutions_in_with_clause(
    fact_collection_6: FactCollection,
):
    cypher = (
        "MATCH (n:Thing {foo: 2})-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]"
        "->(o:OtherThing) "
        "WITH COLLECT(o.foo) AS co, n.foo AS nfoo, m.bar AS mbar "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    aggregated_results = result.parse_tree.cypher.match_clause.with_clause.\
        transform_solutions_by_aggregations(
        fact_collection_6
    )
    expected_results = [
        {
            "n": "4",
            "m": "2",
            "o": ["5", "3"],
        }
    ]

    assert aggregated_results == expected_results


def test_transform_solutions_in_with_clause_no_solutions(
    fact_collection_6: FactCollection,
):
    cypher = (
        "MATCH (n:Thing {foo: 37})-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "WITH COLLECT(o.foo) AS co, n.foo AS nfoo, m.bar AS mbar "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    aggregated_results = result.parse_tree.cypher.match_clause.with_clause.\
        transform_solutions_by_aggregations(
        fact_collection_6
    )
    expected_results = []

    assert aggregated_results == expected_results


def test_transform_solutions_in_with_clause_multiple_solutions(
    fact_collection_6: FactCollection,
):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->"
        "(o:OtherThing) "
        "WITH COLLECT(o.foo) AS co, n.foo AS nfoo, m.bar AS mbar "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    aggregated_results = result.parse_tree.cypher.match_clause.with_clause.\
        transform_solutions_by_aggregations(
        fact_collection_6
    )
    expected_results = [
        {"n": "4", "m": "2", "o": ["5", "3"]},
        {"n": "1", "m": "2", "o": ["5", "3"]},
    ]

    assert aggregated_results == expected_results


def test_apply_substitutions_to_one_projection(
    fact_collection_7: FactCollection,
):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "WITH COLLECT(o.oattr) AS co, n.foo AS nfoo, m.bar AS mbar "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)

    projection = {"n": "4", "m": "2", "o": ["5", "3"]}

    out = result.parse_tree.cypher.match_clause.with_clause._evaluate_one_projection(
        fact_collection_7,
        projection=projection,
    )
    expected_result = {
        "co": Collection([Literal(5), Literal(4)]),
        "nfoo": Literal(2),
        "mbar": Literal(3),
    }
    assert out == expected_result


def test_apply_substitutions_to_projection_list(
    fact_collection_7: FactCollection,
):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "WITH COLLECT(o.oattr) AS co, n.foo AS nfoo, m.bar AS mbar "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)

    solutions = [
        {"n": "4", "m": "2", "o": ["5", "3"]},
        {"n": "1", "m": "2", "o": ["5", "3"]},
    ]

    out = result.parse_tree.cypher.match_clause.with_clause._evaluate(
        fact_collection_7,
        projection=solutions,
    )
    expected_result = [
        {
            "co": Collection([Literal(5), Literal(4)]),
            "nfoo": Literal(2),
            "mbar": Literal(3),
            "__match_solution__": {"n": "4", "m": "2", "o": ["5", "3"]},
        },
        {
            "co": Collection([Literal(5), Literal(4)]),
            "nfoo": Literal(42),
            "mbar": Literal(3),
            "__match_solution__": {"n": "1", "m": "2", "o": ["5", "3"]},
        },
    ]

    assert out == expected_result


def test_distinct_evaluation_removes_duplicates():
    assert Distinct(Collection([Literal(1), Literal(2), Literal(2)]))._evaluate(
        None
    ) == Collection([Literal(1), Literal(2)])


def test_distinct_evaluation_removes_nothing_if_no_duplicates():
    assert Distinct(Collection([Literal(1), Literal(2), Literal(3)]))._evaluate(
        None
    ) == Collection([Literal(1), Literal(2), Literal(3)])


def test_distinct_evaluation_removes_nothing_if_different_types():
    assert Distinct(Collection([Literal(1), Literal(2), Literal("2")]))._evaluate(
        None
    ) == Collection([Literal(1), Literal(2), Literal("2")])


def test_size_of_list():
    assert Size(Collection([Literal(1), Literal(2), Literal("2")]))._evaluate(
        None
    ) == Literal(3)


def test_size_of_empty_list_is_zero():
    assert Size(Collection([]))._evaluate(None) == Literal(0)


def test_size_around_distinct():
    assert Size(Distinct(Collection([Literal(1), Literal(2), Literal(2)])))._evaluate(
        None
    ) == Literal(2)


def test_parse_distinct_keyword_with_collect_no_dups(fact_collection_7):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "WITH DISTINCT COLLECT(o.oattr) AS co, n.foo AS nfoo, m.bar AS mbar "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = [
        {"n": "4", "m": "2", "o": ["5", "3"]},
        {"n": "1", "m": "2", "o": ["5", "3"]},
    ]
    out = result.parse_tree.cypher.match_clause.with_clause._evaluate(
        fact_collection_7,
        projection=solutions,
    )
    expected = [
        {
            "co": Collection([Literal(5), Literal(4)]),
            "nfoo": Literal(2),
            "mbar": Literal(3),
            "__match_solution__": {"n": "4", "m": "2", "o": ["5", "3"]},
        },
        {
            "co": Collection([Literal(5), Literal(4)]),
            "nfoo": Literal(42),
            "mbar": Literal(3),
            "__match_solution__": {"n": "1", "m": "2", "o": ["5", "3"]},
        },
    ]
    assert out == expected


def test_parse_distinct_keyword_with_collect_one_dup(fact_collection_7):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "WITH DISTINCT COLLECT(o.oattr) AS co, n.foo AS nfoo, m.bar AS mbar "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = [
        {"n": "4", "m": "2", "o": ["5", "5"]},
        {"n": "1", "m": "2", "o": ["5", "3"]},
    ]
    out = result.parse_tree.cypher.match_clause.with_clause._evaluate(
        fact_collection_7,
        projection=solutions,
    )
    expected = [
        {
            "co": Collection([Literal(5)]),
            "nfoo": Literal(2),
            "mbar": Literal(3),
            "__match_solution__": {"n": "4", "m": "2", "o": ["5", "5"]},
        },
        {
            "co": Collection([Literal(5), Literal(4)]),
            "nfoo": Literal(42),
            "mbar": Literal(3),
            "__match_solution__": {"n": "1", "m": "2", "o": ["5", "3"]},
        },
    ]
    assert out == expected


def test_evaluate_return_after_with_clause(fact_collection_7):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "WITH DISTINCT COLLECT(o.oattr) AS co, n.foo AS nfoo, m.bar AS mbar "
        "RETURN nfoo, co"
    )
    result = CypherParser(cypher)
    expected = [
        {
            "nfoo": Literal(2),
            "co": Collection([Literal(5), Literal(4)]),
            "__with_clause_projection__": {
                "co": Collection([Literal(5), Literal(4)]),
                "nfoo": Literal(2),
                "mbar": Literal(3),
                "__match_solution__": {"m": "2", "n": "4", "o": ["5", "3"]},
            },
        },
        {
            "nfoo": Literal(42),
            "co": Collection([Literal(5), Literal(4)]),
            "__with_clause_projection__": {
                "co": Collection([Literal(5), Literal(4)]),
                "nfoo": Literal(42),
                "mbar": Literal(3),
                "__match_solution__": {"m": "2", "n": "1", "o": ["5", "3"]},
            },
        },
    ]
    out = result.parse_tree.cypher.return_clause._evaluate(
        fact_collection_7, projection=None
    )
    assert out == expected


def test_tree_mixing_get_parse_object():
    query = "MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar, m.baz AS qux RETURN n.foobar"
    obj = CypherParser(query)
    assert obj.parse_tree.cypher.match_clause.pattern.parse_obj is obj.parse_tree


def test_collection_children():
    # Create a mock Evaluable object
    mock_evaluable = Collection(values=[])

    # Create a Collection instance with the mock evaluable object
    collection = Collection(values=[mock_evaluable])

    # Get the children of the collection
    children = list(collection.children)

    # Assert that the children list contains the mock evaluable object
    assert children == [mock_evaluable]


def test_distinct_children():
    # Create a mock Collection object
    mock_collection = Collection(values=[])

    # Create a Distinct instance with the mock collection
    distinct = Distinct(collection=mock_collection)

    # Get the children of the distinct instance
    children = list(distinct.children)

    # Assert that the children list contains the mock collection
    assert children == [mock_collection]


def test_size_children():
    # Create a mock Collection object
    mock_collection = Collection(values=[])

    # Create a Size instance with the mock collection
    size = Size(collection=mock_collection)

    # Get the children of the size instance
    children = list(size.children)

    # Assert that the children list contains the mock collection
    assert children == [mock_collection]


def test_distinct_collect_children():
    # Create a mock ObjectAttributeLookup object
    mock_object_attribute_lookup = ObjectAttributeLookup(
        "object_name", "attribute_name"
    )

    # Create a Collect instance with the mock object attribute lookup
    collect = Collect(object_attribute_lookup=mock_object_attribute_lookup)

    # Get the children of the collect instance
    children = list(collect.children)

    # Assert that the children list contains the mock object attribute lookup
    assert children == [mock_object_attribute_lookup]


def test_cypher_collection_children():
    # Create a mock Evaluable object
    mock_evaluable = Collection(values=[])

    # Create a Collection instance with the mock evaluable object
    collection = Collection(values=[mock_evaluable])

    # Get the children of the collection
    children = list(collection.children)

    # Assert that the children list contains the mock evaluable object
    assert children == [mock_evaluable]


def test_cypher_distinct_children():
    # Create a mock Collection object
    mock_collection = Collection(values=[])

    # Create a Distinct instance with the mock collection
    distinct = Distinct(collection=mock_collection)

    # Get the children of the distinct instance
    children = list(distinct.children)

    # Assert that the children list contains the mock collection
    assert children == [mock_collection]


def test_cypher_size_children():
    # Create a mock Collection object
    mock_collection = Collection(values=[])

    # Create a Size instance with the mock collection
    size = Size(collection=mock_collection)

    # Get the children of the size instance
    children = list(size.children)

    # Assert that the children list contains the mock collection
    assert children == [mock_collection]


def test_cypher_collect_children():
    # Create a mock ObjectAttributeLookup object
    mock_object_attribute_lookup = ObjectAttributeLookup(
        "object_name", "attribute_name"
    )

    # Create a Collect instance with the mock object attribute lookup
    collect = Collect(object_attribute_lookup=mock_object_attribute_lookup)

    # Get the children of the collect instance
    children = list(collect.children)

    # Assert that the children list contains the mock object attribute lookup
    assert children == [mock_object_attribute_lookup]


def test_aggregation_children():
    # Create a mock Evaluable object
    mock_evaluable = Collection(values=[])

    # Create an Aggregation instance with the mock evaluable object
    aggregation = Aggregation(aggregation=mock_evaluable)

    # Get the children of the aggregation instance
    children = list(aggregation.children)

    # Assert that the children list contains the mock evaluable object
    assert children == [mock_evaluable]


def test_collect_evaluate():
    # Create a mock ObjectAttributeLookup object
    mock_object_attribute_lookup = ObjectAttributeLookup(
        "object_name", "attribute_name"
    )

    # Create a Collect instance with the mock object attribute lookup
    collect = Collect(object_attribute_lookup=mock_object_attribute_lookup)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"object_name": ["instance1", "instance2"]}

    # Mock the _evaluate method of ObjectAttributeLookup
    def mock_evaluate(_, projection):
        return Literal(projection["object_name"])

    mock_object_attribute_lookup._evaluate = mock_evaluate

    # Evaluate the collect instance
    result = collect._evaluate(mock_fact_collection, projection=mock_projection)

    # Assert that the result is a Collection with the expected values
    assert isinstance(result, Collection)
    assert result.values == [Literal("instance1"), Literal("instance2")]


def test_collect_evaluate_empty_projection():
    # Create a mock ObjectAttributeLookup object
    mock_object_attribute_lookup = ObjectAttributeLookup(
        "object_name", "attribute_name"
    )

    # Create a Collect instance with the mock object attribute lookup
    collect = Collect(object_attribute_lookup=mock_object_attribute_lookup)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create an empty mock projection
    mock_projection = {"object_name": []}

    # Mock the _evaluate method of ObjectAttributeLookup
    def mock_evaluate(_, projection):
        return Literal(projection["object_name"])

    mock_object_attribute_lookup._evaluate = mock_evaluate

    # Evaluate the collect instance
    result = collect._evaluate(mock_fact_collection, projection=mock_projection)

    # Assert that the result is a Collection with no values
    assert isinstance(result, Collection)
    assert result.values == []


def test_collect_evaluate_with_projection():
    # Create a mock ObjectAttributeLookup object
    mock_object_attribute_lookup = ObjectAttributeLookup(
        "object_name", "attribute_name"
    )

    # Create a Collect instance with the mock object attribute lookup
    collect = Collect(object_attribute_lookup=mock_object_attribute_lookup)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"object_name": ["instance1", "instance2"]}

    # Mock the _evaluate method of ObjectAttributeLookup
    def mock_evaluate(_, projection):
        return Literal(projection["object_name"])

    mock_object_attribute_lookup._evaluate = mock_evaluate

    # Evaluate the collect instance
    result = collect._evaluate(mock_fact_collection, projection=mock_projection)

    # Assert that the result is a Collection with the expected values
    assert isinstance(result, Collection)
    assert result.values == [Literal("instance1"), Literal("instance2")]


def test_query_children():
    # Create a mock Collection object
    mock_collection = Collection(values=[])

    # Create a Cypher instance with the mock collection
    cypher = Cypher(cypher=mock_collection)

    # Get the children of the Cypher instance
    children = list(cypher.children)

    # Assert that the children list contains the mock collection
    assert children == [mock_collection]


def test_trigger_gather_constraints_to_match():
    query = (
        """MATCH (n:Thing {key1: "value", key2: 5})-[r]->(m:OtherThing {key3: "hithere"}) """
        """WHERE n.key = 2, n.foo = 3 """
        """RETURN n.foobar, n.baz"""
    )
    obj = CypherParser(query)
    obj.parse_tree.trigger_gather_constraints_to_match()
    assert len(obj.parse_tree.cypher.match_clause.constraints) == 18  # check this


def test_trigger_gather_constraints_to_match_no_match():
    class MockCypher(Cypher):  # pylint: disable=too-few-public-methods,missing-class-docstring
        def walk(self):
            return [Collection(values=[]), Collection(values=[])]

    # Create a mock Cypher instance
    mock_cypher = MockCypher(cypher=Collection(values=[]))

    # Trigger the gathering of constraints
    mock_cypher.trigger_gather_constraints_to_match()

    # Check that no constraints were gathered since there are no Match instances
    for node in mock_cypher.walk():
        if isinstance(node, Collection):
            assert not hasattr(node, "constraints_gathered")


def test_match_children_with_all_attributes():
    # Create mock objects for pattern, where, and with_clause
    mock_pattern = Collection(values=[])
    mock_where = Collection(values=[])
    mock_with_clause = Collection(values=[])

    # Create a Match instance with all attributes
    match = Match(
        pattern=mock_pattern,
        where_clause=mock_where,
        with_clause=mock_with_clause,
    )

    # Get the children of the match instance
    children = list(match.children)

    # Assert that the children list contains the mock pattern, where, and with_clause
    assert children == [mock_pattern, mock_where, mock_with_clause]


def test_match_children_with_pattern_only():
    # Create a mock object for pattern
    mock_pattern = Collection(values=[])

    # Create a Match instance with only the pattern attribute
    match = Match(pattern=mock_pattern, where_clause=None, with_clause=None)

    # Get the children of the match instance
    children = list(match.children)

    # Assert that the children list contains only the mock pattern
    assert children == [mock_pattern]


def test_match_children_with_pattern_and_where():
    # Create mock objects for pattern and where
    mock_pattern = Collection(values=[])
    mock_where = Collection(values=[])

    # Create a Match instance with pattern and where attributes
    match = Match(pattern=mock_pattern, where_clause=mock_where, with_clause=None)

    # Get the children of the match instance
    children = list(match.children)

    # Assert that the children list contains the mock pattern and where
    assert children == [mock_pattern, mock_where]


def test_match_children_with_pattern_and_with_clause():
    # Create mock objects for pattern and with_clause
    mock_pattern = Collection(values=[])
    mock_with_clause = Collection(values=[])

    # Create a Match instance with pattern and with_clause attributes
    match = Match(pattern=mock_pattern, where_clause=None, with_clause=mock_with_clause)

    # Get the children of the match instance
    children = list(match.children)

    # Assert that the children list contains the mock pattern and with_clause
    assert children == [mock_pattern, mock_with_clause]


def test_return_children():
    # Create a mock Projection object
    mock_projection = Projection(lookups=[])

    # Create a Return instance with the mock projection
    return_clause = Return(node=mock_projection)

    # Get the children of the return instance
    children = list(return_clause.children)

    # Assert that the children list contains the mock projection
    assert children == [mock_projection]


def test_projection_children():
    # Create mock ObjectAttributeLookup objects
    mock_lookup1 = ObjectAttributeLookup("object1", "attribute1")
    mock_lookup2 = ObjectAttributeLookup("object2", "attribute2")

    # Create a Projection instance with the mock lookups
    projection = Projection(lookups=[mock_lookup1, mock_lookup2])

    # Get the children of the projection instance
    children = list(projection.children)

    # Assert that the children list contains the mock lookups
    assert children == [mock_lookup1, mock_lookup2]


def test_projection_children_empty():
    # Create a Projection instance with no lookups
    projection = Projection(lookups=[])

    # Get the children of the projection instance
    children = list(projection.children)

    # Assert that the children list is empty
    assert not children


def test_node_constraints_with_label():
    # Create a mock NodeNameLabel with a label
    mock_node_name_label = NodeNameLabel(name="node1", label="Label1")

    # Create a Node instance with the mock NodeNameLabel
    node = Node(node_name_label=mock_node_name_label)

    # Get the constraints of the node
    constraints = node.constraints

    # Assert that the constraints list contains the expected ConstraintNodeHasLabel
    assert len(constraints) == 1
    assert isinstance(constraints[0], ConstraintNodeHasLabel)
    assert constraints[0].variable == "node1"
    assert constraints[0].label == "Label1"


def test_node_constraints_without_label():
    # Create a mock NodeNameLabel without a label
    mock_node_name_label = NodeNameLabel(name="node1", label=None)

    # Create a Node instance with the mock NodeNameLabel
    node = Node(node_name_label=mock_node_name_label)

    # Get the constraints of the node
    constraints = node.constraints

    # Assert that the constraints list is empty
    assert len(constraints) == 0


def test_evaluate_with_projection():
    # Create a mock Projection object with lookups
    mock_lookup1 = ObjectAttributeLookup("object1", "attribute1")
    mock_lookup2 = ObjectAttributeLookup("object2", "attribute2")
    mock_projection = Projection(lookups=[mock_lookup1, mock_lookup2])

    # Create a Return instance with the mock projection
    return_clause = Return(node=mock_projection)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection_data = [
        {"object1": "value1", "object2": "value2"},
        {"object1": "value3", "object2": "value4"},
    ]

    # Mock the _evaluate method of the parent match_clause.with_clause
    class MockWithClause:  # pylint: disable=too-few-public-methods,missing-class-docstring
        def _evaluate(self, *args, **kwargs):  # pylint: disable=unused-argument
            return mock_projection_data

    class MockMatchClause:  # pylint: disable=too-few-public-methods,missing-class-docstring
        with_clause = MockWithClause()

    class MockParent:  # pylint: disable=too-few-public-methods,missing-class-docstring
        match_clause = MockMatchClause()

    return_clause.parent = MockParent()

    # Evaluate the return instance
    result = return_clause._evaluate(mock_fact_collection, projection=None)

    # Assert that the result is as expected
    # expected_result = [
    #     {"object1": "value1", "object2": "value2"},
    #     {"object1": "value3", "object2": "value4"},
    # ]
    expected_result = [
        {
            "object1": "value1",
            "object2": "value2",
            "__with_clause_projection__": {
                "object1": "value1",
                "object2": "value2",
            },
        },
        {
            "object1": "value3",
            "object2": "value4",
            "__with_clause_projection__": {
                "object1": "value3",
                "object2": "value4",
            },
        },
    ]
    assert result == expected_result


def test_evaluate_with_given_projection():
    # Create a mock Projection object with lookups
    mock_lookup1 = ObjectAttributeLookup("object1", "attribute1")
    mock_lookup2 = ObjectAttributeLookup("object2", "attribute2")
    mock_projection = Projection(lookups=[mock_lookup1, mock_lookup2])

    # Create a Return instance with the mock projection
    return_clause = Return(node=mock_projection)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a given projection
    given_projection = [
        {"object1": "value1", "object2": "value2"},
        {"object1": "value3", "object2": "value4"},
    ]

    # Evaluate the return instance with the given projection
    result = return_clause._evaluate(mock_fact_collection, projection=given_projection)

    # Assert that the result is as expected
    expected_result = [
        {
            "object1": "value1",
            "object2": "value2",
            "__with_clause_projection__": {
                "object1": "value1",
                "object2": "value2",
            },
        },
        {
            "object1": "value3",
            "object2": "value4",
            "__with_clause_projection__": {
                "object1": "value3",
                "object2": "value4",
            },
        },
    ]
    assert result == expected_result


def test_node_name_label_children_with_label():
    # Create a NodeNameLabel instance with a name and label
    node_name_label = NodeNameLabel(name="node1", label="Label1")

    # Get the children of the NodeNameLabel instance
    children = list(node_name_label.children)

    # Assert that the children list contains the name and label
    assert children == ["node1", "Label1"]


def test_node_name_label_children_without_label():
    # Create a NodeNameLabel instance with a name and no label
    node_name_label = NodeNameLabel(name="node1", label=None)

    # Get the children of the NodeNameLabel instance
    children = list(node_name_label.children)

    # Assert that the children list contains only the name
    assert children == ["node1"]


def test_node_children_with_node_name_label_only():
    # Create a mock NodeNameLabel
    mock_node_name_label = NodeNameLabel(name="node1", label="Label1")

    # Create a Node instance with the mock NodeNameLabel and no mappings
    node = Node(node_name_label=mock_node_name_label, mapping_list=[])

    # Get the children of the node instance
    children = list(node.children)

    # Assert that the children list contains only the mock NodeNameLabel
    assert children == [mock_node_name_label]


def test_node_children_empty():
    # Create a Node instance with no NodeNameLabel and no mappings
    node = Node(node_name_label=None, mapping_list=[])

    # Get the children of the node instance
    children = list(node.children)

    # Assert that the children list is empty
    assert not children


def test_relationship_children():
    # Create a mock NodeNameLabel
    mock_name_label = NodeNameLabel(name="relationship1", label="Label1")

    # Create a Relationship instance with the mock NodeNameLabel
    relationship = Relationship(name_label=mock_name_label)

    # Get the children of the relationship instance
    children = list(relationship.children)

    # Assert that the children list contains the mock NodeNameLabel
    assert children == [mock_name_label]


def test_relationship_chain_children():
    # Create mock Relationship objects
    mock_relationship1 = Relationship(
        name_label=NodeNameLabel(name="relationship1", label="Label1")
    )
    mock_relationship2 = Relationship(
        name_label=NodeNameLabel(name="relationship2", label="Label2")
    )

    # Create a RelationshipChain instance with the mock relationships
    relationship_chain = RelationshipChain(
        steps=[mock_relationship1, mock_relationship2]
    )

    # Get the children of the relationship chain instance
    children = list(relationship_chain.children)

    # Assert that the children list contains the mock relationships
    assert children == [mock_relationship1, mock_relationship2]


def test_relationship_left_right_children():
    # Create a mock Relationship object
    mock_relationship = Relationship(
        name_label=NodeNameLabel(name="relationship1", label="Label1")
    )

    # Create a RelationshipLeftRight instance with the mock relationship
    relationship_left_right = RelationshipLeftRight(relationship=mock_relationship)

    # Get the children of the relationship left-right instance
    children = list(relationship_left_right.children)

    # Assert that the children list contains the mock relationship
    assert children == [mock_relationship]


def test_relationship_right_left_children():
    # Create a mock Relationship object
    mock_relationship = Relationship(
        name_label=NodeNameLabel(name="relationship1", label="Label1")
    )

    # Create a RelationshipRightLeft instance with the mock relationship
    relationship_right_left = RelationshipRightLeft(relationship=mock_relationship)

    # Get the children of the relationship right-left instance
    children = list(relationship_right_left.children)

    # Assert that the children list contains the mock relationship
    assert children == [mock_relationship]


def test_mapping_set_children():
    # Create mock Mapping objects
    mock_mapping1 = Mapping(key="key1", value="value1")
    mock_mapping2 = Mapping(key="key2", value="value2")

    # Create a MappingSet instance with the mock mappings
    mapping_set = MappingSet(mappings=[mock_mapping1, mock_mapping2])

    # Get the children of the mapping set instance
    children = list(mapping_set.children)

    # Assert that the children list contains the mock mappings
    assert children == [mock_mapping1, mock_mapping2]


def test_mapping_set_children_empty():
    # Create a MappingSet instance with no mappings
    mapping_set = MappingSet(mappings=[])

    # Get the children of the mapping set instance
    children = list(mapping_set.children)

    # Assert that the children list is empty
    assert not children


def test_relationship_chain_list_children():
    # Create mock RelationshipChain objects
    mock_relationship_chain1 = RelationshipChain(steps=[])
    mock_relationship_chain2 = RelationshipChain(steps=[])

    # Create a RelationshipChainList instance with the mock relationship chains
    relationship_chain_list = RelationshipChainList(
        relationships=[mock_relationship_chain1, mock_relationship_chain2]
    )

    # Get the children of the relationship chain list instance
    children = list(relationship_chain_list.children)

    # Assert that the children list contains the mock relationship chains
    assert children == [mock_relationship_chain1, mock_relationship_chain2]


def test_addition_children():
    # Create mock TreeMixin objects
    mock_left = Literal(value=1)
    mock_right = Literal(value=2)

    # Create an Addition instance with the mock left and right operands
    addition = Addition(left=mock_left, right=mock_right)

    # Get the children of the addition instance
    children = list(addition.children)

    # Assert that the children list contains the mock left and right operands
    assert children == [mock_left, mock_right]


def test_where_children():
    # Create a mock Predicate object
    class MockPredicate(Predicate):  # pylint: disable=missing-class-docstring
        def tree(self):
            return "Predicate Tree"

    mock_predicate = MockPredicate(
        left_side=Literal(value=1), right_side=Literal(value=2)
    )

    # Create a Where instance with the mock predicate
    where_clause = Where(predicate=mock_predicate)

    # Get the children of the where instance
    children = list(where_clause.children)

    # Assert that the children list contains the mock predicate
    assert children == [mock_predicate]


def test_aliased_name_children():
    # Create an AliasedName instance with a name
    aliased_name = AliasedName(name="alias")

    # Get the children of the AliasedName instance
    children = list(aliased_name.children)

    # Assert that the children list contains the name
    assert children == ["alias"]


def test_evaluate_equals():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=5)

    # Create an Equals instance with the mock literals
    equals = Equals(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the equals instance
    result = equals._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value True
    assert isinstance(result, Literal)
    assert result.value is True


def test_evaluate_equals_false():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create an Equals instance with the mock literals
    equals = Equals(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the equals instance
    result = equals._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value False
    assert isinstance(result, Literal)
    assert result.value is False


def test_evaluate_equals_with_projection():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=5)

    # Create an Equals instance with the mock literals
    equals = Equals(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the equals instance with the projection
    result = equals._evaluate(mock_fact_collection, projection=mock_projection)

    # Assert that the result is a Literal with the value True
    assert isinstance(result, Literal)
    assert result.value is True


def test_evaluate_equals_with_projection_false():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create an Equals instance with the mock literals
    equals = Equals(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the equals instance with the projection
    result = equals._evaluate(mock_fact_collection, projection=mock_projection)

    # Assert that the result is a Literal with the value False
    assert isinstance(result, Literal)
    assert result.value is False


def test_evaluate_less_than():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create a LessThan instance with the mock literals
    less_than = LessThan(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the less_than instance
    result = less_than._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value True
    assert isinstance(result, Literal)
    assert result.value is True


def test_evaluate_less_than_false():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=5)

    # Create a LessThan instance with the mock literals
    less_than = LessThan(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the less_than instance
    result = less_than._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value False
    assert isinstance(result, Literal)
    assert result.value is False


def test_evaluate_less_than_with_projection():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create a LessThan instance with the mock literals
    less_than = LessThan(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the less_than instance with the projection
    result = less_than._evaluate(mock_fact_collection, projection=mock_projection)

    # Assert that the result is a Literal with the value True
    assert isinstance(result, Literal)
    assert result.value is True


def test_evaluate_less_than_with_projection_false():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=5)

    # Create a LessThan instance with the mock literals
    less_than = LessThan(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the less_than instance with the projection
    result = less_than._evaluate(mock_fact_collection, projection=mock_projection)

    # Assert that the result is a Literal with the value False
    assert isinstance(result, Literal)
    assert result.value is False


def test_evaluate_greater_than():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=5)

    # Create a GreaterThan instance with the mock literals
    greater_than = GreaterThan(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the greater_than instance
    result = greater_than._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value True
    assert isinstance(result, Literal)
    assert result.value is True


def test_evaluate_greater_than_false():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create a GreaterThan instance with the mock literals
    greater_than = GreaterThan(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the greater_than instance
    result = greater_than._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value False
    assert isinstance(result, Literal)
    assert result.value is False


def test_evaluate_greater_than_with_projection():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=5)

    # Create a GreaterThan instance with the mock literals
    greater_than = GreaterThan(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the greater_than instance with the projection
    result = greater_than._evaluate(mock_fact_collection, projection=mock_projection)

    # Assert that the result is a Literal with the value True
    assert isinstance(result, Literal)
    assert result.value is True


def test_evaluate_greater_than_with_projection_false():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create a GreaterThan instance with the mock literals
    greater_than = GreaterThan(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the greater_than instance with the projection
    result = greater_than._evaluate(mock_fact_collection, projection=mock_projection)

    # Assert that the result is a Literal with the value False
    assert isinstance(result, Literal)
    assert result.value is False


def test_evaluate_subtraction():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=5)

    # Create a Subtraction instance with the mock literals
    subtraction = Subtraction(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the subtraction instance
    result = subtraction._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value 5
    assert isinstance(result, Literal)
    assert result.value == 5


def test_evaluate_subtraction_negative_result():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create a Subtraction instance with the mock literals
    subtraction = Subtraction(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the subtraction instance
    result = subtraction._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value -5
    assert isinstance(result, Literal)
    assert result.value == -5


def test_evaluate_subtraction_with_projection():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=5)

    # Create a Subtraction instance with the mock literals
    subtraction = Subtraction(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the subtraction instance with the projection
    result = subtraction._evaluate(mock_fact_collection, projection=mock_projection)

    # Assert that the result is a Literal with the value 5
    assert isinstance(result, Literal)
    assert result.value == 5


def test_evaluate_subtraction_with_projection_negative_result():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create a Subtraction instance with the mock literals
    subtraction = Subtraction(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the subtraction instance with the projection
    result = subtraction._evaluate(mock_fact_collection, projection=mock_projection)

    # Assert that the result is a Literal with the value -5
    assert isinstance(result, Literal)
    assert result.value == -5


def test_evaluate_multiplication():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create a Multiplication instance with the mock literals
    multiplication = Multiplication(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the multiplication instance
    result = multiplication._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value 50
    assert isinstance(result, Literal)
    assert result.value == 50


def test_evaluate_multiplication_with_projection():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create a Multiplication instance with the mock literals
    multiplication = Multiplication(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the multiplication instance with the projection
    result = multiplication._evaluate(mock_fact_collection, projection=mock_projection)

    # Assert that the result is a Literal with the value 50
    assert isinstance(result, Literal)
    assert result.value == 50


def test_evaluate_multiplication_negative():
    # Create mock Literal objects
    mock_left = Literal(value=-5)
    mock_right = Literal(value=10)

    # Create a Multiplication instance with the mock literals
    multiplication = Multiplication(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the multiplication instance
    result = multiplication._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value -50
    assert isinstance(result, Literal)
    assert result.value == -50


def test_evaluate_multiplication_zero():
    # Create mock Literal objects
    mock_left = Literal(value=0)
    mock_right = Literal(value=10)

    # Create a Multiplication instance with the mock literals
    multiplication = Multiplication(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the multiplication instance
    result = multiplication._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value 0
    assert isinstance(result, Literal)
    assert result.value == 0


def test_evaluate_multiplication_with_projection_negative():
    # Create mock Literal objects
    mock_left = Literal(value=-5)
    mock_right = Literal(value=10)

    # Create a Multiplication instance with the mock literals
    multiplication = Multiplication(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the multiplication instance with the projection
    result = multiplication._evaluate(mock_fact_collection, projection=mock_projection)

    # Assert that the result is a Literal with the value -50
    assert isinstance(result, Literal)
    assert result.value == -50


def test_evaluate_division():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=2)

    # Create a Division instance with the mock literals
    division = Division(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the division instance
    result = division._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value 5
    assert isinstance(result, Literal)
    assert result.value == 5


def test_evaluate_division_with_projection():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=2)

    # Create a Division instance with the mock literals
    division = Division(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the division instance with the projection
    result = division._evaluate(mock_fact_collection, projection=mock_projection)  # pylint: disable=protected-access

    # Assert that the result is a Literal with the value 5
    assert isinstance(result, Literal)
    assert result.value == 5


def test_evaluate_division_by_zero():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=0)

    # Create a Division instance with the mock literals
    division = Division(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # # Evaluate the division instance and expect an exception
    # with pytest.raises(ZeroDivisionError):
    with pytest.raises(WrongCypherTypeError):
        division._evaluate(mock_fact_collection)  # pylint: disable=protected-access


def test_evaluate_division_negative():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=-2)

    # Create a Division instance with the mock literals
    division = Division(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the division instance
    result = division._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value -5
    assert isinstance(result, Literal)
    assert result.value == -5


def test_evaluate_division_with_projection_negative():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=-2)

    # Create a Division instance with the mock literals
    division = Division(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the division instance with the projection
    result = division._evaluate(mock_fact_collection, projection=mock_projection)

    # Assert that the result is a Literal with the value -5
    assert isinstance(result, Literal)
    assert result.value == -5


def test_is_true_eq_same_predicate():
    predicate = Mock()
    constraint1 = IsTrue(predicate)
    constraint2 = IsTrue(predicate)
    assert constraint1 == constraint2


def test_is_true_eq_different_predicate():
    predicate1 = Mock()
    predicate2 = Mock()
    constraint1 = IsTrue(predicate1)
    constraint2 = IsTrue(predicate2)
    assert constraint1 != constraint2


def test_is_true_eq_different_type():
    predicate = Mock()
    constraint = IsTrue(predicate)
    assert constraint != "not a constraint"


def test_fact_node_has_label_matches_constraint():
    constraint1 = ConstraintNodeHasLabel("n", "Thingy")
    fact1 = FactNodeHasLabel("123", "Thingy")
    assert fact1 + constraint1 == {"n": "123"}


def test_fact_matching_error_on_non_constraint():
    with pytest.raises(ValueError):
        fact1 = FactNodeHasLabel("123", "Thingy")
        thing = "Thingy"
        fact1 + thing  # pylint: disable=pointless-statement


def test_fact_node_has_label_no_match_empty_result():
    constraint1 = ConstraintNodeHasLabel("n", "Thingy")
    fact1 = FactNodeHasLabel("123", "NotThingy")
    assert fact1 + constraint1 is None


def test_fact_node_has_label_no_match_wrong_constraint_type():
    constraint1 = ConstraintRelationshipHasLabel("r", "Thingy")
    fact1 = FactNodeHasLabel("123", "Thingy")
    assert fact1 + constraint1 is None


def test_fact_relationship_has_label_matches_constraint():
    fact2 = FactRelationshipHasLabel("234", "Relationship")
    constraint2 = ConstraintRelationshipHasLabel("r", "Relationship")
    assert fact2 + constraint2 == {"r": "234"}


def test_fact_relationship_has_label_matching_error_on_non_constraint():
    with pytest.raises(ValueError):
        fact2 = FactRelationshipHasLabel("234", "Relationship")
        thing = "Thingy"
        fact2 + thing  # pylint: disable=pointless-statement


def test_fact_relationship_has_label_no_match_empty_result():
    fact2 = FactRelationshipHasLabel("234", "Relationship")
    constraint2 = ConstraintRelationshipHasLabel("r", "NotTheRelationship")
    assert fact2 + constraint2 is None


def test_fact_relationship_has_label_no_match_wrong_constraint_type():
    fact2 = FactRelationshipHasLabel("234", "Relationship")
    constraint2 = ConstraintNodeHasLabel("r", "NotRightAtAll")
    assert fact2 + constraint2 is None


def test_initialize_empty_session(empty_session):
    assert isinstance(empty_session, Session)
    assert len(empty_session.fact_collection) == 0
    assert not empty_session.trigger_dict


def test_empty_session_fact_collection_empty(empty_session):
    assert len(empty_session.fact_collection) == 0


def test_add_fact_collection_to_session(empty_session, fact_collection_1):
    assert len(empty_session.fact_collection) == 0
    empty_session.attach_fact_collection(fact_collection_1)
    assert len(empty_session.fact_collection) == 3


def test_fact_collection_empty():
    fact_collection = FactCollection(facts=[])
    assert fact_collection.is_empty()


def test_fact_collection_not_empty(fact_collection_0):
    assert not fact_collection_0.is_empty()


def test_argument_propagates_from_function_signature(
    empty_session,
):
    @empty_session.trigger("MATCH (n:Thingy) RETURN n.foo")
    def test_function(n) -> VariableAttribute["n", "bar"]:  # type: ignore
        return 1

    assert list(empty_session.trigger_dict.values())[0].parameter_names == ["n"]


def test_trigger_decorator_function_works(empty_session):
    @empty_session.trigger("MATCH (n) RETURN n.foo")
    def test_function(n) -> VariableAttribute["n", "bar"]:  # type: ignore
        return 1

    assert list(empty_session.trigger_dict.values())[0].function(1) == 1


def test_trigger_decorator_function_has_return_variable(empty_session):
    @empty_session.trigger("MATCH (n) RETURN n.foo")
    def test_function(n) -> VariableAttribute["n", "bar"]:  # type: ignore
        return 1

    assert list(empty_session.trigger_dict.values())[0].variable_set == "n"


def test_trigger_decorator_function_has_variable_attribute_trigger_type(
    empty_session,
):
    @empty_session.trigger("MATCH (n) RETURN n.foo")
    def test_function(n) -> VariableAttribute["n", "bar"]:  # type: ignore
        return 1

    assert isinstance(
        list(empty_session.trigger_dict.values())[0], VariableAttributeTrigger
    )


def test_relationship_trigger_decorator_function_has_return_source_variable(
    empty_session,
):
    @empty_session.trigger(
        "MATCH (n)-[m:relationshipthingy]->(m) WITH n.foo AS foo, m.bar AS bar RETURN foo, bar"
    )
    def test_function(n) -> NodeRelationship["n", "relationshipthingy", "m"]:  # type: ignore
        return 1

    assert list(empty_session.trigger_dict.values())[0].source_variable == "n"


def test_relationship_trigger_decorator_function_has_return_target_variable(
    empty_session,
):
    @empty_session.trigger(
        "MATCH (n)-[m:relationshipthingy]->(m) WITH n.foo AS foo, m.bar AS bar RETURN foo, bar"
    )
    def test_function(foo, bar) -> NodeRelationship["n", "relationshipthingy", "m"]:  # pylint: disable=unused-argument
        return 1

    assert list(empty_session.trigger_dict.values())[0].target_variable == "m"


def test_relationship_trigger_decorator_function_has_return_relationship_name(
    empty_session,
):
    @empty_session.trigger(
        "MATCH (n)-[s:relationshipthingy]->(t) WITH n.foo AS foo, t.bar AS bar RETURN foo, bar"
    )
    def test_function(foo, bar) -> NodeRelationship["n", "relationshipthingy", "t"]:  # type: ignore
        return 1

    assert (
        list(empty_session.trigger_dict.values())[0].relationship_name
        == "relationshipthingy"
    )


def test_relationship_trigger_decorator_function_has_relationship_trigger_type(
    empty_session,
):
    @empty_session.trigger(
        "MATCH (n)-[s:relationshipthingy]->(t) WITH n.foo AS foo, t.bar AS bar RETURN foo, bar"
    )
    def test_function(n) -> NodeRelationship["n", "relationshipthingy", "t"]:  # type: ignore
        return 1

    assert isinstance(
        list(empty_session.trigger_dict.values())[0], NodeRelationshipTrigger
    )


def test_trigger_decorator_function_insert_to_dict(empty_session):
    @empty_session.trigger("MATCH (n) RETURN n.foo")
    def test_function(n) -> VariableAttribute["n", "bar"]:  # type: ignore
        return n + 1

    assert list(empty_session.trigger_dict.values())[0].function(1) == 2


def test_reject_non_fact_collection_on_attach(empty_session):
    with pytest.raises(ValueError):
        empty_session.attach_fact_collection("not a fact collection")


def test_attach_fact_collection_manually(empty_session):
    fact_collection = FactCollection(facts=[])
    empty_session.attach_fact_collection(fact_collection)
    assert empty_session.fact_collection is fact_collection


def test_session_decorator_registers_trigger(empty_session):
    @empty_session.trigger("MATCH (n) RETURN n.foo")
    def test_function(n) -> VariableAttribute["n", "bar"]:  # type: ignore
        return 1

    assert (
        list(empty_session.trigger_dict.values())[0].function.__name__
        == "test_function"
    )
    assert list(empty_session.trigger_dict.values())[0].function("hithere") == 1


def test_iadd_session_fact_collection(mocker, empty_session):
    mocker.patch.object(empty_session, "attach_fact_collection")
    fact_collection = FactCollection(facts=[])
    empty_session += fact_collection
    empty_session.attach_fact_collection.assert_called_once_with(fact_collection)


def test_iadd_raises_value_error(empty_session):
    with pytest.raises(ValueError):
        empty_session += "not a fact collection"


def test_trigger_has_constraint_from_cypher():
    trigger = VariableAttributeTrigger(
        function=lambda x: x,
        cypher_string="MATCH (n:Thingy) RETURN n.foo",
    )
    constraint = ConstraintNodeHasLabel("n", "Thingy")
    assert constraint in trigger.constraints
    assert len(trigger.constraints) == 1


def test_constraint_propogates_to_session_from_decorator(empty_session):
    assert not empty_session.constraints

    @empty_session.trigger("MATCH (n:Thingy) RETURN n.foo")
    def test_function(n) -> VariableAttribute["n", "bar"]:  # type: ignore
        return 1

    assert empty_session.constraints


def test_fact_matches_constraint_in_session(empty_session):
    @empty_session.trigger("MATCH (n:Thingy) RETURN n.foo")
    def test_function(n) -> VariableAttribute["n", "bar"]:  # type: ignore
        return 1

    fact = FactNodeHasLabel("123", "Thingy")

    assert fact + empty_session.constraints[0] == {"n": "123"}


def test_fact_matches_constraint_generator_in_session(empty_session):
    @empty_session.trigger("MATCH (n:Thingy) RETURN n.foo")
    def test_function(n) -> VariableAttribute["n", "bar"]:  # type: ignore
        return 1

    fact_list = [FactNodeHasLabel("123", "Thingy")]

    assert list(empty_session.facts_matching_constraints(fact_list)) == [
        (
            fact_list[0],
            ConstraintNodeHasLabel(variable="n", label="Thingy"),
            {"n": "123"},
        )
    ]


def test_fact_does_not_match_wrong_constraint_generator_in_session(
    empty_session,
):
    @empty_session.trigger("MATCH (n:NotTheThingy) RETURN n.foo")
    def test_function(n) -> VariableAttribute["n", "bar"]:  # type: ignore
        return 1

    fact_list = [FactNodeHasLabel("123", "Thingy")]

    assert not list(empty_session.facts_matching_constraints(fact_list))


def test_fact_matches_exactly_one_constraint_generator_in_session(
    empty_session,
):
    @empty_session.trigger("MATCH (n:Thingy) RETURN n.foo")
    def test_function(n) -> VariableAttribute["n", "bar"]:  # type: ignore
        return 1

    fact_list = [
        FactNodeHasLabel("123", "Thingy"),
        FactNodeHasLabel("456", "NotTheThingy"),
    ]

    assert list(empty_session.facts_matching_constraints(fact_list)) == [
        (
            fact_list[0],
            ConstraintNodeHasLabel(variable="n", label="Thingy"),
            {"n": "123"},
        )
    ]


def test_variable_propagates_from_return_annotation(
    empty_session,
):
    @empty_session.trigger("MATCH (n:Thingy) RETURN n.foo")
    def test_function(n) -> VariableAttribute["n", "bar"]:  # type: ignore
        return 1

    assert list(empty_session.trigger_dict.values())[0].variable_set == "n"


def test_attribute_propagates_from_return_annotation(
    empty_session,
):
    @empty_session.trigger("MATCH (n:Thingy) RETURN n.foo")
    def test_function(n) -> VariableAttribute["n", "bar"]:  # type: ignore
        return 1

    assert list(empty_session.trigger_dict.values())[0].attribute_set == "bar"


def test_decorated_function_requires_return_annotation(empty_session):
    with pytest.raises(ValueError):

        @empty_session.trigger("MATCH (n:Thingy) RETURN n.foo")
        def test_function(n):  # pylint: disable=unused-argument
            return 1


def test_parameter_not_present_in_cypher(empty_session):
    with pytest.raises(BadTriggerReturnAnnotationError):

        @empty_session.trigger("MATCH (n:Thingy) RETURN n.foo")
        def test_function(
            imnotinthecypher,  # pylint: disable=unused-argument
        ) -> VariableAttribute["n", "thingy"]:  # type: ignore
            return 1


def test_raise_error_on_bad_cypher_string(empty_session):
    with pytest.raises(ValueError):

        @empty_session.trigger("i am not a valid cypher string")
        def test_function(n) -> VariableAttribute["n", "thingy"]:  # type: ignore
            return 1


def test_return_clause_gather_variables():
    obj = CypherParser("MATCH (n:Thing) RETURN n.foo, n.baz, m.bar")
    assert obj.parse_tree.cypher.return_clause.gather_variables() == ["n", "m"]


def test_return_clause_gather_variables_from_aliases():
    obj = CypherParser(
        "MATCH (n:Thing) RETURN n.foo AS thingy, n.baz AS otherthingy, m.bar"
    )
    assert obj.parse_tree.cypher.return_clause.gather_variables() == [
        "thingy",
        "otherthingy",
        "m",
    ]


def test_data_source_is_right_class(fixture_data_source_0):
    assert isinstance(fixture_data_source_0, DataSource)


def test_data_source_has_right_type_row_iterator(fixture_data_source_0):
    assert isinstance(fixture_data_source_0.rows(), Iterable)


def test_data_source_row_iterator_yields_data(fixture_data_source_0):
    counter = 0
    for row in fixture_data_source_0.rows():
        assert row
        assert isinstance(row, dict)
        assert len(row) == 5
        counter += 1
    assert counter == 7


def test_attach_data_source_mapping_to_data_source(
    fixture_0_data_source_mapping_list,
    fixture_data_source_0,
):
    for fixture_0_data_source_mapping in fixture_0_data_source_mapping_list:
        fixture_data_source_0.attach_mapping(fixture_0_data_source_mapping)

    assert fixture_data_source_0.mappings == fixture_0_data_source_mapping_list


def test_attach_data_source_mapping_list_to_data_source(
    fixture_0_data_source_mapping_list,
    fixture_data_source_0,
):
    fixture_data_source_0.attach_mapping(fixture_0_data_source_mapping_list)
    assert fixture_data_source_0.mappings == fixture_0_data_source_mapping_list


def test_attach_data_source_to_session(empty_session, fixture_data_source_0):
    empty_session.attach_data_source(fixture_data_source_0)
    assert fixture_data_source_0 in empty_session.data_sources


def test_data_source_requires_queue(fixture_data_source_0):
    with pytest.raises(ValueError):
        fixture_data_source_0.queue_rows()


def test_data_source_cannot_attach_non_queue(fixture_data_source_0):
    with pytest.raises(ValueError):
        fixture_data_source_0.attach_queue("not a queue")


def test_data_source_attach_queue(fixture_data_source_0):
    fixture_data_source_0.attach_queue(QueueGenerator())
    assert fixture_data_source_0.raw_input_queue
    assert isinstance(fixture_data_source_0.raw_input_queue, QueueGenerator)


def test_attaching_data_source_also_attaches_queue(
    empty_session, fixture_data_source_0
):
    empty_session.attach_data_source(fixture_data_source_0)
    assert isinstance(
        fixture_data_source_0.raw_input_queue,
        (
            queue.Queue,
            QueueGenerator,
        ),
    )


def test_data_source_queue_rows(fixture_data_source_0):
    raw_input_queue = QueueGenerator()  # queue.Queue()
    fixture_data_source_0.attach_queue(raw_input_queue)
    fixture_data_source_0.queue_rows()
    counter = 0
    for obj in raw_input_queue.yield_items():
        counter += 1
        assert isinstance(
            obj,
            (
                EndOfData,
                RawDatum,
            ),
        )
    assert counter == 7


def test_data_source_not_started_yet_flag(fixture_data_source_0):
    raw_input_queue = QueueGenerator()
    fixture_data_source_0.attach_queue(raw_input_queue)
    assert not fixture_data_source_0.started


def test_data_source_started_flag(fixture_data_source_0):
    """Allow one second for data source fixture to start."""
    raw_input_queue = QueueGenerator()
    fixture_data_source_0.attach_queue(raw_input_queue)
    fixture_data_source_0.start()
    test_time = time.time()
    while not fixture_data_source_0.started and time.time() - test_time < 1:
        pass
    assert fixture_data_source_0.started


def test_data_source_finished_flag(fixture_data_source_0):
    """Allow one second for data source fixture to finish."""
    raw_input_queue = QueueGenerator()
    fixture_data_source_0.attach_queue(raw_input_queue)
    fixture_data_source_0.start()
    test_time = time.time()
    while not fixture_data_source_0.finished and time.time() - test_time < 5:
        pass
    assert fixture_data_source_0.finished


def test_raw_input_queue_not_empty_after_finished_flag(fixture_data_source_0):
    """Allow one second for data source fixture to finish."""
    raw_input_queue = QueueGenerator()
    fixture_data_source_0.attach_queue(raw_input_queue)
    fixture_data_source_0.start()
    while not fixture_data_source_0.finished:
        pass
    assert not raw_input_queue.empty()


def test_attach_non_data_source_mapping_raises_error(fixture_data_source_0):
    with pytest.raises(ValueError):
        fixture_data_source_0.attach_mapping("not a mapping")


def test_start_loading_thread_from_session(empty_session, fixture_data_source_0):
    empty_session.attach_data_source(fixture_data_source_0)
    empty_session.start_threads()
    assert isinstance(fixture_data_source_0.loading_thread, threading.Thread)


def test_session_reports_unfinished_data_source_if_not_started(
    empty_session, fixture_data_source_0
):
    empty_session.attach_data_source(fixture_data_source_0)
    assert empty_session.has_unfinished_data_source()


def test_session_reports_unfinished_data_source_if_running(
    empty_session, fixture_data_source_0
):
    fixture_data_source_0.delay = 0.1
    empty_session.attach_data_source(fixture_data_source_0)
    empty_session.start_threads()
    assert empty_session.has_unfinished_data_source()


def test_session_reports_no_unfinished_data_source_if_complete(
    empty_session, fixture_data_source_0
):
    empty_session.attach_data_source(fixture_data_source_0)
    empty_session.start_threads()
    while not fixture_data_source_0.finished:
        pass
    assert not empty_session.has_unfinished_data_source()


def test_session_has_raw_data_processor(
    fixture_0_data_source_mapping_list, empty_session, fixture_data_source_0
):
    fixture_data_source_0.attach_mapping(fixture_0_data_source_mapping_list)
    empty_session.attach_data_source(fixture_data_source_0)
    assert empty_session.raw_data_processor


def test_session_has_fact_generated_queue_processor(
    fixture_0_data_source_mapping_list, empty_session, fixture_data_source_0
):
    fixture_data_source_0.attach_mapping(fixture_0_data_source_mapping_list)
    empty_session.attach_data_source(fixture_data_source_0)
    assert empty_session.fact_generated_queue_processor


def test_session_fact_generated_queue_processor_starts_in_thread(
    fixture_0_data_source_mapping_list, empty_session, fixture_data_source_0
):
    # This one takes a few seconds to complete because thread has to time out
    fixture_data_source_0.attach_mapping(fixture_0_data_source_mapping_list)
    empty_session.attach_data_source(fixture_data_source_0)
    empty_session.start_threads()
    assert empty_session.fact_generated_queue_processor.started


def test_session_fact_generated_queue_processor_appends_facts_to_collection(
    fixture_0_data_source_mapping_list, empty_session, fixture_data_source_0
):
    fixture_data_source_0.attach_mapping(fixture_0_data_source_mapping_list)
    empty_session.attach_data_source(fixture_data_source_0)
    empty_session.start_threads()
    empty_session.block_until_finished()
    assert len(empty_session.fact_collection) == 42


def test_queue_generator_not_completed_immediately():
    q = QueueGenerator()
    assert not q.completed


def test_queue_generator_completed_after_end_of_data():
    q = QueueGenerator()
    q.put("hi")
    q.put(EndOfData())
    for _ in q.yield_items():
        pass
    assert q.completed


def test_queue_generator_not_completed_during_generation():
    q = QueueGenerator()
    q.put("hi")
    q.put("there")
    q.put(EndOfData())
    for _ in q.yield_items():
        assert not q.completed


def test_queue_generator_yields_correct_items():
    q = QueueGenerator()
    q.incoming_queue_processors.append(Mock())  # otherwise will exit immediately
    q.put("hi")
    q.put("there")
    q.put("you")
    q.put(EndOfData())
    items = []
    for item in q.yield_items():
        items.append(item)
    assert items == [
        "hi",
        "there",
        "you",
    ]


def test_queue_generator_exit_code_1_if_timeout():
    q = QueueGenerator()
    q.put("hi")
    q.put("there")
    q.put("you")
    q.incoming_queue_processors.append(Mock())  # otherwise will exit immediately
    for _ in q.yield_items():
        pass
    time.sleep(0.4)
    assert q.exit_code == 1


def test_queue_generator_exit_0_if_normal_stop():
    q = QueueGenerator()
    q.put("hi")
    q.put("there")
    q.put("you")
    q.put(EndOfData())
    # ?
    q.incoming_queue_processors.append(Mock())  # otherwise will exit immediately
    for _ in q.yield_items():
        pass
    assert q.exit_code == 0


def test_data_source_mapping_against_row(
    fixture_data_source_0, fixture_0_data_source_mapping_list
):
    row = fixture_data_source_0.data[0]
    data_source_mapping = fixture_0_data_source_mapping_list[0]
    fact = list(data_source_mapping.process_against_raw_datum(row))
    assert fact == [
        FactNodeHasAttributeWithValue(
            node_id="Person::001", attribute="Identifier", value="001"
        )
    ]


def test_plus_operator_calls_process_against_raw_datum(
    fixture_data_source_0, fixture_0_data_source_mapping_list
):
    mocked = Mock()
    with patch("nmetl.data_source.DataSourceMapping.__add__", mocked) as _:
        row = fixture_data_source_0.data[0]
        data_source_mapping = fixture_0_data_source_mapping_list[0]
        data_source_mapping + row  # pylint: disable=pointless-statement
        mocked.assert_called_once_with(row)


def test_data_source_mapping_against_row_from_data_source(
    fixture_data_source_0, empty_session, fixture_0_data_source_mapping_list
):
    fixture_data_source_0.attach_mapping(fixture_0_data_source_mapping_list)
    empty_session.attach_data_source(fixture_data_source_0)
    row = fixture_data_source_0.data[0]
    fact_list = list(empty_session.data_sources[0].generate_raw_facts_from_row(row))
    expected_fact_list = [
        FactNodeHasAttributeWithValue(
            node_id="Person::001", attribute="Identifier", value="001"
        ),
        FactNodeHasAttributeWithValue(
            node_id="Person::001", attribute="Name", value="Alice"
        ),
        FactNodeHasAttributeWithValue(node_id="Person::001", attribute="Age", value=25),
        FactNodeHasAttributeWithValue(
            node_id="Person::001", attribute="ZipCode", value="02056"
        ),
        FactNodeHasAttributeWithValue(
            node_id="Person::001", attribute="WidgetsPurchased", value=5
        ),
        FactNodeHasLabel(node_id="Person::001", label="Person"),
    ]
    assert fact_list == expected_fact_list


def test_data_source_mapping_against_row_from_session(
    fixture_data_source_0, empty_session, fixture_0_data_source_mapping_list
):
    fixture_data_source_0.attach_mapping(fixture_0_data_source_mapping_list)
    empty_session.attach_data_source(fixture_data_source_0)
    empty_session.start_threads()
    empty_session.block_until_finished()
    assert empty_session.raw_data_processor.received_counter > 0
    assert empty_session.raw_data_processor.sent_counter > 0


@pytest.mark.timeout(15)
def test_can_stop_session(
    fixture_data_source_0, empty_session, fixture_0_data_source_mapping_list
):
    fixture_data_source_0.loop = True
    fixture_data_source_0.attach_mapping(fixture_0_data_source_mapping_list)
    empty_session.attach_data_source(fixture_data_source_0)
    empty_session.start_threads()
    empty_session.halt()
    empty_session.block_until_finished()


def test_constraint_variable_refers_to_specific_object():
    constraint = ConstraintVariableRefersToSpecificObject("n", "00001")
    assert constraint.variable == "n"
    assert constraint.node_id == "00001"


# @pytest.mark.skip
def test_find_solution_node_has_label_with_node_identity_constraint(
    fact_collection_0: FactCollection,
):
    cypher = "MATCH (n:Thing) RETURN n.foobar"
    result = CypherParser(cypher)

    added_constraint = ConstraintVariableRefersToSpecificObject("n", "1")
    result.parse_tree.cypher.match_clause.constraints.append(added_constraint)

    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_0)
    expected = [{"n": "1"}]
    assert solutions == expected


def test_find_solution_node_has_label_with_node_identity_constraint_unsatisfiable(
    fact_collection_0: FactCollection,
):
    cypher = "MATCH (n:Thing) RETURN n.foobar"
    result = CypherParser(cypher)

    added_constraint = ConstraintVariableRefersToSpecificObject("n", "idontexist")
    result.parse_tree.cypher.match_clause.constraints.append(added_constraint)

    solutions = result.parse_tree.cypher.match_clause.solutions(fact_collection_0)
    expected = []
    assert solutions == expected


def test_fact_plus_constraint_variable_refers_to_specific_object_true():
    fact = FactNodeHasLabel("1", "Thing")
    constraint = ConstraintVariableRefersToSpecificObject("n", "1")
    assert fact + constraint == {"n": "1"}


def test_fact_plus_constraint_variable_refers_to_specific_object_false():
    fact = FactNodeHasLabel("1", "Thing")
    constraint = ConstraintVariableRefersToSpecificObject("n", "2")
    assert fact + constraint is None


def test_end_to_end_with_decorated_function_and_fact_collection(
    fixture_data_source_0, empty_session, fixture_0_data_source_mapping_list
):
    fixture_data_source_0.attach_mapping(fixture_0_data_source_mapping_list)
    empty_session.attach_data_source(fixture_data_source_0)

    @empty_session.trigger(
        "MATCH (n:Person {age: 45}) WITH n.name AS person_name RETURN person_name"
    )
    def test_function(
        person_name,  # pylint: disable=unused-argument
    ) -> VariableAttribute["n", "thingy"]:  # type: ignore
        return 1

    empty_session.start_threads()
    empty_session.block_until_finished()


def test_create_csv_data_source_from_uri():
    squares_csv = TEST_DATA_DIRECTORY / "squares.csv"
    squares_csv_uri = ensure_uri(squares_csv)
    csv_data_source = DataSource.from_uri(squares_csv_uri)
    assert isinstance(csv_data_source, CSVDataSource)


def test_ensure_uri_creates_uri():
    squares_csv = TEST_DATA_DIRECTORY / "squares.csv"
    squares_csv_uri = ensure_uri(squares_csv)
    assert squares_csv_uri.scheme == "file"


def test_ensure_uri_idempotent():
    squares_csv = TEST_DATA_DIRECTORY / "squares.csv"
    assert ensure_uri(squares_csv) == ensure_uri(ensure_uri(squares_csv))


def test_rows_method_yields_csv_rows(squares_csv_data_source):
    counter = 0
    for row in squares_csv_data_source.rows():
        assert row
        assert isinstance(row, dict)
        assert len(row) == 3
        counter += 1
    assert counter == 4


def test_rows_method_yields_csv_correct_data(squares_csv_data_source):
    actual = [
        {"name": "squarename1", "length": "1", "color": "blue"},
        {"name": "squarename2", "length": "5", "color": "red"},
        {"name": "squarename3", "length": "3", "color": "blue"},
        {"name": "squarename4", "length": "10", "color": "orange"},
    ]
    expected = [row for row in squares_csv_data_source.rows()]
    assert actual == expected


def test_load_configuration_file_for_session_job():
    ingest_file = TEST_DATA_DIRECTORY / "ingest.yaml"
    session = load_session_config(ingest_file)
    assert isinstance(session, Session)


def test_load_configuration_file_data_sources():
    ingest_file = TEST_DATA_DIRECTORY / "ingest.yaml"
    session = load_session_config(ingest_file)
    assert isinstance(session.data_sources, list)


def test_load_configuration_file_data_sources_have_correct_type():
    ingest_file = TEST_DATA_DIRECTORY / "ingest.yaml"
    session = load_session_config(ingest_file)
    assert all(isinstance(ds, DataSource) for ds in session.data_sources)


def test_session_runs_from_ingest_file():
    ingest_file = TEST_DATA_DIRECTORY / "ingest.yaml"
    session = load_session_config(ingest_file)
    start_threads_mock = Mock()

    with patch("nmetl.session.Session.start_threads", start_threads_mock) as _:
        session(block=False)
        start_threads_mock.assert_called_once()


def test_load_configuration_three_data_sources():
    ingest_file = TEST_DATA_DIRECTORY / "ingest.yaml"
    session = load_session_config(ingest_file)
    assert len(session.data_sources) == 3
    assert all(isinstance(ds, DataSource) for ds in session.data_sources)


def test_cast_row():
    ingest_file = TEST_DATA_DIRECTORY / "ingest.yaml"
    session = load_session_config(ingest_file)
    row = {"name": "Alice", "length": "25", "color": "blue"}
    casted = session.data_sources[0].cast_row(row)
    assert isinstance(casted["length"], float)


def test_cast_row_with_bad_data():
    ingest_file = TEST_DATA_DIRECTORY / "ingest.yaml"
    session = load_session_config(ingest_file)
    row = {"name": "", "length": "25", "color": "blue"}
    with pytest.raises(ValueError):
        session.data_sources[0].cast_row(row)


def test_fact_collection_has_casted_value():
    ingest_file = TEST_DATA_DIRECTORY / "ingest.yaml"
    session = load_session_config(ingest_file)
    session.start_threads()
    session.block_until_finished()
    fact = FactNodeHasAttributeWithValue(
        node_id="Square::squarename1", attribute="side_length", value=1.0
    )
    assert fact in session.fact_collection


def test_type_dispatch_dict_casting_date():
    out = TYPE_DISPATCH_DICT["Date"]("1972-10-18")
    assert out == datetime.date(1972, 10, 18)


def test_type_dispatch_dict_casting_datetime():
    out = TYPE_DISPATCH_DICT["DateTime"]("1972-10-18T12:34:56")
    assert out == datetime.datetime(1972, 10, 18, 12, 34, 56)


def test_type_dispatch_dict_casting_positive_int_1():
    out = TYPE_DISPATCH_DICT["PositiveInteger"](5)
    assert out == 5


def test_type_dispatch_dict_casting_positive_int_fail():
    with pytest.raises(Exception):
        TYPE_DISPATCH_DICT["PositiveInteger"](-5)


def test_type_dispatch_dict_casting_positive_int_2():
    out = TYPE_DISPATCH_DICT["PositiveInteger"]("5")
    assert out == 5


def test_type_dispatch_dict_casting_negative_int_1():
    out = TYPE_DISPATCH_DICT["NegativeInteger"](-5)
    assert out == -5


def test_type_dispatch_dict_casting_negative_int_2():
    out = TYPE_DISPATCH_DICT["NegativeInteger"]("-5")
    assert out == -5


def test_type_dispatch_dict_casting_negative_int_fail_1():
    with pytest.raises(ValueError):
        TYPE_DISPATCH_DICT["NegativeInteger"](5)


def test_type_dispatch_dict_casting_negative_int_fail_2():
    with pytest.raises(ValueError):
        TYPE_DISPATCH_DICT["NegativeInteger"]("5")


def test_type_dispatch_dict_casting_boolean_1():
    out = TYPE_DISPATCH_DICT["Boolean"](True)
    assert out


def test_type_dispatch_dict_casting_boolean_2():
    out = TYPE_DISPATCH_DICT["Boolean"]("True")
    assert out


def test_type_dispatch_dict_casting_boolean_3():
    out = TYPE_DISPATCH_DICT["Boolean"]("TRUE")
    assert out


def test_type_dispatch_dict_casting_boolean_4():
    out = TYPE_DISPATCH_DICT["Boolean"]("true")
    assert out


def test_type_dispatch_dict_casting_boolean_5():
    out = TYPE_DISPATCH_DICT["Boolean"]("False")
    assert not out


def test_type_dispatch_dict_casting_boolean_6():
    out = TYPE_DISPATCH_DICT["Boolean"]("FALSE")
    assert not out


def test_type_dispatch_dict_casting_boolean_7():
    out = TYPE_DISPATCH_DICT["Boolean"]("false")
    assert not out


def test_type_dispatch_dict_casting_boolean_8():
    out = TYPE_DISPATCH_DICT["Boolean"](False)
    assert not out


def test_type_dispatch_dict_casting_boolean_fail():
    with pytest.raises(ValueError):
        TYPE_DISPATCH_DICT["Boolean"]("not a boolean")


def test_type_dispatch_dict_casting_float_1():
    out = TYPE_DISPATCH_DICT["Float"](1.0)
    assert out == 1.0


def test_type_dispatch_dict_casting_float_2():
    out = TYPE_DISPATCH_DICT["Float"]("1.0")
    assert out == 1.0


def test_type_dispatch_dict_casting_float_fail():
    with pytest.raises(ValueError):
        TYPE_DISPATCH_DICT["Float"]("not a float")


def test_type_dispatch_dict_nonempty_string_1():
    out = TYPE_DISPATCH_DICT["NonEmptyString"]("hi")
    assert out == "hi"


def test_type_dispastch_dict_nonempty_string_fail():
    with pytest.raises(ValueError):
        TYPE_DISPATCH_DICT["NonEmptyString"]("")


def test_relationship_data_mapping():
    relationship_mapping = DataSourceMapping(
        source_key="1",
        target_key="2",
        relationship="related",
        source_label="foo",
        target_label="bar",
    )
    assert relationship_mapping.is_relationship_mapping
    assert not relationship_mapping.is_label_mapping
    assert not relationship_mapping.is_attribute_mapping


def test_relationship_mapping_with_raw_datum_source():
    relationship_mapping = DataSourceMapping(
        source_key="1",
        target_key="2",
        relationship="related",
        source_label="foo",
        target_label="bar",
    )
    row = {
        "1": "a",
        "2": "b",
    }
    source_fact, _, _ = relationship_mapping.process_against_raw_datum(row)

    assert source_fact.source_node_id == "foo::a"


def test_relationship_mapping_with_raw_datum_target():
    relationship_mapping = DataSourceMapping(
        source_key="1",
        target_key="2",
        relationship="related",
        source_label="foo",
        target_label="bar",
    )
    row = {
        "1": "a",
        "2": "b",
    }
    _, target_fact, _ = relationship_mapping.process_against_raw_datum(row)

    assert target_fact.target_node_id == "bar::b"


def test_relationship_mapping_with_raw_datum_relationship():
    relationship_mapping = DataSourceMapping(
        source_key="1",
        target_key="2",
        relationship="related",
        source_label="foo",
        target_label="bar",
    )
    row = {
        "1": "a",
        "2": "b",
    }
    _, _, label_fact = relationship_mapping.process_against_raw_datum(row)

    assert label_fact.relationship_label == "related"


def test_fact_collection_has_relationship_fact_source_node():
    with patch("uuid.uuid4", patched_uuid) as _:
        ingest_file = TEST_DATA_DIRECTORY / "ingest.yaml"
        session = load_session_config(ingest_file)
        session.start_threads()
        session.block_until_finished()
        fact1 = FactRelationshipHasSourceNode(
            relationship_id="SOME_HEX",
            source_node_id="Square::squarename1",
        )
        assert fact1 in session.fact_collection


def test_fact_collection_has_relationship_fact_target_node():
    with patch("uuid.uuid4", patched_uuid) as _:
        ingest_file = TEST_DATA_DIRECTORY / "ingest.yaml"
        session = load_session_config(ingest_file)
        session.start_threads()
        session.block_until_finished()
        fact1 = FactRelationshipHasTargetNode(
            relationship_id="SOME_HEX",
            target_node_id="Circle::circle_a",
        )
        assert fact1 in session.fact_collection


def test_nmetl_cli_runs():
    result = subprocess.run(
        [
            "nmetl",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0


def test_nmetl_cli_validation_passes():
    result = subprocess.run(
        [
            "nmetl",
            "validate",
            str(TEST_DATA_DIRECTORY / "ingest.yaml"),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0


def test_nmetl_cli_validation_fails():
    with pytest.raises(subprocess.CalledProcessError) as _:
        subprocess.run(
            [
                "nmetl",
                "validate",
                str(TEST_DATA_DIRECTORY / "bad_ingest.yaml"),
            ],
            capture_output=True,
            text=True,
            check=True,
        )


def test_fact_collection_inventory(fact_collection_6):
    obj = fact_collection_6.node_label_attribute_inventory()
    expected = collections.defaultdict(
        set,
        {
            "Thing": {"foo"},
            "MiddleThing": set(),
            "OtherThing": {"foo"},
            "Irrelevant": set(),
        },
    )
    assert obj == expected


def test_nodes_with_label(shapes_session):
    obj = list(shapes_session.fact_collection.nodes_with_label("Square"))
    obj.sort()
    expected = [
        "Square::squarename1",
        "Square::squarename2",
        "Square::squarename3",
        "Square::squarename4",
    ]
    assert obj == expected


def test_attributes_for_specific_node(shapes_session):
    obj = shapes_session.fact_collection.attributes_for_specific_node(
        "Square::squarename1", "side_length", "square_color"
    )
    expected = {
        "side_length": 1.0,
        "square_color": "blue",
    }
    assert obj == expected


def test_rows_by_node_label(shapes_session):
    obj = list(shapes_session.fact_collection.rows_by_node_label("Square"))
    obj = sorted(obj, key=lambda x: x["name"])
    expected = [
        {"name": "squarename1", "square_color": "blue", "side_length": 1.0},
        {"name": "squarename2", "square_color": "red", "side_length": 5.0},
        {"name": "squarename3", "square_color": "blue", "side_length": 3.0},
        {"name": "squarename4", "square_color": "orange", "side_length": 10.0},
    ]
    expected = sorted(expected, key=lambda x: x["name"])
    assert obj == expected


def test_trigger_function_on_relationship_match(session_with_trigger):
    session_with_trigger.start_threads()
    session_with_trigger.block_until_finished()

    assert list(session_with_trigger.trigger_dict.values())[0].call_counter > 0


def test_trigger_function_on_relationship_match_insert_results(
    session_with_trigger,
):
    session_with_trigger.start_threads()
    session_with_trigger.block_until_finished()

    assert (
        session_with_trigger.fact_collection.query(
            QueryValueOfNodeAttribute(node_id="Square::squarename1", attribute="area")
        )
        == 1.0
    )
    assert (
        session_with_trigger.fact_collection.query(
            QueryValueOfNodeAttribute(node_id="Square::squarename2", attribute="area")
        )
        == 25.0
    )
    assert (
        session_with_trigger.fact_collection.query(
            QueryValueOfNodeAttribute(node_id="Square::squarename3", attribute="area")
        )
        == 9.0
    )


def test_trigger_function_on_relationship_match_no_insert_no_match(
    session_with_trigger,
):
    session_with_trigger.start_threads()
    session_with_trigger.block_until_finished()

    result = session_with_trigger.fact_collection.query(
        QueryValueOfNodeAttribute(node_id="Square::squarename4", attribute="area")
    )
    assert isinstance(result, NullResult)


def test_fact_collection_rejects_duplicate_fact():
    fact_collection = FactCollection([])
    fact = FactNodeHasLabel("1", "Thing")
    fact_collection += fact
    fact_collection += fact
    assert len(fact_collection) == 1


def test_fact_node_has_label_hashable():
    fact = FactNodeHasLabel("1", "Thing")
    assert hash(fact)


def test_fact_node_has_attribute_with_value_hashable():
    fact = FactNodeHasAttributeWithValue("1", "foo", "bar")
    assert hash(fact)


def test_fact_relationship_has_source_node_hashable():
    fact = FactRelationshipHasSourceNode("1", "2")
    assert hash(fact)


def test_fact_relationship_has_target_node_hashable():
    fact = FactRelationshipHasTargetNode("1", "2")
    assert hash(fact)


def test_fact_relationship_has_label_hashable():
    fact = FactRelationshipHasLabel("1", "2")
    assert hash(fact)


def test_fact_relationship_has_attribute_with_value_hashable():
    fact = FactRelationshipHasAttributeWithValue("1", "foo", "bar")
    assert hash(fact)


def test_second_order_trigger_executes(session_with_two_triggers):
    # LOGGER.setLevel(logging.DEBUG)
    session_with_two_triggers.start_threads()
    session_with_two_triggers.block_until_finished()
    assert (
        session_with_two_triggers.fact_collection.query(
            QueryValueOfNodeAttribute(node_id="Square::squarename1", attribute="big")
        )
        is False
    )


def test_third_order_trigger_executes(session_with_three_triggers):
    session_with_three_triggers.start_threads()
    session_with_three_triggers.block_until_finished()
    assert (
        session_with_three_triggers.fact_collection.query(
            QueryValueOfNodeAttribute(node_id="Square::squarename1", attribute="small")
        )
        is True
    )

    inventory = (
        session_with_three_triggers.fact_collection.node_label_attribute_inventory()
    )
    expected = {
        "Circle": {
            "y_coordinate",
            "x_coordinate",
            "identification_string",
            "name",
        },
        "Square": {
            "small",
            "area",
            "square_color",
            "name",
            "big",
            "side_length",
        },
    }
    expected = collections.defaultdict(set, expected)
    assert inventory == expected


def test_inventory_on_session_executes_fact_collection_method(
    session_with_three_triggers,
):
    inventory_mock = Mock()
    with patch(
        "pycypher.fact.FactCollection.node_label_attribute_inventory",
        inventory_mock,
    ) as _:
        session_with_three_triggers.node_label_attribute_inventory()
        inventory_mock.assert_called_once()


def test_get_writer_from_csv_uri():
    uri = "file:///tmp/test.csv"
    writer = TableWriter.get_writer(uri)
    assert isinstance(writer, CSVTableWriter)


def test_get_writer_from_parquet_uri():
    uri = "file:///tmp/test.parquet"
    writer = TableWriter.get_writer(uri)
    assert isinstance(writer, ParquetTableWriter)


def test_raise_error_on_unsupported_uri():
    uri = "file:///tmp/test.idontexist"
    with pytest.raises(ValueError):
        TableWriter.get_writer(uri)


def test_nodes_with_label_generator(session_with_three_triggers):
    session_with_three_triggers.start_threads()
    session_with_three_triggers.block_until_finished()
    nodes = session_with_three_triggers.fact_collection.nodes_with_label("Square")
    nodes = list(nodes)
    assert nodes == [
        "Square::squarename1",
        "Square::squarename2",
        "Square::squarename3",
        "Square::squarename4",
    ]


def test_generate_rows_for_entity_type(session_with_three_triggers):
    session_with_three_triggers.start_threads()
    session_with_three_triggers.block_until_finished()
    rows = session_with_three_triggers.fact_collection.rows_by_node_label("Square")
    rows = list(rows)
    expected = [
        {
            "big": False,
            "name": "squarename1",
            "area": 1.0,
            "side_length": 1.0,
            "small": True,
            "square_color": "blue",
        },
        {
            "big": True,
            "name": "squarename2",
            "area": 25.0,
            "side_length": 5.0,
            "small": False,
            "square_color": "red",
        },
        {
            "big": False,
            "name": "squarename3",
            "area": 9.0,
            "side_length": 3.0,
            "small": True,
            "square_color": "blue",
        },
        {
            "big": None,
            "name": "squarename4",
            "area": None,
            "side_length": 10.0,
            "small": None,
            "square_color": "orange",
        },
    ]
    assert rows == unordered(expected)


def test_generate_rows_for_entity_type_from_session_method(
    session_with_three_triggers,
):
    session_with_three_triggers.start_threads()
    session_with_three_triggers.block_until_finished()
    rows = session_with_three_triggers.rows_by_node_label("Square")
    rows = list(rows)
    expected = [
        {
            "big": False,
            "name": "squarename1",
            "area": 1.0,
            "side_length": 1.0,
            "small": True,
            "square_color": "blue",
        },
        {
            "big": True,
            "name": "squarename2",
            "area": 25.0,
            "side_length": 5.0,
            "small": False,
            "square_color": "red",
        },
        {
            "big": False,
            "name": "squarename3",
            "area": 9.0,
            "side_length": 3.0,
            "small": True,
            "square_color": "blue",
        },
        {
            "big": None,
            "name": "squarename4",
            "area": None,
            "side_length": 10.0,
            "small": None,
            "square_color": "orange",
        },
    ]
    assert rows == unordered(expected)


def test_write_csv_table_with_one_entity(
    session_with_three_triggers,
    tmp_path,
):
    uri = tmp_path.with_suffix(".csv").as_uri()
    session_with_three_triggers.start_threads()
    session_with_three_triggers.block_until_finished()
    writer = TableWriter.get_writer(uri)
    writer.write_entity_table(session_with_three_triggers, "Square")
    outfile = tmp_path.with_suffix(".csv").as_uri().replace("file://", "")
    assert filecmp.cmp(outfile, TEST_DATA_DIRECTORY / "square_entity_output.csv")


def test_parser_gets_solutions_from_fact_collection(
    fact_collection_squares_circles,
):
    cypher = (
        "MATCH (s:Square)-[r:contains]->(c:Circle) "
        "WITH s.name AS square_name, s.length AS square_length, COLLECT(c.radius) AS radii "
        "RETURN square_name, square_length, radii"
    )
    parser = CypherParser(cypher)
    solutions = parser.solutions(fact_collection_squares_circles)
    assert solutions


def test_parser_gets_aggregated_solutions_from_fact_collection(
    fact_collection_squares_circles,
):
    cypher = (
        "MATCH (s:Square)-[r:contains]->(c:Circle) "
        "WITH s.name AS square_name, s.length AS square_length, COLLECT(c.radius) AS radii "
        "RETURN square_name, square_length, radii"
    )
    parser = CypherParser(cypher)
    solutions = parser.solutions(fact_collection_squares_circles)
    assert solutions


def test_evaluate_call_on_return_with_alias(
    fact_collection_squares_circles,
):
    cypher = (
        "MATCH (s:Square)-[my_relationship:contains]->(c:Circle) "
        "WITH s.side_length AS side_length RETURN side_length"
    )
    parser = CypherParser(cypher)
    parser.parse_tree.get_return_clause()._evaluate(fact_collection_squares_circles)


def test_test_gather_variables_on_return_with_alias():
    cypher = (
        "MATCH (s:Square)-[my_relationship:contains]->(c:Circle) "
        "RETURN s.side_length AS side_length"
    )
    parser = CypherParser(cypher)
    assert parser.parse_tree.get_return_clause().gather_variables() == ["side_length"]


# @pytest.mark.xfail
def test_aggregation_trigger_in_session_inserts_facts(
    session_with_aggregation_fixture,
):
    session_with_aggregation_fixture.start_threads()
    session_with_aggregation_fixture.block_until_finished()

    assert (
        FactNodeHasAttributeWithValue(
            node_id="Square::squarename1", attribute="num_circles", value=3
        )
        in session_with_aggregation_fixture.fact_collection
    )

    assert (
        FactNodeHasAttributeWithValue(
            node_id="Square::squarename2", attribute="num_circles", value=3
        )
        in session_with_aggregation_fixture.fact_collection
    )

    assert (
        FactNodeHasAttributeWithValue(
            node_id="Square::squarename3", attribute="num_circles", value=1
        )
        in session_with_aggregation_fixture.fact_collection
    )


def test_with_clause_records_variables(fact_collection_squares_circles):
    cypher = (
        "MATCH (s:Square)-[my_relationship:contains]->(c:Circle) "
        "WITH s.length AS length RETURN length"
    )
    parser = CypherParser(cypher)
    out = parser.parse_tree.cypher.match_clause.with_clause._evaluate(
        fact_collection_squares_circles
    )
    expected = [
        {"length": Literal(4), "__match_solution__": {"s": "square_3"}},
        {"length": Literal(3), "__match_solution__": {"s": "square_2"}},
        {"length": Literal(2), "__match_solution__": {"s": "square_1"}},
    ]
    assert out == expected


def test_new_column_annotation_inserts_to_session_dict(empty_session):
    @empty_session.new_column("imadatasource", attach_to_data_source=False)
    def new_column(column1, column2) -> NewColumn["new_column"]:
        return column1 + column2

    assert "new_column" in empty_session.new_column_dict


def test_new_column_annotation_has_callable_function(empty_session):
    @empty_session.new_column("imadatasource", attach_to_data_source=False)
    def new_column(column1, column2) -> NewColumn["new_column"]:
        return column1 + column2

    assert isinstance(empty_session.new_column_dict["new_column"].func, Callable)


def test_new_column_annotation_has_parameters_from_function(empty_session):
    @empty_session.new_column("imadatasource", attach_to_data_source=False)
    def new_column(column1, column2) -> NewColumn["new_column"]:
        return column1 + column2

    assert empty_session.new_column_dict["new_column"].parameter_names == [
        "column1",
        "column2",
    ]


def test_new_column_annotation_has_data_source_name(empty_session):
    @empty_session.new_column("imadatasource", attach_to_data_source=False)
    def new_column(column1, column2) -> NewColumn["new_column"]:
        return column1 + column2

    assert (
        empty_session.new_column_dict["new_column"].data_source_name == "imadatasource"
    )


def test_new_column_annotation_return_value_required(empty_session):
    with pytest.raises(ValueError):

        @empty_session.new_column("imadatasource", attach_to_data_source=False)
        def new_column(column1, column2):  # pylint: disable=unused-argument
            return 1


def test_new_column_annotation_unknown_data_source_raises_error(
    empty_session,
):
    with pytest.raises(UnknownDataSourceError):

        @empty_session.new_column("imadatasource", attach_to_data_source=True)
        def new_column(column1, column2) -> NewColumn["new_column"]:  # pylint: disable=unused-argument
            return 1


def test_new_column_config_created_on_data_source(
    session_with_city_state_fixture,
):
    assert "city_state" in session_with_city_state_fixture.new_column_dict
    assert isinstance(
        session_with_city_state_fixture.new_column_dict["city_state"],
        NewColumnConfig,
    )


def test_ingestion_with_new_column_annotation(
    session_with_city_state_fixture,
):
    session_with_city_state_fixture.start_threads()
    session_with_city_state_fixture.block_until_finished()
    assert (
        FactNodeHasAttributeWithValue(
            node_id="City::Seattle__Washington",
            attribute="population",
            value=652405,
        )
        in session_with_city_state_fixture.fact_collection
    )


def test_trigger_decorator_function_exception_if_bad_number_of_return_variable(
    session_with_trigger,
):
    with pytest.raises(TypeError):

        @session_with_trigger.trigger("MATCH (n) RETURN n.foo")
        def test_function(n) -> NodeRelationship["idontexist", "bar"]:  # type: ignore
            return n + 1


def test_trigger_decorator_function_relationship_function_raises_error_if_unknown_source_variable(
    session_with_city_state_fixture,
):
    with pytest.raises(BadTriggerReturnAnnotationError):

        @session_with_city_state_fixture.trigger(
            "MATCH (n: City) WITH n.foo AS nfoo, m.bar AS mbar RETURN nfoo, nbar"
        )
        def test_function(n) -> NodeRelationship["idontexist", "bar", "m"]:  # type: ignore
            return n + 1


def test_trigger_decorator_function_relationship_function_raises_error_if_unknown_target_variable(
    session_with_city_state_fixture,
):
    with pytest.raises(BadTriggerReturnAnnotationError):

        @session_with_city_state_fixture.trigger(
            "MATCH (n: City) WITH n.foo AS nfoo, m.bar AS mbar RETURN nfoo, nbar"
        )
        def test_function(n) -> NodeRelationship["n", "bar", "idontexist"]:  # type: ignore
            return n + 1


def test_trigger_decorator_function_insert_relationship_labels(
    session_with_trigger,
):
    @session_with_trigger.trigger(
        "MATCH (s:Square)-[r:contains]->(c:Circle) "
        "WITH s.side_length AS side_length, c.radius AS radius "
        "RETURN side_length, radius"
    )
    def test_function(
        side_length,
        radius,  # pylint: disable=unused-argument
    ) -> NodeRelationship["c", "contained_by", "s"]:
        return True

    session_with_trigger.start_threads()
    session_with_trigger.block_until_finished()

    assert (
        FactRelationshipHasLabel(
            relationship_id="4f64697615ed409f1ad4be73706c6e11",
            relationship_label="contained_by",
        )
        in session_with_trigger.fact_collection
    )
    assert (
        FactRelationshipHasLabel(
            relationship_id="8787716373083ea1ce8305360fd2bebf",
            relationship_label="contained_by",
        )
        in session_with_trigger.fact_collection
    )
    assert (
        FactRelationshipHasLabel(
            relationship_id="8cea9fd9306e58ccfffbe3e4a657dbca",
            relationship_label="contained_by",
        )
        in session_with_trigger.fact_collection
    )
    assert (
        FactRelationshipHasLabel(
            relationship_id="a842fe315d0e482cc1f1000132cb02bd",
            relationship_label="contained_by",
        )
        in session_with_trigger.fact_collection
    )
    assert (
        FactRelationshipHasLabel(
            relationship_id="e47e8fd79d20027896d1a151bd13c0a0",
            relationship_label="contained_by",
        )
        in session_with_trigger.fact_collection
    )
    assert (
        FactRelationshipHasLabel(
            relationship_id="eadfe2c7ff2be67a7948d91a035fbcf1",
            relationship_label="contained_by",
        )
        in session_with_trigger.fact_collection
    )
    assert (
        FactRelationshipHasLabel(
            relationship_id="fad6e43cbacaafaa4345ab96c4f89edc",
            relationship_label="contained_by",
        )
        in session_with_trigger.fact_collection
    )


def test_trigger_decorator_function_insert_relationship_source_nodes(
    session_with_trigger,
):
    @session_with_trigger.trigger(
        "MATCH (s:Square)-[r:contains]->(c:Circle) "
        "WITH s.side_length AS side_length, c.radius AS radius "
        "RETURN side_length, radius"
    )
    def test_function(
        side_length,
        radius,  # pylint: disable=unused-argument
    ) -> NodeRelationship["c", "contained_by", "s"]:
        return True

    session_with_trigger.start_threads()
    session_with_trigger.block_until_finished()

    assert FactRelationshipHasSourceNode(
        relationship_id="4f64697615ed409f1ad4be73706c6e11",
        source_node_id="Circle::circle_a",
    )
    assert FactRelationshipHasSourceNode(
        relationship_id="8787716373083ea1ce8305360fd2bebf",
        source_node_id="Circle::circle_c",
    )
    assert FactRelationshipHasSourceNode(
        relationship_id="8cea9fd9306e58ccfffbe3e4a657dbca",
        source_node_id="Circle::circle_g",
    )
    assert FactRelationshipHasSourceNode(
        relationship_id="a842fe315d0e482cc1f1000132cb02bd",
        source_node_id="Circle::circle_e",
    )
    assert FactRelationshipHasSourceNode(
        relationship_id="e47e8fd79d20027896d1a151bd13c0a0",
        source_node_id="Circle::circle_d",
    )
    assert FactRelationshipHasSourceNode(
        relationship_id="eadfe2c7ff2be67a7948d91a035fbcf1",
        source_node_id="Circle::circle_f",
    )
    assert FactRelationshipHasSourceNode(
        relationship_id="fad6e43cbacaafaa4345ab96c4f89edc",
        source_node_id="Circle::circle_b",
    )


def test_trigger_decorator_function_insert_relationship_target_nodes(
    session_with_trigger,
):
    @session_with_trigger.trigger(
        "MATCH (s:Square)-[r:contains]->(c:Circle) "
        "WITH s.side_length AS side_length, c.radius AS radius "
        "RETURN side_length, radius"
    )
    def test_function(
        side_length, radius
    ) -> NodeRelationship["c", "contained_by", "s"]:  # type: ignore
        return True

    session_with_trigger.start_threads()
    session_with_trigger.block_until_finished()

    assert FactRelationshipHasTargetNode(
        relationship_id="4f64697615ed409f1ad4be73706c6e11",
        target_node_id="Square::squarename1",
    )
    assert FactRelationshipHasTargetNode(
        relationship_id="8787716373083ea1ce8305360fd2bebf",
        target_node_id="Square::squarename1",
    )
    assert FactRelationshipHasTargetNode(
        relationship_id="8cea9fd9306e58ccfffbe3e4a657dbca",
        target_node_id="Square::squarename3",
    )
    assert FactRelationshipHasTargetNode(
        relationship_id="a842fe315d0e482cc1f1000132cb02bd",
        target_node_id="Square::squarename2",
    )
    assert FactRelationshipHasTargetNode(
        relationship_id="e47e8fd79d20027896d1a151bd13c0a0",
        target_node_id="Square::squarename2",
    )
    assert FactRelationshipHasTargetNode(
        relationship_id="eadfe2c7ff2be67a7948d91a035fbcf1",
        target_node_id="Square::squarename2",
    )
    assert FactRelationshipHasTargetNode(
        relationship_id="fad6e43cbacaafaa4345ab96c4f89edc",
        target_node_id="Square::squarename1",
    )


def test_trigger_decorator_function_insert_no_extra_relationship_labels(
    session_with_trigger,
):
    @session_with_trigger.trigger(
        "MATCH (s:Square)-[r:contains]->(c:Circle) "
        "WITH s.side_length AS side_length, c.radius AS radius "
        "RETURN side_length, radius"
    )
    def test_function(
        side_length, radius
    ) -> NodeRelationship["c", "contained_by", "s"]:  # type: ignore
        return True

    session_with_trigger.start_threads()
    session_with_trigger.block_until_finished()
    num_facts = len(
        [
            fact
            for fact in session_with_trigger.fact_collection
            if isinstance(fact, FactRelationshipHasLabel)
        ]
    )
    assert num_facts == 14


def test_trigger_decorator_function_insert_no_extra_relationship_source_nodes(
    session_with_trigger,
):
    @session_with_trigger.trigger(
        "MATCH (s:Square)-[r:contains]->(c:Circle) "
        "WITH s.side_length AS side_length, c.radius AS radius "
        "RETURN side_length, radius"
    )
    def test_function(
        side_length, radius
    ) -> NodeRelationship["c", "contained_by", "s"]:  # type: ignore
        return True

    session_with_trigger.start_threads()
    session_with_trigger.block_until_finished()

    num_facts = len(
        [
            fact
            for fact in session_with_trigger.fact_collection
            if isinstance(fact, FactRelationshipHasSourceNode)
        ]
    )
    assert num_facts == 14


def test_trigger_decorator_function_insert_no_extra_relationship_target_nodes(
    session_with_trigger,
):
    @session_with_trigger.trigger(
        "MATCH (s:Square)-[r:contains]->(c:Circle) "
        "WITH s.side_length AS side_length, c.radius AS radius "
        "RETURN side_length, radius"
    )
    def test_function(
        side_length, radius
    ) -> NodeRelationship["c", "contained_by", "s"]:  # type: ignore
        return True

    session_with_trigger.start_threads()
    session_with_trigger.block_until_finished()

    num_facts = len(
        [
            fact
            for fact in session_with_trigger.fact_collection
            if isinstance(fact, FactRelationshipHasTargetNode)
        ]
    )
    assert num_facts == 14


def test_data_asset_has_attributes(data_asset_1):
    assert data_asset_1.name == "data_asset_1"
    assert isinstance(data_asset_1.obj, dict)
    assert data_asset_1.obj["foo"] == "bar"


def test_register_data_asset(session_with_data_asset):
    assert "data_asset_1" in session_with_data_asset.data_asset_names
    assert isinstance(
        session_with_data_asset.get_data_asset_by_name("data_asset_1"),
        DataAsset,
    )
    assert isinstance(
        session_with_data_asset.get_data_asset_by_name("data_asset_1").obj, dict
    )


def test_trigger_function_can_access_data_asset(
    session_with_trigger_using_data_asset,
):
    LOGGER.setLevel(logging.INFO)
    session_with_trigger_using_data_asset.start_threads()
    session_with_trigger_using_data_asset.block_until_finished()
    assert (
        FactNodeHasAttributeWithValue(
            node_id="Square::squarename3", attribute="foo", value="bar"
        )
        in session_with_trigger_using_data_asset.fact_collection
    )
