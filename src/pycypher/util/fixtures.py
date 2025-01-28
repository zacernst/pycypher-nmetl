"""
Fixtures for the unit tests.
"""

# pylint: disable=missing-function-docstring,protected-access,redefined-outer-name,too-many-lines
import networkx as nx
import pytest

from pycypher.core.node_classes import Literal
from pycypher.etl.data_source import DataSourceMapping, FixtureDataSource
from pycypher.etl.fact import (  # We might get rid of this class entirely
    FactCollection,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactNodeRelatedToNode,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
)
from pycypher.etl.goldberg import Goldberg, RawDataProcessor


class patched_uuid:  # pylint: disable=invalid-name,too-few-public-methods
    """Creates a deterministic value for uuid hex"""

    @property
    def hex(self):
        return "SOME_HEX"


@pytest.fixture
def raw_data_processor():
    return RawDataProcessor()


@pytest.fixture
def fixture_data_source_0():
    # name, age, zip code, widgets_purchased
    obj = FixtureDataSource(
        name="people_fixture",
        data=[
            {
                "person_id": "001",
                "name": "Alice",
                "age": 25,
                "zip_code": "02056",
                "widgets": 5,
            },
            {
                "person_id": "002",
                "name": "Bob",
                "age": 30,
                "zip_code": "02055",
                "widgets": 3,
            },
            {
                "person_id": "003",
                "name": "Charlie",
                "age": 35,
                "zip_code": "02054",
                "widgets": 2,
            },
            {
                "person_id": "004",
                "name": "David",
                "age": 40,
                "zip_code": "02053",
                "widgets": 1,
            },
            {
                "person_id": "005",
                "name": "Eve",
                "age": 45,
                "zip_code": "02052",
                "widgets": 4,
            },
            {
                "person_id": "006",
                "name": "Frank",
                "age": 50,
                "zip_code": "02051",
                "widgets": 6,
            },
            {
                "person_id": "007",
                "name": "Grace",
                "age": 55,
                "zip_code": "02050",
                "widgets": 7,
            },
        ],
    )
    return obj


@pytest.fixture
def fixture_0_data_source_mapping_list():
    data_source_mapping_0 = DataSourceMapping(
        attribute_key="person_id",
        identifier_key="person_id",
        attribute="Identifier",
        label="Person",
    )
    data_source_mapping_1 = DataSourceMapping(
        attribute_key="name",
        identifier_key="person_id",
        attribute="Name",
        label="Person",
    )
    data_source_mapping_2 = DataSourceMapping(
        attribute_key="age",
        identifier_key="person_id",
        attribute="Age",
        label="Person",
    )
    data_source_mapping_3 = DataSourceMapping(
        attribute_key="zip_code",
        identifier_key="person_id",
        attribute="ZipCode",
        label="Person",
    )
    data_source_mapping_4 = DataSourceMapping(
        attribute_key="widgets",
        identifier_key="person_id",
        attribute="WidgetsPurchased",
        label="Person",
    )
    return [
        data_source_mapping_0,
        data_source_mapping_1,
        data_source_mapping_2,
        data_source_mapping_3,
        data_source_mapping_4,
    ]


@pytest.fixture
def fact_collection_0():
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasAttributeWithValue("1", "key", Literal(2))
    fact3 = FactNodeRelatedToNode("1", "2", "MyRelationship")
    fact4 = FactNodeHasLabel("2", "OtherThing")
    fact5 = FactNodeHasAttributeWithValue("2", "key", Literal(5))
    fact6 = FactRelationshipHasLabel("relationship_123", "MyRelationship")
    fact7 = FactRelationshipHasSourceNode("relationship_123", "1")
    fact8 = FactRelationshipHasTargetNode("relationship_123", "2")
    fact_collection = FactCollection(
        [
            fact1,
            fact2,
            fact3,
            fact4,
            fact5,
            fact6,
            fact7,
            fact8,
        ]
    )

    return fact_collection


@pytest.fixture
def fact_collection_1():
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("2", "Thing")
    fact3 = FactNodeHasLabel("3", "OtherThing")
    fact_collection = FactCollection(
        [
            fact1,
            fact2,
            fact3,
        ]
    )

    return fact_collection


@pytest.fixture
def fact_collection_7():  # pylint: disable=too-many-locals
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("2", "MiddleThing")
    fact3 = FactNodeHasLabel("3", "OtherThing")
    fact4 = FactRelationshipHasLabel("relationship_1", "MyRelationship")
    fact5 = FactRelationshipHasLabel("relationship_2", "OtherRelationship")
    fact6 = FactRelationshipHasSourceNode("relationship_1", "1")
    fact7 = FactRelationshipHasTargetNode("relationship_1", "2")
    fact8 = FactRelationshipHasSourceNode("relationship_2", "2")
    fact9 = FactRelationshipHasTargetNode("relationship_2", "3")

    fact10 = FactNodeHasLabel("4", "Thing")
    fact11 = FactRelationshipHasLabel("relationship_3", "MyRelationship")
    fact12 = FactRelationshipHasSourceNode("relationship_3", "4")
    fact13 = FactRelationshipHasTargetNode("relationship_3", "2")

    fact14 = FactNodeHasLabel("5", "OtherThing")
    fact15 = FactRelationshipHasLabel("relationship_4", "OtherRelationship")
    fact16 = FactRelationshipHasSourceNode("relationship_4", "2")
    fact17 = FactRelationshipHasTargetNode("relationship_4", "5")

    fact18 = FactNodeHasAttributeWithValue("4", "foo", Literal(2))
    fact20 = FactNodeHasAttributeWithValue("2", "bar", Literal(3))
    fact21 = FactNodeHasAttributeWithValue("2", "foo", Literal(20))
    fact22 = FactNodeHasAttributeWithValue("4", "bar", Literal(30))

    fact23 = FactNodeHasAttributeWithValue("5", "oattr", Literal(5))
    fact24 = FactNodeHasAttributeWithValue("3", "oattr", Literal(4))

    fact25 = FactNodeHasAttributeWithValue("1", "foo", Literal(42))

    fact19 = FactNodeHasLabel("6", "Irrelevant")

    fact_collection = FactCollection(
        [
            fact1,
            fact2,
            fact3,
            fact4,
            fact5,
            fact6,
            fact7,
            fact8,
            fact9,
            fact10,
            fact11,
            fact12,
            fact13,
            fact14,
            fact15,
            fact16,
            fact17,
            fact18,
            fact19,
            fact20,
            fact21,
            fact22,
            fact23,
            fact24,
            fact25,
        ]
    )

    return fact_collection


@pytest.fixture
def fact_collection_2():
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("2", "MiddleThing")
    fact3 = FactNodeHasLabel("3", "OtherThing")
    fact4 = FactRelationshipHasLabel("relationship_1", "MyRelationship")
    fact5 = FactRelationshipHasLabel("relationship_2", "OtherRelationship")
    fact6 = FactRelationshipHasSourceNode("relationship_1", "1")
    fact7 = FactRelationshipHasTargetNode("relationship_1", "2")
    fact8 = FactRelationshipHasSourceNode("relationship_2", "2")
    fact9 = FactRelationshipHasTargetNode("relationship_2", "3")
    fact_collection = FactCollection(
        [
            fact1,
            fact2,
            fact3,
            fact4,
            fact5,
            fact6,
            fact7,
            fact8,
            fact9,
        ]
    )

    return fact_collection


@pytest.fixture
def fact_collection_3():
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("2", "MiddleThing")
    fact3 = FactNodeHasLabel("3", "OtherThing")
    fact4 = FactRelationshipHasLabel("relationship_1", "MyRelationship")
    fact5 = FactRelationshipHasLabel("relationship_2", "OtherRelationship")
    fact6 = FactRelationshipHasSourceNode("relationship_1", "1")
    fact7 = FactRelationshipHasTargetNode("relationship_1", "2")
    fact8 = FactRelationshipHasSourceNode("relationship_2", "2")
    fact9 = FactRelationshipHasTargetNode("relationship_2", "3")
    fact10 = FactNodeHasLabel("4", "Thing")
    fact11 = FactRelationshipHasLabel("relationship_3", "MyRelationship")
    fact12 = FactRelationshipHasSourceNode("relationship_3", "4")
    fact13 = FactRelationshipHasTargetNode("relationship_3", "2")

    fact_collection = FactCollection(
        [
            fact1,
            fact2,
            fact3,
            fact4,
            fact5,
            fact6,
            fact7,
            fact8,
            fact9,
            fact10,
            fact11,
            fact12,
            fact13,
        ]
    )

    return fact_collection


@pytest.fixture
def fact_collection_4():
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("2", "MiddleThing")
    fact3 = FactNodeHasLabel("3", "OtherThing")
    fact4 = FactRelationshipHasLabel("relationship_1", "MyRelationship")
    fact5 = FactRelationshipHasLabel("relationship_2", "OtherRelationship")
    fact6 = FactRelationshipHasSourceNode("relationship_1", "1")
    fact7 = FactRelationshipHasTargetNode("relationship_1", "2")
    fact8 = FactRelationshipHasSourceNode("relationship_2", "2")
    fact9 = FactRelationshipHasTargetNode("relationship_2", "3")

    fact10 = FactNodeHasLabel("4", "Thing")
    fact11 = FactRelationshipHasLabel("relationship_3", "MyRelationship")
    fact12 = FactRelationshipHasSourceNode("relationship_3", "4")
    fact13 = FactRelationshipHasTargetNode("relationship_3", "2")

    fact14 = FactNodeHasLabel("5", "OtherThing")
    fact15 = FactRelationshipHasLabel("relationship_4", "OtherRelationship")
    fact16 = FactRelationshipHasSourceNode("relationship_4", "2")
    fact17 = FactRelationshipHasTargetNode("relationship_4", "5")

    fact_collection = FactCollection(
        [
            fact1,
            fact2,
            fact3,
            fact4,
            fact5,
            fact6,
            fact7,
            fact8,
            fact9,
            fact10,
            fact11,
            fact12,
            fact13,
            fact14,
            fact15,
            fact16,
            fact17,
        ]
    )

    return fact_collection


@pytest.fixture
def fact_collection_5():
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("2", "MiddleThing")
    fact3 = FactNodeHasLabel("3", "OtherThing")
    fact4 = FactRelationshipHasLabel("relationship_1", "MyRelationship")
    fact5 = FactRelationshipHasLabel("relationship_2", "OtherRelationship")
    fact6 = FactRelationshipHasSourceNode("relationship_1", "1")
    fact7 = FactRelationshipHasTargetNode("relationship_1", "2")
    fact8 = FactRelationshipHasSourceNode("relationship_2", "2")
    fact9 = FactRelationshipHasTargetNode("relationship_2", "3")

    fact10 = FactNodeHasLabel("4", "Thing")
    fact11 = FactRelationshipHasLabel("relationship_3", "MyRelationship")
    fact12 = FactRelationshipHasSourceNode("relationship_3", "4")
    fact13 = FactRelationshipHasTargetNode("relationship_3", "2")

    fact14 = FactNodeHasLabel("5", "OtherThing")
    fact15 = FactRelationshipHasLabel("relationship_4", "OtherRelationship")
    fact16 = FactRelationshipHasSourceNode("relationship_4", "2")
    fact17 = FactRelationshipHasTargetNode("relationship_4", "5")

    fact18 = FactNodeHasAttributeWithValue("4", "foo", Literal(2))

    fact_collection = FactCollection(
        [
            fact1,
            fact2,
            fact3,
            fact4,
            fact5,
            fact6,
            fact7,
            fact8,
            fact9,
            fact10,
            fact11,
            fact12,
            fact13,
            fact14,
            fact15,
            fact16,
            fact17,
            fact18,
        ]
    )

    return fact_collection


@pytest.fixture
def fact_collection_6():  # pylint: disable=too-many-locals
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("2", "MiddleThing")
    fact3 = FactNodeHasLabel("3", "OtherThing")
    fact4 = FactRelationshipHasLabel("relationship_1", "MyRelationship")
    fact5 = FactRelationshipHasLabel("relationship_2", "OtherRelationship")
    fact6 = FactRelationshipHasSourceNode("relationship_1", "1")
    fact7 = FactRelationshipHasTargetNode("relationship_1", "2")
    fact8 = FactRelationshipHasSourceNode("relationship_2", "2")
    fact9 = FactRelationshipHasTargetNode("relationship_2", "3")

    fact10 = FactNodeHasLabel("4", "Thing")
    fact11 = FactRelationshipHasLabel("relationship_3", "MyRelationship")
    fact12 = FactRelationshipHasSourceNode("relationship_3", "4")
    fact13 = FactRelationshipHasTargetNode("relationship_3", "2")

    fact14 = FactNodeHasLabel("5", "OtherThing")
    fact15 = FactRelationshipHasLabel("relationship_4", "OtherRelationship")
    fact16 = FactRelationshipHasSourceNode("relationship_4", "2")
    fact17 = FactRelationshipHasTargetNode("relationship_4", "5")

    fact18 = FactNodeHasAttributeWithValue("4", "foo", Literal(2))

    fact19 = FactNodeHasLabel("6", "Irrelevant")

    fact_collection = FactCollection(
        [
            fact1,
            fact2,
            fact3,
            fact4,
            fact5,
            fact6,
            fact7,
            fact8,
            fact9,
            fact10,
            fact11,
            fact12,
            fact13,
            fact14,
            fact15,
            fact16,
            fact17,
            fact18,
            fact19,
        ]
    )

    return fact_collection


@pytest.fixture
def networkx_graph():
    edge_dictionary = {
        "a": ["b", "c", "d", "e"],
        "b": ["a", "e"],
        "c": ["a", "d"],
        "d": ["a", "c"],
        "e": ["a", "b"],
        "f": ["g"],
        "g": ["f"],
    }

    graph = nx.DiGraph(edge_dictionary)

    graph.nodes["a"]["name"] = "Alice"
    graph.nodes["b"]["name"] = "Bob"
    graph.nodes["c"]["name"] = "Charlie"
    graph.nodes["d"]["name"] = "David"
    graph.nodes["e"]["name"] = "Eve"
    graph.nodes["f"]["name"] = "Frank"
    graph.nodes["g"]["name"] = "Grace"

    graph.nodes["a"]["age"] = 25
    graph.nodes["b"]["age"] = 30
    graph.nodes["c"]["age"] = 35
    graph.nodes["d"]["age"] = 40
    graph.nodes["e"]["age"] = 45
    graph.nodes["f"]["age"] = 50
    graph.nodes["g"]["age"] = 55

    graph.nodes["a"]["category"] = "Person"
    graph.nodes["b"]["category"] = "Person"
    graph.nodes["c"]["category"] = "Person"
    graph.nodes["d"]["category"] = "Person"
    graph.nodes["e"]["category"] = "Person"
    graph.nodes["f"]["category"] = "Person"
    graph.nodes["g"]["category"] = "Person"

    return graph


@pytest.fixture
def number_of_facts(fact_collection_0: FactCollection) -> int:
    return len(fact_collection_0.facts)


@pytest.fixture
def empty_goldberg():
    return Goldberg(run_monitor=False)


@pytest.fixture
def populated_goldberg(
    fixture_0_data_source_mapping_list, empty_goldberg, fixture_data_source_0
):
    # Get data source mappings
    # Attach data source mappings to data source
    # Attach data source to goldberg

    fixture_data_source_0.attach_mapping(fixture_0_data_source_mapping_list)
    empty_goldberg.attach_data_source(fixture_data_source_0)
    return empty_goldberg
