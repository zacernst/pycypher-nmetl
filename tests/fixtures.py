"""
Fixtures for the unit tests.
"""
# pylint: disable=missing-function-docstring,protected-access,redefined-outer-name,too-many-lines

import json
import pathlib

import networkx as nx
import pytest
from nmetl.configuration import load_session_config
from nmetl.data_asset import DataAsset
from nmetl.data_source import (
    DataSource,
    DataSourceMapping,
    FixtureDataSource,
    NewColumn,
)
from nmetl.helpers import ensure_uri
from nmetl.session import RawDataProcessor, Session
from nmetl.trigger import VariableAttribute
from pycypher.fact import (  # We might get rid of this class entirely
    Etcd3FactCollection,
    RocksDBFactCollection,
    FactCollection,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactNodeRelatedToNode,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
    SimpleFactCollection,
)
from pycypher.logger import LOGGER
from pycypher.node_classes import Literal

TEST_DATA_DIRECTORY = pathlib.Path(__file__).parent / "test_data"

BACK_END_STORES = [SimpleFactCollection, Etcd3FactCollection, RocksDBFactCollection]


class patched_uuid:  # pylint: disable=invalid-name,too-few-public-methods
    """Creates a deterministic value for uuid hex"""

    @property
    def hex(self):
        return "SOME_HEX"


@pytest.fixture
def raw_data_processor():
    return RawDataProcessor()


@pytest.fixture
def squares_csv_data_source():
    squares_csv = TEST_DATA_DIRECTORY / "squares.csv"
    squares_csv_uri = ensure_uri(squares_csv)
    csv_data_source = DataSource.from_uri(squares_csv_uri, name="squares_csv")
    return csv_data_source


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
    data_source_label_mapping = DataSourceMapping(
        identifier_key="person_id",
        label="Person",
    )
    return [
        data_source_mapping_0,
        data_source_mapping_1,
        data_source_mapping_2,
        data_source_mapping_3,
        data_source_mapping_4,
        data_source_label_mapping,
    ]


@pytest.fixture(params=BACK_END_STORES)
def fact_collection_factory(request):
    return request.param()


@pytest.fixture(
    params=BACK_END_STORES
)
def fact_collection_cls_factory(request):
    return request.param


@pytest.fixture
def fact_collection_0(fact_collection_cls_factory):
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasAttributeWithValue("1", "key", Literal(2))
    fact3 = FactNodeRelatedToNode("1", "2", "MyRelationship")
    fact4 = FactNodeHasLabel("2", "OtherThing")
    fact5 = FactNodeHasAttributeWithValue("2", "key", Literal(5))
    fact6 = FactRelationshipHasLabel("relationship_123", "MyRelationship")
    fact7 = FactRelationshipHasSourceNode("relationship_123", "1")
    fact8 = FactRelationshipHasTargetNode("relationship_123", "2")
    fact_collection = fact_collection_cls_factory()
    fact_collection.append(fact1)
    fact_collection.append(fact2)
    fact_collection.append(fact3)
    fact_collection.append(fact4)
    fact_collection.append(fact5)
    fact_collection.append(fact6)
    fact_collection.append(fact7)
    fact_collection.append(fact8)
    yield fact_collection
    fact_collection.close()


@pytest.fixture
def fact_collection_1(fact_collection_cls_factory):
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("2", "Thing")
    fact3 = FactNodeHasLabel("3", "OtherThing")
    fact_collection = fact_collection_cls_factory()
    fact_collection.append(fact1)
    fact_collection.append(fact2)
    fact_collection.append(fact3)
    yield fact_collection
    fact_collection.close()


@pytest.fixture
def fact_collection_7(fact_collection_cls_factory):  # pylint: disable=too-many-locals
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

    fact_collection = fact_collection_cls_factory()

    fact_collection.append(fact1)
    fact_collection.append(fact2)
    fact_collection.append(fact3)
    fact_collection.append(fact4)
    fact_collection.append(fact5)
    fact_collection.append(fact6)
    fact_collection.append(fact7)
    fact_collection.append(fact8)
    fact_collection.append(fact9)
    fact_collection.append(fact10)
    fact_collection.append(fact11)
    fact_collection.append(fact12)
    fact_collection.append(fact13)
    fact_collection.append(fact14)
    fact_collection.append(fact15)
    fact_collection.append(fact16)
    fact_collection.append(fact17)
    fact_collection.append(fact18)
    fact_collection.append(fact19)
    fact_collection.append(fact20)
    fact_collection.append(fact21)
    fact_collection.append(fact22)
    fact_collection.append(fact23)
    fact_collection.append(fact24)
    fact_collection.append(fact25)

    yield fact_collection
    fact_collection.close()


@pytest.fixture
def fact_collection_squares_circles(fact_collection_cls_factory):  # pylint: disable=too-many-locals
    facts = [
        FactNodeHasLabel("square_1", "Square"),
        FactNodeHasAttributeWithValue("square_1", "length", Literal(2)),
        FactNodeHasLabel("square_2", "Square"),
        FactNodeHasAttributeWithValue("square_2", "length", Literal(3)),
        FactNodeHasLabel("square_3", "Square"),
        FactNodeHasAttributeWithValue("square_3", "length", Literal(4)),
        FactNodeHasLabel("square_4", "Square"),
        FactNodeHasAttributeWithValue("square_4", "length", Literal(5)),
        FactNodeHasLabel("circle_1", "Circle"),
        FactNodeHasAttributeWithValue("circle_1", "radius", Literal(2)),
        FactNodeHasLabel("circle_2", "Circle"),
        FactNodeHasAttributeWithValue("circle_2", "radius", Literal(3)),
        FactNodeHasLabel("circle_3", "Circle"),
        FactNodeHasAttributeWithValue("circle_3", "radius", Literal(4)),
        FactNodeHasLabel("circle_4", "Circle"),
        FactNodeHasAttributeWithValue("circle_4", "radius", Literal(5)),
        FactRelationshipHasLabel("relationship_1", "contains"),
        FactRelationshipHasSourceNode("relationship_1", "square_1"),
        FactRelationshipHasTargetNode("relationship_1", "circle_1"),
        FactRelationshipHasLabel("relationship_2", "contains"),
        FactRelationshipHasSourceNode("relationship_2", "square_2"),
        FactRelationshipHasTargetNode("relationship_2", "circle_2"),
        FactRelationshipHasLabel("relationship_3", "contains"),
        FactRelationshipHasSourceNode("relationship_3", "square_3"),
        FactRelationshipHasTargetNode("relationship_3", "circle_3"),
        FactRelationshipHasLabel("relationship_4", "contains"),
        FactRelationshipHasSourceNode("relationship_4", "square_3"),
        FactRelationshipHasTargetNode("relationship_4", "circle_4"),
        FactNodeHasAttributeWithValue(
            "square_1", "name", Literal("square_alice")
        ),
        FactNodeHasAttributeWithValue(
            "square_2", "name", Literal("square_bob")
        ),
        FactNodeHasAttributeWithValue(
            "square_3", "name", Literal("square_carol")
        ),
        FactNodeHasAttributeWithValue(
            "square_4", "name", Literal("square_dave")
        ),
        FactNodeHasAttributeWithValue(
            "circle_1", "name", Literal("circle_alice")
        ),
        FactNodeHasAttributeWithValue(
            "circle_2", "name", Literal("circle_bob")
        ),
        FactNodeHasAttributeWithValue(
            "circle_3", "name", Literal("circle_carol")
        ),
        FactNodeHasAttributeWithValue(
            "circle_4", "name", Literal("circle_dave")
        ),
    ]
    fact_collection = fact_collection_cls_factory()
    fact_collection += facts
    yield fact_collection
    fact_collection.close()


@pytest.fixture
def fact_collection_2(fact_collection_cls_factory):
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("2", "MiddleThing")
    fact3 = FactNodeHasLabel("3", "OtherThing")
    fact4 = FactRelationshipHasLabel("relationship_1", "MyRelationship")
    fact5 = FactRelationshipHasLabel("relationship_2", "OtherRelationship")
    fact6 = FactRelationshipHasSourceNode("relationship_1", "1")
    fact7 = FactRelationshipHasTargetNode("relationship_1", "2")
    fact8 = FactRelationshipHasSourceNode("relationship_2", "2")
    fact9 = FactRelationshipHasTargetNode("relationship_2", "3")
    facts = [
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
    fact_collection = fact_collection_cls_factory()
    fact_collection += facts
    yield fact_collection
    fact_collection.close()


@pytest.fixture
def fact_collection_3(fact_collection_cls_factory):
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

    facts = [
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
    fact_collection = fact_collection_cls_factory()
    fact_collection += facts

    yield fact_collection
    fact_collection.close()


@pytest.fixture
def fact_collection_4(fact_collection_cls_factory):
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

    facts = [
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
    fact_collection = fact_collection_cls_factory()
    fact_collection += facts

    yield fact_collection
    fact_collection.close()


@pytest.fixture
def fact_collection_5(fact_collection_cls_factory):
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

    #         'MATCH (n:Thing {foo: "2"})-[r:MyRelationship]->(m:MiddleThing)-'
    #         "[s:OtherRelationship]->(o:OtherThing) "
    #         "RETURN n.foobar"
    # [
    #     {'m': '2', 'r': 'relationship_3', 's': 'relationship_4', 'n': '4', 'o': '5'},
    #     {'m': '2', 'r': 'relationship_3', 's': 'relationship_2', 'n': '4', 'o': '3'}
    # ]
    facts = [
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

    fact_collection = fact_collection_cls_factory()
    fact_collection += facts

    yield fact_collection
    fact_collection.close()


@pytest.fixture
def fact_collection_6(fact_collection_cls_factory):  # pylint: disable=too-many-locals
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
    fact20 = FactNodeHasAttributeWithValue("3", "foo", Literal(2))
    fact21 = FactNodeHasAttributeWithValue("5", "foo", Literal(2))

    fact19 = FactNodeHasLabel("6", "Irrelevant")

    facts = [
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
    ]

    fact_collection = fact_collection_cls_factory()
    fact_collection += facts

    yield fact_collection
    fact_collection.close()


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
def empty_session():
    return Session(run_monitor=False)


@pytest.fixture
def populated_session(
    fixture_0_data_source_mapping_list, empty_session, fixture_data_source_0
):
    fixture_data_source_0.attach_mapping(fixture_0_data_source_mapping_list)
    empty_session.attach_data_source(fixture_data_source_0)
    return empty_session


@pytest.fixture
def shapes_session():
    LOGGER.setLevel("DEBUG")
    ingest_file = TEST_DATA_DIRECTORY / "ingest.yaml"
    session = load_session_config(ingest_file)
    session.start_threads()
    session.block_until_finished()
    return session


@pytest.fixture
def session_with_trigger():
    ingest_file = TEST_DATA_DIRECTORY / "ingest.yaml"
    session = load_session_config(ingest_file)

    @session.trigger(
        "MATCH (s:Square)-[my_relationship:contains]->(c:Circle) "
        "WITH s.side_length AS side_length "
        "RETURN side_length"
    )
    def compute_area(side_length) -> VariableAttribute["s", "area"]:  # type: ignore
        return side_length**2

    return session


@pytest.fixture
def session_with_two_triggers():
    ingest_file = TEST_DATA_DIRECTORY / "ingest.yaml"
    session = load_session_config(ingest_file)

    @session.trigger(
        "MATCH (s:Square)-[my_relationship:contains]->(c:Circle) "
        "WITH s.side_length AS side_length RETURN side_length"
    )
    def compute_area(side_length) -> VariableAttribute["s", "area"]:  # type: ignore
        return side_length**2

    @session.trigger(
        "MATCH (s:Square)-[my_relationship:contains]->(c:Circle) "
        "WITH s.area AS square_area "
        "RETURN square_area"
    )
    def compute_bigness(square_area) -> VariableAttribute["s", "big"]:  # type: ignore
        return square_area > 10

    return session


@pytest.fixture
def session_with_three_triggers():
    ingest_file = TEST_DATA_DIRECTORY / "ingest.yaml"
    session = load_session_config(ingest_file)

    @session.trigger(
        "MATCH (s:Square)-[my_relationship:contains]->(c:Circle) "
        "WITH s.side_length AS side_length "
        "RETURN side_length"
    )
    def compute_area(side_length) -> VariableAttribute["s", "area"]:  # type: ignore
        return side_length**2

    @session.trigger(
        "MATCH (s:Square)-[my_relationship:contains]->(c:Circle) "
        "WITH s.area AS square_area "
        "RETURN square_area"
    )
    def compute_bigness(square_area) -> VariableAttribute["s", "big"]:  # type: ignore
        return square_area > 10

    @session.trigger(
        "MATCH (s:Square)-[my_relationship:contains]->(c:Circle) "
        "WITH s.big AS bigness "
        "RETURN bigness"
    )
    def compute_smallness(bigness) -> VariableAttribute["s", "small"]:  # type: ignore
        return not bigness

    return session


@pytest.fixture
def session_with_aggregation_fixture():
    ingest_file = TEST_DATA_DIRECTORY / "ingest.yaml"
    session = load_session_config(ingest_file)

    @session.trigger(
        "MATCH (s:Square)-[my_relationship:contains]->(c:Circle) "
        "WITH s.side_length AS side_length "
        "RETURN side_length"
    )
    def compute_area(side_length) -> VariableAttribute["s", "area"]:  # type: ignore
        return side_length**2

    @session.trigger(
        "MATCH (s:Square)-[my_relationship:contains]->(c:Circle) "
        "WITH s.area AS square_area "
        "RETURN square_area"
    )
    def compute_bigness(square_area) -> VariableAttribute["s", "big"]:  # type: ignore
        return square_area > 10

    @session.trigger(
        "MATCH (s:Square)-[my_relationship:contains]->(c:Circle) "
        "WITH s.big AS bigness "
        "RETURN bigness"
    )
    def compute_smallness(bigness) -> VariableAttribute["s", "small"]:  # type: ignore
        return not bigness

    @session.trigger(
        "MATCH (s:Square)-[r:contains]->(c:Circle) "
        "WITH s.name AS square_name, s.side_length AS side_length, COLLECT(c.radius) AS radii "
        "RETURN side_length, radii"
    )  # Should be an alias, not an ObjectAttributeLookup
    def aggregation_of_radii(
        side_length,
        radii,  # pylint: disable=unused-argument
    ) -> VariableAttribute["s", "num_circles"]:  # type: ignore
        return len(radii)

    return session


@pytest.fixture
def session_with_city_state_fixture():
    ingest_file = TEST_DATA_DIRECTORY / "ingest_city_state.yaml"
    session = load_session_config(ingest_file)

    @session.new_column("city_table")
    def city_state(city: str, state: str) -> NewColumn["city_state"]:
        return "__".join([city, state])

    return session


@pytest.fixture
def data_asset_1():
    with open(
        TEST_DATA_DIRECTORY / "data_asset_1.json", "r", encoding="utf8"
    ) as file:
        data = json.load(file)
    return DataAsset(name="data_asset_1", obj=data)


@pytest.fixture
def session_with_data_asset(data_asset_1):
    ingest_file = TEST_DATA_DIRECTORY / "ingest.yaml"
    session = load_session_config(ingest_file)
    session.register_data_asset(data_asset_1)
    return session


@pytest.fixture
def session_with_trigger_using_data_asset(session_with_data_asset):
    with open(
        TEST_DATA_DIRECTORY / "data_asset_1.json", "r", encoding="utf8"
    ) as file:
        data = json.load(file)
    data_asset = DataAsset(name="my_data_asset", obj=data)
    session_with_data_asset.register_data_asset(data_asset)

    @session_with_data_asset.trigger(
        "MATCH (s:Square)-[my_relationship:contains]->(c:Circle) "
        "WITH s.side_length AS side_length, c.radius AS radius "
        "RETURN side_length"
    )
    def compute_bar_with_data_asset(
        side_length,  # pylint: disable=unused-argument
        my_data_asset,
    ) -> VariableAttribute["s", "foo"]:
        return my_data_asset["foo"]

    return session_with_data_asset


@pytest.fixture
def etcd3_fact_collection():
    fact_collection = Etcd3FactCollection()
    yield fact_collection
    fact_collection.clear()


# Cypher
# └── Query
#     ├── Match
#     │   ├── RelationshipChainList
#     │   │   └── RelationshipChain
#     │   │       ├── Node
#     │   │       │   ├── NodeNameLabel
#     │   │       │   │   ├── s
#     │   │       │   │   └── Square
#     │   │       │   └── MappingSet
#     │   │       ├── RelationshipLeftRight
#     │   │       │   └── Relationship
#     │   │       │       └── NodeNameLabel
#     │   │       │           ├── r
#     │   │       │           └── contains
#     │   │       └── Node
#     │   │           ├── NodeNameLabel
#     │   │           │   ├── c
#     │   │           │   └── Circle
#     │   │           └── MappingSet
#     │   └── WithClause
#     │       └── ObjectAsSeries
#     │           ├── Alias
#     │           │   ├── ObjectAttributeLookup
#     │           │   │   ├── c
#     │           │   │   └── name
#     │           │   └── circle_name
#     │           └── Alias
#     │               ├── Aggregation
#     │               │   └── Collect
#     │               │       └── ObjectAttributeLookup
#     │               │           ├── s
#     │               │           └── length
#     │               └── lengths
#     └── Return
#         └── Projection
#             ├── ObjectAttributeLookup
#             │   └── circle_name
#             └── ObjectAttributeLookup
#                 └── length
