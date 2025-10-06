# """All the tests."""

import copy
import logging
import pathlib
from typing import TYPE_CHECKING, Any, Dict, List, Tuple
from unittest.mock import patch

import pytest
from nmetl.queue_processor import (
    CheckFactAgainstTriggersQueueProcessor,
    SubTriggerPair,
    TriggeredLookupProcessor,
)
from nmetl.session import Session
from nmetl.trigger import (
    NodeRelationship,
    NodeRelationshipTrigger,
    VariableAttribute,
    VariableAttributeTrigger,
)
from pycypher.cypher_parser import CypherParser
from pycypher.fact import (
    AtomicFact,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
)
from pycypher.fact_collection import FactCollection
from pycypher.fact_collection.simple import SimpleFactCollection
from pycypher.node_classes import (
    FALSE,
    TRUE,
    Addition,
    Aggregation,
    Alias,
    AliasedName,
    And,
    Collect,
    Collection,
    Cypher,
    Equals,
    Evaluable,
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
    Relationship,
    RelationshipChain,
    RelationshipChainList,
    RelationshipLeftRight,
    Return,
    Size,
    Where,
    WithClause,
    get_all_substitutions,
    get_variable_substitutions,
    model_to_projection,
    models_to_projection_list,
)
from pycypher.query import (
    NullResult,
    QuerySourceNodeOfRelationship,
    QueryTargetNodeOfRelationship,
)
from pycypher.solutions import Projection, ProjectionList
from pysat.solvers import Glucose42
from shared.logger import LOGGER

if TYPE_CHECKING:
    from pycypher.solutions import Projection, ProjectionList

TEST_DATA_DIRECTORY: pathlib.Path = pathlib.Path(__file__).parent / "test_data"

LOGGER.setLevel(logging.INFO)


@pytest.fixture
def session(city_state_fact_collection):
    """A session fixture."""

    session: Session = Session(dask_client=None, create_queue_generators=False)
    yield session.attach_fact_collection(city_state_fact_collection)
    # Clean up here?


@pytest.fixture
def check_fact_against_triggers_queue_processor():
    obj = CheckFactAgainstTriggersQueueProcessor()
    return obj


def test_evaluate_trigger_against_fact_collection(
    session, check_fact_against_triggers_queue_processor
):
    # Need to patch QueueProcessor.get_trigger_dict to return session's trigger_dict
    with patch(
        "nmetl.queue_processor.CheckFactAgainstTriggersQueueProcessor.get_trigger_dict"
    ) as mocked_function:

        @session.trigger("MATCH (c:City) WITH c.has_beach AS beachy RETURN beachy")
        def has_sand(beachy) -> VariableAttribute["c", "sandy"]:  # type: ignore
            return beachy

        mocked_function.return_value = session.trigger_dict

        kalamazoo: FactNodeHasAttributeWithValue = FactNodeHasAttributeWithValue(
            node_id="kalamazoo",
            attribute="has_beach",
            value=Literal(False),
        )
        batch: list[AtomicFact] = [kalamazoo]

        trigger = list(session.trigger_dict.values())[0]
        with patch(
            "nmetl.queue_processor.CheckFactAgainstTriggersQueueProcessor.get_trigger_dict"
        ) as mocked_function_trigger:
            mocked_function_trigger.return_value = session.trigger_dict

            with patch(
                "nmetl.queue_processor.CheckFactAgainstTriggersQueueProcessor.evaluate_fact_against_trigger"
            ) as mocked_function:
                out = trigger.cypher._evaluate(
                    session.fact_collection,
                    projection_list=ProjectionList(
                        projection_list=[Projection(projection={"c": "kalamazoo"})]
                    ),
                )
            assert isinstance(out, ProjectionList)
            assert len(out.projection_list) == 1
            assert out.projection_list[0].projection == {"beachy": Literal(False)}


def test_check_fact_against_triggers_queue_processor_return_no_sub_trigger_pair_if_no_match(
    session,
    check_fact_against_triggers_queue_processor,
    city_state_fact_collection,
):
    # Need to patch QueueProcessor.get_trigger_dict to return session's trigger_dict
    with patch(
        "nmetl.queue_processor.QueueProcessor.get_trigger_dict"
    ) as mocked_function:

        @session.trigger("MATCH (c:City) WITH c.idontexist AS beachy RETURN beachy")
        def has_sand(beachy) -> VariableAttribute["c", "sandy"]:  # type: ignore
            return beachy

        mocked_function.return_value = session.trigger_dict

        kalamazoo: FactNodeHasAttributeWithValue = FactNodeHasAttributeWithValue(
            node_id="kalamazoo",
            attribute="has_beach",
            value=Literal(False),
        )
        buffer: list[AtomicFact] = [kalamazoo]

        trigger = list(session.trigger_dict.values())[0]
        with patch(
            "nmetl.queue_processor.QueueProcessor.get_fact_collection"
        ) as mocked_function_trigger:
            mocked_function_trigger.return_value = city_state_fact_collection

            with patch(
                "nmetl.queue_processor.CheckFactAgainstTriggersQueueProcessor.evaluate_fact_against_trigger"
            ) as mocked_function:
                out = (
                    check_fact_against_triggers_queue_processor.process_item_from_queue(
                        buffer,
                    )
                )
                # [SubTriggerPair(sub={'c': 'kalamazoo'}, trigger=VariableAttributeTrigger)]
                assert not out


def test_check_fact_against_triggers_queue_processor_return_sub_trigger_pair(
    session,
    check_fact_against_triggers_queue_processor,
    city_state_fact_collection,
):
    # Need to patch QueueProcessor.get_trigger_dict to return session's trigger_dict
    with patch(
        "nmetl.queue_processor.QueueProcessor.get_trigger_dict"
    ) as mocked_function:

        @session.trigger("MATCH (c:City) WITH c.has_beach AS beachy RETURN beachy")
        def has_sand(beachy) -> VariableAttribute["c", "sandy"]:  # type: ignore
            return beachy

        mocked_function.return_value = session.trigger_dict

        kalamazoo: FactNodeHasAttributeWithValue = FactNodeHasAttributeWithValue(
            node_id="kalamazoo",
            attribute="has_beach",
            value=Literal(False),
        )
        buffer: list[AtomicFact] = [kalamazoo]

        trigger = list(session.trigger_dict.values())[0]
        with patch(
            "nmetl.queue_processor.QueueProcessor.get_fact_collection"
        ) as mocked_function_trigger:
            mocked_function_trigger.return_value = city_state_fact_collection

            out = check_fact_against_triggers_queue_processor.process_item_from_queue(
                buffer,
            )
            sub_trigger_pair: SubTriggerPair = out[0]
            assert isinstance(sub_trigger_pair, SubTriggerPair)
            assert isinstance(sub_trigger_pair.sub, dict)
            assert isinstance(sub_trigger_pair.trigger, VariableAttributeTrigger)
            assert sub_trigger_pair.sub == {"c": "kalamazoo"}


def test_check_fact_against_trigger_with_aggregation_queue_processor_return_sub_trigger_pair(
    session,
    check_fact_against_triggers_queue_processor,
    city_state_fact_collection,
):
    # Need to patch QueueProcessor.get_trigger_dict to return session's trigger_dict
    with patch(
        "nmetl.queue_processor.QueueProcessor.get_trigger_dict"
    ) as mocked_function:

        @session.trigger(
            "MATCH (c:City)-[r:In]->(s:State) "
            "WITH COLLECT(c.has_beach) AS beachy_list "
            "RETURN beachy_list AS beachy_collection"
        )
        def beach_collection_length(
            beachy_collection,
        ) -> VariableAttribute["s", "num_beaches"]:  # type: ignore
            return len(beachy_collection) > 0

        mocked_function.return_value = session.trigger_dict

        kalamazoo: FactNodeHasAttributeWithValue = FactNodeHasAttributeWithValue(
            node_id="kalamazoo",
            attribute="has_beach",
            value=Literal(False),
        )
        buffer: list[AtomicFact] = [kalamazoo]

        trigger = list(session.trigger_dict.values())[0]
        with patch(
            "nmetl.queue_processor.QueueProcessor.get_fact_collection"
        ) as mocked_function_trigger:
            mocked_function_trigger.return_value = city_state_fact_collection

            # with patch('nmetl.queue_processor.CheckFactAgainstTriggersQueueProcessor.evaluate_fact_against_trigger') as mocked_function:
            out = check_fact_against_triggers_queue_processor.process_item_from_queue(
                buffer,
            )
            # [SubTriggerPair(sub={'c': 'kalamazoo'}, trigger=VariableAttributeTrigger)]
            sub_trigger_pair: SubTriggerPair = out[0]
            assert isinstance(sub_trigger_pair, SubTriggerPair)
            assert isinstance(sub_trigger_pair.sub, dict)
            assert isinstance(sub_trigger_pair.trigger, VariableAttributeTrigger)
            assert sub_trigger_pair.sub == {"c": "kalamazoo"}

            # variable_to_set: str = sub_trigger_pair.trigger.variable_set
            # attribute_to_set: str = sub_trigger_pair.trigger.attribute_set
            # instance_of_variable_to_set: str = sub_trigger_pair.projection_list[0].root[0][variable_to_set]
            # re_query_projection_list: ProjectionList = ProjectionList(projection_list=[Projection(projection={variable_to_set: instance_of_variable_to_set})])
            # sub_trigger_pair.trigger.cypher._evaluate(city_state_fact_collection, projection_list=re_query_projection_list)


def test_re_query(
    session,
    check_fact_against_triggers_queue_processor,
    city_state_fact_collection,
):
    with patch(
        "nmetl.queue_processor.QueueProcessor.get_trigger_dict"
    ) as mocked_function:

        @session.trigger(
            "MATCH (c:City)-[r:In]->(s:State) WITH COLLECT(c.has_beach) AS beachy_list "
            "RETURN beachy_list AS beachy_collection"
        )
        def beach_collection_length(
            beachy_collection,
        ) -> VariableAttribute["s", "num_beaches"]:  # type: ignore
            return len([i for i in beachy_collection if i])

        mocked_function.return_value = session.trigger_dict

        kalamazoo: FactNodeHasAttributeWithValue = FactNodeHasAttributeWithValue(
            node_id="kalamazoo",
            attribute="has_beach",
            value=Literal(False),
        )
        buffer: list[AtomicFact] = [kalamazoo]

        trigger = list(session.trigger_dict.values())[0]
        with patch(
            "nmetl.queue_processor.QueueProcessor.get_fact_collection",
        ) as mocked_function_trigger:
            mocked_function_trigger.return_value = city_state_fact_collection

            out = check_fact_against_triggers_queue_processor.process_item_from_queue(
                buffer,
            )
            sub_trigger_pair: SubTriggerPair = out[
                0
            ]  # Always a singleton, theoretically

            generated_fact: FactNodeHasAttributeWithValue | None = (
                TriggeredLookupProcessor._process_sub_trigger_pair(sub_trigger_pair)
            )

        expected_fact: FactNodeHasAttributeWithValue = FactNodeHasAttributeWithValue(
            node_id="michigan",
            attribute="num_beaches",
            value=1,
        )

        assert generated_fact == expected_fact


def test_pythonify_literal():
    assert Literal(1).pythonify() == 1
    assert Literal(1.0).pythonify() == 1.0
    assert Literal("1").pythonify() == "1"
    assert Literal(True).pythonify() is True
    assert Literal(False).pythonify() is False
    assert Literal(None).pythonify() is None


def test_pythonify_collection():
    c = Collection([Literal(1), Literal("foo"), Literal(True)])
    assert c.pythonify() == [1, "foo", True]


def test_trigger_initialization_variable_attribute_calls_initializer(
    session,
) -> None:
    with patch("nmetl.session.Session.process_return_annotation") as mocked_function:

        @session.trigger("MATCH (c:City) WITH c.has_beach AS beachy RETURN beachy")
        def has_sand(beachy) -> VariableAttribute["c", "sandy"]:  # type: ignore
            return beachy

        mocked_function.assert_called_once()


def test_trigger_initialization_variable_attribute_calls_process_variable_attribute_annotation(
    session,
) -> None:
    with patch(
        "nmetl.session.Session.process_variable_attribute_annotation"
    ) as mocked_function:

        @session.trigger("MATCH (c:City) WITH c.has_beach AS beachy RETURN beachy")
        def has_sand(beachy) -> VariableAttribute["c", "sandy"]:  # type: ignore
            return beachy

        mocked_function.assert_called_once()


def test_trigger_initialization_creates_trigger_object_attribute_value(
    session,
) -> None:
    @session.trigger("MATCH (c:City) WITH c.has_beach AS beachy RETURN beachy")
    def has_sand(beachy) -> VariableAttribute["c", "sandy"]:  # type: ignore
        return beachy

    assert session.trigger_dict
    assert len(session.trigger_dict) == 1
    trigger = list(session.trigger_dict.values())[0]
    assert isinstance(trigger, VariableAttributeTrigger)
    assert trigger.is_attribute_trigger
    assert not trigger.is_relationship_trigger
    assert trigger.attribute_set == "sandy"
    assert trigger.variable_set == "c"


def test_trigger_initialization_creates_trigger_attribute_with_collection(
    session,
) -> None:
    @session.trigger(
        "MATCH (c:City)-[r:In]->(s:State) WITH COLLECT(c.has_beach) AS beachy_list RETURN beachy_list AS beachy_collection"
    )
    def beachy_collection_function(
        beachy_collection,
    ) -> VariableAttribute["s", "num_beachy_cities"]:  # type: ignore
        return len(beachy_collection)

    assert session.trigger_dict
    assert len(session.trigger_dict) == 1
    trigger = list(session.trigger_dict.values())[0]
    assert isinstance(trigger, VariableAttributeTrigger)
    assert trigger.parameter_names == ["beachy_collection"]
    assert trigger.variable_set == "s"
    assert trigger.attribute_set == "num_beachy_cities"
    assert not trigger.is_relationship_trigger
    assert trigger.is_attribute_trigger


@pytest.fixture
def with_clause() -> WithClause:
    clause: WithClause = WithClause(
        lookups=ObjectAsSeries(
            lookups=[
                Alias(
                    reference=ObjectAttributeLookup(
                        object="state", attribute="looks_like_mitten"
                    ),
                    alias="mitten_state",
                ),
                Alias(
                    reference=ObjectAttributeLookup(
                        object="state", attribute="lots_of_lakes"
                    ),
                    alias="lakes",
                ),
                Alias(
                    reference=Collect(
                        ObjectAttributeLookup(
                            object="city",
                            attribute="has_beach",
                        ),
                    ),
                    alias="sandy",
                ),
            ],
        ),
    )
    return clause


@pytest.fixture
def relationship_chain_list_1() -> RelationshipChainList:
    source_node: Node = Node(
        name_label=NodeNameLabel(name="i", label="City"),
        mapping_set=MappingSet(
            mappings=[
                Mapping(
                    key="has_beach",
                    value=Literal(True),
                ),
            ],
        ),
    )

    target_node: Node = Node(
        name_label=NodeNameLabel(name="k", label="State"),
        mapping_set=MappingSet(
            mappings=[
                Mapping(key="looks_like_mitten", value=Literal(True)),
            ],
        ),
    )

    relationship: Relationship = Relationship(
        name_label=NodeNameLabel(name="r", label="In")
    )

    relationship_left_right: RelationshipLeftRight = RelationshipLeftRight(
        relationship=relationship
    )

    relationship_chain: RelationshipChain = RelationshipChain(
        source_node=source_node,
        target_node=target_node,
        relationship=relationship_left_right,
    )

    relationship_chain_list: RelationshipChainList = RelationshipChainList(
        relationships=[relationship_chain],
    )

    return relationship_chain_list


@pytest.fixture
def simple_projection() -> Projection:
    return Projection(projection={"a": 1, "b": 2})


@pytest.fixture
def compatible_projection_1() -> Projection:
    return Projection(projection={"a": 1, "b": 2})


@pytest.fixture
def compatible_projection_2() -> Projection:
    return Projection(projection={"a": 1})


@pytest.fixture
def compatible_projection_3() -> Projection:
    return Projection(projection={"a": 1, "c": 10})


@pytest.fixture
def conflicting_simple_projection() -> Projection:
    return Projection(projection={"a": 2, "b": 2})


@pytest.fixture
def empty_projection() -> Projection:
    return Projection(projection={})


@pytest.fixture
def simple_projection_list() -> ProjectionList:
    projection_list: ProjectionList = ProjectionList(
        projection_list=[
            Projection(projection={"a": 1, "b": 2}),
            Projection(projection={"a": 3, "b": 4}),
        ]
    )
    return projection_list


@pytest.fixture
def simple_fact_collection() -> FactCollection:
    fact_collection: SimpleFactCollection = SimpleFactCollection(
        facts=[
            FactNodeHasAttributeWithValue(
                node_id="a", attribute="attr", value=Literal(1)
            ),
            FactNodeHasAttributeWithValue(
                node_id="b", attribute="attr", value=Literal(2)
            ),
        ]
    )
    return fact_collection


@pytest.fixture
def city_state_fact_collection() -> FactCollection:
    fact_collection: SimpleFactCollection = SimpleFactCollection(
        facts=[
            FactNodeHasLabel(node_id="michigan", label="State"),
            FactNodeHasLabel(node_id="wisconsin", label="State"),
            FactNodeHasLabel(node_id="texas", label="State"),
            FactNodeHasLabel(node_id="kalamazoo", label="City"),
            FactNodeHasLabel(node_id="detroit", label="City"),
            FactNodeHasLabel(node_id="south_haven", label="City"),
            FactNodeHasLabel(node_id="madison", label="City"),
            FactNodeHasAttributeWithValue(
                node_id="kalamazoo",
                attribute="name",
                value=Literal("KALAMAZOO"),
            ),
            FactNodeHasAttributeWithValue(
                node_id="detroit", attribute="name", value=Literal("DETROIT")
            ),
            FactNodeHasAttributeWithValue(
                node_id="south_haven",
                attribute="name",
                value=Literal("SOUTH_HAVEN"),
            ),
            FactNodeHasAttributeWithValue(
                node_id="kalamazoo",
                attribute="university_count",
                value=Literal(2),
            ),
            FactNodeHasAttributeWithValue(
                node_id="detroit",
                attribute="university_count",
                value=Literal(3),
            ),
            FactNodeHasAttributeWithValue(
                node_id="south_haven",
                attribute="university_count",
                value=Literal(0),
            ),
            FactNodeHasAttributeWithValue(
                node_id="kalamazoo",
                attribute="has_beach",
                value=Literal(False),
            ),
            FactNodeHasAttributeWithValue(
                node_id="madison",
                attribute="has_beach",
                value=Literal(True),
            ),
            FactNodeHasAttributeWithValue(
                node_id="detroit", attribute="has_beach", value=Literal(False)
            ),
            FactNodeHasAttributeWithValue(
                node_id="south_haven",
                attribute="has_beach",
                value=Literal(True),
            ),
            FactRelationshipHasLabel(relationship_id="r1", relationship_label="In"),
            FactRelationshipHasLabel(relationship_id="r2", relationship_label="In"),
            FactRelationshipHasLabel(relationship_id="r3", relationship_label="In"),
            FactRelationshipHasLabel(relationship_id="r4", relationship_label="In"),
            FactRelationshipHasSourceNode(
                relationship_id="r1", source_node_id="kalamazoo"
            ),
            FactRelationshipHasTargetNode(
                relationship_id="r1", target_node_id="michigan"
            ),
            FactRelationshipHasSourceNode(
                relationship_id="r2", source_node_id="detroit"
            ),
            FactRelationshipHasTargetNode(
                relationship_id="r2", target_node_id="michigan"
            ),
            FactRelationshipHasSourceNode(
                relationship_id="r3", source_node_id="south_haven"
            ),
            FactRelationshipHasTargetNode(
                relationship_id="r3", target_node_id="michigan"
            ),
            FactRelationshipHasSourceNode(
                relationship_id="r4", source_node_id="madison"
            ),
            FactRelationshipHasTargetNode(
                relationship_id="r4", target_node_id="wisconsin"
            ),
            FactNodeHasAttributeWithValue(
                node_id="wisconsin",
                attribute="looks_like_mitten",
                value=Literal(False),
            ),
            FactNodeHasAttributeWithValue(
                node_id="michigan",
                attribute="looks_like_mitten",
                value=Literal(True),
            ),
            FactNodeHasAttributeWithValue(
                node_id="texas",
                attribute="looks_like_mitten",
                value=Literal(False),
            ),
            FactNodeHasAttributeWithValue(
                node_id="wisconsin",
                attribute="lots_of_lakes",
                value=Literal(True),
            ),
            FactNodeHasAttributeWithValue(
                node_id="michigan",
                attribute="lots_of_lakes",
                value=Literal(True),
            ),
            FactNodeHasAttributeWithValue(
                node_id="texas",
                attribute="lots_of_lakes",
                value=Literal(False),
            ),
        ],
    )
    return fact_collection


def test_projection_init(simple_projection: Projection):
    assert simple_projection.projection == {"a": 1, "b": 2}


def test_projection_get_item(simple_projection):
    assert simple_projection["a"] == 1


def test_projection_get_item_raises_key_error(simple_projection):
    with pytest.raises(KeyError):
        simple_projection["c"]


def test_projection_get_item_raises_key_error(simple_projection):
    with pytest.raises(KeyError):
        simple_projection[1]


def test_projection_len(simple_projection):
    assert len(simple_projection) == 2


def test_conflicting_projections_1(simple_projection, compatible_projection_1):
    assert simple_projection.conflicts_with(compatible_projection_1) is False


def test_conflicting_projections_2(simple_projection, compatible_projection_2):
    assert simple_projection.conflicts_with(compatible_projection_2) is False


def test_conflicting_projections_3(simple_projection, compatible_projection_3):
    assert simple_projection.conflicts_with(compatible_projection_3) is False


def test_empty_projection(empty_projection):
    assert empty_projection.is_empty()


def test_not_empty_projection(simple_projection):
    assert not simple_projection.is_empty()


def test_add_projections(simple_projection, compatible_projection_3):
    added = simple_projection + compatible_projection_3
    assert added == Projection(projection={"a": 1, "b": 2, "c": 10})


def test_init_simple_fact_collection(simple_fact_collection):
    assert isinstance(simple_fact_collection, SimpleFactCollection)


def test_contains_simple_fact_collection(simple_fact_collection):
    assert (
        FactNodeHasAttributeWithValue(node_id="a", attribute="attr", value=Literal(1))
        in simple_fact_collection
    )


def test_not_contains_simple_fact_collection(simple_fact_collection):
    assert (
        FactNodeHasAttributeWithValue(node_id="b", attribute="attr", value=1)
        not in simple_fact_collection
    )


def test_evaluate_object_attribute_lookup(simple_fact_collection):
    object_attribute_lookup: ObjectAttributeLookup = ObjectAttributeLookup(
        object="name", attribute="attr"
    )
    assert object_attribute_lookup._evaluate(
        fact_collection=simple_fact_collection,
        projection=Projection(projection={"name": "a"}),
    ) == Literal(1)


def test_evaluate_object_attribute_lookup_not_equal(simple_fact_collection):
    object_attribute_lookup: ObjectAttributeLookup = ObjectAttributeLookup(
        object="name", attribute="attr"
    )
    assert (
        object_attribute_lookup._evaluate(
            fact_collection=simple_fact_collection,
            projection=Projection(projection={"name": "a"}),
        )
        != 2
    )


def test_evaluate_object_attribute_lookup_not_present(simple_fact_collection):
    object_attribute_lookup: ObjectAttributeLookup = ObjectAttributeLookup(
        object="name", attribute="attr"
    )
    assert isinstance(
        object_attribute_lookup._evaluate(
            fact_collection=simple_fact_collection,
            projection=Projection(projection={"name": "imnothere"}),
        ),
        NullResult,
    )


def test_evaluate_alias(simple_fact_collection) -> None:
    object_attribute_lookup: ObjectAttributeLookup = ObjectAttributeLookup(
        object="name", attribute="attr"
    )
    alias: Alias = Alias(reference=object_attribute_lookup, alias="aliased_name")
    result: Projection = alias._evaluate(
        fact_collection=simple_fact_collection,
        projection=Projection(projection={"name": "a"}),
    )
    assert isinstance(result, Projection)
    assert result["aliased_name"] == Literal(1)


def test_evaluate_attribute(city_state_fact_collection):
    out: Literal = ObjectAttributeLookup(
        object="city", attribute="has_beach"
    )._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(projection={"city": "south_haven"}),
    )
    assert out == Literal(True)


def test_evaluate_boolean_or(city_state_fact_collection):
    obj1: ObjectAttributeLookup = ObjectAttributeLookup(
        object="city", attribute="has_beach"
    )
    obj2: ObjectAttributeLookup = ObjectAttributeLookup(
        object="city", attribute="has_beach"
    )

    obj3: Or = Or(left_side=obj1, right_side=obj2)
    evaluation: Any = obj3._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(projection={"city": "south_haven"}),
    )
    assert evaluation == Literal(True)


def test_evaluate_boolean_and_1(city_state_fact_collection):
    obj1: ObjectAttributeLookup = ObjectAttributeLookup(
        object="city1", attribute="has_beach"
    )
    obj2: ObjectAttributeLookup = ObjectAttributeLookup(
        object="city2", attribute="has_beach"
    )

    obj3: And = And(left_side=obj1, right_side=obj2)
    evaluation: Any = obj3._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(
            projection={"city1": "south_haven", "city2": "kalamazoo"}
        ),
    )
    assert evaluation == Literal(False)


def test_evaluate_boolean_and_2(city_state_fact_collection):
    obj1: ObjectAttributeLookup = ObjectAttributeLookup(
        object="city1", attribute="has_beach"
    )
    obj2: ObjectAttributeLookup = ObjectAttributeLookup(
        object="city2", attribute="has_beach"
    )

    obj3: And = And(left_side=obj1, right_side=obj2)
    evaluation: Any = obj3._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(projection={"city1": "detroit", "city2": "kalamazoo"}),
    )
    assert evaluation == Literal(False)


def test_evaluate_boolean_and_3(city_state_fact_collection):
    obj1: ObjectAttributeLookup = ObjectAttributeLookup(
        object="city1", attribute="has_beach"
    )
    obj2: ObjectAttributeLookup = ObjectAttributeLookup(
        object="city2", attribute="has_beach"
    )

    obj3: And = And(left_side=obj1, right_side=obj2)
    evaluation: Any = obj3._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(
            projection={"city1": "south_haven", "city2": "south_haven"}
        ),
    )
    assert evaluation == Literal(True)


def test_evaluate_boolean_not_or(city_state_fact_collection):
    obj1: ObjectAttributeLookup = ObjectAttributeLookup(
        object="city",
        attribute="has_beach",
    )
    obj2: ObjectAttributeLookup = ObjectAttributeLookup(
        object="city",
        attribute="has_beach",
    )

    obj3: Or = Or(left_side=obj1, right_side=obj2)
    obj4: Not = Not(obj3)
    evaluation: Any = obj4._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(projection={"city": "south_haven"}),
    )
    assert evaluation == Literal(False)


def test_evaluate_boolean_not_and_1(city_state_fact_collection):
    obj1: ObjectAttributeLookup = ObjectAttributeLookup(
        object="city1",
        attribute="has_beach",
    )
    obj2: ObjectAttributeLookup = ObjectAttributeLookup(
        object="city2",
        attribute="has_beach",
    )

    obj3: And = And(left_side=obj1, right_side=obj2)
    obj4: Not = Not(obj3)
    evaluation: Any = obj4._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(
            projection={"city1": "south_haven", "city2": "kalamazoo"}
        ),
    )
    assert evaluation == Literal(True)


def test_evaluate_boolean_not_and_2(city_state_fact_collection):
    obj1: ObjectAttributeLookup = ObjectAttributeLookup(
        object="city1", attribute="has_beach"
    )
    obj2: ObjectAttributeLookup = ObjectAttributeLookup(
        object="city2", attribute="has_beach"
    )

    obj3: And = And(left_side=obj1, right_side=obj2)
    obj4: Not = Not(obj3)
    evaluation: Any = obj4._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(projection={"city1": "detroit", "city2": "kalamazoo"}),
    )
    assert evaluation == Literal(True)


def test_evaluate_boolean_not_and_3(city_state_fact_collection):
    obj1: ObjectAttributeLookup = ObjectAttributeLookup(
        object="city1",
        attribute="has_beach",
    )
    obj2: ObjectAttributeLookup = ObjectAttributeLookup(
        object="city2",
        attribute="has_beach",
    )

    obj3: And = And(left_side=obj1, right_side=obj2)
    obj4: Not = Not(obj3)
    evaluation: Any = obj4._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(
            projection={"city1": "south_haven", "city2": "south_haven"},
        ),
    )
    assert evaluation == Literal(False)


def test_node_eval_one_mapping_1(city_state_fact_collection) -> None:
    node: Node = Node(
        name_label=NodeNameLabel(name="i", label="City"),
        mapping_set=MappingSet(
            mappings=[
                Mapping(
                    key="has_beach",
                    value=Literal(True),
                ),
            ],
        ),
    )
    evaluation: Any = node._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(projection={"i": "south_haven"}),
    )
    assert evaluation == Literal(True)


def test_node_eval_one_mapping_2(city_state_fact_collection) -> None:
    node: Node = Node(
        name_label=NodeNameLabel(name="i", label="City"),
        mapping_set=MappingSet(
            mappings=[
                Mapping(
                    key="has_beach",
                    value=Literal(True),
                ),
            ],
        ),
    )
    evaluation: Any = node._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(projection={"i": "kalamazoo"}),
    )
    assert evaluation == Literal(False)


def test_eval_nonexistent_attribute_in_node_mapping(
    city_state_fact_collection,
) -> None:
    node: Node = Node(
        name_label=NodeNameLabel(name="i", label="City"),
        mapping_set=MappingSet(
            mappings=[
                Mapping(
                    key="idontexist",
                    value=Literal(True),
                ),
            ],
        ),
    )
    evaluation: Any = node._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(projection={"i": "kalamazoo"}),
    )
    assert evaluation == Literal(False)


def test_literal_boolean_eval_1():
    assert Literal(True) and Literal(True)


def test_literal_boolean_eval_2():
    assert Literal(True) or Literal(False)


def test_literal_boolean_eval_3():
    assert not (Literal(True) and Literal(False))


def test_literal_boolean_eval_4():
    assert not (Literal(False) or Literal(False))


def test_literal_boolean_eval_5():
    assert Literal(True) and True


def test_literal_boolean_eval_6():
    assert Literal(True) or False


def test_literal_boolean_eval_7():
    assert not (Literal(True) and False)


def test_literal_boolean_eval_8():
    assert not (Literal(False) or False)


def bak_test_relationship_chain_list_true(
    city_state_fact_collection, relationship_chain_list_1
):
    evaluation: bool = relationship_chain_list_1._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(
            projection={"i": "south_haven", "k": "michigan", "r": "r3"},
        ),
    )
    assert evaluation


def bak_test_relationship_chain_list_false_1(
    city_state_fact_collection, relationship_chain_list_1
):
    evaluation: bool = relationship_chain_list_1._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(
            projection={"i": "kalamazoo", "k": "michigan", "r": "r3"},
        ),
    )
    assert not evaluation


def bak_test_relationship_chain_list_false_2(
    city_state_fact_collection, relationship_chain_list_1
):
    evaluation: bool = relationship_chain_list_1._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(
            projection={"i": "south_haven", "k": "wisconsin", "r": "r3"},
        ),
    )
    assert not evaluation


def bak_test_relationship_chain_list_false_3(
    city_state_fact_collection, relationship_chain_list_1
):
    evaluation: bool = relationship_chain_list_1._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(
            projection={"i": "south_haven", "k": "michigan", "r": "r2"},
        ),
    )
    assert not evaluation


def test_non_aggregated_with_clause(city_state_fact_collection):
    with_clause: WithClause = WithClause(
        lookups=ObjectAsSeries(
            lookups=[
                Alias(
                    reference=ObjectAttributeLookup(
                        object="city",
                        attribute="has_beach",
                    ),
                    alias="has_beach",
                ),
            ],
        ),
    )
    evaluation: Any = with_clause._evaluate(
        fact_collection=city_state_fact_collection,
        projection_list=ProjectionList(
            [
                Projection(projection={"city": "south_haven"}),
            ],
        ),
    )
    assert isinstance(evaluation, ProjectionList)
    assert len(evaluation) == 1
    assert Projection(projection={"has_beach": Literal(True)}) in evaluation


def test_get_aggregated_aliases():
    with_clause: WithClause = WithClause(
        lookups=ObjectAsSeries(
            lookups=[
                Alias(
                    reference=ObjectAttributeLookup(
                        object="city",
                        attribute="has_beach",
                    ),
                    alias="has_beach_1",
                ),
                Alias(
                    reference=ObjectAttributeLookup(
                        object="city",
                        attribute="attr2",
                    ),
                    alias="has_beach_2",
                ),
                Alias(
                    reference=Collect(
                        ObjectAttributeLookup(
                            object="city",
                            attribute="attr2",
                        ),
                    ),
                    alias="has_beach_3",
                ),
            ],
        ),
    )
    aggregated_aliases: List[Alias] = with_clause.aggregated_aliases()

    assert len(aggregated_aliases) == 1
    assert aggregated_aliases[0].alias == "has_beach_3"
    assert isinstance(aggregated_aliases[0].reference, Aggregation)
    assert isinstance(aggregated_aliases[0].reference, Collect)
    assert isinstance(
        aggregated_aliases[0].reference.object_attribute_lookup,
        ObjectAttributeLookup,
    )
    assert aggregated_aliases[0].reference.object_attribute_lookup.object == "city"


def test_get_non_aggregated_aliases():
    with_clause: WithClause = WithClause(
        lookups=ObjectAsSeries(
            lookups=[
                Alias(
                    reference=ObjectAttributeLookup(
                        object="city",
                        attribute="has_beach",
                    ),
                    alias="has_beach_1",
                ),
                Alias(
                    reference=ObjectAttributeLookup(
                        object="city",
                        attribute="attr2",
                    ),
                    alias="has_beach_2",
                ),
                Alias(
                    reference=Collect(
                        ObjectAttributeLookup(
                            object="city",
                            attribute="attr2",
                        ),
                    ),
                    alias="has_beach_3",
                ),
            ],
        ),
    )
    aggregated_aliases: list[Alias] = with_clause.non_aggregated_aliases()

    assert len(aggregated_aliases) == 2
    assert isinstance(aggregated_aliases[0].reference, ObjectAttributeLookup)
    assert aggregated_aliases[0].alias == "has_beach_1"


##############################################################################################################
# WITH state.looks_like_mitten AS mitten_state, state.lots_of_lakes AS lakes, COLLECT(city.has_beach) AS sandy
##############################################################################################################
def test_aggregated_with_clause(city_state_fact_collection, with_clause):
    projection_list: ProjectionList = ProjectionList(
        [
            Projection(projection={"state": "michigan", "city": "kalamazoo"}),
            Projection(projection={"state": "michigan", "city": "south_haven"}),
            Projection(projection={"state": "michigan", "city": "detroit"}),
            Projection(projection={"state": "wisconsin", "city": "madison"}),
        ],
    )
    result: ProjectionList = with_clause._evaluate(
        fact_collection=city_state_fact_collection,
        projection_list=projection_list,
    )

    assert len(result) == 2
    assert isinstance(result, ProjectionList)
    assert isinstance(result[0], Projection)
    assert isinstance(result[1], Projection)
    assert result[0].projection == {
        "mitten_state": Literal(True),
        "lakes": Literal(True),
        "sandy": Collection([Literal(False), Literal(True), Literal(False)]),
    }
    assert result[1].projection == {
        "mitten_state": Literal(False),
        "lakes": Literal(True),
        "sandy": Collection([Literal(True)]),
    }


def test_simple_where_clause(city_state_fact_collection):
    where_clause: Where = Where(
        predicate=ObjectAttributeLookup(object="city", attribute="has_beach")
    )
    actual_output: ProjectionList = where_clause._evaluate(
        fact_collection=city_state_fact_collection,
        projection_list=ProjectionList(
            projection_list=[Projection(projection={"city": "south_haven"})]
        ),
    )
    expected: ProjectionList = ProjectionList(
        projection_list=[Projection({"city": "south_haven"})]
    )
    assert actual_output == expected


def test_return_clause_one_projection(city_state_fact_collection):
    projection: Projection = Projection(
        projection={"city": "madison", "state": "wisconsin"}
    )
    object_as_series: ObjectAsSeries = ObjectAsSeries(
        lookups=[
            Alias(
                reference=ObjectAttributeLookup(object="city", attribute="has_beach"),
                alias="beachy",
            ),
            Alias(
                reference=ObjectAttributeLookup(
                    object="state", attribute="looks_like_mitten"
                ),
                alias="handish",
            ),
        ],
    )
    return_clause: Return = Return(projection=object_as_series)
    result: ProjectionList = return_clause._evaluate(
        fact_collection=city_state_fact_collection,
        projection_list=ProjectionList(projection_list=[projection]),
    )
    assert isinstance(result, ProjectionList)
    assert result.projection_list[0] == Projection(
        projection={
            "beachy": Literal(True),
            "handish": Literal(False),
        }
    )


def test_get_free_variables_1(relationship_chain_list_1):
    projection: Projection = Projection(projection={"i": "south_haven", "r": "r3"})
    free_variables: Dict[str, List[Node]] = relationship_chain_list_1.free_variables(
        projection=projection
    )
    assert len(free_variables) == 1
    assert "k" in free_variables


def test_get_free_variables_2(relationship_chain_list_1):
    projection: Projection = Projection(
        projection={"i": "south_haven", "k": "thing", "r": "whatever"}
    )
    free_variables: Dict[str, List[Node]] = relationship_chain_list_1.free_variables(
        projection=projection
    )
    assert not free_variables


def test_get_free_variables_3(relationship_chain_list_1):
    projection: Projection = Projection(projection={"w": "thing"})
    free_variables: Dict[str, List[Node]] = relationship_chain_list_1.free_variables(
        projection=projection
    )
    assert len(free_variables) == 3
    assert "i" in free_variables
    assert "k" in free_variables
    assert "r" in free_variables


def bak_test_pattern_with_free_variables(
    city_state_fact_collection, relationship_chain_list_1
) -> None:
    projection_list: ProjectionList = ProjectionList(
        projection_list=[Projection(projection={"i": "south_haven"})]
    )
    all_substitutions: ProjectionList = get_all_substitutions(
        fact_collection=city_state_fact_collection,
        relationship_chain_list=relationship_chain_list_1,
        projection_list=projection_list,
    )
    expected: ProjectionList = ProjectionList(
        projection_list=[
            Projection(projection={"r": "r1", "k": "michigan", "i": "south_haven"}),
            Projection(projection={"r": "r1", "k": "wisconsin", "i": "south_haven"}),
            Projection(projection={"r": "r1", "k": "texas", "i": "south_haven"}),
            Projection(projection={"r": "r2", "k": "michigan", "i": "south_haven"}),
            Projection(projection={"r": "r2", "k": "wisconsin", "i": "south_haven"}),
            Projection(projection={"r": "r2", "k": "texas", "i": "south_haven"}),
            Projection(projection={"r": "r3", "k": "michigan", "i": "south_haven"}),
            Projection(projection={"r": "r3", "k": "wisconsin", "i": "south_haven"}),
            Projection(projection={"r": "r3", "k": "texas", "i": "south_haven"}),
            Projection(projection={"r": "r4", "k": "michigan", "i": "south_haven"}),
            Projection(projection={"r": "r4", "k": "wisconsin", "i": "south_haven"}),
            Projection(projection={"r": "r4", "k": "texas", "i": "south_haven"}),
        ]
    )
    assert all_substitutions == expected


def test_parse_cypher_clause_1(city_state_fact_collection):
    query = """MATCH (i:State) WITH i.looks_like_mitten AS mitten_state RETURN mitten_state AS thingy"""
    parsed: CypherParser = CypherParser(query)
    projection_list: ProjectionList = ProjectionList(
        projection_list=[Projection(projection={"i": "michigan"})]
    )

    actual: ProjectionList = parsed.parse_tree.cypher._evaluate(
        city_state_fact_collection, projection_list=projection_list
    )
    expected: ProjectionList = ProjectionList(
        projection_list=[Projection(projection={"thingy": Literal(True)})]
    )
    assert actual == expected


def test_parse_cypher_clause_2(city_state_fact_collection):
    query = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH c.has_beach AS beachy, c.name AS city_name, s.looks_like_mitten AS mitten_state """
        """RETURN beachy AS b, mitten_state AS thingy, city_name AS cname"""
    )
    parsed: CypherParser = CypherParser(query)
    projection_list: ProjectionList = ProjectionList(
        projection_list=[
            Projection(projection={"s": "michigan", "c": "kalamazoo"}),
        ],
    )

    actual: ProjectionList = parsed.parse_tree.cypher._evaluate(
        city_state_fact_collection, projection_list=projection_list
    )

    expected: ProjectionList = ProjectionList(
        projection_list=[
            Projection(
                projection={
                    "b": Literal(False),
                    "thingy": Literal(True),
                    "cname": Literal("KALAMAZOO"),
                },
            ),
        ],
    )
    assert actual == expected


def test_parse_cypher_clause_3(city_state_fact_collection):
    query = """MATCH (c:City)-[r:In]->(s:State) WITH COLLECT(c.has_beach) AS beach_list, s.looks_like_mitten AS mitten_state RETURN mitten_state AS mitteny, beach_list AS beach_things"""
    parsed: CypherParser = CypherParser(query)
    projection_list: ProjectionList = ProjectionList(
        projection_list=[
            Projection(
                projection={"s": "michigan"},
            ),
        ],
    )

    actual: ProjectionList = parsed.parse_tree.cypher._evaluate(
        city_state_fact_collection,
        projection_list=projection_list,
    )
    expected: ProjectionList = ProjectionList(
        projection_list=[
            Projection(
                projection={
                    "mitteny": Literal(True),
                    "beach_things": Collection(
                        values=[
                            Literal(False),
                            Literal(False),
                            Literal(True),
                        ],
                    ),
                },
            ),
        ],
    )
    assert actual == expected


def test_size_of_small_collection(city_state_fact_collection):
    collection: Literal = Size(
        collect=Collect(ObjectAttributeLookup(object="i", attribute="has_beach"))
    )._evaluate(
        city_state_fact_collection,
        projection_list=ProjectionList(
            projection_list=[
                Projection({"i": "kalamazoo"}),
                Projection({"i": "south_haven"}),
                Projection({"i": "madison"}),
            ],
        ),
    )
    assert collection == Literal(3)


def test_size_of_collection_with_null_result(city_state_fact_collection):
    collection: Literal = Size(
        collect=Collect(ObjectAttributeLookup(object="i", attribute="has_beach"))
    )._evaluate(
        city_state_fact_collection,
        projection_list=ProjectionList(
            projection_list=[
                Projection({"i": "kalamazoo"}),
                Projection({"i": "south_haven"}),
                Projection({"i": "madison"}),
                Projection({"i": "idontexistatall"}),
            ],
        ),
    )
    assert collection == Literal(3)


def test_size_of_empty_collection(city_state_fact_collection):
    collection: Literal = Size(
        collect=Collect(ObjectAttributeLookup(object="i", attribute="has_beach"))
    )._evaluate(
        city_state_fact_collection,
        projection_list=ProjectionList(
            projection_list=[],
        ),
    )
    assert collection == Literal(0)


def test_parse_cypher_clause_4(city_state_fact_collection):
    query = """MATCH (c:City)-[r:In]->(s:State) WITH SIZE(COLLECT(c.has_beach)) AS beach_list, s.looks_like_mitten AS mitten_state RETURN mitten_state AS mitteny, beach_list AS beach_things"""
    parsed: CypherParser = CypherParser(query)
    projection_list: ProjectionList = ProjectionList(
        projection_list=[
            Projection(
                projection={"s": "michigan"},
            ),
        ],
    )

    actual: ProjectionList = parsed.parse_tree.cypher._evaluate(
        city_state_fact_collection,
        projection_list=projection_list,
    )
    expected: ProjectionList = ProjectionList(
        projection_list=[
            Projection(
                projection={
                    "mitteny": Literal(True),
                    "beach_things": Literal(value=3),
                },
            ),
        ],
    )
    assert actual == expected


def test_sat_1(city_state_fact_collection):
    query = """MATCH (c:City)-[r:In]->(s:State) WITH SIZE(COLLECT(c.has_beach)) AS beach_list, s.looks_like_mitten AS mitten_state RETURN mitten_state AS mitteny, beach_list AS beach_things"""
    parsed: CypherParser = CypherParser(query)

    match_clause = parsed.parse_tree.cypher.match_clause
    variable_substitutions = get_variable_substitutions(
        city_state_fact_collection, match_clause.pattern
    )
    expected = {
        "c": ["kalamazoo", "detroit", "south_haven", "madison"],
        "r": ["r1", "r2", "r3", "r4"],
        "s": ["michigan", "wisconsin", "texas"],
    }
    assert variable_substitutions == expected


def test_sat_2(city_state_fact_collection):
    query = """MATCH (c:City)-[r:In]->(s:State) WITH SIZE(COLLECT(c.has_beach)) AS beach_list, s.looks_like_mitten AS mitten_state RETURN mitten_state AS mitteny, beach_list AS beach_things"""
    parsed: CypherParser = CypherParser(query)

    match_clause = parsed.parse_tree.cypher.match_clause
    variable_substitution_dict = match_clause.get_variable_substitution_dict(
        city_state_fact_collection
    )
    expected = {
        1: ("c", "kalamazoo"),
        2: ("c", "detroit"),
        3: ("c", "south_haven"),
        4: ("c", "madison"),
        5: ("r", "r1"),
        6: ("r", "r2"),
        7: ("r", "r3"),
        8: ("r", "r4"),
        9: ("s", "michigan"),
        10: ("s", "wisconsin"),
        11: ("s", "texas"),
    }
    assert variable_substitution_dict == expected


def test_sat_3(city_state_fact_collection):
    query = """MATCH (c:City)-[r:In]->(s:State) WITH SIZE(COLLECT(c.has_beach)) AS beach_list, s.looks_like_mitten AS mitten_state RETURN mitten_state AS mitteny, beach_list AS beach_things"""
    parsed: CypherParser = CypherParser(query)

    match_clause = parsed.parse_tree.cypher.match_clause
    actual = match_clause.get_instance_disjunctions(city_state_fact_collection)
    expected: list[Tuple[int, ...]] = [(1, 2, 3, 4), (5, 6, 7, 8), (9, 10, 11)]
    assert actual == expected


def test_sat_4(city_state_fact_collection):
    query = """MATCH (c:City)-[r:In]->(s:State) WITH SIZE(COLLECT(c.has_beach)) AS beach_list, s.looks_like_mitten AS mitten_state RETURN mitten_state AS mitteny, beach_list AS beach_things"""
    parsed: CypherParser = CypherParser(query)

    match_clause = parsed.parse_tree.cypher.match_clause
    actual = match_clause.get_mutual_exclusions(city_state_fact_collection)
    assert actual == [
        (-1, -2),
        (-1, -3),
        (-1, -4),
        (-2, -1),
        (-2, -3),
        (-2, -4),
        (-3, -1),
        (-3, -2),
        (-3, -4),
        (-4, -1),
        (-4, -2),
        (-4, -3),
        (-5, -6),
        (-5, -7),
        (-5, -8),
        (-6, -5),
        (-6, -7),
        (-6, -8),
        (-7, -5),
        (-7, -6),
        (-7, -8),
        (-8, -5),
        (-8, -6),
        (-8, -7),
        (-9, -10),
        (-9, -11),
        (-10, -9),
        (-10, -11),
        (-11, -9),
        (-11, -10),
    ]


def test_sat_5(city_state_fact_collection):
    query = """MATCH (c:City)-[r:In]->(s:State) WITH SIZE(COLLECT(c.has_beach)) AS beach_list, s.looks_like_mitten AS mitten_state RETURN mitten_state AS mitteny, beach_list AS beach_things"""
    parsed: CypherParser = CypherParser(query)
    match_clause = parsed.parse_tree.cypher.match_clause
    relationship_assertions = match_clause.get_relationship_assertions(
        city_state_fact_collection
    )
    assert relationship_assertions == [
        (-5, 1),
        (-5, 9),
        (-6, 2),
        (-6, 9),
        (-7, 3),
        (-7, 9),
        (-8, 4),
        (-8, 10),
    ]


def test_sat_6(city_state_fact_collection):
    query = """MATCH (c:City)-[r:In]->(s:State) WITH SIZE(COLLECT(c.has_beach)) AS beach_list, s.looks_like_mitten AS mitten_state RETURN mitten_state AS mitteny, beach_list AS beach_things"""
    parsed: CypherParser = CypherParser(query)
    match_clause = parsed.parse_tree.cypher.match_clause
    variable_substitutions = match_clause.get_variable_substitution_dict(
        city_state_fact_collection
    )

    relationship_assertions = match_clause.get_relationship_assertions(
        city_state_fact_collection
    )
    disjunctions = match_clause.get_instance_disjunctions(city_state_fact_collection)
    exclusions = match_clause.get_mutual_exclusions(city_state_fact_collection)

    all_assertions = relationship_assertions + disjunctions + exclusions
    assert all_assertions == [
        (-5, 1),
        (-5, 9),
        (-6, 2),
        (-6, 9),
        (-7, 3),
        (-7, 9),
        (-8, 4),
        (-8, 10),
        (1, 2, 3, 4),
        (5, 6, 7, 8),
        (9, 10, 11),
        (-1, -2),
        (-1, -3),
        (-1, -4),
        (-2, -1),
        (-2, -3),
        (-2, -4),
        (-3, -1),
        (-3, -2),
        (-3, -4),
        (-4, -1),
        (-4, -2),
        (-4, -3),
        (-5, -6),
        (-5, -7),
        (-5, -8),
        (-6, -5),
        (-6, -7),
        (-6, -8),
        (-7, -5),
        (-7, -6),
        (-7, -8),
        (-8, -5),
        (-8, -6),
        (-8, -7),
        (-9, -10),
        (-9, -11),
        (-10, -9),
        (-10, -11),
        (-11, -9),
        (-11, -10),
    ]


def test_sat_7(city_state_fact_collection):
    query = """MATCH (c:City)-[r:In]->(s:State) WITH SIZE(COLLECT(c.has_beach)) AS beach_list, s.looks_like_mitten AS mitten_state RETURN mitten_state AS mitteny, beach_list AS beach_things"""
    parsed: CypherParser = CypherParser(query)
    match_clause = parsed.parse_tree.cypher.match_clause
    variable_substitutions = match_clause.get_variable_substitution_dict(
        city_state_fact_collection
    )

    relationship_assertions = match_clause.get_relationship_assertions(
        city_state_fact_collection
    )
    disjunctions = match_clause.get_instance_disjunctions(city_state_fact_collection)
    exclusions = match_clause.get_mutual_exclusions(city_state_fact_collection)

    all_assertions = relationship_assertions + disjunctions + exclusions
    all_assertions = [tuple(x) for x in all_assertions]

    g = Glucose42()
    for clause in all_assertions:
        g.add_clause(clause)

    assert g.solve() == True
    assert list(g.enum_models()) == [
        [1, -2, -3, -4, 5, -6, -7, -8, 9, -10, -11],
        [-1, 2, -3, -4, -5, 6, -7, -8, 9, -10, -11],
        [-1, -2, -3, 4, -5, -6, -7, 8, -9, 10, -11],
        [-1, -2, 3, -4, -5, -6, 7, -8, 9, -10, -11],
    ]


def test_sat_8(city_state_fact_collection) -> None:
    assignment_dict: dict[int, tuple[str, str]] = {
        1: ("c", "kalamazoo"),
        2: ("c", "detroit"),
        3: ("c", "south_haven"),
        4: ("c", "madison"),
        5: ("r", "r1"),
        6: ("r", "r2"),
        7: ("r", "r3"),
        8: ("r", "r4"),
        9: ("s", "michigan"),
        10: ("s", "wisconsin"),
        11: ("s", "texas"),
    }
    found_model: List[int] = [1, -2, -3, -4, 5, -6, -7, -8, 9, -10, -11]
    output: Projection = model_to_projection(
        city_state_fact_collection, assignment_dict, found_model
    )
    expected: Projection = Projection(
        projection={"c": "kalamazoo", "r": "r1", "s": "michigan"}
    )
    assert output == expected


def test_sat_9(city_state_fact_collection) -> None:
    assignment_dict: dict[int, tuple[str, str]] = {
        1: ("c", "kalamazoo"),
        2: ("c", "detroit"),
        3: ("c", "south_haven"),
        4: ("c", "madison"),
        5: ("r", "r1"),
        6: ("r", "r2"),
        7: ("r", "r3"),
        8: ("r", "r4"),
        9: ("s", "michigan"),
        10: ("s", "wisconsin"),
        11: ("s", "texas"),
    }
    found_models: List[List[int]] = [
        [1, -2, -3, -4, 5, -6, -7, -8, 9, -10, -11],
        [-1, 2, -3, -4, -5, 6, -7, -8, 9, -10, -11],
        [-1, -2, -3, 4, -5, -6, -7, 8, -9, 10, -11],
        [-1, -2, 3, -4, -5, -6, 7, -8, 9, -10, -11],
    ]
    output: ProjectionList = models_to_projection_list(
        city_state_fact_collection, assignment_dict, found_models
    )
    expected: ProjectionList = ProjectionList(
        projection_list=[
            Projection(projection={"c": "kalamazoo", "r": "r1", "s": "michigan"}),
            Projection(projection={"c": "detroit", "r": "r2", "s": "michigan"}),
            Projection(projection={"c": "madison", "r": "r4", "s": "wisconsin"}),
            Projection(projection={"c": "south_haven", "r": "r3", "s": "michigan"}),
        ],
    )
    assert output == expected


def test_sat_10(city_state_fact_collection) -> None:
    query = """MATCH (c:City)-[r:In]->(s:State) RETURN c, r, s"""
    parsed: CypherParser = CypherParser(query)
    match_clause: Match = parsed.parse_tree.cypher.match_clause
    output: ProjectionList = match_clause._evaluate(city_state_fact_collection)
    expected: ProjectionList = ProjectionList(
        projection_list=[
            Projection(projection={"c": "kalamazoo", "r": "r1", "s": "michigan"}),
            Projection(projection={"c": "detroit", "r": "r2", "s": "michigan"}),
            Projection(projection={"c": "madison", "r": "r4", "s": "wisconsin"}),
            Projection(projection={"c": "south_haven", "r": "r3", "s": "michigan"}),
        ],
    )
    assert output == expected


def test_sat_11(city_state_fact_collection) -> None:
    query = """MATCH (c:City)-[r:In]->(s:State) WITH SIZE(COLLECT(c.has_beach)) AS beach_list, s.looks_like_mitten AS mitten_state RETURN mitten_state AS mitteny, beach_list AS beach_things"""
    parsed: CypherParser = CypherParser(query)
    match_clause = parsed.parse_tree.cypher.match_clause
    projection: Projection = Projection(projection={"s": "michigan"})
    projection_list: ProjectionList = ProjectionList(projection_list=[projection])
    output = match_clause._evaluate(
        city_state_fact_collection, projection_list=projection_list
    )
    expected: ProjectionList = ProjectionList(
        projection_list=[
            Projection(
                projection={
                    "mitten_state": Literal(True),
                    "beach_list": Literal(3),
                }
            )
        ],
    )
    assert output == expected


def test_sat_12(city_state_fact_collection) -> None:
    query = """MATCH (c:City)-[r:In]->(s:State) RETURN c, r, s"""
    parsed: CypherParser = CypherParser(query)
    match_clause: Match = parsed.parse_tree.cypher.match_clause
    projection: Projection = Projection(projection={"s": "wisconsin"})
    output: ProjectionList = match_clause._evaluate(
        city_state_fact_collection,
        projection_list=ProjectionList(
            projection_list=[Projection(projection={"s": "wisconsin"})]
        ),
    )
    expected: ProjectionList = ProjectionList(
        projection_list=[
            Projection(projection={"c": "madison", "r": "r4", "s": "wisconsin"}),
        ],
    )
    assert output == expected


def test_sat_13(city_state_fact_collection) -> None:
    query = """MATCH (c:City)-[r:In]->(s:State) RETURN c, r, s"""
    parsed: CypherParser = CypherParser(query)
    match_clause = parsed.parse_tree.cypher.match_clause
    projection: Projection = Projection(projection={"s": "idontexist"})
    projection_list: ProjectionList = ProjectionList(projection_list=[projection])
    output = match_clause._evaluate(
        city_state_fact_collection, projection_list=projection_list
    )
    expected: ProjectionList = ProjectionList(
        projection_list=[],
    )
    assert output == expected


def test_evaluate_match_clause(city_state_fact_collection):
    query = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH SIZE(COLLECT(c.has_beach)) AS beach_list, s.looks_like_mitten AS mitten_state """
        """RETURN mitten_state AS mitteny, beach_list AS beach_things"""
    )
    parsed: CypherParser = CypherParser(query)
    match_clause = parsed.parse_tree.cypher.match_clause
    output = match_clause._evaluate(city_state_fact_collection)
    expected: ProjectionList = ProjectionList(
        projection_list=[
            Projection(
                projection={
                    "mitten_state": Literal(True),
                    "beach_list": Literal(3),
                }
            ),
            Projection(
                projection={
                    "mitten_state": Literal(False),
                    "beach_list": Literal(1),
                }
            ),
        ]
    )
    assert output == expected


def test_collect_aggregated_aliases_in_with_clause_1():
    query = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH SIZE(COLLECT(c.has_beach)) AS beach_list, s.looks_like_mitten AS mitten_state """
        """RETURN mitten_state AS mitteny, beach_list AS beach_things"""
    )
    parsed: CypherParser = CypherParser(query)
    output = parsed.parse_tree.cypher.match_clause.with_clause.aggregated_aliases()
    assert len(output) == 1
    assert isinstance(output[0], Alias)
    assert output[0].alias == "beach_list"
    assert isinstance(output[0].reference, Size)
    assert isinstance(output[0].reference.collect, Collect)
    assert output[0].reference.collect.object_attribute_lookup.object == "c"
    assert output[0].reference.collect.object_attribute_lookup.attribute == "has_beach"


def test_collect_aggregated_aliases_in_with_clause_2():
    query = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH c.has_beach AS beachythingy, s.looks_like_mitten AS mitten_state """
        """RETURN mitten_state AS mitteny, beachythingy AS beach_things"""
    )
    parsed: CypherParser = CypherParser(query)
    output = parsed.parse_tree.cypher.match_clause.with_clause.aggregated_aliases()
    assert not output


def test_collect_aggregated_aliases_in_with_clause_3():
    query = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH COLLECT(c.has_beach) AS beachythingy, SIZE(COLLECT(c.has_beach)) AS otherbeachythingy, s.looks_like_mitten AS mitten_state """
        """RETURN mitten_state AS mitteny, beachythingy AS beach_things"""
    )
    parsed: CypherParser = CypherParser(query)
    output = parsed.parse_tree.cypher.match_clause.with_clause.aggregated_aliases()
    assert len(output) == 2


def test_collect_non_aggregated_aliases_in_with_clause_1():
    query = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH c.has_beach AS beachythingy, s.looks_like_mitten AS mitten_state """
        """RETURN mitten_state AS mitteny, beachythingy AS beach_things"""
    )
    parsed: CypherParser = CypherParser(query)
    output: list[Alias] = (
        parsed.parse_tree.cypher.match_clause.with_clause.non_aggregated_aliases()
    )
    assert len(output) == 2
    assert isinstance(output[0], Alias)
    assert output[0].alias == "beachythingy"


def test_get_variable_substitutions_from_relationship_chain_list(
    city_state_fact_collection,
):
    query = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH c.has_beach AS beachythingy, s.looks_like_mitten AS mitten_state """
        """RETURN mitten_state AS mitteny, beachythingy AS beach_things"""
    )
    parsed: CypherParser = CypherParser(query)
    match_clause: Match = parsed.parse_tree.cypher.match_clause
    relationship_chain_list: RelationshipChainList = match_clause.pattern
    variable_substitutions: dict[str, list[str]] = get_variable_substitutions(
        city_state_fact_collection, relationship_chain_list
    )
    assert variable_substitutions == {
        "c": ["kalamazoo", "detroit", "south_haven", "madison"],
        "r": ["r1", "r2", "r3", "r4"],
        "s": ["michigan", "wisconsin", "texas"],
    }


def test_get_variable_substitution_dict_from_relationship_chain_list(
    city_state_fact_collection,
):
    query = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH c.has_beach AS beachythingy, s.looks_like_mitten AS mitten_state """
        """RETURN mitten_state AS mitteny, beachythingy AS beach_things"""
    )
    parsed: CypherParser = CypherParser(query)
    match_clause: Match = parsed.parse_tree.cypher.match_clause
    relationship_chain_list: RelationshipChainList = match_clause.pattern
    variable_substitution_dict: dict[int, tuple[str, str]] = (
        relationship_chain_list.get_variable_substitution_dict(
            city_state_fact_collection
        )
    )
    expected: dict[int, tuple[str, str]] = {
        1: ("c", "kalamazoo"),
        2: ("c", "detroit"),
        3: ("c", "south_haven"),
        4: ("c", "madison"),
        5: ("r", "r1"),
        6: ("r", "r2"),
        7: ("r", "r3"),
        8: ("r", "r4"),
        9: ("s", "michigan"),
        10: ("s", "wisconsin"),
        11: ("s", "texas"),
    }
    assert variable_substitution_dict == expected


def test_get_instance_disjunctions_from_relationship_chain_list(
    city_state_fact_collection,
):
    query = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH c.has_beach AS beachythingy, s.looks_like_mitten AS mitten_state """
        """RETURN mitten_state AS mitteny, beachythingy AS beach_things"""
    )
    parsed: CypherParser = CypherParser(query)
    match_clause: Match = parsed.parse_tree.cypher.match_clause
    relationship_chain_list: RelationshipChainList = match_clause.pattern
    instance_disjunctions = relationship_chain_list.get_instance_disjunctions(
        city_state_fact_collection
    )
    expected: list[tuple[int, ...]] = [(1, 2, 3, 4), (5, 6, 7, 8), (9, 10, 11)]
    assert instance_disjunctions == expected


def test_get_mutual_exclusions_from_relationship_chain_list(
    city_state_fact_collection,
):
    query = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH c.has_beach AS beachythingy, s.looks_like_mitten AS mitten_state """
        """RETURN mitten_state AS mitteny, beachythingy AS beach_things"""
    )
    parsed: CypherParser = CypherParser(query)
    match_clause: Match = parsed.parse_tree.cypher.match_clause
    relationship_chain_list: RelationshipChainList = match_clause.pattern
    mutual_exclusions: list[tuple[int, int]] = (
        relationship_chain_list.get_mutual_exclusions(city_state_fact_collection)
    )
    expected_sorted: list[tuple[int, int]] = [
        (-11, -10),
        (-11, -9),
        (-10, -11),
        (-10, -9),
        (-9, -11),
        (-9, -10),
        (-8, -7),
        (-8, -6),
        (-8, -5),
        (-7, -8),
        (-7, -6),
        (-7, -5),
        (-6, -8),
        (-6, -7),
        (-6, -5),
        (-5, -8),
        (-5, -7),
        (-5, -6),
        (-4, -3),
        (-4, -2),
        (-4, -1),
        (-3, -4),
        (-3, -2),
        (-3, -1),
        (-2, -4),
        (-2, -3),
        (-2, -1),
        (-1, -4),
        (-1, -3),
        (-1, -2),
    ]
    assert sorted(mutual_exclusions) == expected_sorted


def test_get_relationship_assertions_from_relationship_chain_list(
    city_state_fact_collection,
) -> None:
    query = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH c.has_beach AS beachythingy, s.looks_like_mitten AS mitten_state """
        """RETURN mitten_state AS mitteny, beachythingy AS beach_things"""
    )
    parsed: CypherParser = CypherParser(query)
    match_clause: Match = parsed.parse_tree.cypher.match_clause
    relationship_chain_list: RelationshipChainList = match_clause.pattern
    relationship_assertions: list[tuple[int, int]] = (
        relationship_chain_list.get_relationship_assertions(city_state_fact_collection)
    )
    sorted_expected: list[tuple[int, int]] = sorted(
        [
            (-5, 1),
            (-5, 9),
            (-6, 2),
            (-6, 9),
            (-7, 3),
            (-7, 9),
            (-8, 4),
            (-8, 10),
        ]
    )
    assert sorted(relationship_assertions) == sorted_expected


def test_evalate_relationship_chain_list_no_assumptions(
    city_state_fact_collection,
) -> None:
    query = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH c.has_beach AS beachythingy, s.looks_like_mitten AS mitten_state """
        """RETURN mitten_state AS mitteny, beachythingy AS beach_things"""
    )
    parsed: CypherParser = CypherParser(query)
    match_clause: Match = parsed.parse_tree.cypher.match_clause
    relationship_chain_list: RelationshipChainList = match_clause.pattern
    out: ProjectionList = relationship_chain_list._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(projection={}),
    )
    expected: ProjectionList = ProjectionList(
        projection_list=[
            Projection(projection={"c": "kalamazoo", "r": "r1", "s": "michigan"}),
            Projection(projection={"c": "detroit", "r": "r2", "s": "michigan"}),
            Projection(projection={"c": "madison", "r": "r4", "s": "wisconsin"}),
            Projection(projection={"c": "south_haven", "r": "r3", "s": "michigan"}),
        ]
    )
    assert out == expected


def test_evalate_relationship_chain_list_assumption_1(
    city_state_fact_collection,
) -> None:
    query = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH c.has_beach AS beachythingy, s.looks_like_mitten AS mitten_state """
        """RETURN mitten_state AS mitteny, beachythingy AS beach_things"""
    )
    parsed: CypherParser = CypherParser(query)
    match_clause: Match = parsed.parse_tree.cypher.match_clause
    relationship_chain_list: RelationshipChainList = match_clause.pattern
    out: ProjectionList = relationship_chain_list._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(projection={"c": "madison"}),
    )
    expected: ProjectionList = ProjectionList(
        projection_list=[
            Projection(projection={"c": "madison", "r": "r4", "s": "wisconsin"}),
        ]
    )
    assert out == expected


def test_evalate_relationship_chain_list_assumption_2(
    city_state_fact_collection,
) -> None:
    query = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH c.has_beach AS beachythingy, s.looks_like_mitten AS mitten_state """
        """RETURN mitten_state AS mitteny, beachythingy AS beach_things"""
    )
    parsed: CypherParser = CypherParser(query)
    match_clause: Match = parsed.parse_tree.cypher.match_clause
    relationship_chain_list: RelationshipChainList = match_clause.pattern
    out: ProjectionList = relationship_chain_list._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(projection={"s": "michigan"}),
    )
    expected: ProjectionList = ProjectionList(
        projection_list=[
            Projection(projection={"c": "kalamazoo", "r": "r1", "s": "michigan"}),
            Projection(projection={"c": "detroit", "r": "r2", "s": "michigan"}),
            Projection(projection={"c": "south_haven", "r": "r3", "s": "michigan"}),
        ]
    )
    assert out == expected


def test_evalate_relationship_chain_list_assumption_3(
    city_state_fact_collection,
) -> None:
    query = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH c.has_beach AS beachythingy, s.looks_like_mitten AS mitten_state """
        """RETURN mitten_state AS mitteny, beachythingy AS beach_things"""
    )
    parsed: CypherParser = CypherParser(query)
    match_clause: Match = parsed.parse_tree.cypher.match_clause
    relationship_chain_list: RelationshipChainList = match_clause.pattern
    out: ProjectionList = relationship_chain_list._evaluate(
        fact_collection=city_state_fact_collection,
        projection=Projection(projection={"r": "r2"}),
    )
    expected: ProjectionList = ProjectionList(
        projection_list=[
            Projection(projection={"c": "detroit", "r": "r2", "s": "michigan"}),
        ]
    )
    assert out == expected


def test_identify_aggregated_aliases_in_with_clause() -> None:
    query = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH COLLECT(c.has_beach) AS beachstats, s.looks_like_mitten AS mitten_state """
        """RETURN mitten_state AS mitteny, beachystats AS beach_thingy_list"""
    )
    parsed: CypherParser = CypherParser(query)
    match_clause: Match = parsed.parse_tree.cypher.match_clause
    with_clause: WithClause = match_clause.with_clause
    out: list[Alias] = with_clause.aggregated_aliases()
    expected: list[Alias] = [
        Alias(
            reference=Collect(
                object_attribute_lookup=ObjectAttributeLookup(
                    object="c", attribute="has_beach"
                )
            ),
            alias="beachstats",
        )
    ]
    assert out == expected


def test_identify_non_aggregated_aliases_in_with_clause() -> None:
    query = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH COLLECT(c.has_beach) AS beachstats, s.looks_like_mitten AS mitten_state """
        """RETURN mitten_state AS mitteny, beachystats AS beach_thingy_list"""
    )
    parsed: CypherParser = CypherParser(query)
    match_clause: Match = parsed.parse_tree.cypher.match_clause
    with_clause: WithClause = match_clause.with_clause
    out: list[Alias] = with_clause.non_aggregated_aliases()
    expected: list[Alias] = [
        Alias(
            reference=ObjectAttributeLookup(object="s", attribute="looks_like_mitten"),
            alias="mitten_state",
        )
    ]
    assert out == expected


def test_disaggregation_in_with_clause_collect() -> None:
    aggregated_alias: Alias = Alias(
        reference=Collect(
            object_attribute_lookup=ObjectAttributeLookup(
                object="c", attribute="has_beach"
            )
        ),
        alias="beachstats",
    )

    expected: Alias = Alias(
        reference=ObjectAttributeLookup(object="c", attribute="has_beach"),
        alias="beachstats",
    )
    assert aggregated_alias.disaggregate() == expected


def test_end_to_end_query_with_aggregation(
    city_state_fact_collection,
) -> None:
    query: str = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH COLLECT(c.has_beach) AS beachstats, s.looks_like_mitten AS mitten_state """
        """RETURN mitten_state AS mitteny, beachstats AS beach_thingy_list"""
    )
    parsed: CypherParser = CypherParser(query)
    ast: Cypher = parsed.parse_tree
    evaluation: ProjectionList = ast._evaluate(
        fact_collection=city_state_fact_collection
    )
    expected: ProjectionList = ProjectionList(
        projection_list=[
            Projection(
                projection={
                    "mitteny": Literal(value=True),
                    "beach_thingy_list": Collection(
                        values=[
                            Literal(value=False),
                            Literal(value=False),
                            Literal(value=True),
                        ],
                    ),
                },
            ),
            Projection(
                projection={
                    "mitteny": Literal(value=False),
                    "beach_thingy_list": Collection(
                        values=[
                            Literal(value=True),
                        ],
                    ),
                },
            ),
        ],
    )
    assert evaluation == expected


def test_addition_0(city_state_fact_collection) -> None:
    expr: Addition = Addition(left_side=Literal(2), right_side=Literal(2))
    actual: Literal = expr._evaluate(
        fact_collection=city_state_fact_collection, projection=Projection(projection={})
    )
    expected: Literal = Literal(4)
    assert actual == expected


def test_addition_1(city_state_fact_collection) -> None:
    expr: Addition = Addition(
        left_side=Addition(left_side=Literal(2), right_side=Literal(2)),
        right_side=Literal(10),
    )
    actual: Literal = expr._evaluate(
        fact_collection=city_state_fact_collection, projection=Projection(projection={})
    )
    expected: Literal = Literal(14)
    assert actual == expected


def test_multiplication_0(city_state_fact_collection) -> None:
    expr: Addition = Multiplication(left_side=Literal(3), right_side=Literal(2))
    actual: Literal = expr._evaluate(
        fact_collection=city_state_fact_collection, projection=Projection(projection={})
    )
    expected: Literal = Literal(6)
    assert actual == expected


def test_multiplication_1(city_state_fact_collection) -> None:
    expr: Multiplication = Multiplication(
        left_side=Addition(left_side=Literal(2), right_side=Literal(2)),
        right_side=Literal(10),
    )
    actual: Literal = expr._evaluate(
        fact_collection=city_state_fact_collection, projection=Projection(projection={})
    )
    expected: Literal = Literal(40)
    assert actual == expected


def test_multiplication_2(city_state_fact_collection) -> None:
    expr: Multiplication = Multiplication(
        left_side=Addition(
            left_side=Addition(left_side=Literal(1), right_side=Literal(1)),
            right_side=Literal(2),
        ),
        right_side=Literal(10),
    )
    actual: Literal = expr._evaluate(
        fact_collection=city_state_fact_collection, projection=Projection(projection={})
    )
    expected: Literal = Literal(40)
    assert actual == expected


def test_equals_true(city_state_fact_collection) -> None:
    expr: Equals = Equals(left_side=Literal(2), right_side=Literal(2))
    actual: Literal = expr._evaluate(
        projection=Projection(projection={}), fact_collection=city_state_fact_collection
    )
    assert actual


def test_equals_false(city_state_fact_collection) -> None:
    expr: Equals = Equals(left_side=Literal(3), right_side=Literal(2))
    actual: Literal = expr._evaluate(
        projection=Projection(projection={}), fact_collection=city_state_fact_collection
    )
    assert not actual


def test_true_is_true() -> None:
    assert TRUE


def test_false_is_false() -> None:
    assert not FALSE


def test_true_and_true(city_state_fact_collection) -> None:
    assert And(left_side=TRUE, right_side=TRUE)._evaluate(
        fact_collection=city_state_fact_collection, projection=Projection(projection={})
    )


def test_true_and_false(city_state_fact_collection) -> None:
    assert not And(left_side=TRUE, right_side=FALSE)._evaluate(
        fact_collection=city_state_fact_collection, projection=Projection(projection={})
    )


def test_false_and_true(city_state_fact_collection) -> None:
    assert not And(left_side=FALSE, right_side=TRUE)._evaluate(
        fact_collection=city_state_fact_collection, projection=Projection(projection={})
    )


def test_false_and_false(city_state_fact_collection) -> None:
    assert not And(left_side=FALSE, right_side=FALSE)._evaluate(
        fact_collection=city_state_fact_collection, projection=Projection(projection={})
    )


def test_true_or_true(city_state_fact_collection) -> None:
    assert Or(left_side=TRUE, right_side=TRUE)._evaluate(
        fact_collection=city_state_fact_collection, projection=Projection(projection={})
    )


def test_true_or_false(city_state_fact_collection) -> None:
    assert Or(left_side=TRUE, right_side=FALSE)._evaluate(
        fact_collection=city_state_fact_collection, projection=Projection(projection={})
    )


def test_false_or_true(city_state_fact_collection) -> None:
    assert Or(left_side=FALSE, right_side=TRUE)._evaluate(
        fact_collection=city_state_fact_collection, projection=Projection(projection={})
    )


def test_false_or_false(city_state_fact_collection) -> None:
    assert not Or(left_side=FALSE, right_side=FALSE)._evaluate(
        fact_collection=city_state_fact_collection, projection=Projection(projection={})
    )


def test_complex_where_clause_with_result(city_state_fact_collection):
    query: str = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH SIZE(COLLECT(c.has_beach)) AS numbeach, s.looks_like_mitten AS mitten_state """
        """WHERE numbeach = 3 """
        """RETURN mitten_state AS mitteny, numbeach AS beach_thingy_list"""
    )
    parsed: CypherParser = CypherParser(query)
    ast: Cypher = parsed.parse_tree

    evaluation: ProjectionList = ast._evaluate(
        fact_collection=city_state_fact_collection
    )
    expected: ProjectionList = ProjectionList(
        projection_list=[
            Projection(
                projection={"mitteny": Literal(True), "beach_thingy_list": Literal(3)}
            )
        ]
    )
    assert evaluation == expected


def test_complex_where_clause_with_no_result(city_state_fact_collection):
    query: str = (
        """MATCH (c:City)-[r:In]->(s:State) """
        """WITH SIZE(COLLECT(c.has_beach)) AS numbeach, s.looks_like_mitten AS mitten_state """
        """WHERE numbeach = 100 """
        """RETURN mitten_state AS mitteny, numbeach AS beach_thingy_list"""
    )
    parsed: CypherParser = CypherParser(query)
    ast: Cypher = parsed.parse_tree

    evaluation: ProjectionList = ast._evaluate(
        fact_collection=city_state_fact_collection
    )
    expected: ProjectionList = ProjectionList(projection_list=[])
    assert evaluation == expected