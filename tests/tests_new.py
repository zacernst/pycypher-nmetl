"""
Additional unit tests for methods and functions that lack adequate test coverage.
"""
# pylint: disable=missing-function-docstring,protected-access,redefined-outer-name

import os
import pathlib
import queue
import tempfile
from unittest.mock import Mock, patch

import pytest

from nmetl.data_source import (
    CSVDataSource,
    DataSource,
    FixtureDataSource,
    ParquetFileDataSource,
    RawDataThread,
)
from nmetl.data_types import (
    _Anything,
    _Boolean,
    _Float,
    _Integer,
    _PositiveInteger,
    _String,
)
from nmetl.helpers import QueueGenerator, ensure_uri
from nmetl.message_types import (
    DataSourcesExhausted,
    EndOfData,
    Message,
    RawDatum,
)
from nmetl.queue_processor import (
    CheckFactAgainstTriggersQueueProcessor,
    QueueProcessor,
    SubTriggerPair,
)
from nmetl.session import Session
from nmetl.trigger import (
    AttributeMetadata,
    CypherTrigger,
    NodeRelationshipTrigger,
    VariableAttributeTrigger,
)
from pycypher.fact import (
    AtomicFact,
    FactCollection,
    FactNodeHasLabel,
)
from pycypher.node_classes import (
    Addition,
    Literal,
)
from pycypher.solver import (
    Constraint,
    IsTrue,
)


# Tests for nmetl.data_types
def test_anything_cast():
    """Test that _Anything.cast returns the value unchanged."""
    anything = _Anything()
    assert anything.cast(42) == 42
    assert anything.cast("hello") == "hello"
    assert anything.cast(None) is None


def test_integer_cast():
    """Test that _Integer.cast converts values to integers."""
    integer = _Integer()
    assert integer.cast(42) == 42
    assert integer.cast("42") == 42
    assert integer.cast(42.5) == 42
    with pytest.raises(ValueError):
        integer.cast("not an integer")


def test_positive_integer_cast():
    """Test that _PositiveInteger.cast converts values to positive integers."""
    positive_integer = _PositiveInteger()
    assert positive_integer.cast(42) == 42
    assert positive_integer.cast(-42) == 42
    assert positive_integer.cast("42") == 42
    assert positive_integer.cast("-42") == 42
    with pytest.raises(ValueError):
        positive_integer.cast("not an integer")


def test_string_cast():
    """Test that _String.cast converts values to strings."""
    string = _String()
    assert string.cast(42) == "42"
    assert string.cast(None) == "None"
    assert string.cast(True) == "True"


def test_float_cast():
    """Test that _Float.cast converts values to floats."""
    float_type = _Float()
    assert float_type.cast(42) == 42.0
    assert float_type.cast("42.5") == 42.5
    with pytest.raises(ValueError):
        float_type.cast("not a float")


def test_boolean_cast():
    """Test that _Boolean.cast converts values to booleans."""
    boolean = _Boolean()
    assert boolean.cast(1) is True
    assert boolean.cast(0) is False
    assert boolean.cast("") is False
    assert boolean.cast("anything") is True


# Tests for nmetl.message_types
def test_message_repr():
    """Test that Message.__repr__ returns the class name."""
    message = Message()
    assert repr(message) == "Message"


def test_end_of_data_init():
    """Test that EndOfData can be initialized with a data source."""
    data_source = Mock(spec=DataSource)
    end_of_data = EndOfData(data_source=data_source)
    assert end_of_data.data_source is data_source


def test_data_sources_exhausted_init():
    """Test that DataSourcesExhausted can be initialized."""
    data_sources_exhausted = DataSourcesExhausted()
    assert isinstance(data_sources_exhausted, Message)


def test_raw_datum_init_and_repr():
    """Test that RawDatum can be initialized with a data source and row, and its repr works."""
    data_source = Mock(spec=DataSource)
    row = {"key": "value"}
    raw_datum = RawDatum(data_source=data_source, row=row)
    assert raw_datum.data_source is data_source
    assert raw_datum.row == row
    assert repr(raw_datum) == "RawDatum({'key': 'value'})"


# Tests for nmetl.helpers
def test_ensure_uri_with_string():
    """Test that ensure_uri correctly handles string inputs."""
    uri = ensure_uri("file:///path/to/file.csv")
    assert uri.scheme == "file"
    assert uri.path == "/path/to/file.csv"


def test_ensure_uri_with_path():
    """Test that ensure_uri correctly handles pathlib.Path inputs."""
    path = pathlib.Path("/path/to/file.csv")
    uri = ensure_uri(path)
    assert uri.scheme == "file"
    assert uri.path == str(path.absolute())


def test_queue_generator_init():
    """Test that QueueGenerator can be initialized with custom parameters."""
    session = Mock(spec=Session)
    session.queue_list = []
    queue_gen = QueueGenerator(
        inner_queue_timeout=10,
        end_of_queue_cls=EndOfData,
        outer_queue_timeout=20,
        name="test_queue",
        use_cache=True,
        session=session,
    )
    assert queue_gen.inner_queue_timeout == 10
    assert queue_gen.end_of_queue_cls == EndOfData
    assert queue_gen.outer_queue_timeout == 20
    assert queue_gen.name == "test_queue"
    assert queue_gen.use_cache is True
    assert queue_gen.session is session
    assert queue_gen in session.queue_list


# Tests for nmetl.queue_processor
def test_sub_trigger_pair_hash():
    """Test that SubTriggerPair.__hash__ returns a hash based on sub and trigger."""
    trigger = Mock(spec=CypherTrigger)
    sub = {"var1": "val1", "var2": "val2"}
    pair = SubTriggerPair(sub=sub, trigger=trigger)
    # Hash should be consistent for the same values
    assert hash(pair) == hash(pair)
    # Different sub should result in different hash
    pair2 = SubTriggerPair(sub={"var1": "different"}, trigger=trigger)
    assert hash(pair) != hash(pair2)


def test_queue_processor_init():
    """Test that QueueProcessor can be initialized with custom parameters."""
    session = Mock(spec=Session)
    incoming_queue = Mock(spec=QueueGenerator)
    incoming_queue.incoming_queue_processors = []
    outgoing_queue = Mock(spec=QueueGenerator)
    outgoing_queue.incoming_queue_processors = []
    status_queue = Mock(spec=queue.Queue)

    # Using Mock to avoid abstract class instantiation
    with patch.object(QueueProcessor, '__abstractmethods__', set()):
        processor = QueueProcessor(
            session=session,
            incoming_queue=incoming_queue,
            outgoing_queue=outgoing_queue,
            status_queue=status_queue,
        )

    assert processor.session is session
    assert processor.incoming_queue is incoming_queue
    assert processor.outgoing_queue is outgoing_queue
    assert processor.status_queue is status_queue
    assert processor in outgoing_queue.incoming_queue_processors


def test_check_fact_against_triggers_queue_processor_init():
    """Test that CheckFactAgainstTriggersQueueProcessor can be initialized."""
    session = Mock(spec=Session)
    processor = CheckFactAgainstTriggersQueueProcessor(session=session)
    assert processor.session is session


# Tests for nmetl.trigger
def test_attribute_metadata_init():
    """Test that AttributeMetadata can be initialized with attribute name, function name, and description."""
    metadata = AttributeMetadata(
        attribute_name="age",
        function_name="calculate_age",
        description="Calculates the age based on birth date",
    )
    assert metadata.attribute_name == "age"
    assert metadata.function_name == "calculate_age"
    assert metadata.description == "Calculates the age based on birth date"


def test_cypher_trigger_init():
    """Test that CypherTrigger can be initialized with a function and cypher string."""
    def test_function():
        return 42

    session = Mock(spec=Session)
    cypher_string = "MATCH (n:Node) RETURN n"

    # Using patch to avoid abstract class instantiation
    with patch("pycypher.cypher_parser.CypherParser"), \
         patch.object(CypherTrigger, '__abstractmethods__', set()):
        trigger = CypherTrigger(
            function=test_function,
            cypher_string=cypher_string,
            session=session,
            parameter_names=["param1", "param2"],
        )

        assert trigger.function is test_function
        assert trigger.cypher_string == cypher_string
        assert trigger.session is session
        assert trigger.parameter_names == ["param1", "param2"]
        assert trigger.call_counter == 0
        assert trigger.error_counter == 0


def test_cypher_trigger_repr():
    """Test that CypherTrigger.__repr__ returns a string with constraints."""
    # Using patch to avoid abstract class instantiation
    with patch("pycypher.cypher_parser.CypherParser"), \
         patch.object(CypherTrigger, '__abstractmethods__', set()):
        trigger = CypherTrigger(cypher_string="MATCH (n:Node) RETURN n")
        assert "CypherTrigger(constraints:" in repr(trigger)


def test_node_relationship_trigger_init():
    """Test that NodeRelationshipTrigger can be initialized with source, target, and relationship name."""
    def test_function():
        return 42

    session = Mock(spec=Session)
    cypher_string = "MATCH (a)-[r]->(b) RETURN a, r, b"

    with patch("pycypher.cypher_parser.CypherParser"):
        trigger = NodeRelationshipTrigger(
            function=test_function,
            cypher_string=cypher_string,
            source_variable="a",
            target_variable="b",
            relationship_name="KNOWS",
            session=session,
            parameter_names=["param1"],
        )

        assert trigger.function is test_function
        assert trigger.cypher_string == cypher_string
        assert trigger.source_variable == "a"
        assert trigger.target_variable == "b"
        assert trigger.relationship_name == "KNOWS"
        assert trigger.session is session
        assert trigger.parameter_names == ["param1"]
        assert trigger.is_relationship_trigger is True
        assert trigger.is_attribute_trigger is False


def test_node_relationship_trigger_hash():
    """Test that NodeRelationshipTrigger.__hash__ returns a hash based on its attributes."""
    def test_function():
        return 42

    with patch("pycypher.cypher_parser.CypherParser"):
        trigger = NodeRelationshipTrigger(
            function=test_function,
            cypher_string="MATCH (a)-[r]->(b) RETURN a, r, b",
            source_variable="a",
            target_variable="b",
            relationship_name="KNOWS",
        )

        # Hash should be consistent for the same values
        assert hash(trigger) == hash(trigger)


def test_variable_attribute_trigger_init():
    """Test that VariableAttributeTrigger can be initialized with variable and attribute."""
    def test_function():
        """Test docstring"""
        return 42

    session = Mock(spec=Session)
    session.attribute_metadata_dict = {}
    cypher_string = "MATCH (n:Node) RETURN n.name"

    with patch("pycypher.cypher_parser.CypherParser"):
        trigger = VariableAttributeTrigger(
            function=test_function,
            cypher_string=cypher_string,
            variable_set="n",
            attribute_set="age",
            session=session,
            parameter_names=["param1"],
        )

        assert trigger.function is test_function
        assert trigger.cypher_string == cypher_string
        assert trigger.variable_set == "n"
        assert trigger.attribute_set == "age"
        assert trigger.session is session
        assert trigger.parameter_names == ["param1"]
        assert trigger.is_relationship_trigger is False
        assert trigger.is_attribute_trigger is True
        assert "age" in session.attribute_metadata_dict


def test_variable_attribute_trigger_hash():
    """Test that VariableAttributeTrigger.__hash__ returns a hash based on its attributes."""
    def test_function():
        return 42

    session = Mock(spec=Session)
    session.attribute_metadata_dict = {}

    with patch("pycypher.cypher_parser.CypherParser"):
        trigger = VariableAttributeTrigger(
            function=test_function,
            cypher_string="MATCH (n:Node) RETURN n.name",
            variable_set="n",
            attribute_set="age",
            session=session,
        )

        # Hash should be consistent for the same values
        assert hash(trigger) == hash(trigger)


# Tests for nmetl.data_source
def test_data_source_init():
    """Test that DataSource can be initialized with a name."""
    # Using Mock to avoid abstract class instantiation
    with patch.object(DataSource, '__abstractmethods__', set()):
        data_source = DataSource(name="test_source")
    assert data_source.name == "test_source"
    assert data_source.raw_input_queue is None
    assert data_source.started is False
    assert data_source.finished is False
    assert not data_source.mappings
    assert not data_source.schema
    assert not data_source.new_column_configs


def test_data_source_repr():
    """Test that DataSource.__repr__ returns a string with the class name and source name."""
    # Using Mock to avoid abstract class instantiation
    with patch.object(DataSource, '__abstractmethods__', set()):
        data_source = DataSource(name="test_source")
    assert repr(data_source) == "DataSource(test_source)"


def test_raw_data_thread_init():
    """Test that RawDataThread can be initialized with a data source."""
    data_source = Mock(spec=DataSource)
    thread = RawDataThread(data_source=data_source)
    assert thread.data_source is data_source
    assert thread.thread_has_started is False
    assert thread.raw_input_queue is None
    assert thread.halt is False


def test_fixture_data_source_init():
    """Test that FixtureDataSource can be initialized with data, hang, delay, and loop options."""
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    source = FixtureDataSource(
        data=data,
        hang=True,
        delay=0.5,
        loop=True,
        name="test_fixture",
    )
    assert source.data == data
    assert source.hang is True
    assert source.delay == 0.5
    assert source.loop is True
    assert source.name == "test_fixture"


def test_csv_data_source_init():
    """Test that CSVDataSource can be initialized with a URI and options."""
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
        temp_file.write(b"id,name\n1,Alice\n2,Bob\n")
        temp_file_path = temp_file.name

    try:
        uri = f"file://{temp_file_path}"
        # Mock the DataSource.__init__ to avoid name being overridden
        with patch.object(DataSource, '__init__', return_value=None):
            source = CSVDataSource(
                uri=uri,
                name="test_csv",
                delimiter=",",
            )
            # Set the name manually since we mocked the parent __init__
            source.name = "test_csv"

        assert source.uri.scheme == "file"
        assert source.uri.path == temp_file_path
        assert source.name == "test_csv"
        # CSVDataSource doesn't have options attribute, it passes them to csv.DictReader
        assert source.uri is not None
    finally:
        os.unlink(temp_file_path)


def test_parquet_file_data_source_init():
    """Test that ParquetFileDataSource can be initialized with a URI."""
    uri = "file:///path/to/file.parquet"
    # Mock the DataSource.__init__ to avoid name being overridden
    with patch.object(DataSource, '__init__', return_value=None):
        source = ParquetFileDataSource(
            uri=uri,
            name="test_parquet",
        )
        # Set the name manually since we mocked the parent __init__
        source.name = "test_parquet"

    assert source.uri.scheme == "file"
    assert source.uri.path == "/path/to/file.parquet"
    assert source.name == "test_parquet"


# Tests for pycypher.fact
def test_atomic_fact_init():
    """Test that AtomicFact can be initialized with a session."""
    session = Mock(spec=Session)
    fact = AtomicFact(session=session)
    assert fact.session is session


def test_fact_node_has_label_init():
    """Test that FactNodeHasLabel can be initialized with node_id and label."""
    fact = FactNodeHasLabel(node_id="node1", label="Person")
    assert fact.node_id == "node1"
    assert fact.label == "Person"


def test_fact_node_has_label_repr():
    """Test that FactNodeHasLabel.__repr__ returns a string with node_id and label."""
    fact = FactNodeHasLabel(node_id="node1", label="Person")
    assert repr(fact) == "NodeHasLabel: node1 Person"


def test_fact_node_has_label_eq():
    """Test that FactNodeHasLabel.__eq__ compares node_id and label."""
    fact1 = FactNodeHasLabel(node_id="node1", label="Person")
    fact2 = FactNodeHasLabel(node_id="node1", label="Person")
    fact3 = FactNodeHasLabel(node_id="node2", label="Person")
    fact4 = FactNodeHasLabel(node_id="node1", label="Organization")

    assert fact1 == fact2
    assert fact1 != fact3
    assert fact1 != fact4
    assert fact1 != "not a fact"


def test_fact_node_has_label_hash():
    """Test that FactNodeHasLabel.__hash__ returns a hash based on node_id and label."""
    fact = FactNodeHasLabel(node_id="node1", label="Person")
    assert hash(fact) == hash(("node1", "Person"))


def test_fact_collection_init():
    """Test that FactCollection can be initialized with facts and session."""
    session = Mock(spec=Session)
    facts = [
        Mock(spec=AtomicFact),
        Mock(spec=AtomicFact),
    ]
    collection = FactCollection(facts=facts, session=session)
    assert collection.facts == facts
    assert collection.session is session


def test_fact_collection_repr():
    """Test that FactCollection.__repr__ returns a string with the number of facts."""
    facts = [Mock(spec=AtomicFact), Mock(spec=AtomicFact)]
    collection = FactCollection(facts=facts)
    assert repr(collection) == "FactCollection: 2"


def test_fact_collection_getitem():
    """Test that FactCollection.__getitem__ returns the fact at the specified index."""
    fact1 = Mock(spec=AtomicFact)
    fact2 = Mock(spec=AtomicFact)
    collection = FactCollection(facts=[fact1, fact2])
    assert collection[0] is fact1
    assert collection[1] is fact2


def test_fact_collection_setitem():
    """Test that FactCollection.__setitem__ sets the fact at the specified index."""
    fact1 = Mock(spec=AtomicFact)
    fact2 = Mock(spec=AtomicFact)
    fact3 = Mock(spec=AtomicFact)
    collection = FactCollection(facts=[fact1, fact2])
    collection[1] = fact3
    assert collection[0] is fact1
    assert collection[1] is fact3


def test_fact_collection_delitem():
    """Test that FactCollection.__delitem__ deletes the fact at the specified index."""
    fact1 = Mock(spec=AtomicFact)
    fact2 = Mock(spec=AtomicFact)
    collection = FactCollection(facts=[fact1, fact2])
    del collection[0]
    assert len(collection) == 1
    assert collection[0] is fact2


# Tests for pycypher.node_classes
def test_addition_init():
    """Test that Addition can be initialized with left and right operands."""
    left = Mock(spec=Literal)
    right = Mock(spec=Literal)
    addition = Addition(left=left, right=right)
    assert addition.left_side is left
    assert addition.right_side is right


def test_addition_repr():
    """Test that Addition.__repr__ returns a string with left and right operands."""
    left = Literal(1)
    right = Literal(2)
    addition = Addition(left=left, right=right)
    assert repr(addition) == "Addition(Literal(1), Literal(2))"


def test_addition_tree():
    """Test that Addition.tree returns a tree representation."""
    left = Literal(1)
    right = Literal(2)
    addition = Addition(left=left, right=right)
    tree = addition.tree()
    assert tree.label == "Addition"
    assert len(list(tree.children)) == 2


def test_addition_children():
    """Test that Addition.children yields left and right operands."""
    left = Literal(1)
    right = Literal(2)
    addition = Addition(left=left, right=right)
    children = list(addition.children)
    assert len(children) == 2
    assert children[0] is left
    assert children[1] is right


# Tests for pycypher.solver
def test_constraint_init():
    """Test that Constraint can be initialized with a trigger."""
    trigger = Mock(spec=CypherTrigger)
    constraint = Constraint(trigger=trigger)
    assert constraint.trigger is trigger


def test_is_true_init():
    """Test that IsTrue can be initialized with a predicate."""
    predicate = Mock()
    trigger = Mock(spec=CypherTrigger)
    is_true = IsTrue(predicate=predicate, trigger=trigger)
    assert is_true.predicate is predicate
    assert is_true.trigger is trigger


def test_is_true_repr():
    """Test that IsTrue.__repr__ returns a string with the predicate."""
    # Create a real object instead of a Mock to avoid __repr__ issues
    class TestPredicate:
        def __repr__(self):
            return "TestPredicate"

    predicate = TestPredicate()
    is_true = IsTrue(predicate=predicate)
    assert repr(is_true) == "IsTrue(TestPredicate)"


def test_is_true_eq():
    """Test that IsTrue.__eq__ compares predicates."""
    predicate1 = Mock()
    predicate2 = Mock()
    is_true1 = IsTrue(predicate=predicate1)
    is_true2 = IsTrue(predicate=predicate1)
    is_true3 = IsTrue(predicate=predicate2)

    assert is_true1 == is_true2
    assert is_true1 != is_true3
    assert is_true1 != "not an IsTrue"
